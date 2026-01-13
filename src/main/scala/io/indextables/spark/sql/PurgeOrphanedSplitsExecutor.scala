/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.indextables.spark.sql

import java.io.{BufferedReader, InputStreamReader}

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.{CloudFileInfo, CloudStorageProvider, CloudStorageProviderFactory}
import io.indextables.spark.transaction._
import io.indextables.spark.transaction.compression.CompressionUtils
import org.slf4j.LoggerFactory

/**
 * File information for purge operation.
 *
 * @param path
 *   Absolute path to the file (for deletion)
 * @param fileName
 *   Filename only (for comparison with transaction log)
 * @param size
 *   File size in bytes
 * @param modificationTime
 *   Last modification timestamp in milliseconds
 */
case class FileInfo(
  path: String,
  fileName: String,
  size: Long,
  modificationTime: Long)

/**
 * Executor for PURGE ORPHANED SPLITS command.
 *
 * Implementation follows Delta Lake VACUUM and Iceberg DeleteOrphanFiles patterns:
 *   - Two-dataset anti-join to identify orphaned files
 *   - Distributed file listing and deletion across executors
 *   - Retention-based safety filtering
 *   - Graceful partial failure handling
 *   - Retry logic for transient cloud storage errors
 *
 * @param overrideOptions
 *   Optional map of configuration overrides (e.g., from write options). Used to pass AWS/Azure credentials and other
 *   settings from write operations.
 */
class PurgeOrphanedSplitsExecutor(
  spark: SparkSession,
  tablePath: String,
  retentionHours: Long,
  txLogRetentionDuration: Option[Long],
  dryRun: Boolean,
  overrideOptions: Option[Map[String, String]] = None) {

  private val logger = LoggerFactory.getLogger(classOf[PurgeOrphanedSplitsExecutor])

  def purge(): PurgeResult = {
    // Step 1: Get transaction log with resolved credentials
    val cloudConfigs = extractCloudStorageConfigs()
    import scala.jdk.CollectionConverters._
    val txLog = TransactionLogFactory.create(new Path(tablePath), spark, new CaseInsensitiveStringMap(cloudConfigs.asJava))

    // Step 2: Determine which transaction log files will be deleted
    // Get the list of versions that will remain after cleanup (for time travel support)
    val versionsBeforeCleanup = txLog.getVersions()
    val versionsToDelete      = getTransactionLogVersionsToDelete(txLog)
    val versionsToKeep        = versionsBeforeCleanup.filterNot(versionsToDelete.contains)
    logger.info(s"Transaction log versions: ${versionsBeforeCleanup.size} total, ${versionsToDelete.size} to delete, ${versionsToKeep.size} to keep")

    // Step 3: Get ALL files referenced in ANY retained transaction file or checkpoint
    // CRITICAL FIX: For time travel support, we must preserve files from:
    //   1. All retained checkpoints (not just the latest)
    //   2. All retained version files (those not being deleted)
    //
    // A file should NOT be deleted if it appears in ANY transaction file or
    // checkpoint that still exists after the purge operation.
    val allRetainedFiles = getAllFilesFromRetainedState(txLog, versionsToKeep)
    logger.info(s"Files referenced in retained transaction state: ${allRetainedFiles.size}")

    // Step 4: Clean up old transaction log files AFTER getting current state
    // This prevents race condition where we delete tx logs and then try to read them.
    // Use pre-computed versionsToDelete to ensure consistency.
    val transactionLogsDeleted = cleanupOldTransactionLogFilesWithVersions(txLog, versionsToDelete)
    logger.info(s"Transaction log cleanup: deleted $transactionLogsDeleted old log files")

    // Step 5: Clean up old checkpoint files
    // Checkpoints older than retention period are safe to delete (except the most recent one)
    val checkpointsDeleted = cleanupOldCheckpointFiles(txLog)
    logger.info(s"Checkpoint cleanup: deleted $checkpointsDeleted old checkpoint files")

    // Use all retained files as the valid files set
    val allFiles = allRetainedFiles

    // Step 2: List all .split and .crc files from filesystem (distributed)
    val allSplitFiles        = listAllSplitFiles(tablePath)
    val totalFilesystemCount = allSplitFiles.count()
    logger.info(s"Total split/crc files found in filesystem: $totalFilesystemCount")

    if (totalFilesystemCount == 0) {
      logger.info("No split files found in filesystem")
      return PurgeResult(
        status = if (dryRun) "DRY_RUN" else "SUCCESS",
        orphanedFilesFound = 0,
        orphanedFilesDeleted = 0,
        sizeMBDeleted = 0.0,
        transactionLogsDeleted = transactionLogsDeleted,
        message =
          Some(s"No split files found in table directory. Deleted $transactionLogsDeleted old transaction log files.")
      )
    }

    // Step 3: Get valid split files from transaction log
    val validSplitFiles = getValidSplitFilesFromTransactionLog(allFiles)
    val validFilesCount = validSplitFiles.count()
    logger.info(s"Valid split/crc files in transaction log: $validFilesCount")

    // Step 4: Find orphaned files (LEFT ANTI JOIN)
    val orphanedFiles = findOrphanedFiles(allSplitFiles, validSplitFiles)
    val orphanedCount = orphanedFiles.count()
    logger.info(s"Orphaned files found (before retention filter): $orphanedCount")

    if (orphanedCount == 0) {
      logger.info("No orphaned files found")
      return PurgeResult(
        status = if (dryRun) "DRY_RUN" else "SUCCESS",
        orphanedFilesFound = 0,
        orphanedFilesDeleted = 0,
        sizeMBDeleted = 0.0,
        transactionLogsDeleted = transactionLogsDeleted,
        message = Some(s"No orphaned files found. Deleted $transactionLogsDeleted old transaction log files.")
      )
    }

    // Step 5: Apply retention filter
    val retentionTimestamp  = System.currentTimeMillis() - (retentionHours * 3600 * 1000)
    val eligibleForDeletion = orphanedFiles.filter(col("modificationTime") < retentionTimestamp)

    val eligibleCount = eligibleForDeletion.count()
    logger.info(s"Orphaned files eligible for deletion (after retention filter): $eligibleCount")
    logger.info(s"Orphaned files skipped (too recent): ${orphanedCount - eligibleCount}")

    if (eligibleCount == 0) {
      return PurgeResult(
        status = if (dryRun) "DRY_RUN" else "SUCCESS",
        orphanedFilesFound = orphanedCount,
        orphanedFilesDeleted = 0,
        sizeMBDeleted = 0.0,
        transactionLogsDeleted = transactionLogsDeleted,
        message =
          Some(s"$orphanedCount orphaned files found, but all are newer than retention period ($retentionHours hours). Deleted $transactionLogsDeleted old transaction log files.")
      )
    }

    // Step 6: Check max files limit
    val maxFilesToDelete = spark.conf
      .getOption("spark.indextables.purge.maxFilesToDelete")
      .map(_.toLong)
      .getOrElse(1000000L) // 1M default

    val filesToDelete = if (eligibleCount > maxFilesToDelete) {
      logger.warn(s"Limiting deletion to $maxFilesToDelete files (found $eligibleCount eligible)")
      eligibleForDeletion.limit(maxFilesToDelete.toInt)
    } else {
      eligibleForDeletion
    }

    // Step 7: Delete or preview orphaned splits
    if (dryRun) {
      previewDeletion(filesToDelete, eligibleCount, orphanedCount, transactionLogsDeleted)
    } else {
      executeDeletion(filesToDelete, eligibleCount, orphanedCount, transactionLogsDeleted)
    }
  }

  /**
   * Clean up old transaction log files using a pre-computed set of versions to delete.
   * This eliminates the race condition by not recalculating which versions to delete.
   *
   * CRITICAL FIX: The original cleanupOldTransactionLogFiles() independently recalculated
   * which versions to delete, which could differ from getTransactionLogVersionsToDelete()
   * if time passed between the two calls (files aging past retention boundary).
   *
   * This method accepts the pre-computed versionsToDelete set to ensure consistency.
   *
   * @param txLog The transaction log instance
   * @param versionsToDelete Pre-computed set of versions to delete (from getTransactionLogVersionsToDelete)
   * @return Number of transaction log files deleted (or would be deleted in DRY RUN)
   */
  private def cleanupOldTransactionLogFilesWithVersions(
    txLog: io.indextables.spark.transaction.TransactionLog,
    versionsToDelete: Set[Long]
  ): Long =
    try {
      if (versionsToDelete.isEmpty) {
        logger.info("No transaction log versions to delete")
        return 0L
      }

      logger.info(s"Cleaning up ${versionsToDelete.size} old transaction log files (dryRun=$dryRun)...")

      val transactionLogPath = new Path(tablePath, "_transaction_log")

      // Use CloudStorageProvider for multi-cloud support with credentials
      val cloudConfigs = extractCloudStorageConfigs()
      val optionsMap   = new java.util.HashMap[String, String]()
      cloudConfigs.foreach { case (k, v) => optionsMap.put(k, v) }
      val configOptions = new CaseInsensitiveStringMap(optionsMap)

      val provider = CloudStorageProviderFactory.createProvider(
        transactionLogPath.toString,
        configOptions,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        var deletedCount = 0L

        // Use the PRE-COMPUTED set - no recalculation!
        versionsToDelete.toSeq.sorted.foreach { version =>
          val versionFileName = f"$version%020d.json"
          val versionFilePath = new Path(transactionLogPath, versionFileName).toString

          try {
            if (dryRun) {
              logger.info(s"DRY RUN: Would delete transaction log file: $versionFileName")
              deletedCount += 1
            } else {
              if (provider.deleteFile(versionFilePath)) {
                deletedCount += 1
                logger.debug(s"Deleted transaction log file: $versionFileName")
              }
            }
          } catch {
            case _: java.io.FileNotFoundException =>
              // Already deleted, continue
              logger.debug(s"Transaction log file already deleted: $versionFileName")
            case e: Exception =>
              logger.warn(s"Failed to delete transaction log file $versionFileName: ${e.getMessage}")
          }
        }

        if (deletedCount > 0) {
          val action = if (dryRun) "Would delete" else "Deleted"
          logger.info(s"$action $deletedCount old transaction log files")
        }

        deletedCount
      } finally
        provider.close()

    } catch {
      case e: Exception =>
        // Don't fail the entire purge operation if transaction log cleanup fails
        logger.warn(s"Failed to clean up old transaction log files: ${e.getMessage}", e)
        0L
    }

  // Regex patterns for checkpoint file detection
  // Manifest/legacy checkpoint: <version>.checkpoint.json (20 digits + .checkpoint.json)
  private val ManifestPattern = """^(\d{20})\.checkpoint\.json$""".r
  // Part file: <version>.checkpoint.<uuid>.<partNum>.json
  private val PartFilePattern = """^(\d{20})\.checkpoint\.([a-f0-9]+)\.(\d{5})\.json$""".r

  /**
   * Clean up old checkpoint files based on retention policy.
   *
   * Handles both:
   *   1. Legacy single-file checkpoints: `<version>.checkpoint.json`
   *   2. Multi-part checkpoints: manifest + UUID-based part files
   *
   * For multi-part checkpoints, deletes both the manifest AND all referenced parts.
   * Also cleans up orphaned part files (from failed checkpoint attempts).
   *
   * @param txLog The transaction log instance
   * @return Number of checkpoint files deleted (or would be deleted in DRY RUN)
   */
  private def cleanupOldCheckpointFiles(txLog: io.indextables.spark.transaction.TransactionLog): Long =
    try {
      import io.indextables.spark.util.JsonUtil

      val transactionLogPath = new Path(tablePath, "_transaction_log")

      // Get retention configuration - use explicit parameter if provided, otherwise fall back to config
      val logRetentionDuration = txLogRetentionDuration.getOrElse {
        spark.conf
          .getOption("spark.indextables.logRetention.duration")
          .map(_.toLong)
          .getOrElse(30L * 24 * 60 * 60 * 1000) // 30 days default
      }

      val currentTime = System.currentTimeMillis()

      // Get the latest checkpoint version to preserve
      val latestCheckpointVersion = txLog.getLastCheckpointVersion()

      if (latestCheckpointVersion.isEmpty) {
        logger.info("No checkpoint available - skipping checkpoint cleanup")
        return 0L
      }

      val latestVersion = latestCheckpointVersion.get
      logger.info(s"Cleaning up old checkpoint files (preserving latest checkpoint at v$latestVersion, dryRun=$dryRun)...")

      // Use CloudStorageProvider for multi-cloud support with credentials
      val cloudConfigs = extractCloudStorageConfigs()
      val optionsMap   = new java.util.HashMap[String, String]()
      cloudConfigs.foreach { case (k, v) => optionsMap.put(k, v) }
      val configOptions = new CaseInsensitiveStringMap(optionsMap)

      val provider = CloudStorageProviderFactory.createProvider(
        transactionLogPath.toString,
        configOptions,
        spark.sparkContext.hadoopConfiguration
      )

      try {
        // List all files in transaction log directory
        val allFiles = provider.listFiles(transactionLogPath.toString, recursive = false)

        // Separate manifest/legacy checkpoints from part files
        val manifestFiles = scala.collection.mutable.Map[Long, io.indextables.spark.io.CloudFileInfo]()
        val partFiles = scala.collection.mutable.ListBuffer[(Long, String, io.indextables.spark.io.CloudFileInfo)]() // (version, uuid, fileInfo)

        allFiles.foreach { f =>
          val fileName = new Path(f.path).getName
          fileName match {
            case ManifestPattern(versionStr) =>
              val version = versionStr.toLong
              manifestFiles(version) = f
            case PartFilePattern(versionStr, uuid, _) =>
              val version = versionStr.toLong
              partFiles += ((version, uuid, f))
            case _ => // Ignore other files
          }
        }

        logger.info(s"Found ${manifestFiles.size} checkpoint manifests/legacy checkpoints and ${partFiles.size} checkpoint parts")

        var deletedCount = 0L

        // Track which UUIDs belong to retained checkpoints
        val retainedCheckpointIds = scala.collection.mutable.Set[String]()

        // Process manifest/legacy checkpoint files
        manifestFiles.foreach { case (checkpointVersion, checkpointFile) =>
          val fileName = new Path(checkpointFile.path).getName
          val fileAge = currentTime - checkpointFile.modificationTime

          // Never delete the latest checkpoint, even if it's old
          if (checkpointVersion == latestVersion) {
            logger.debug(s"Preserving latest checkpoint: $fileName (v$checkpointVersion)")

            // If this is a manifest, track its checkpoint ID (uses file size heuristic to skip large legacy checkpoints)
            tryReadManifest(provider, checkpointFile).foreach { manifest =>
              retainedCheckpointIds += manifest.checkpointId
              logger.debug(s"Tracking retained checkpoint ID: ${manifest.checkpointId}")
            }

          } else if (fileAge > logRetentionDuration) {
            // Delete old checkpoint (manifest or legacy)
            // Check if it's a manifest and get part files to delete (uses file size heuristic to skip large legacy checkpoints)
            val partsToDelete = tryReadManifest(provider, checkpointFile) match {
              case Some(manifest) =>
                logger.debug(s"Checkpoint v$checkpointVersion is multi-part with ${manifest.parts.size} parts")
                manifest.parts.map(p => s"${transactionLogPath.toString}/$p")
              case None =>
                Seq.empty[String]
            }

            if (dryRun) {
              logger.info(s"DRY RUN: Would delete old checkpoint file: $fileName (v$checkpointVersion, age: ${fileAge / 1000}s)")
              deletedCount += 1
              if (partsToDelete.nonEmpty) {
                logger.info(s"DRY RUN: Would delete ${partsToDelete.size} part files for checkpoint v$checkpointVersion")
                deletedCount += partsToDelete.size
              }
            } else {
              // Delete manifest/legacy checkpoint
              try {
                if (provider.deleteFile(checkpointFile.path)) {
                  deletedCount += 1
                  logger.debug(s"Deleted old checkpoint file: $fileName (v$checkpointVersion, age: ${fileAge / 1000}s)")
                }
              } catch {
                case _: java.io.FileNotFoundException =>
                  logger.debug(s"Checkpoint file already deleted: $fileName")
                case e: Exception =>
                  logger.warn(s"Failed to delete checkpoint file $fileName: ${e.getMessage}")
              }

              // Delete associated part files
              partsToDelete.foreach { partPath =>
                try {
                  if (provider.deleteFile(partPath)) {
                    deletedCount += 1
                    logger.debug(s"Deleted checkpoint part: ${new Path(partPath).getName}")
                  }
                } catch {
                  case _: java.io.FileNotFoundException =>
                    logger.debug(s"Checkpoint part already deleted: $partPath")
                  case e: Exception =>
                    logger.warn(s"Failed to delete checkpoint part $partPath: ${e.getMessage}")
                }
              }
            }
          } else {
            logger.debug(s"Keeping checkpoint: $fileName (v$checkpointVersion, age: ${fileAge / 1000}s, retention: ${logRetentionDuration / 1000}s)")

            // Track checkpoint ID if it's a manifest within retention (uses file size heuristic to skip large legacy checkpoints)
            tryReadManifest(provider, checkpointFile).foreach { manifest =>
              retainedCheckpointIds += manifest.checkpointId
            }
          }
        }

        // Clean up orphaned part files (parts not belonging to any retained checkpoint)
        val orphanedParts = partFiles.filter { case (_, uuid, _) => !retainedCheckpointIds.contains(uuid) }

        if (orphanedParts.nonEmpty) {
          logger.info(s"Found ${orphanedParts.size} orphaned checkpoint parts to clean up")

          orphanedParts.foreach { case (version, uuid, partFile) =>
            val fileName = new Path(partFile.path).getName
            if (dryRun) {
              logger.info(s"DRY RUN: Would delete orphaned checkpoint part: $fileName (v$version, uuid=$uuid)")
              deletedCount += 1
            } else {
              try {
                if (provider.deleteFile(partFile.path)) {
                  deletedCount += 1
                  logger.debug(s"Deleted orphaned checkpoint part: $fileName")
                }
              } catch {
                case _: java.io.FileNotFoundException =>
                  logger.debug(s"Orphaned part already deleted: $fileName")
                case e: Exception =>
                  logger.warn(s"Failed to delete orphaned part $fileName: ${e.getMessage}")
              }
            }
          }
        }

        if (deletedCount > 0) {
          val action = if (dryRun) "Would delete" else "Deleted"
          logger.info(s"$action $deletedCount old checkpoint files (manifests + parts)")
        } else {
          logger.info("No old checkpoint files to delete")
        }

        deletedCount
      } finally
        provider.close()

    } catch {
      case e: Exception =>
        // Don't fail the entire purge operation if checkpoint cleanup fails
        logger.warn(s"Failed to clean up old checkpoint files: ${e.getMessage}", e)
        0L
    }

  /**
   * Determine which transaction log versions will be deleted based on checkpoint and retention policy. This duplicates
   * the logic from cleanupOldTransactionLogFiles but doesn't actually delete.
   *
   * Returns: Set of version numbers that will be deleted
   */
  private def getTransactionLogVersionsToDelete(txLog: io.indextables.spark.transaction.TransactionLog): Set[Long] = {
    val checkpointVersionOpt = txLog.getLastCheckpointVersion()

    checkpointVersionOpt match {
      case Some(checkpointVersion) =>
        val logRetentionDuration = txLogRetentionDuration.getOrElse {
          spark.conf
            .getOption("spark.indextables.logRetention.duration")
            .map(_.toLong)
            .getOrElse(30L * 24 * 60 * 60 * 1000) // 30 days default
        }

        val currentTime        = System.currentTimeMillis()
        val transactionLogPath = new Path(tablePath, "_transaction_log")

        val cloudConfigs = extractCloudStorageConfigs()
        val optionsMap   = new java.util.HashMap[String, String]()
        cloudConfigs.foreach { case (k, v) => optionsMap.put(k, v) }
        val configOptions = new CaseInsensitiveStringMap(optionsMap)

        val provider = CloudStorageProviderFactory.createProvider(
          transactionLogPath.toString,
          configOptions,
          spark.sparkContext.hadoopConfiguration
        )

        try {
          val allFiles = provider.listFiles(transactionLogPath.toString, recursive = false)
          val versions = allFiles
            .map(f => new Path(f.path).getName)
            .filter(_.endsWith(".json"))
            .filterNot(_.contains("checkpoint"))
            .filterNot(_.startsWith("_"))
            .map(_.replace(".json", "").toLong)

          val currentVersion  = if (versions.nonEmpty) versions.max else 0L
          val versionsToCheck = (0L until currentVersion).filter(_ < checkpointVersion)

          val toDelete = versionsToCheck.filter { version =>
            val versionFileName = f"$version%020d.json"
            allFiles.find(f => new Path(f.path).getName == versionFileName) match {
              case Some(fileInfo) =>
                val fileAge = currentTime - fileInfo.modificationTime
                fileAge > logRetentionDuration
              case None => false
            }
          }.toSet

          logger.debug(s"Identified ${toDelete.size} transaction log versions to delete (< checkpoint v$checkpointVersion, older than retention)")
          toDelete
        } finally
          provider.close()

      case None =>
        logger.debug("No checkpoint available - no transaction logs will be deleted")
        Set.empty[Long]
    }
  }

  /**
   * Get ALL files referenced in ANY of the specified transaction log versions. This is critical for time travel support
   * \- we can't delete files that might be referenced by historical versions within the retention window.
   *
   * @param versionsToScan
   *   The specific versions to scan (typically versions that will remain after cleanup) Returns: Seq[AddAction]
   *   containing all files that appear in any of the specified versions
   */
  private def getAllFilesFromVersions(txLog: io.indextables.spark.transaction.TransactionLog, versionsToScan: Seq[Long])
    : Seq[AddAction] = {
    logger.info(s"Scanning ${versionsToScan.size} transaction log versions for file references (time travel support)")

    // Collect all unique file paths that appear in ANY of the specified versions
    val allFilePaths     = scala.collection.mutable.Set[String]()
    val filePathToAction = scala.collection.mutable.HashMap[String, AddAction]()

    versionsToScan.sorted.foreach { version =>
      try {
        val actions = txLog.readVersion(version)
        actions.foreach {
          case add: AddAction =>
            allFilePaths += add.path
            // Keep the most recent AddAction for each path (for metadata like size)
            filePathToAction(add.path) = add
          case _ => // Ignore remove, protocol, metadata for this purpose
        }
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to read version $version while scanning for file references: ${e.getMessage}")
      }
    }

    logger.info(
      s"Found ${allFilePaths.size} unique files referenced across ${versionsToScan.size} transaction log versions"
    )
    filePathToAction.values.toSeq
  }

  /**
   * Get ALL files referenced in ANY retained transaction file or checkpoint.
   *
   * For time travel support, we must preserve files from:
   *   1. All retained checkpoints (those not being deleted)
   *   2. All retained version files (those in versionsToKeep)
   *
   * A file should NOT be deleted if it appears in ANY transaction file or
   * checkpoint that still exists after the purge operation.
   *
   * @param txLog The transaction log instance
   * @param versionsToKeep The version numbers that will be retained (not deleted)
   * @return All AddActions from retained state
   */
  private def getAllFilesFromRetainedState(
    txLog: io.indextables.spark.transaction.TransactionLog,
    versionsToKeep: Seq[Long]
  ): Seq[AddAction] = {
    val allFilePaths = scala.collection.mutable.Set[String]()
    val filePathToAction = scala.collection.mutable.HashMap[String, AddAction]()

    // Step 1: Get files from ALL retained checkpoints
    val checkpointFiles = getFilesFromRetainedCheckpoints(txLog)
    checkpointFiles.foreach { add =>
      allFilePaths += add.path
      filePathToAction(add.path) = add
    }
    logger.info(s"Found ${checkpointFiles.size} files from retained checkpoints")

    // Step 2: Get AddActions from ALL retained version files
    val versionFiles = getAllFilesFromVersions(txLog, versionsToKeep)
    versionFiles.foreach { add =>
      allFilePaths += add.path
      // Keep the most recent AddAction for each path (for metadata like size)
      filePathToAction(add.path) = add
    }
    logger.info(s"Found ${versionFiles.size} files from retained version files")

    logger.info(s"Total unique files in retained transaction state: ${allFilePaths.size}")
    filePathToAction.values.toSeq
  }

  /**
   * Get files from ALL retained checkpoints.
   *
   * A checkpoint is retained if:
   *   1. It's the latest checkpoint (always preserved), OR
   *   2. It's newer than the retention period
   *
   * Handles both legacy single-file checkpoints and multi-part checkpoints with manifests.
   *
   * @param txLog The transaction log instance
   * @return All AddActions from retained checkpoints
   */
  private def getFilesFromRetainedCheckpoints(
    txLog: io.indextables.spark.transaction.TransactionLog
  ): Seq[AddAction] = {
    val transactionLogPath = new Path(tablePath, "_transaction_log")

    // Get retention configuration
    val logRetentionDuration = txLogRetentionDuration.getOrElse {
      spark.conf
        .getOption("spark.indextables.logRetention.duration")
        .map(_.toLong)
        .getOrElse(30L * 24 * 60 * 60 * 1000) // 30 days default
    }

    val currentTime = System.currentTimeMillis()
    val latestCheckpointVersion = txLog.getLastCheckpointVersion()

    // Use CloudStorageProvider for multi-cloud support
    val cloudConfigs = extractCloudStorageConfigs()
    val optionsMap = new java.util.HashMap[String, String]()
    cloudConfigs.foreach { case (k, v) => optionsMap.put(k, v) }
    val configOptions = new CaseInsensitiveStringMap(optionsMap)

    val provider = CloudStorageProviderFactory.createProvider(
      transactionLogPath.toString,
      configOptions,
      spark.sparkContext.hadoopConfiguration
    )

    try {
      // List all files in transaction log directory
      val allFiles = provider.listFiles(transactionLogPath.toString, recursive = false)

      // Filter for manifest/legacy checkpoint files only (NOT part files)
      // Part files match: <version>.checkpoint.<uuid>.<partNum>.json
      // Manifest/legacy files match: <version>.checkpoint.json
      val checkpointFiles = allFiles.filter { f =>
        val fileName = new Path(f.path).getName
        ManifestPattern.findFirstIn(fileName).isDefined
      }

      logger.info(s"Found ${checkpointFiles.size} checkpoint manifests/legacy files in transaction log")

      val filePathToAction = scala.collection.mutable.HashMap[String, AddAction]()

      checkpointFiles.foreach { checkpointFile =>
        val fileName = new Path(checkpointFile.path).getName
        val fileAge = currentTime - checkpointFile.modificationTime

        // Extract version number from checkpoint filename using pattern
        val checkpointVersion = fileName match {
          case ManifestPattern(versionStr) => versionStr.toLong
          case _ =>
            logger.warn(s"Skipping checkpoint file with unparseable name: $fileName")
            -1L
        }

        // Determine if this checkpoint is retained
        val isLatestCheckpoint = latestCheckpointVersion.contains(checkpointVersion)
        val isWithinRetention = fileAge <= logRetentionDuration
        val isRetained = isLatestCheckpoint || isWithinRetention

        if (checkpointVersion >= 0 && isRetained) {
          logger.debug(s"Reading retained checkpoint: $fileName (latest=$isLatestCheckpoint, withinRetention=$isWithinRetention)")

          // Read the checkpoint file and extract AddActions
          // readCheckpointFile handles both legacy and manifest-based checkpoints
          try {
            val checkpointActions = readCheckpointFile(provider, checkpointFile.path)
            checkpointActions.foreach {
              case add: AddAction =>
                filePathToAction(add.path) = add
              case _ => // Ignore other action types
            }
            logger.debug(s"Read ${checkpointActions.count(_.isInstanceOf[AddAction])} AddActions from checkpoint v$checkpointVersion")
          } catch {
            case e: Exception =>
              logger.warn(s"Failed to read checkpoint file $fileName: ${e.getMessage}")
          }
        } else if (checkpointVersion >= 0) {
          logger.debug(s"Skipping old checkpoint that will be deleted: $fileName (age: ${fileAge / 1000}s)")
        }
      }

      filePathToAction.values.toSeq
    } finally {
      provider.close()
    }
  }

  /**
   * Read actions from a checkpoint file.
   *
   * Handles both:
   *   1. Legacy single-file checkpoints (newline-delimited JSON actions)
   *   2. Multi-part checkpoints (manifest file pointing to part files)
   *
   * Uses streaming to avoid OOM for large checkpoint files (>1GB).
   */
  private def readCheckpointFile(provider: CloudStorageProvider, checkpointPath: String): Seq[Action] = {
    import io.indextables.spark.util.JsonUtil

    // First, check if this is a manifest file by reading just the first line
    // Manifest files are small single-line JSON, so this is safe
    val rawStream = provider.openInputStream(checkpointPath)
    val firstLineReader = new BufferedReader(new InputStreamReader(rawStream, "UTF-8"))
    try {
      val firstLine = firstLineReader.readLine()
      if (firstLine != null && firstLine.trim.startsWith("{") && firstLine.contains("\"checkpointId\"")) {
        // This is a manifest file - read all parts using streaming
        val manifest = JsonUtil.mapper.readValue(firstLine, classOf[MultiPartCheckpointManifest])
        readMultiPartCheckpointStreaming(provider, checkpointPath, manifest)
      } else {
        // Legacy format - use streaming to read the checkpoint
        parseActionsFromStream(provider, checkpointPath)
      }
    } finally {
      firstLineReader.close()
    }
  }

  /**
   * Read a multi-part checkpoint using the manifest file with streaming.
   *
   * Parts are read in parallel for better performance, using streaming to avoid OOM.
   */
  private def readMultiPartCheckpointStreaming(
    provider: CloudStorageProvider,
    manifestPath: String,
    manifest: MultiPartCheckpointManifest
  ): Seq[Action] = {
    import scala.concurrent.{Future, Await}
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global

    val transactionLogPath = new Path(manifestPath).getParent.toString

    logger.debug(s"Reading multi-part checkpoint (id=${manifest.checkpointId}) with ${manifest.parts.size} parts in parallel using streaming")

    // Read all parts in parallel using streaming
    val partFutures = manifest.parts.zipWithIndex.map { case (partFile, idx) =>
      Future {
        val partPath = s"$transactionLogPath/$partFile"

        if (!provider.exists(partPath)) {
          throw new java.io.FileNotFoundException(
            s"Checkpoint part does not exist: $partPath (referenced in manifest)"
          )
        }

        // Use streaming to avoid OOM for large part files
        val partActions = parseActionsFromStream(provider, partPath)

        logger.debug(s"Read ${partActions.length} actions from checkpoint part $partFile")
        (idx, partActions)
      }
    }

    // Wait for all parts and combine in order
    val timeout = 5.minutes
    val results = Await.result(Future.sequence(partFutures), timeout)

    // Sort by index to maintain order and flatten
    results.sortBy(_._1).flatMap(_._2)
  }

  /**
   * Parse actions directly from a cloud storage file using full streaming.
   *
   * This method provides memory-efficient parsing by streaming data from
   * cloud storage directly through decompression and into line-by-line parsing,
   * without ever loading the entire file into memory.
   */
  private def parseActionsFromStream(provider: CloudStorageProvider, filePath: String): Seq[Action] = {
    import io.indextables.spark.util.JsonUtil

    val rawStream = provider.openInputStream(filePath)
    val decompressingStream = CompressionUtils.createDecompressingInputStream(rawStream)
    val reader = new BufferedReader(new InputStreamReader(decompressingStream, "UTF-8"))
    val actions = ListBuffer[Action]()

    try {
      var line = reader.readLine()
      while (line != null) {
        if (line.nonEmpty) {
          val jsonNode = JsonUtil.mapper.readTree(line)

          // Use treeToValue instead of toString + readValue to avoid re-serializing large JSON nodes (OOM fix)
          val actionOpt: Option[Action] = if (jsonNode.has("protocol")) {
            Some(JsonUtil.mapper.treeToValue(jsonNode.get("protocol"), classOf[ProtocolAction]))
          } else if (jsonNode.has("metaData")) {
            Some(JsonUtil.mapper.treeToValue(jsonNode.get("metaData"), classOf[MetadataAction]))
          } else if (jsonNode.has("add")) {
            Some(JsonUtil.mapper.treeToValue(jsonNode.get("add"), classOf[AddAction]))
          } else if (jsonNode.has("remove")) {
            Some(JsonUtil.mapper.treeToValue(jsonNode.get("remove"), classOf[RemoveAction]))
          } else if (jsonNode.has("mergeskip")) {
            Some(JsonUtil.mapper.treeToValue(jsonNode.get("mergeskip"), classOf[SkipAction]))
          } else {
            None
          }
          actionOpt.foreach(actions += _)
        }
        line = reader.readLine()
      }
      actions.toSeq
    } finally {
      reader.close()
    }
  }

  /**
   * Parse actions from newline-delimited JSON content.
   */
  // Use treeToValue instead of toString + readValue to avoid re-serializing large JSON nodes (OOM fix)
  private def parseActionsFromContent(content: String): Seq[Action] = {
    import io.indextables.spark.util.JsonUtil

    content.split("\n").filter(_.nonEmpty).map { line =>
      val jsonNode = JsonUtil.mapper.readTree(line)

      if (jsonNode.has("protocol")) {
        JsonUtil.mapper.treeToValue(jsonNode.get("protocol"), classOf[ProtocolAction])
      } else if (jsonNode.has("metaData")) {
        JsonUtil.mapper.treeToValue(jsonNode.get("metaData"), classOf[MetadataAction])
      } else if (jsonNode.has("add")) {
        JsonUtil.mapper.treeToValue(jsonNode.get("add"), classOf[AddAction])
      } else if (jsonNode.has("remove")) {
        JsonUtil.mapper.treeToValue(jsonNode.get("remove"), classOf[RemoveAction])
      } else if (jsonNode.has("mergeskip")) {
        JsonUtil.mapper.treeToValue(jsonNode.get("mergeskip"), classOf[SkipAction])
      } else {
        throw new IllegalArgumentException(s"Unknown action type in line: $line")
      }
    }.toSeq
  }

  // Multi-part manifest files are small JSON (typically < 2KB)
  // Legacy checkpoints are NDJSON with many actions (typically > 10KB)
  // Use this threshold to skip reading large files entirely
  private val MANIFEST_MAX_SIZE_BYTES = 10 * 1024L // 10KB

  /**
   * Try to read a checkpoint file as a manifest, using file size as a heuristic.
   *
   * This optimization avoids making network requests for large legacy checkpoint files:
   * - If file size > 10KB, it's definitely a legacy checkpoint (no network read needed)
   * - If file size <= 10KB, read first line to check if it's a manifest
   *
   * @param provider Cloud storage provider
   * @param fileInfo File info including size
   * @return Some(manifest) if the file is a multi-part checkpoint manifest, None otherwise
   */
  private def tryReadManifest(provider: CloudStorageProvider, fileInfo: CloudFileInfo): Option[MultiPartCheckpointManifest] = {
    // Skip large files - they're definitely legacy checkpoints, not manifests
    if (fileInfo.size > MANIFEST_MAX_SIZE_BYTES) {
      logger.debug(s"Skipping manifest check for large file (${fileInfo.size} bytes > ${MANIFEST_MAX_SIZE_BYTES}): ${fileInfo.path}")
      return None
    }

    // Small file - could be a manifest, read first line to check
    tryReadManifestFromFirstLine(provider, fileInfo.path)
  }

  /**
   * Safely read just the first line of a checkpoint file to check if it's a manifest.
   *
   * This avoids loading entire legacy checkpoint files (which can be >1GB) into memory.
   * Manifest files are single-line JSON that's very small, while legacy checkpoints
   * are NDJSON with one action per line (potentially millions of lines).
   *
   * @return Some(manifest) if the file is a multi-part checkpoint manifest, None otherwise
   */
  private def tryReadManifestFromFirstLine(provider: CloudStorageProvider, filePath: String): Option[MultiPartCheckpointManifest] = {
    import io.indextables.spark.util.JsonUtil
    import scala.util.Try

    val rawStream = provider.openInputStream(filePath)
    val reader = new BufferedReader(new InputStreamReader(rawStream, "UTF-8"))
    try {
      val firstLine = reader.readLine()
      if (firstLine != null && firstLine.trim.startsWith("{") && firstLine.contains("\"checkpointId\"")) {
        Try(JsonUtil.mapper.readValue(firstLine, classOf[MultiPartCheckpointManifest])).toOption
      } else {
        None
      }
    } catch {
      case _: Exception => None
    } finally {
      reader.close()
    }
  }

  /**
   * Extract all spark.indextables.* configuration from Spark configuration. Returns a map of configuration keys/values
   * to broadcast to executors. This includes AWS credentials, Azure credentials, and any other indextables settings.
   *
   * Priority order (highest to lowest):
   *   1. overrideOptions (from write options) 2. Spark session configuration
   */
  private def extractCloudStorageConfigs(): Map[String, String] = {
    import io.indextables.spark.util.ConfigUtils

    // Get configs from Spark session
    val sparkConfigs = spark.conf.getAll.filter { case (key, _) => key.startsWith("spark.indextables.") }.toMap

    // Merge with override options (override takes precedence)
    val mergedConfigs = overrideOptions match {
      case Some(overrides) =>
        val merged = sparkConfigs ++ overrides.filter { case (key, _) => key.startsWith("spark.indextables.") }
        logger.info(s"Extracted ${merged.size} spark.indextables.* configuration keys (${sparkConfigs.size} from Spark session, ${overrides.size} from override options)")
        merged
      case None =>
        logger.info(s"Extracted ${sparkConfigs.size} spark.indextables.* configuration keys from Spark session")
        sparkConfigs
    }

    // Resolve credentials from custom provider on driver if configured
    // This fetches actual AWS credentials so workers don't need to run the provider
    ConfigUtils.resolveCredentialsFromProviderOnDriver(mergedConfigs, tablePath.toString)
  }

  /**
   * List all .split and .crc files from the table directory. Uses distributed listing across executors (Iceberg
   * pattern).
   */
  private def listAllSplitFiles(basePath: String): Dataset[FileInfo] = {
    import spark.implicits._

    // Use CloudStorageProvider for multi-cloud support with credentials
    val cloudConfigs = extractCloudStorageConfigs()
    val optionsMap   = new java.util.HashMap[String, String]()
    cloudConfigs.foreach { case (k, v) => optionsMap.put(k, v) }
    val configOptions = new CaseInsensitiveStringMap(optionsMap)

    val provider = CloudStorageProviderFactory.createProvider(
      basePath,
      configOptions,
      spark.sparkContext.hadoopConfiguration
    )

    try {
      // List files recursively (exclude _transaction_log directory)
      val files = provider
        .listFiles(basePath, recursive = true)
        .filter { fileInfo =>
          val path = fileInfo.path
          !path.contains("_transaction_log") &&
          !fileInfo.isDirectory &&
          (path.endsWith(".split") || path.endsWith(".crc"))
        }
        .map { fileInfo =>
          // Store both full path (for deletion) and filename (for comparison)
          // Extract filename from path without using Hadoop Path class
          val fileName = fileInfo.path.split('/').last

          FileInfo(
            path = fileInfo.path, // Full path for deletion
            fileName = fileName,  // Filename only for comparison
            size = fileInfo.size,
            modificationTime = fileInfo.modificationTime
          )
        }

      logger.info(s"Listed ${files.size} split/crc files from filesystem")
      spark.createDataset(files)
    } finally
      provider.close()
  }

  /**
   * Get all valid split files from the transaction log. Includes files from current state and their corresponding .crc
   * files.
   */
  private def getValidSplitFilesFromTransactionLog(
    addActions: Seq[AddAction]
  ): Dataset[String] = {
    import spark.implicits._

    // Delta Lake approach: use relative paths for comparison
    // This handles both relative and absolute paths in AddActions
    val addedFiles = addActions.map { addFile =>
      // Extract filename from AddAction path (basename)
      // Transaction log only contains actual files, so we don't need to
      // artificially add .crc files here
      // Extract filename from path without using Hadoop Path class
      addFile.path.split('/').last
    }

    logger.info(s"Found ${addedFiles.size} active files (split + crc) in transaction log")
    spark.createDataset(addedFiles)
  }

  /**
   * Find orphaned files using LEFT ANTI JOIN pattern (Delta Lake/Iceberg). Files in filesystem that are NOT in
   * transaction log are orphaned.
   */
  private def findOrphanedFiles(
    allFiles: Dataset[FileInfo],
    validFiles: Dataset[String]
  ): Dataset[FileInfo] = {
    import spark.implicits._

    // Compare using fileName field (basename comparison)
    allFiles
      .join(
        validFiles.toDF("valid_filename"),
        allFiles("fileName") === col("valid_filename"),
        "leftanti"
      )
      .as[FileInfo]
  }

  /** Preview deletion (DRY RUN mode). */
  private def previewDeletion(
    files: Dataset[FileInfo],
    totalEligibleCount: Long,
    orphanedCount: Long,
    transactionLogsDeleted: Long
  ): PurgeResult = {
    val filesToPreview = files.collect()
    val totalSizeBytes = filesToPreview.map(_.size).sum
    val totalSizeMB    = totalSizeBytes / (1024.0 * 1024.0)
    val count          = filesToPreview.length

    logger.info(s"DRY RUN: Would delete $count files ($totalSizeMB MB)")

    // Show sample of files that would be deleted
    println("\n=== DRY RUN: Files that would be deleted ===")
    println(f"${"Path"}%-100s ${"Size (MB)"}%12s ${"Modified"}%20s")
    println("-" * 135)

    filesToPreview
      .sortBy(-_.size) // Sort by size descending
      .take(20)
      .foreach { file =>
        val sizeMB       = file.size / (1024.0 * 1024.0)
        val modifiedDate = new java.util.Date(file.modificationTime)
        println(f"${file.path}%-100s $sizeMB%12.2f $modifiedDate%20s")
      }

    if (filesToPreview.length > 20) {
      println(s"... and ${filesToPreview.length - 20} more files")
    }
    println("=" * 135)

    PurgeResult(
      status = "DRY_RUN",
      orphanedFilesFound = orphanedCount,
      orphanedFilesDeleted = 0,
      sizeMBDeleted = totalSizeMB,
      transactionLogsDeleted = transactionLogsDeleted,
      message =
        Some(s"Dry run completed. $count split files would be deleted ($totalSizeMB MB). $transactionLogsDeleted transaction log files would be deleted.")
    )
  }

  /** Execute file deletion (distributed across executors). */
  private def executeDeletion(
    files: Dataset[FileInfo],
    totalEligibleCount: Long,
    orphanedCount: Long,
    transactionLogsDeleted: Long
  ): PurgeResult = {
    import spark.implicits._

    val filesToDelete = files.collect()
    val totalCount    = filesToDelete.length

    // Get parallelism config
    val parallelism = spark.conf
      .getOption("spark.indextables.purge.parallelism")
      .map(_.toInt)
      .getOrElse(spark.sparkContext.defaultParallelism)

    logger.info(s"Deleting $totalCount files across $parallelism partitions")

    // Get delete retries config
    val maxRetries = spark.conf
      .getOption("spark.indextables.purge.deleteRetries")
      .map(_.toInt)
      .getOrElse(3)

    // CRITICAL FIX: Extract cloud storage credentials from Spark config and broadcast to executors
    // This allows executors to authenticate with S3/Azure when deleting files
    val cloudStorageConfigs = extractCloudStorageConfigs()
    val broadcastConfigs    = spark.sparkContext.broadcast(cloudStorageConfigs)

    // Distribute deletion across executors using CloudStorageProvider
    val hadoopConf = spark.sparkContext.broadcast(
      new org.apache.spark.util.SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    )

    val deletionResults = spark
      .createDataset(filesToDelete)
      .repartition(parallelism)
      .mapPartitions { fileIter =>
        val conf         = hadoopConf.value.value
        val configs      = broadcastConfigs.value
        var successCount = 0L
        var failCount    = 0L
        var deletedBytes = 0L
        val retries      = maxRetries

        // Create CloudStorageProvider for this partition WITH credentials
        val optionsMap = new java.util.HashMap[String, String]()
        configs.foreach { case (k, v) => optionsMap.put(k, v) }
        val configOptions                  = new CaseInsensitiveStringMap(optionsMap)
        var provider: CloudStorageProvider = null

        try
          fileIter.foreach { fileInfo =>
            // Lazy initialize provider on first file (to get base path)
            if (provider == null) {
              // Extract base path from file path (everything before the filename)
              val basePath = fileInfo.path.substring(0, fileInfo.path.lastIndexOf('/'))
              provider = CloudStorageProviderFactory.createProvider(basePath, configOptions, conf)
            }

            // Inline retry logic to avoid serialization issues
            var attempt  = 0
            var deleted  = false
            var retrying = true
            while (retrying && attempt < retries)
              try {
                deleted = provider.deleteFile(fileInfo.path)
                retrying = false
              } catch {
                case _: java.io.FileNotFoundException =>
                  // Already deleted, consider success
                  deleted = true
                  retrying = false
                case e: Exception if attempt < retries - 1 =>
                  Thread.sleep(100 * (attempt + 1)) // Exponential backoff
                  attempt += 1
                case _: Exception =>
                  deleted = false
                  retrying = false
              }

            if (deleted) {
              successCount += 1
              deletedBytes += fileInfo.size
            } else {
              failCount += 1
            }
          }
        finally
          if (provider != null) {
            provider.close()
          }

        Iterator((successCount, failCount, deletedBytes))
      }
      .collect()

    hadoopConf.unpersist()

    val (totalSuccess, totalFailed, totalDeletedBytes) = deletionResults.foldLeft((0L, 0L, 0L)) {
      case ((s1, f1, b1), (s2, f2, b2)) => (s1 + s2, f1 + f2, b1 + b2)
    }

    val deletedSizeMB = totalDeletedBytes / (1024.0 * 1024.0)

    logger.info(s"Deletion complete: $totalSuccess succeeded, $totalFailed failed")
    logger.info(s"Total size deleted: $deletedSizeMB MB")

    PurgeResult(
      status = if (totalFailed == 0) "SUCCESS" else "PARTIAL_SUCCESS",
      orphanedFilesFound = orphanedCount,
      orphanedFilesDeleted = totalSuccess,
      sizeMBDeleted = deletedSizeMB,
      transactionLogsDeleted = transactionLogsDeleted,
      message = if (totalFailed > 0) {
        Some(s"Successfully deleted $totalSuccess split files, $totalFailed files failed to delete. Deleted $transactionLogsDeleted transaction log files.")
      } else {
        Some(s"Successfully deleted $totalSuccess orphaned split files ($deletedSizeMB MB) and $transactionLogsDeleted transaction log files.")
      }
    )
  }

}
