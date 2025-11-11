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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction._
import io.indextables.spark.io.{CloudStorageProvider, CloudStorageProviderFactory}
import org.slf4j.LoggerFactory

/**
 * File information for purge operation.
 *
 * @param path Absolute path to the file (for deletion)
 * @param fileName Filename only (for comparison with transaction log)
 * @param size File size in bytes
 * @param modificationTime Last modification timestamp in milliseconds
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
 *  - Two-dataset anti-join to identify orphaned files
 *  - Distributed file listing and deletion across executors
 *  - Retention-based safety filtering
 *  - Graceful partial failure handling
 *  - Retry logic for transient cloud storage errors
 */
class PurgeOrphanedSplitsExecutor(
    spark: SparkSession,
    tablePath: String,
    retentionHours: Long,
    txLogRetentionDuration: Option[Long],
    dryRun: Boolean) {

  private val logger = LoggerFactory.getLogger(classOf[PurgeOrphanedSplitsExecutor])

  def purge(): PurgeResult = {
    import spark.implicits._

    // Step 1: Get transaction log
    val emptyMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    val txLog = TransactionLogFactory.create(new Path(tablePath), spark, emptyMap)

    // Step 2: Clean up old transaction log files FIRST (before orphaned splits)
    val transactionLogsDeleted = cleanupOldTransactionLogFiles(txLog)
    logger.info(s"Transaction log cleanup: deleted $transactionLogsDeleted old log files")

    val allFiles = txLog.listFiles()
    logger.info(s"Active files in transaction log: ${allFiles.size}")

    // Step 2: List all .split and .crc files from filesystem (distributed)
    val allSplitFiles = listAllSplitFiles(tablePath)
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
        message = Some(s"No split files found in table directory. Deleted $transactionLogsDeleted old transaction log files.")
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
    val retentionTimestamp = System.currentTimeMillis() - (retentionHours * 3600 * 1000)
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
        message = Some(s"$orphanedCount orphaned files found, but all are newer than retention period ($retentionHours hours). Deleted $transactionLogsDeleted old transaction log files.")
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
   * Clean up old transaction log files based on checkpoint and retention policy.
   * This is called BEFORE purging orphaned splits.
   *
   * @return Number of transaction log files deleted (or would be deleted in DRY RUN)
   */
  private def cleanupOldTransactionLogFiles(txLog: io.indextables.spark.transaction.TransactionLog): Long = {
    try {
      logger.info(s"Cleaning up old transaction log files (dryRun=$dryRun)...")

      // Get checkpoint version using public API
      val checkpointVersionOpt = txLog.getLastCheckpointVersion()

      checkpointVersionOpt match {
        case Some(checkpointVersion) =>
          // Get retention configuration - use explicit parameter if provided, otherwise fall back to config
          val logRetentionDuration = txLogRetentionDuration.getOrElse {
            spark.conf
              .getOption("spark.indextables.logRetention.duration")
              .map(_.toLong)
              .getOrElse(30L * 24 * 60 * 60 * 1000) // 30 days default
          }

          val currentTime = System.currentTimeMillis()
          val transactionLogPath = new Path(tablePath, "_transaction_log")

          // Use CloudStorageProvider for multi-cloud support
          val emptyMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
          val provider = CloudStorageProviderFactory.createProvider(
            transactionLogPath.toString,
            emptyMap,
            spark.sparkContext.hadoopConfiguration
          )

          try {
            // List all transaction log files and determine current version
            val allFiles = provider.listFiles(transactionLogPath.toString, recursive = false)

            // Extract version numbers from .json files (excluding checkpoints)
            val versions = allFiles
              .map(f => new Path(f.path).getName)
              .filter(_.endsWith(".json"))
              .filterNot(_.contains("checkpoint"))
              .filterNot(_.startsWith("_"))
              .map(_.replace(".json", "").toLong)

            val currentVersion = if (versions.nonEmpty) versions.max else 0L

            logger.info(s"Cleanup: checkpoint=$checkpointVersion, current=$currentVersion, retention=${logRetentionDuration}ms")

            // Only cleanup versions older than checkpoint
            val versionsToCheck = (0L until currentVersion).filter(_ < checkpointVersion)

            var deletedCount = 0L

            versionsToCheck.foreach { version =>
              val versionFileName = f"$version%020d.json"

              allFiles.find(f => new Path(f.path).getName == versionFileName) match {
                case Some(fileInfo) =>
                  val fileAge = currentTime - fileInfo.modificationTime

                  if (fileAge > logRetentionDuration) {
                    if (dryRun) {
                      logger.info(s"DRY RUN: Would delete transaction log file: $versionFileName (age: ${fileAge / 1000}s)")
                      deletedCount += 1
                    } else {
                      if (provider.deleteFile(fileInfo.path)) {
                        deletedCount += 1
                        logger.info(s"Deleted transaction log file: $versionFileName (age: ${fileAge / 1000}s)")
                      }
                    }
                  } else {
                    logger.debug(s"Skipping transaction log file $versionFileName (age: ${fileAge / 1000}s, retention: ${logRetentionDuration / 1000}s)")
                  }
                case None =>
                  // File doesn't exist, skip
                  ()
              }
            }

            if (deletedCount > 0) {
              val action = if (dryRun) "Would delete" else "Deleted"
              logger.info(s"$action $deletedCount old transaction log files (retention: ${logRetentionDuration / 1000}s)")
            } else {
              logger.info("No old transaction log files to delete")
            }

            deletedCount
          } finally {
            provider.close()
          }

        case None =>
          logger.info("No checkpoint available - skipping transaction log cleanup")
          0L
      }
    } catch {
      case e: Exception =>
        // Don't fail the entire purge operation if transaction log cleanup fails
        logger.warn(s"Failed to clean up old transaction log files: ${e.getMessage}", e)
        0L
    }
  }

  /**
   * List all .split and .crc files from the table directory.
   * Uses distributed listing across executors (Iceberg pattern).
   */
  private def listAllSplitFiles(basePath: String): Dataset[FileInfo] = {
    import spark.implicits._

    // Use CloudStorageProvider for multi-cloud support
    val emptyMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    val provider = CloudStorageProviderFactory.createProvider(
      basePath,
      emptyMap,
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
            path = fileInfo.path,  // Full path for deletion
            fileName = fileName,    // Filename only for comparison
            size = fileInfo.size,
            modificationTime = fileInfo.modificationTime
          )
        }

      logger.info(s"Listed ${files.size} split/crc files from filesystem")
      spark.createDataset(files)
    } finally {
      provider.close()
    }
  }

  /**
   * Get all valid split files from the transaction log.
   * Includes files from current state and their corresponding .crc files.
   */
  private def getValidSplitFilesFromTransactionLog(
      addActions: Seq[AddAction]): Dataset[String] = {
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
   * Find orphaned files using LEFT ANTI JOIN pattern (Delta Lake/Iceberg).
   * Files in filesystem that are NOT in transaction log are orphaned.
   */
  private def findOrphanedFiles(
      allFiles: Dataset[FileInfo],
      validFiles: Dataset[String]): Dataset[FileInfo] = {
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

  /**
   * Preview deletion (DRY RUN mode).
   */
  private def previewDeletion(files: Dataset[FileInfo], totalEligibleCount: Long, orphanedCount: Long, transactionLogsDeleted: Long): PurgeResult = {
    val filesToPreview = files.collect()
    val totalSizeBytes = filesToPreview.map(_.size).sum
    val totalSizeMB = totalSizeBytes / (1024.0 * 1024.0)
    val count = filesToPreview.length

    logger.info(s"DRY RUN: Would delete $count files (${totalSizeMB} MB)")

    // Show sample of files that would be deleted
    println("\n=== DRY RUN: Files that would be deleted ===")
    println(f"${"Path"}%-100s ${"Size (MB)"}%12s ${"Modified"}%20s")
    println("-" * 135)

    filesToPreview
      .sortBy(-_.size) // Sort by size descending
      .take(20)
      .foreach { file =>
        val sizeMB = file.size / (1024.0 * 1024.0)
        val modifiedDate = new java.util.Date(file.modificationTime)
        println(f"${file.path}%-100s ${sizeMB}%12.2f ${modifiedDate}%20s")
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
      message = Some(s"Dry run completed. $count split files would be deleted (${totalSizeMB} MB). ${transactionLogsDeleted} transaction log files would be deleted.")
    )
  }

  /**
   * Execute file deletion (distributed across executors).
   */
  private def executeDeletion(files: Dataset[FileInfo], totalEligibleCount: Long, orphanedCount: Long, transactionLogsDeleted: Long): PurgeResult = {
    import spark.implicits._

    val filesToDelete = files.collect()
    val totalSizeBytes = filesToDelete.map(_.size).sum
    val totalSizeMB = totalSizeBytes / (1024.0 * 1024.0)
    val totalCount = filesToDelete.length

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

    // Distribute deletion across executors using CloudStorageProvider
    val hadoopConf = spark.sparkContext.broadcast(
      new org.apache.spark.util.SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    )

    val deletionResults = spark
      .createDataset(filesToDelete)
      .repartition(parallelism)
      .mapPartitions { fileIter =>
        val conf = hadoopConf.value.value
        var successCount = 0L
        var failCount = 0L
        var deletedBytes = 0L
        val retries = maxRetries

        // Create CloudStorageProvider for this partition
        val emptyMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
        var provider: CloudStorageProvider = null

        try {
          fileIter.foreach { fileInfo =>
            // Lazy initialize provider on first file (to get base path)
            if (provider == null) {
              // Extract base path from file path (everything before the filename)
              val basePath = fileInfo.path.substring(0, fileInfo.path.lastIndexOf('/'))
              provider = CloudStorageProviderFactory.createProvider(basePath, emptyMap, conf)
            }

            // Inline retry logic to avoid serialization issues
            var attempt = 0
            var deleted = false
            var retrying = true
            while (retrying && attempt < retries) {
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
            }

            if (deleted) {
              successCount += 1
              deletedBytes += fileInfo.size
            } else {
              failCount += 1
            }
          }
        } finally {
          if (provider != null) {
            provider.close()
          }
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
    logger.info(s"Total size deleted: ${deletedSizeMB} MB")

    PurgeResult(
      status = if (totalFailed == 0) "SUCCESS" else "PARTIAL_SUCCESS",
      orphanedFilesFound = orphanedCount,
      orphanedFilesDeleted = totalSuccess,
      sizeMBDeleted = deletedSizeMB,
      transactionLogsDeleted = transactionLogsDeleted,
      message = if (totalFailed > 0) {
        Some(s"Successfully deleted $totalSuccess split files, $totalFailed files failed to delete. Deleted $transactionLogsDeleted transaction log files.")
      } else {
        Some(s"Successfully deleted $totalSuccess orphaned split files (${deletedSizeMB} MB) and $transactionLogsDeleted transaction log files.")
      }
    )
  }

}
