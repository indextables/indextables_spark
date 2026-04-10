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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{LongType, StringType}

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.{ActionJsonSerializer, TransactionLogFactory}
import io.indextables.spark.util.{ConfigNormalization, ConfigUtils}
import org.slf4j.LoggerFactory

/**
 * SQL command to truncate time travel data from an IndexTables table.
 *
 * This command removes all historical transaction log versions, keeping only the current state (latest checkpoint).
 * After truncation, time travel to earlier versions is no longer possible.
 *
 * Syntax:
 *   - TRUNCATE INDEXTABLES TIME TRAVEL '/path/to/table'
 *   - TRUNCATE INDEXTABLES TIME TRAVEL '/path/to/table' DRY RUN
 *   - TRUNCATE TANTIVY4SPARK TIME TRAVEL table_name
 *
 * This command:
 *   1. Creates a checkpoint at the current version (if none exists) 2. Deletes all transaction log version files older
 *      than the checkpoint 3. Deletes all older checkpoint files (keeps only the latest) 4. Preserves all data files
 *      (splits) - only metadata is affected
 *
 * Use this command to:
 *   - Reduce transaction log storage overhead
 *   - Clean up after many small write operations
 *   - Prepare a table for archival (remove history)
 */
case class TruncateTimeTravelCommand(
  tablePath: String,
  dryRun: Boolean = false)
    extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[TruncateTimeTravelCommand])

  // Regex patterns for file detection (from PurgeOrphanedSplitsExecutor)
  private val VersionFilePattern  = """^(\d{20})\.json$""".r
  private val ManifestPattern     = """^(\d{20})\.checkpoint\.json$""".r
  private val PartFilePattern     = """^(\d{20})\.checkpoint\.([a-f0-9]+)\.(\d{5})\.json$""".r
  private val AvroStateDirPattern = """^state-v(\d{20})$""".r

  override val output: Seq[Attribute] = Seq(
    AttributeReference("table_path", StringType)(),
    AttributeReference("status", StringType)(),
    AttributeReference("checkpoint_version", LongType)(),
    AttributeReference("versions_deleted", LongType)(),
    AttributeReference("checkpoints_deleted", LongType)(),
    AttributeReference("files_preserved", LongType)(),
    AttributeReference("message", StringType)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] =
    try {
      // Resolve the table path
      val resolvedPath = resolveTablePath(tablePath, sparkSession)
      logger.info(s"Truncating time travel data for table: $resolvedPath (dryRun=$dryRun)")

      // Extract and merge configuration with proper precedence (consistent with PURGE command)
      val hadoopConf    = sparkSession.sparkContext.hadoopConfiguration
      val sparkConfigs  = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
      val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
      val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs) +
        ("spark.indextables.databricks.credential.operation" -> "PATH_READ_WRITE")

      // Create transaction log with PATH_READ_WRITE credentials since truncate
      // deletes old version files and creates checkpoints
      val options        = new org.apache.spark.sql.util.CaseInsensitiveStringMap(mergedConfigs.asJava)
      val transactionLog = TransactionLogFactory.create(resolvedPath, sparkSession, options)

      // Invalidate cache to ensure fresh read of transaction log state
      transactionLog.invalidateCache()

      try {
        // Get current versions
        val versions = transactionLog.getVersions()
        if (versions.isEmpty) {
          return Seq(
            Row(
              resolvedPath.toString,
              "ERROR",
              0L,
              0L,
              0L,
              0L,
              "No transaction log versions found"
            )
          )
        }

        val currentVersion = versions.max
        logger.info(s"Current transaction log version: $currentVersion, total versions: ${versions.size}")

        // Check if checkpoint exists, create one if not
        val checkpointVersion = transactionLog.getLastCheckpointVersion() match {
          case Some(cpVersion) if cpVersion == currentVersion =>
            logger.info(s"Checkpoint already exists at current version $cpVersion")
            cpVersion
          case Some(cpVersion) =>
            // Checkpoint exists but not at current version - create new one
            logger.info(s"Existing checkpoint at v$cpVersion, creating new checkpoint at v$currentVersion")
            createCheckpointAtCurrentVersion(transactionLog, resolvedPath, options)
          case None =>
            // No checkpoint exists - create one
            logger.info(s"No checkpoint exists, creating checkpoint at v$currentVersion")
            createCheckpointAtCurrentVersion(transactionLog, resolvedPath, options)
        }

        // Now delete old transaction log files
        val transactionLogPath = new Path(resolvedPath, "_transaction_log")
        val cloudProvider = CloudStorageProviderFactory.createProvider(
          transactionLogPath.toString,
          options,
          sparkSession.sparkContext.hadoopConfiguration
        )

        try {
          // List all files in transaction log directory (recursive to include Avro state dir contents)
          val allFiles = cloudProvider.listFiles(transactionLogPath.toString, recursive = true)

          // Categorize files
          val versionFiles    = scala.collection.mutable.ListBuffer[(Long, String)]()
          val checkpointFiles = scala.collection.mutable.ListBuffer[(Long, String)]()
          val partFiles       = scala.collection.mutable.ListBuffer[(Long, String, String)]() // (version, uuid, path)
          val avroStateFiles  = scala.collection.mutable.ListBuffer[(Long, String)]()         // (version, file path)

          allFiles.foreach { f =>
            val filePath = new Path(f.path)
            val fileName = filePath.getName
            // Check if file is inside an Avro state directory (e.g., state-v5/_manifest)
            val parentName = Option(filePath.getParent).map(_.getName).getOrElse("")
            parentName match {
              case AvroStateDirPattern(versionStr) =>
                avroStateFiles += ((versionStr.toLong, f.path))
              case _ =>
                fileName match {
                  case VersionFilePattern(versionStr) =>
                    versionFiles += ((versionStr.toLong, f.path))
                  case ManifestPattern(versionStr) =>
                    checkpointFiles += ((versionStr.toLong, f.path))
                  case PartFilePattern(versionStr, uuid, _) =>
                    partFiles += ((versionStr.toLong, uuid, f.path))
                  case _ => // Ignore other files (like _last_checkpoint)
                }
            }
          }

          logger.info(
            s"Found ${versionFiles.size} version files, ${checkpointFiles.size} JSON checkpoints, " +
              s"${partFiles.size} checkpoint parts, ${avroStateFiles.map(_._1).toSet.size} Avro state checkpoints"
          )

          // Identify files to delete:
          // - Version files with version < checkpointVersion
          // - Checkpoint files with version < checkpointVersion
          // - Part files with version < checkpointVersion
          // - Avro state files with version < checkpointVersion
          val versionsToDelete    = versionFiles.filter(_._1 < checkpointVersion)
          val checkpointsToDelete = checkpointFiles.filter(_._1 < checkpointVersion)
          val partsToDelete       = partFiles.filter(_._1 < checkpointVersion)
          val avroStateToDelete   = avroStateFiles.filter(_._1 < checkpointVersion)

          val totalFilesToDelete =
            versionsToDelete.size + checkpointsToDelete.size + partsToDelete.size + avroStateToDelete.size

          logger.info(
            s"Files to delete: ${versionsToDelete.size} versions, ${checkpointsToDelete.size} checkpoints, " +
              s"${partsToDelete.size} parts, ${avroStateToDelete.size} Avro state files"
          )

          if (totalFilesToDelete == 0) {
            return Seq(
              Row(
                resolvedPath.toString,
                if (dryRun) "DRY_RUN" else "SUCCESS",
                checkpointVersion,
                0L,
                0L,
                transactionLog.listFiles().size.toLong,
                "No old transaction log files to delete - table already at minimal state"
              )
            )
          }

          if (dryRun) {
            // Preview mode - don't actually delete
            val totalCheckpointDeletes = checkpointsToDelete.size + partsToDelete.size + avroStateToDelete.size
            logger.info(s"DRY RUN: Would delete ${versionsToDelete.size} version files, $totalCheckpointDeletes checkpoint files/state files")

            Seq(
              Row(
                resolvedPath.toString,
                "DRY_RUN",
                checkpointVersion,
                versionsToDelete.size.toLong,
                totalCheckpointDeletes.toLong,
                transactionLog.listFiles().size.toLong,
                s"Would delete ${versionsToDelete.size} version files and $totalCheckpointDeletes checkpoint files (DRY RUN - no changes made)"
              )
            )
          } else {
            // Actually delete the files
            var deletedVersions    = 0L
            var deletedCheckpoints = 0L

            // Delete version files
            versionsToDelete.foreach {
              case (version, path) =>
                try
                  if (cloudProvider.deleteFile(path)) {
                    deletedVersions += 1
                    logger.debug(s"Deleted version file: v$version")
                  }
                catch {
                  case e: Exception =>
                    logger.warn(s"Failed to delete version file v$version: ${e.getMessage}")
                }
            }

            // Delete checkpoint manifests/legacy checkpoints
            checkpointsToDelete.foreach {
              case (version, path) =>
                try
                  if (cloudProvider.deleteFile(path)) {
                    deletedCheckpoints += 1
                    logger.debug(s"Deleted checkpoint file: v$version")
                  }
                catch {
                  case e: Exception =>
                    logger.warn(s"Failed to delete checkpoint file v$version: ${e.getMessage}")
                }
            }

            // Delete checkpoint parts
            partsToDelete.foreach {
              case (version, uuid, path) =>
                try
                  if (cloudProvider.deleteFile(path)) {
                    deletedCheckpoints += 1
                    logger.debug(s"Deleted checkpoint part: v$version (uuid=$uuid)")
                  }
                catch {
                  case e: Exception =>
                    logger.warn(s"Failed to delete checkpoint part v$version: ${e.getMessage}")
                }
            }

            // Delete Avro state files (files inside state-v{N}/ directories)
            val avroStateDirs = scala.collection.mutable.Set[String]()
            avroStateToDelete.foreach {
              case (version, path) =>
                try {
                  // Track parent directory for cleanup
                  val parentDir = new Path(path).getParent.toString
                  avroStateDirs += parentDir
                  if (cloudProvider.deleteFile(path)) {
                    deletedCheckpoints += 1
                    logger.debug(s"Deleted Avro state file: v$version")
                  }
                } catch {
                  case e: Exception =>
                    logger.warn(s"Failed to delete Avro state file v$version: ${e.getMessage}")
                }
            }
            // Clean up empty state directories
            avroStateDirs.foreach { dirPath =>
              try cloudProvider.deleteFile(dirPath)
              catch { case _: Exception => }
            }

            logger.info(
              s"Truncation complete: deleted $deletedVersions version files, $deletedCheckpoints checkpoint files"
            )

            // Invalidate snapshot cache so listFiles() reads fresh state
            // after version files were deleted from storage
            transactionLog.invalidateCache()

            Seq(
              Row(
                resolvedPath.toString,
                "SUCCESS",
                checkpointVersion,
                deletedVersions,
                deletedCheckpoints,
                transactionLog.listFiles().size.toLong,
                s"Successfully truncated time travel data. Deleted $deletedVersions version files and $deletedCheckpoints checkpoint files. Table now at v$checkpointVersion."
              )
            )
          }
        } finally
          cloudProvider.close()
      } finally
        transactionLog.close()
    } catch {
      case scala.util.control.NonFatal(e) =>
        val errorMsg = s"Failed to truncate time travel data: ${e.getMessage}"
        logger.error(errorMsg, e)
        Seq(
          Row(
            tablePath,
            "ERROR",
            0L,
            0L,
            0L,
            0L,
            errorMsg
          )
        )
    }

  /** Create a checkpoint at the current version using the existing transaction log. Returns the checkpoint version. */
  private def createCheckpointAtCurrentVersion(
    transactionLog: io.indextables.spark.transaction.TransactionLogInterface,
    resolvedPath: Path,
    options: org.apache.spark.sql.util.CaseInsensitiveStringMap
  ): Long = {
    val versions       = transactionLog.getVersions()
    val currentVersion = versions.max

    // Get all current actions (file state)
    val allFiles = transactionLog.listFiles()
    val metadata = transactionLog.getMetadata()
    val protocol = transactionLog.getProtocol()

    logger.info(s"Creating checkpoint at version $currentVersion with ${allFiles.length + 2} actions")

    val protocolJson = ActionJsonSerializer.protocolToJson(protocol)
    val metadataJson = ActionJsonSerializer.metadataToJson(metadata)
    val addsJson     = ActionJsonSerializer.addActionsToJson(allFiles)

    // Use the transaction log instance so resolved credentials (including Unity Catalog) are used.
    val checkpointInfo = transactionLog.createCheckpoint(addsJson, metadataJson, protocolJson)
    // Invalidate snapshot cache so subsequent reads see the new checkpoint
    transactionLog.invalidateCache()
    val checkpointVersion = checkpointInfo.getVersion
    logger.info(s"Created checkpoint at version $checkpointVersion")
    checkpointVersion
  }

  /** Resolve table path from string path or table identifier. */
  private def resolveTablePath(pathOrTable: String, sparkSession: SparkSession): Path =
    if (
      pathOrTable.startsWith("/") || pathOrTable.startsWith("s3://") || pathOrTable.startsWith("s3a://") ||
      pathOrTable.startsWith("hdfs://") || pathOrTable.startsWith("file://") ||
      pathOrTable.startsWith("abfss://") || pathOrTable.startsWith("wasbs://")
    ) {
      // It's a path
      new Path(pathOrTable)
    } else {
      // Try to resolve as table identifier
      try {
        val tableIdentifier = sparkSession.sessionState.sqlParser.parseTableIdentifier(pathOrTable)
        val catalog         = sparkSession.sessionState.catalog
        if (catalog.tableExists(tableIdentifier)) {
          val tableMetadata = catalog.getTableMetadata(tableIdentifier)
          new Path(tableMetadata.location)
        } else {
          throw new IllegalArgumentException(s"Table not found: $pathOrTable")
        }
      } catch {
        case _: Exception =>
          // If it fails as a table identifier, treat it as a path
          new Path(pathOrTable)
      }
    }
}
