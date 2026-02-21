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

import java.io.{FileNotFoundException, IOException}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.TransactionLogFactory
import io.indextables.spark.util.{JsonUtil, StatisticsTruncation}
import org.slf4j.LoggerFactory

/**
 * SQL command to repair corrupted or problematic transaction logs by reading the existing log, validating split files,
 * and writing a clean transaction log with metadata and checkpoint to a new location.
 *
 * This is a read-only repair operation that never modifies or deletes source files.
 *
 * Syntax: REPAIR INDEXFILES TRANSACTION LOG 's3://bucket/table/_transaction_log' AT LOCATION
 * 's3://bucket/table/_transaction_log_repaired'
 *
 * Use Cases:
 *   1. Corrupted checkpoint recovery: Rebuild checkpoint from valid transaction files 2. Orphaned file cleanup: Create
 *      clean log excluding splits that no longer exist 3. Transaction log optimization: Consolidate fragmented
 *      transaction history 4. Migration scenarios: Prepare clean transaction log for table relocation 5. Audit and
 *      validation: Generate validated transaction log with integrity checks
 *
 * Statistics Truncation Integration: The repair process automatically applies statistics truncation (if enabled) to
 * optimize transaction log size. This can reduce transaction log size by up to 98% for tables with long text fields.
 * See DATA_SKIPPING_STATS_TRUNCATION_DESIGN.md for details.
 */
case class RepairIndexFilesTransactionLogCommand(
  sourcePath: String,
  targetPath: String)
    extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[RepairIndexFilesTransactionLogCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("source_path", StringType)(),
    AttributeReference("target_path", StringType)(),
    AttributeReference("source_version", LongType)(),
    AttributeReference("total_splits", IntegerType)(),
    AttributeReference("valid_splits", IntegerType)(),
    AttributeReference("missing_splits", IntegerType)(),
    AttributeReference("status", StringType)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info("===== STARTING REPAIR COMMAND =====")
    logger.info(s"Repair source: $sourcePath")
    logger.info(s"Repair target: $targetPath")

    try {
      // Phase 1: Read & Validate Source Transaction Log
      logger.info("Phase 1 - Reading source transaction log")
      val hadoopConf    = sparkSession.sessionState.newHadoopConf()
      val sourceLogPath = new Path(sourcePath)
      val targetLogPath = new Path(targetPath)

      // Get configuration for statistics truncation
      val config = sparkSession.conf.getAll

      // Validate source exists
      if (!sourceLogPath.getFileSystem(hadoopConf).exists(sourceLogPath)) {
        val errorMsg = s"Source transaction log not found: $sourcePath"
        logger.error(errorMsg)
        return Seq(Row(sourcePath, targetPath, -1L, 0, 0, 0, s"ERROR: $errorMsg"))
      }

      // Validate target doesn't exist or is empty
      val targetFs = targetLogPath.getFileSystem(hadoopConf)
      if (targetFs.exists(targetLogPath)) {
        val targetStatus = targetFs.listStatus(targetLogPath)
        if (targetStatus.nonEmpty) {
          val errorMsg = s"Target location already exists and is not empty: $targetPath"
          logger.error(errorMsg)
          throw new IllegalArgumentException(errorMsg)
        }
      }

      // Create transaction log instance for source
      logger.info(s"Reading source transaction log from: $sourcePath")
      val sourceOptions = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
        Map(
          "spark.indextables.transaction.allowDirectUsage" -> "true",
          "spark.indextables.databricks.credential.operation" -> "PATH_READ_WRITE"
        ).asJava
      )

      // Extract table path from transaction log path (remove /_transaction_log suffix)
      val tablePathStr = if (sourcePath.endsWith("/_transaction_log")) {
        sourcePath.stripSuffix("/_transaction_log")
      } else if (sourcePath.endsWith("_transaction_log")) {
        sourcePath.stripSuffix("_transaction_log").stripSuffix("/")
      } else {
        throw new IllegalArgumentException(s"Source path must end with _transaction_log: $sourcePath")
      }

      val tablePath = new Path(tablePathStr)
      val transactionLog = TransactionLogFactory.create(
        tablePath,
        sparkSession,
        sourceOptions
      )

      try {
        // Read current version and snapshot
        val currentVersion = transactionLog.getVersions().max
        logger.info(s"Source transaction log current version: $currentVersion")

        // Get all visible splits from snapshot
        val visibleSplits = transactionLog.listFiles()
        logger.info(s"Found ${visibleSplits.length} visible splits in source transaction log")

        // Get protocol and metadata actions
        val protocolAction = transactionLog.getProtocol()
        val metadataAction = transactionLog.getMetadata()
        logger.info(s"Retrieved protocol action: minReader=${protocolAction.minReaderVersion}, minWriter=${protocolAction.minWriterVersion}")
        logger.info(s"Retrieved metadata action: ${metadataAction.id}")
        logger.debug(s"Metadata partition columns: ${metadataAction.partitionColumns}")
        logger.debug(s"Metadata schema: ${metadataAction.schemaString}")

        // Phase 2: Validate split files exist in storage
        logger.info("Validating split file existence...")
        val cloudProvider = CloudStorageProviderFactory.createProvider(
          tablePathStr,
          sourceOptions,
          hadoopConf
        )

        try {
          val validatedSplits = visibleSplits.filter { split =>
            val splitPath = if (split.path.startsWith("/")) {
              // Absolute path
              split.path
            } else {
              // Relative path - combine with table path
              new Path(tablePath, split.path).toString
            }

            val exists = cloudProvider.exists(splitPath)
            if (!exists) {
              logger.warn(s"Split file not found (will be excluded): $splitPath")
            }
            exists
          }

          val totalSplits   = visibleSplits.length
          val validSplits   = validatedSplits.length
          val missingSplits = totalSplits - validSplits

          logger.info(s"Validation complete: total=$totalSplits, valid=$validSplits, missing=$missingSplits")

          // Phase 3: Build Clean Transaction Log with Statistics Truncation
          logger.info(s"Creating target transaction log at: $targetPath")
          logger.debug(s"Target log path object: $targetLogPath")
          val mkdirsSuccess = targetFs.mkdirs(targetLogPath)
          logger.debug(s"mkdirs result: $mkdirsSuccess")

          // Create cloud provider for target
          val targetTablePathStr = if (targetPath.endsWith("/_transaction_log")) {
            targetPath.stripSuffix("/_transaction_log")
          } else if (targetPath.endsWith("_transaction_log")) {
            targetPath.stripSuffix("_transaction_log").stripSuffix("/")
          } else {
            throw new IllegalArgumentException(s"Target path must end with _transaction_log: $targetPath")
          }

          val targetCloudProvider = CloudStorageProviderFactory.createProvider(
            targetTablePathStr,
            sourceOptions,
            hadoopConf
          )

          try {
            // Write protocol + metadata transaction (version 00000000000000000000.json - 20 digits)
            // Both actions must be in the first transaction file as newline-delimited JSON
            val version0Path    = new Path(targetLogPath, "00000000000000000000.json")
            val wrappedProtocol = Map("protocol" -> protocolAction)
            val wrappedMetadata = Map("metaData" -> metadataAction)
            val protocolJson    = JsonUtil.mapper.writeValueAsString(wrappedProtocol)
            val metadataJson    = JsonUtil.mapper.writeValueAsString(wrappedMetadata)
            val version0Content = protocolJson + "\n" + metadataJson + "\n"
            logger.info(s"Writing protocol + metadata to: $version0Path")
            logger.debug(s"Protocol + Metadata JSON length: ${version0Content.length} bytes")
            targetCloudProvider.writeFile(version0Path.toString, version0Content.getBytes("UTF-8"))
            logger.info("Successfully wrote protocol + metadata file")

            // Write consolidated add actions with truncated statistics (version 00000000000000000001.json - 20 digits)
            logger.info("Applying statistics truncation to add actions...")
            val partitionColumnSet = metadataAction.partitionColumns.toSet
            val addActionsWithTruncatedStats = validatedSplits.map { split =>
              // Apply statistics truncation, but preserve partition column statistics
              val originalMinValues = split.minValues.getOrElse(Map.empty[String, String])
              val originalMaxValues = split.maxValues.getOrElse(Map.empty[String, String])

              // Separate partition column stats from data column stats
              val (partitionMinValues, dataMinValues) = originalMinValues.partition {
                case (col, _) =>
                  partitionColumnSet.contains(col)
              }
              val (partitionMaxValues, dataMaxValues) = originalMaxValues.partition {
                case (col, _) =>
                  partitionColumnSet.contains(col)
              }

              // Only truncate data column statistics, not partition column statistics
              val (truncatedDataMinValues, truncatedDataMaxValues) =
                StatisticsTruncation.truncateStatistics(dataMinValues, dataMaxValues, config)

              // Merge partition stats back (they are never truncated)
              val finalMinValues = partitionMinValues ++ truncatedDataMinValues
              val finalMaxValues = partitionMaxValues ++ truncatedDataMaxValues

              // Copy the original AddAction, only updating statistics
              // This preserves all metadata including footer offsets
              split.copy(
                dataChange = true,
                minValues = if (finalMinValues.nonEmpty) Some(finalMinValues) else None,
                maxValues = if (finalMaxValues.nonEmpty) Some(finalMaxValues) else None
              )
            }

            // Write add actions using streaming to avoid OOM for large tables
            val version1Path = new Path(targetLogPath, "00000000000000000001.json")

            // Debug: Log partition values for partitioned tables
            addActionsWithTruncatedStats.foreach { action =>
              if (action.partitionValues.nonEmpty) {
                logger.debug(s"AddAction for ${action.path} has partitionValues: ${action.partitionValues}")
              }
            }

            logger.info(s"Writing $validSplits add actions to: $version1Path")

            // Use streaming write to avoid OOM for large tables with many splits
            // This prevents StringBuilder exceeding JVM's ~2GB array size limit
            io.indextables.spark.transaction.StreamingActionWriter.writeActionsStreaming(
              actions = addActionsWithTruncatedStats.asInstanceOf[Seq[io.indextables.spark.transaction.Action]],
              cloudProvider = targetCloudProvider,
              path = version1Path.toString,
              codec = None, // Repair command doesn't compress
              ifNotExists = false
            )
            logger.info("Successfully wrote add actions file")

            // No checkpoint needed - the transaction log is already consolidated
            // with just 2 files (protocol+metadata in v0, adds in v1), which is optimal
            logger.info("Skipping checkpoint creation - transaction log already consolidated")
            logger.info("Repair operation completed successfully")

            // Phase 5: Return Results
            Seq(
              Row(
                sourcePath,
                targetPath,
                currentVersion,
                totalSplits,
                validSplits,
                missingSplits,
                "SUCCESS"
              )
            )
          } finally
            targetCloudProvider.close()
        } finally
          cloudProvider.close()
      } finally
        transactionLog.close()
    } catch {
      case e: FileNotFoundException =>
        val errorMsg = s"Source transaction log not found: ${e.getMessage}"
        logger.error(errorMsg, e)
        Seq(Row(sourcePath, targetPath, -1L, 0, 0, 0, s"ERROR: $errorMsg"))

      case e: IOException =>
        val errorMsg = s"I/O error during repair: ${e.getMessage}"
        logger.error(errorMsg, e)
        Seq(Row(sourcePath, targetPath, -1L, 0, 0, 0, s"ERROR: $errorMsg"))

      case e: IllegalArgumentException =>
        // For validation errors like "target already exists", we want to throw
        // the exception so tests can catch it with intercept[Exception]
        val errorMsg = e.getMessage
        logger.error(s"Validation error: $errorMsg", e)
        throw e

      case e: Exception =>
        val errorMsg = s"Unexpected error during repair: ${e.getMessage}"
        logger.error(errorMsg, e)
        Seq(Row(sourcePath, targetPath, -1L, 0, 0, 0, s"ERROR: $errorMsg"))
    }
  }
}
