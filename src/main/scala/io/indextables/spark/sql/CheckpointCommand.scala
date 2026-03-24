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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{BooleanType, LongType, StringType}

import org.apache.hadoop.fs.Path

import io.indextables.jni.txlog.TransactionLogWriter
import io.indextables.spark.transaction.{
  ActionJsonSerializer,
  ConfigMapper,
  TransactionLogFactory
}
import org.slf4j.LoggerFactory

/**
 * SQL command to force a checkpoint on an IndexTables table.
 *
 * Syntax:
 *   - CHECKPOINT INDEXTABLES '/path/to/table'
 *   - CHECKPOINT INDEXTABLES table_name
 *   - CHECKPOINT TANTIVY4SPARK '/path/to/table'
 *
 * This command:
 *   1. Reads the current transaction log state 2. Delegates checkpoint creation to the native
 *      tantivy4java implementation 3. Returns status information about the checkpoint
 *
 * Use this command to:
 *   - Force checkpoint creation at a specific point in time
 *   - Optimize read performance by creating a checkpoint
 */
case class CheckpointCommand(tablePath: String) extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[CheckpointCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("table_path", StringType)(),
    AttributeReference("status", StringType)(),
    AttributeReference("checkpoint_version", LongType)(),
    AttributeReference("num_actions", LongType)(),
    AttributeReference("num_files", LongType)(),
    AttributeReference("protocol_version", LongType)(),
    AttributeReference("is_multi_part", BooleanType)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] =
    try {
      // Resolve the table path
      val resolvedPath = resolveTablePath(tablePath, sparkSession)
      logger.info(s"Creating checkpoint for table: $resolvedPath")

      // Build options map from Spark configuration
      val optionsMap = new java.util.HashMap[String, String]()
      sparkSession.conf.getAll.filter(_._1.startsWith("spark.indextables.")).foreach {
        case (k, v) => optionsMap.put(k, v)
      }
      // Checkpoint writes files, so ensure PATH_READ_WRITE credentials
      optionsMap.put("spark.indextables.databricks.credential.operation", "PATH_READ_WRITE")
      val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(optionsMap)

      // Create transaction log instance to read current state
      val transactionLog = TransactionLogFactory.create(resolvedPath, sparkSession, options)

      // Invalidate cache to ensure fresh read of transaction log state
      transactionLog.invalidateCache()

      try {
        // Get current version and all actions
        val versions = transactionLog.getVersions()
        if (versions.isEmpty) {
          return Seq(
            Row(
              resolvedPath.toString,
              "ERROR: No transaction log versions found",
              0L,
              0L,
              0L,
              0L,
              false
            )
          )
        }

        val currentVersion = versions.max

        // Read all current actions (file state)
        val allFiles = transactionLog.listFiles()
        val metadata = transactionLog.getMetadata()
        val protocol = transactionLog.getProtocol()

        logger.info(
          s"Creating checkpoint at version $currentVersion with ${allFiles.length} files"
        )

        // Serialize actions to JSON for the native checkpoint API
        val entriesJson  = ActionJsonSerializer.addActionsToJson(allFiles)
        val metadataJson = ActionJsonSerializer.metadataToJson(metadata)
        val protocolJson = ActionJsonSerializer.protocolToJson(protocol)

        // Normalize table path and build native config
        val nativeTablePath = ConfigMapper.normalizeTablePath(resolvedPath)
        val nativeConfig    = ConfigMapper.toNativeConfig(options)

        // Delegate checkpoint creation to the native tantivy4java implementation
        val checkpointInfo = TransactionLogWriter.createCheckpoint(
          nativeTablePath,
          nativeConfig,
          entriesJson,
          metadataJson,
          protocolJson
        )

        val checkpointVersion = checkpointInfo.getVersion
        val numFiles          = checkpointInfo.getNumFiles
        val numActions        = checkpointInfo.getSize

        logger.info(
          s"Checkpoint created successfully at version $checkpointVersion " +
            s"(format=${checkpointInfo.getFormat}, numFiles=$numFiles)"
        )

        // Invalidate cache AFTER checkpoint creation so subsequent reads use the new checkpoint
        transactionLog.invalidateCache()

        Seq(
          Row(
            resolvedPath.toString,
            "SUCCESS",
            checkpointVersion,
            numActions,
            numFiles,
            protocol.minReaderVersion.toLong,
            false
          )
        )
      } finally
        transactionLog.close()
    } catch {
      case scala.util.control.NonFatal(e) =>
        val errorMsg = s"Failed to create checkpoint: ${e.getMessage}"
        logger.error(errorMsg, e)
        Seq(
          Row(
            tablePath,
            s"ERROR: ${e.getMessage}",
            0L,
            0L,
            0L,
            0L,
            false
          )
        )
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
