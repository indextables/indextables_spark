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
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType}

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.{LastCheckpointInfo, TransactionLogFactory}
import io.indextables.spark.transaction.avro.{StateConfig, StateManifestIO}
import org.slf4j.LoggerFactory

/**
 * SQL command to describe the state format of an IndexTables table.
 *
 * Syntax:
 *   - DESCRIBE INDEXTABLES STATE '/path/to/table'
 *   - DESCRIBE INDEXTABLES STATE table_name
 *   - DESCRIBE TANTIVY4SPARK STATE '/path/to/table'
 *
 * This command outputs:
 *   - format: Current state format (json, json-multipart, avro-state)
 *   - version: Current transaction log version
 *   - num_files: Number of active files in the table
 *   - num_manifests: Number of Avro manifests (if using avro-state)
 *   - num_tombstones: Number of tombstone entries
 *   - tombstone_ratio: Tombstones / Total entries
 *   - total_bytes: Total size of all files
 *   - compression: Compression codec used (for Avro state)
 *   - needs_compaction: Whether the state would benefit from compaction
 */
case class DescribeStateCommand(tablePath: String) extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[DescribeStateCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("table_path", StringType)(),
    AttributeReference("format", StringType)(),
    AttributeReference("version", LongType)(),
    AttributeReference("num_files", LongType)(),
    AttributeReference("num_manifests", IntegerType)(),
    AttributeReference("num_tombstones", IntegerType)(),
    AttributeReference("tombstone_ratio", DoubleType)(),
    AttributeReference("total_bytes", LongType)(),
    AttributeReference("needs_compaction", BooleanType)(),
    AttributeReference("status", StringType)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] =
    try {
      val resolvedPath = resolveTablePath(tablePath, sparkSession)
      logger.info(s"Describing state for table: $resolvedPath")

      // Build options map from Spark configuration
      val optionsMap = new java.util.HashMap[String, String]()
      sparkSession.conf.getAll.filter(_._1.startsWith("spark.indextables.")).foreach {
        case (k, v) => optionsMap.put(k, v)
      }
      val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(optionsMap)

      val transactionLogPath = new Path(resolvedPath, "_transaction_log")
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        transactionLogPath.toString,
        options,
        sparkSession.sparkContext.hadoopConfiguration
      )

      try {
        // Read _last_checkpoint to determine format
        val lastCheckpointPath = new Path(transactionLogPath, "_last_checkpoint")

        if (!cloudProvider.exists(lastCheckpointPath.toString)) {
          return Seq(
            Row(
              resolvedPath.toString,
              "none",
              0L,
              0L,
              0,
              0,
              0.0,
              0L,
              false,
              "No checkpoint found"
            )
          )
        }

        val lastCheckpointBytes = cloudProvider.readFile(lastCheckpointPath.toString)
        val lastCheckpointJson  = new String(lastCheckpointBytes, "UTF-8")
        val checkpointInfo = io.indextables.spark.util.JsonUtil.mapper.readValue(
          lastCheckpointJson,
          classOf[LastCheckpointInfo]
        )

        val format = checkpointInfo.format.getOrElse(
          if (checkpointInfo.parts.isDefined) "json-multipart" else "json"
        )

        if (format == StateConfig.Format.AVRO_STATE) {
          describeAvroState(resolvedPath.toString, transactionLogPath, cloudProvider, checkpointInfo)
        } else {
          describeJsonState(resolvedPath.toString, cloudProvider, checkpointInfo, format)
        }
      } finally
        cloudProvider.close()
    } catch {
      case e: Exception =>
        val errorMsg = s"Failed to describe state: ${e.getMessage}"
        logger.error(errorMsg, e)
        Seq(
          Row(
            tablePath,
            "error",
            0L,
            0L,
            0,
            0,
            0.0,
            0L,
            false,
            s"ERROR: ${e.getMessage}"
          )
        )
    }

  private def describeAvroState(
    resolvedPathStr: String,
    transactionLogPath: Path,
    cloudProvider: io.indextables.spark.io.CloudStorageProvider,
    checkpointInfo: LastCheckpointInfo
  ): Seq[Row] = {

    val stateDir = checkpointInfo.stateDir.getOrElse(
      throw new IllegalStateException("Avro state checkpoint missing stateDir")
    )

    val stateDirPath = new Path(transactionLogPath, stateDir).toString
    val manifestIO   = StateManifestIO(cloudProvider)
    val manifest     = manifestIO.readStateManifest(stateDirPath)

    val numManifests  = manifest.manifests.size
    val numTombstones = manifest.tombstones.size
    val totalEntries  = manifest.manifests.map(_.numEntries).sum

    val tombstoneRatio = if (totalEntries > 0) {
      numTombstones.toDouble / totalEntries
    } else {
      0.0
    }

    val needsCompaction =
      tombstoneRatio > StateConfig.COMPACTION_TOMBSTONE_THRESHOLD_DEFAULT ||
        numManifests > StateConfig.COMPACTION_MAX_MANIFESTS_DEFAULT

    Seq(
      Row(
        resolvedPathStr,
        "avro-state",
        manifest.stateVersion,
        manifest.numFiles.toLong,
        numManifests,
        numTombstones,
        tombstoneRatio,
        manifest.totalBytes,
        needsCompaction,
        "OK"
      )
    )
  }

  private def describeJsonState(
    resolvedPathStr: String,
    cloudProvider: io.indextables.spark.io.CloudStorageProvider,
    checkpointInfo: LastCheckpointInfo,
    format: String
  ): Seq[Row] =
    Seq(
      Row(
        resolvedPathStr,
        format,
        checkpointInfo.version,
        checkpointInfo.numFiles.toLong,
        if (checkpointInfo.parts.isDefined) checkpointInfo.parts.get else 1,
        0, // JSON format doesn't have tombstones
        0.0,
        checkpointInfo.sizeInBytes,
        false, // JSON format doesn't need compaction in the same sense
        "OK"
      )
    )

  private def resolveTablePath(pathOrTable: String, sparkSession: SparkSession): Path =
    if (
      pathOrTable.startsWith("/") || pathOrTable.startsWith("s3://") || pathOrTable.startsWith("s3a://") ||
      pathOrTable.startsWith("hdfs://") || pathOrTable.startsWith("file://") ||
      pathOrTable.startsWith("abfss://") || pathOrTable.startsWith("wasbs://")
    ) {
      new Path(pathOrTable)
    } else {
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
          new Path(pathOrTable)
      }
    }
}
