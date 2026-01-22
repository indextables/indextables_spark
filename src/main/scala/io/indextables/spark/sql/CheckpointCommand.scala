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

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.{AddAction, LastCheckpointInfo, TransactionLogCheckpoint, TransactionLogFactory}
import io.indextables.spark.transaction.avro.{ConcurrentStateWriteException, FileEntry, StateConfig, StateManifestIO, StateRetryConfig, StateWriter}
import io.indextables.spark.util.JsonUtil
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
 *   1. Reads the current transaction log state 2. Creates a checkpoint at the current version 3. Upgrades the table to
 *      V3 protocol (always) 4. Returns status information about the checkpoint
 *
 * Use this command to:
 *   - Force V3 protocol upgrade on existing tables
 *   - Create a checkpoint at a specific point in time
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

        // Build complete action list for checkpoint
        val allActions = Seq(protocol) ++ Seq(metadata) ++ allFiles

        logger.info(
          s"Creating checkpoint at version $currentVersion with ${allActions.length} actions (${allFiles.length} files)"
        )

        // Create checkpoint handler directly (TransactionLogCheckpoint handles V3 upgrade)
        val transactionLogPath = new Path(resolvedPath, "_transaction_log")
        val cloudProvider = CloudStorageProviderFactory.createProvider(
          transactionLogPath.toString,
          options,
          sparkSession.sparkContext.hadoopConfiguration
        )

        try {
          // Check if Avro state format is enabled
          val stateFormat = Option(options.get(StateConfig.FORMAT_KEY))
            .getOrElse(StateConfig.FORMAT_DEFAULT)

          if (stateFormat.equalsIgnoreCase("avro")) {
            // Write checkpoint in Avro state format
            createAvroStateCheckpoint(
              transactionLogPath,
              cloudProvider,
              currentVersion,
              allFiles,
              options,
              resolvedPath.toString
            )
          } else {
            // Use existing JSON checkpoint format
            val checkpoint = new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options)
            checkpoint.createCheckpoint(currentVersion, allActions)

            // Get updated checkpoint info
            val checkpointInfo = checkpoint.getLastCheckpointInfo()
            val isMultiPart    = checkpointInfo.exists(_.parts.isDefined)
            val numActions     = checkpointInfo.map(_.size).getOrElse(allActions.length.toLong)

            // Re-read protocol from the newly created checkpoint to get the upgraded version
            val newProtocolVersion = checkpointInfo
              .map(_ => 3L) // V3 upgrade happens in createCheckpoint
              .getOrElse(protocol.minReaderVersion.toLong)

            logger.info(s"Checkpoint created successfully at version $currentVersion with protocol V$newProtocolVersion")

            Seq(
              Row(
                resolvedPath.toString,
                "SUCCESS",
                currentVersion,
                numActions,
                allFiles.length.toLong,
                newProtocolVersion,
                isMultiPart
              )
            )
          }
        } finally
          cloudProvider.close()
      } finally
        transactionLog.close()
    } catch {
      case e: Exception =>
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

  /**
   * Create checkpoint in Avro state format with retry support for concurrent write conflicts.
   *
   * This writes a compacted state with all live files sorted by partition values for optimal pruning.
   * Uses conditional writes and retry logic to handle concurrent checkpoint creation.
   */
  private def createAvroStateCheckpoint(
      transactionLogPath: Path,
      cloudProvider: io.indextables.spark.io.CloudStorageProvider,
      currentVersion: Long,
      allFiles: Seq[AddAction],
      options: org.apache.spark.sql.util.CaseInsensitiveStringMap,
      resolvedPathStr: String): Seq[Row] = {

    val compression = Option(options.get(StateConfig.COMPRESSION_KEY))
      .getOrElse(StateConfig.COMPRESSION_DEFAULT)
    val compressionLevel = Option(options.get(StateConfig.COMPRESSION_LEVEL_KEY))
      .map(_.toInt)
      .getOrElse(StateConfig.COMPRESSION_LEVEL_DEFAULT)
    val entriesPerManifest = Option(options.get(StateConfig.ENTRIES_PER_MANIFEST_KEY))
      .map(_.toInt)
      .getOrElse(StateConfig.ENTRIES_PER_MANIFEST_DEFAULT)

    // Build retry configuration from options
    val retryConfig = StateRetryConfig(
      maxAttempts = Option(options.get(StateConfig.RETRY_MAX_ATTEMPTS_KEY))
        .map(_.toInt)
        .getOrElse(StateConfig.RETRY_MAX_ATTEMPTS_DEFAULT),
      baseDelayMs = Option(options.get(StateConfig.RETRY_BASE_DELAY_MS_KEY))
        .map(_.toLong)
        .getOrElse(StateConfig.RETRY_BASE_DELAY_MS_DEFAULT),
      maxDelayMs = Option(options.get(StateConfig.RETRY_MAX_DELAY_MS_KEY))
        .map(_.toLong)
        .getOrElse(StateConfig.RETRY_MAX_DELAY_MS_DEFAULT)
    )

    val stateWriter = StateWriter(
      cloudProvider,
      transactionLogPath.toString,
      compression,
      compressionLevel,
      entriesPerManifest,
      retryConfig
    )

    val manifestIO = StateManifestIO(cloudProvider)

    // Build schema registry from AddActions with docMappingJson
    // This deduplicates schema storage and allows restoration when reading
    import java.security.MessageDigest
    val schemaRegistry = scala.collection.mutable.Map[String, String]()

    def hashSchema(json: String): String = {
      val md = MessageDigest.getInstance("SHA-256")
      val digest = md.digest(json.getBytes("UTF-8"))
      digest.take(8).map("%02x".format(_)).mkString
    }

    // Convert AddActions to FileEntries with current version and timestamp
    // Also build schema registry and set docMappingRef
    val timestamp = System.currentTimeMillis()
    val fileEntries = allFiles.map { add =>
      val refAndJson = add.docMappingJson.map { json =>
        val ref = add.docMappingRef.getOrElse(hashSchema(json))
        schemaRegistry.put(ref, json)
        ref
      }
      FileEntry.fromAddAction(add.copy(docMappingRef = refAndJson.orElse(add.docMappingRef)), currentVersion, timestamp)
    }

    logger.info(s"Creating Avro state checkpoint at version $currentVersion with ${fileEntries.size} files, schemaRegistry size=${schemaRegistry.size}")

    // Write compacted state with retry support for concurrent conflicts
    // This uses conditional writes for _manifest.json and automatic version increment on conflict
    val writeResult = stateWriter.writeStateWithRetry(
      currentVersion,
      fileEntries,
      schemaRegistry.toMap
    )

    // Extract the actual version and state directory from the write result
    val actualVersion = writeResult.version
    val stateDir = manifestIO.formatStateDir(actualVersion)

    if (writeResult.conflictDetected) {
      logger.info(s"Checkpoint created at version $actualVersion after ${writeResult.attempts} attempts " +
        s"(original version $currentVersion had concurrent conflict)")
    }

    // Update _last_checkpoint to point to new Avro state
    // Use version-checked write to avoid regressing to an older checkpoint
    val lastCheckpointInfo = LastCheckpointInfo(
      version = actualVersion,
      size = fileEntries.size,
      sizeInBytes = fileEntries.map(_.size).sum,
      numFiles = fileEntries.size,
      createdTime = timestamp,
      parts = None,
      checkpointId = None,
      format = Some(StateConfig.Format.AVRO_STATE),
      stateDir = Some(stateDir)
    )

    val lastCheckpointJson = JsonUtil.mapper.writeValueAsString(lastCheckpointInfo)
    val checkpointUpdated = manifestIO.writeLastCheckpointIfNewer(
      transactionLogPath.toString,
      actualVersion,
      lastCheckpointJson
    )

    if (checkpointUpdated) {
      logger.info(s"Avro state checkpoint created successfully at version $actualVersion (format=avro-state)")
    } else {
      logger.info(s"Avro state written at version $actualVersion, but _last_checkpoint not updated " +
        s"(a newer checkpoint already exists)")
    }

    Seq(
      Row(
        resolvedPathStr,
        "SUCCESS",
        actualVersion,
        fileEntries.size.toLong,
        fileEntries.size.toLong,
        4L, // Protocol version 4 for Avro state
        false // Not multi-part in the JSON sense
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
