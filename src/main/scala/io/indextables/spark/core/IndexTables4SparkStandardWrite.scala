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

package io.indextables.spark.core

import org.apache.spark.sql.connector.write.{
  BatchWrite,
  DataWriterFactory,
  PhysicalWriteInfo,
  Write,
  WriterCommitMessage
}
import org.apache.spark.sql.connector.write.LogicalWriteInfo

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.{AddAction, TransactionLog}
import org.slf4j.LoggerFactory

/**
 * Standard write implementation for IndexTables4Spark tables. This implementation does NOT include
 * RequiresDistributionAndOrdering, so it uses Spark's default partitioning without any optimization. Used when
 * optimizeWrite is disabled.
 */
class IndexTables4SparkStandardWrite(
  @transient transactionLog: TransactionLog,
  tablePath: Path,
  @transient writeInfo: LogicalWriteInfo,
  serializedOptions: Map[String, String], // Use serializable Map instead of CaseInsensitiveStringMap
  @transient hadoopConf: org.apache.hadoop.conf.Configuration,
  isOverwrite: Boolean = false // Track whether this is an overwrite operation
) extends Write
    with BatchWrite
    with Serializable {

  @transient private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkStandardWrite])

  // Extract serializable values from transient fields during construction
  private val writeSchema = writeInfo.schema()
  private val serializedHadoopConf = {
    // Serialize only the tantivy4spark config properties from hadoopConf
    val props = scala.collection.mutable.Map[String, String]()
    val iter  = hadoopConf.iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      if (entry.getKey.startsWith("spark.indextables.") || entry.getKey.startsWith("spark.indextables.")) {
        val normalizedKey = if (entry.getKey.startsWith("spark.indextables.")) {
          entry.getKey.replace("spark.indextables.", "spark.indextables.")
        } else entry.getKey
        props.put(normalizedKey, entry.getValue)
      }
    }
    props.toMap
  }
  private val partitionColumns =
    // Extract partition columns from write options (set by .partitionBy())
    // Spark sets this as a JSON array string like ["col1","col2"]
    serializedOptions.get("__partition_columns") match {
      case Some(partitionColumnsJson) =>
        try {
          // Parse JSON array to extract column names
          import com.fasterxml.jackson.module.scala.DefaultScalaModule
          import com.fasterxml.jackson.databind.ObjectMapper

          val mapper = new ObjectMapper()
          mapper.registerModule(DefaultScalaModule)
          val partitionCols = mapper.readValue(partitionColumnsJson, classOf[Array[String]]).toSeq

          logger.debug(s"PARTITION DEBUG: Extracted partition columns from options: $partitionCols")
          logger.debug(s"PARTITION DEBUG: Extracted partition columns from options: $partitionCols")
          partitionCols
        } catch {
          case e: Exception =>
            logger.debug(s"PARTITION DEBUG: Failed to parse partition columns JSON: $partitionColumnsJson, error: ${e.getMessage}")
            logger.warn(s"Failed to parse partition columns from options: $partitionColumnsJson", e)
            Seq.empty
        }
      case None =>
        // Fallback: try to read from existing transaction log
        try {
          val cols = transactionLog.getPartitionColumns()
          logger.debug(s"PARTITION DEBUG: Fallback - read partition columns from transaction log: $cols")
          logger.debug(s"PARTITION DEBUG: Fallback - read partition columns from transaction log: $cols")
          cols
        } catch {
          case ex: Exception =>
            logger.debug(s"PARTITION DEBUG: No partition columns found")
            logger.warn(s"Could not retrieve partition columns during construction: ${ex.getMessage}")
            Seq.empty[String]
        }
    }

  override def toBatch: BatchWrite = this

  // Store staging uploader for use in commit()
  @transient private var stagingUploader: Option[io.indextables.spark.merge.SplitStagingUploader] = None

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    logger.info(s"Creating standard batch writer factory for ${info.numPartitions} partitions")
    logger.info(s"Using Spark's default partitioning (no optimization)")

    if (partitionColumns.nonEmpty) {
      logger.info(s"Table is partitioned by: ${partitionColumns.mkString(", ")}")
    }

    // Combine serialized hadoop config with tantivy4spark options (normalize spark.indextables to spark.tantivy4spark)
    val normalizedTantivyOptions = serializedOptions
      .filter(kv => kv._1.startsWith("spark.indextables.") || kv._1.startsWith("spark.indextables."))
      .map {
        case (key, value) =>
          val normalizedKey = if (key.startsWith("spark.indextables.")) {
            key.replace("spark.indextables.", "spark.indextables.")
          } else key
          normalizedKey -> value
      }
    val combinedHadoopConfig = serializedHadoopConf ++ normalizedTantivyOptions

    serializedOptions.foreach {
      case (key, value) =>
        if (key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")) {
          val normalizedKey = if (key.startsWith("spark.indextables.")) {
            key.replace("spark.indextables.", "spark.indextables.")
          } else key
          logger.info(
            s"Will copy DataFrame option to Hadoop config: $normalizedKey = ${if (key.toLowerCase.contains("secret") || key.toLowerCase.contains("token") || key.toLowerCase.contains("password")) "***"
              else value}"
          )
        }
    }

    // Check if merge-on-write is enabled
    import io.indextables.spark.merge._
    import org.apache.spark.sql.util.CaseInsensitiveStringMap
    import scala.jdk.CollectionConverters._

    val options = new CaseInsensitiveStringMap(serializedOptions.asJava)
    val mergeOnWriteEnabled = MergeOnWriteHelper.isMergeOnWriteEnabled(options)

    // Create merge-on-write config if enabled
    val mergeOnWriteConfigOpt = if (mergeOnWriteEnabled) {
      logger.info("🔀 Merge-on-write is ENABLED - creating serializable config")
      val basePath = MergeOnWriteHelper.getStagingBasePath(tablePath.toString, options)

      // Create serializable config instead of non-serializable uploader
      val config = io.indextables.spark.merge.MergeOnWriteConfig(
        enabled = true,
        stagingBasePath = basePath,
        serializedHadoopConfig = combinedHadoopConfig,
        serializedOptions = serializedOptions
      )

      // Create uploader on driver for use in commit() only
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tablePath.toString, options, hadoopConf
      )
      val numThreads = options.getOrDefault("spark.indextables.mergeOnWrite.stagingThreads", "4").toInt
      val maxRetries = options.getOrDefault("spark.indextables.mergeOnWrite.stagingRetries", "3").toInt
      val uploader = new SplitStagingUploader(
        cloudProvider,
        basePath,
        numThreads,
        maxRetries
      )
      stagingUploader = Some(uploader)

      Some(config)
    } else {
      logger.info("Traditional write mode (merge-on-write disabled)")
      None
    }

    new IndexTables4SparkWriterFactory(
      tablePath,
      writeSchema,
      serializedOptions,
      combinedHadoopConfig,
      partitionColumns,
      mergeOnWriteConfigOpt
    )
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logger.debug(s"DEBUG: Committing ${messages.length} writer messages (overwrite mode: $isOverwrite)")
    logger.debug(s"DEBUG: Committing ${messages.length} writer messages (overwrite mode: $isOverwrite)")
    logger.debug(s"DEBUG: serializedOptions keys: ${serializedOptions.keys.mkString(", ")}")
    serializedOptions.foreach {
      case (k, v) =>
        val redactedValue =
          if (
            k.toLowerCase.contains("secret") || k.toLowerCase
              .contains("key") || k.toLowerCase.contains("password") || k.toLowerCase.contains("token")
          ) {
            "***REDACTED***"
          } else {
            v
          }
        logger.debug(s"DEBUG: serializedOption $k = $redactedValue")
    }

    // Validate indexing configuration for append operations
    if (!isOverwrite) {
      validateIndexingConfigurationForAppend()
    }

    // Extract both AddActions (traditional) and StagedSplitInfo (merge-on-write)
    import io.indextables.spark.merge._

    val addActions: Seq[AddAction] = messages.flatMap {
      case msg: IndexTables4SparkMergeOnWriteCommitMessage => msg.addActions
      case msg: IndexTables4SparkCommitMessage => msg.addActions
      case _                                   => Seq.empty[AddAction]
    }

    // IN-MEMORY SHUFFLE: Extract ShuffledSplitData (with in-memory bytes) from commit messages
    val shuffledSplits: Seq[ShuffledSplitData] = messages.flatMap {
      case msg: IndexTables4SparkMergeOnWriteCommitMessage => msg.shuffledSplits
      case _                                                => Seq.empty[ShuffledSplitData]
    }

    // Legacy support for file-based stagedSplits (deprecated)
    val stagedSplits: Seq[StagedSplitInfo] = messages.flatMap {
      case msg: IndexTables4SparkMergeOnWriteCommitMessage => msg.stagedSplits
      case _                                                => Seq.empty[StagedSplitInfo]
    }

    // Check if merge-on-write is being used (prefer shuffledSplits, fall back to stagedSplits)
    val mergeOnWriteActive = shuffledSplits.nonEmpty || stagedSplits.nonEmpty

    if (mergeOnWriteActive) {
      if (shuffledSplits.nonEmpty) {
        logger.info(s"🔀 Merge-on-write ACTIVE (in-memory shuffle): ${shuffledSplits.size} splits with in-memory bytes, ${addActions.size} direct AddActions")
      } else {
        logger.info(s"🔀 Merge-on-write ACTIVE (legacy): ${stagedSplits.size} staged splits, ${addActions.size} direct AddActions")
      }
    }

    // Log how many empty partitions were filtered out
    val totalItems = addActions.size + shuffledSplits.size + stagedSplits.size
    val emptyPartitionsCount = messages.length - totalItems
    if (emptyPartitionsCount > 0) {
      println(s"⚠️  Filtered out $emptyPartitionsCount empty partitions (0 records) from transaction log")
      logger.info(s"⚠️  Filtered out $emptyPartitionsCount empty partitions (0 records) from transaction log")
    }

    // Determine if this should be an overwrite based on existing table state and mode
    logger.debug(s"SAVEMODE DEBUG: isOverwrite flag = $isOverwrite")
    logger.debug(s"SAVEMODE DEBUG: serializedOptions.get(saveMode) = ${serializedOptions.get("saveMode")}")

    val shouldOverwrite = if (isOverwrite) {
      // Explicit overwrite flag from truncate() or overwrite() call
      logger.debug(s"SAVEMODE DEBUG: Using isOverwrite=true from truncate/overwrite call")
      true
    } else {
      // For DataSource V2, SaveMode.Overwrite might not trigger truncate()/overwrite() methods
      // Instead, we need to detect overwrite by checking the logical write info or options
      val saveMode = serializedOptions.get("saveMode") match {
        case Some("Overwrite")     =>
          logger.debug("SAVEMODE DEBUG: saveMode=Overwrite in options, returning true")
          true
        case Some("ErrorIfExists") =>
          logger.debug("SAVEMODE DEBUG: saveMode=ErrorIfExists in options, returning false")
          false
        case Some("Ignore")        =>
          logger.debug("SAVEMODE DEBUG: saveMode=Ignore in options, returning false")
          false
        case Some("Append")        =>
          logger.debug("SAVEMODE DEBUG: saveMode=Append in options, returning false")
          false
        case None                  =>
          // Check if this looks like an initial write (no existing files) - treat as overwrite
          logger.debug("SAVEMODE DEBUG: No saveMode in options, checking existing files")
          try {
            val existingFiles = transactionLog.listFiles()
            if (existingFiles.isEmpty) {
              logger.debug("SAVEMODE DEBUG: No existing files, treating as initial write (append)")
              false // Initial write doesn't need overwrite semantics, just add files
            } else {
              logger.debug(s"SAVEMODE DEBUG: Found ${existingFiles.length} existing files, defaulting to append")
              // Without explicit mode info, default to append to be safe
              false
            }
          } catch {
            case e: Exception =>
              logger.debug(s"SAVEMODE DEBUG: Exception reading transaction log: ${e.getMessage}, assuming append")
              false // If we can't read transaction log, assume append
          }
        case Some(other) =>
          logger.debug(s"SAVEMODE DEBUG: Unknown saveMode: $other, defaulting to append")
          false
      }
      logger.debug(s"SAVEMODE DEBUG: Final saveMode decision = $saveMode")
      saveMode
    }

    logger.debug(s"SAVEMODE DEBUG: shouldOverwrite = $shouldOverwrite")

    // Initialize transaction log with schema if this is the first commit
    transactionLog.initialize(writeSchema, partitionColumns)

    // Convert serializedOptions Map to CaseInsensitiveStringMap
    import scala.jdk.CollectionConverters._
    val optionsMap = new java.util.HashMap[String, String]()
    serializedOptions.foreach { case (k, v) => optionsMap.put(k, v) }
    val writeOptions = new org.apache.spark.sql.util.CaseInsensitiveStringMap(optionsMap)

    // Set thread-local write options so TransactionLog can access compression settings
    TransactionLog.setWriteOptions(writeOptions)

    try {
      // Determine final AddActions (either direct or from merge-on-write)
      val finalAddActions = if (mergeOnWriteActive) {
        if (shuffledSplits.nonEmpty) {
          logger.debug(s"MERGE DEBUG: Starting in-memory shuffle merge-on-write with ${shuffledSplits.size} shuffled splits (in-memory), ${addActions.size} direct addActions")
          logger.info("🔀 Executing in-memory shuffle-based merge-on-write...")
        } else {
          logger.debug(s"MERGE DEBUG: Starting legacy merge-on-write with ${stagedSplits.size} staged splits, ${addActions.size} direct addActions")
          logger.info("🔀 Executing legacy shuffle-based merge-on-write orchestration...")
        }

        // Create orchestrator (using shuffle-based approach with in-memory data)
        val orchestrator = new io.indextables.spark.merge.ShuffleBasedMergeOrchestrator(
          tablePath = tablePath.toString,
          schema = writeSchema,
          options = writeOptions,
          hadoopConf = hadoopConf
        )

        // IN-MEMORY SHUFFLE: Use shuffledSplits (with in-memory bytes) if available, fall back to stagedSplits
        val splitsToMerge = if (shuffledSplits.nonEmpty) shuffledSplits else {
          // Legacy: Convert StagedSplitInfo to ShuffledSplitData by reading files
          logger.warn("Using legacy file-based merge (reading split files) - consider upgrading to in-memory shuffle")
          throw new UnsupportedOperationException(
            "Legacy file-based merge is no longer supported. " +
            "This indicates an internal error - split bytes should be in memory."
          )
        }

        // Execute merge-on-write
        val mergedActions = orchestrator.executeMergeOnWrite(
          splitsToMerge,
          if (shouldOverwrite) org.apache.spark.sql.SaveMode.Overwrite else org.apache.spark.sql.SaveMode.Append
        )

        logger.debug(s"MERGE DEBUG: Orchestrator returned ${mergedActions.size} merged actions")

        // Shutdown staging uploader if present
        stagingUploader.foreach(_.shutdown())

        // Combine with any direct AddActions
        val combined = addActions ++ mergedActions
        logger.debug(s"MERGE DEBUG: Combined total: ${combined.size} AddActions (${addActions.size} direct + ${mergedActions.size} merged)")
        combined
      } else {
        // Traditional mode - use AddActions directly
        addActions
      }

      // Commit the changes
      if (shouldOverwrite) {
        logger.debug(s"COMMIT DEBUG: Performing OVERWRITE with ${finalAddActions.length} new files")
        val version = transactionLog.overwriteFiles(finalAddActions)
        logger.debug(s"COMMIT DEBUG: Overwrite completed in transaction version $version")

        // Log what's in the transaction log after this operation
        val filesAfter = transactionLog.listFiles()
        logger.debug(s"COMMIT DEBUG: After OVERWRITE, transaction log contains ${filesAfter.length} files:")
        filesAfter.foreach { action =>
          logger.debug(s"  - ${action.path}: ${action.numRecords.getOrElse(0)} records")
        }

        logger.info(s"Overwrite completed in transaction version $version, added ${finalAddActions.length} files")
      } else {
        logger.debug(s"COMMIT DEBUG: Performing APPEND with ${finalAddActions.length} new files")
        // Standard append operation
        val version = transactionLog.addFiles(finalAddActions)
        logger.debug(s"COMMIT DEBUG: Append completed in transaction version $version")

        // Log what's in the transaction log after this operation
        val filesAfter = transactionLog.listFiles()
        logger.debug(s"COMMIT DEBUG: After APPEND, transaction log contains ${filesAfter.length} files:")
        filesAfter.foreach { action =>
          logger.debug(s"  - ${action.path}: ${action.numRecords.getOrElse(0)} records")
        }

        logger.info(s"Added ${finalAddActions.length} files in transaction version $version")
      }

      logger.debug(s"COMMIT DEBUG: Successfully committed ${finalAddActions.length} files")
      logger.info(s"Successfully committed ${finalAddActions.length} files")
    } finally
      // Always clear thread-local to prevent memory leaks
      TransactionLog.clearWriteOptions()
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logger.warn(s"Aborting write with ${messages.length} messages")

    // Clean up any files that were created but not committed
    val addActions: Seq[AddAction] = messages.flatMap {
      case msg: IndexTables4SparkCommitMessage => msg.addActions
      case _                                   => Seq.empty[AddAction]
    }

    // TODO: In a real implementation, we would delete the physical files here
    logger.warn(s"Would clean up ${addActions.length} uncommitted files")
  }

  /**
   * Validate indexing configuration for append operations. Checks that the new configuration is compatible with the
   * existing table configuration.
   */
  private def validateIndexingConfigurationForAppend(): Unit =
    try {
      logger.debug("VALIDATION DEBUG: Running append configuration validation")

      // Read existing doc mapping from latest add actions
      val existingFiles = transactionLog.listFiles()
      val existingDocMapping = existingFiles
        .flatMap(_.docMappingJson)
        .headOption // Get the first available doc mapping

      if (existingDocMapping.isDefined) {
        logger.debug("VALIDATION DEBUG: Found existing doc mapping, validating configuration")

        // Parse existing configuration
        import com.fasterxml.jackson.databind.JsonNode
        import io.indextables.spark.util.JsonUtil
        import scala.jdk.CollectionConverters._

        val existingMapping = JsonUtil.mapper.readTree(existingDocMapping.get: String)
        logger.debug(s"VALIDATION DEBUG: Parsed existing mapping JSON: $existingMapping")

        // The docMappingJson is directly an array of field definitions
        if (existingMapping.isArray) {
          logger.warn(
            s"VALIDATION DEBUG: Found existing fields array with ${existingMapping.size()} fields, processing..."
          )
          val tantivyOptions = io.indextables.spark.core.IndexTables4SparkOptions(
            new org.apache.spark.sql.util.CaseInsensitiveStringMap(serializedOptions.asJava)
          )
          val errors = scala.collection.mutable.ListBuffer[String]()

          logger.debug(s"VALIDATION DEBUG: Schema has ${writeSchema.fields.length} fields")

          // Check each field in the current schema for configuration conflicts
          writeSchema.fields.foreach { field =>
            try {
              val fieldName = field.name
              logger.debug(s"VALIDATION DEBUG: Processing field '$fieldName'")

              val currentConfig = tantivyOptions.getFieldIndexingConfig(fieldName)

              // Find the field in the array by name
              val existingFieldConfig = existingMapping.asScala.find { fieldNode =>
                Option(fieldNode.get("name")).map(_.asText()).contains(fieldName)
              }

              logger.debug(s"VALIDATION DEBUG: Current config: $currentConfig")
              logger.debug(s"VALIDATION DEBUG: Existing field config present: ${existingFieldConfig.isDefined}")

              if (existingFieldConfig.isDefined) {
                val existing     = existingFieldConfig.get
                val existingType = Option(existing.get("type")).map(_.asText())
                logger.debug(s"VALIDATION DEBUG: Existing field type: $existingType")

                // Check field type configuration conflicts
                if (currentConfig.fieldType.isDefined) {
                  val currentType = currentConfig.fieldType.get
                  logger.debug(s"VALIDATION DEBUG: Current type: $currentType")

                  // Strict validation: field types must match exactly
                  if (existingType.isDefined && existingType.get != currentType) {
                    logger.debug(s"VALIDATION DEBUG: CONFLICT DETECTED for field '$fieldName'!")
                    errors += s"Field '$fieldName' type mismatch: existing table has ${existingType.get} field, cannot append with $currentType configuration"
                  } else {
                    logger.debug(s"VALIDATION DEBUG: Compatible types for field '$fieldName' (existing: ${existingType.getOrElse("none")}, current: $currentType)")
                  }
                } else {
                  logger.debug(s"VALIDATION DEBUG: No current field type configured for '$fieldName'")
                }
              } else {
                logger.debug(s"VALIDATION DEBUG: Field '$fieldName' not found in existing configuration")
              }
            } catch {
              case e: Exception =>
                logger.debug(s"VALIDATION DEBUG: Exception processing field '${field.name}': ${e.getMessage}")
            }
          }

          logger.debug(s"VALIDATION DEBUG: Finished processing all fields. Errors found: ${errors.length}")
          if (errors.nonEmpty) {
            val errorMessage = s"Configuration validation failed for append operation:\n${errors.mkString("\n")}"
            logger.error(errorMessage)
            throw new IllegalArgumentException(errorMessage)
          }
        } else {
          logger.debug("VALIDATION DEBUG: Existing mapping is not an array - unexpected format")
        }
      } else {
        logger.debug("VALIDATION DEBUG: No existing doc mapping found, skipping validation")
      }
    } catch {
      case e: IllegalArgumentException => throw e // Re-throw validation errors
      case e: Exception =>
        logger.debug(s"VALIDATION DEBUG: Validation failed with exception: ${e.getMessage}")
      // Don't fail the write for other types of errors
    }

}
