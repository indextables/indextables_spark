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

import io.indextables.spark.transaction.{AddAction, EnhancedTransactionLogCache, TransactionLog}
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

  // Validate the write schema
  io.indextables.spark.util.SchemaValidator.validateNoDuplicateColumns(writeSchema)

  // Validate that indexing configuration fields exist in schema
  {
    import scala.jdk.CollectionConverters._
    val tantivyOptions = IndexTables4SparkOptions(
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(serializedOptions.asJava)
    )
    tantivyOptions.validateFieldsExist(writeSchema)
  }
  private val serializedHadoopConf =
    // Serialize only the tantivy4spark config properties from hadoopConf (includes credentials from enrichedHadoopConf)
    io.indextables.spark.util.ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
  protected val partitionColumns =
    // Extract partition columns from write options (set by .partitionBy())
    // Spark sets this as a JSON array string like ["col1","col2"]
    serializedOptions.get("__partition_columns") match {
      case Some(partitionColumnsJson) =>
        try {
          val partitionCols = io.indextables.spark.util.JsonUtil.parseStringArray(partitionColumnsJson)
          logger.debug(s"PARTITION DEBUG: Extracted partition columns from options: $partitionCols")
          partitionCols
        } catch {
          case e: Exception =>
            logger.debug(
              s"PARTITION DEBUG: Failed to parse partition columns JSON: $partitionColumnsJson, error: ${e.getMessage}"
            )
            logger.warn(s"Failed to parse partition columns from options: $partitionColumnsJson", e)
            Seq.empty
        }
      case None =>
        // Fallback: try to read from existing transaction log
        try {
          val cols = transactionLog.getPartitionColumns()
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

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    logger.info(s"Creating standard batch writer factory for ${info.numPartitions} partitions")
    logger.info(s"Using Spark's default partitioning (no optimization)")

    if (partitionColumns.nonEmpty) {
      logger.info(s"Table is partitioned by: ${partitionColumns.mkString(", ")}")
    }

    // Combine serialized options with normalized indextables options
    // Map-based config - no HadoopConf needed (fast path)
    val normalizedTantivyOptions =
      io.indextables.spark.util.ConfigNormalization.filterAndNormalizeTantivyConfigs(serializedOptions)
    val combinedConfig = serializedHadoopConf ++ normalizedTantivyOptions ++ serializedOptions

    // Log the options being passed
    normalizedTantivyOptions.foreach {
      case (key, value) =>
        logger.info(
          s"DataFrame option: $key = ${io.indextables.spark.util.CredentialRedaction
              .redactValue(key, value)}"
        )
    }

    new IndexTables4SparkWriterFactory(
      tablePath,
      writeSchema,
      combinedConfig,
      partitionColumns
    )
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logger.debug(s"DEBUG: Committing ${messages.length} writer messages (overwrite mode: $isOverwrite)")
    logger.debug(s"DEBUG: Committing ${messages.length} writer messages (overwrite mode: $isOverwrite)")
    logger.debug(s"DEBUG: serializedOptions keys: ${serializedOptions.keys.mkString(", ")}")
    serializedOptions.foreach {
      case (k, v) =>
        logger.debug(s"DEBUG: serializedOption $k = ${io.indextables.spark.util.CredentialRedaction.redactValue(k, v)}")
    }

    // Guard: reject writes to companion-mode tables (must use BUILD INDEXTABLES COMPANION FOR ...)
    try {
      val metadata = transactionLog.getMetadata()
      if (metadata.configuration.getOrElse("indextables.companion.enabled", "false") == "true") {
        throw new UnsupportedOperationException(
          "Cannot write to a companion-mode table. Use BUILD INDEXTABLES COMPANION FOR <format> to update."
        )
      }
      // Also check AddAction fields (companion tables created via BUILD COMPANION)
      val files = transactionLog.listFiles()
      if (files.exists(_.companionSourceFiles.nonEmpty)) {
        throw new UnsupportedOperationException(
          "Cannot write to a companion-mode table. Use BUILD INDEXTABLES COMPANION FOR <format> to update."
        )
      }
    } catch {
      case e: UnsupportedOperationException => throw e
      case _: Exception => // Table may not exist yet, which is fine
    }

    // Validate indexing configuration for append operations
    if (!isOverwrite) {
      validateIndexingConfigurationForAppend()
    }

    // Extract AddActions from commit messages
    val addActions: Seq[AddAction] = messages.flatMap {
      case msg: IndexTables4SparkCommitMessage => msg.addActions
      case _                                   => Seq.empty[AddAction]
    }

    // Log how many empty partitions were filtered out
    val emptyPartitionsCount = messages.length - addActions.size
    if (emptyPartitionsCount > 0) {
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
        case Some("Overwrite") =>
          logger.debug("SAVEMODE DEBUG: saveMode=Overwrite in options, returning true")
          true
        case Some("ErrorIfExists") =>
          logger.debug("SAVEMODE DEBUG: saveMode=ErrorIfExists in options, returning false")
          false
        case Some("Ignore") =>
          logger.debug("SAVEMODE DEBUG: saveMode=Ignore in options, returning false")
          false
        case Some("Append") =>
          logger.debug("SAVEMODE DEBUG: saveMode=Append in options, returning false")
          false
        case None =>
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
    val optionsMap = new java.util.HashMap[String, String]()
    serializedOptions.foreach { case (k, v) => optionsMap.put(k, v) }
    val writeOptions = new org.apache.spark.sql.util.CaseInsensitiveStringMap(optionsMap)

    // Set thread-local write options so TransactionLog can access compression settings
    TransactionLog.setWriteOptions(writeOptions)

    try {
      // Commit the changes to transaction log
      if (shouldOverwrite) {
        logger.debug(s"COMMIT DEBUG: Performing OVERWRITE with ${addActions.length} new files")
        val version = transactionLog.overwriteFiles(addActions)
        logger.debug(s"COMMIT DEBUG: Overwrite completed in transaction version $version")

        // Log what's in the transaction log after this operation
        val filesAfter = transactionLog.listFiles()
        logger.debug(s"COMMIT DEBUG: After OVERWRITE, transaction log contains ${filesAfter.length} files:")
        filesAfter.foreach(action => logger.debug(s"  - ${action.path}: ${action.numRecords.getOrElse(0)} records"))

        logger.info(s"Overwrite completed in transaction version $version, added ${addActions.length} files")
      } else {
        logger.debug(s"COMMIT DEBUG: Performing APPEND with ${addActions.length} new files")
        // Standard append operation
        val version = transactionLog.addFiles(addActions)
        logger.debug(s"COMMIT DEBUG: Append completed in transaction version $version")

        // Log what's in the transaction log after this operation
        val filesAfter = transactionLog.listFiles()
        logger.debug(s"COMMIT DEBUG: After APPEND, transaction log contains ${filesAfter.length} files:")
        filesAfter.foreach(action => logger.debug(s"  - ${action.path}: ${action.numRecords.getOrElse(0)} records"))

        logger.info(s"Added ${addActions.length} files in transaction version $version")
      }

      logger.debug(s"COMMIT DEBUG: Successfully committed ${addActions.length} files")
      logger.info(s"Successfully committed ${addActions.length} files")

      // POST-COMMIT EVALUATION: Check if merge-on-write should run
      val mergeExecuted = evaluateAndExecuteMergeOnWrite(writeOptions)

      // POST-COMMIT EVALUATION: Check if purge-on-write should run
      evaluateAndExecutePurgeOnWrite(writeOptions, mergeWasExecuted = mergeExecuted)
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
        import io.indextables.spark.util.JsonUtil
        import scala.jdk.CollectionConverters._

        EnhancedTransactionLogCache.incrementGlobalJsonParseCounter()
        val existingMapping = JsonUtil.mapper.readTree(existingDocMapping.get: String)
        logger.debug(s"VALIDATION DEBUG: Parsed existing mapping JSON: $existingMapping")

        // The docMappingJson is directly an array of field definitions
        if (existingMapping.isArray) {
          logger.debug(
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

                  // Check for type compatibility
                  // Note: "json" config maps to "object" type in tantivy, these are equivalent
                  val typesCompatible = existingType.isEmpty || {
                    val existing = existingType.get
                    val current  = currentType
                    existing == current ||
                    (existing == "object" && current == "json") ||
                    (existing == "json" && current == "object")
                  }

                  if (!typesCompatible) {
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

  /**
   * Evaluate if merge-on-write should run after transaction commit.
   *
   * This method:
   *   1. Checks if merge-on-write is enabled 2. Checks if merge already in progress for this table 3. Calculates batch
   *      size and threshold using new formulas 4. Counts mergeable groups from the transaction log 5. If async enabled,
   *      submits to AsyncMergeOnWriteManager 6. Otherwise invokes MERGE SPLITS command synchronously
   *
   * New threshold formula: threshold = batchSize × minBatchesToTrigger Batch size formula: batchSize = max(1,
   * totalClusterCpus × batchCpuFraction)
   */
  private def evaluateAndExecuteMergeOnWrite(
    writeOptions: org.apache.spark.sql.util.CaseInsensitiveStringMap
  ): Boolean =
    try {
      import io.indextables.spark.merge.{AsyncMergeOnWriteConfig, AsyncMergeOnWriteManager}

      // Load configuration
      val config = AsyncMergeOnWriteConfig.fromOptions(writeOptions)

      if (!config.enabled) {
        logger.debug("Merge-on-write is disabled, skipping post-commit evaluation")
        return false
      }

      logger.info("🔀 Merge-on-write enabled - evaluating if merge is worthwhile...")

      // Configure manager with current config (always configure when merge-on-write enabled)
      AsyncMergeOnWriteManager.configure(config)

      // Check if merge already in progress for this table
      val tablePathStr = tablePath.toString
      if (AsyncMergeOnWriteManager.isMergeInProgress(tablePathStr)) {
        logger.info(s"Merge already in progress for $tablePathStr, skipping")
        return false
      }

      // Get Spark session for default parallelism (represents total cluster CPUs)
      val spark            = org.apache.spark.sql.SparkSession.active
      val totalClusterCpus = spark.sparkContext.defaultParallelism

      // Calculate batch size and threshold using new formulas
      val batchSize = config.calculateBatchSize(totalClusterCpus)
      val threshold = config.calculateMergeThreshold(totalClusterCpus)

      logger.info(
        s"Merge config: batchSize=$batchSize (CPUs: $totalClusterCpus × fraction: ${config.batchCpuFraction}), " +
          s"threshold=$threshold (batchSize: $batchSize × minBatches: ${config.minBatchesToTrigger})"
      )

      // Count mergeable groups using MergeSplitsExecutor (dry-run to get accurate count)
      val mergeGroups = countMergeGroupsUsingExecutor(config.targetSizeString, writeOptions)

      logger.info(s"Found $mergeGroups mergeable groups (threshold: $threshold)")

      if (mergeGroups >= threshold) {
        if (config.asyncEnabled) {
          // Async execution - submit to manager and return immediately
          logger.info(s"✅ Merge worthwhile: $mergeGroups groups ≥ $threshold threshold - submitting async merge job")

          // Convert writeOptions to Map and merge with hadoop config
          import scala.jdk.CollectionConverters._
          val optionsMap    = writeOptions.asCaseSensitiveMap().asScala.toMap
          val mergedOptions = serializedHadoopConf ++ optionsMap

          // Submit async job
          AsyncMergeOnWriteManager.submitMergeJob(
            tablePath = tablePathStr,
            totalMergeGroups = mergeGroups,
            batchSize = batchSize,
            transactionLog = transactionLog,
            writeOptions = mergedOptions,
            serializedHadoopConf = serializedHadoopConf,
            sparkSession = spark
          ) match {
            case Right(job) =>
              logger.info(s"Async merge job ${job.jobId} submitted for $tablePathStr")
            case Left(reason) =>
              logger.warn(s"Async merge job rejected: $reason")
          }

          true // Merge job was submitted (async)

        } else {
          // Synchronous execution (existing behavior)
          logger.info(
            s"✅ Merge worthwhile: $mergeGroups groups ≥ $threshold threshold - executing MERGE SPLITS synchronously"
          )
          executeMergeSplitsCommand(writeOptions)
          true // Merge was executed
        }
      } else {
        logger.info(s"⏭️  Merge not worthwhile: $mergeGroups groups < $threshold threshold - skipping")
        false // Merge was not executed
      }

    } catch {
      case e: Exception =>
        // Don't fail the write operation if merge evaluation fails
        logger.warn(s"Failed to evaluate merge-on-write: ${e.getMessage}", e)
        false // Merge was not executed due to error
    }

  /**
   * Count number of merge groups using MergeSplitsExecutor's dry-run logic. This ensures we use the exact same
   * algorithm as the actual merge operation, avoiding any discrepancies between decision-making and execution.
   */
  private def countMergeGroupsUsingExecutor(
    targetSizeStr: String,
    writeOptions: org.apache.spark.sql.util.CaseInsensitiveStringMap
  ): Int =
    try {
      import io.indextables.spark.sql.MergeSplitsExecutor
      import scala.jdk.CollectionConverters._

      val targetSizeBytes = io.indextables.spark.util.SizeParser.parseSize(targetSizeStr)
      val spark           = org.apache.spark.sql.SparkSession.active

      // Extract options to pass to executor (same as executeMergeSplitsCommand)
      val optionsFromWrite = writeOptions
        .asCaseSensitiveMap()
        .asScala
        .toMap
        .filter { case (key, _) => key.startsWith("spark.indextables.") }

      // Merge with serializedHadoopConf to include credentials
      val optionsToPass = serializedHadoopConf ++ optionsFromWrite

      // Create executor with same parameters as actual merge
      val executor = new MergeSplitsExecutor(
        sparkSession = spark,
        transactionLog = transactionLog,
        tablePath = tablePath,
        partitionPredicates = Seq.empty, // No partition filtering for count
        targetSize = targetSizeBytes,
        maxDestSplits = None,           // No limit for counting
        maxSourceSplitsPerMerge = None, // Use config default
        preCommitMerge = false,
        overrideOptions = Some(optionsToPass)
      )

      // Use the dry-run method to count groups
      val count = executor.countMergeGroups()
      logger.debug(s"MergeSplitsExecutor counted $count merge groups")
      count

    } catch {
      case e: Exception =>
        logger.warn(s"Failed to count merge groups using executor: ${e.getMessage}", e)
        0
    }

  /**
   * Execute MERGE SPLITS command programmatically with write options.
   *
   * This method passes write options directly to MergeSplitsExecutor so it has access to AWS/Azure credentials, temp
   * directories, heap size, and all other write-time configuration.
   */
  private def executeMergeSplitsCommand(writeOptions: org.apache.spark.sql.util.CaseInsensitiveStringMap): Unit =
    try {
      import scala.jdk.CollectionConverters._

      val targetSizeStr   = writeOptions.getOrDefault("spark.indextables.mergeOnWrite.targetSize", "4G")
      val targetSizeBytes = io.indextables.spark.util.SizeParser.parseSize(targetSizeStr)

      logger.info(s"Executing MERGE SPLITS for table: ${tablePath.toString} with target size: $targetSizeStr ($targetSizeBytes bytes)")

      val spark = org.apache.spark.sql.SparkSession.active

      // Extract all indextables options from writeOptions to pass to merge executor
      val optionsFromWrite = writeOptions
        .asCaseSensitiveMap()
        .asScala
        .toMap
        .filter { case (key, _) => key.startsWith("spark.indextables.") }

      // CRITICAL FIX: Merge with serializedHadoopConf to include credentials from enrichedHadoopConf
      // Write options take precedence over hadoopConf
      val optionsToPass = serializedHadoopConf ++ optionsFromWrite

      logger.info(s"Passing ${optionsToPass.size} options to MERGE SPLITS executor (${optionsFromWrite.size} from write options, ${serializedHadoopConf.size} from hadoop conf)")
      optionsToPass.foreach {
        case (key, value) =>
          logger.debug(
            s"  Passing option: $key = ${io.indextables.spark.util.CredentialRedaction.redactValue(key, value)}"
          )
      }

      // Create executor with transaction log and write options
      val executor = new io.indextables.spark.sql.MergeSplitsExecutor(
        sparkSession = spark,
        transactionLog = transactionLog,
        tablePath = tablePath,
        partitionPredicates = Seq.empty, // Merge all partitions
        targetSize = targetSizeBytes,
        maxDestSplits = None,           // No limit on groups for auto-merge
        maxSourceSplitsPerMerge = None, // Use config default
        preCommitMerge = false,
        overrideOptions = Some(optionsToPass) // Pass write options directly
      )

      // Execute merge
      val results = executor.merge()

      // Log first row of results (contains metrics)
      if (results.nonEmpty) {
        val firstRow = results.head
        logger.info(
          s"✅ MERGE SPLITS completed: ${firstRow.getString(0)} - merged ${firstRow.getStruct(1).getLong(1)} files"
        )
      } else {
        logger.info(s"✅ MERGE SPLITS completed with no results")
      }

    } catch {
      case e: Exception =>
        // Don't fail the write if merge fails
        logger.warn(s"Failed to execute MERGE SPLITS: ${e.getMessage}", e)
    }

  /**
   * Evaluate if purge-on-write should run after transaction commit.
   *
   * This is a POST-COMMIT evaluation that:
   *   1. Checks if purge-on-write is enabled 2. Checks if purge should trigger based on configuration:
   *      - After merge-on-write completion (if triggerAfterMerge=true)
   *      - After N write transactions (if triggerAfterWrites > 0) 3. Invokes PURGE ORPHANED SPLITS command with
   *        configured retention periods
   *
   * @param writeOptions
   *   Write options containing purge-on-write configuration
   * @param mergeWasExecuted
   *   Whether merge-on-write just executed
   */
  private def evaluateAndExecutePurgeOnWrite(
    writeOptions: org.apache.spark.sql.util.CaseInsensitiveStringMap,
    mergeWasExecuted: Boolean
  ): Unit =
    try {
      import io.indextables.spark.purge.{PurgeOnWriteConfig, PurgeOnWriteTransactionCounter}

      // Load purge-on-write configuration
      val config = PurgeOnWriteConfig.fromOptions(writeOptions)

      if (!config.enabled) {
        logger.debug("Purge-on-write is disabled, skipping post-commit evaluation")
        return
      }

      logger.info("🧹 Purge-on-write enabled - evaluating if purge should run...")

      // Determine if purge should trigger
      val shouldTrigger = if (config.triggerAfterMerge && mergeWasExecuted) {
        // Trigger 1: After merge-on-write completion
        logger.info("✅ Purge triggered: merge-on-write just completed")
        true
      } else if (config.triggerAfterWrites > 0) {
        // Trigger 2: After N write transactions
        val txCount = PurgeOnWriteTransactionCounter.incrementAndGet(tablePath.toString)
        logger.debug(s"Transaction count for $tablePath: $txCount (threshold: ${config.triggerAfterWrites})")

        if (txCount >= config.triggerAfterWrites) {
          logger.info(s"✅ Purge triggered: transaction count $txCount ≥ threshold ${config.triggerAfterWrites}")
          // Reset counter after triggering
          PurgeOnWriteTransactionCounter.reset(tablePath.toString)
          true
        } else {
          logger.info(s"⏭️  Purge not triggered: transaction count $txCount < threshold ${config.triggerAfterWrites}")
          false
        }
      } else {
        logger.debug(
          "Purge-on-write enabled but no trigger conditions met (triggerAfterMerge=false, triggerAfterWrites=0)"
        )
        false
      }

      if (shouldTrigger) {
        executePurgeOrphanedSplitsCommand(config, writeOptions)
      }

    } catch {
      case e: Exception =>
        // Don't fail the write operation if purge evaluation fails
        logger.warn(s"Failed to evaluate purge-on-write: ${e.getMessage}", e)
    }

  /**
   * Execute PURGE ORPHANED SPLITS command programmatically with write options.
   *
   * This method passes write options directly to PurgeOrphanedSplitsExecutor so it has access to AWS/Azure credentials,
   * cache directories, and all other write-time configuration.
   *
   * @param config
   *   Purge-on-write configuration
   * @param writeOptions
   *   Write options containing credentials and configuration
   */
  private def executePurgeOrphanedSplitsCommand(
    config: io.indextables.spark.purge.PurgeOnWriteConfig,
    writeOptions: org.apache.spark.sql.util.CaseInsensitiveStringMap
  ): Unit =
    try {
      import io.indextables.spark.sql.PurgeOrphanedSplitsExecutor
      import scala.jdk.CollectionConverters._

      logger.info(s"Executing PURGE ORPHANED SPLITS for table: ${tablePath.toString}")
      logger.info(s"Split retention: ${config.splitRetentionHours} hours, Transaction log retention: ${config.txLogRetentionHours} hours")

      val spark = org.apache.spark.sql.SparkSession.active

      // Extract all indextables options from writeOptions to pass to purge executor
      val optionsFromWrite = writeOptions
        .asCaseSensitiveMap()
        .asScala
        .toMap
        .filter { case (key, _) => key.startsWith("spark.indextables.") }

      // CRITICAL: Merge with serializedHadoopConf to include credentials from enrichedHadoopConf
      // Write options take precedence over hadoopConf
      val optionsToPass = serializedHadoopConf ++ optionsFromWrite

      logger.info(s"Passing ${optionsToPass.size} options to PURGE ORPHANED SPLITS executor (${optionsFromWrite.size} from write options, ${serializedHadoopConf.size} from hadoop conf)")
      optionsToPass.foreach {
        case (key, value) =>
          logger.debug(
            s"  Passing option: $key = ${io.indextables.spark.util.CredentialRedaction.redactValue(key, value)}"
          )
      }

      // Convert retention from hours to milliseconds for transaction log
      val txLogRetentionMs = config.txLogRetentionHours * 60 * 60 * 1000

      // Create executor with write options (including credentials)
      val executor = new PurgeOrphanedSplitsExecutor(
        spark = spark,
        tablePath = tablePath.toString,
        retentionHours = config.splitRetentionHours,
        txLogRetentionDuration = Some(txLogRetentionMs),
        dryRun = false,
        overrideOptions = Some(optionsToPass)
      )

      // Execute purge
      val result = executor.purge()

      // Log results
      logger.info(s"✅ PURGE ORPHANED SPLITS completed:")
      logger.info(s"   - Status: ${result.status}")
      logger.info(s"   - Orphaned files found: ${result.orphanedFilesFound}")
      logger.info(s"   - Orphaned files deleted: ${result.orphanedFilesDeleted}")
      logger.info(s"   - Size deleted: ${result.sizeMBDeleted} MB")
      logger.info(s"   - Expired states found: ${result.expiredStatesFound}")
      logger.info(s"   - Expired states deleted: ${result.expiredStatesDeleted}")

    } catch {
      case e: Exception =>
        // Don't fail the write if purge fails
        logger.warn(s"Failed to execute PURGE ORPHANED SPLITS: ${e.getMessage}", e)
    }

}
