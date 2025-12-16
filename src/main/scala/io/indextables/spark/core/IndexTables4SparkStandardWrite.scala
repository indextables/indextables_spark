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
import io.indextables.spark.xref.{XRefAutoIndexer, XRefConfig}
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

  // Validate schema for duplicate column names
  private def validateSchema(schema: org.apache.spark.sql.types.StructType): Unit = {
    val fieldNames = schema.fieldNames
    val duplicates = fieldNames.groupBy(identity).filter(_._2.length > 1).keys.toSeq

    if (duplicates.nonEmpty) {
      val duplicateList = duplicates.mkString(", ")
      val errorMsg = s"Schema contains duplicate column names: [$duplicateList]. " +
        s"Please ensure all column names are unique. Duplicate columns can cause JVM crashes."
      logger.error(errorMsg)
      throw new IllegalArgumentException(errorMsg)
    }

    // Also check for case-insensitive duplicates (warn only, don't fail)
    val lowerCaseNames            = fieldNames.map(_.toLowerCase)
    val caseInsensitiveDuplicates = lowerCaseNames.groupBy(identity).filter(_._2.length > 1).keys.toSeq

    if (caseInsensitiveDuplicates.nonEmpty) {
      val originalNames =
        caseInsensitiveDuplicates.flatMap(lower => fieldNames.filter(_.toLowerCase == lower)).distinct.mkString(", ")
      logger.warn(
        s"Schema contains columns that differ only in case: [$originalNames]. " +
          s"This may cause issues with case-insensitive storage systems."
      )
    }
  }

  // Validate the write schema
  validateSchema(writeSchema)
  private val serializedHadoopConf =
    // Serialize only the tantivy4spark config properties from hadoopConf (includes credentials from enrichedHadoopConf)
    io.indextables.spark.util.ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
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

    new IndexTables4SparkWriterFactory(
      tablePath,
      writeSchema,
      serializedOptions,
      combinedHadoopConfig,
      partitionColumns
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
    import scala.jdk.CollectionConverters._
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

      // POST-COMMIT EVALUATION: Check if XRef auto-indexing should run
      evaluateAndExecuteXRefAutoIndex(addActions)
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
   *   1. Checks if merge-on-write is enabled 2. Counts mergeable groups from the transaction log 3. Compares against
   *      threshold (defaultParallelism * mergeGroupMultiplier) 4. Invokes MERGE SPLITS command if worthwhile
   */
  private def evaluateAndExecuteMergeOnWrite(
    writeOptions: org.apache.spark.sql.util.CaseInsensitiveStringMap
  ): Boolean =
    try {
      // Check if merge-on-write is enabled
      val mergeOnWriteEnabled = writeOptions.getOrDefault("spark.indextables.mergeOnWrite.enabled", "false").toBoolean

      if (!mergeOnWriteEnabled) {
        logger.debug("Merge-on-write is disabled, skipping post-commit evaluation")
        return false
      }

      logger.info("🔀 Merge-on-write enabled - evaluating if merge is worthwhile...")

      // Get configuration
      val mergeGroupMultiplier = writeOptions
        .getOrDefault(
          "spark.indextables.mergeOnWrite.mergeGroupMultiplier",
          "2.0"
        )
        .toDouble
      val targetSize = writeOptions.getOrDefault(
        "spark.indextables.mergeOnWrite.targetSize",
        "4G"
      )

      // Get Spark session for default parallelism
      val spark              = org.apache.spark.sql.SparkSession.active
      val defaultParallelism = spark.sparkContext.defaultParallelism
      val threshold          = (defaultParallelism * mergeGroupMultiplier).toInt

      logger.info(s"Merge threshold: $threshold merge groups (parallelism: $defaultParallelism × multiplier: $mergeGroupMultiplier)")

      // Count mergeable groups using MergeSplitsExecutor (dry-run to get accurate count)
      val mergeGroups = countMergeGroupsUsingExecutor(targetSize, writeOptions)

      logger.info(s"Found $mergeGroups mergeable groups (threshold: $threshold)")

      if (mergeGroups >= threshold) {
        logger.info(s"✅ Merge worthwhile: $mergeGroups groups ≥ $threshold threshold - executing MERGE SPLITS")
        executeMergeSplitsCommand(writeOptions)
        true // Merge was executed
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
        maxGroups = None, // No limit for counting
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
      import io.indextables.spark.sql.MergeSplitsExecutor
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
          val displayValue =
            if (
              key.toLowerCase.contains("secret") || key.toLowerCase
                .contains("key") || key.toLowerCase.contains("password")
            ) "***"
            else value
          logger.debug(s"  Passing option: $key = $displayValue")
      }

      // Create executor with transaction log and write options
      val executor = new io.indextables.spark.sql.MergeSplitsExecutor(
        sparkSession = spark,
        transactionLog = transactionLog,
        tablePath = tablePath,
        partitionPredicates = Seq.empty, // Merge all partitions
        targetSize = targetSizeBytes,
        maxGroups = None, // No limit on groups for auto-merge
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
          val displayValue =
            if (
              key.toLowerCase.contains("secret") || key.toLowerCase
                .contains("key") || key.toLowerCase.contains("password")
            ) "***"
            else value
          logger.debug(s"  Passing option: $key = $displayValue")
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
      logger.info(s"   - Transaction logs deleted: ${result.transactionLogsDeleted}")

    } catch {
      case e: Exception =>
        // Don't fail the write if purge fails
        logger.warn(s"Failed to execute PURGE ORPHANED SPLITS: ${e.getMessage}", e)
    }

  /**
   * Evaluate if XRef auto-indexing should run after transaction commit.
   *
   * This is a POST-COMMIT evaluation that:
   *   1. Checks if XRef auto-indexing is enabled
   *   2. Checks if enough uncovered splits exist to trigger indexing
   *   3. Invokes XRefAutoIndexer to build XRef indexes
   *
   * @param newlyAddedSplits
   *   Splits that were just committed
   */
  private def evaluateAndExecuteXRefAutoIndex(newlyAddedSplits: Seq[AddAction]): Unit =
    try {
      val spark = org.apache.spark.sql.SparkSession.active
      val xrefConfig = XRefConfig.fromSparkSession(spark)

      if (!xrefConfig.autoIndex.enabled) {
        logger.debug("XRef auto-indexing is disabled, skipping post-commit evaluation")
        return
      }

      logger.info("📑 XRef auto-indexing enabled - evaluating if indexing should run...")

      // Create merged config map with proper precedence: hadoop < spark < write options
      // This ensures cloud credentials and other write-time settings are available
      val optionsFromWrite = serializedOptions
        .filter { case (key, _) => key.startsWith("spark.indextables.") }
      val mergedConfigMap = serializedHadoopConf ++ optionsFromWrite

      logger.debug(s"Passing ${mergedConfigMap.size} config entries to XRef auto-indexer")

      val autoIndexer = new XRefAutoIndexer(transactionLog, xrefConfig, spark, Some(mergedConfigMap))
      val result = autoIndexer.onCommit(newlyAddedSplits)

      if (result.triggered) {
        if (result.error.isDefined) {
          logger.warn(s"⚠️  XRef auto-indexing failed: ${result.error.get}")
        } else {
          logger.info(s"✅ XRef auto-indexing completed: ${result.xrefsBuilt} XRefs built, ${result.splitsIndexed} splits indexed in ${result.durationMs}ms")
        }
      } else {
        logger.info(s"⏭️  XRef auto-indexing skipped: ${result.reason}")
      }

    } catch {
      case e: Exception =>
        // Don't fail the write operation if XRef auto-indexing fails
        logger.warn(s"Failed to evaluate XRef auto-indexing: ${e.getMessage}", e)
    }

}
