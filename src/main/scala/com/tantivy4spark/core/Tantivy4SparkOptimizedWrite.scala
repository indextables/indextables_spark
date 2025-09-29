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

package com.tantivy4spark.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, SupportsOverwrite, SupportsTruncate, Write, WriterCommitMessage, RequiresDistributionAndOrdering}
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions, ClusteredDistribution}
import org.apache.spark.sql.connector.expressions.{SortOrder, SortDirection, Expressions, LogicalExpressions}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.transaction.{TransactionLog, AddAction}
import com.tantivy4spark.config.{Tantivy4SparkConfig, Tantivy4SparkSQLConf}
import com.tantivy4spark.optimize.Tantivy4SparkOptimizedWriterExec
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.slf4j.LoggerFactory

/**
 * Write implementation that supports optimized writes for Tantivy4Spark tables.
 * Similar to Delta Lake's TransactionalWrite, this adds an optimization layer
 * that can shuffle data to target optimal split sizes.
 */
class Tantivy4SparkOptimizedWrite(
    @transient transactionLog: TransactionLog,
    tablePath: Path,
    @transient writeInfo: LogicalWriteInfo,
    serializedOptions: Map[String, String],  // Use serializable Map instead of CaseInsensitiveStringMap
    @transient hadoopConf: org.apache.hadoop.conf.Configuration,
    isOverwrite: Boolean = false,  // Track whether this is an overwrite operation
    estimatedRowCount: Long = 1000000L  // Estimated row count for optimized partitioning
) extends Write with BatchWrite with RequiresDistributionAndOrdering with Serializable {

  @transient private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkOptimizedWrite])

  // Extract serializable values from transient fields during construction
  private val writeSchema = writeInfo.schema()
  private val serializedHadoopConf = {
    // Serialize only the tantivy4spark config properties from hadoopConf
    val props = scala.collection.mutable.Map[String, String]()
    val iter = hadoopConf.iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      if (entry.getKey.startsWith("spark.tantivy4spark.") || entry.getKey.startsWith("spark.indextables.")) {
        val normalizedKey = if (entry.getKey.startsWith("spark.indextables.")) {
          entry.getKey.replace("spark.indextables.", "spark.tantivy4spark.")
        } else entry.getKey
        props.put(normalizedKey, entry.getValue)
      }
    }
    props.toMap
  }
  private val partitionColumns = try {
    transactionLog.getPartitionColumns()
  } catch {
    case ex: Exception =>
      logger.warn(s"Could not retrieve partition columns during construction: ${ex.getMessage}")
      Seq.empty[String]
  }

  @transient private lazy val tantivyOptions = {
    // Recreate CaseInsensitiveStringMap from serialized options for API compatibility
    import scala.jdk.CollectionConverters._
    val optionsMap = new java.util.HashMap[String, String]()
    serializedOptions.foreach { case (k, v) => optionsMap.put(k, v) }
    val caseInsensitiveMap = new org.apache.spark.sql.util.CaseInsensitiveStringMap(optionsMap)
    Tantivy4SparkOptions(caseInsensitiveMap)
  }

  /**
   * Determine if optimized writes should be enabled based on configuration hierarchy:
   * 1. DataFrame write options
   * 2. Spark session configuration  
   * 3. Table properties
   * 4. Default (enabled)
   */
  private def shouldOptimizeWrite(): Boolean = {
    logger.warn(s"🔍 OPTIMIZED WRITE: shouldOptimizeWrite called")
    val spark = SparkSession.active

    // Check DataFrame write options first
    val writeOptionValue = tantivyOptions.optimizeWrite
    if (writeOptionValue.isDefined) {
      logger.debug(s"Using write option optimizeWrite = ${writeOptionValue.get}")
      return writeOptionValue.get
    }

    // Check Spark session configuration
    val sessionConfValue = spark.conf.getOption(Tantivy4SparkSQLConf.TANTIVY4SPARK_OPTIMIZE_WRITE_ENABLED)
      .map(_.toBoolean)
    if (sessionConfValue.isDefined) {
      logger.debug(s"Using session config optimizeWrite = ${sessionConfValue.get}")
      return sessionConfValue.get
    }

    // Check table properties (from metadata)
    try {
      val metadata = transactionLog.getMetadata()
      val tablePropertyValue = Tantivy4SparkConfig.OPTIMIZE_WRITE.fromMetadata(metadata)
      if (tablePropertyValue.isDefined) {
        logger.debug(s"Using table property optimizeWrite = ${tablePropertyValue.get}")
        return tablePropertyValue.get
      }
    } catch {
      case e: Exception =>
        logger.debug("Could not read table metadata for optimized write configuration", e)
    }

    // Default: enabled
    val defaultValue = Tantivy4SparkConfig.OPTIMIZE_WRITE.defaultValue
    logger.debug(s"Using default optimizeWrite = $defaultValue")
    defaultValue
  }

  /**
   * Get the target records per split from configuration hierarchy:
   * 1. Auto-sizing based on historical data (if enabled)
   * 2. DataFrame write options
   * 3. Spark session configuration
   * 4. Table properties
   * 5. Default (1M records)
   */
  private def getTargetRecordsPerSplit(): Long = {
    val spark = SparkSession.active

    // Check if auto-sizing is enabled first
    val autoSizeEnabled = tantivyOptions.autoSizeEnabled.getOrElse {
      spark.conf.getOption(Tantivy4SparkSQLConf.TANTIVY4SPARK_AUTO_SIZE_ENABLED)
        .map(_.toBoolean)
        .getOrElse(false) // Auto-sizing is disabled by default
    }

    if (autoSizeEnabled) {
      // Try to get target split size for auto-sizing
      val targetSplitSizeStr = tantivyOptions.autoSizeTargetSplitSize.orElse {
        spark.conf.getOption(Tantivy4SparkSQLConf.TANTIVY4SPARK_AUTO_SIZE_TARGET_SPLIT_SIZE)
      }

      // Validate auto-sizing configuration early - these errors should not be caught
      val targetSizeBytes = targetSplitSizeStr match {
        case Some(sizeStr) =>
          import com.tantivy4spark.util.SizeParser
          val bytes = SizeParser.parseSize(sizeStr)
          if (bytes <= 0) {
            throw new IllegalArgumentException(s"Auto-sizing target split size must be positive: $sizeStr")
          }
          bytes
        case None =>
          throw new IllegalArgumentException("Auto-sizing enabled but no target split size specified")
      }

      try {
        import com.tantivy4spark.util.{SizeParser, SplitSizeAnalyzer}
        logger.info(s"Auto-sizing enabled with target split size: ${SizeParser.formatBytes(targetSizeBytes)}")

        // Analyze historical data to calculate target rows
        val analyzer = SplitSizeAnalyzer(tablePath, spark, {
          import scala.jdk.CollectionConverters._
          val optionsMap = new java.util.HashMap[String, String]()
          serializedOptions.foreach { case (k, v) => optionsMap.put(k, v) }
          new org.apache.spark.sql.util.CaseInsensitiveStringMap(optionsMap)
        })

        analyzer.calculateTargetRows(targetSizeBytes) match {
          case Some(calculatedRows) =>
            logger.info(s"Auto-sizing calculated target rows per split: $calculatedRows")
            return calculatedRows
          case None =>
            logger.warn("Auto-sizing failed to calculate target rows from historical data, falling back to manual configuration")
            // Fall through to manual configuration
        }
      } catch {
        case ex: Exception =>
          logger.error(s"Auto-sizing historical analysis failed: ${ex.getMessage}", ex)
          // Fall through to manual configuration
      }
    }

    // Check DataFrame write options
    val writeOptionValue = tantivyOptions.targetRecordsPerSplit
    if (writeOptionValue.isDefined) {
      logger.debug(s"Using write option targetRecordsPerSplit = ${writeOptionValue.get}")
      return writeOptionValue.get
    }

    // Check Spark session configuration
    val sessionConfValue = spark.conf.getOption(Tantivy4SparkSQLConf.TANTIVY4SPARK_OPTIMIZE_WRITE_TARGET_RECORDS_PER_SPLIT)
      .map(_.toLong)
    if (sessionConfValue.isDefined) {
      logger.debug(s"Using session config targetRecordsPerSplit = ${sessionConfValue.get}")
      return sessionConfValue.get
    }

    // Check table properties (from metadata)
    try {
      val metadata = transactionLog.getMetadata()
      val tablePropertyValue = Tantivy4SparkConfig.OPTIMIZE_WRITE_TARGET_RECORDS_PER_SPLIT.fromMetadata(metadata)
      if (tablePropertyValue.isDefined) {
        logger.debug(s"Using table property targetRecordsPerSplit = ${tablePropertyValue.get}")
        return tablePropertyValue.get
      }
    } catch {
      case e: Exception =>
        logger.debug("Could not read table metadata for target records per split configuration", e)
    }

    // Default: 1M records
    val defaultValue = Tantivy4SparkConfig.OPTIMIZE_WRITE_TARGET_RECORDS_PER_SPLIT.defaultValue
    logger.debug(s"Using default targetRecordsPerSplit = $defaultValue")
    defaultValue
  }

  /**
   * Create a potentially optimized physical plan for writing data.
   * This is similar to Delta Lake's TransactionalWrite.writeFiles method.
   */
  def createOptimizedWritePlan(queryExecution: org.apache.spark.sql.execution.QueryExecution): org.apache.spark.sql.execution.SparkPlan = {
    logger.warn(s"🔍 OPTIMIZED WRITE: createOptimizedWritePlan called")
    val originalPlan = queryExecution.executedPlan

    if (shouldOptimizeWrite()) {
      val targetRecords = getTargetRecordsPerSplit()
      logger.info(s"Enabling optimized write with target $targetRecords records per split")
      
      // TODO: Extract partition columns from table metadata if available
      val partitionColumns = Seq.empty[String] // For now, no partitioning support
      
      Tantivy4SparkOptimizedWriterExec(
        child = originalPlan,
        partitionColumns = partitionColumns,
        targetRecordsPerSplit = targetRecords
      )
    } else {
      logger.info("Optimized write disabled, using original execution plan")
      originalPlan
    }
  }

  override def toBatch: BatchWrite = {
    logger.warn(s"🔍 OPTIMIZED WRITE: toBatch called")
    this
  }
  
  /**
   * RequiresDistributionAndOrdering implementation for V2 optimized writes.
   * This tells Spark how to distribute (repartition) the data before writing.
   */
  override def requiredDistribution(): Distribution = {
    if (partitionColumns.nonEmpty) {
      // For partitioned tables, cluster by partition columns using the correct constructor
      val clusteredColumns = partitionColumns.toArray
      logger.info(s"V2 optimized write: clustering by partition columns: ${partitionColumns.mkString(", ")}")
      Distributions.clustered(clusteredColumns.map(Expressions.identity))
    } else if (writeSchema.fields.nonEmpty) {
      // For non-partitioned tables, use the first field in the schema for clustering
      logger.info(s"V2 optimized write: clustering by first schema field: ${writeSchema.fields(0).name}")
      Distributions.clustered(Array(Expressions.identity(writeSchema.fields(0).name)))
    } else {
      // Fallback to unspecified distribution if no schema fields
      logger.warn("No schema fields available for clustering, using unspecified distribution")
      Distributions.unspecified()
    }
  }
  
  /**
   * Return the number of partitions required for optimized writes.
   * This is the key method that actually controls partitioning in V2.
   * For auto-sizing, this method will attempt to count the input DataFrame.
   */
  override def requiredNumPartitions(): Int = {
    logger.warn(s"🔍 OPTIMIZED WRITE: requiredNumPartitions called")
    val targetRecords = getTargetRecordsPerSplit()

    // Check if auto-sizing is enabled
    val spark = SparkSession.active
    val autoSizeEnabled = tantivyOptions.autoSizeEnabled.getOrElse {
      spark.conf.getOption(Tantivy4SparkSQLConf.TANTIVY4SPARK_AUTO_SIZE_ENABLED)
        .map(_.toBoolean)
        .getOrElse(false)
    }

    val actualRowCount = if (autoSizeEnabled) {
      logger.info("V2 Auto-sizing enabled for partitioning")

      // Check if explicit row count was provided in options
      val explicitRowCount = tantivyOptions.autoSizeInputRowCount

      explicitRowCount match {
        case Some(rowCount) =>
          logger.info(s"V2 Auto-sizing: using explicit row count = $rowCount")
          rowCount
        case None =>
          logger.warn("V2 Auto-sizing: enabled but no explicit row count provided. " +
                     "Using estimated row count for partitioning. " +
                     "For accurate auto-sizing with V2 API, call df.count() first and pass as option: " +
                     ".option(\"spark.tantivy4spark.autoSize.inputRowCount\", rowCount.toString)")
          logger.warn("V2 Auto-sizing: Falling back to estimated row count, partitioning may not be optimal")
          estimatedRowCount
      }
    } else {
      estimatedRowCount
    }

    // Calculate number of partitions based on row count and target records per split
    val numPartitions = math.ceil(actualRowCount.toDouble / targetRecords).toInt
    val finalPartitions = math.max(1, numPartitions)

    if (autoSizeEnabled) {
      logger.info(s"Auto-sizing: Requesting $finalPartitions partitions for $actualRowCount records with target $targetRecords per split")
    } else {
      logger.info(s"Standard partitioning: Requesting $finalPartitions partitions for ~$actualRowCount records with target $targetRecords per split")
    }

    finalPartitions
  }
  
  /**
   * No specific ordering requirements for Tantivy4Spark writes.
   */
  override def requiredOrdering(): Array[SortOrder] = Array.empty

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    logger.warn(s"🔍 OPTIMIZED WRITE: createBatchWriterFactory called with ${info.numPartitions} partitions")
    val targetRecords = getTargetRecordsPerSplit()

    logger.info(s"Creating batch writer factory for ${info.numPartitions} partitions")
    logger.info(s"V2 optimized write enabled, target records per split: $targetRecords")
    logger.info(s"V2 optimized write: Using RequiresDistributionAndOrdering to control partitioning")
    logger.info(s"Actual partitions: ${info.numPartitions} (controlled by requiredNumPartitions)")
    
    if (partitionColumns.nonEmpty) {
      logger.info(s"Table is partitioned by: ${partitionColumns.mkString(", ")}")
    }
    
    // Combine serialized hadoop config with tantivy4spark options (normalize spark.indextables to spark.tantivy4spark)
    val normalizedTantivyOptions = serializedOptions
      .filter(kv => kv._1.startsWith("spark.tantivy4spark.") || kv._1.startsWith("spark.indextables."))
      .map { case (key, value) =>
        val normalizedKey = if (key.startsWith("spark.indextables.")) {
          key.replace("spark.indextables.", "spark.tantivy4spark.")
        } else key
        normalizedKey -> value
      }
    val combinedHadoopConfig = serializedHadoopConf ++ normalizedTantivyOptions

    serializedOptions.foreach { case (key, value) =>
      if (key.startsWith("spark.tantivy4spark.") || key.startsWith("spark.indextables.")) {
        val normalizedKey = if (key.startsWith("spark.indextables.")) {
          key.replace("spark.indextables.", "spark.tantivy4spark.")
        } else key
        logger.info(s"Will copy DataFrame option to Hadoop config: $normalizedKey = ${if (key.contains("secretKey") || key.contains("sessionToken")) "***" else value}")
      }
    }
    
    new Tantivy4SparkWriterFactory(tablePath, writeSchema, serializedOptions, combinedHadoopConfig, partitionColumns)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    println(s"🔍 DEBUG: Committing ${messages.length} writer messages (overwrite mode: $isOverwrite)")
    logger.warn(s"🔍 DEBUG: Committing ${messages.length} writer messages (overwrite mode: $isOverwrite)")
    println(s"🔍 DEBUG: serializedOptions keys: ${serializedOptions.keys.mkString(", ")}")
    serializedOptions.foreach { case (k, v) =>
      val redactedValue = if (k.toLowerCase.contains("secret") || k.toLowerCase.contains("key") || k.toLowerCase.contains("password") || k.toLowerCase.contains("token")) {
        "***REDACTED***"
      } else {
        v
      }
      println(s"🔍 DEBUG: serializedOption $k = $redactedValue")
    }
    
    val addActions: Seq[AddAction] = messages.flatMap {
      case msg: Tantivy4SparkCommitMessage => msg.addActions
      case _ => Seq.empty[AddAction]
    }

    // Determine if this should be an overwrite based on existing table state and mode
    val shouldOverwrite = if (isOverwrite) {
      // Explicit overwrite flag from truncate() or overwrite() call
      println("🔍 DEBUG: Using explicit overwrite flag (isOverwrite = true)")
      logger.warn("🔍 DEBUG: Using explicit overwrite flag (isOverwrite = true)")
      true
    } else {
      println("🔍 DEBUG: isOverwrite = false, checking other conditions")
      logger.warn("🔍 DEBUG: isOverwrite = false, checking other conditions")
      // For DataSource V2, SaveMode.Overwrite might not trigger truncate()/overwrite() methods
      // Instead, we need to detect overwrite by checking the logical write info or options
      val saveMode = serializedOptions.get("saveMode") match {
        case Some("Overwrite") => true
        case Some("ErrorIfExists") => false
        case Some("Ignore") => false  
        case Some("Append") => false
        case None => {
          // Check if this looks like an initial write (no existing files) - treat as overwrite
          try {
            val existingFiles = transactionLog.listFiles()
            if (existingFiles.isEmpty) {
              logger.info("No existing files found - treating as initial write (overwrite semantics)")
              false // Initial write doesn't need overwrite semantics, just add files
            } else {
              logger.info(s"Found ${existingFiles.length} existing files - need to determine write mode")
              // Without explicit mode info, default to append to be safe
              false
            }
          } catch {
            case _: Exception => false // If we can't read transaction log, assume append
          }
        }
        case Some(other) => {
          logger.warn(s"Unknown saveMode: $other, defaulting to append")
          false
        }
      }
      saveMode
    }
    
    // Extract partition columns from write options (same fix as StandardWrite)
    val partitionColumns = serializedOptions.get("__partition_columns") match {
      case Some(partitionColumnsJson) =>
        try {
          import com.fasterxml.jackson.module.scala.DefaultScalaModule
          import com.fasterxml.jackson.databind.ObjectMapper
          val mapper = new ObjectMapper()
          mapper.registerModule(DefaultScalaModule)
          val partitionCols = mapper.readValue(partitionColumnsJson, classOf[Array[String]]).toSeq
          logger.info(s"🔍 V2 OPTIMIZED DEBUG: Extracted partition columns: $partitionCols")
          partitionCols
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to parse partition columns in OptimizedWrite: $partitionColumnsJson", e)
            Seq.empty
        }
      case None => Seq.empty
    }

    // Initialize transaction log with schema and partition columns
    transactionLog.initialize(writeInfo.schema(), partitionColumns)

    // Use appropriate transaction log method based on write mode
    val version = if (shouldOverwrite) {
      println(s"🔍 DEBUG: Performing overwrite operation with ${addActions.length} new files")
      logger.warn(s"🔍 DEBUG: Performing overwrite operation with ${addActions.length} new files")
      transactionLog.overwriteFiles(addActions)
    } else {
      println(s"🔍 DEBUG: Performing append operation with ${addActions.length} files")
      logger.warn(s"🔍 DEBUG: Performing append operation with ${addActions.length} files")
      transactionLog.addFiles(addActions)
    }
    
    logger.info(s"Successfully committed ${addActions.length} files in transaction version $version")
    
    // Log total row count after commit (safely handling different types)
    try {
      val totalRows = transactionLog.getTotalRowCount()
      logger.info(s"Total rows in table after commit: $totalRows")
    } catch {
      case e: Exception =>
        logger.warn(s"Could not calculate total row count: ${e.getMessage}")
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logger.warn(s"Aborting write with ${messages.length} messages")
    
    // Clean up any files that were created but not committed
    val addActions: Seq[AddAction] = messages.flatMap {
      case msg: Tantivy4SparkCommitMessage => msg.addActions
      case _ => Seq.empty[AddAction]
    }

    // In a real implementation, we would delete the physical files here
    logger.warn(s"Would clean up ${addActions.length} uncommitted files")
  }

}