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
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{SortOrder, SortDirection, Expressions}
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
      if (entry.getKey.startsWith("spark.tantivy4spark.")) {
        props.put(entry.getKey, entry.getValue)
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
   * 1. DataFrame write options
   * 2. Spark session configuration
   * 3. Table properties  
   * 4. Default (1M records)
   */
  private def getTargetRecordsPerSplit(): Long = {
    val spark = SparkSession.active

    // Check DataFrame write options first
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

  override def toBatch: BatchWrite = this
  
  /**
   * RequiresDistributionAndOrdering implementation for V2 optimized writes.
   * This tells Spark how to distribute (repartition) the data before writing.
   */
  override def requiredDistribution(): Distribution = {
    // Use clustered distribution with the first available column for clustering
    // This ensures we have non-empty clustering expressions to avoid the 
    // "UnspecifiedDistribution + numPartitions" validation error
    val clusteringExpressions = if (writeSchema.fields.nonEmpty) {
      // Use the first field in the schema for clustering
      Array[org.apache.spark.sql.connector.expressions.Expression](
        Expressions.column(writeSchema.fields(0).name)
      )
    } else {
      // Fallback to empty array if no schema fields (shouldn't happen in practice)
      logger.warn("No schema fields available for clustering, using empty clustering")
      Array[org.apache.spark.sql.connector.expressions.Expression]()
    }
    
    val distribution = Distributions.clustered(clusteringExpressions)
    logger.info(s"V2 optimized write: requiredDistribution() returning ClusteredDistribution with ${clusteringExpressions.length} clustering expressions")
    if (clusteringExpressions.nonEmpty) {
      logger.info(s"Clustering by field: ${clusteringExpressions(0)}")
    }
    distribution
  }
  
  /**
   * Return the number of partitions required for optimized writes.
   * This is the key method that actually controls partitioning in V2.
   * Returns 0 to indicate no specific requirement.
   */
  override def requiredNumPartitions(): Int = {
    // Since this is Tantivy4SparkOptimizedWrite, always optimize
    val targetRecords = getTargetRecordsPerSplit()
    
    // Use the estimated row count passed from the write builder
    val numPartitions = math.ceil(estimatedRowCount.toDouble / targetRecords).toInt
    val finalPartitions = math.max(1, numPartitions)
    
    logger.info(s"V2 optimized write: Requesting $finalPartitions partitions for ~$estimatedRowCount records with target $targetRecords per split")
    finalPartitions
  }
  
  /**
   * No specific ordering requirements for Tantivy4Spark writes.
   */
  override def requiredOrdering(): Array[SortOrder] = Array.empty

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    val targetRecords = getTargetRecordsPerSplit()
    
    logger.info(s"Creating batch writer factory for ${info.numPartitions} partitions")
    logger.info(s"V2 optimized write enabled, target records per split: $targetRecords")
    logger.info(s"V2 optimized write: Using RequiresDistributionAndOrdering to control partitioning")
    logger.info(s"Actual partitions: ${info.numPartitions} (controlled by requiredNumPartitions)")
    
    if (partitionColumns.nonEmpty) {
      logger.info(s"Table is partitioned by: ${partitionColumns.mkString(", ")}")
    }
    
    // Combine serialized hadoop config with tantivy4spark options
    val combinedHadoopConfig = serializedHadoopConf ++ 
      serializedOptions.filter(_._1.startsWith("spark.tantivy4spark."))
      
    serializedOptions.foreach { case (key, value) =>
      if (key.startsWith("spark.tantivy4spark.")) {
        logger.info(s"Will copy DataFrame option to Hadoop config: $key = ${if (key.contains("secretKey") || key.contains("sessionToken")) "***" else value}")
      }
    }
    
    new Tantivy4SparkWriterFactory(tablePath, writeSchema, serializedOptions, combinedHadoopConfig, partitionColumns)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    println(s"🔍 DEBUG: Committing ${messages.length} writer messages (overwrite mode: $isOverwrite)")
    logger.warn(s"🔍 DEBUG: Committing ${messages.length} writer messages (overwrite mode: $isOverwrite)")
    println(s"🔍 DEBUG: serializedOptions keys: ${serializedOptions.keys.mkString(", ")}")
    serializedOptions.foreach { case (k, v) => 
      println(s"🔍 DEBUG: serializedOption $k = $v")
    }
    
    val addActions = messages.collect {
      case msg: Tantivy4SparkCommitMessage => msg.addAction
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
    
    // Initialize transaction log with schema if this is the first commit  
    transactionLog.initialize(writeInfo.schema())

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
    val addActions = messages.collect {
      case msg: Tantivy4SparkCommitMessage => msg.addAction
    }

    // In a real implementation, we would delete the physical files here
    logger.warn(s"Would clean up ${addActions.length} uncommitted files")
  }

}