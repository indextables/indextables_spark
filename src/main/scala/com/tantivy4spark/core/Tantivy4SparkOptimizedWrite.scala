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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, SupportsOverwrite, SupportsTruncate, Write, WriterCommitMessage}
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
    transactionLog: TransactionLog,
    tablePath: Path,
    writeInfo: LogicalWriteInfo,
    options: CaseInsensitiveStringMap,
    hadoopConf: org.apache.hadoop.conf.Configuration,
    isOverwrite: Boolean = false  // Track whether this is an overwrite operation
) extends Write with BatchWrite with SupportsOverwrite with SupportsTruncate {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkOptimizedWrite])

  private val tantivyOptions = Tantivy4SparkOptions(options)

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

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    val shouldOptimize = shouldOptimizeWrite()
    val targetRecords = getTargetRecordsPerSplit()
    
    logger.info(s"Creating batch writer factory for ${info.numPartitions} partitions")
    logger.info(s"Optimized write enabled: $shouldOptimize, target records per split: $targetRecords")
    
    if (shouldOptimize) {
      logger.warn("ISSUE: Optimized write is enabled but DataSource V2 API doesn't allow execution plan modification at this stage")
      logger.warn("The number of partitions (${info.numPartitions}) was determined by Spark's default partitioning")
      logger.warn("Each partition will create one split file, regardless of target records per split")
    }
    
    // Get partition columns from transaction log metadata
    val partitionColumns = try {
      transactionLog.getPartitionColumns()
    } catch {
      case ex: Exception =>
        logger.warn(s"Could not retrieve partition columns: ${ex.getMessage}")
        Seq.empty[String]
    }
    
    if (partitionColumns.nonEmpty) {
      logger.info(s"Table is partitioned by: ${partitionColumns.mkString(", ")}")
    }
    
    // Ensure DataFrame options are copied to Hadoop configuration for executor distribution
    val enrichedHadoopConf = new org.apache.hadoop.conf.Configuration(hadoopConf)
    
    // Copy all tantivy4spark options to hadoop config to ensure they reach executors
    import scala.jdk.CollectionConverters._
    options.entrySet().asScala.foreach { entry =>
      val key = entry.getKey
      val value = entry.getValue
      if (key.startsWith("spark.tantivy4spark.")) {
        enrichedHadoopConf.set(key, value)
        logger.info(s"Copied DataFrame option to Hadoop config: $key = ${if (key.contains("secretKey") || key.contains("sessionToken")) "***" else value}")
      }
    }
    
    new Tantivy4SparkWriterFactory(tablePath, writeInfo.schema(), options, enrichedHadoopConf, partitionColumns)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logger.info(s"Committing ${messages.length} writer messages (overwrite mode: $isOverwrite)")
    
    val addActions = messages.collect {
      case msg: Tantivy4SparkCommitMessage => msg.addAction
    }

    // Determine if this should be an overwrite based on existing table state and mode
    val shouldOverwrite = if (isOverwrite) {
      // Explicit overwrite flag from truncate() call
      true
    } else {
      // Check if this is the first write to the table and we have SaveMode.Overwrite semantics
      // For DataSource V2, SaveMode.Overwrite might not trigger truncate() but we should still
      // check if we need to overwrite existing data
      try {
        val existingFiles = transactionLog.listFiles()
        if (existingFiles.nonEmpty) {
          logger.info(s"Table has ${existingFiles.length} existing files, checking if overwrite is needed")
          // If the table exists with files and this is a write operation, we need to determine
          // if this should overwrite. For now, we'll assume it's append unless explicitly truncated.
          false
        } else {
          false
        }
      } catch {
        case _: Exception => false // If we can't read transaction log, assume append
      }
    }
    
    // Initialize transaction log with schema if this is the first commit  
    transactionLog.initialize(writeInfo.schema())

    // Use appropriate transaction log method based on write mode
    val version = if (shouldOverwrite) {
      logger.info(s"Performing overwrite operation with ${addActions.length} new files")
      transactionLog.overwriteFiles(addActions)
    } else {
      logger.info(s"Performing append operation with ${addActions.length} files")
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

  override def overwrite(filters: Array[org.apache.spark.sql.sources.Filter]): org.apache.spark.sql.connector.write.WriteBuilder = {
    logger.info(s"Overwrite operation with ${filters.length} filters")
    // Return a new WriteBuilder that creates an overwrite Write
    new org.apache.spark.sql.connector.write.WriteBuilder {
      override def build(): org.apache.spark.sql.connector.write.Write = 
        new Tantivy4SparkOptimizedWrite(transactionLog, tablePath, writeInfo, options, hadoopConf, isOverwrite = true)
    }
  }

  override def truncate(): org.apache.spark.sql.connector.write.WriteBuilder = {
    logger.info("Truncate operation")
    // Truncate is treated as overwrite with no filters
    new org.apache.spark.sql.connector.write.WriteBuilder {
      override def build(): org.apache.spark.sql.connector.write.Write = 
        new Tantivy4SparkOptimizedWrite(transactionLog, tablePath, writeInfo, options, hadoopConf, isOverwrite = true)
    }
  }
}