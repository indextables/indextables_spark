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

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, Write, WriterCommitMessage}
import com.tantivy4spark.transaction.TransactionLog
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.slf4j.LoggerFactory

/**
 * Standard write implementation for Tantivy4Spark tables.
 * This implementation does NOT include RequiresDistributionAndOrdering,
 * so it uses Spark's default partitioning without any optimization.
 * Used when optimizeWrite is disabled.
 */
class Tantivy4SparkStandardWrite(
    @transient transactionLog: TransactionLog,
    tablePath: Path,
    @transient writeInfo: LogicalWriteInfo,
    serializedOptions: Map[String, String],  // Use serializable Map instead of CaseInsensitiveStringMap
    @transient hadoopConf: org.apache.hadoop.conf.Configuration,
    isOverwrite: Boolean = false  // Track whether this is an overwrite operation
) extends Write with BatchWrite with Serializable {

  @transient private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkStandardWrite])

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

  override def toBatch: BatchWrite = this

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    logger.info(s"Creating standard batch writer factory for ${info.numPartitions} partitions")
    logger.info(s"Using Spark's default partitioning (no optimization)")
    
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
    transactionLog.initialize(writeSchema, partitionColumns)
    
    // Commit the changes
    if (shouldOverwrite) {
      logger.info(s"Performing overwrite with ${addActions.length} new files")
      val version = transactionLog.overwriteFiles(addActions)
      logger.info(s"Overwrite completed in transaction version $version, added ${addActions.length} files")
    } else {
      // Standard append operation
      val version = transactionLog.addFiles(addActions)
      logger.info(s"Added ${addActions.length} files in transaction version $version")
    }
    
    logger.info(s"Successfully committed ${addActions.length} files")
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logger.warn(s"Aborting write with ${messages.length} messages")
    
    // Clean up any files that were created but not committed
    val addActions = messages.collect {
      case msg: Tantivy4SparkCommitMessage => msg.addAction
    }

    // TODO: In a real implementation, we would delete the physical files here
    logger.warn(s"Would clean up ${addActions.length} uncommitted files")
  }
}