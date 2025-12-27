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

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.{AddAction, TransactionLog}
import org.slf4j.LoggerFactory

class IndexTables4SparkBatchWrite(
  transactionLog: TransactionLog,
  tablePath: Path,
  writeInfo: LogicalWriteInfo,
  options: CaseInsensitiveStringMap,
  hadoopConf: org.apache.hadoop.conf.Configuration)
    extends BatchWrite
    with org.apache.spark.sql.connector.write.Write {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkBatchWrite])

  // Validate the write schema
  io.indextables.spark.util.SchemaValidator.validateNoDuplicateColumns(writeInfo.schema())

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    logger.info(s"Creating batch writer factory for ${info.numPartitions} partitions")

    // Set split conversion max parallelism if not already configured
    // Default: max(1, availableProcessors / 4)
    val configKey = io.indextables.spark.config.IndexTables4SparkSQLConf.TANTIVY4SPARK_SPLIT_CONVERSION_MAX_PARALLELISM
    val computedMaxParallelism = if (options.get(configKey) == null) {
      val availableProcessors = Runtime.getRuntime.availableProcessors()
      val maxParallelism      = Math.max(1, availableProcessors / 4)
      logger.info(s"Auto-configuring split conversion max parallelism: $maxParallelism (from availableProcessors=$availableProcessors)")
      Some(maxParallelism)
    } else {
      None
    }

    // Ensure DataFrame options are copied to Hadoop configuration for executor distribution
    val enrichedHadoopConf = new org.apache.hadoop.conf.Configuration(hadoopConf)

    // Copy all indextables options to hadoop config to ensure they reach executors
    val normalizedOptions = io.indextables.spark.util.ConfigNormalization.extractTantivyConfigsFromOptions(options)
    val serializedOptions = scala.collection.mutable.Map[String, String]() ++ normalizedOptions
    normalizedOptions.foreach {
      case (key, value) =>
        enrichedHadoopConf.set(key, value)
        logger.info(
          s"Copied DataFrame option to Hadoop config: $key = ${io.indextables.spark.util.CredentialRedaction
              .redactValue(key, value)}"
        )
    }

    // Add computed split conversion max parallelism if we calculated it
    computedMaxParallelism.foreach { maxPar =>
      serializedOptions.put(configKey, maxPar.toString)
      enrichedHadoopConf.set(configKey, maxPar.toString)
      logger.info(s"Added computed split conversion max parallelism to executor config: $maxPar")
    }

    // Serialize hadoop config properties to avoid Configuration serialization issues
    val serializedHadoopConfig =
      io.indextables.spark.util.ConfigNormalization.extractTantivyConfigsFromHadoop(enrichedHadoopConf)

    // BatchWrite does NOT support merge-on-write - pass empty partition columns
    new IndexTables4SparkWriterFactory(
      tablePath,
      writeInfo.schema(),
      serializedOptions.toMap,
      serializedHadoopConfig,
      Seq.empty
    )
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logger.info(s"Committing ${messages.length} writer messages")

    // Extract partition columns from write options (same fix as StandardWrite)
    val partitionColumns = Option(options.get("__partition_columns")) match {
      case Some(partitionColumnsJson) =>
        try {
          val partitionCols = io.indextables.spark.util.JsonUtil.parseStringArray(partitionColumnsJson)
          logger.debug(s"V2 BATCH DEBUG: Extracted partition columns: $partitionCols")
          partitionCols
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to parse partition columns in BatchWrite: $partitionColumnsJson", e)
            Seq.empty
        }
      case None => Seq.empty
    }

    // Initialize transaction log with schema and partition columns
    transactionLog.initialize(writeInfo.schema(), partitionColumns)

    val addActions: Seq[AddAction] = messages.flatMap {
      case msg: IndexTables4SparkCommitMessage => msg.addActions
      case _                                   => Seq.empty[AddAction]
    }

    // Log how many empty partitions were filtered out
    val emptyPartitionsCount = messages.length - addActions.length
    if (emptyPartitionsCount > 0) {
      logger.info(s"⚠️  Filtered out $emptyPartitionsCount empty partitions (0 records) from transaction log")
    }

    // Add all files in a single transaction (like Delta Lake)
    val version = transactionLog.addFiles(addActions)
    logger.info(s"Added ${addActions.length} files in transaction version $version")

    logger.info(s"Successfully committed ${addActions.length} files")
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logger.warn(s"Aborting write with ${messages.length} messages")

    // Clean up any files that were created but not committed
    val addActions: Seq[AddAction] = messages.flatMap {
      case msg: IndexTables4SparkCommitMessage => msg.addActions
      case _                                   => Seq.empty[AddAction]
    }

    // In a real implementation, we would delete the physical files here
    logger.warn(s"Would clean up ${addActions.length} uncommitted files")
  }
}

case class IndexTables4SparkCommitMessage(addActions: Seq[AddAction]) extends WriterCommitMessage
