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
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.broadcast.Broadcast
import com.tantivy4spark.transaction.TransactionLog
import org.slf4j.LoggerFactory

/**
 * Specialized scan that returns count directly from transaction log for optimal performance.
 * This is used when we can optimize COUNT(*) queries without any non-partition filters.
 */
class TransactionLogCountScan(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  broadcastConfig: Broadcast[Map[String, String]]
) extends Scan {

  private val logger = LoggerFactory.getLogger(classOf[TransactionLogCountScan])

  override def readSchema(): StructType = {
    // Return schema with a single count column
    StructType(Seq(StructField("count", LongType, nullable = false)))
  }

  override def toBatch: Batch = {
    new TransactionLogCountBatch(sparkSession, transactionLog, pushedFilters, options, broadcastConfig)
  }

  override def description(): String = {
    s"TransactionLogCountScan(path=${transactionLog.getTablePath()})"
  }
}

/**
 * Batch implementation for transaction log count.
 */
class TransactionLogCountBatch(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  broadcastConfig: Broadcast[Map[String, String]]
) extends Batch {

  private val logger = LoggerFactory.getLogger(classOf[TransactionLogCountBatch])

  override def planInputPartitions(): Array[InputPartition] = {
    // Create a single partition for the count operation
    Array(TransactionLogCountPartition(transactionLog, pushedFilters))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new TransactionLogCountReaderFactory(broadcastConfig)
  }
}

/**
 * Input partition for transaction log count.
 */
case class TransactionLogCountPartition(
  transactionLog: TransactionLog,
  pushedFilters: Array[Filter]
) extends InputPartition

/**
 * Reader factory for transaction log count.
 */
class TransactionLogCountReaderFactory(
  broadcastConfig: Broadcast[Map[String, String]]
) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case countPartition: TransactionLogCountPartition =>
        new TransactionLogCountPartitionReader(countPartition, broadcastConfig)
      case _ =>
        throw new IllegalArgumentException(s"Unexpected partition type: ${partition.getClass}")
    }
  }
}

/**
 * Partition reader that computes count from transaction log.
 */
class TransactionLogCountPartitionReader(
  partition: TransactionLogCountPartition,
  broadcastConfig: Broadcast[Map[String, String]]
) extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(classOf[TransactionLogCountPartitionReader])
  private var hasNext = true
  private var rowCount: Long = -1

  override def next(): Boolean = {
    if (hasNext) {
      // Compute count from transaction log
      rowCount = computeCountFromTransactionLog()
      hasNext = false
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = {
    if (rowCount == -1) {
      throw new IllegalStateException("next() must be called before get()")
    }
    // Return a single row with the count
    InternalRow(rowCount)
  }

  override def close(): Unit = {
    // Nothing to close
  }

  /**
   * Compute the count from transaction log, applying any partition filters.
   */
  private def computeCountFromTransactionLog(): Long = {
    logger.info(s"🔍 TRANSACTION LOG COUNT: Computing count from transaction log")

    if (partition.pushedFilters.isEmpty) {
      // No filters - return total count from transaction log
      val totalCount = partition.transactionLog.getTotalRowCount()
      logger.info(s"🔍 TRANSACTION LOG COUNT: Total count (no filters): $totalCount")
      totalCount
    } else {
      // Apply partition filters and sum counts
      val partitionFilters = partition.pushedFilters.filter(isPartitionFilter)
      val matchingFiles = partition.transactionLog.listFiles()
        .filter(file => matchesPartitionFilters(file, partitionFilters))

      val filteredCount = matchingFiles.map { file =>
        file.numRecords.map { (count: Any) =>
          // Handle any numeric type and convert to Long
          count match {
            case l: Long => l
            case i: Int => i.toLong
            case i: java.lang.Integer => i.toLong
            case _ => count.toString.toLong
          }
        }.getOrElse(0L)
      }.sum

      logger.info(s"🔍 TRANSACTION LOG COUNT: Filtered count (${partitionFilters.length} partition filters): $filteredCount")
      filteredCount
    }
  }

  /**
   * Check if a filter is a partition filter.
   */
  private def isPartitionFilter(filter: Filter): Boolean = {
    // For now, assume no partition filters are present
    // TODO: Implement proper partition filter detection based on partition columns
    false
  }

  /**
   * Check if a file matches the given partition filters.
   */
  private def matchesPartitionFilters(file: com.tantivy4spark.transaction.AddAction, filters: Array[Filter]): Boolean = {
    // For now, return true since we don't support partition filters yet
    true
  }
}