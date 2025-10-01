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
 * Specialized scan that returns count directly from transaction log for optimal performance. This is used when we can
 * optimize COUNT(*) queries without any non-partition filters.
 */
class TransactionLogCountScan(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  config: Map[String, String] // Direct config instead of broadcast
) extends Scan {

  private val logger = LoggerFactory.getLogger(classOf[TransactionLogCountScan])

  override def readSchema(): StructType =
    // Return schema with a single count column
    StructType(Seq(StructField("count", LongType, nullable = false)))

  override def toBatch: Batch =
    new TransactionLogCountBatch(sparkSession, transactionLog, pushedFilters, options, config)

  override def description(): String =
    s"TransactionLogCountScan(path=${transactionLog.getTablePath()})"
}

/** Batch implementation for transaction log count. */
class TransactionLogCountBatch(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  config: Map[String, String] // Direct config instead of broadcast
) extends Batch {

  private val logger = LoggerFactory.getLogger(classOf[TransactionLogCountBatch])

  override def planInputPartitions(): Array[InputPartition] = {
    // Pre-compute the count on the driver side to avoid serialization issues
    val precomputedCount = computeCountFromTransactionLog(transactionLog, pushedFilters)
    Array(TransactionLogCountPartition(precomputedCount))
  }

  /** Compute the count from transaction log on the driver side. */
  private def computeCountFromTransactionLog(transactionLog: TransactionLog, pushedFilters: Array[Filter]): Long = {
    println(s"🔍 TRANSACTION LOG COUNT: Computing count from transaction log on driver")
    println(s"🔍 TRANSACTION LOG COUNT: Number of pushed filters: ${pushedFilters.length}")
    pushedFilters.foreach(filter => println(s"🔍 TRANSACTION LOG COUNT: Filter: $filter"))

    if (pushedFilters.isEmpty) {
      // No filters - return total count from transaction log
      val totalCount = transactionLog.getTotalRowCount()
      println(s"🔍 TRANSACTION LOG COUNT: Total count (no filters): $totalCount")
      totalCount
    } else {
      // Apply partition filters and sum counts
      val partitionFilters = pushedFilters.filter(isPartitionFilter(_, transactionLog))
      println(s"🔍 TRANSACTION LOG COUNT: Found ${partitionFilters.length} partition filters out of ${pushedFilters.length} total filters")
      partitionFilters.foreach(filter => println(s"🔍 TRANSACTION LOG COUNT: Partition filter: $filter"))

      val allFiles = transactionLog.listFiles()
      println(s"🔍 TRANSACTION LOG COUNT: Total files in transaction log: ${allFiles.length}")

      val matchingFiles = allFiles.filter(file => matchesPartitionFilters(file, partitionFilters))
      println(s"🔍 TRANSACTION LOG COUNT: Files matching partition filters: ${matchingFiles.length}")

      matchingFiles.foreach { file =>
        println(s"🔍 TRANSACTION LOG COUNT: Matching file: ${file.path}, partitionValues: ${file.partitionValues}, numRecords: ${file.numRecords}")
      }

      val filteredCount = matchingFiles.map { file =>
        file.numRecords
          .map { (count: Any) =>
            // Handle any numeric type and convert to Long
            count match {
              case l: Long              => l
              case i: Int               => i.toLong
              case i: java.lang.Integer => i.toLong
              case _                    => count.toString.toLong
            }
          }
          .getOrElse(0L)
      }.sum

      println(s"🔍 TRANSACTION LOG COUNT: Filtered count (${partitionFilters.length} partition filters): $filteredCount")
      filteredCount
    }
  }

  /** Check if a filter is a partition filter. */
  private def isPartitionFilter(filter: Filter, transactionLog: TransactionLog): Boolean = {
    val partitionColumns  = transactionLog.getPartitionColumns()
    val referencedColumns = getFilterReferencedColumns(filter)
    referencedColumns.nonEmpty && referencedColumns.forall(partitionColumns.contains)
  }

  /** Get columns referenced by a filter. */
  private def getFilterReferencedColumns(filter: Filter): Set[String] = {
    import org.apache.spark.sql.sources._
    filter match {
      case EqualTo(attribute, _)            => Set(attribute)
      case EqualNullSafe(attribute, _)      => Set(attribute)
      case GreaterThan(attribute, _)        => Set(attribute)
      case GreaterThanOrEqual(attribute, _) => Set(attribute)
      case LessThan(attribute, _)           => Set(attribute)
      case LessThanOrEqual(attribute, _)    => Set(attribute)
      case In(attribute, _)                 => Set(attribute)
      case IsNull(attribute)                => Set(attribute)
      case IsNotNull(attribute)             => Set(attribute)
      case StringStartsWith(attribute, _)   => Set(attribute)
      case StringEndsWith(attribute, _)     => Set(attribute)
      case StringContains(attribute, _)     => Set(attribute)
      case And(left, right)                 => getFilterReferencedColumns(left) ++ getFilterReferencedColumns(right)
      case Or(left, right)                  => getFilterReferencedColumns(left) ++ getFilterReferencedColumns(right)
      case Not(child)                       => getFilterReferencedColumns(child)
      case _                                => Set.empty[String]
    }
  }

  /** Check if a file matches the given partition filters. */
  private def matchesPartitionFilters(file: com.tantivy4spark.transaction.AddAction, filters: Array[Filter]): Boolean =
    if (filters.isEmpty) {
      true
    } else {
      // Use PartitionPruning to check if the file matches the filters
      val fileSeq = Seq(file)
      // We need partition columns for this, but we can't access transaction log here easily
      // Let's implement a simple match logic for now
      filters.forall(matchesFilter(file, _))
    }

  /** Check if a file matches a single filter. */
  private def matchesFilter(file: com.tantivy4spark.transaction.AddAction, filter: Filter): Boolean = {
    import org.apache.spark.sql.sources._
    filter match {
      case EqualTo(attribute, value) =>
        file.partitionValues.get(attribute).contains(value.toString)
      case EqualNullSafe(attribute, value) =>
        file.partitionValues.get(attribute).contains(value.toString)
      case And(left, right) =>
        matchesFilter(file, left) && matchesFilter(file, right)
      case Or(left, right) =>
        matchesFilter(file, left) || matchesFilter(file, right)
      case Not(child) =>
        !matchesFilter(file, child)
      case _ => true // Default to including the file if we can't evaluate the filter
    }
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new TransactionLogCountReaderFactory(config)
}

/**
 * Input partition for transaction log count. Instead of serializing the transaction log, we pre-compute the count on
 * the driver.
 */
case class TransactionLogCountPartition(
  precomputedCount: Long)
    extends InputPartition

/** Reader factory for transaction log count. */
class TransactionLogCountReaderFactory(
  config: Map[String, String] // Direct config instead of broadcast
) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    partition match {
      case countPartition: TransactionLogCountPartition =>
        new TransactionLogCountPartitionReader(countPartition, config)
      case _ =>
        throw new IllegalArgumentException(s"Unexpected partition type: ${partition.getClass}")
    }
}

/** Partition reader that returns the precomputed count. */
class TransactionLogCountPartitionReader(
  partition: TransactionLogCountPartition,
  config: Map[String, String] // Direct config instead of broadcast
) extends PartitionReader[InternalRow] {

  private val logger  = LoggerFactory.getLogger(classOf[TransactionLogCountPartitionReader])
  private var hasNext = true

  override def next(): Boolean =
    if (hasNext) {
      hasNext = false
      true
    } else {
      false
    }

  override def get(): InternalRow =
    // Return a single row with the precomputed count
    InternalRow(partition.precomputedCount)

  override def close(): Unit = {
    // Nothing to close
  }
}
