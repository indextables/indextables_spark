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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import io.indextables.spark.transaction.TransactionLog
import org.slf4j.LoggerFactory

/**
 * Specialized scan that returns count directly from transaction log for optimal performance. This is used when we can
 * optimize COUNT(*) queries without any non-partition filters, including:
 *   - Simple COUNT(*) with no GROUP BY
 *   - GROUP BY partition_column(s) COUNT(*) - groups and counts from transaction log metadata
 */
class TransactionLogCountScan(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  config: Map[String, String],                  // Direct config instead of broadcast
  groupByColumns: Option[Array[String]] = None, // GROUP BY columns (if any)
  hasAggregations: Boolean = true               // Whether this is COUNT or just DISTINCT (GROUP BY without agg)
) extends Scan {

  private val logger = LoggerFactory.getLogger(classOf[TransactionLogCountScan])

  override def readSchema(): StructType = {
    val schema = groupByColumns match {
      case Some(cols) =>
        // GROUP BY: Return schema with GROUP BY columns + optional count column
        // IMPORTANT: Spark expects GROUP BY columns to be named group_col_0, group_col_1, etc.
        // See org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown lines 94-102 (Spark 3.5.3)
        val groupByFields = cols.zipWithIndex.map {
          case (col, idx) =>
            StructField(s"group_col_$idx", StringType, nullable = true)
        }

        if (hasAggregations) {
          // COUNT aggregation: Add agg_func_0 column
          val countField = StructField("agg_func_0", LongType, nullable = false)
          StructType(groupByFields :+ countField)
        } else {
          // DISTINCT (GROUP BY without aggregations): Only GROUP BY columns
          StructType(groupByFields)
        }
      case None =>
        // Simple COUNT: Return schema with a single count column
        StructType(Seq(StructField("count", LongType, nullable = false)))
    }
    logger.debug(s"TRANSACTION LOG readSchema(): Returning schema with ${schema.fields.length} fields: ${schema.fieldNames.mkString(", ")}, hasAggregations=$hasAggregations")
    schema
  }

  override def toBatch: Batch =
    new TransactionLogCountBatch(
      sparkSession,
      transactionLog,
      pushedFilters,
      options,
      config,
      groupByColumns,
      hasAggregations
    )

  override def description(): String = {
    val groupByDesc = groupByColumns.map(cols => s", groupBy=[${cols.mkString(", ")}]").getOrElse("")
    s"TransactionLogCountScan(path=${transactionLog.getTablePath()}$groupByDesc)"
  }
}

/** Batch implementation for transaction log count. */
class TransactionLogCountBatch(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  config: Map[String, String], // Direct config instead of broadcast
  groupByColumns: Option[Array[String]] = None,
  hasAggregations: Boolean = true)
    extends Batch {

  private val logger = LoggerFactory.getLogger(classOf[TransactionLogCountBatch])

  override def planInputPartitions(): Array[InputPartition] =
    groupByColumns match {
      case Some(cols) =>
        // GROUP BY: Pre-compute grouped counts on driver
        val groupedCounts = computeGroupByCountFromTransactionLog(cols)
        Array(TransactionLogGroupByCountPartition(groupedCounts, cols))
      case None =>
        // Simple COUNT: Pre-compute total count on driver
        val precomputedCount = computeCountFromTransactionLog(transactionLog, pushedFilters)
        Array(TransactionLogCountPartition(precomputedCount))
    }

  /** Compute the count from transaction log on the driver side. */
  private def computeCountFromTransactionLog(transactionLog: TransactionLog, pushedFilters: Array[Filter]): Long =
    if (pushedFilters.isEmpty) {
      // No filters - return total count from transaction log
      transactionLog.getTotalRowCount()
    } else {
      // Apply partition filters and sum counts
      val partitionFilters = pushedFilters.filter(isPartitionFilter(_, transactionLog))
      val allFiles         = transactionLog.listFiles()
      val matchingFiles    = allFiles.filter(file => matchesPartitionFilters(file, partitionFilters))

      matchingFiles.map { file =>
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
    }

  /** Compute grouped counts from transaction log for GROUP BY partition columns. */
  private def computeGroupByCountFromTransactionLog(groupByCols: Array[String]): Seq[(Array[String], Long)] = {
    // Get all files from transaction log
    val allFiles = transactionLog.listFiles()

    // Apply partition filters if any
    val partitionFilters = pushedFilters.filter(isPartitionFilter(_, transactionLog))

    val matchingFiles = if (partitionFilters.isEmpty) {
      allFiles
    } else {
      allFiles.filter(file => matchesPartitionFilters(file, partitionFilters))
    }

    // Group by partition values and sum record counts
    val grouped = matchingFiles.groupBy { file =>
      // Extract partition values for GROUP BY columns (preserve order)
      groupByCols.map(col => file.partitionValues.getOrElse(col, ""))
    }

    // Sum counts for each group
    grouped
      .map {
        case (partitionValues, files) =>
          val totalCount = files.map { file =>
            file.numRecords
              .map { (count: Any) =>
                count match {
                  case l: Long              => l
                  case i: Int               => i.toLong
                  case i: java.lang.Integer => i.toLong
                  case other                => other.toString.toLong
                }
              }
              .getOrElse(0L)
          }.sum
          (partitionValues, totalCount)
      }
      .toSeq
      .sortBy(_._1.mkString(",")) // Sort for deterministic output
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
  private def matchesPartitionFilters(file: io.indextables.spark.transaction.AddAction, filters: Array[Filter]): Boolean =
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
  private def matchesFilter(file: io.indextables.spark.transaction.AddAction, filter: Filter): Boolean = {
    import org.apache.spark.sql.sources._
    filter match {
      case EqualTo(attribute, value) =>
        file.partitionValues.get(attribute).contains(value.toString)
      case EqualNullSafe(attribute, value) =>
        file.partitionValues.get(attribute).contains(value.toString)
      case GreaterThan(attribute, value) =>
        file.partitionValues.get(attribute).exists(v => compareValues(v, value.toString) > 0)
      case GreaterThanOrEqual(attribute, value) =>
        file.partitionValues.get(attribute).exists(v => compareValues(v, value.toString) >= 0)
      case LessThan(attribute, value) =>
        file.partitionValues.get(attribute).exists(v => compareValues(v, value.toString) < 0)
      case LessThanOrEqual(attribute, value) =>
        file.partitionValues.get(attribute).exists(v => compareValues(v, value.toString) <= 0)
      case In(attribute, values) =>
        file.partitionValues.get(attribute).exists(v => values.map(_.toString).contains(v))
      case IsNull(attribute) =>
        !file.partitionValues.contains(attribute) || file.partitionValues.get(attribute).isEmpty
      case IsNotNull(attribute) =>
        file.partitionValues.get(attribute).nonEmpty
      case And(left, right) =>
        matchesFilter(file, left) && matchesFilter(file, right)
      case Or(left, right) =>
        matchesFilter(file, left) || matchesFilter(file, right)
      case Not(child) =>
        !matchesFilter(file, child)
      case _ =>
        // Log warning for unhandled filters instead of silently including
        logger.warn(
          s"TRANSACTION LOG: Unhandled filter type: ${filter.getClass.getSimpleName}, defaulting to include file"
        )
        true
    }
  }

  /** Compare two partition values for range filters. Handles strings, dates, and numeric values. */
  private def compareValues(partitionValue: String, filterValue: String): Int =
    // Try numeric comparison first
    try {
      val pNum = partitionValue.toDouble
      val fNum = filterValue.toDouble
      pNum.compareTo(fNum)
    } catch {
      case _: NumberFormatException =>
        // Fall back to string comparison (works for ISO dates like "2024-01-01")
        partitionValue.compareTo(filterValue)
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

/**
 * Input partition for transaction log GROUP BY count. Contains precomputed grouped counts to avoid serialization
 * issues.
 */
case class TransactionLogGroupByCountPartition(
  groupedCounts: Seq[(Array[String], Long)],
  groupByColumns: Array[String])
    extends InputPartition

/** Reader factory for transaction log count. */
class TransactionLogCountReaderFactory(
  config: Map[String, String] // Direct config instead of broadcast
) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    partition match {
      case countPartition: TransactionLogCountPartition =>
        new TransactionLogCountPartitionReader(countPartition, config)
      case groupByPartition: TransactionLogGroupByCountPartition =>
        new TransactionLogGroupByCountPartitionReader(groupByPartition, config)
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

/** Partition reader that returns the precomputed grouped counts. */
class TransactionLogGroupByCountPartitionReader(
  partition: TransactionLogGroupByCountPartition,
  config: Map[String, String])
    extends PartitionReader[InternalRow] {

  private val logger                  = LoggerFactory.getLogger(classOf[TransactionLogGroupByCountPartitionReader])
  private val iterator                = partition.groupedCounts.iterator
  private var currentRow: InternalRow = _

  override def next(): Boolean =
    if (iterator.hasNext) {
      val (partitionValues, count) = iterator.next()
      // Convert partition values to UTF8String
      val utf8PartitionValues = partitionValues.map(v => org.apache.spark.unsafe.types.UTF8String.fromString(v))
      // Create row: partition columns + count
      currentRow = InternalRow.fromSeq(utf8PartitionValues :+ count)
      true
    } else {
      false
    }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    // Nothing to close
  }
}
