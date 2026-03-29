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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DateType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import io.indextables.spark.transaction.TransactionLogInterface
import org.slf4j.LoggerFactory

/**
 * Specialized scan that returns count directly from transaction log for optimal performance. This is used when we can
 * optimize COUNT(*) queries without any non-partition filters, including:
 *   - Simple COUNT(*) with no GROUP BY
 *   - GROUP BY partition_column(s) COUNT(*) - groups and counts from transaction log metadata
 */
class TransactionLogCountScan(
  sparkSession: SparkSession,
  transactionLog: TransactionLogInterface,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  config: Map[String, String],                  // Direct config instead of broadcast
  groupByColumns: Option[Array[String]] = None, // GROUP BY columns (if any)
  hasAggregations: Boolean = true,              // Whether this is COUNT or just DISTINCT (GROUP BY without agg)
  tableSchema: Option[StructType] = None        // Original table schema for proper type conversion
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
            // Use actual field type from table schema if available, otherwise fall back to StringType
            val fieldType = tableSchema.flatMap(_.fields.find(_.name == col)).map(_.dataType).getOrElse(StringType)
            logger.debug(s"TRANSACTION LOG readSchema(): GROUP BY column '$col' has type $fieldType")
            StructField(s"group_col_$idx", fieldType, nullable = true)
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
      hasAggregations,
      tableSchema
    )

  override def description(): String = {
    val groupByDesc = groupByColumns.map(cols => s", groupBy=[${cols.mkString(", ")}]").getOrElse("")
    s"TransactionLogCountScan(path=${transactionLog.getTablePath()}$groupByDesc)"
  }
}

/** Batch implementation for transaction log count. */
class TransactionLogCountBatch(
  sparkSession: SparkSession,
  transactionLog: TransactionLogInterface,
  pushedFilters: Array[Filter],
  options: CaseInsensitiveStringMap,
  config: Map[String, String], // Direct config instead of broadcast
  groupByColumns: Option[Array[String]] = None,
  hasAggregations: Boolean = true,
  tableSchema: Option[StructType] = None)
    extends Batch {

  private val logger = LoggerFactory.getLogger(classOf[TransactionLogCountBatch])

  override def planInputPartitions(): Array[InputPartition] =
    groupByColumns match {
      case Some(cols) =>
        // GROUP BY: Pre-compute grouped counts on driver
        val groupedCounts = computeGroupByCountFromTransactionLog(cols)
        Array(TransactionLogGroupByCountPartition(groupedCounts, cols, tableSchema))
      case None =>
        // Simple COUNT: Pre-compute total count on driver
        val precomputedCount = computeCountFromTransactionLog(transactionLog, pushedFilters)
        Array(TransactionLogCountPartition(precomputedCount))
    }

  /** Compute the count from transaction log on the driver side. */
  private def computeCountFromTransactionLog(transactionLog: TransactionLogInterface, pushedFilters: Array[Filter]): Long =
    if (pushedFilters.isEmpty) {
      // No filters - return total count from transaction log
      transactionLog.getTotalRowCount()
    } else {
      // Get filtered files (native filtering handles partition pruning)
      val partitionFilters = pushedFilters.filter(isPartitionFilter(_, transactionLog)).toSeq
      val matchingFiles = if (partitionFilters.nonEmpty) {
        transactionLog.listFilesWithPartitionFilters(partitionFilters)
      } else {
        transactionLog.listFiles()
      }

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
    // Get filtered files from transaction log (native filtering handles partition pruning)
    val partitionFilters = pushedFilters.filter(isPartitionFilter(_, transactionLog)).toSeq
    val matchingFiles = if (partitionFilters.nonEmpty) {
      transactionLog.listFilesWithPartitionFilters(partitionFilters)
    } else {
      transactionLog.listFiles()
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
  private def isPartitionFilter(filter: Filter, transactionLog: TransactionLogInterface): Boolean = {
    val partitionColumns  = transactionLog.getPartitionColumns()
    val referencedColumns = getFilterReferencedColumns(filter)
    referencedColumns.nonEmpty && referencedColumns.forall(partitionColumns.contains)
  }

  /** Get columns referenced by a filter. */
  private def getFilterReferencedColumns(filter: Filter): Set[String] =
    io.indextables.spark.util.FilterUtils.extractFieldNames(filter)

  // matchesPartitionFilters removed — partition filtering now handled natively via listFilesWithPartitionFilters

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
  groupByColumns: Array[String],
  tableSchema: Option[StructType] = None)
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
      // Convert partition values based on their actual types from the table schema
      val convertedValues = partitionValues.zipWithIndex.map {
        case (v, idx) =>
          val colName = partition.groupByColumns(idx)
          val fieldType = partition.tableSchema
            .flatMap(_.fields.find(_.name == colName))
            .map(_.dataType)
            .getOrElse(StringType)
          convertPartitionValue(v, fieldType, colName)
      }
      // Create row: partition columns + count
      currentRow = InternalRow.fromSeq(convertedValues :+ count)
      true
    } else {
      false
    }

  /**
   * Convert a partition value string to the appropriate Spark internal representation. Handles DateType (days since
   * epoch) and TimestampType (microseconds since epoch).
   */
  private def convertPartitionValue(
    value: String,
    dataType: org.apache.spark.sql.types.DataType,
    columnName: String
  ): Any = {
    import org.apache.spark.unsafe.types.UTF8String
    import org.apache.spark.sql.types._
    import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}

    if (value == null || value.isEmpty) {
      return null
    }

    dataType match {
      case StringType =>
        UTF8String.fromString(value)

      case IntegerType =>
        try
          value.toInt
        catch {
          case e: NumberFormatException =>
            logger.warn(s"Cannot convert partition value '$value' for column '$columnName' to Int: ${e.getMessage}")
            0
        }

      case LongType =>
        try
          value.toLong
        catch {
          case e: NumberFormatException =>
            logger.warn(s"Cannot convert partition value '$value' for column '$columnName' to Long: ${e.getMessage}")
            0L
        }

      case DateType =>
        // Convert date string to days since epoch (Int) as Spark expects
        try {
          val localDate = if (value.contains("T")) {
            // ISO datetime format - extract date part
            LocalDate.parse(value.substring(0, 10))
          } else {
            // Simple date format YYYY-MM-DD
            LocalDate.parse(value)
          }
          localDate.toEpochDay.toInt
        } catch {
          case e: Exception =>
            throw new IllegalArgumentException(
              s"Cannot convert partition value '$value' for column '$columnName' to DateType: ${e.getMessage}",
              e
            )
        }

      case TimestampType =>
        // Convert timestamp string to microseconds since epoch (Long) as Spark expects
        try
          // Check if already numeric (microseconds from tantivy)
          if (value.forall(c => c.isDigit || c == '-')) {
            value.toLong
          } else {
            // Check for URL-encoded strings (partition paths encode colons as %3A)
            val decodedStr = if (value.contains("%")) {
              java.net.URLDecoder.decode(value, "UTF-8")
            } else {
              value
            }

            // Parse based on format
            val instant = if (decodedStr.endsWith("Z")) {
              Instant.parse(decodedStr)
            } else if (decodedStr.contains("T")) {
              LocalDateTime.parse(decodedStr).toInstant(ZoneOffset.UTC)
            } else if (decodedStr.contains(" ")) {
              // Space-separated format: "2024-01-01 15:00:00"
              LocalDateTime.parse(decodedStr.replace(" ", "T")).toInstant(ZoneOffset.UTC)
            } else {
              throw new IllegalArgumentException(s"Unrecognized timestamp format: $value")
            }

            // Convert to microseconds
            instant.getEpochSecond * 1000000L + instant.getNano / 1000L
          }
        catch {
          case e: IllegalArgumentException => throw e
          case e: Exception =>
            throw new IllegalArgumentException(
              s"Cannot convert partition value '$value' for column '$columnName' to TimestampType: ${e.getMessage}",
              e
            )
        }

      case _ =>
        // Default to string for unknown types
        UTF8String.fromString(value)
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    // Nothing to close
  }
}
