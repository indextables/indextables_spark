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

package io.indextables.spark.transaction

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import org.slf4j.LoggerFactory

/**
 * Utilities for handling partition operations similar to Delta Lake. Provides functionality for extracting partition
 * values from data rows and managing partition metadata.
 */
object PartitionUtils {

  private val logger = LoggerFactory.getLogger(PartitionUtils.getClass)

  /**
   * Precomputed partition column information for efficient extraction. This avoids creating a schema field map on every
   * row by caching the column indices and data types once.
   *
   * @param columns
   *   Array of (columnName, schemaIndex, dataType) tuples for partition columns
   */
  case class PartitionColumnInfo(columns: Array[(String, Int, DataType)]) {
    def isEmpty: Boolean  = columns.isEmpty
    def nonEmpty: Boolean = columns.nonEmpty
  }

  /**
   * Precompute partition column indices and types from schema. Call this once per task/writer, then reuse the result
   * for all rows. This provides O(1) per-row extraction instead of O(schema.size).
   *
   * @param schema
   *   The data schema
   * @param partitionColumns
   *   Partition column names
   * @return
   *   Precomputed partition column info for use with extractPartitionValuesFast
   */
  def precomputePartitionInfo(
    schema: StructType,
    partitionColumns: Seq[String]
  ): PartitionColumnInfo = {
    if (partitionColumns.isEmpty) {
      return PartitionColumnInfo(Array.empty)
    }

    // Build field map once
    val fieldMap = schema.fields.zipWithIndex.map { case (field, idx) => field.name -> ((field, idx)) }.toMap

    val columns = partitionColumns.map { partitionCol =>
      fieldMap.get(partitionCol) match {
        case Some((field, index)) =>
          (partitionCol, index, field.dataType)
        case None =>
          throw new IllegalArgumentException(
            s"Partition column '$partitionCol' not found in schema. " +
              s"Available columns: ${schema.fieldNames.mkString(", ")}"
          )
      }
    }.toArray

    PartitionColumnInfo(columns)
  }

  /**
   * Fast extraction of partition values using precomputed column info. This is O(partitionColumns.size) per row instead
   * of O(schema.size).
   *
   * @param row
   *   The data row
   * @param partitionInfo
   *   Precomputed partition column info from precomputePartitionInfo
   * @return
   *   Map of partition column names to string values
   */
  def extractPartitionValuesFast(
    row: InternalRow,
    partitionInfo: PartitionColumnInfo
  ): Map[String, String] = {
    if (partitionInfo.isEmpty) {
      return Map.empty
    }

    val partitionValues = mutable.Map[String, String]()
    val columns         = partitionInfo.columns
    var i               = 0
    while (i < columns.length) {
      val (colName, index, dataType) = columns(i)
      val value                      = convertPartitionValue(row, index, dataType)
      partitionValues(colName) = value
      i += 1
    }

    partitionValues.toMap
  }

  /**
   * Extract partition values from a data row based on partition column indices. Returns a map of partition column names
   * to their string representations.
   *
   * NOTE: For high-performance scenarios (many rows), prefer precomputePartitionInfo + extractPartitionValuesFast to
   * avoid rebuilding the schema field map on every row.
   */
  def extractPartitionValues(
    row: InternalRow,
    schema: StructType,
    partitionColumns: Seq[String]
  ): Map[String, String] = {
    if (partitionColumns.isEmpty) {
      return Map.empty
    }

    // Delegate to fast path by building partition info
    // Note: For single calls this is fine; for loops, use precomputePartitionInfo
    val partitionInfo = precomputePartitionInfo(schema, partitionColumns)
    extractPartitionValuesFast(row, partitionInfo)
  }

  /**
   * Convert a partition value to its string representation for storage in transaction log. This follows Spark's
   * partition path encoding rules.
   */
  private def convertPartitionValue(
    row: InternalRow,
    index: Int,
    dataType: DataType
  ): String = {
    if (row.isNullAt(index)) {
      return null
    }

    dataType match {
      case StringType  => row.getUTF8String(index).toString
      case IntegerType => row.getInt(index).toString
      case LongType    => row.getLong(index).toString
      case FloatType   => row.getFloat(index).toString
      case DoubleType  => row.getDouble(index).toString
      case BooleanType => row.getBoolean(index).toString
      case DateType    =>
        // Convert date to string representation
        val days = row.getInt(index)
        java.time.LocalDate.ofEpochDay(days.toLong).toString
      case TimestampType =>
        // Convert timestamp to string representation
        val micros = row.getLong(index)
        java.time.Instant.ofEpochSecond(micros / 1000000, (micros % 1000000) * 1000).toString
      case dt: DecimalType =>
        val decimal = row.getDecimal(index, dt.precision, dt.scale)
        decimal.toJavaBigDecimal.toString
      case _ =>
        logger.warn(s"Unsupported partition column type: $dataType")
        row.get(index, dataType).toString
    }
  }

  /** Validate that partition columns exist in the schema and are of supported types. */
  def validatePartitionColumns(schema: StructType, partitionColumns: Seq[String]): Unit = {
    val schemaFields = schema.fieldNames.toSet
    val fieldMap     = schema.fields.map(f => f.name -> f.dataType).toMap

    // Check if all partition columns exist in schema
    val missingColumns = partitionColumns.filterNot(schemaFields.contains)
    if (missingColumns.nonEmpty) {
      throw new IllegalArgumentException(s"Partition columns not found in schema: ${missingColumns.mkString(", ")}")
    }

    // Check if partition columns are of supported types
    val unsupportedColumns = partitionColumns.filter { colName =>
      fieldMap.get(colName) match {
        case Some(dataType) => !isSupportedPartitionType(dataType)
        case None           => false // Already caught above
      }
    }

    if (unsupportedColumns.nonEmpty) {
      throw new IllegalArgumentException(s"Unsupported partition column types: ${unsupportedColumns.mkString(", ")}")
    }
  }

  /** Check if a data type is supported for partitioning. */
  def isSupportedPartitionType(dataType: DataType): Boolean =
    dataType match {
      case StringType | IntegerType | LongType | FloatType | DoubleType | BooleanType | DateType | TimestampType |
          _: DecimalType =>
        true
      case _ => false
    }

  /**
   * Extract unique partition values from a set of rows for a given partition column. This is useful for understanding
   * partition distribution.
   */
  def getUniquePartitionValues(
    rows: Seq[InternalRow],
    schema: StructType,
    partitionColumn: String
  ): Set[String] = {
    val fieldIndex = schema.fieldIndex(partitionColumn)
    val field      = schema(fieldIndex)

    rows.map(row => convertPartitionValue(row, fieldIndex, field.dataType)).toSet.filter(_ != null)
  }

  /** Create a partition path string from partition values. Format: col1=value1/col2=value2/... */
  def createPartitionPath(partitionValues: Map[String, String], partitionColumns: Seq[String]): String =
    partitionColumns
      .map { col =>
        val value        = partitionValues.getOrElse(col, null)
        val encodedValue = if (value == null) "__HIVE_DEFAULT_PARTITION__" else escapePathName(value)
        s"$col=$encodedValue"
      }
      .mkString("/")

  /** Escape special characters in partition values for file system paths. This follows Hive's path encoding rules. */
  private def escapePathName(path: String): String =
    if (path == null || path.isEmpty) {
      "__HIVE_DEFAULT_PARTITION__"
    } else {
      // Simple escaping - replace common problematic characters
      path
        .replace("/", "%2F")
        .replace("\\", "%5C")
        .replace(":", "%3A")
        .replace("*", "%2A")
        .replace("?", "%3F")
        .replace("\"", "%22")
        .replace("<", "%3C")
        .replace(">", "%3E")
        .replace("|", "%7C")
        .replace(" ", "%20")
    }
}
