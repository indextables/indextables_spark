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
   * Extract partition values from a data row based on partition column indices. Returns a map of partition column names
   * to their string representations.
   */
  def extractPartitionValues(
    row: InternalRow,
    schema: StructType,
    partitionColumns: Seq[String]
  ): Map[String, String] = {
    if (partitionColumns.isEmpty) {
      return Map.empty
    }

    val partitionValues = mutable.Map[String, String]()
    val fieldMap        = schema.fields.zipWithIndex.map { case (field, idx) => field.name -> ((field, idx)) }.toMap

    for (partitionCol <- partitionColumns)
      fieldMap.get(partitionCol) match {
        case Some((field, index)) =>
          val value = convertPartitionValue(row, index, field.dataType)
          partitionValues(partitionCol) = value
        case None =>
          logger.warn(s"Partition column '$partitionCol' not found in schema")
          partitionValues(partitionCol) = null
      }

    partitionValues.toMap
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
