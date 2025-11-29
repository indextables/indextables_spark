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

package io.indextables.spark.util

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

object StatisticsCalculator {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /** Data types that are eligible for statistics collection (have meaningful min/max ordering). */
  val ELIGIBLE_DATA_TYPES: Set[DataType] = Set(
    IntegerType, LongType, FloatType, DoubleType,
    BooleanType, StringType, DateType, TimestampType
  )

  /**
   * Check if a field is eligible for statistics collection.
   * Only primitive types with meaningful ordering are eligible.
   */
  def isEligibleForStats(field: StructField): Boolean =
    ELIGIBLE_DATA_TYPES.contains(field.dataType)

  /**
   * Filter schema fields to only include those eligible for statistics collection,
   * applying the configured column limits.
   *
   * Priority:
   * 1. If dataSkippingStatsColumns is set, use only those columns (intersection with eligible)
   * 2. Otherwise, take first N eligible columns where N = dataSkippingNumIndexedCols (-1 = all)
   *
   * @param schema The full schema
   * @param config Configuration map
   * @return Sequence of (field, originalIndex) tuples for columns to collect stats for
   */
  def getStatsEligibleFields(
    schema: StructType,
    config: Map[String, String]
  ): Seq[(StructField, Int)] = {
    // Get all eligible fields with their original indices
    val eligibleFields = schema.fields.zipWithIndex.filter { case (field, _) =>
      isEligibleForStats(field)
    }

    // Check if explicit column list is configured
    val explicitColumns = ConfigUtils.getDataSkippingStatsColumns(config)

    explicitColumns match {
      case Some(columnSet) =>
        // Use explicit column list - filter to only include specified columns that are eligible
        val filtered = eligibleFields.filter { case (field, _) =>
          columnSet.contains(field.name)
        }
        if (filtered.size < columnSet.size) {
          val eligibleNames = eligibleFields.map(_._1.name).toSet
          val invalidColumns = columnSet.diff(eligibleNames)
          if (invalidColumns.nonEmpty) {
            logger.warn(s"dataSkippingStatsColumns contains columns not eligible for stats " +
              s"(non-existent or unsupported type): ${invalidColumns.mkString(", ")}")
          }
        }
        logger.debug(s"Using explicit dataSkippingStatsColumns: ${filtered.map(_._1.name).mkString(", ")}")
        filtered.toSeq

      case None =>
        // Use numIndexedCols to limit number of columns
        val numIndexedCols = ConfigUtils.getDataSkippingNumIndexedCols(config)

        if (numIndexedCols < 0) {
          // -1 means all eligible columns
          logger.debug(s"Collecting stats for all ${eligibleFields.length} eligible columns")
          eligibleFields.toSeq
        } else if (numIndexedCols == 0) {
          // 0 means no stats collection
          logger.debug("Stats collection disabled (dataSkippingNumIndexedCols=0)")
          Seq.empty
        } else {
          // Take first N eligible columns
          val limited = eligibleFields.take(numIndexedCols)
          if (eligibleFields.length > numIndexedCols) {
            logger.debug(s"Limiting stats collection to first $numIndexedCols of ${eligibleFields.length} " +
              s"eligible columns: ${limited.map(_._1.name).mkString(", ")}")
          }
          limited.toSeq
        }
    }
  }

  class ColumnStatistics(dataType: DataType) {
    private var minValue: Any = null
    private var maxValue: Any = null
    private var hasValues     = false

    def update(value: Any, dataType: DataType): Unit =
      if (value != null) {
        if (!hasValues) {
          minValue = value
          maxValue = value
          hasValues = true
        } else {
          if (compareValues(value, minValue, dataType) < 0) {
            minValue = value
          }
          if (compareValues(value, maxValue, dataType) > 0) {
            maxValue = value
          }
        }
      }

    def getMin: Option[String] = Option(minValue).map(convertToString)
    def getMax: Option[String] = Option(maxValue).map(convertToString)

    private def convertToString(value: Any): String =
      dataType match {
        case DateType =>
          // Convert days since epoch to ISO date string
          value match {
            case days: Int  => java.time.LocalDate.ofEpochDay(days.toLong).toString
            case days: Long => java.time.LocalDate.ofEpochDay(days).toString
            case _          => value.toString
          }
        case _ => value.toString
      }

    private def compareValues(
      v1: Any,
      v2: Any,
      dataType: DataType
    ): Int =
      (v1, v2, dataType) match {
        case (a: Int, b: Int, IntegerType)         => a.compareTo(b)
        case (a: Long, b: Long, LongType)          => a.compareTo(b)
        case (a: Float, b: Float, FloatType)       => a.compareTo(b)
        case (a: Double, b: Double, DoubleType)    => a.compareTo(b)
        case (a: Boolean, b: Boolean, BooleanType) => a.compareTo(b)
        case (a: String, b: String, StringType)    => a.compareTo(b)
        case (a: Int, b: Int, DateType)            => a.compareTo(b) // DateType stores days since epoch as Int
        case (a: Long, b: Long, TimestampType) => a.compareTo(b) // TimestampType stores microseconds since epoch as Long
        case _ => 0 // Default to equal for unsupported types
      }
  }

  /**
   * Dataset statistics collector that respects column filtering configuration.
   *
   * @param schema Full schema of the dataset
   * @param config Configuration map for dataSkippingStatsColumns and dataSkippingNumIndexedCols
   */
  class DatasetStatistics(schema: StructType, config: Map[String, String] = Map.empty) {
    private val columnStats = mutable.Map[String, ColumnStatistics]()

    // Get the fields we should collect stats for, with their original indices
    private val statsFields: Seq[(StructField, Int)] = getStatsEligibleFields(schema, config)

    // Initialize column statistics only for eligible fields
    statsFields.foreach { case (field, _) =>
      columnStats(field.name) = new ColumnStatistics(field.dataType)
    }

    def updateRow(row: InternalRow): Unit =
      statsFields.foreach { case (field, index) =>
        if (!row.isNullAt(index)) {
          val value = extractValue(row, index, field.dataType)
          columnStats(field.name).update(value, field.dataType)
        }
      }

    def getMinValues: Map[String, String] =
      columnStats.flatMap {
        case (columnName, stats) =>
          stats.getMin.map(columnName -> _)
      }.toMap

    def getMaxValues: Map[String, String] =
      columnStats.flatMap {
        case (columnName, stats) =>
          stats.getMax.map(columnName -> _)
      }.toMap

    /** Returns the names of columns being tracked for statistics. */
    def getTrackedColumns: Set[String] = columnStats.keySet.toSet

    private def extractValue(
      row: InternalRow,
      index: Int,
      dataType: DataType
    ): Any =
      try
        dataType match {
          case StringType => row.getUTF8String(index).toString
          case IntegerType =>
            try
              row.getInt(index)
            catch {
              case _: ClassCastException =>
                val value = row.get(index, dataType)
                if (value.isInstanceOf[java.lang.Long]) {
                  value.asInstanceOf[java.lang.Long].intValue()
                } else {
                  value
                }
            }
          case LongType =>
            try
              row.getLong(index)
            catch {
              case _: ClassCastException =>
                val value = row.get(index, dataType)
                if (value.isInstanceOf[java.lang.Integer]) {
                  value.asInstanceOf[java.lang.Integer].longValue()
                } else {
                  value
                }
            }
          case FloatType   => row.getFloat(index)
          case DoubleType  => row.getDouble(index)
          case BooleanType => row.getBoolean(index)
          case TimestampType =>
            try
              row.getLong(index)
            catch {
              case _: ClassCastException =>
                val value = row.get(index, dataType)
                if (value.isInstanceOf[java.lang.Integer]) {
                  value.asInstanceOf[java.lang.Integer].longValue()
                } else {
                  value
                }
            }
          case DateType =>
            try
              row.getInt(index)
            catch {
              case _: ClassCastException =>
                val value = row.get(index, dataType)
                if (value.isInstanceOf[java.lang.Long]) {
                  value.asInstanceOf[java.lang.Long].intValue()
                } else {
                  value
                }
            }
          case _ => row.get(index, dataType)
        }
      catch {
        case _: Exception =>
          // Fallback to generic extraction if any casting fails
          row.get(index, dataType)
      }
  }
}
