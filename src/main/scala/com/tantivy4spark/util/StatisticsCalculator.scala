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

package com.tantivy4spark.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import scala.collection.mutable

object StatisticsCalculator {
  
  class ColumnStatistics {
    private var minValue: Any = null
    private var maxValue: Any = null
    private var hasValues = false
    
    def update(value: Any, dataType: DataType): Unit = {
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
    }
    
    def getMin: Option[String] = Option(minValue).map(_.toString)
    def getMax: Option[String] = Option(maxValue).map(_.toString)
    
    private def compareValues(v1: Any, v2: Any, dataType: DataType): Int = {
      (v1, v2, dataType) match {
        case (a: Int, b: Int, IntegerType) => a.compareTo(b)
        case (a: Long, b: Long, LongType) => a.compareTo(b)
        case (a: Float, b: Float, FloatType) => a.compareTo(b)
        case (a: Double, b: Double, DoubleType) => a.compareTo(b)
        case (a: Boolean, b: Boolean, BooleanType) => a.compareTo(b)
        case (a: String, b: String, StringType) => a.compareTo(b)
        case _ => 0 // Default to equal for unsupported types
      }
    }
  }
  
  class DatasetStatistics(schema: StructType) {
    private val columnStats = mutable.Map[String, ColumnStatistics]()
    
    schema.fields.foreach { field =>
      columnStats(field.name) = new ColumnStatistics()
    }
    
    def updateRow(row: InternalRow): Unit = {
      schema.fields.zipWithIndex.foreach { case (field, index) =>
        if (!row.isNullAt(index)) {
          val value = extractValue(row, index, field.dataType)
          columnStats(field.name).update(value, field.dataType)
        }
      }
    }
    
    def getMinValues: Map[String, String] = {
      columnStats.flatMap { case (columnName, stats) =>
        stats.getMin.map(columnName -> _)
      }.toMap
    }
    
    def getMaxValues: Map[String, String] = {
      columnStats.flatMap { case (columnName, stats) =>
        stats.getMax.map(columnName -> _)
      }.toMap
    }
    
    private def extractValue(row: InternalRow, index: Int, dataType: DataType): Any = {
      dataType match {
        case StringType => row.getUTF8String(index).toString
        case IntegerType => row.getInt(index)
        case LongType => row.getLong(index)
        case FloatType => row.getFloat(index)
        case DoubleType => row.getDouble(index)
        case BooleanType => row.getBoolean(index)
        case TimestampType => row.getLong(index)
        case DateType => row.getInt(index)
        case _ => row.get(index, dataType)
      }
    }
  }
}