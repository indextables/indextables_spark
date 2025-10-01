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

import org.apache.spark.sql.connector.read.Statistics
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics
import java.util.{OptionalLong, Map => JavaMap, HashMap => JavaHashMap, Optional}
import io.indextables.spark.transaction.AddAction

/**
 * Implementation of Spark's Statistics interface for IndexTables4Spark. Provides table-level statistics including size in
 * bytes, number of rows, and column-level statistics where available.
 */
class IndexTables4SparkStatistics(
  totalSizeInBytes: Option[Long],
  totalNumRows: Option[Long],
  columnStats: Map[String, ColumnStatistics] = Map.empty)
    extends Statistics {

  override def sizeInBytes(): OptionalLong =
    totalSizeInBytes match {
      case Some(size) => OptionalLong.of(size)
      case None       => OptionalLong.empty()
    }

  override def numRows(): OptionalLong =
    totalNumRows match {
      case Some(rows) => OptionalLong.of(rows)
      case None       => OptionalLong.empty()
    }

  override def columnStats(): JavaMap[NamedReference, ColumnStatistics] = {
    val javaMap = new JavaHashMap[NamedReference, ColumnStatistics]()
    columnStats.foreach {
      case (columnName, stats) =>
        val namedRef = org.apache.spark.sql.connector.expressions.Expressions.column(columnName)
        javaMap.put(namedRef, stats)
    }
    javaMap
  }
}

object IndexTables4SparkStatistics {

  /** Simple column statistics implementation for basic min/max and null count information. */
  private class SimpleColumnStatistics(
    nullCount: Option[Long],
    distinctCount: Option[Long],
    minValue: Option[Any] = None,
    maxValue: Option[Any] = None)
      extends ColumnStatistics {

    override def nullCount(): OptionalLong =
      nullCount match {
        case Some(count) => OptionalLong.of(count)
        case None        => OptionalLong.empty()
      }

    override def distinctCount(): OptionalLong =
      distinctCount match {
        case Some(count) => OptionalLong.of(count)
        case None        => OptionalLong.empty()
      }

    override def min(): Optional[Object] =
      minValue match {
        case Some(value) => Optional.of(value.asInstanceOf[Object])
        case None        => Optional.empty()
      }

    override def max(): Optional[Object] =
      maxValue match {
        case Some(value) => Optional.of(value.asInstanceOf[Object])
        case None        => Optional.empty()
      }
  }

  /**
   * Creates statistics from a collection of AddAction entries from the transaction log. This aggregates information
   * from all splits to provide table-level statistics.
   */
  def fromAddActions(addActions: Seq[AddAction]): IndexTables4SparkStatistics = {
    if (addActions.isEmpty) {
      return new IndexTables4SparkStatistics(Some(0L), Some(0L))
    }

    // Aggregate size information
    val totalSize = addActions.map(_.size).sum

    // Aggregate row count information - handle type conversion safely using helper function
    val totalRows = addActions.flatMap(_.numRecords).map(convertToLong).foldLeft(0L)(_ + _)

    // Aggregate column-level statistics from min/max values in AddActions
    val columnStats = aggregateColumnStatistics(addActions, totalRows)

    new IndexTables4SparkStatistics(
      totalSizeInBytes = if (totalSize > 0) Some(totalSize) else None,
      totalNumRows = if (totalRows > 0) Some(totalRows) else None,
      columnStats = columnStats
    )
  }

  /** Helper function to safely convert various numeric types to Long. */
  private def convertToLong(value: Any): Long =
    value match {
      case l: Long              => l
      case i: Int               => i.toLong
      case i: java.lang.Integer => i.toLong
      case l: java.lang.Long    => l.longValue()
      case other                => other.toString.toLong
    }

  /** Aggregates column statistics from AddAction min/max values. */
  private def aggregateColumnStatistics(addActions: Seq[AddAction], totalRows: Long): Map[String, ColumnStatistics] = {
    val columnStatsBuilder = scala.collection.mutable.Map[String, ColumnStatistics]()

    // Collect all column names that have min/max values
    val allColumnNames = addActions.flatMap { action =>
      action.minValues.getOrElse(Map.empty).keys ++ action.maxValues.getOrElse(Map.empty).keys
    }.toSet

    allColumnNames.foreach { columnName =>
      try {
        // Extract min/max values for this column across all files
        val minValues = addActions.flatMap(_.minValues.getOrElse(Map.empty).get(columnName)).filter(_.nonEmpty)
        val maxValues = addActions.flatMap(_.maxValues.getOrElse(Map.empty).get(columnName)).filter(_.nonEmpty)

        if (minValues.nonEmpty && maxValues.nonEmpty) {
          // For strings, use lexicographic ordering; for numbers, parse and compare
          val globalMin = minValues.min
          val globalMax = maxValues.max

          // We don't have null count or distinct count information in AddActions,
          // so we'll provide basic min/max statistics only
          val colStats = new SimpleColumnStatistics(
            nullCount = None,     // Not available from transaction log
            distinctCount = None, // Not available from transaction log
            minValue = Some(globalMin),
            maxValue = Some(globalMax)
          )

          columnStatsBuilder(columnName) = colStats
        }
      } catch {
        case _: Exception =>
        // Skip columns that can't be processed (e.g., incomparable types)
      }
    }

    columnStatsBuilder.toMap
  }

  /** Creates statistics for an empty table. */
  def empty(): IndexTables4SparkStatistics =
    new IndexTables4SparkStatistics(Some(0L), Some(0L))

  /** Creates statistics with unknown values. Used when statistics cannot be computed reliably. */
  def unknown(): IndexTables4SparkStatistics =
    new IndexTables4SparkStatistics(None, None)
}
