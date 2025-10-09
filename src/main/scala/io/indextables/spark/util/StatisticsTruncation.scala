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

import org.slf4j.LoggerFactory

/**
 * Utility object for dropping statistics for columns with excessively long values.
 *
 * This prevents transaction log bloat by automatically removing min/max statistics for columns
 * with values exceeding a configurable threshold (default: 256 characters).
 *
 * Note: Statistics are completely dropped (not truncated) to avoid creating misleading partial data
 * that could lead to incorrect query results.
 */
object StatisticsTruncation {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Drops statistics for columns with excessively long values.
   *
   * @param minValues Map of column names to minimum values
   * @param maxValues Map of column names to maximum values
   * @param config Configuration map
   * @return Tuple of (filtered minValues, filtered maxValues)
   */
  def truncateStatistics(
    minValues: Map[String, String],
    maxValues: Map[String, String],
    config: Map[String, String]
  ): (Map[String, String], Map[String, String]) = {

    val truncationEnabled = ConfigUtils.getBoolean(
      config,
      ConfigUtils.STATS_TRUNCATION_ENABLED,
      ConfigUtils.DEFAULT_STATS_TRUNCATION_ENABLED
    )

    if (!truncationEnabled) {
      return (minValues, maxValues)
    }

    val maxLength = ConfigUtils.getInt(
      config,
      ConfigUtils.STATS_TRUNCATION_MAX_LENGTH,
      ConfigUtils.DEFAULT_STATS_TRUNCATION_MAX_LENGTH
    )

    // Find columns that need truncation (checking both min and max values)
    val minValuesToDrop = minValues.filter { case (colName, value) =>
      value != null && value.length > maxLength
    }.keySet

    val maxValuesToDrop = maxValues.filter { case (colName, value) =>
      value != null && value.length > maxLength
    }.keySet

    val columnsToDrop = minValuesToDrop ++ maxValuesToDrop

    if (columnsToDrop.isEmpty) {
      return (minValues, maxValues)
    }

    // Log truncation action
    columnsToDrop.foreach { colName =>
      val minLen = minValues.get(colName).map(_.length).getOrElse(0)
      val maxLen = maxValues.get(colName).map(_.length).getOrElse(0)
      logger.info(s"Dropping statistics for column '$colName' due to excessive value length " +
                  s"(min: $minLen chars, max: $maxLen chars, threshold: $maxLength)")
    }

    // Drop statistics for columns with long values
    val filteredMinValues = minValues.filterKeys(!columnsToDrop.contains(_))
    val filteredMaxValues = maxValues.filterKeys(!columnsToDrop.contains(_))

    (filteredMinValues, filteredMaxValues)
  }
}
