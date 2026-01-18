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

package io.indextables.spark.metrics

import java.text.NumberFormat
import java.util.Locale

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomSumMetric, CustomTaskMetric}

// ============================================================================
// CustomMetric implementations (aggregate metrics reported in Spark UI)
// ============================================================================

/**
 * CustomMetric for total files considered before any skipping.
 * This is reported from driver-side planning and shows in Spark UI.
 */
class TotalFilesConsidered extends CustomSumMetric {
  override def name(): String = TotalFilesConsidered.NAME
  override def description(): String = "total files considered"
}

object TotalFilesConsidered {
  val NAME = "totalFilesConsidered"
}

/**
 * CustomMetric for files pruned by partition filtering.
 * Shows how many files were eliminated by partition predicates.
 */
class PartitionPrunedFiles extends CustomSumMetric {
  override def name(): String = PartitionPrunedFiles.NAME
  override def description(): String = "files pruned by partitions"
}

object PartitionPrunedFiles {
  val NAME = "partitionPrunedFiles"
}

/**
 * CustomMetric for files skipped by data skipping (min/max statistics).
 * Shows how many files were eliminated by column statistics after partition pruning.
 */
class DataSkippedFiles extends CustomSumMetric {
  override def name(): String = DataSkippedFiles.NAME
  override def description(): String = "files skipped by stats"
}

object DataSkippedFiles {
  val NAME = "dataSkippedFiles"
}

/**
 * CustomMetric for result files that survived all skipping.
 * These are the files that will actually be scanned.
 */
class ResultFiles extends CustomSumMetric {
  override def name(): String = ResultFiles.NAME
  override def description(): String = "files to scan"
}

object ResultFiles {
  val NAME = "resultFiles"
}

/**
 * CustomMetric for total skip rate (percentage of files skipped).
 * Uses max aggregation since this is a driver-only metric.
 */
class TotalSkipRate extends CustomMetric {
  override def name(): String = TotalSkipRate.NAME
  override def description(): String = "total skip rate"

  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    // Task metrics are encoded as skip rate * 10000 (for 2 decimal precision)
    // We take the max since this is a driver-only metric
    val maxRate = if (taskMetrics.nonEmpty) taskMetrics.max else 0L
    val rate = maxRate / 10000.0
    val fmt = NumberFormat.getPercentInstance(Locale.ROOT)
    fmt.setMinimumFractionDigits(1)
    fmt.setMaximumFractionDigits(1)
    fmt.format(rate)
  }
}

object TotalSkipRate {
  val NAME = "totalSkipRate"
}

// ============================================================================
// CustomTaskMetric implementations (per-task metrics)
// ============================================================================

/**
 * Per-task metric for total files considered.
 * Reported from driver-side planning via reportDriverMetrics().
 */
class TaskTotalFilesConsidered(val value: Long) extends CustomTaskMetric {
  override def name(): String = TotalFilesConsidered.NAME
}

/**
 * Per-task metric for files pruned by partition filtering.
 * Reported from driver-side planning via reportDriverMetrics().
 */
class TaskPartitionPrunedFiles(val value: Long) extends CustomTaskMetric {
  override def name(): String = PartitionPrunedFiles.NAME
}

/**
 * Per-task metric for files skipped by data skipping (min/max statistics).
 * Reported from driver-side planning via reportDriverMetrics().
 */
class TaskDataSkippedFiles(val value: Long) extends CustomTaskMetric {
  override def name(): String = DataSkippedFiles.NAME
}

/**
 * Per-task metric for result files that survived all skipping.
 * Reported from driver-side planning via reportDriverMetrics().
 */
class TaskResultFiles(val value: Long) extends CustomTaskMetric {
  override def name(): String = ResultFiles.NAME
}

/**
 * Per-task metric for total skip rate.
 * Value is encoded as skip rate * 10000 (for 2 decimal precision).
 * Reported from driver-side planning via reportDriverMetrics().
 */
class TaskTotalSkipRate(encodedValue: Long) extends CustomTaskMetric {
  override def name(): String = TotalSkipRate.NAME
  override def value(): Long = encodedValue

  /**
   * Alternate constructor that takes a skip rate (0.0 to 1.0).
   */
  def this(rate: Double) = this((rate * 10000).toLong)
}
