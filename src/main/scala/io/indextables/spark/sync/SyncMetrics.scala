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

package io.indextables.spark.sync

import org.apache.spark.util.AccumulatorV2

/**
 * Metrics collected during a BUILD INDEXTABLES COMPANION operation.
 *
 * Aggregated across all executor tasks via Spark accumulators, then reported
 * in the result rows and Spark UI.
 */
case class SyncMetrics(
  parquetBytesDownloaded: Long = 0,
  splitBytesUploaded: Long = 0,
  parquetFilesIndexed: Int = 0,
  splitsCreated: Int = 0)
    extends Serializable {

  def merge(other: SyncMetrics): SyncMetrics = SyncMetrics(
    parquetBytesDownloaded = this.parquetBytesDownloaded + other.parquetBytesDownloaded,
    splitBytesUploaded = this.splitBytesUploaded + other.splitBytesUploaded,
    parquetFilesIndexed = this.parquetFilesIndexed + other.parquetFilesIndexed,
    splitsCreated = this.splitsCreated + other.splitsCreated
  )
}

object SyncMetrics {
  val zero: SyncMetrics = SyncMetrics()
}

/**
 * Spark accumulator for aggregating SyncMetrics across executor tasks.
 *
 * Register with SparkContext before dispatching sync tasks. Each executor
 * calls `accumulator.add(taskMetrics)`. Driver reads final metrics for
 * result rows and Spark UI.
 */
class SyncMetricsAccumulator extends AccumulatorV2[SyncMetrics, SyncMetrics] {
  private var _metrics: SyncMetrics = SyncMetrics.zero

  override def isZero: Boolean =
    _metrics.parquetBytesDownloaded == 0 &&
      _metrics.splitBytesUploaded == 0 &&
      _metrics.parquetFilesIndexed == 0 &&
      _metrics.splitsCreated == 0

  override def copy(): AccumulatorV2[SyncMetrics, SyncMetrics] = {
    val acc = new SyncMetricsAccumulator()
    acc._metrics = _metrics
    acc
  }

  override def reset(): Unit = _metrics = SyncMetrics.zero

  override def add(v: SyncMetrics): Unit = _metrics = _metrics.merge(v)

  override def merge(other: AccumulatorV2[SyncMetrics, SyncMetrics]): Unit =
    _metrics = _metrics.merge(other.value)

  override def value: SyncMetrics = _metrics
}
