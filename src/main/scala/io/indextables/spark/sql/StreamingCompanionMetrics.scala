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

package io.indextables.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

/**
 * Spark UI accumulators for streaming companion sync observability.
 *
 * Registered with the SparkContext so values are visible in the Spark UI. All metrics are
 * cumulative across the lifetime of a streaming sync session.
 *
 * Accumulators:
 *   - syncCycles: number of completed sync cycles (successful or no-op)
 *   - totalFilesIndexed: total parquet files indexed across all cycles
 *   - totalDurationMs: total time spent in sync execution across all cycles
 *   - errorCount: total number of failed cycles
 */
private[sql] class StreamingCompanionMetrics(sparkContext: SparkContext) {

  val syncCycles: LongAccumulator =
    sparkContext.longAccumulator("indextables.companion.streaming.syncCycles")

  val totalFilesIndexed: LongAccumulator =
    sparkContext.longAccumulator("indextables.companion.streaming.totalFilesIndexed")

  val totalDurationMs: LongAccumulator =
    sparkContext.longAccumulator("indextables.companion.streaming.totalDurationMs")

  val errorCount: LongAccumulator =
    sparkContext.longAccumulator("indextables.companion.streaming.errorCount")

  def recordCycleSuccess(filesIndexed: Long, durationMs: Long): Unit = {
    syncCycles.add(1)
    totalFilesIndexed.add(filesIndexed)
    totalDurationMs.add(durationMs)
  }

  def recordCycleError(): Unit =
    errorCount.add(1)
}
