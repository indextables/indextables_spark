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

import org.slf4j.LoggerFactory

/**
 * Collects granular timing metrics for the read pipeline on executors.
 *
 * Tracks time spent in each phase:
 *   - Split engine creation (S3 footer fetch + index open)
 *   - Query building (filter conversion to Tantivy AST)
 *   - Streaming session start (initial index lookup / segment open)
 *   - Per-batch retrieval (nextBatch FFI calls — S3 GETs + parquet decode)
 *   - Arrow batch assembly (FFI import + partition column interleave)
 *
 * All durations are in nanoseconds for precision, converted to milliseconds for logging. Callers are responsible for
 * logging the `summary` string — this class does not log directly.
 */
class ReadPipelineMetrics(val splitPath: String) {

  var splitEngineCreationNs: Long   = 0L
  var queryBuildNs: Long            = 0L
  var streamingSessionStartNs: Long = 0L
  var nextBatchTotalNs: Long        = 0L
  var nextBatchCount: Int           = 0
  var batchAssemblyTotalNs: Long    = 0L
  var totalRows: Long               = 0L

  /** Merge another instance's timings into this one (for multi-split aggregation). */
  def mergeFrom(other: ReadPipelineMetrics): Unit = {
    splitEngineCreationNs += other.splitEngineCreationNs
    queryBuildNs += other.queryBuildNs
    streamingSessionStartNs += other.streamingSessionStartNs
    nextBatchTotalNs += other.nextBatchTotalNs
    nextBatchCount += other.nextBatchCount
    batchAssemblyTotalNs += other.batchAssemblyTotalNs
    totalRows += other.totalRows
  }

  def splitEngineCreationMs: Double   = splitEngineCreationNs / 1e6
  def queryBuildMs: Double            = queryBuildNs / 1e6
  def streamingSessionStartMs: Double = streamingSessionStartNs / 1e6
  def nextBatchTotalMs: Double        = nextBatchTotalNs / 1e6
  def batchAssemblyTotalMs: Double    = batchAssemblyTotalNs / 1e6

  def totalTrackedMs: Double =
    splitEngineCreationMs + queryBuildMs + streamingSessionStartMs +
      nextBatchTotalMs + batchAssemblyTotalMs

  def summary: String = {
    val avg = if (nextBatchCount > 0) f" avgBatch=${nextBatchTotalMs / nextBatchCount}%.1fms" else ""
    f"ReadPipelineMetrics path=$splitPath engineCreation=$splitEngineCreationMs%.1fms queryBuild=$queryBuildMs%.1fms " +
      f"streamingStart=$streamingSessionStartMs%.1fms nextBatch=$nextBatchTotalMs%.1fms(${nextBatchCount}batches$avg) " +
      f"batchAssembly=$batchAssemblyTotalMs%.1fms total=$totalTrackedMs%.1fms rows=$totalRows"
  }
}

/**
 * Collects granular timing metrics for driver-side scan planning.
 *
 * Tracks time spent in:
 *   - Native filtering (single JNI call: partition pruning + data skipping + metadata)
 *   - Locality assignment
 *   - Total planInputPartitions wall time
 */
class ScanPlanningMetrics(val tablePath: String) {

  private val logger = LoggerFactory.getLogger(classOf[ScanPlanningMetrics])

  var nativeFilteringNs: Long    = 0L
  var localityAssignmentNs: Long = 0L
  var totalPlanNs: Long          = 0L

  var totalFilesBeforeFiltering: Long = 0L
  var resultFiles: Long               = 0L
  var resultPartitions: Int           = 0

  def nativeFilteringMs: Double    = nativeFilteringNs / 1e6
  def localityAssignmentMs: Double = localityAssignmentNs / 1e6
  def totalPlanMs: Double          = totalPlanNs / 1e6

  def logSummary(): Unit =
    logger.info(
      f"ScanPlanningMetrics table=$tablePath nativeFiltering=$nativeFilteringMs%.1fms($totalFilesBeforeFiltering->${resultFiles}files) " +
        f"locality=$localityAssignmentMs%.1fms totalPlan=$totalPlanMs%.1fms partitions=$resultPartitions"
    )
}
