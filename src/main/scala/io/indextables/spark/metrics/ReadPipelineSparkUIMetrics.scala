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

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomSumMetric, CustomTaskMetric}

// ============================================================================
// Driver-side planning timing metrics (reported via reportDriverMetrics)
// Uses max aggregation since these are single-value driver-only metrics.
// ============================================================================

abstract class CustomMaxMillisMetric extends CustomMetric {
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    val maxVal = if (taskMetrics.nonEmpty) taskMetrics.max else 0L
    s"$maxVal ms"
  }
}

class NativeFilteringTime extends CustomMaxMillisMetric {
  override def name(): String        = NativeFilteringTime.NAME
  override def description(): String = "native filtering time (ms)"
}
object NativeFilteringTime { val NAME = "nativeFilteringTimeMs" }

class ScanPlanTotalTime extends CustomMaxMillisMetric {
  override def name(): String        = ScanPlanTotalTime.NAME
  override def description(): String = "scan planning time (ms)"
}
object ScanPlanTotalTime { val NAME = "scanPlanTotalTimeMs" }

// ============================================================================
// Executor-side per-task timing metrics (aggregated via sum across tasks)
// ============================================================================

class SplitEngineCreationTime extends CustomSumMetric {
  override def name(): String        = SplitEngineCreationTime.NAME
  override def description(): String = "split engine creation time (ms)"
}
object SplitEngineCreationTime { val NAME = "splitEngineCreationTimeMs" }

class QueryBuildTime extends CustomSumMetric {
  override def name(): String        = QueryBuildTime.NAME
  override def description(): String = "query build time (ms)"
}
object QueryBuildTime { val NAME = "queryBuildTimeMs" }

class StreamingSessionStartTime extends CustomSumMetric {
  override def name(): String        = StreamingSessionStartTime.NAME
  override def description(): String = "streaming session start time (ms)"
}
object StreamingSessionStartTime { val NAME = "streamingSessionStartTimeMs" }

class NextBatchTime extends CustomSumMetric {
  override def name(): String        = NextBatchTime.NAME
  override def description(): String = "next batch time (ms)"
}
object NextBatchTime { val NAME = "nextBatchTimeMs" }

class BatchAssemblyTime extends CustomSumMetric {
  override def name(): String        = BatchAssemblyTime.NAME
  override def description(): String = "batch assembly time (ms)"
}
object BatchAssemblyTime { val NAME = "batchAssemblyTimeMs" }

// ============================================================================
// TaskMetric wrappers
// ============================================================================

class TaskNativeFilteringTime(val value: Long) extends CustomTaskMetric {
  override def name(): String = NativeFilteringTime.NAME
}
class TaskScanPlanTotalTime(val value: Long) extends CustomTaskMetric {
  override def name(): String = ScanPlanTotalTime.NAME
}
class TaskSplitEngineCreationTime(val value: Long) extends CustomTaskMetric {
  override def name(): String = SplitEngineCreationTime.NAME
}
class TaskQueryBuildTime(val value: Long) extends CustomTaskMetric { override def name(): String = QueryBuildTime.NAME }
class TaskStreamingSessionStartTime(val value: Long) extends CustomTaskMetric {
  override def name(): String = StreamingSessionStartTime.NAME
}
class TaskNextBatchTime(val value: Long) extends CustomTaskMetric { override def name(): String = NextBatchTime.NAME }
class TaskBatchAssemblyTime(val value: Long) extends CustomTaskMetric {
  override def name(): String = BatchAssemblyTime.NAME
}
