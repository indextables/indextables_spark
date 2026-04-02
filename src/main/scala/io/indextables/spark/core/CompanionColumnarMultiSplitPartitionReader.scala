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

import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.hadoop.fs.Path

import io.indextables.spark.metrics.{ReadPipelineMetrics, TaskSplitEngineCreationTime, TaskQueryBuildTime, TaskStreamingSessionStartTime, TaskNextBatchTime, TaskBatchAssemblyTime}
import io.indextables.spark.transaction.AddAction
import org.slf4j.LoggerFactory

/**
 * Columnar partition reader that processes multiple splits sequentially. Stops early if pushed limit is satisfied
 * before querying all splits.
 *
 * Works for all split types (both companion and regular splits).
 */
class ColumnarMultiSplitPartitionReader(
  addActions: Seq[AddAction],
  readSchema: StructType,
  fullTableSchema: StructType,
  filters: Array[Filter],
  limit: Option[Int] = None,
  config: Map[String, String],
  tablePath: Path,
  indexQueryFilters: Array[Any] = Array.empty,
  metricsAccumulator: Option[io.indextables.spark.storage.BatchOptimizationMetricsAccumulator] = None)
    extends PartitionReader[ColumnarBatch] {

  private val logger = LoggerFactory.getLogger(classOf[ColumnarMultiSplitPartitionReader])

  private val effectiveLimit: Int = SplitReaderContext.computeEffectiveLimit(config, limit)

  // Multi-split iteration state
  private var currentSplitIndex                              = 0
  private var currentReader: Option[ColumnarPartitionReader] = None
  private var totalRowsReturned                              = 0L
  private var initialized                                    = false

  // Capture baseline metrics
  private val baselineMetrics: io.indextables.spark.storage.BatchOptMetrics =
    if (metricsAccumulator.isDefined) io.indextables.spark.storage.BatchOptMetrics.fromJavaMetrics()
    else io.indextables.spark.storage.BatchOptMetrics.empty

  private val aggregateMetrics = new ReadPipelineMetrics(s"multi-split[${addActions.length}]")

  logger.info(
    s"ColumnarMultiSplitPartitionReader created with ${addActions.length} splits, effectiveLimit=$effectiveLimit"
  )

  // Track pending row count from the last get() call, applied at the start of next()
  // to avoid double-counting if get() is called multiple times per next().
  private var pendingRowCount = 0

  override def next(): Boolean = {
    // Accumulate rows from the previous batch (set by get()) at the start of next(),
    // ensuring double-calls to get() don't inflate totalRowsReturned.
    totalRowsReturned += pendingRowCount
    pendingRowCount = 0

    if (!initialized) {
      initialized = true
      logger.debug(s"ColumnarMultiSplitPartitionReader: initializing with ${addActions.length} splits")
    }

    // Check if current reader has more rows
    if (currentReader.exists(_.next())) {
      return true
    }

    // Close current reader before moving to next
    closeCurrentReader()

    // Check if we've satisfied the limit (safe Long subtraction avoids Int overflow)
    val remainingLimit = math.max(0L, effectiveLimit.toLong - totalRowsReturned).toInt
    if (remainingLimit <= 0) {
      logger.debug(
        s"ColumnarMultiSplitPartitionReader: limit satisfied ($totalRowsReturned >= $effectiveLimit), " +
          s"skipping remaining ${addActions.length - currentSplitIndex} splits"
      )
      return false
    }

    // Move to next split
    while (currentSplitIndex < addActions.length) {
      val addAction = addActions(currentSplitIndex)
      currentSplitIndex += 1

      logger.debug(
        s"ColumnarMultiSplitPartitionReader: initializing split $currentSplitIndex/${addActions.length}: ${addAction.path}"
      )

      // Pass remaining limit to child for per-split early termination via streaming.
      // Pass None for metricsAccumulator — the multi-split reader reports cumulative metrics
      // from its own baseline to avoid double-counting.
      val singleSplitReader = new ColumnarPartitionReader(
        addAction,
        readSchema,
        fullTableSchema,
        filters,
        Some(remainingLimit),
        config,
        tablePath,
        indexQueryFilters,
        metricsAccumulator = None
      )

      currentReader = Some(singleSplitReader)

      if (singleSplitReader.next()) {
        return true
      }

      // This split had no results, close and try next
      closeCurrentReader()
    }

    false
  }

  override def get(): ColumnarBatch =
    currentReader match {
      case Some(reader) =>
        val batch = reader.get()
        pendingRowCount = batch.numRows()
        batch
      case None =>
        throw new IllegalStateException("get() called without successful next()")
    }

  private def closeCurrentReader(): Unit = {
    currentReader.foreach { reader =>
      aggregateMetrics.mergeFrom(reader.getReadPipelineMetrics)
      try reader.close()
      catch {
        case ex: Exception =>
          logger.warn(s"Error closing columnar split reader: ${ex.getMessage}")
      }
    }
    currentReader = None
  }

  override def close(): Unit = {
    closeCurrentReader()

    // Collect batch optimization metrics delta for all splits combined
    metricsAccumulator.foreach { acc =>
      try {
        val currentMetrics = io.indextables.spark.storage.BatchOptMetrics.fromJavaMetrics()
        val delta = io.indextables.spark.storage.BatchOptMetrics(
          totalOperations = currentMetrics.totalOperations - baselineMetrics.totalOperations,
          totalDocuments = currentMetrics.totalDocuments - baselineMetrics.totalDocuments,
          totalRequests = currentMetrics.totalRequests - baselineMetrics.totalRequests,
          consolidatedRequests = currentMetrics.consolidatedRequests - baselineMetrics.consolidatedRequests,
          bytesTransferred = currentMetrics.bytesTransferred - baselineMetrics.bytesTransferred,
          bytesWasted = currentMetrics.bytesWasted - baselineMetrics.bytesWasted,
          totalPrefetchDurationMs = currentMetrics.totalPrefetchDurationMs - baselineMetrics.totalPrefetchDurationMs,
          segmentsProcessed = currentMetrics.segmentsProcessed - baselineMetrics.segmentsProcessed
        )
        if (delta.totalOperations > 0 || delta.totalDocuments > 0) {
          acc.add(delta)
        }
      } catch {
        case ex: Exception =>
          logger.warn("Error collecting batch optimization metrics for columnar multi-split partition", ex)
      }
    }

    // Report bytesRead to Spark UI for all processed splits
    val bytesRead = addActions.take(currentSplitIndex).map(_.size).sum
    org.apache.spark.sql.indextables.OutputMetricsUpdater.incInputMetrics(bytesRead, 0)

    logger.info(aggregateMetrics.summary)
    logger.info(
      s"ColumnarMultiSplitPartitionReader closed: processed $currentSplitIndex/${addActions.length} splits, returned $totalRowsReturned rows"
    )
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = Array(
    new TaskSplitEngineCreationTime(aggregateMetrics.splitEngineCreationMs.toLong),
    new TaskQueryBuildTime(aggregateMetrics.queryBuildMs.toLong),
    new TaskStreamingSessionStartTime(aggregateMetrics.streamingSessionStartMs.toLong),
    new TaskNextBatchTime(aggregateMetrics.nextBatchTotalMs.toLong),
    new TaskBatchAssemblyTime(aggregateMetrics.batchAssemblyTotalMs.toLong)
  )

  def getReadPipelineMetrics: ReadPipelineMetrics = aggregateMetrics
}
