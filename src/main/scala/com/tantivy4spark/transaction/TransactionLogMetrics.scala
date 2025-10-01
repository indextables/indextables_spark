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

package com.tantivy4spark.transaction

import com.codahale.metrics._
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

/**
 * Comprehensive metrics collection for transaction log operations. Provides detailed performance monitoring and
 * diagnostics.
 */
object TransactionLogMetrics {

  private val logger         = LoggerFactory.getLogger(getClass)
  private val metricRegistry = new MetricRegistry()

  // Read performance metrics
  val transactionLogReadLatency: Histogram =
    metricRegistry.histogram("transaction.log.read.latency")

  val checkpointReadLatency: Histogram =
    metricRegistry.histogram("checkpoint.read.latency")

  val parallelReadSpeedup: Gauge[Double] =
    metricRegistry.gauge(
      "parallel.read.speedup",
      () =>
        new Gauge[Double] {
          def getValue: Double = calculateParallelSpeedup()
        }
    )

  // Write performance metrics
  val batchWriteLatency: Histogram =
    metricRegistry.histogram("batch.write.latency")

  val actionsPerSecond: Meter =
    metricRegistry.meter("actions.per.second")

  val checkpointCreationTime: Timer =
    metricRegistry.timer("checkpoint.creation.time")

  // Cache effectiveness metrics
  val cacheHitRate: Gauge[Double] =
    metricRegistry.gauge(
      "cache.hit.rate",
      () =>
        new Gauge[Double] {
          def getValue: Double = calculateCacheHitRate()
        }
    )

  val cacheMissLatency: Histogram =
    metricRegistry.histogram("cache.miss.latency")

  // Thread pool utilization metrics
  val threadPoolUtilization: Gauge[Double] =
    metricRegistry.gauge(
      "thread.pool.utilization",
      () =>
        new Gauge[Double] {
          def getValue: Double = calculateThreadPoolUtilization()
        }
    )

  val queuedTasks: Counter =
    metricRegistry.counter("queued.tasks")

  val rejectedTasks: Counter =
    metricRegistry.counter("rejected.tasks")

  // File operation metrics
  val fileListingLatency: Histogram =
    metricRegistry.histogram("file.listing.latency")

  val filesProcessedPerSecond: Meter =
    metricRegistry.meter("files.processed.per.second")

  val totalFilesListed: Counter =
    metricRegistry.counter("total.files.listed")

  // Checkpoint metrics
  val checkpointSize: Histogram =
    metricRegistry.histogram("checkpoint.size")

  val checkpointsCreated: Counter =
    metricRegistry.counter("checkpoints.created")

  val checkpointsRead: Counter =
    metricRegistry.counter("checkpoints.read")

  // Memory metrics
  val memoryUsage: Gauge[Long] =
    metricRegistry.gauge(
      "memory.usage",
      () =>
        new Gauge[Long] {
          def getValue: Long = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()
        }
    )

  val heapUtilization: Gauge[Double] =
    metricRegistry.gauge(
      "heap.utilization",
      () =>
        new Gauge[Double] {
          def getValue: Double = {
            val runtime = Runtime.getRuntime
            val used    = runtime.totalMemory() - runtime.freeMemory()
            val max     = runtime.maxMemory()
            (used.toDouble / max) * 100
          }
        }
    )

  // Error tracking
  val readErrors: Counter       = metricRegistry.counter("read.errors")
  val writeErrors: Counter      = metricRegistry.counter("write.errors")
  val checkpointErrors: Counter = metricRegistry.counter("checkpoint.errors")

  // Performance tracking
  private val sequentialReadTimes = new AtomicLong(0)
  private val parallelReadTimes   = new AtomicLong(0)
  private val sequentialReadCount = new AtomicInteger(0)
  private val parallelReadCount   = new AtomicInteger(0)

  // Cache statistics tracking
  private val cacheHits   = new AtomicLong(0)
  private val cacheMisses = new AtomicLong(0)

  /** Record a transaction log read operation */
  def recordRead(latencyMs: Long, isParallel: Boolean = false): Unit = {
    transactionLogReadLatency.update(latencyMs)

    if (isParallel) {
      parallelReadTimes.addAndGet(latencyMs)
      parallelReadCount.incrementAndGet()
    } else {
      sequentialReadTimes.addAndGet(latencyMs)
      sequentialReadCount.incrementAndGet()
    }
  }

  /** Record a checkpoint read operation */
  def recordCheckpointRead(latencyMs: Long): Unit = {
    checkpointReadLatency.update(latencyMs)
    checkpointsRead.inc()
  }

  /** Record a batch write operation */
  def recordBatchWrite(latencyMs: Long, actionCount: Int): Unit = {
    batchWriteLatency.update(latencyMs)
    actionsPerSecond.mark(actionCount)
  }

  /** Record checkpoint creation */
  def recordCheckpointCreation(context: Timer.Context, size: Long): Unit = {
    context.stop()
    checkpointSize.update(size)
    checkpointsCreated.inc()
  }

  /** Record cache hit/miss */
  def recordCacheHit(): Unit =
    cacheHits.incrementAndGet()

  def recordCacheMiss(latencyMs: Long): Unit = {
    cacheMisses.incrementAndGet()
    cacheMissLatency.update(latencyMs)
  }

  /** Record file listing operation */
  def recordFileListing(latencyMs: Long, fileCount: Int): Unit = {
    fileListingLatency.update(latencyMs)
    filesProcessedPerSecond.mark(fileCount)
    totalFilesListed.inc(fileCount)
  }

  /** Record thread pool task queueing */
  def recordTaskQueued(): Unit =
    queuedTasks.inc()

  def recordTaskRejected(): Unit =
    rejectedTasks.inc()

  /** Record errors */
  def recordReadError(): Unit =
    readErrors.inc()

  def recordWriteError(): Unit =
    writeErrors.inc()

  def recordCheckpointError(): Unit =
    checkpointErrors.inc()

  /** Get comprehensive metrics report */
  def getMetricsReport(): MetricsReport =
    MetricsReport(
      readMetrics = ReadMetrics(
        averageLatency = getHistogramMean(transactionLogReadLatency),
        p99Latency = getHistogramPercentile(transactionLogReadLatency, 0.99),
        parallelSpeedup = calculateParallelSpeedup(),
        totalReads = sequentialReadCount.get() + parallelReadCount.get(),
        errorCount = readErrors.getCount
      ),
      writeMetrics = WriteMetrics(
        averageLatency = getHistogramMean(batchWriteLatency),
        p99Latency = getHistogramPercentile(batchWriteLatency, 0.99),
        actionsPerSecond = actionsPerSecond.getMeanRate,
        totalWrites = batchWriteLatency.getCount,
        errorCount = writeErrors.getCount
      ),
      cacheMetrics = CacheMetrics(
        hitRate = calculateCacheHitRate(),
        totalHits = cacheHits.get(),
        totalMisses = cacheMisses.get(),
        averageMissLatency = getHistogramMean(cacheMissLatency)
      ),
      checkpointMetrics = CheckpointMetrics(
        totalCreated = checkpointsCreated.getCount,
        totalRead = checkpointsRead.getCount,
        averageSize = getHistogramMean(checkpointSize),
        averageCreationTime = getTimerMean(checkpointCreationTime),
        errorCount = checkpointErrors.getCount
      ),
      threadPoolMetrics = ThreadPoolMetrics(
        utilization = calculateThreadPoolUtilization(),
        queuedTasks = queuedTasks.getCount,
        rejectedTasks = rejectedTasks.getCount
      ),
      memoryMetrics = MemoryMetrics(
        currentUsage = memoryUsage.getValue,
        heapUtilization = heapUtilization.getValue
      )
    )

  /** Print metrics summary to logs */
  def logMetricsSummary(): Unit = {
    val report = getMetricsReport()

    logger.info("=== Transaction Log Metrics Summary ===")
    logger.info(s"Read Performance:")
    logger.info(s"  Average Latency: ${report.readMetrics.averageLatency}ms")
    logger.info(s"  P99 Latency: ${report.readMetrics.p99Latency}ms")
    logger.info(s"  Parallel Speedup: ${report.readMetrics.parallelSpeedup}x")

    logger.info(s"Write Performance:")
    logger.info(s"  Average Latency: ${report.writeMetrics.averageLatency}ms")
    logger.info(s"  Actions/sec: ${report.writeMetrics.actionsPerSecond}")

    logger.info(s"Cache Performance:")
    logger.info(s"  Hit Rate: ${report.cacheMetrics.hitRate * 100}%")
    logger.info(s"  Total Hits: ${report.cacheMetrics.totalHits}")
    logger.info(s"  Total Misses: ${report.cacheMetrics.totalMisses}")

    logger.info(s"Thread Pool:")
    logger.info(s"  Utilization: ${report.threadPoolMetrics.utilization * 100}%")
    logger.info(s"  Queued Tasks: ${report.threadPoolMetrics.queuedTasks}")

    logger.info(s"Memory:")
    logger.info(s"  Heap Utilization: ${report.memoryMetrics.heapUtilization}%")
    logger.info("=====================================")
  }

  /** Reset all metrics */
  def reset(): Unit = {
    metricRegistry.removeMatching(MetricFilter.ALL)
    sequentialReadTimes.set(0)
    parallelReadTimes.set(0)
    sequentialReadCount.set(0)
    parallelReadCount.set(0)
    cacheHits.set(0)
    cacheMisses.set(0)
  }

  // Helper methods

  private def calculateParallelSpeedup(): Double = {
    val seqCount = sequentialReadCount.get()
    val parCount = parallelReadCount.get()

    if (seqCount == 0 || parCount == 0) return 1.0

    val avgSeqTime = sequentialReadTimes.get().toDouble / seqCount
    val avgParTime = parallelReadTimes.get().toDouble / parCount

    if (avgParTime == 0) return 1.0
    avgSeqTime / avgParTime
  }

  private def calculateCacheHitRate(): Double = {
    val hits   = cacheHits.get()
    val misses = cacheMisses.get()
    val total  = hits + misses

    if (total == 0) return 0.0
    hits.toDouble / total
  }

  private def calculateThreadPoolUtilization(): Double = {
    val stats       = TransactionLogThreadPools.getStatistics()
    val totalActive = stats.totalActiveThreads
    val totalMax = stats.checkpointPoolStats.maximumPoolSize +
      stats.commitPoolStats.maximumPoolSize +
      stats.asyncUpdatePoolStats.maximumPoolSize +
      stats.statsPoolStats.maximumPoolSize +
      stats.fileListingPoolStats.maximumPoolSize +
      stats.parallelReadPoolStats.maximumPoolSize

    if (totalMax == 0) return 0.0
    totalActive.toDouble / totalMax
  }

  private def getHistogramMean(histogram: Histogram): Double = {
    val snapshot = histogram.getSnapshot
    snapshot.getMean
  }

  private def getHistogramPercentile(histogram: Histogram, percentile: Double): Double = {
    val snapshot = histogram.getSnapshot
    snapshot.getValue(percentile)
  }

  private def getTimerMean(timer: Timer): Double = {
    val snapshot = timer.getSnapshot
    TimeUnit.NANOSECONDS.toMillis(snapshot.getMean.toLong).toDouble
  }
}

// Metrics data structures

case class MetricsReport(
  readMetrics: ReadMetrics,
  writeMetrics: WriteMetrics,
  cacheMetrics: CacheMetrics,
  checkpointMetrics: CheckpointMetrics,
  threadPoolMetrics: ThreadPoolMetrics,
  memoryMetrics: MemoryMetrics)

case class ReadMetrics(
  averageLatency: Double,
  p99Latency: Double,
  parallelSpeedup: Double,
  totalReads: Long,
  errorCount: Long)

case class WriteMetrics(
  averageLatency: Double,
  p99Latency: Double,
  actionsPerSecond: Double,
  totalWrites: Long,
  errorCount: Long)

case class CacheMetrics(
  hitRate: Double,
  totalHits: Long,
  totalMisses: Long,
  averageMissLatency: Double)

case class CheckpointMetrics(
  totalCreated: Long,
  totalRead: Long,
  averageSize: Double,
  averageCreationTime: Double,
  errorCount: Long)

case class ThreadPoolMetrics(
  utilization: Double,
  queuedTasks: Long,
  rejectedTasks: Long)

case class MemoryMetrics(
  currentUsage: Long,
  heapUtilization: Double)
