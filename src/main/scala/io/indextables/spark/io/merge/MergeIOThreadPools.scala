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

package io.indextables.spark.io.merge

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import org.slf4j.LoggerFactory

/**
 * Manages thread pools for merge I/O operations (downloads and uploads).
 *
 * Thread pools are lazily initialized as JVM-wide singletons. All threads are daemon threads to ensure they don't
 * prevent JVM shutdown.
 */
object MergeIOThreadPools {

  private val logger        = LoggerFactory.getLogger(getClass)
  private val threadCounter = new AtomicInteger(0)

  // Pool sizes based on typical I/O workload characteristics
  private val DOWNLOAD_COORDINATION_POOL_SIZE = 16 // Coordinates async download futures
  private val UPLOAD_POOL_SIZE                = 8  // Handles upload operations
  private val SCHEDULER_POOL_SIZE             = 2  // Handles retry delays and timeouts

  // Mutable pool references with double-checked locking for recoverability after shutdown
  @volatile private var _downloadPool: ThreadPoolExecutor            = _
  @volatile private var _uploadPool: ThreadPoolExecutor              = _
  @volatile private var _scheduledPool: ScheduledThreadPoolExecutor  = _
  @volatile private var _downloadExecCtx: scala.concurrent.ExecutionContext = _
  private val poolLock = new Object

  /**
   * Thread pool for coordinating async download operations. This pool handles CompletableFuture callbacks and
   * coordination, not the actual I/O which is handled by the async S3/Azure clients.
   *
   * Lazily initialized and automatically recreated after shutdown.
   */
  def downloadCoordinationPool: ThreadPoolExecutor = {
    if (_downloadPool == null || _downloadPool.isShutdown) {
      poolLock.synchronized {
        if (_downloadPool == null || _downloadPool.isShutdown) {
          _downloadPool = createThreadPool("merge-download", DOWNLOAD_COORDINATION_POOL_SIZE)
          _downloadExecCtx = scala.concurrent.ExecutionContext.fromExecutor(_downloadPool)
        }
      }
    }
    _downloadPool
  }

  /** Thread pool for upload operations. Lazily initialized and automatically recreated after shutdown. */
  def uploadPool: ThreadPoolExecutor = {
    if (_uploadPool == null || _uploadPool.isShutdown) {
      poolLock.synchronized {
        if (_uploadPool == null || _uploadPool.isShutdown) {
          _uploadPool = createThreadPool("merge-upload", UPLOAD_POOL_SIZE)
        }
      }
    }
    _uploadPool
  }

  /** Scheduled thread pool for retry delays and timeouts. Uses ScheduledThreadPoolExecutor for precise timing. */
  def scheduledPool: ScheduledThreadPoolExecutor = {
    if (_scheduledPool == null || _scheduledPool.isShutdown) {
      poolLock.synchronized {
        if (_scheduledPool == null || _scheduledPool.isShutdown) {
          val pool = new ScheduledThreadPoolExecutor(
            SCHEDULER_POOL_SIZE,
            new ThreadFactory {
              override def newThread(r: Runnable): Thread = {
                val t = new Thread(r, s"tantivy4spark-merge-scheduler-${threadCounter.incrementAndGet()}")
                t.setDaemon(true)
                t
              }
            }
          )
          // Remove cancelled tasks from the queue immediately
          pool.setRemoveOnCancelPolicy(true)
          _scheduledPool = pool
        }
      }
    }
    _scheduledPool
  }

  /**
   * ExecutionContext backed by the download coordination pool. Reads the context under the same lock used to create it,
   * so the returned context is always paired with the current live pool.
   */
  def downloadExecutionContext: scala.concurrent.ExecutionContext =
    poolLock.synchronized {
      if (_downloadPool == null || _downloadPool.isShutdown) {
        _downloadPool = createThreadPool("merge-download", DOWNLOAD_COORDINATION_POOL_SIZE)
        _downloadExecCtx = scala.concurrent.ExecutionContext.fromExecutor(_downloadPool)
      }
      _downloadExecCtx
    }

  /** Create a thread pool with daemon threads and CallerRunsPolicy for backpressure. */
  private def createThreadPool(name: String, size: Int): ThreadPoolExecutor =
    new ThreadPoolExecutor(
      size, // core pool size
      size, // maximum pool size
      60L,
      TimeUnit.SECONDS, // keep-alive time for idle threads
      new LinkedBlockingQueue[Runnable](1000), // bounded to prevent unbounded growth on stalled downloads
      new ThreadFactory {
        override def newThread(r: Runnable): Thread = {
          val t = new Thread(r, s"tantivy4spark-$name-${threadCounter.incrementAndGet()}")
          t.setDaemon(true) // Daemon threads don't prevent JVM shutdown
          t
        }
      },
      new ThreadPoolExecutor.CallerRunsPolicy() // Execute in caller thread if pool is full
    )

  /** Get statistics for all thread pools. */
  def getStatistics: MergeIOPoolStatistics =
    MergeIOPoolStatistics(
      downloadPoolStats = getPoolStats(downloadCoordinationPool),
      uploadPoolStats = getPoolStats(uploadPool),
      schedulerPoolStats = getSchedulerPoolStats(scheduledPool)
    )

  private def getPoolStats(pool: ThreadPoolExecutor): PoolStats =
    PoolStats(
      activeCount = pool.getActiveCount,
      completedTaskCount = pool.getCompletedTaskCount,
      taskCount = pool.getTaskCount,
      queueSize = pool.getQueue.size(),
      corePoolSize = pool.getCorePoolSize,
      maximumPoolSize = pool.getMaximumPoolSize
    )

  private def getSchedulerPoolStats(pool: ScheduledThreadPoolExecutor): PoolStats =
    PoolStats(
      activeCount = pool.getActiveCount,
      completedTaskCount = pool.getCompletedTaskCount,
      taskCount = pool.getTaskCount,
      queueSize = pool.getQueue.size(),
      corePoolSize = pool.getCorePoolSize,
      maximumPoolSize = pool.getMaximumPoolSize
    )

  /**
   * Shutdown all thread pools gracefully. Uses backing fields directly to avoid creating pools just to shut them down.
   * Waits up to 30 seconds for tasks to complete before forcing shutdown.
   */
  def shutdown(): Unit =
    poolLock.synchronized {
      logger.info("Shutting down merge I/O thread pools")

      val regularPools = Seq(_downloadPool, _uploadPool).filter(_ != null)
      regularPools.foreach(_.shutdown())
      if (_scheduledPool != null) _scheduledPool.shutdown()

      regularPools.foreach { pool =>
        if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
          logger.warn("Thread pool did not terminate gracefully, forcing shutdown")
          pool.shutdownNow()
        }
      }

      if (_scheduledPool != null && !_scheduledPool.awaitTermination(30, TimeUnit.SECONDS)) {
        logger.warn("Scheduler pool did not terminate gracefully, forcing shutdown")
        _scheduledPool.shutdownNow()
      }
    }

  /** Schedule a task to run after a delay. Returns a ScheduledFuture that can be cancelled. */
  def schedule(task: Runnable, delayMs: Long): ScheduledFuture[_] =
    scheduledPool.schedule(task, delayMs, TimeUnit.MILLISECONDS)

  /** Schedule a task to run after a delay, returning the result. Returns a ScheduledFuture that can be cancelled. */
  def schedule[T](task: Callable[T], delayMs: Long): ScheduledFuture[T] =
    scheduledPool.schedule(task, delayMs, TimeUnit.MILLISECONDS)
}

/** Statistics for a single thread pool. */
case class PoolStats(
  activeCount: Int,
  completedTaskCount: Long,
  taskCount: Long,
  queueSize: Int,
  corePoolSize: Int,
  maximumPoolSize: Int) {

  def utilizationPercent: Double =
    if (maximumPoolSize == 0) 0.0
    else (activeCount.toDouble / maximumPoolSize) * 100
}

/** Aggregated statistics for all merge I/O thread pools. */
case class MergeIOPoolStatistics(
  downloadPoolStats: PoolStats,
  uploadPoolStats: PoolStats,
  schedulerPoolStats: PoolStats) {

  def totalActiveThreads: Int =
    downloadPoolStats.activeCount +
      uploadPoolStats.activeCount +
      schedulerPoolStats.activeCount

  def totalCompletedTasks: Long =
    downloadPoolStats.completedTaskCount +
      uploadPoolStats.completedTaskCount +
      schedulerPoolStats.completedTaskCount
}
