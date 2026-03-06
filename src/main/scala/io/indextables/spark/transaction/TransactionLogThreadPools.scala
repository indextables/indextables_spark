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

package io.indextables.spark.transaction

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.SparkSession

import org.slf4j.LoggerFactory

/**
 * Manages specialized thread pools for different transaction log operations, following Delta Lake's architecture for
 * optimal performance.
 */
object TransactionLogThreadPools {

  private val logger        = LoggerFactory.getLogger(getClass)
  private val threadCounter = new AtomicInteger(0)

  // Configuration defaults (can be overridden via Spark config)
  private val CHECKPOINT_POOL_SIZE    = 4
  private val COMMIT_POOL_SIZE        = 8
  private val ASYNC_UPDATE_POOL_SIZE  = 2
  private val STATS_POOL_SIZE         = 4
  private val FILE_LISTING_POOL_SIZE  = 6
  private val PARALLEL_READ_POOL_SIZE = 8

  // Thread pools for different operations
  lazy val checkpointThreadPool: DeltaThreadPool = {
    checkpointPoolInitialized = true
    new DeltaThreadPool(createThreadPool("checkpoint", CHECKPOINT_POOL_SIZE))
  }

  lazy val commitThreadPool: DeltaThreadPool = {
    commitPoolInitialized = true
    new DeltaThreadPool(createThreadPool("commit", COMMIT_POOL_SIZE))
  }

  lazy val asyncUpdateThreadPool: DeltaThreadPool = {
    asyncUpdatePoolInitialized = true
    new DeltaThreadPool(createThreadPool("async-update", ASYNC_UPDATE_POOL_SIZE))
  }

  lazy val statsThreadPool: DeltaThreadPool = {
    statsPoolInitialized = true
    new DeltaThreadPool(createThreadPool("stats", STATS_POOL_SIZE))
  }

  lazy val fileListingThreadPool: DeltaThreadPool = {
    fileListingPoolInitialized = true
    new DeltaThreadPool(createThreadPool("file-listing", FILE_LISTING_POOL_SIZE))
  }

  lazy val parallelReadThreadPool: DeltaThreadPool = {
    parallelReadPoolInitialized = true
    new DeltaThreadPool(createThreadPool("parallel-read", PARALLEL_READ_POOL_SIZE))
  }

  /** Create a thread pool with custom thread factory */
  private def createThreadPool(name: String, size: Int): ThreadPoolExecutor =
    new ThreadPoolExecutor(
      size, // core pool size
      size, // maximum pool size
      60L,
      TimeUnit.SECONDS, // keep-alive time
      new LinkedBlockingQueue[Runnable](),
      new ThreadFactory {
        override def newThread(r: Runnable): Thread = {
          val t = new Thread(r, s"tantivy4spark-$name-${threadCounter.incrementAndGet()}")
          t.setDaemon(true)
          t
        }
      },
      new ThreadPoolExecutor.CallerRunsPolicy() // Fallback to caller thread when pool is full
    )

  // Track which pools have been initialized
  @volatile private var checkpointPoolInitialized   = false
  @volatile private var commitPoolInitialized       = false
  @volatile private var asyncUpdatePoolInitialized  = false
  @volatile private var statsPoolInitialized        = false
  @volatile private var fileListingPoolInitialized  = false
  @volatile private var parallelReadPoolInitialized = false

  /** Shutdown all thread pools gracefully (only those that have been initialized) */
  def shutdown(): Unit = {
    logger.info("Shutting down transaction log thread pools")

    val pools = Seq(
      (checkpointPoolInitialized, () => checkpointThreadPool),
      (commitPoolInitialized, () => commitThreadPool),
      (asyncUpdatePoolInitialized, () => asyncUpdateThreadPool),
      (statsPoolInitialized, () => statsThreadPool),
      (fileListingPoolInitialized, () => fileListingThreadPool),
      (parallelReadPoolInitialized, () => parallelReadThreadPool)
    ).collect { case (true, pool) => pool() }

    pools.foreach(_.shutdown())

    // Wait for termination
    pools.foreach { pool =>
      if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
        logger.warn(s"Thread pool did not terminate gracefully, forcing shutdown")
        pool.shutdownNow()
      }
    }
  }

  /** Get thread pool statistics for monitoring */
  def getStatistics(): ThreadPoolStatistics =
    ThreadPoolStatistics(
      checkpointPoolStats = checkpointThreadPool.getStatistics(),
      commitPoolStats = commitThreadPool.getStatistics(),
      asyncUpdatePoolStats = asyncUpdateThreadPool.getStatistics(),
      statsPoolStats = statsThreadPool.getStatistics(),
      fileListingPoolStats = fileListingThreadPool.getStatistics(),
      parallelReadPoolStats = parallelReadThreadPool.getStatistics()
    )
}

/**
 * A wrapper around ThreadPoolExecutor that provides Spark-aware execution context and Delta Lake-style non-fate-sharing
 * futures.
 */
class DeltaThreadPool(private val executor: ThreadPoolExecutor) {

  implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(executor)

  /** Submit a task to be executed with the active Spark session */
  def submit[T](spark: SparkSession)(body: => T): Future[T] =
    Future {
      SparkSession.setActiveSession(spark)
      body
    }

  /** Submit a task without Spark session context */
  def submitSimple[T](body: => T): Future[T] =
    Future(body)

  /**
   * Create a non-fate-sharing future that falls back to synchronous execution if the async operation fails or times
   * out.
   */
  def nonFateSharingFuture[T](spark: SparkSession, timeout: Duration = 30.seconds)(body: => T)
    : NonFateSharingFuture[T] =
    new NonFateSharingFuture[T](this, spark, timeout)(body)

  /** Execute multiple tasks in parallel and collect results */
  def parallelExecute[T](tasks: Seq[() => T]): Seq[Try[T]] = {
    val futures = tasks.map(task => submitSimple(task()))

    // Wait for all futures with timeout
    val promise = Promise[Seq[Try[T]]]()
    val results = futures.map(f => f.map(Success(_)).recover { case e => Failure(e) })

    Future.sequence(results).onComplete {
      case Success(res) => promise.success(res)
      case Failure(e)   => promise.failure(e)
    }

    Try(scala.concurrent.Await.result(promise.future, 60.seconds))
      .getOrElse(futures.map(_ => Failure(new TimeoutException("Parallel execution timeout"))))
  }

  /** Get current thread pool statistics */
  def getStatistics(): PoolStatistics =
    PoolStatistics(
      activeCount = executor.getActiveCount,
      completedTaskCount = executor.getCompletedTaskCount,
      taskCount = executor.getTaskCount,
      queueSize = executor.getQueue.size(),
      corePoolSize = executor.getCorePoolSize,
      maximumPoolSize = executor.getMaximumPoolSize
    )

  def shutdown(): Unit =
    executor.shutdown()

  def shutdownNow(): Unit =
    executor.shutdownNow()

  def awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
    executor.awaitTermination(timeout, unit)
}

/**
 * Non-fate-sharing future implementation that provides fallback to synchronous execution. This prevents cascading
 * failures when one async operation fails.
 */
class NonFateSharingFuture[T](
  pool: DeltaThreadPool,
  spark: SparkSession,
  timeout: Duration
)(
  body: => T) {

  private val logger  = LoggerFactory.getLogger(classOf[NonFateSharingFuture[T]])
  private val promise = Promise[T]()

  // Submit the async task
  private val future = pool.submit(spark) {
    try {
      val result = body
      promise.trySuccess(result)
      result
    } catch {
      case e: Exception =>
        promise.tryFailure(e)
        throw e
    }
  }

  /** Get the result with fallback to synchronous execution */
  def get(): Option[T] =
    Try(scala.concurrent.Await.result(future, timeout)) match {
      case Success(value) =>
        Some(value)

      case Failure(_: TimeoutException) =>
        logger.warn(s"Async operation timed out after $timeout, falling back to synchronous execution")
        // Check if the async future completed while we were handling the timeout
        if (future.isCompleted) {
          future.value.flatMap(_.toOption)
        } else {
          Try {
            SparkSession.setActiveSession(spark)
            body
          }.toOption
        }

      case Failure(e) =>
        logger.warn(s"Async operation failed, falling back to synchronous execution", e)
        Try {
          SparkSession.setActiveSession(spark)
          body
        }.toOption
    }

  /** Get the result or throw exception */
  def getOrThrow(): T =
    get().getOrElse(throw new RuntimeException("Failed to execute operation"))

  /** Check if the future is completed */
  def isCompleted: Boolean = future.isCompleted

  /** Get the underlying future */
  def toFuture: Future[T] = future
}

/** Statistics for a single thread pool */
case class PoolStatistics(
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

/** Aggregated statistics for all thread pools */
case class ThreadPoolStatistics(
  checkpointPoolStats: PoolStatistics,
  commitPoolStats: PoolStatistics,
  asyncUpdatePoolStats: PoolStatistics,
  statsPoolStats: PoolStatistics,
  fileListingPoolStats: PoolStatistics,
  parallelReadPoolStats: PoolStatistics) {
  def totalActiveThreads: Int =
    checkpointPoolStats.activeCount +
      commitPoolStats.activeCount +
      asyncUpdatePoolStats.activeCount +
      statsPoolStats.activeCount +
      fileListingPoolStats.activeCount +
      parallelReadPoolStats.activeCount

  def totalCompletedTasks: Long =
    checkpointPoolStats.completedTaskCount +
      commitPoolStats.completedTaskCount +
      asyncUpdatePoolStats.completedTaskCount +
      statsPoolStats.completedTaskCount +
      fileListingPoolStats.completedTaskCount +
      parallelReadPoolStats.completedTaskCount
}
