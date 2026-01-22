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

package io.indextables.spark.merge

import java.util.UUID
import java.util.concurrent.{
  ConcurrentHashMap,
  CountDownLatch,
  ExecutorService,
  Executors,
  Semaphore,
  TimeUnit
}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession

import io.indextables.spark.sql.MergeSplitsExecutor
import io.indextables.spark.transaction.TransactionLog

import org.slf4j.LoggerFactory

/**
 * Manages asynchronous merge-on-write jobs on the driver. This is a JVM-wide singleton
 * that handles:
 *   - Concurrency limiting via Semaphore (max concurrent batches across all tables)
 *   - Job tracking (active and completed jobs)
 *   - Progress tracking via AtomicInteger for real-time visibility
 *   - Preventing duplicate jobs for the same table
 *   - Graceful shutdown with timeout
 *
 * Following the AsyncPrewarmJobManager pattern for singleton management.
 */
object AsyncMergeOnWriteManager {
  private val logger = LoggerFactory.getLogger(getClass)

  // Configuration defaults
  private val DEFAULT_MAX_CONCURRENT_BATCHES = 3
  private val DEFAULT_COMPLETED_RETENTION_MS = 3600000L // 1 hour
  private val DEFAULT_SHUTDOWN_TIMEOUT_MS = 300000L // 5 minutes

  // Singleton state with volatile for visibility
  @volatile private var maxConcurrentBatches: Int = DEFAULT_MAX_CONCURRENT_BATCHES
  @volatile private var completedRetentionMs: Long = DEFAULT_COMPLETED_RETENTION_MS
  @volatile private var shutdownTimeoutMs: Long = DEFAULT_SHUTDOWN_TIMEOUT_MS
  @volatile private var batchSemaphore: Semaphore = new Semaphore(DEFAULT_MAX_CONCURRENT_BATCHES)
  @volatile private var executorService: ExecutorService = _
  @volatile private var initialized: Boolean = false
  private val initLock = new Object

  // Job tracking
  private val activeJobs = new ConcurrentHashMap[String, AsyncMergeJob]()
  private val completedJobs = new ConcurrentHashMap[String, AsyncMergeJobResult]()

  // Table-level lock to prevent duplicate jobs for the same table
  private val tableJobIds = new ConcurrentHashMap[String, String]()

  // Cleanup tracking
  @volatile private var lastCleanupTime: Long = System.currentTimeMillis()
  private val CLEANUP_INTERVAL_MS = 60000L // 1 minute

  /**
   * Configure the manager with specific settings. Should be called before starting jobs.
   * Safe to call multiple times - will reconfigure if settings change.
   */
  def configure(config: AsyncMergeOnWriteConfig): Unit = {
    initLock.synchronized {
      val needsReconfigure = !initialized ||
        maxConcurrentBatches != config.maxConcurrentBatches ||
        shutdownTimeoutMs != config.shutdownTimeoutMs

      if (needsReconfigure) {
        // Shutdown existing executor if reconfiguring
        if (executorService != null) {
          executorService.shutdown()
        }

        maxConcurrentBatches = config.maxConcurrentBatches
        shutdownTimeoutMs = config.shutdownTimeoutMs
        batchSemaphore = new Semaphore(maxConcurrentBatches)

        // Create daemon thread executor for background jobs
        executorService = Executors.newCachedThreadPool(r => {
          val t = new Thread(r, s"async-merge-on-write-worker")
          t.setDaemon(true)
          t
        })

        initialized = true
        logger.info(s"AsyncMergeOnWriteManager configured: maxConcurrentBatches=$maxConcurrentBatches, " +
          s"shutdownTimeoutMs=$shutdownTimeoutMs")
      }
    }
  }

  /**
   * Submit an async merge job for a table. Returns Right(job) if started successfully,
   * Left(reason) if rejected.
   *
   * The job is rejected if:
   *   - A merge job is already in progress for the same table
   *   - The manager is shutting down
   *
   * @param tablePath The table path to merge
   * @param totalMergeGroups Total number of merge groups to process
   * @param batchSize Number of merge groups per batch
   * @param transactionLog Transaction log for the table
   * @param writeOptions Write options (includes credentials, temp directories)
   * @param serializedHadoopConf Serialized Hadoop configuration
   * @param sparkSession Active Spark session
   * @return Either the started job or a rejection reason
   */
  def submitMergeJob(
    tablePath: String,
    totalMergeGroups: Int,
    batchSize: Int,
    transactionLog: TransactionLog,
    writeOptions: Map[String, String],
    serializedHadoopConf: Map[String, String],
    sparkSession: SparkSession
  ): Either[String, AsyncMergeJob] = {
    ensureInitialized()
    maybeCleanupOldJobs()

    // Check if a job already exists for this table
    if (isMergeInProgress(tablePath)) {
      val existingJobId = tableJobIds.get(tablePath)
      logger.info(s"Merge job rejected for $tablePath: job $existingJobId already in progress")
      return Left(s"Merge already in progress for $tablePath (job: $existingJobId)")
    }

    // Calculate number of batches
    val totalBatches = (totalMergeGroups + batchSize - 1) / batchSize // ceil division

    // Generate job ID
    val jobId = UUID.randomUUID().toString.take(8)

    // Create job object
    val job = AsyncMergeJob(
      jobId = jobId,
      tablePath = tablePath,
      startTime = System.currentTimeMillis(),
      totalMergeGroups = totalMergeGroups,
      totalBatches = totalBatches,
      batchSize = batchSize,
      completedGroups = new AtomicInteger(0),
      completedBatches = new AtomicInteger(0),
      status = new AtomicReference[MergeJobStatus](MergeJobStatus.Running)
    )

    // Register table and job
    tableJobIds.put(tablePath, jobId)
    activeJobs.put(jobId, job)
    logger.info(s"Starting async merge job $jobId for $tablePath: $totalMergeGroups groups in $totalBatches batches (batch size: $batchSize)")

    // Capture scheduler pool from caller's thread for FAIR scheduling
    // This allows merge to share resources fairly with the write that triggered it
    val callerSchedulerPool = Option(sparkSession.sparkContext.getLocalProperty("spark.scheduler.pool"))
    if (callerSchedulerPool.isDefined) {
      logger.info(s"Job $jobId inheriting scheduler pool '${callerSchedulerPool.get}' from caller")
    }

    // Submit work to executor
    executorService.submit(new Runnable {
      override def run(): Unit = {
        try {
          // Restore scheduler pool in background thread
          callerSchedulerPool.foreach { pool =>
            sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
          }
          executeMergeJob(job, transactionLog, writeOptions, serializedHadoopConf, sparkSession)
        } catch {
          case e: InterruptedException =>
            Thread.currentThread().interrupt()
            failJob(job, "Job cancelled")
          case e: Exception =>
            logger.error(s"Async merge job ${job.jobId} failed: ${e.getMessage}", e)
            failJob(job, e.getMessage)
        } finally {
          // Clear local property
          sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", null)
        }
      }
    })

    Right(job)
  }

  /**
   * Check if a merge job is in progress for a table.
   */
  def isMergeInProgress(tablePath: String): Boolean =
    tableJobIds.containsKey(tablePath)

  /**
   * Await completion of a merge job for a table.
   *
   * @param tablePath The table path
   * @param timeoutMs Timeout in milliseconds
   * @return true if completed within timeout, false otherwise
   */
  def awaitCompletion(tablePath: String, timeoutMs: Long): Boolean = {
    val jobId = tableJobIds.get(tablePath)
    if (jobId == null) {
      return true // No job to wait for
    }

    val job = activeJobs.get(jobId)
    if (job == null) {
      return true // Job already completed
    }

    // Wait for completion using a latch
    val startTime = System.currentTimeMillis()
    val pollIntervalMs = 100L
    while (System.currentTimeMillis() - startTime < timeoutMs) {
      val status = job.status.get()
      if (status != MergeJobStatus.Running) {
        return true
      }
      Thread.sleep(pollIntervalMs)
    }
    false
  }

  /**
   * Get an active job by ID.
   */
  def getActiveJob(jobId: String): Option[AsyncMergeJob] =
    Option(activeJobs.get(jobId))

  /**
   * Get a completed job result by ID.
   */
  def getCompletedJob(jobId: String): Option[AsyncMergeJobResult] =
    Option(completedJobs.get(jobId))

  /**
   * Get all job statuses (both active and recently completed).
   * Used by DESCRIBE MERGE JOBS command.
   */
  def getAllJobStatus: Seq[MergeJobStatus.MergeJobStatusReport] = {
    val now = System.currentTimeMillis()

    // Active jobs
    val activeStatus = activeJobs.values().asScala.map { job =>
      MergeJobStatus.MergeJobStatusReport(
        jobId = job.jobId,
        tablePath = job.tablePath,
        status = job.status.get().toString,
        totalGroups = job.totalMergeGroups,
        completedGroups = job.completedGroups.get(),
        totalBatches = job.totalBatches,
        completedBatches = job.completedBatches.get(),
        durationMs = now - job.startTime,
        errorMessage = None
      )
    }.toSeq

    // Completed jobs (within retention period)
    val completedStatus = completedJobs.values().asScala.map { result =>
      MergeJobStatus.MergeJobStatusReport(
        jobId = result.jobId,
        tablePath = result.tablePath,
        status = if (result.success) "COMPLETED" else "FAILED",
        totalGroups = result.totalMergeGroups,
        completedGroups = result.completedGroups,
        totalBatches = result.totalBatches,
        completedBatches = result.completedBatches,
        durationMs = result.durationMs,
        errorMessage = result.errorMessage
      )
    }.toSeq

    activeStatus ++ completedStatus
  }

  /**
   * Get the number of active merge jobs.
   */
  def getActiveJobCount: Int = activeJobs.size()

  /**
   * Get the number of available batch slots.
   */
  def getAvailableBatchSlots: Int = batchSemaphore.availablePermits()

  /**
   * Shutdown the manager with graceful timeout.
   *
   * @param timeoutMs Timeout to wait for completion
   * @return true if shutdown completed gracefully
   */
  def shutdown(timeoutMs: Long = DEFAULT_SHUTDOWN_TIMEOUT_MS): Boolean = {
    initLock.synchronized {
      if (executorService != null) {
        logger.info(s"Shutting down AsyncMergeOnWriteManager with ${activeJobs.size()} active jobs")
        executorService.shutdown()
        val completed = executorService.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)
        if (!completed) {
          logger.warn("Shutdown timeout exceeded, forcing shutdown")
          executorService.shutdownNow()
        }
        return completed
      }
      true
    }
  }

  /**
   * Reset the manager (for testing). Clears all jobs and resets state.
   */
  def reset(): Unit = {
    initLock.synchronized {
      activeJobs.clear()
      completedJobs.clear()
      tableJobIds.clear()
      if (executorService != null) {
        executorService.shutdownNow()
        executorService = null
      }
      batchSemaphore = new Semaphore(DEFAULT_MAX_CONCURRENT_BATCHES)
      maxConcurrentBatches = DEFAULT_MAX_CONCURRENT_BATCHES
      completedRetentionMs = DEFAULT_COMPLETED_RETENTION_MS
      shutdownTimeoutMs = DEFAULT_SHUTDOWN_TIMEOUT_MS
      initialized = false
      logger.info("AsyncMergeOnWriteManager reset")
    }
  }

  /**
   * Execute a merge job. This runs in a background thread.
   */
  private def executeMergeJob(
    job: AsyncMergeJob,
    transactionLog: TransactionLog,
    writeOptions: Map[String, String],
    serializedHadoopConf: Map[String, String],
    sparkSession: SparkSession
  ): Unit = {
    try {
      logger.info(s"Executing async merge job ${job.jobId} for ${job.tablePath}")

      val targetSizeStr = writeOptions.getOrElse(
        AsyncMergeOnWriteConfig.KEY_TARGET_SIZE,
        AsyncMergeOnWriteConfig.formatBytes(AsyncMergeOnWriteConfig.DEFAULT_TARGET_SIZE_BYTES)
      )
      val targetSizeBytes = io.indextables.spark.util.SizeParser.parseSize(targetSizeStr)

      // Merge write options with hadoop conf (credentials)
      val optionsToPass = serializedHadoopConf ++ writeOptions

      // Create executor
      val executor = new MergeSplitsExecutor(
        sparkSession = sparkSession,
        transactionLog = transactionLog,
        tablePath = new Path(job.tablePath),
        partitionPredicates = Seq.empty,
        targetSize = targetSizeBytes,
        maxDestSplits = None,
        maxSourceSplitsPerMerge = None,
        preCommitMerge = false,
        overrideOptions = Some(optionsToPass)
      )

      // Create a custom batch executor that respects our semaphore and reports progress
      val results = executeMergeWithBatching(job, executor, sparkSession)

      // Job completed successfully
      completeJob(job, success = true, None)
      logger.info(s"Async merge job ${job.jobId} completed successfully: " +
        s"${job.completedGroups.get()}/${job.totalMergeGroups} groups, " +
        s"${job.completedBatches.get()}/${job.totalBatches} batches")

    } catch {
      case e: Exception =>
        logger.error(s"Async merge job ${job.jobId} failed: ${e.getMessage}", e)
        failJob(job, e.getMessage)
        throw e
    }
  }

  /**
   * Execute merge with batching using configured concurrency.
   *
   * This method:
   * 1. Configures batchSize (1/6 of cluster CPUs) for the executor
   * 2. Configures maxConcurrentBatches (default 3) for parallel batch execution
   * 3. Lets the MergeSplitsExecutor handle parallelism via its internal ForkJoinPool
   * 4. Reports progress to the job
   *
   * With defaults on a 24-CPU cluster:
   * - batchSize = 4 (24 × 0.167)
   * - maxConcurrentBatches = 3
   * - Total concurrent work = 4 × 3 = 12 merge groups (50% of cluster)
   */
  private def executeMergeWithBatching(
    job: AsyncMergeJob,
    executor: MergeSplitsExecutor,
    sparkSession: SparkSession
  ): Seq[org.apache.spark.sql.Row] = {
    // The executor uses Scala parallel collections with ForkJoinPool
    // to process multiple batches concurrently

    // Set our batch size and concurrency in spark config for the executor to pick up
    val originalBatchSize = sparkSession.conf.getOption("spark.indextables.merge.batchSize")
    val originalMaxConcurrent = sparkSession.conf.getOption("spark.indextables.merge.maxConcurrentBatches")

    try {
      // Configure the executor to use our batch size (1/6 of cluster CPUs)
      sparkSession.conf.set("spark.indextables.merge.batchSize", job.batchSize.toString)
      // Configure max concurrent batches (default 3, so up to 50% of cluster used)
      sparkSession.conf.set("spark.indextables.merge.maxConcurrentBatches", maxConcurrentBatches.toString)

      logger.info(s"Job ${job.jobId}: batchSize=${job.batchSize}, maxConcurrentBatches=$maxConcurrentBatches " +
        s"(max concurrent groups: ${job.batchSize * maxConcurrentBatches})")

      // Execute merge - the executor handles parallel batch execution internally
      executeMergeWithSemaphore(job, executor)

    } finally {
      // Restore original config values
      originalBatchSize match {
        case Some(v) => sparkSession.conf.set("spark.indextables.merge.batchSize", v)
        case None => sparkSession.conf.unset("spark.indextables.merge.batchSize")
      }
      originalMaxConcurrent match {
        case Some(v) => sparkSession.conf.set("spark.indextables.merge.maxConcurrentBatches", v)
        case None => sparkSession.conf.unset("spark.indextables.merge.maxConcurrentBatches")
      }
    }
  }

  /**
   * Execute merge with job-level semaphore control.
   *
   * The semaphore limits concurrent merge JOBS (not batches).
   * Within each job, the executor handles batch-level parallelism via ForkJoinPool
   * using maxConcurrentBatches (default 3).
   *
   * This prevents resource exhaustion when multiple tables trigger merges simultaneously.
   */
  private def executeMergeWithSemaphore(
    job: AsyncMergeJob,
    executor: MergeSplitsExecutor
  ): Seq[org.apache.spark.sql.Row] = {
    // Acquire permit for this job (limits concurrent jobs, not batches)
    logger.debug(s"Job ${job.jobId} acquiring job semaphore (available: ${batchSemaphore.availablePermits()})")
    batchSemaphore.acquire()
    logger.debug(s"Job ${job.jobId} acquired job semaphore")

    try {
      // Check if job was cancelled while waiting
      if (job.status.get() != MergeJobStatus.Running) {
        logger.info(s"Job ${job.jobId} cancelled, skipping execution")
        return Seq.empty
      }

      // Execute merge via the executor
      // The executor handles batching internally based on spark.indextables.merge.batchSize
      val results = executor.merge()

      // Update progress - mark all groups as completed since executor processed everything
      job.completedGroups.set(job.totalMergeGroups)
      job.completedBatches.set(job.totalBatches)

      logger.info(s"Job ${job.jobId} completed: ${job.completedGroups.get()}/${job.totalMergeGroups} groups")

      results

    } finally {
      // Always release job semaphore
      batchSemaphore.release()
      logger.debug(s"Job ${job.jobId} released job semaphore")
    }
  }

  private def completeJob(job: AsyncMergeJob, success: Boolean, errorMessage: Option[String]): Unit = {
    val result = AsyncMergeJobResult(
      jobId = job.jobId,
      tablePath = job.tablePath,
      totalMergeGroups = job.totalMergeGroups,
      completedGroups = job.completedGroups.get(),
      totalBatches = job.totalBatches,
      completedBatches = job.completedBatches.get(),
      durationMs = System.currentTimeMillis() - job.startTime,
      success = success,
      errorMessage = errorMessage,
      completionTime = Some(System.currentTimeMillis())
    )

    job.status.set(if (success) MergeJobStatus.Completed else MergeJobStatus.Failed)
    activeJobs.remove(job.jobId)
    tableJobIds.remove(job.tablePath)
    completedJobs.put(job.jobId, result)
  }

  private def failJob(job: AsyncMergeJob, error: String): Unit = {
    completeJob(job, success = false, Some(error))
  }

  private def ensureInitialized(): Unit = {
    if (!initialized) {
      configure(AsyncMergeOnWriteConfig.default)
    }
  }

  private def maybeCleanupOldJobs(): Unit = {
    val now = System.currentTimeMillis()
    if (now - lastCleanupTime > CLEANUP_INTERVAL_MS) {
      lastCleanupTime = now
      val cutoff = now - completedRetentionMs
      var removed = 0
      completedJobs.entrySet().removeIf { entry =>
        val shouldRemove = entry.getValue.completionTime.exists(_ < cutoff)
        if (shouldRemove) removed += 1
        shouldRemove
      }
      if (removed > 0) {
        logger.debug(s"Cleaned up $removed old completed merge jobs")
      }
    }
  }
}

/**
 * Status of an async merge job.
 */
sealed trait MergeJobStatus
object MergeJobStatus {
  case object Running extends MergeJobStatus { override def toString = "RUNNING" }
  case object Completed extends MergeJobStatus { override def toString = "COMPLETED" }
  case object Failed extends MergeJobStatus { override def toString = "FAILED" }
  case object Cancelled extends MergeJobStatus { override def toString = "CANCELLED" }

  /**
   * Report object for job status queries.
   */
  case class MergeJobStatusReport(
    jobId: String,
    tablePath: String,
    status: String,
    totalGroups: Int,
    completedGroups: Int,
    totalBatches: Int,
    completedBatches: Int,
    durationMs: Long,
    errorMessage: Option[String]
  ) extends Serializable
}

/**
 * Represents an active async merge job.
 */
case class AsyncMergeJob(
  jobId: String,
  tablePath: String,
  startTime: Long,
  totalMergeGroups: Int,
  totalBatches: Int,
  batchSize: Int,
  completedGroups: AtomicInteger,
  completedBatches: AtomicInteger,
  status: AtomicReference[MergeJobStatus]
)

/**
 * Result from a completed async merge job.
 */
case class AsyncMergeJobResult(
  jobId: String,
  tablePath: String,
  totalMergeGroups: Int,
  completedGroups: Int,
  totalBatches: Int,
  completedBatches: Int,
  durationMs: Long,
  success: Boolean,
  errorMessage: Option[String],
  completionTime: Option[Long] = None
)
