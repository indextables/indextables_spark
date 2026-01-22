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

package io.indextables.spark.prewarm

import java.util.concurrent.{
  ConcurrentHashMap,
  ExecutorService,
  Executors,
  Semaphore,
  TimeUnit
}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.jdk.CollectionConverters._

import org.slf4j.LoggerFactory

/**
 * Manages asynchronous prewarm jobs on executors. This is a JVM-wide singleton
 * that handles:
 *   - Concurrency limiting via Semaphore (max concurrent jobs per executor)
 *   - Job tracking (active and completed jobs)
 *   - Progress tracking via AtomicInteger for real-time visibility
 *   - Automatic cleanup of old completed jobs
 *
 * Following the CloudDownloadManager pattern for singleton management.
 */
object AsyncPrewarmJobManager {
  private val logger = LoggerFactory.getLogger(getClass)

  // Configuration defaults
  private val DEFAULT_MAX_CONCURRENT = 1
  private val DEFAULT_COMPLETED_RETENTION_MS = 3600000L // 1 hour

  // Singleton state with volatile for visibility
  @volatile private var maxConcurrent: Int = DEFAULT_MAX_CONCURRENT
  @volatile private var completedRetentionMs: Long = DEFAULT_COMPLETED_RETENTION_MS
  @volatile private var semaphore: Semaphore = new Semaphore(DEFAULT_MAX_CONCURRENT)
  @volatile private var executorService: ExecutorService = _
  @volatile private var initialized: Boolean = false
  private val initLock = new Object

  // Job tracking
  private val activeJobs = new ConcurrentHashMap[String, AsyncPrewarmJob]()
  private val completedJobs = new ConcurrentHashMap[String, AsyncPrewarmJobResult]()

  // Cleanup tracking
  @volatile private var lastCleanupTime: Long = System.currentTimeMillis()
  private val CLEANUP_INTERVAL_MS = 60000L // 1 minute

  /**
   * Configure the manager with specific settings. Should be called before starting jobs.
   * Safe to call multiple times - will reconfigure if settings change.
   */
  def configure(maxConcurrentJobs: Int, completedJobRetentionMs: Long = DEFAULT_COMPLETED_RETENTION_MS): Unit = {
    initLock.synchronized {
      if (!initialized || maxConcurrent != maxConcurrentJobs || completedRetentionMs != completedJobRetentionMs) {
        // Shutdown existing executor if reconfiguring
        if (executorService != null) {
          executorService.shutdown()
        }

        maxConcurrent = maxConcurrentJobs
        completedRetentionMs = completedJobRetentionMs
        semaphore = new Semaphore(maxConcurrentJobs)

        // Create daemon thread executor for background jobs
        executorService = Executors.newCachedThreadPool(r => {
          val t = new Thread(r, s"async-prewarm-worker")
          t.setDaemon(true)
          t
        })

        initialized = true
        logger.info(s"AsyncPrewarmJobManager configured: maxConcurrent=$maxConcurrent, retentionMs=$completedRetentionMs")
      }
    }
  }

  /**
   * Try to start an async prewarm job. Returns Right(job) if started successfully,
   * Left(reason) if rejected (e.g., at capacity).
   *
   * @param jobId Unique job identifier
   * @param tablePath The table being prewarmed
   * @param hostname The executor hostname
   * @param totalSplits Number of splits to prewarm
   * @param work The actual prewarm work to execute
   * @return Either the started job or a rejection reason
   */
  def tryStartJob(
    jobId: String,
    tablePath: String,
    hostname: String,
    totalSplits: Int,
    work: () => AsyncPrewarmJobResult
  ): Either[String, AsyncPrewarmJob] = {
    ensureInitialized()
    maybeCleanupOldJobs()

    // Check if job already exists
    if (activeJobs.containsKey(jobId) || completedJobs.containsKey(jobId)) {
      return Left(s"Job $jobId already exists")
    }

    // Try to acquire semaphore (non-blocking)
    if (!semaphore.tryAcquire()) {
      val activeCount = activeJobs.size()
      logger.info(s"Async prewarm job rejected: at capacity ($activeCount active, max=$maxConcurrent)")
      return Left(s"At capacity: $activeCount jobs running (max=$maxConcurrent)")
    }

    // Create and track the job
    val job = AsyncPrewarmJob(
      jobId = jobId,
      tablePath = tablePath,
      hostname = hostname,
      startTime = System.currentTimeMillis(),
      totalSplits = totalSplits,
      completedSplits = new AtomicInteger(0),
      status = new AtomicReference[JobStatus](JobStatus.Running)
    )

    activeJobs.put(jobId, job)
    logger.info(s"Starting async prewarm job $jobId for $tablePath on $hostname ($totalSplits splits)")

    // Submit work to executor
    executorService.submit(new Runnable {
      override def run(): Unit = {
        try {
          val result = work()
          completeJob(jobId, result)
        } catch {
          case e: InterruptedException =>
            Thread.currentThread().interrupt()
            completeJob(jobId, AsyncPrewarmJobResult(
              jobId = jobId,
              tablePath = tablePath,
              hostname = hostname,
              totalSplits = totalSplits,
              splitsPrewarmed = job.completedSplits.get(),
              durationMs = System.currentTimeMillis() - job.startTime,
              success = false,
              errorMessage = Some("Job cancelled")
            ))
          case e: Exception =>
            logger.error(s"Async prewarm job $jobId failed: ${e.getMessage}", e)
            completeJob(jobId, AsyncPrewarmJobResult(
              jobId = jobId,
              tablePath = tablePath,
              hostname = hostname,
              totalSplits = totalSplits,
              splitsPrewarmed = job.completedSplits.get(),
              durationMs = System.currentTimeMillis() - job.startTime,
              success = false,
              errorMessage = Some(e.getMessage)
            ))
        } finally {
          semaphore.release()
        }
      }
    })

    Right(job)
  }

  /**
   * Get an active job by ID.
   */
  def getActiveJob(jobId: String): Option[AsyncPrewarmJob] =
    Option(activeJobs.get(jobId))

  /**
   * Get a completed job result by ID.
   */
  def getCompletedJob(jobId: String): Option[AsyncPrewarmJobResult] =
    Option(completedJobs.get(jobId))

  /**
   * Get all job status (both active and recently completed).
   * Used by DESCRIBE PREWARM JOBS command.
   */
  def getAllJobStatus: Seq[PrewarmJobStatus] = {
    val now = System.currentTimeMillis()

    // Active jobs
    val activeStatus = activeJobs.values().asScala.map { job =>
      PrewarmJobStatus(
        jobId = job.jobId,
        tablePath = job.tablePath,
        hostname = job.hostname,
        status = job.status.get().toString,
        totalSplits = job.totalSplits,
        completedSplits = job.completedSplits.get(),
        durationMs = now - job.startTime,
        errorMessage = None
      )
    }.toSeq

    // Completed jobs (within retention period)
    val completedStatus = completedJobs.values().asScala.map { result =>
      PrewarmJobStatus(
        jobId = result.jobId,
        tablePath = result.tablePath,
        hostname = result.hostname,
        status = if (result.success) "COMPLETED" else "FAILED",
        totalSplits = result.totalSplits,
        completedSplits = result.splitsPrewarmed,
        durationMs = result.durationMs,
        errorMessage = result.errorMessage
      )
    }.toSeq

    activeStatus ++ completedStatus
  }

  /**
   * Cancel a running job by ID. Returns true if the job was found and cancelled.
   */
  def cancelJob(jobId: String): Boolean = {
    Option(activeJobs.get(jobId)) match {
      case Some(job) =>
        if (job.status.compareAndSet(JobStatus.Running, JobStatus.Cancelled)) {
          logger.info(s"Cancelled async prewarm job $jobId")
          true
        } else {
          false
        }
      case None =>
        false
    }
  }

  /**
   * Update progress for a job. Called during prewarm execution.
   */
  def updateProgress(jobId: String, completedCount: Int): Unit = {
    Option(activeJobs.get(jobId)).foreach { job =>
      job.completedSplits.set(completedCount)
    }
  }

  /**
   * Increment progress for a job. Called after each split is prewarmed.
   */
  def incrementProgress(jobId: String): Int = {
    Option(activeJobs.get(jobId)) match {
      case Some(job) => job.completedSplits.incrementAndGet()
      case None => 0
    }
  }

  /**
   * Check if a job is cancelled (for cooperative cancellation).
   */
  def isJobCancelled(jobId: String): Boolean = {
    Option(activeJobs.get(jobId)) match {
      case Some(job) => job.status.get() == JobStatus.Cancelled
      case None => false
    }
  }

  /**
   * Get the number of active jobs.
   */
  def getActiveJobCount: Int = activeJobs.size()

  /**
   * Get the number of available slots.
   */
  def getAvailableSlots: Int = semaphore.availablePermits()

  /**
   * Reset the manager (for testing). Clears all jobs and resets state.
   */
  def reset(): Unit = {
    initLock.synchronized {
      activeJobs.clear()
      completedJobs.clear()
      if (executorService != null) {
        executorService.shutdownNow()
        executorService = null
      }
      semaphore = new Semaphore(DEFAULT_MAX_CONCURRENT)
      maxConcurrent = DEFAULT_MAX_CONCURRENT
      completedRetentionMs = DEFAULT_COMPLETED_RETENTION_MS
      initialized = false
      logger.info("AsyncPrewarmJobManager reset")
    }
  }

  // Internal: Complete a job and move to completed map
  private def completeJob(jobId: String, result: AsyncPrewarmJobResult): Unit = {
    val job = activeJobs.remove(jobId)
    if (job != null) {
      job.status.set(if (result.success) JobStatus.Completed else JobStatus.Failed)
      completedJobs.put(jobId, result.copy(completionTime = Some(System.currentTimeMillis())))
      logger.info(s"Async prewarm job $jobId completed: ${result.splitsPrewarmed}/${result.totalSplits} splits in ${result.durationMs}ms")
    }
  }

  // Internal: Ensure manager is initialized
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      configure(DEFAULT_MAX_CONCURRENT, DEFAULT_COMPLETED_RETENTION_MS)
    }
  }

  // Internal: Clean up old completed jobs
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
        logger.debug(s"Cleaned up $removed old completed prewarm jobs")
      }
    }
  }
}

/**
 * Status of an async prewarm job.
 */
sealed trait JobStatus
object JobStatus {
  case object Running extends JobStatus { override def toString = "RUNNING" }
  case object Completed extends JobStatus { override def toString = "COMPLETED" }
  case object Failed extends JobStatus { override def toString = "FAILED" }
  case object Cancelled extends JobStatus { override def toString = "CANCELLED" }
}

/**
 * Represents an active async prewarm job.
 */
case class AsyncPrewarmJob(
  jobId: String,
  tablePath: String,
  hostname: String,
  startTime: Long,
  totalSplits: Int,
  completedSplits: AtomicInteger,
  status: AtomicReference[JobStatus]
)

/**
 * Result from a completed async prewarm job.
 */
case class AsyncPrewarmJobResult(
  jobId: String,
  tablePath: String,
  hostname: String,
  totalSplits: Int,
  splitsPrewarmed: Int,
  durationMs: Long,
  success: Boolean,
  errorMessage: Option[String],
  completionTime: Option[Long] = None
)

/**
 * Status of a prewarm job for reporting. Used by DESCRIBE PREWARM JOBS.
 */
case class PrewarmJobStatus(
  jobId: String,
  tablePath: String,
  hostname: String,
  status: String,
  totalSplits: Int,
  completedSplits: Int,
  durationMs: Long,
  errorMessage: Option[String]
) extends Serializable
