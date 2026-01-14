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

import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, Semaphore, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.function.BiConsumer

import scala.jdk.CollectionConverters._

import org.slf4j.LoggerFactory

/**
 * Orchestrator for concurrent cloud downloads with priority-based batch processing.
 *
 * Key features:
 *   - Priority queuing: First-submitted batches complete before later batches
 *   - Concurrency control: Semaphore-bounded concurrent downloads (default: 8 * cores)
 *   - Retry logic: Exponential backoff with configurable retries
 *   - Cancellation: Cancel remaining downloads in a batch on failure
 *   - Metrics: Track bytes downloaded, files, latencies, and retries
 *
 * This is a JVM-wide singleton that should be used for all merge download operations on an executor.
 */
object CloudDownloadManager {

  private val logger = LoggerFactory.getLogger(getClass)

  @volatile private var instance: CloudDownloadManager = _
  private val instanceLock                             = new Object

  /**
   * Get or create the singleton instance with default configuration.
   */
  def getInstance: CloudDownloadManager =
    getInstance(MergeIOConfig.default)

  /**
   * Get or create the singleton instance with the specified configuration.
   *
   * Note: If an instance already exists, the configuration is ignored. Use reset() to reconfigure.
   */
  def getInstance(config: MergeIOConfig): CloudDownloadManager =
    if (instance != null) {
      instance
    } else {
      instanceLock.synchronized {
        if (instance == null) {
          instance = new CloudDownloadManager(config)
          logger.info(s"Created CloudDownloadManager with maxConcurrency=${config.maxConcurrentDownloads}")
        }
        instance
      }
    }

  /**
   * Reset the singleton instance. Useful for testing or reconfiguration.
   */
  def reset(): Unit =
    instanceLock.synchronized {
      if (instance != null) {
        instance.shutdown()
        instance = null
        logger.info("CloudDownloadManager instance reset")
      }
    }
}

/**
 * Manager for concurrent cloud downloads with priority-based batch processing.
 *
 * @param config
 *   Configuration for download operations
 */
class CloudDownloadManager(config: MergeIOConfig) {

  private val logger = LoggerFactory.getLogger(classOf[CloudDownloadManager])

  // Priority queue for download requests
  private val downloadQueue = new PriorityDownloadQueue()

  // Semaphore to control concurrent downloads
  private val concurrencySemaphore = new Semaphore(config.maxConcurrentDownloads)

  // Track active batch completions
  private val batchCompletions = new ConcurrentHashMap[Long, BatchCompletion]()

  // Flag to control the download pump
  private val running = new AtomicBoolean(true)

  // Metrics
  private val totalBytesDownloaded = new AtomicLong(0)
  private val totalFilesDownloaded = new AtomicLong(0)
  private val totalDownloadTimeMs  = new AtomicLong(0)
  private val totalRetries         = new AtomicLong(0)
  private val totalFailures        = new AtomicLong(0)

  // Active download workers
  private val activeWorkers = new AtomicInteger(0)

  /**
   * Submit a batch of downloads and wait for all to complete.
   *
   * The batch is assigned a priority based on submission order (earlier = higher priority). Downloads proceed
   * concurrently up to the configured limit, with preference given to completing earlier batches first.
   *
   * @param requests
   *   Download requests to process
   * @param downloader
   *   Protocol-specific downloader to use
   * @param retries
   *   Number of retry attempts per download (default: from config)
   * @return
   *   CompletableFuture that completes with all download results
   */
  def submitBatch(
    requests: Seq[DownloadRequest],
    downloader: AsyncDownloader,
    retries: Int = config.downloadRetries
  ): CompletableFuture[Seq[DownloadResult]] = {

    if (requests.isEmpty) {
      return CompletableFuture.completedFuture(Seq.empty)
    }

    val batchId = downloadQueue.nextBatchId()
    val completion = new BatchCompletion(batchId, requests.size)
    batchCompletions.put(batchId, completion)

    logger.info(s"Submitting batch $batchId with ${requests.size} downloads")

    // Create batch with assigned ID
    val batch = DownloadBatch(
      batchId = batchId,
      submissionTime = System.currentTimeMillis(),
      downloads = requests.zipWithIndex.map { case (req, idx) =>
        req.copy(batchId = batchId, index = idx)
      }
    )

    // Submit to queue
    downloadQueue.submitBatch(batch)

    // Start workers to process the queue
    val workersToStart = math.min(requests.size, config.maxConcurrentDownloads - activeWorkers.get())
    (0 until workersToStart).foreach { _ =>
      startDownloadWorker(downloader, retries)
    }

    // Return future that completes when all downloads in this batch are done
    completion.future.thenApply { results =>
      batchCompletions.remove(batchId)
      results
    }
  }

  /**
   * Start a worker that processes downloads from the queue.
   *
   * Workers continue processing until the queue is empty, then exit. New workers are started as needed when batches are
   * submitted.
   */
  private def startDownloadWorker(downloader: AsyncDownloader, retries: Int): Unit = {
    if (!running.get()) return

    activeWorkers.incrementAndGet()

    CompletableFuture.runAsync(
      new Runnable {
        override def run(): Unit = {
          try {
            processDownloads(downloader, retries)
          } finally {
            activeWorkers.decrementAndGet()
          }
        }
      },
      MergeIOThreadPools.downloadCoordinationPool
    )
  }

  /**
   * Main download processing loop for a worker.
   */
  private def processDownloads(downloader: AsyncDownloader, maxRetries: Int): Unit =
    while (running.get()) {
      // Try to get a download request
      downloadQueue.poll() match {
        case Some(request) =>
          // Acquire semaphore (may block if at capacity)
          concurrencySemaphore.acquire()

          try {
            // Process this download with retries
            val result = processDownloadWithRetry(request, downloader, maxRetries)

            // Record metrics
            if (result.success) {
              totalBytesDownloaded.addAndGet(result.bytesDownloaded)
              totalFilesDownloaded.incrementAndGet()
              totalDownloadTimeMs.addAndGet(result.durationMs)
            } else {
              totalFailures.incrementAndGet()
            }
            totalRetries.addAndGet(result.retryCount)

            // Notify batch completion tracker
            val completion = batchCompletions.get(request.batchId)
            if (completion != null) {
              completion.recordResult(result)
            }
          } finally {
            concurrencySemaphore.release()
          }

        case None =>
          // Queue is empty, exit this worker
          return
      }
    }

  /**
   * Process a single download with retry logic.
   */
  private def processDownloadWithRetry(
    request: DownloadRequest,
    downloader: AsyncDownloader,
    remainingRetries: Int
  ): DownloadResult = {
    val startTime = System.currentTimeMillis()

    try {
      // Execute the download (blocking wait on the async operation)
      val result = downloader.downloadAsync(request).get(5, TimeUnit.MINUTES)

      if (result.success) {
        logger.debug(s"Downloaded ${request.sourcePath} -> ${result.localPath} " +
          s"(${result.bytesDownloaded} bytes in ${result.durationMs}ms)")
        result
      } else if (remainingRetries > 0) {
        // Failed but can retry
        logger.warn(s"Download failed for ${request.sourcePath}, retrying " +
          s"($remainingRetries retries remaining): ${result.error.map(_.getMessage).getOrElse("unknown error")}")

        // Calculate backoff delay
        val attempt  = config.downloadRetries - remainingRetries + 1
        val delayMs = calculateBackoffDelay(attempt)

        // Wait before retry
        Thread.sleep(delayMs)

        // Retry
        val retryResult = processDownloadWithRetry(request, downloader, remainingRetries - 1)
        retryResult.copy(retryCount = retryResult.retryCount + 1)
      } else {
        // Failed and no more retries
        logger.error(s"Download failed for ${request.sourcePath} after all retries: " +
          s"${result.error.map(_.getMessage).getOrElse("unknown error")}")
        result
      }
    } catch {
      case e: Exception =>
        val duration = System.currentTimeMillis() - startTime

        if (remainingRetries > 0) {
          logger.warn(s"Download exception for ${request.sourcePath}, retrying " +
            s"($remainingRetries retries remaining): ${e.getMessage}")

          val attempt  = config.downloadRetries - remainingRetries + 1
          val delayMs = calculateBackoffDelay(attempt)
          Thread.sleep(delayMs)

          val retryResult = processDownloadWithRetry(request, downloader, remainingRetries - 1)
          retryResult.copy(retryCount = retryResult.retryCount + 1)
        } else {
          logger.error(s"Download failed for ${request.sourcePath} after all retries", e)
          DownloadResult.failure(request, e, duration, config.downloadRetries)
        }
    }
  }

  /**
   * Calculate exponential backoff delay with jitter.
   */
  private def calculateBackoffDelay(attempt: Int): Long = {
    val exponentialDelay = config.retryBaseDelayMs * math.pow(2, attempt - 1).toLong
    val cappedDelay      = math.min(exponentialDelay, config.retryMaxDelayMs)
    // Add 10% jitter to prevent thundering herd
    val jitter = (cappedDelay * 0.1 * math.random()).toLong
    cappedDelay + jitter
  }

  /**
   * Cancel all pending downloads for a batch.
   *
   * This is called when a merge operation fails. In-progress downloads will complete, but no new downloads from this
   * batch will start.
   *
   * @param batchId
   *   The batch ID to cancel
   */
  def cancelBatch(batchId: Long): Unit = {
    val cancelled = downloadQueue.cancelBatch(batchId)
    logger.info(s"Cancelled batch $batchId ($cancelled pending downloads)")

    // Complete the batch future with failure if still pending
    val completion = batchCompletions.remove(batchId)
    if (completion != null && !completion.future.isDone) {
      completion.future.completeExceptionally(
        new RuntimeException(s"Batch $batchId was cancelled")
      )
    }
  }

  /**
   * Get current download metrics.
   */
  def getMetrics: DownloadMetrics =
    DownloadMetrics(
      totalBytes = totalBytesDownloaded.get(),
      totalFiles = totalFilesDownloaded.get(),
      totalTimeMs = totalDownloadTimeMs.get(),
      totalRetries = totalRetries.get(),
      failedDownloads = totalFailures.get()
    )

  /**
   * Get queue statistics.
   */
  def getQueueStats: PriorityQueueStats =
    downloadQueue.getStats

  /**
   * Get the number of currently active download workers.
   */
  def activeWorkerCount: Int =
    activeWorkers.get()

  /**
   * Get the number of available download slots.
   */
  def availableSlots: Int =
    concurrencySemaphore.availablePermits()

  /**
   * Shutdown the download manager.
   */
  def shutdown(): Unit = {
    running.set(false)
    downloadQueue.clear()
    batchCompletions.clear()
    logger.info("CloudDownloadManager shutdown complete")
  }
}

/**
 * Tracks completion of a batch of downloads.
 *
 * Thread-safe accumulator that completes a future when all downloads in the batch finish.
 */
private class BatchCompletion(val batchId: Long, val totalCount: Int) {

  private val results         = new ConcurrentHashMap[Int, DownloadResult]()
  private val completedCount = new AtomicInteger(0)
  private val failed         = new AtomicBoolean(false)

  val future: CompletableFuture[Seq[DownloadResult]] = new CompletableFuture[Seq[DownloadResult]]()

  /**
   * Record a download result and complete the future if all downloads are done.
   */
  def recordResult(result: DownloadResult): Unit = {
    results.put(result.request.index, result)

    if (!result.success) {
      failed.set(true)
    }

    val completed = completedCount.incrementAndGet()

    if (completed >= totalCount) {
      // All downloads complete, resolve the future
      val orderedResults = (0 until totalCount).map { idx =>
        results.get(idx)
      }.toSeq

      if (failed.get()) {
        // At least one download failed - we still complete successfully with results
        // The caller can inspect results to find failures
        future.complete(orderedResults)
      } else {
        future.complete(orderedResults)
      }
    }
  }
}
