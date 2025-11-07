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

import java.io.{File, FileInputStream}
import java.util.concurrent.{ConcurrentHashMap, Executors, Semaphore, TimeUnit, TimeoutException}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

import io.indextables.spark.io.CloudStorageProvider
import org.slf4j.LoggerFactory

/**
 * Manages asynchronous upload of split files from worker local disk to permanent
 * storage staging area. Enables workers to continue processing while uploads occur
 * in background threads.
 *
 * GAP MITIGATIONS:
 * - Gap #2 (Partial Staging Failures): Implements retry logic with exponential backoff
 * - Gap #2 (Disk Space Management): Tracks upload progress and can report disk usage
 * - Gap #8 (Cost Optimization): Supports configurable minimum size threshold for staging
 */
class SplitStagingUploader(
  cloudProvider: CloudStorageProvider,
  stagingBasePath: String,
  numThreads: Int = 4,
  maxRetries: Int = 3,               // Gap #2: Configurable retry attempts
  retryDelayMs: Long = 1000,         // Gap #2: Base retry delay with exponential backoff
  minSizeToStage: Long = 10 * 1024 * 1024 // Gap #8: Minimum size (10M) to stage
) extends Serializable {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[SplitStagingUploader])

  @transient private lazy val uploadExecutor = Executors.newFixedThreadPool(numThreads)
  @transient private lazy val pendingUploads = new ConcurrentHashMap[String, Future[StagingResult]]()

  // NEW: Track total bytes uploaded for metrics (Gap #9: Observability)
  @transient private lazy val totalBytesUploaded = new java.util.concurrent.atomic.AtomicLong(0)
  @transient private lazy val totalUploadFailures = new java.util.concurrent.atomic.AtomicInteger(0)
  @transient private lazy val totalRetries = new java.util.concurrent.atomic.AtomicInteger(0)

  /**
   * Initiate asynchronous upload of split file to staging area
   *
   * Gap #8: Skips staging for small files below threshold to reduce API costs
   * Gap #2: Implements retry logic for failed uploads
   *
   * @param splitUuid Unique identifier for split
   * @param localPath Path to split on local disk
   * @param metadata Split metadata to include
   * @return Future that completes when upload finishes
   */
  def stageAsync(
    splitUuid: String,
    localPath: String,
    metadata: StagedSplitInfo
  ): Future[StagingResult] = {

    val file = new File(localPath)
    val fileSize = file.length()

    // Gap #8: Skip staging for small files below threshold
    if (fileSize < minSizeToStage) {
      logger.info(s"Skipping staging for small split: ${fileSize / 1024 / 1024}MB < ${minSizeToStage / 1024 / 1024}MB (uuid: $splitUuid)")

      val result = StagingResult(
        success = true,
        stagingPath = "",
        bytesUploaded = 0,
        durationMs = 0,
        error = Some(s"Skipped staging (below size threshold: ${fileSize / 1024 / 1024}MB)"),
        retriesAttempted = 0
      )

      val immediateFuture = Future.successful(result)
      pendingUploads.put(splitUuid, immediateFuture)
      return immediateFuture
    }

    val stagingPath = s"$stagingBasePath/$splitUuid.tmp"

    val uploadFuture = Future {
      uploadWithRetry(splitUuid, localPath, stagingPath, fileSize, attempt = 0)
    }(ExecutionContext.fromExecutor(uploadExecutor))

    pendingUploads.put(splitUuid, uploadFuture)
    uploadFuture
  }

  /**
   * Upload with retry logic for fault tolerance
   *
   * Gap #2: Implements exponential backoff retry strategy
   */
  private def uploadWithRetry(
    splitUuid: String,
    localPath: String,
    stagingPath: String,
    fileSize: Long,
    attempt: Int
  ): StagingResult = {
    val startTime = System.currentTimeMillis()

    try {
      val file = new File(localPath)
      require(file.exists(), s"Local split file not found: $localPath")

      logger.info(s"Starting staging upload (attempt ${attempt + 1}/$maxRetries): $localPath → $stagingPath")

      cloudProvider.writeFileFromStream(
        stagingPath,
        new FileInputStream(file),
        Some(file.length())
      )

      val duration = System.currentTimeMillis() - startTime
      totalBytesUploaded.addAndGet(fileSize)

      logger.info(s"Completed staging upload: $stagingPath (${file.length()} bytes, ${duration}ms)")

      StagingResult(
        success = true,
        stagingPath = stagingPath,
        bytesUploaded = file.length(),
        durationMs = duration,
        error = None,
        retriesAttempted = attempt
      )

    } catch {
      case e: Exception =>
        logger.error(s"Staging upload failed for $splitUuid (attempt ${attempt + 1}/$maxRetries)", e)
        totalUploadFailures.incrementAndGet()

        // Gap #2: Retry with exponential backoff
        if (attempt < maxRetries - 1) {
          val delay = retryDelayMs * Math.pow(2, attempt).toLong
          logger.warn(s"Retrying staging upload after ${delay}ms delay...")
          totalRetries.incrementAndGet()

          Thread.sleep(delay)
          uploadWithRetry(splitUuid, localPath, stagingPath, fileSize, attempt + 1)
        } else {
          val duration = System.currentTimeMillis() - startTime
          StagingResult(
            success = false,
            stagingPath = stagingPath,
            bytesUploaded = 0,
            durationMs = duration,
            error = Some(e.getMessage),
            retriesAttempted = attempt
          )
        }
    }
  }

  /**
   * Wait for specific split upload to complete
   */
  def awaitStaging(splitUuid: String, timeoutMillis: Long = 300000): StagingResult = {
    val future = pendingUploads.get(splitUuid)
    require(future != null, s"No staging operation found for split: $splitUuid")

    try {
      Await.result(future, Duration(timeoutMillis, TimeUnit.MILLISECONDS))
    } catch {
      case e: TimeoutException =>
        logger.error(s"Staging upload timeout for split: $splitUuid")
        totalUploadFailures.incrementAndGet()
        StagingResult(
          success = false,
          stagingPath = "",
          bytesUploaded = 0,
          durationMs = timeoutMillis,
          error = Some(s"Upload timeout after ${timeoutMillis}ms"),
          retriesAttempted = maxRetries
        )
    }
  }

  /**
   * Wait for all pending uploads to complete with granular retry handling
   *
   * Gap #2: Implements partial success handling with configurable thresholds
   *
   * @param timeoutMillis Total timeout for all uploads
   * @param allowPartialStaging If true, allows some uploads to fail
   * @param minSuccessRate Minimum required success rate (0.0-1.0)
   * @return Map of split UUID to staging result
   */
  def awaitAllStaging(
    timeoutMillis: Long = 600000,
    allowPartialStaging: Boolean = false,
    minSuccessRate: Double = 0.95
  ): Map[String, StagingResult] = {

    val results = pendingUploads.asScala.map { case (uuid, future) =>
      uuid -> awaitStaging(uuid, timeoutMillis)
    }.toMap

    // Gap #2: Analyze results and handle partial failures
    val successful = results.values.count(_.success)
    val total = results.size
    val successRate = if (total > 0) successful.toDouble / total else 1.0

    logger.info(s"Staging upload summary: $successful/$total successful (${(successRate * 100).formatted("%.1f")}%)")
    logger.info(s"Total bytes uploaded: ${totalBytesUploaded.get() / 1024 / 1024}MB, Failures: ${totalUploadFailures.get()}, Retries: ${totalRetries.get()}")

    // Gap #2: Enforce success rate threshold
    if (!allowPartialStaging && successRate < minSuccessRate) {
      val failedUuids = results.filter(!_._2.success).keys.mkString(", ")
      throw new RuntimeException(
        s"Staging upload success rate ${(successRate * 100).formatted("%.1f")}% below threshold ${(minSuccessRate * 100).formatted("%.1f")}%. " +
        s"Failed splits: $failedUuids"
      )
    }

    results
  }

  /**
   * Get staging metrics for observability
   *
   * Gap #9: Provides metrics for monitoring and debugging
   */
  def getMetrics: Map[String, Long] = {
    Map(
      "totalBytesUploaded" -> totalBytesUploaded.get(),
      "totalUploadFailures" -> totalUploadFailures.get().toLong,
      "totalRetries" -> totalRetries.get().toLong,
      "pendingUploads" -> pendingUploads.size().toLong
    )
  }

  /**
   * Shutdown upload thread pool
   */
  def shutdown(): Unit = {
    if (uploadExecutor != null && !uploadExecutor.isShutdown) {
      uploadExecutor.shutdown()
      if (!uploadExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        uploadExecutor.shutdownNow()
        logger.warn("Forcefully shut down staging uploader after timeout")
      }
    }
  }
}
