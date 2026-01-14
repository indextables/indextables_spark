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

import java.util.concurrent.CompletableFuture

/**
 * Trait defining the interface for async file downloaders.
 *
 * Implementations handle protocol-specific async downloads (S3, Azure, local filesystem). All implementations should:
 *   - Use non-blocking I/O where possible
 *   - Stream directly to disk to minimize heap usage
 *   - Handle errors gracefully without throwing from the async context
 */
trait AsyncDownloader extends AutoCloseable {

  /**
   * Download a file asynchronously from source to destination.
   *
   * The download streams directly to disk to minimize memory usage. The returned CompletableFuture completes with a
   * DownloadResult indicating success or failure.
   *
   * @param request
   *   The download request containing source and destination paths
   * @return
   *   A CompletableFuture that completes with the download result
   */
  def downloadAsync(request: DownloadRequest): CompletableFuture[DownloadResult]

  /**
   * Get the protocol this downloader handles (e.g., "s3", "azure", "file").
   */
  def protocol: String

  /**
   * Check if this downloader can handle the given path.
   */
  def canHandle(path: String): Boolean

  /**
   * Close any resources held by this downloader.
   */
  override def close(): Unit = {}
}

/**
 * Request to download a file from a remote source to a local destination.
 *
 * @param sourcePath
 *   Full URL/path to the source file (e.g., s3://bucket/path/file.split)
 * @param destinationPath
 *   Local filesystem path where the file should be written
 * @param expectedSize
 *   Expected file size in bytes (for validation, 0 if unknown)
 * @param batchId
 *   ID of the batch this download belongs to (for priority ordering)
 * @param index
 *   Position of this download within its batch (for result ordering)
 */
case class DownloadRequest(
  sourcePath: String,
  destinationPath: String,
  expectedSize: Long,
  batchId: Long,
  index: Int) {

  /**
   * Create a copy with a new batch ID (used when submitting to the queue).
   */
  def withBatchId(newBatchId: Long): DownloadRequest =
    copy(batchId = newBatchId)
}

object DownloadRequest {

  /**
   * Create a download request with default batch ID and index. Useful for single downloads outside of batch context.
   */
  def apply(sourcePath: String, destinationPath: String, expectedSize: Long): DownloadRequest =
    DownloadRequest(
      sourcePath = sourcePath,
      destinationPath = destinationPath,
      expectedSize = expectedSize,
      batchId = 0,
      index = 0
    )
}

/**
 * Result of a download operation.
 *
 * @param request
 *   The original download request
 * @param localPath
 *   Path to the downloaded file (empty string on failure)
 * @param bytesDownloaded
 *   Number of bytes downloaded (0 on failure)
 * @param durationMs
 *   Duration of the download in milliseconds
 * @param success
 *   Whether the download succeeded
 * @param error
 *   Error that occurred (None on success)
 * @param retryCount
 *   Number of retries that were attempted before this result
 */
case class DownloadResult(
  request: DownloadRequest,
  localPath: String,
  bytesDownloaded: Long,
  durationMs: Long,
  success: Boolean,
  error: Option[Throwable],
  retryCount: Int = 0) {

  /**
   * Create a copy indicating a retry was attempted.
   */
  def withRetry: DownloadResult =
    copy(retryCount = retryCount + 1)

  /**
   * Get a human-readable description of the result.
   */
  def describe: String =
    if (success) {
      s"Downloaded ${request.sourcePath} -> $localPath ($bytesDownloaded bytes in ${durationMs}ms)"
    } else {
      s"Failed to download ${request.sourcePath}: ${error.map(_.getMessage).getOrElse("unknown error")}"
    }
}

object DownloadResult {

  /**
   * Create a successful download result.
   */
  def success(
    request: DownloadRequest,
    localPath: String,
    bytesDownloaded: Long,
    durationMs: Long,
    retryCount: Int = 0
  ): DownloadResult =
    DownloadResult(
      request = request,
      localPath = localPath,
      bytesDownloaded = bytesDownloaded,
      durationMs = durationMs,
      success = true,
      error = None,
      retryCount = retryCount
    )

  /**
   * Create a failed download result.
   */
  def failure(
    request: DownloadRequest,
    error: Throwable,
    durationMs: Long,
    retryCount: Int = 0
  ): DownloadResult =
    DownloadResult(
      request = request,
      localPath = "",
      bytesDownloaded = 0,
      durationMs = durationMs,
      success = false,
      error = Some(error),
      retryCount = retryCount
    )
}

/**
 * Batch of download requests from a single merge operation.
 *
 * All downloads in a batch share the same priority (submission order). The batch completes when all downloads finish
 * (successfully or with failure).
 *
 * @param batchId
 *   Unique identifier for this batch (lower = higher priority)
 * @param submissionTime
 *   Timestamp when the batch was submitted
 * @param downloads
 *   Sequence of download requests in this batch
 */
case class DownloadBatch(
  batchId: Long,
  submissionTime: Long,
  downloads: Seq[DownloadRequest]) {

  /**
   * Number of downloads in this batch.
   */
  def size: Int = downloads.size
}

/**
 * Metrics for download operations.
 *
 * @param totalBytes
 *   Total bytes downloaded
 * @param totalFiles
 *   Total number of files downloaded
 * @param totalTimeMs
 *   Total time spent downloading in milliseconds
 * @param totalRetries
 *   Total number of retries across all downloads
 * @param failedDownloads
 *   Number of downloads that failed after all retries
 */
case class DownloadMetrics(
  totalBytes: Long,
  totalFiles: Long,
  totalTimeMs: Long,
  totalRetries: Long,
  failedDownloads: Long) {

  /**
   * Average download throughput in bytes per second.
   */
  def avgBytesPerSecond: Double =
    if (totalTimeMs > 0) totalBytes.toDouble / totalTimeMs * 1000 else 0

  /**
   * Average download time per file in milliseconds.
   */
  def avgTimePerFileMs: Double =
    if (totalFiles > 0) totalTimeMs.toDouble / totalFiles else 0

  /**
   * Retry rate as a percentage.
   */
  def retryRate: Double =
    if (totalFiles > 0) totalRetries.toDouble / totalFiles * 100 else 0

  /**
   * Merge with another metrics instance.
   */
  def merge(other: DownloadMetrics): DownloadMetrics =
    DownloadMetrics(
      totalBytes = totalBytes + other.totalBytes,
      totalFiles = totalFiles + other.totalFiles,
      totalTimeMs = totalTimeMs + other.totalTimeMs,
      totalRetries = totalRetries + other.totalRetries,
      failedDownloads = failedDownloads + other.failedDownloads
    )
}

object DownloadMetrics {

  /**
   * Empty metrics instance.
   */
  val empty: DownloadMetrics = DownloadMetrics(0, 0, 0, 0, 0)
}
