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

package com.tantivy4spark.io

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.core.sync.RequestBody
import org.slf4j.LoggerFactory
import java.util.concurrent.{CompletableFuture, Executors, ThreadFactory, ForkJoinPool}
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try, Success, Failure}
import java.io.{ByteArrayInputStream, InputStream}
import java.security.MessageDigest

/**
 * High-performance S3 multipart uploader with parallel part uploads.
 *
 * Features:
 *   - Parallel upload of multiple parts
 *   - Configurable part size (minimum 5MB for S3)
 *   - Automatic retry logic for failed parts
 *   - Efficient memory management with streaming
 *   - Progress tracking and monitoring
 *   - Fallback to single-part upload for small files
 */
class S3MultipartUploader(
  s3Client: S3Client,
  config: S3MultipartConfig = S3MultipartConfig.default) {

  private val logger = LoggerFactory.getLogger(classOf[S3MultipartUploader])

  // Use a dedicated thread pool for multipart uploads
  private val uploadExecutor = {
    val threadFactory = new ThreadFactory {
      private val counter = new AtomicInteger(0)
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r, s"s3-multipart-upload-" + counter.incrementAndGet())
        thread.setDaemon(true)
        thread
      }
    }
    Executors.newFixedThreadPool(config.maxConcurrency, threadFactory)
  }

  implicit private val ec: ExecutionContext = ExecutionContext.fromExecutor(uploadExecutor)

  /**
   * Upload a file using multipart upload strategy. Automatically chooses between single-part and multipart based on
   * file size.
   *
   * @param bucket
   *   S3 bucket name
   * @param key
   *   S3 object key
   * @param content
   *   File content as byte array
   * @return
   *   Upload result with metadata
   */
  def uploadFile(
    bucket: String,
    key: String,
    content: Array[Byte]
  ): S3UploadResult = {
    val contentLength = content.length.toLong

    if (contentLength < config.multipartThreshold) {
      logger.info(s"Using single-part upload for small file: s3://$bucket/$key (${formatBytes(contentLength)})")
      uploadSinglePart(bucket, key, content)
    } else {
      logger.info(s"Using multipart upload for large file: s3://$bucket/$key (${formatBytes(contentLength)})")
      uploadMultipart(bucket, key, content)
    }
  }

  /**
   * Upload a file from an InputStream using multipart upload. Reads the stream in chunks and uploads parts in parallel.
   *
   * @param bucket
   *   S3 bucket name
   * @param key
   *   S3 object key
   * @param inputStream
   *   Input stream containing file data
   * @param contentLength
   *   Total content length (if known)
   * @return
   *   Upload result with metadata
   */
  def uploadStream(
    bucket: String,
    key: String,
    inputStream: InputStream,
    contentLength: Option[Long] = None
  ): S3UploadResult =
    contentLength match {
      case Some(length) if length < config.multipartThreshold =>
        // For small streams, read everything into memory and use single-part
        logger.info(s"Using single-part upload for small stream: s3://$bucket/$key (${formatBytes(length)})")
        val content = inputStream.readAllBytes()
        uploadSinglePart(bucket, key, content)

      case _ =>
        // For large or unknown-size streams, use multipart
        logger.info(s"Using multipart upload for stream: s3://$bucket/$key")
        uploadMultipartFromStream(bucket, key, inputStream, contentLength)
    }

  /** Single-part upload for small files */
  private def uploadSinglePart(
    bucket: String,
    key: String,
    content: Array[Byte]
  ): S3UploadResult = {
    val startTime = System.currentTimeMillis()

    try {
      val request = PutObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .contentLength(content.length.toLong)
        .build()

      val response   = s3Client.putObject(request, RequestBody.fromBytes(content))
      val uploadTime = System.currentTimeMillis() - startTime

      logger.info(s"✅ Single-part upload completed: s3://$bucket/$key in ${uploadTime}ms")

      S3UploadResult(
        bucket = bucket,
        key = key,
        etag = response.eTag(),
        uploadId = None,
        partCount = 1,
        totalSize = content.length.toLong,
        uploadTimeMs = uploadTime,
        strategy = "single-part"
      )
    } catch {
      case ex: Exception =>
        logger.error(s"❌ Single-part upload failed: s3://$bucket/$key", ex)
        throw new RuntimeException(s"Single-part upload failed: ${ex.getMessage}", ex)
    }
  }

  /** Multipart upload for large files */
  private def uploadMultipart(
    bucket: String,
    key: String,
    content: Array[Byte]
  ): S3UploadResult = {
    val startTime     = System.currentTimeMillis()
    val contentLength = content.length.toLong

    // Calculate part size and count
    val partSize  = calculateOptimalPartSize(contentLength)
    val partCount = ((contentLength + partSize - 1) / partSize).toInt

    logger.info(s"🚀 Starting multipart upload: s3://$bucket/$key")
    logger.info(s"   Total size: ${formatBytes(contentLength)}")
    logger.info(s"   Part size: ${formatBytes(partSize)}")
    logger.info(s"   Part count: $partCount")
    logger.info(s"   Max concurrency: ${config.maxConcurrency}")

    var uploadId: String = null

    try {
      // 1. Initiate multipart upload
      val createRequest = CreateMultipartUploadRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .build()

      val createResponse = s3Client.createMultipartUpload(createRequest)
      uploadId = createResponse.uploadId()

      logger.info(s"📝 Initiated multipart upload with ID: $uploadId")

      // 2. Upload parts in parallel
      val uploadedParts = uploadPartsInParallel(bucket, key, uploadId, content, partSize, partCount)

      // 3. Complete multipart upload
      val completedParts = uploadedParts.zipWithIndex
        .map {
          case (etag, index) =>
            CompletedPart
              .builder()
              .partNumber(index + 1)
              .eTag(etag)
              .build()
        }
        .toList
        .asJava

      val completeRequest = CompleteMultipartUploadRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .uploadId(uploadId)
        .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
        .build()

      val completeResponse = s3Client.completeMultipartUpload(completeRequest)
      val uploadTime       = System.currentTimeMillis() - startTime

      logger.info(s"✅ Multipart upload completed: s3://$bucket/$key in ${uploadTime}ms")
      logger.info(s"   Final ETag: ${completeResponse.eTag()}")
      logger.info(s"   Upload rate: ${formatBytes((contentLength * 1000) / uploadTime)}/s")

      S3UploadResult(
        bucket = bucket,
        key = key,
        etag = completeResponse.eTag(),
        uploadId = Some(uploadId),
        partCount = partCount,
        totalSize = contentLength,
        uploadTimeMs = uploadTime,
        strategy = "multipart"
      )

    } catch {
      case ex: Exception =>
        logger.error(s"❌ Multipart upload failed: s3://$bucket/$key", ex)

        // Clean up failed upload
        if (uploadId != null) {
          try {
            val abortRequest = AbortMultipartUploadRequest
              .builder()
              .bucket(bucket)
              .key(key)
              .uploadId(uploadId)
              .build()
            s3Client.abortMultipartUpload(abortRequest)
            logger.info(s"🧹 Aborted failed multipart upload: $uploadId")
          } catch {
            case abortEx: Exception =>
              logger.warn(s"Failed to abort multipart upload $uploadId", abortEx)
          }
        }

        throw new RuntimeException(s"Multipart upload failed: ${ex.getMessage}", ex)
    }
  }

  /** Upload parts in parallel using futures */
  private def uploadPartsInParallel(
    bucket: String,
    key: String,
    uploadId: String,
    content: Array[Byte],
    partSize: Long,
    partCount: Int
  ): Array[String] = {

    val futures = (0 until partCount).map { partIndex =>
      Future {
        val partNumber  = partIndex + 1
        val startOffset = partIndex * partSize
        val endOffset   = math.min(startOffset + partSize, content.length)
        val partData    = java.util.Arrays.copyOfRange(content, startOffset.toInt, endOffset.toInt)

        uploadSinglePart(bucket, key, uploadId, partNumber, partData)
      }
    }

    // Use a custom ForkJoinPool to limit parallelism
    val pool = new ForkJoinPool(config.maxConcurrency)

    try {
      import scala.concurrent.duration._
      import scala.concurrent.Await

      val allFutures = Future.sequence(futures)
      Await.result(allFutures, config.uploadTimeout).toArray
    } finally
      pool.shutdown()
  }

  /** Upload a single part with retry logic */
  private def uploadSinglePart(
    bucket: String,
    key: String,
    uploadId: String,
    partNumber: Int,
    partData: Array[Byte]
  ): String = {

    var attempt                  = 0
    var lastException: Exception = null

    while (attempt < config.maxRetries)
      try {
        val partStartTime = System.currentTimeMillis()

        val request = UploadPartRequest
          .builder()
          .bucket(bucket)
          .key(key)
          .uploadId(uploadId)
          .partNumber(partNumber)
          .contentLength(partData.length.toLong)
          .build()

        val response = s3Client.uploadPart(request, RequestBody.fromBytes(partData))
        val partTime = System.currentTimeMillis() - partStartTime

        logger.debug(s"✅ Part $partNumber uploaded: ${formatBytes(partData.length)} in ${partTime}ms")
        return response.eTag()

      } catch {
        case ex: Exception =>
          attempt += 1
          lastException = ex

          if (attempt < config.maxRetries) {
            val delay = config.baseRetryDelay * math.pow(2, attempt - 1).toLong
            logger.warn(
              s"⚠️ Part $partNumber upload failed (attempt $attempt/${config.maxRetries}), retrying in ${delay}ms",
              ex
            )

            try
              Thread.sleep(delay)
            catch {
              case _: InterruptedException =>
                Thread.currentThread().interrupt()
                throw new RuntimeException("Upload interrupted", ex)
            }
          }
      }

    throw new RuntimeException(s"Part $partNumber upload failed after ${config.maxRetries} attempts", lastException)
  }

  /** Multipart upload from input stream (for very large files that don't fit in memory) */
  private def uploadMultipartFromStream(
    bucket: String,
    key: String,
    inputStream: InputStream,
    contentLength: Option[Long]
  ): S3UploadResult = {

    val startTime = System.currentTimeMillis()

    // For stream uploads, we need to read and buffer parts sequentially
    // This is a simplified implementation - for production, consider using reactive streams
    logger.info(s"🚀 Starting multipart upload from stream: s3://$bucket/$key")

    var uploadId: String = null
    val uploadedParts    = scala.collection.mutable.ArrayBuffer[String]()
    var totalBytesRead   = 0L

    try {
      // 1. Initiate multipart upload
      val createRequest = CreateMultipartUploadRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .build()

      val createResponse = s3Client.createMultipartUpload(createRequest)
      uploadId = createResponse.uploadId()

      logger.info(s"📝 Initiated streaming multipart upload with ID: $uploadId")

      // 2. Read stream in chunks and upload parts
      val buffer     = new Array[Byte](config.partSize.toInt)
      var partNumber = 1
      var bytesRead  = 0

      while ({ bytesRead = inputStream.read(buffer); bytesRead } > 0) {
        val partData = if (bytesRead == buffer.length) buffer.clone() else java.util.Arrays.copyOf(buffer, bytesRead)

        val etag = uploadSinglePart(bucket, key, uploadId, partNumber, partData)
        uploadedParts += etag

        totalBytesRead += bytesRead
        partNumber += 1

        logger.debug(
          s"📊 Uploaded part $partNumber: ${formatBytes(partData.length)} (total: ${formatBytes(totalBytesRead)})"
        )
      }

      // 3. Complete multipart upload
      val completedParts = uploadedParts.zipWithIndex
        .map {
          case (etag, index) =>
            CompletedPart
              .builder()
              .partNumber(index + 1)
              .eTag(etag)
              .build()
        }
        .toList
        .asJava

      val completeRequest = CompleteMultipartUploadRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .uploadId(uploadId)
        .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
        .build()

      val completeResponse = s3Client.completeMultipartUpload(completeRequest)
      val uploadTime       = System.currentTimeMillis() - startTime

      logger.info(s"✅ Streaming multipart upload completed: s3://$bucket/$key in ${uploadTime}ms")
      logger.info(s"   Total size: ${formatBytes(totalBytesRead)}")
      logger.info(s"   Parts count: ${uploadedParts.length}")
      logger.info(s"   Upload rate: ${formatBytes((totalBytesRead * 1000) / uploadTime)}/s")

      S3UploadResult(
        bucket = bucket,
        key = key,
        etag = completeResponse.eTag(),
        uploadId = Some(uploadId),
        partCount = uploadedParts.length,
        totalSize = totalBytesRead,
        uploadTimeMs = uploadTime,
        strategy = "multipart-stream"
      )

    } catch {
      case ex: Exception =>
        logger.error(s"❌ Streaming multipart upload failed: s3://$bucket/$key", ex)

        // Clean up failed upload
        if (uploadId != null) {
          try {
            val abortRequest = AbortMultipartUploadRequest
              .builder()
              .bucket(bucket)
              .key(key)
              .uploadId(uploadId)
              .build()
            s3Client.abortMultipartUpload(abortRequest)
            logger.info(s"🧹 Aborted failed streaming multipart upload: $uploadId")
          } catch {
            case abortEx: Exception =>
              logger.warn(s"Failed to abort multipart upload $uploadId", abortEx)
          }
        }

        throw new RuntimeException(s"Streaming multipart upload failed: ${ex.getMessage}", ex)
    }
  }

  /** Calculate optimal part size for multipart upload */
  private def calculateOptimalPartSize(contentLength: Long): Long = {
    // AWS S3 limits: minimum 5MB per part (except last), maximum 10,000 parts
    val minPartSize = 5L * 1024 * 1024 // 5MB
    val maxParts    = 10000L

    // Start with configured part size
    var partSize = config.partSize

    // Ensure minimum part size
    partSize = math.max(partSize, minPartSize)

    // Ensure we don't exceed maximum number of parts
    while (contentLength / partSize > maxParts)
      partSize *= 2

    // Cap at reasonable maximum (1GB per part)
    val maxPartSize = 1024L * 1024 * 1024
    partSize = math.min(partSize, maxPartSize)

    partSize
  }

  /** Format bytes for human-readable output */
  private def formatBytes(bytes: Long): String = {
    val kb = 1024L
    val mb = kb * 1024
    val gb = mb * 1024

    if (bytes >= gb) {
      f"${bytes.toDouble / gb}%.2f GB"
    } else if (bytes >= mb) {
      f"${bytes.toDouble / mb}%.2f MB"
    } else if (bytes >= kb) {
      f"${bytes.toDouble / kb}%.2f KB"
    } else {
      s"$bytes bytes"
    }
  }

  /** Shutdown the uploader and clean up resources */
  def shutdown(): Unit = {
    uploadExecutor.shutdown()
    logger.info("S3 multipart uploader shutdown complete")
  }
}

/** Configuration for S3 multipart uploads */
case class S3MultipartConfig(
  partSize: Long = 64L * 1024 * 1024,            // 64MB default part size
  multipartThreshold: Long = 100L * 1024 * 1024, // 100MB threshold for multipart
  maxConcurrency: Int = 4,                       // Maximum parallel part uploads
  maxRetries: Int = 3,                           // Retry attempts per part
  baseRetryDelay: Long = 1000,                   // Base retry delay in ms
  uploadTimeout: scala.concurrent.duration.Duration = scala.concurrent.duration.Duration(30, "minutes"))

object S3MultipartConfig {
  def default: S3MultipartConfig = S3MultipartConfig()

  def forLargeMergedSplits: S3MultipartConfig = S3MultipartConfig(
    partSize = 128L * 1024 * 1024,                                    // 128MB parts for large splits
    multipartThreshold = 200L * 1024 * 1024,                          // 200MB threshold
    maxConcurrency = 6,                                               // Higher concurrency for merge operations
    maxRetries = 5,                                                   // More retries for critical merge uploads
    uploadTimeout = scala.concurrent.duration.Duration(60, "minutes") // Longer timeout
  )
}

/** Result of S3 upload operation */
case class S3UploadResult(
  bucket: String,
  key: String,
  etag: String,
  uploadId: Option[String],
  partCount: Int,
  totalSize: Long,
  uploadTimeMs: Long,
  strategy: String) {
  def uploadRateMBps: Double = (totalSize.toDouble / (1024 * 1024)) / (uploadTimeMs.toDouble / 1000)

  def s3Url: String = s"s3://$bucket/$key"
}
