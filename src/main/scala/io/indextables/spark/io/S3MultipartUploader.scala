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

package io.indextables.spark.io

import java.io.InputStream
import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}
import java.util.concurrent.{CompletableFuture, Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Client}
import software.amazon.awssdk.services.s3.model._

/**
 * High-performance S3 multipart uploader with parallel part uploads using AWS async SDK.
 *
 * Features:
 *   - TRUE async uploads using S3AsyncClient (non-blocking I/O)
 *   - Parallel upload of multiple parts with bounded queue
 *   - Configurable part size (minimum 5MB for S3, default 128MB)
 *   - Automatic retry logic for failed parts
 *   - Efficient memory management with streaming
 *   - Progress tracking and monitoring
 *   - Fallback to single-part upload for small files
 */
class S3MultipartUploader(
  s3Client: S3Client,
  s3AsyncClient: S3AsyncClient,
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

  /**
   * Upload a local file using memory-mapped I/O for zero-copy uploads. This is the most efficient upload method as it:
   *   - Never loads the file into JVM heap
   *   - Uses OS-level page cache for I/O
   *   - Creates zero-copy ByteBuffer views for each part
   *   - Leverages async S3 client for true non-blocking uploads
   *
   * @param bucket
   *   S3 bucket name
   * @param key
   *   S3 object key
   * @param filePath
   *   Local file path to upload
   * @return
   *   Upload result with metadata
   */
  def uploadFileWithMemoryMapping(
    bucket: String,
    key: String,
    filePath: Path
  ): S3UploadResult = {
    val fileSize = filePath.toFile.length()

    if (fileSize < config.multipartThreshold) {
      // For small files, use single-part upload (still memory-mapped, just one part)
      logger.info(s"Using single-part memory-mapped upload: s3://$bucket/$key (${formatBytes(fileSize)})")
      uploadSinglePartMemoryMapped(bucket, key, filePath, fileSize)
    } else {
      // For large files, use multipart with memory-mapped I/O
      logger.info(s"Using multipart memory-mapped upload: s3://$bucket/$key (${formatBytes(fileSize)})")
      uploadMultipartMemoryMapped(bucket, key, filePath, fileSize)
    }
  }

  /** Single-part upload using memory-mapped I/O (zero-copy) */
  private def uploadSinglePartMemoryMapped(
    bucket: String,
    key: String,
    filePath: Path,
    fileSize: Long
  ): S3UploadResult = {
    val startTime = System.currentTimeMillis()

    var fileChannel: FileChannel = null
    try {
      // Open file channel and memory-map the file
      fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)
      val mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize)

      // Create request
      val request = PutObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .contentLength(fileSize)
        .build()

      // Upload using AsyncRequestBody with mapped buffer (zero-copy!)
      val asyncBody    = AsyncRequestBody.fromByteBuffer(mappedBuffer)
      val uploadFuture = s3AsyncClient.putObject(request, asyncBody)

      // Wait for completion
      val response   = uploadFuture.get()
      val uploadTime = System.currentTimeMillis() - startTime

      logger.info(s"✅ Single-part memory-mapped upload completed: s3://$bucket/$key in ${uploadTime}ms")

      S3UploadResult(
        bucket = bucket,
        key = key,
        etag = response.eTag(),
        uploadId = None,
        partCount = 1,
        totalSize = fileSize,
        uploadTimeMs = uploadTime,
        strategy = "single-part-mmap"
      )
    } catch {
      case ex: Exception =>
        logger.error(s"❌ Single-part memory-mapped upload failed: s3://$bucket/$key", ex)
        throw new RuntimeException(s"Single-part memory-mapped upload failed: ${ex.getMessage}", ex)
    } finally
      if (fileChannel != null) fileChannel.close()
  }

  /** Multipart upload using memory-mapped I/O with zero-copy parts */
  private def uploadMultipartMemoryMapped(
    bucket: String,
    key: String,
    filePath: Path,
    fileSize: Long
  ): S3UploadResult = {
    val startTime = System.currentTimeMillis()

    // Calculate part size and count
    val partSize  = calculateOptimalPartSize(fileSize)
    val partCount = ((fileSize + partSize - 1) / partSize).toInt

    logger.info(s"🚀 Starting memory-mapped multipart upload: s3://$bucket/$key")
    logger.info(s"   Total size: ${formatBytes(fileSize)}")
    logger.info(s"   Part size: ${formatBytes(partSize)}")
    logger.info(s"   Part count: $partCount")
    logger.info(s"   Max concurrency: ${config.maxConcurrency}")

    var uploadId: String         = null
    var fileChannel: FileChannel = null

    try {
      // Open file channel for memory mapping
      fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)

      // 1. Initiate multipart upload
      val createRequest = CreateMultipartUploadRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .build()

      uploadId = s3Client.createMultipartUpload(createRequest).uploadId()
      logger.debug(s"✅ Multipart upload initiated: uploadId=$uploadId")

      // 2. Upload all parts in parallel using memory-mapped buffers (zero-copy!)
      val etags = uploadMemoryMappedPartsInParallel(
        bucket,
        key,
        uploadId,
        fileChannel,
        fileSize,
        partSize,
        partCount
      )

      // 3. Complete multipart upload
      val completedParts = etags.zipWithIndex.map {
        case (etag, index) =>
          CompletedPart.builder().partNumber(index + 1).eTag(etag).build()
      }

      val completeRequest = CompleteMultipartUploadRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .uploadId(uploadId)
        .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts: _*).build())
        .build()

      val completeResponse = s3Client.completeMultipartUpload(completeRequest)
      val uploadTime       = System.currentTimeMillis() - startTime

      logger.info(s"✅ Memory-mapped multipart upload completed: s3://$bucket/$key")
      logger.info(s"   Upload time: ${uploadTime}ms")
      logger.info(s"   Upload rate: ${formatBytes((fileSize.toDouble / (uploadTime / 1000.0)).toLong)}/s")

      S3UploadResult(
        bucket = bucket,
        key = key,
        etag = completeResponse.eTag(),
        uploadId = Some(uploadId),
        partCount = partCount,
        totalSize = fileSize,
        uploadTimeMs = uploadTime,
        strategy = "multipart-mmap"
      )
    } catch {
      case ex: Exception =>
        // Abort the multipart upload on failure
        if (uploadId != null) {
          try {
            val abortRequest = AbortMultipartUploadRequest
              .builder()
              .bucket(bucket)
              .key(key)
              .uploadId(uploadId)
              .build()
            s3Client.abortMultipartUpload(abortRequest)
            logger.info(s"⚠️ Multipart upload aborted: uploadId=$uploadId")
          } catch {
            case abortEx: Exception =>
              logger.error(s"❌ Failed to abort multipart upload: uploadId=$uploadId", abortEx)
          }
        }

        logger.error(s"❌ Memory-mapped multipart upload failed: s3://$bucket/$key", ex)
        throw new RuntimeException(s"Memory-mapped multipart upload failed: ${ex.getMessage}", ex)
    } finally
      if (fileChannel != null) fileChannel.close()
  }

  /**
   * Upload parts in parallel using memory-mapped ByteBuffers (zero-copy). Creates slice views of the mapped file
   * without copying data into JVM heap.
   */
  private def uploadMemoryMappedPartsInParallel(
    bucket: String,
    key: String,
    uploadId: String,
    fileChannel: FileChannel,
    fileSize: Long,
    partSize: Long,
    partCount: Int
  ): Array[String] = {

    // Create CompletableFutures for all parts using memory-mapped buffers
    val futures = (0 until partCount).map { partIndex =>
      val partNumber      = partIndex + 1
      val startOffset     = partIndex * partSize
      val currentPartSize = math.min(partSize, fileSize - startOffset)

      // Memory-map this specific part (zero-copy view!)
      val partBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, startOffset, currentPartSize)

      // Upload this part asynchronously
      uploadPartFromMemoryMappedBuffer(bucket, key, uploadId, partNumber, partBuffer, currentPartSize)
    }.toArray

    // Wait for all uploads to complete
    try {
      CompletableFuture.allOf(futures: _*).get()
      futures.map(_.get())
    } catch {
      case ex: Exception =>
        logger.error(s"❌ Memory-mapped parallel upload failed for s3://$bucket/$key", ex)
        throw new RuntimeException(s"Memory-mapped parallel upload failed: ${ex.getMessage}", ex)
    }
  }

  /** Upload a single part from a memory-mapped ByteBuffer with retry logic - TRUE ASYNC */
  private def uploadPartFromMemoryMappedBuffer(
    bucket: String,
    key: String,
    uploadId: String,
    partNumber: Int,
    partBuffer: java.nio.MappedByteBuffer,
    partSize: Long
  ): CompletableFuture[String] = {

    def attemptUpload(attemptNum: Int, startTime: Long): CompletableFuture[String] = {
      // Reset buffer position for retry attempts
      partBuffer.rewind()

      val request = UploadPartRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .uploadId(uploadId)
        .partNumber(partNumber)
        .contentLength(partSize)
        .build()

      // Create async body from memory-mapped buffer (zero-copy!)
      val asyncBody = AsyncRequestBody.fromByteBuffer(partBuffer)

      // Start async upload - NO BLOCKING!
      val uploadFuture = s3AsyncClient.uploadPart(request, asyncBody)

      // Chain callbacks without blocking
      val resultFuture = new CompletableFuture[String]()

      uploadFuture.whenComplete(
        new java.util.function.BiConsumer[software.amazon.awssdk.services.s3.model.UploadPartResponse, Throwable] {
          override def accept(response: software.amazon.awssdk.services.s3.model.UploadPartResponse, ex: Throwable)
            : Unit =
            if (ex != null) {
              // Upload failed - retry if we have attempts left
              if (attemptNum < config.maxRetries) {
                val delay = config.baseRetryDelay * math.pow(2, attemptNum - 1).toLong
                logger.warn(
                  s"⚠️ Part $partNumber memory-mapped upload failed (attempt $attemptNum/${config.maxRetries}), retrying in ${delay}ms",
                  ex
                )

                // Schedule retry asynchronously (no thread blocking!)
                val scheduler = java.util.concurrent.Executors.newScheduledThreadPool(1)
                scheduler.schedule(
                  new Runnable {
                    override def run(): Unit =
                      try
                        // Retry upload (recursive call)
                        attemptUpload(attemptNum + 1, startTime).whenComplete(
                          new java.util.function.BiConsumer[String, Throwable] {
                            override def accept(retryResult: String, retryEx: Throwable): Unit = {
                              if (retryEx != null) resultFuture.completeExceptionally(retryEx)
                              else resultFuture.complete(retryResult)
                              scheduler.shutdown()
                            }
                          }
                        )
                      catch {
                        case retryEx: Exception =>
                          resultFuture.completeExceptionally(retryEx)
                          scheduler.shutdown()
                      }
                  },
                  delay,
                  java.util.concurrent.TimeUnit.MILLISECONDS
                )
              } else {
                // All retries exhausted
                resultFuture.completeExceptionally(
                  new RuntimeException(
                    s"Part $partNumber memory-mapped upload failed after ${config.maxRetries} attempts",
                    ex
                  )
                )
              }
            } else {
              // Success!
              val partTime = System.currentTimeMillis() - startTime
              logger.debug(
                s"✅ Part $partNumber uploaded (EVENT-BASED ASYNC ZERO-COPY): ${formatBytes(partSize)} in ${partTime}ms"
              )
              resultFuture.complete(response.eTag())
            }
        }
      )

      resultFuture
    }

    // Start first attempt
    attemptUpload(1, System.currentTimeMillis())
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

  /**
   * Upload parts in parallel using CompletableFuture for true async I/O. Uses S3AsyncClient with
   * CompletableFuture.allOf() to avoid blocking.
   */
  private def uploadPartsInParallel(
    bucket: String,
    key: String,
    uploadId: String,
    content: Array[Byte],
    partSize: Long,
    partCount: Int
  ): Array[String] = {

    // Create CompletableFutures for all parts (no blocking!)
    val futures = (0 until partCount).map { partIndex =>
      val partNumber  = partIndex + 1
      val startOffset = partIndex * partSize
      val endOffset   = math.min(startOffset + partSize, content.length)
      val partData    = java.util.Arrays.copyOfRange(content, startOffset.toInt, endOffset.toInt)

      // Return CompletableFuture that uploads this part asynchronously
      uploadSinglePartAsyncNonBlocking(bucket, key, uploadId, partNumber, partData)
    }.toArray

    // Wait for all uploads to complete (only blocks here at the end, not per-part)
    try {
      CompletableFuture.allOf(futures: _*).get()

      // Extract ETags from completed futures
      futures.map(_.get())
    } catch {
      case ex: Exception =>
        logger.error(s"❌ Parallel upload failed for s3://$bucket/$key", ex)
        throw new RuntimeException(s"Parallel upload failed: ${ex.getMessage}", ex)
    }
  }

  /**
   * Upload a single part using ASYNC S3 client with retry logic. This provides true async I/O instead of just
   * threading. DEPRECATED: Blocks on .get() - use uploadSinglePartAsyncNonBlocking instead
   */
  private def uploadSinglePartAsync(
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

        // Use ASYNC S3 client - true non-blocking I/O
        val asyncBody = AsyncRequestBody.fromBytes(partData)
        val future    = s3AsyncClient.uploadPart(request, asyncBody)

        // Wait for async upload to complete (blocks current worker thread but S3 I/O is async)
        val response = future.get()
        val partTime = System.currentTimeMillis() - partStartTime

        logger.debug(s"✅ Part $partNumber uploaded (ASYNC): ${formatBytes(partData.length)} in ${partTime}ms")
        return response.eTag()

      } catch {
        case ex: Exception =>
          attempt += 1
          lastException = ex

          if (attempt < config.maxRetries) {
            val delay = config.baseRetryDelay * math.pow(2, attempt - 1).toLong
            logger.warn(
              s"⚠️ Part $partNumber async upload failed (attempt $attempt/${config.maxRetries}), retrying in ${delay}ms",
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

    throw new RuntimeException(
      s"Part $partNumber async upload failed after ${config.maxRetries} attempts",
      lastException
    )
  }

  /**
   * Upload a single part using ASYNC S3 client with retry logic - NON-BLOCKING version. Returns
   * CompletableFuture[String] to allow true async composition without blocking.
   */
  private def uploadSinglePartAsyncNonBlocking(
    bucket: String,
    key: String,
    uploadId: String,
    partNumber: Int,
    partData: Array[Byte]
  ): CompletableFuture[String] = {

    def attemptUpload(attemptNum: Int, startTime: Long): CompletableFuture[String] = {
      val request = UploadPartRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .uploadId(uploadId)
        .partNumber(partNumber)
        .contentLength(partData.length.toLong)
        .build()

      val asyncBody = AsyncRequestBody.fromBytes(partData)

      // Start async upload - NO BLOCKING!
      val uploadFuture = s3AsyncClient.uploadPart(request, asyncBody)

      // Chain callbacks without blocking
      val resultFuture = new CompletableFuture[String]()

      uploadFuture.whenComplete(
        new java.util.function.BiConsumer[software.amazon.awssdk.services.s3.model.UploadPartResponse, Throwable] {
          override def accept(response: software.amazon.awssdk.services.s3.model.UploadPartResponse, ex: Throwable)
            : Unit =
            if (ex != null) {
              // Upload failed - retry if we have attempts left
              if (attemptNum < config.maxRetries) {
                val delay = config.baseRetryDelay * math.pow(2, attemptNum - 1).toLong
                logger.warn(
                  s"⚠️ Part $partNumber async upload failed (attempt $attemptNum/${config.maxRetries}), retrying in ${delay}ms",
                  ex
                )

                // Schedule retry asynchronously (no thread blocking!)
                val scheduler = java.util.concurrent.Executors.newScheduledThreadPool(1)
                scheduler.schedule(
                  new Runnable {
                    override def run(): Unit =
                      try
                        // Retry upload (recursive call)
                        attemptUpload(attemptNum + 1, startTime).whenComplete(
                          new java.util.function.BiConsumer[String, Throwable] {
                            override def accept(retryResult: String, retryEx: Throwable): Unit = {
                              if (retryEx != null) resultFuture.completeExceptionally(retryEx)
                              else resultFuture.complete(retryResult)
                              scheduler.shutdown()
                            }
                          }
                        )
                      catch {
                        case retryEx: Exception =>
                          resultFuture.completeExceptionally(retryEx)
                          scheduler.shutdown()
                      }
                  },
                  delay,
                  java.util.concurrent.TimeUnit.MILLISECONDS
                )
              } else {
                // All retries exhausted
                resultFuture.completeExceptionally(
                  new RuntimeException(
                    s"Part $partNumber async upload failed after ${config.maxRetries} attempts",
                    ex
                  )
                )
              }
            } else {
              // Success!
              val partTime = System.currentTimeMillis() - startTime
              logger.debug(
                s"✅ Part $partNumber uploaded (EVENT-BASED ASYNC): ${formatBytes(partData.length)} in ${partTime}ms"
              )
              resultFuture.complete(response.eTag())
            }
        }
      )

      resultFuture
    }

    // Start first attempt
    attemptUpload(1, System.currentTimeMillis())
  }

  /**
   * Multipart upload from input stream with parallel uploads and minimal memory buffering.
   *
   * Uses a producer-consumer pattern with bounded queue:
   *   - Producer thread: Reads from InputStream in chunks, adds to queue
   *   - Consumer threads: Pull from queue and upload parts in parallel
   *   - Bounded queue: Limits memory usage (maxQueueSize * partSize)
   *
   * This minimizes memory while maximizing upload parallelism.
   */
  private def uploadMultipartFromStream(
    bucket: String,
    key: String,
    inputStream: InputStream,
    contentLength: Option[Long]
  ): S3UploadResult = {

    val startTime = System.currentTimeMillis()

    logger.info(s"🚀 Starting parallel multipart upload from stream: s3://$bucket/$key")
    logger.info(s"   Part size: ${formatBytes(config.partSize)}")
    logger.info(s"   Max concurrency: ${config.maxConcurrency}")
    logger.info(s"   Max queue size: ${config.maxQueueSize}")

    var uploadId: String = null
    val uploadedParts    = new java.util.concurrent.ConcurrentHashMap[Int, String]()
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

      // 2. Use bounded queue for producer-consumer pattern
      val partQueue =
        new java.util.concurrent.ArrayBlockingQueue[(Int, Array[Byte], Int)](config.maxQueueSize)
      val producerError  = new java.util.concurrent.atomic.AtomicReference[Exception](null)
      val consumerErrors = new java.util.concurrent.ConcurrentLinkedQueue[Exception]()
      val partSizes      = new java.util.concurrent.ConcurrentHashMap[Int, Long]()

      // Producer thread: Read from stream and queue parts
      val producerFuture = CompletableFuture.runAsync(
        new Runnable {
          override def run(): Unit =
            try {
              val buffer     = new Array[Byte](config.partSize.toInt)
              var partNumber = 1
              var bytesRead  = 0

              while ({ bytesRead = readFully(inputStream, buffer); bytesRead } > 0) {
                val partData =
                  if (bytesRead == buffer.length) buffer.clone() else java.util.Arrays.copyOf(buffer, bytesRead)

                // This blocks if queue is full - natural backpressure
                partQueue.put((partNumber, partData, bytesRead))

                logger.debug(s"📥 Queued part $partNumber: ${formatBytes(partData.length)}")
                partNumber += 1
              }

              // Signal end of stream with sentinel value
              partQueue.put((-1, Array.empty, 0))
              logger.info(s"✅ Producer finished reading ${partNumber - 1} parts from stream")

            } catch {
              case ex: Exception =>
                producerError.set(ex)
                logger.error("❌ Producer thread failed", ex)
                // Try to signal consumers to stop
                try partQueue.put((-1, Array.empty, 0))
                catch { case _: InterruptedException => () }
            }
        },
        uploadExecutor
      )

      // Consumer threads: Upload parts in parallel using ASYNC S3 client
      val consumerFutures = (1 to config.maxConcurrency).map { workerId =>
        CompletableFuture.runAsync(
          new Runnable {
            override def run(): Unit =
              try {
                var continue = true
                while (continue) {
                  val (partNumber, partData, partSize) = partQueue.take()

                  if (partNumber == -1) {
                    // End-of-stream sentinel - put it back for other consumers and exit
                    partQueue.put((-1, Array.empty, 0))
                    continue = false
                  } else {
                    // Upload this part using ASYNC S3 client (non-blocking I/O)
                    logger.debug(s"🔼 Worker $workerId uploading part $partNumber: ${formatBytes(partData.length)}")
                    val etag = uploadSinglePartAsync(bucket, key, uploadId, partNumber, partData)
                    uploadedParts.put(partNumber, etag)
                    partSizes.put(partNumber, partSize.toLong)
                    logger.debug(s"✅ Worker $workerId completed part $partNumber")
                  }
                }
                logger.debug(s"✅ Worker $workerId finished")
              } catch {
                case ex: Exception =>
                  consumerErrors.add(ex)
                  logger.error(s"❌ Worker $workerId failed", ex)
              }
          },
          uploadExecutor
        )
      }.toList

      // Wait for producer and all consumers to complete
      CompletableFuture.allOf((producerFuture :: consumerFutures): _*).join()

      // Check for errors
      if (producerError.get() != null) {
        throw new RuntimeException("Producer thread failed", producerError.get())
      }
      if (!consumerErrors.isEmpty) {
        throw new RuntimeException("Consumer threads failed", consumerErrors.peek())
      }

      // Calculate total bytes from uploaded parts (sum actual part sizes)
      totalBytesRead = partSizes.values().asScala.map(_.toLong).sum

      // 3. Complete multipart upload
      val sortedParts = uploadedParts.asScala.toSeq.sortBy(_._1)
      val completedParts = sortedParts
        .map {
          case (partNum, etag) =>
            CompletedPart
              .builder()
              .partNumber(partNum)
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

      logger.info(s"✅ Parallel streaming multipart upload completed: s3://$bucket/$key in ${uploadTime}ms")
      logger.info(s"   Parts count: ${uploadedParts.size()}")
      logger.info(s"   Upload rate: ${formatBytes((totalBytesRead * 1000) / uploadTime)}/s")

      S3UploadResult(
        bucket = bucket,
        key = key,
        etag = completeResponse.eTag(),
        uploadId = Some(uploadId),
        partCount = uploadedParts.size(),
        totalSize = totalBytesRead,
        uploadTimeMs = uploadTime,
        strategy = "multipart-stream-parallel"
      )

    } catch {
      case ex: Exception =>
        logger.error(s"❌ Parallel streaming multipart upload failed: s3://$bucket/$key", ex)

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

        throw new RuntimeException(s"Parallel streaming multipart upload failed: ${ex.getMessage}", ex)
    }
  }

  /** Read from input stream fully, filling the buffer as much as possible. Returns the total number of bytes read. */
  private def readFully(inputStream: InputStream, buffer: Array[Byte]): Int = {
    var totalRead = 0
    var bytesRead = 0

    while (
      totalRead < buffer.length && {
        bytesRead = inputStream.read(buffer, totalRead, buffer.length - totalRead); bytesRead
      } != -1
    )
      totalRead += bytesRead

    totalRead
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
  partSize: Long = 128L * 1024 * 1024,           // 128MB default part size (configurable)
  multipartThreshold: Long = 200L * 1024 * 1024, // 200MB threshold for multipart
  maxConcurrency: Int = 4,                       // Maximum parallel part uploads
  maxQueueSize: Int = 3,                         // Max parts buffered in memory (maxQueueSize * partSize = max memory)
  maxRetries: Int = 3,                           // Retry attempts per part
  baseRetryDelay: Long = 1000,                   // Base retry delay in ms
  uploadTimeout: scala.concurrent.duration.Duration = scala.concurrent.duration.Duration(30, "minutes"))

object S3MultipartConfig {
  def default: S3MultipartConfig = S3MultipartConfig()

  def forLargeMergedSplits: S3MultipartConfig = S3MultipartConfig(
    partSize = 128L * 1024 * 1024,                                    // 128MB parts for large splits
    multipartThreshold = 200L * 1024 * 1024,                          // 200MB threshold
    maxConcurrency = 6,                                               // Higher concurrency for merge operations
    maxQueueSize = 4,                                                 // 512MB max buffered (4 * 128MB)
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
