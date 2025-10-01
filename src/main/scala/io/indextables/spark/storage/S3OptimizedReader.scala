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

package io.indextables.spark.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, HeadObjectRequest}
import org.slf4j.LoggerFactory
import io.indextables.spark.util.ErrorUtil
import java.io.ByteArrayOutputStream
import java.util.concurrent.{ConcurrentHashMap, Executors, CompletableFuture}

class S3OptimizedReader(path: Path, conf: Configuration) extends StorageStrategy {

  private val logger = LoggerFactory.getLogger(classOf[S3OptimizedReader])
  private val uri    = path.toUri
  private val bucket = uri.getHost
  private val key    = uri.getPath.substring(1) // Remove leading slash

  private val s3Client: S3Client = {
    val region = Option(conf.get("fs.s3a.endpoint.region"))
      .orElse(Option(System.getProperty("aws.region")))
      .map(Region.of)
      .getOrElse(Region.US_EAST_1)

    S3Client
      .builder()
      .region(region)
      .credentialsProvider(DefaultCredentialsProvider.create())
      .build()
  }

  private val executor         = Executors.newCachedThreadPool()
  private val chunkCache       = new ConcurrentHashMap[Long, Array[Byte]]()
  private val defaultChunkSize = 1024 * 1024 // 1MB chunks

  private lazy val fileSize: Long =
    try {
      val headRequest = HeadObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .build()

      val response = s3Client.headObject(headRequest)
      response.contentLength()
    } catch {
      case ex: Exception =>
        ErrorUtil.logAndThrow(logger, s"Failed to get file size for s3://$bucket/$key", ex)
    }

  override def readFile(): Array[Byte] = {
    logger.info(s"Reading entire file s3://$bucket/$key ($fileSize bytes)")

    if (fileSize <= defaultChunkSize) {
      readRange(0, fileSize)
    } else {
      // Read file in parallel chunks
      val numChunks = ((fileSize + defaultChunkSize - 1) / defaultChunkSize).toInt
      val futures = (0 until numChunks).map { chunkIndex =>
        val offset = chunkIndex * defaultChunkSize
        val length = Math.min(defaultChunkSize, fileSize - offset)

        CompletableFuture.supplyAsync(() => readRange(offset, length), executor)
      }

      try {
        val chunks = futures.map(_.get())
        val result = new ByteArrayOutputStream(fileSize.toInt)
        chunks.foreach(chunk => result.write(chunk))
        result.toByteArray
      } catch {
        case ex: Exception =>
          ErrorUtil.logAndThrow(logger, s"Failed to read file s3://$bucket/$key", ex)
      }
    }
  }

  override def readRange(offset: Long, length: Long): Array[Byte] = {
    val chunkKey = (offset / defaultChunkSize) * defaultChunkSize

    // Check cache first
    Option(chunkCache.get(chunkKey)) match {
      case Some(cachedChunk) =>
        val chunkOffset  = (offset - chunkKey).toInt
        val actualLength = Math.min(length, cachedChunk.length - chunkOffset).toInt
        val result       = new Array[Byte](actualLength)
        System.arraycopy(cachedChunk, chunkOffset, result, 0, actualLength)
        result

      case None =>
        val chunkLength = Math.min(defaultChunkSize, fileSize - chunkKey)
        val chunk       = readS3Range(chunkKey, chunkLength)

        // Cache the chunk
        chunkCache.put(chunkKey, chunk)

        // Predictively read next chunk if this is a sequential read
        if (offset + length > chunkKey + chunkLength) {
          val nextChunkKey = chunkKey + defaultChunkSize
          if (nextChunkKey < fileSize && !chunkCache.containsKey(nextChunkKey)) {
            CompletableFuture.runAsync(
              () =>
                try {
                  val nextChunkLength = Math.min(defaultChunkSize, fileSize - nextChunkKey)
                  val nextChunk       = readS3Range(nextChunkKey, nextChunkLength)
                  chunkCache.put(nextChunkKey, nextChunk)
                  logger.debug(s"Predictively cached chunk at offset $nextChunkKey")
                } catch {
                  case ex: Exception =>
                    logger.warn(s"Failed to predictively read chunk at offset $nextChunkKey", ex)
                },
              executor
            )
          }
        }

        // Extract requested range from chunk
        val chunkOffset  = (offset - chunkKey).toInt
        val actualLength = Math.min(length, chunk.length - chunkOffset).toInt
        val result       = new Array[Byte](actualLength)
        System.arraycopy(chunk, chunkOffset, result, 0, actualLength)
        result
    }
  }

  private def readS3Range(offset: Long, length: Long): Array[Byte] = {
    val rangeHeader = s"bytes=$offset-${offset + length - 1}"

    val getRequest = GetObjectRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .range(rangeHeader)
      .build()

    try {
      logger.debug(s"Reading S3 range: $rangeHeader from s3://$bucket/$key")
      val response = s3Client.getObject(getRequest)
      response.readAllBytes()
    } catch {
      case ex: Exception =>
        ErrorUtil.logAndThrow(logger, s"Failed to read range $rangeHeader from s3://$bucket/$key", ex)
    }
  }

  override def getFileSize(): Long = fileSize

  override def close(): Unit = {
    executor.shutdown()
    chunkCache.clear()
    s3Client.close()
    logger.debug(s"Closed S3OptimizedReader for s3://$bucket/$key")
  }
}
