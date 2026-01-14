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

import java.io.File
import java.nio.file.{Files, Paths}

import io.indextables.spark.io.{CloudStorageProvider, CloudStorageProviderFactory}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

/**
 * Helper for uploading merged splits to cloud storage.
 *
 * Uses the existing CloudStorageProvider infrastructure for uploads, including multipart uploads for large files.
 */
object MergeUploader {

  private val logger = LoggerFactory.getLogger(getClass)

  // Threshold for using streaming uploads (100MB)
  private val STREAMING_THRESHOLD = 100L * 1024 * 1024

  /**
   * Upload a local file to cloud storage.
   *
   * @param localPath
   *   Path to the local file to upload
   * @param destPath
   *   Destination cloud storage path
   * @param configs
   *   Configuration map with cloud provider settings
   * @param hadoopConf
   *   Hadoop configuration (optional)
   * @return
   *   The number of bytes uploaded
   */
  def upload(
    localPath: String,
    destPath: String,
    configs: Map[String, String],
    hadoopConf: Configuration = new Configuration()
  ): Long = {
    val localFile = new File(localPath)
    val fileSize  = localFile.length()

    logger.info(s"Uploading merged split: $localPath -> $destPath ($fileSize bytes)")

    // Determine if this is a cloud path
    val isCloudPath = destPath.startsWith("s3://") || destPath.startsWith("s3a://") ||
      destPath.startsWith("azure://") || destPath.startsWith("wasb://") ||
      destPath.startsWith("wasbs://") || destPath.startsWith("abfs://") ||
      destPath.startsWith("abfss://")

    if (!isCloudPath) {
      // Local path - just copy the file
      val destFile = new File(destPath)
      destFile.getParentFile.mkdirs()
      Files.copy(localFile.toPath, destFile.toPath)
      logger.info(s"Copied merged split to local path: $destPath")
      return fileSize
    }

    // Create cloud storage provider
    val options = new CaseInsensitiveStringMap(configs.asJava)
    val cloudProvider = CloudStorageProviderFactory.createProvider(destPath, options, hadoopConf)

    try {
      if (fileSize > STREAMING_THRESHOLD) {
        // Use streaming upload for large files (memory-efficient)
        logger.info(s"Using streaming upload for large split: ${fileSize / (1024 * 1024)} MB")
        val inputStream = Files.newInputStream(localFile.toPath)
        try {
          cloudProvider.writeFileFromStream(destPath, inputStream, Some(fileSize))
        } finally {
          inputStream.close()
        }
      } else {
        // Use traditional upload for smaller files
        logger.info(s"Using traditional upload for split: ${fileSize / (1024 * 1024)} MB")
        val content = Files.readAllBytes(localFile.toPath)
        cloudProvider.writeFile(destPath, content)
      }

      logger.info(s"Upload completed: $destPath ($fileSize bytes)")
      fileSize
    } finally {
      cloudProvider.close()
    }
  }

  /**
   * Upload a local file to cloud storage with retry logic.
   *
   * @param localPath
   *   Path to the local file to upload
   * @param destPath
   *   Destination cloud storage path
   * @param configs
   *   Configuration map with cloud provider settings
   * @param hadoopConf
   *   Hadoop configuration (optional)
   * @param maxRetries
   *   Maximum number of retry attempts (default: 3)
   * @return
   *   The number of bytes uploaded
   */
  def uploadWithRetry(
    localPath: String,
    destPath: String,
    configs: Map[String, String],
    hadoopConf: Configuration = new Configuration(),
    maxRetries: Int = 3
  ): Long = {
    var lastException: Throwable = null
    var attempt                  = 0

    while (attempt < maxRetries) {
      attempt += 1
      try {
        return upload(localPath, destPath, configs, hadoopConf)
      } catch {
        case e: Exception =>
          lastException = e
          if (attempt < maxRetries) {
            val delayMs = 1000L * math.pow(2, attempt - 1).toLong
            logger.warn(s"Upload attempt $attempt failed, retrying in ${delayMs}ms: ${e.getMessage}")
            Thread.sleep(delayMs)
          }
      }
    }

    throw new RuntimeException(s"Upload failed after $maxRetries attempts: $destPath", lastException)
  }
}
