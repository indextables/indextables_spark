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
import java.net.URI
import java.nio.file.{Files, Paths}

import io.indextables.spark.utils.CredentialProviderFactory

import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import org.slf4j.LoggerFactory

/**
 * Helper for uploading merged splits to cloud storage.
 *
 * Uses the same credential resolution as S3AsyncDownloader for consistency:
 * CredentialProviderFactory.resolveAWSCredentialsFromConfig(configs, tablePath)
 */
object MergeUploader {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Upload a local file to S3.
   *
   * Uses the same credential resolution pattern as S3AsyncDownloader.fromConfig()
   * to ensure Unity Catalog and other custom credential providers work consistently.
   *
   * @param localPath
   *   Path to the local file to upload
   * @param destPath
   *   Destination S3 path (s3:// or s3a://)
   * @param configs
   *   Configuration map with cloud provider settings
   * @param tablePath
   *   Table path for credential provider resolution
   * @return
   *   The number of bytes uploaded
   */
  def upload(
    localPath: String,
    destPath: String,
    configs: Map[String, String],
    tablePath: String
  ): Long = {
    val localFile = new File(localPath)
    val fileSize  = localFile.length()

    logger.info(s"Uploading merged split: $localPath -> $destPath ($fileSize bytes)")

    // Determine if this is a cloud path
    val isS3Path = destPath.startsWith("s3://") || destPath.startsWith("s3a://")

    if (!isS3Path) {
      // Local path - just copy the file
      val destFile = new File(destPath)
      destFile.getParentFile.mkdirs()
      Files.copy(localFile.toPath, destFile.toPath)
      logger.info(s"Copied merged split to local path: $destPath")
      return fileSize
    }

    // Use the SAME credential resolution as S3AsyncDownloader
    val s3Client = createS3Client(configs, tablePath)

    try {
      // Parse S3 path
      val (bucket, key) = parseS3Path(destPath)

      logger.info(s"Uploading to S3: bucket=$bucket, key=$key")

      // Upload file
      val putRequest = PutObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build()

      s3Client.putObject(putRequest, RequestBody.fromFile(localFile))

      logger.info(s"Upload completed: $destPath ($fileSize bytes)")
      fileSize
    } finally {
      s3Client.close()
    }
  }

  /**
   * Create S3 client using the same credential resolution as S3AsyncDownloader.
   * This ensures Unity Catalog and other custom credential providers work.
   */
  private def createS3Client(configs: Map[String, String], tablePath: String): S3Client = {
    def get(key: String): Option[String] =
      configs.get(key).orElse(configs.get(key.toLowerCase)).filter(_.nonEmpty)

    // Use centralized credential resolution (SAME as S3AsyncDownloader.fromConfig)
    val resolvedCreds = CredentialProviderFactory.resolveAWSCredentialsFromConfig(configs, tablePath)

    val credentialsProvider: AwsCredentialsProvider = resolvedCreds match {
      case Some(creds) =>
        logger.info(s"Using resolved credentials from provider: accessKey=${creds.accessKey.take(4)}...")
        creds.sessionToken match {
          case Some(token) =>
            StaticCredentialsProvider.create(AwsSessionCredentials.create(creds.accessKey, creds.secretKey, token))
          case None =>
            StaticCredentialsProvider.create(AwsBasicCredentials.create(creds.accessKey, creds.secretKey))
        }
      case None =>
        logger.info("No credentials resolved from provider, using default credentials provider chain")
        DefaultCredentialsProvider.create()
    }

    val builder = S3Client.builder()
      .credentialsProvider(credentialsProvider)

    // Configure region
    get("spark.indextables.aws.region").foreach { region =>
      logger.debug(s"S3Client: Setting region to $region")
      builder.region(Region.of(region))
    }

    // Configure endpoint (for S3-compatible services)
    get("spark.indextables.aws.endpoint").foreach { endpoint =>
      val isStandardAwsEndpoint = endpoint.contains("s3.amazonaws.com") || endpoint.contains("amazonaws.com")

      if (!isStandardAwsEndpoint || get("spark.indextables.aws.region").isEmpty) {
        try {
          val endpointUri = if (endpoint.startsWith("http://") || endpoint.startsWith("https://")) {
            URI.create(endpoint)
          } else {
            URI.create(s"https://$endpoint")
          }

          logger.debug(s"S3Client: Setting endpoint to $endpointUri")
          builder.endpointOverride(endpointUri)

          if (get("spark.indextables.aws.pathStyleAccess").exists(_.toBoolean)) {
            builder.forcePathStyle(true)
          }
        } catch {
          case ex: Exception =>
            logger.error(s"Failed to parse S3 endpoint: $endpoint", ex)
        }
      }
    }

    builder.build()
  }

  /**
   * Parse an S3 path into bucket and key.
   */
  private def parseS3Path(path: String): (String, String) = {
    val normalizedPath = path.replaceFirst("^s3a://", "s3://")
    val uri    = new URI(normalizedPath)
    val bucket = uri.getHost
    val key    = uri.getPath.stripPrefix("/")
    (bucket, key)
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
   * @param tablePath
   *   Table path for credential provider resolution
   * @param maxRetries
   *   Maximum number of retry attempts (default: 3)
   * @return
   *   The number of bytes uploaded
   */
  def uploadWithRetry(
    localPath: String,
    destPath: String,
    configs: Map[String, String],
    tablePath: String,
    maxRetries: Int = 3
  ): Long = {
    var lastException: Throwable = null
    var attempt                  = 0

    while (attempt < maxRetries) {
      attempt += 1
      try {
        return upload(localPath, destPath, configs, tablePath)
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
