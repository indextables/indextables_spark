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
import java.nio.file.{Files, Path}
import java.util.concurrent.CompletableFuture

import scala.util.Try

import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse}

import org.slf4j.LoggerFactory

/**
 * S3-specific async downloader using AWS SDK v2 async client.
 *
 * Uses `AsyncResponseTransformer.toFile()` to stream downloads directly to disk without buffering in JVM heap. This is
 * memory-efficient for large files.
 *
 * @param s3AsyncClient
 *   The S3 async client to use for downloads
 */
class S3AsyncDownloader(s3AsyncClient: S3AsyncClient) extends AsyncDownloader {

  private val logger = LoggerFactory.getLogger(classOf[S3AsyncDownloader])

  override def protocol: String = "s3"

  override def canHandle(path: String): Boolean =
    path.startsWith("s3://") || path.startsWith("s3a://")

  override def downloadAsync(request: DownloadRequest): CompletableFuture[DownloadResult] = {
    val startTime = System.currentTimeMillis()
    val destFile  = new File(request.destinationPath)

    // Ensure parent directory exists
    val parentDir = destFile.getParentFile
    if (parentDir != null && !parentDir.exists()) {
      parentDir.mkdirs()
    }

    // Parse S3 path to get bucket and key
    val (bucket, key) = parseS3Path(request.sourcePath)

    logger.debug(s"Starting async download: s3://$bucket/$key -> ${request.destinationPath}")

    val getRequest = GetObjectRequest.builder()
      .bucket(bucket)
      .key(key)
      .build()

    // Use the Path overload for streaming directly to disk
    // This avoids buffering the entire file in memory
    val resultFuture = new CompletableFuture[DownloadResult]()

    s3AsyncClient
      .getObject(getRequest, destFile.toPath)
      .whenComplete(new java.util.function.BiConsumer[GetObjectResponse, Throwable] {
        override def accept(response: GetObjectResponse, ex: Throwable): Unit = {
          val durationMs = System.currentTimeMillis() - startTime

          if (ex != null) {
            logger.warn(s"Failed to download s3://$bucket/$key: ${ex.getMessage}")
            // Clean up partial file if it exists
            Try(Files.deleteIfExists(destFile.toPath))
            resultFuture.complete(DownloadResult.failure(request, ex, durationMs))
          } else {
            val fileSize = destFile.length()
            logger.debug(s"Downloaded s3://$bucket/$key ($fileSize bytes in ${durationMs}ms)")
            resultFuture.complete(DownloadResult.success(
              request = request,
              localPath = destFile.getAbsolutePath,
              bytesDownloaded = fileSize,
              durationMs = durationMs
            ))
          }
        }
      })

    resultFuture
  }

  /**
   * Parse an S3 path into bucket and key.
   *
   * Handles both s3:// and s3a:// schemes.
   */
  private def parseS3Path(path: String): (String, String) = {
    // Normalize s3a:// to s3://
    val normalizedPath = path.replaceFirst("^s3a://", "s3://")

    val uri    = new URI(normalizedPath)
    val bucket = uri.getHost
    val key    = uri.getPath.stripPrefix("/")

    (bucket, key)
  }

  override def close(): Unit = {
    // S3AsyncClient is typically shared and should not be closed here
    // The caller manages the client lifecycle
  }
}

object S3AsyncDownloader {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create an S3AsyncDownloader with a new async client configured from the given parameters.
   *
   * @param accessKey
   *   AWS access key (optional)
   * @param secretKey
   *   AWS secret key (optional)
   * @param sessionToken
   *   AWS session token (optional)
   * @param region
   *   AWS region (optional)
   * @param endpoint
   *   Custom endpoint for S3-compatible services (optional)
   * @param pathStyleAccess
   *   Use path-style access (for S3-compatible services)
   * @return
   *   A new S3AsyncDownloader instance
   */
  def create(
    accessKey: Option[String] = None,
    secretKey: Option[String] = None,
    sessionToken: Option[String] = None,
    region: Option[String] = None,
    endpoint: Option[String] = None,
    pathStyleAccess: Boolean = false
  ): S3AsyncDownloader = {
    val client = createS3AsyncClient(accessKey, secretKey, sessionToken, region, endpoint, pathStyleAccess)
    new S3AsyncDownloader(client)
  }

  /**
   * Create an S3AsyncDownloader from a serializable config map.
   *
   * This is useful when creating downloaders on executors from broadcast configuration.
   */
  def fromConfig(configs: Map[String, String]): S3AsyncDownloader = {
    def get(key: String): Option[String] =
      configs.get(key).orElse(configs.get(key.toLowerCase)).filter(_.nonEmpty)

    create(
      accessKey = get("spark.indextables.aws.accessKey"),
      secretKey = get("spark.indextables.aws.secretKey"),
      sessionToken = get("spark.indextables.aws.sessionToken"),
      region = get("spark.indextables.aws.region"),
      endpoint = get("spark.indextables.aws.endpoint"),
      pathStyleAccess = get("spark.indextables.aws.pathStyleAccess").exists(_.toBoolean)
    )
  }

  /**
   * Create an S3AsyncClient with the given configuration.
   */
  private def createS3AsyncClient(
    accessKey: Option[String],
    secretKey: Option[String],
    sessionToken: Option[String],
    region: Option[String],
    endpoint: Option[String],
    pathStyleAccess: Boolean
  ): S3AsyncClient = {
    val builder = S3AsyncClient.builder()

    // Configure region
    region.foreach { r =>
      logger.debug(s"S3AsyncClient: Setting region to $r")
      builder.region(Region.of(r))
    }

    // Configure endpoint (for S3-compatible services)
    endpoint.foreach { ep =>
      val isStandardAwsEndpoint = ep.contains("s3.amazonaws.com") || ep.contains("amazonaws.com")

      if (!isStandardAwsEndpoint || region.isEmpty) {
        try {
          val endpointUri = if (ep.startsWith("http://") || ep.startsWith("https://")) {
            URI.create(ep)
          } else {
            URI.create(s"https://$ep")
          }

          logger.debug(s"S3AsyncClient: Setting endpoint to $endpointUri")
          builder.endpointOverride(endpointUri)

          if (pathStyleAccess) {
            builder.forcePathStyle(true)
          }
        } catch {
          case ex: Exception =>
            logger.error(s"Failed to parse S3 endpoint: $ep", ex)
        }
      }
    }

    // Configure credentials
    val credentialsProvider = createCredentialsProvider(accessKey, secretKey, sessionToken)
    builder.credentialsProvider(credentialsProvider)

    builder.build()
  }

  /**
   * Create an AWS credentials provider from the given credentials.
   */
  private def createCredentialsProvider(
    accessKey: Option[String],
    secretKey: Option[String],
    sessionToken: Option[String]
  ): AwsCredentialsProvider =
    (accessKey, secretKey) match {
      case (Some(ak), Some(sk)) =>
        sessionToken match {
          case Some(st) =>
            logger.debug(s"Using session credentials with access key: ${ak.take(4)}...")
            StaticCredentialsProvider.create(
              AwsSessionCredentials.create(ak, sk, st)
            )
          case None =>
            logger.debug(s"Using basic credentials with access key: ${ak.take(4)}...")
            StaticCredentialsProvider.create(
              AwsBasicCredentials.create(ak, sk)
            )
        }
      case _ =>
        logger.debug("Using default credentials provider chain")
        DefaultCredentialsProvider.create()
    }
}
