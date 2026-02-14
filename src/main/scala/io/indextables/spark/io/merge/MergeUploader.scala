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

import com.azure.identity.ClientSecretCredentialBuilder
import com.azure.storage.blob.{BlobClient, BlobServiceClient, BlobServiceClientBuilder}
import com.azure.storage.common.StorageSharedKeyCredential
import io.indextables.spark.util.ProtocolNormalizer
import io.indextables.spark.utils.CredentialProviderFactory
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.S3Client

/**
 * Helper for uploading merged splits to cloud storage (S3 and Azure).
 *
 * Supports:
 *   - S3: s3://, s3a:// paths with AWS credentials or custom credential providers
 *   - Azure: azure://, wasb://, wasbs://, abfs://, abfss:// paths with account key or OAuth
 *   - Local: file paths copied directly
 *
 * Uses the same credential resolution patterns as S3AsyncDownloader and AzureAsyncDownloader for consistency across
 * download and upload operations.
 */
object MergeUploader {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Upload a local file to cloud storage (S3, Azure, or local).
   *
   * Uses the same credential resolution patterns as S3AsyncDownloader and AzureAsyncDownloader to ensure Unity Catalog
   * and other custom credential providers work consistently.
   *
   * @param localPath
   *   Path to the local file to upload
   * @param destPath
   *   Destination path: S3 (s3://, s3a://), Azure (azure://, wasb://, wasbs://, abfs://, abfss://), or local
   * @param configs
   *   Configuration map with cloud provider settings
   * @param tablePath
   *   Table path for credential provider resolution (used for S3 custom providers)
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

    // Route to appropriate upload method based on cloud provider
    if (ProtocolNormalizer.isAzurePath(destPath)) {
      return uploadToAzure(localFile, destPath, configs)
    }

    if (!ProtocolNormalizer.isS3Path(destPath)) {
      // Local path - just copy the file
      val destFile = new File(destPath)
      destFile.getParentFile.mkdirs()
      Files.copy(localFile.toPath, destFile.toPath)
      logger.info(s"Copied merged split to local path: $destPath")
      return fileSize
    }

    // S3 upload - Use the SAME credential resolution as S3AsyncDownloader
    val s3Client = createS3Client(configs, tablePath)
    val multipartThreshold = 100L * 1024 * 1024 // 100MB

    try {
      // Parse S3 path
      val (bucket, key) = parseS3Path(destPath)

      if (fileSize >= multipartThreshold) {
        logger.info(s"Uploading to S3 (multipart): bucket=$bucket, key=$key ($fileSize bytes)")
        uploadMultipart(s3Client, bucket, key, localFile)
      } else {
        logger.info(s"Uploading to S3: bucket=$bucket, key=$key ($fileSize bytes)")
        val putRequest = PutObjectRequest
          .builder()
          .bucket(bucket)
          .key(key)
          .build()
        s3Client.putObject(putRequest, RequestBody.fromFile(localFile))
      }

      logger.info(s"Upload completed: $destPath ($fileSize bytes)")
      fileSize
    } finally
      s3Client.close()
  }

  /**
   * Create S3 client using the same credential resolution as S3AsyncDownloader. This ensures Unity Catalog and other
   * custom credential providers work.
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

    val builder = S3Client
      .builder()
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
   * Multipart upload for files exceeding the single-PUT 5GB S3 limit.
   * Uses 128MB parts with sequential upload via the sync S3 client.
   */
  private def uploadMultipart(
    s3Client: S3Client,
    bucket: String,
    key: String,
    localFile: File
  ): Unit = {
    import software.amazon.awssdk.services.s3.model._
    import java.io.RandomAccessFile

    val partSize = 128L * 1024 * 1024 // 128MB parts
    val fileSize = localFile.length()

    val createRequest = CreateMultipartUploadRequest.builder()
      .bucket(bucket)
      .key(key)
      .build()
    val createResponse = s3Client.createMultipartUpload(createRequest)
    val uploadId = createResponse.uploadId()

    try {
      val completedParts = new java.util.ArrayList[CompletedPart]()
      val raf = new RandomAccessFile(localFile, "r")
      try {
        var offset = 0L
        var partNumber = 1
        while (offset < fileSize) {
          val currentPartSize = math.min(partSize, fileSize - offset).toInt
          val buffer = new Array[Byte](currentPartSize)
          raf.seek(offset)
          raf.readFully(buffer)

          val uploadRequest = UploadPartRequest.builder()
            .bucket(bucket)
            .key(key)
            .uploadId(uploadId)
            .partNumber(partNumber)
            .contentLength(currentPartSize.toLong)
            .build()

          val response = s3Client.uploadPart(uploadRequest, RequestBody.fromBytes(buffer))

          completedParts.add(
            CompletedPart.builder()
              .partNumber(partNumber)
              .eTag(response.eTag())
              .build()
          )

          logger.debug(s"Uploaded part $partNumber (${currentPartSize / 1024 / 1024}MB) " +
            s"for s3://$bucket/$key")
          offset += currentPartSize
          partNumber += 1
        }
      } finally {
        raf.close()
      }

      val completeRequest = CompleteMultipartUploadRequest.builder()
        .bucket(bucket)
        .key(key)
        .uploadId(uploadId)
        .multipartUpload(
          CompletedMultipartUpload.builder()
            .parts(completedParts)
            .build()
        )
        .build()
      s3Client.completeMultipartUpload(completeRequest)
      logger.info(s"Multipart upload completed: s3://$bucket/$key " +
        s"(${completedParts.size()} parts, $fileSize bytes)")
    } catch {
      case e: Exception =>
        // Abort the multipart upload on failure to clean up partial parts
        try {
          s3Client.abortMultipartUpload(
            AbortMultipartUploadRequest.builder()
              .bucket(bucket)
              .key(key)
              .uploadId(uploadId)
              .build()
          )
        } catch {
          case abortEx: Exception =>
            logger.warn(s"Failed to abort multipart upload: ${abortEx.getMessage}")
        }
        throw e
    }
  }

  /** Parse an S3 path into bucket and key. */
  private def parseS3Path(path: String): (String, String) = {
    val normalizedPath = path.replaceFirst("^s3a://", "s3://")
    val uri            = new URI(normalizedPath)
    val bucket         = uri.getHost
    val key            = uri.getPath.stripPrefix("/")
    (bucket, key)
  }

  /**
   * Upload a local file to Azure Blob Storage.
   *
   * Uses the same credential resolution pattern as AzureAsyncDownloader.fromConfig() to ensure consistent
   * authentication.
   *
   * @param localFile
   *   The local file to upload
   * @param destPath
   *   Destination Azure path (azure://, wasb://, wasbs://, abfs://, abfss://)
   * @param configs
   *   Configuration map with Azure credentials
   * @return
   *   The number of bytes uploaded
   */
  private def uploadToAzure(
    localFile: File,
    destPath: String,
    configs: Map[String, String]
  ): Long = {
    val fileSize = localFile.length()

    // Create Azure client using same pattern as AzureAsyncDownloader
    val blobClient = createAzureBlobClient(destPath, configs)

    try {
      logger.info(s"Uploading to Azure: ${localFile.getAbsolutePath} -> $destPath")

      // Upload file (overwrite if exists)
      blobClient.uploadFromFile(localFile.getAbsolutePath, true)

      logger.info(s"Azure upload completed: $destPath ($fileSize bytes)")
      fileSize
    } catch {
      case ex: Exception =>
        logger.error(s"Azure upload failed: $destPath", ex)
        throw ex
    }
  }

  /**
   * Create an Azure BlobClient for the specified path.
   *
   * Uses the same credential resolution as AzureAsyncDownloader.fromConfig().
   */
  private def createAzureBlobClient(destPath: String, configs: Map[String, String]): BlobClient = {
    def get(key: String): Option[String] =
      configs.get(key).orElse(configs.get(key.toLowerCase)).filter(_.nonEmpty)

    val accountName = get("spark.indextables.azure.accountName")
      .getOrElse(throw new RuntimeException("Azure account name not configured (spark.indextables.azure.accountName)"))

    val accountKey   = get("spark.indextables.azure.accountKey")
    val tenantId     = get("spark.indextables.azure.tenantId")
    val clientId     = get("spark.indextables.azure.clientId")
    val clientSecret = get("spark.indextables.azure.clientSecret")

    // Build service client with appropriate credentials
    val serviceClient: BlobServiceClient = accountKey match {
      case Some(key) =>
        // Account key authentication
        logger.debug(s"Creating Azure client with account key for account: $accountName")
        val credential = new StorageSharedKeyCredential(accountName, key)
        new BlobServiceClientBuilder()
          .endpoint(s"https://$accountName.blob.core.windows.net")
          .credential(credential)
          .buildClient()

      case None =>
        // Try OAuth (Service Principal) authentication
        (tenantId, clientId, clientSecret) match {
          case (Some(tenant), Some(client), Some(secret)) =>
            logger.debug(s"Creating Azure client with OAuth for account: $accountName")
            val credential = new ClientSecretCredentialBuilder()
              .tenantId(tenant)
              .clientId(client)
              .clientSecret(secret)
              .build()
            new BlobServiceClientBuilder()
              .endpoint(s"https://$accountName.blob.core.windows.net")
              .credential(credential)
              .buildClient()

          case _ =>
            throw new RuntimeException(
              "Azure credentials not configured. Provide either spark.indextables.azure.accountKey " +
                "or OAuth credentials (tenantId, clientId, clientSecret)"
            )
        }
    }

    // Parse Azure path to get container and blob
    val (container, blobPath) = parseAzurePath(destPath)
    logger.debug(s"Azure upload target: container=$container, blob=$blobPath")

    // Get blob client for upload
    serviceClient.getBlobContainerClient(container).getBlobClient(blobPath)
  }

  /**
   * Parse an Azure path into container and blob path.
   *
   * Handles various Azure URL schemes: azure://, wasb://, wasbs://, abfs://, abfss://
   */
  private def parseAzurePath(path: String): (String, String) = {
    // Normalize to azure:// scheme for parsing
    val normalizedPath = path
      .replaceFirst("^wasbs?://", "azure://")
      .replaceFirst("^abfss?://", "azure://")

    val uri = new URI(normalizedPath)
    val pathPart = uri.getPath.stripPrefix("/")

    // Hadoop-style URLs use container@account.host format:
    //   wasbs://container@account.blob.core.windows.net/path
    //   abfss://container@account.dfs.core.windows.net/path
    // In URI parsing, the part before @ becomes userInfo, not part of host.
    val userInfo = uri.getUserInfo
    if (userInfo != null && userInfo.nonEmpty) {
      (userInfo, pathPart)
    } else {
      // Simple format: azure://container/path
      (uri.getHost, pathPart)
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
      try
        return upload(localPath, destPath, configs, tablePath)
      catch {
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
