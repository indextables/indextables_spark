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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.net.URI
import java.util.concurrent.{CompletableFuture, Executors}

import scala.jdk.CollectionConverters._
import scala.util.Try

import io.indextables.spark.utils.CredentialProviderFactory
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsSessionCredentials,
  DefaultCredentialsProvider,
  StaticCredentialsProvider
}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Client}
import software.amazon.awssdk.services.s3.model._

/**
 * High-performance S3 storage provider using AWS SDK directly. Bypasses Hadoop filesystem for better performance and
 * reliability.
 */
class S3CloudStorageProvider(
  config: CloudStorageConfig,
  hadoopConf: org.apache.hadoop.conf.Configuration = null,
  tablePath: String = "s3://dummy")
    extends CloudStorageProvider {

  private val logger   = LoggerFactory.getLogger(classOf[S3CloudStorageProvider])
  private val executor = Executors.newCachedThreadPool()

  // Configurable multipart upload threshold (default 200MB)
  private val multipartThreshold = config.multipartUploadThreshold.getOrElse(200L * 1024 * 1024)

  // Multipart uploader for large files with parallel streaming using ASYNC S3 client
  private lazy val multipartUploader = new S3MultipartUploader(
    s3Client,
    s3AsyncClient, // TRUE async I/O for non-blocking uploads
    S3MultipartConfig(
      multipartThreshold = multipartThreshold,
      partSize = config.partSize.getOrElse(128L * 1024 * 1024), // 128MB default (configurable)
      maxConcurrency = config.maxConcurrency.getOrElse(4),      // 4 parallel uploads
      maxQueueSize = config.maxQueueSize.getOrElse(3),          // 3 parts buffered = 384MB max memory
      maxRetries = config.maxRetries.getOrElse(3)
    )
  )

  logger.debug(s"S3CloudStorageProvider CONFIG:")
  logger.debug(s"  - accessKey: ${config.awsAccessKey.map(_.take(4) + "...")}")
  logger.debug(s"  - secretKey: ${config.awsSecretKey.map(_ => "***")}")
  logger.debug(s"  - endpoint: ${config.awsEndpoint}")
  logger.debug(s"  - pathStyleAccess: ${config.awsPathStyleAccess}")
  logger.debug(s"  - region: ${config.awsRegion}")

  // Detect if we're running against S3Mock for compatibility adjustments
  private val isS3Mock = config.awsEndpoint.exists(_.contains("localhost"))

  // Helper methods for S3Mock path transformation and protocol conversion
  // Use a special delimiter that won't appear in normal filenames
  private val S3_MOCK_DIR_SEPARATOR = "___"

  // Convert s3a:// URLs to s3:// for tantivy4java compatibility
  // tantivy4java only understands s3:// protocol, not s3a:// or s3n://
  private def normalizeProtocolForTantivy(path: String): String =
    io.indextables.spark.util.ProtocolNormalizer.normalizeS3Protocol(path)

  // Convert nested paths to flat structure: path/to/file.txt -> path___to___file.txt
  private def flattenPathForS3Mock(key: String): String =
    if (isS3Mock && key.contains("/")) {
      key.replace("/", S3_MOCK_DIR_SEPARATOR)
    } else {
      key
    }

  // Convert flat paths back to nested: path___to___file.txt -> path/to/file.txt
  private def unflattenPathFromS3Mock(key: String): String =
    if (isS3Mock && key.contains(S3_MOCK_DIR_SEPARATOR)) {
      key.replace(S3_MOCK_DIR_SEPARATOR, "/")
    } else {
      key
    }

  /**
   * Normalize a path to table level by removing filename if present. This ensures that credential providers are always
   * created with the table path, not individual split file paths. IMPORTANT: Preserves the original scheme (s3a://,
   * s3://, etc.) to ensure credential providers receive the same URI scheme as specified by the user.
   */
  private def normalizeToTablePath(path: String): String = {
    val uri      = new URI(path)
    val pathPart = uri.getPath

    // If the path ends with a .split file, remove it to get the table path
    if (pathPart != null && pathPart.endsWith(".split")) {
      val splitIndex = pathPart.lastIndexOf('/')
      if (splitIndex > 0) {
        val tablePath = pathPart.substring(0, splitIndex)
        // Preserve the original scheme, authority, query, and fragment
        new URI(uri.getScheme, uri.getAuthority, tablePath, uri.getQuery, uri.getFragment).toString
      } else {
        path // Fallback to original path if we can't determine table path
      }
    } else {
      path // Not a split file path, use as-is
    }
  }

  private val s3Client: S3Client = {
    val builder = S3Client.builder()

    // Configure region
    config.awsRegion match {
      case Some(region) =>
        logger.info(s"🔧 S3Client: Setting region to $region")
        builder.region(Region.of(region))
      case None =>
        logger.warn(s"⚠️ S3Client: No region configured, this will cause errors!")
    }

    // Configure endpoint (for testing with S3Mock, MinIO, etc.)
    config.awsEndpoint.foreach { endpoint =>
      // Skip endpoint override for standard AWS S3 endpoints when region is configured
      // Using endpoint override with s3.amazonaws.com breaks regional routing
      val isStandardAwsEndpoint = endpoint.contains("s3.amazonaws.com") || endpoint.contains("amazonaws.com")

      if (isStandardAwsEndpoint && config.awsRegion.isDefined) {
        logger.info(s"🔧 Skipping endpoint override for standard AWS endpoint '$endpoint' because region is configured: ${config.awsRegion.get}")
      } else {
        try {
          // Ensure the endpoint has a proper scheme (http:// or https://)
          val endpointUri = if (endpoint.startsWith("http://") || endpoint.startsWith("https://")) {
            URI.create(endpoint)
          } else {
            // Default to https if no scheme specified
            URI.create(s"https://$endpoint")
          }

          logger.info(s"Configuring S3 client with endpoint: $endpointUri")
          builder.endpointOverride(endpointUri)

          if (config.awsPathStyleAccess) {
            builder.forcePathStyle(true)
          }
        } catch {
          case ex: Exception =>
            logger.error(s"Failed to parse S3 endpoint: $endpoint", ex)
            throw new IllegalArgumentException(s"Invalid S3 endpoint: $endpoint", ex)
        }
      }
    }

    // Helper method for fallback credential configuration
    def createFallbackCredentialsProvider() =
      (config.awsAccessKey, config.awsSecretKey, config.awsSessionToken) match {
        case (Some(accessKey), Some(secretKey), Some(sessionToken)) =>
          logger.info(s"Using AWS session credentials with access key: ${accessKey.take(4)}...")
          val credentials = AwsSessionCredentials.create(accessKey, secretKey, sessionToken)
          StaticCredentialsProvider.create(credentials)
        case (Some(accessKey), Some(secretKey), None) =>
          logger.info(s"Using AWS basic credentials with access key: ${accessKey.take(4)}...")
          val credentials = AwsBasicCredentials.create(accessKey, secretKey)
          StaticCredentialsProvider.create(credentials)
        case _ =>
          logger.info("No AWS credentials configured, using DefaultCredentialsProvider")
          DefaultCredentialsProvider.create()
      }

    // Configure credentials with the following priority:
    // 1. Custom provider (if configured)
    // 2. Explicit credentials (access key/secret key in config)
    // 3. Default provider chain
    val credentialsProvider = config.awsCredentialsProviderClass match {
      case Some(providerClassName) =>
        logger.info(s"Using custom AWS credentials provider: $providerClassName")
        try {
          // Create custom credential provider using reflection
          // Normalize the path to table level by removing filename if present
          val normalizedTablePath = normalizeToTablePath(tablePath)
          val customProvider = CredentialProviderFactory.createCredentialProvider(
            providerClassName,
            new URI(normalizedTablePath), // Use normalized table path URI for constructor
            if (hadoopConf != null) hadoopConf else new org.apache.hadoop.conf.Configuration()
          )

          // Extract credentials using reflection to avoid AWS SDK dependencies
          val basicCredentials = CredentialProviderFactory.extractCredentialsViaReflection(customProvider)

          logger.info(s"Successfully extracted credentials from custom provider. Access key: ${basicCredentials.accessKey.take(4)}...")

          // Create static credentials provider with extracted credentials
          if (basicCredentials.hasSessionToken) {
            val credentials = AwsSessionCredentials.create(
              basicCredentials.accessKey,
              basicCredentials.secretKey,
              basicCredentials.sessionToken.get
            )
            StaticCredentialsProvider.create(credentials)
          } else {
            val credentials = AwsBasicCredentials.create(
              basicCredentials.accessKey,
              basicCredentials.secretKey
            )
            StaticCredentialsProvider.create(credentials)
          }

        } catch {
          case ex: Exception =>
            logger.error(s"Failed to create custom credential provider: $providerClassName", ex)
            logger.warn("Falling back to explicit credentials or default provider chain")
            createFallbackCredentialsProvider()
        }

      case None =>
        createFallbackCredentialsProvider()
    }

    builder.credentialsProvider(credentialsProvider).build()
  }

  // Create async S3 client for true async I/O (non-blocking uploads)
  private val s3AsyncClient: S3AsyncClient = {
    val builder = S3AsyncClient.builder()

    // Configure region (same as sync client)
    config.awsRegion.foreach { region =>
      logger.info(s"🔧 S3AsyncClient: Setting region to $region")
      builder.region(Region.of(region))
    }

    // Configure endpoint (same as sync client)
    config.awsEndpoint.foreach { endpoint =>
      val isStandardAwsEndpoint = endpoint.contains("s3.amazonaws.com") || endpoint.contains("amazonaws.com")

      if (!isStandardAwsEndpoint || config.awsRegion.isEmpty) {
        try {
          val endpointUri = if (endpoint.startsWith("http://") || endpoint.startsWith("https://")) {
            URI.create(endpoint)
          } else {
            URI.create(s"https://$endpoint")
          }

          logger.info(s"Configuring async S3 client with endpoint: $endpointUri")
          builder.endpointOverride(endpointUri)

          if (config.awsPathStyleAccess) {
            builder.forcePathStyle(true)
          }
        } catch {
          case ex: Exception =>
            logger.error(s"Failed to parse S3 endpoint for async client: $endpoint", ex)
        }
      }
    }

    // Use same credentials provider as sync client
    val credentialsProvider = config.awsCredentialsProviderClass match {
      case Some(providerClassName) =>
        try {
          val normalizedTablePathForAsync = normalizeToTablePath(tablePath)
          val customProvider = CredentialProviderFactory.createCredentialProvider(
            providerClassName,
            new URI(normalizedTablePathForAsync),
            if (hadoopConf != null) hadoopConf else new org.apache.hadoop.conf.Configuration()
          )

          val basicCredentials = CredentialProviderFactory.extractCredentialsViaReflection(customProvider)

          if (basicCredentials.hasSessionToken) {
            val credentials = AwsSessionCredentials.create(
              basicCredentials.accessKey,
              basicCredentials.secretKey,
              basicCredentials.sessionToken.get
            )
            StaticCredentialsProvider.create(credentials)
          } else {
            val credentials = AwsBasicCredentials.create(
              basicCredentials.accessKey,
              basicCredentials.secretKey
            )
            StaticCredentialsProvider.create(credentials)
          }
        } catch {
          case ex: Exception =>
            logger.warn("Async client falling back to explicit credentials or default provider chain")
            (config.awsAccessKey, config.awsSecretKey, config.awsSessionToken) match {
              case (Some(accessKey), Some(secretKey), Some(sessionToken)) =>
                val credentials = AwsSessionCredentials.create(accessKey, secretKey, sessionToken)
                StaticCredentialsProvider.create(credentials)
              case (Some(accessKey), Some(secretKey), None) =>
                val credentials = AwsBasicCredentials.create(accessKey, secretKey)
                StaticCredentialsProvider.create(credentials)
              case _ =>
                DefaultCredentialsProvider.create()
            }
        }

      case None =>
        (config.awsAccessKey, config.awsSecretKey, config.awsSessionToken) match {
          case (Some(accessKey), Some(secretKey), Some(sessionToken)) =>
            val credentials = AwsSessionCredentials.create(accessKey, secretKey, sessionToken)
            StaticCredentialsProvider.create(credentials)
          case (Some(accessKey), Some(secretKey), None) =>
            val credentials = AwsBasicCredentials.create(accessKey, secretKey)
            StaticCredentialsProvider.create(credentials)
          case _ =>
            DefaultCredentialsProvider.create()
        }
    }

    builder.credentialsProvider(credentialsProvider).build()
  }

  override def listFiles(path: String, recursive: Boolean = false): Seq[CloudFileInfo] = {
    val (bucket, originalPrefix) = parseS3Path(path)

    // Apply uniform path flattening for S3Mock compatibility
    val basePrefix = flattenPathForS3Mock(originalPrefix)

    // Add trailing slash only for transaction log directory listings to prevent matching
    // similarly named directories like _transaction_log_backup
    val prefix =
      if (
        basePrefix.nonEmpty &&
        (basePrefix.endsWith("/_transaction_log") || basePrefix == "_transaction_log") &&
        !basePrefix.endsWith("/")
      ) {
        basePrefix + "/"
      } else {
        basePrefix
      }

    try {
      logger.info(s"S3 LIST DEBUG - Listing S3 files: bucket=$bucket, prefix=$prefix (original: $originalPrefix), recursive=$recursive, isS3Mock=$isS3Mock")
      val request = ListObjectsV2Request
        .builder()
        .bucket(bucket)
        .prefix(prefix)
        .delimiter(if (recursive || isS3Mock) null else "/") // For S3Mock, always list recursively since we flatten paths
        .build()

      val response = s3Client.listObjectsV2(request)

      val files = response
        .contents()
        .asScala
        .map { s3Object =>
          // Transform the path back to the original format for the caller
          val originalKey = s3Object.key()
          val displayKey  = unflattenPathFromS3Mock(originalKey)

          logger.debug(s"S3 LIST ITEM - Original key: $originalKey, Display key: $displayKey")

          CloudFileInfo(
            path = s"s3://$bucket/$displayKey",
            size = s3Object.size(),
            modificationTime = s3Object.lastModified().toEpochMilli,
            isDirectory = false
          )
        }
        .toSeq

      // For S3Mock with flattened paths, we don't have real directories
      val directories = if (!recursive && !isS3Mock) {
        response
          .commonPrefixes()
          .asScala
          .map { commonPrefix =>
            val displayPrefix = unflattenPathFromS3Mock(commonPrefix.prefix())

            CloudFileInfo(
              path = s"s3://$bucket/$displayPrefix",
              size = 0L,
              modificationTime = 0L,
              isDirectory = true
            )
          }
          .toSeq
      } else Seq.empty

      val allResults = files ++ directories
      logger.info(
        s"S3 LIST RESULTS - Found ${files.size} files and ${directories.size} directories for prefix '$prefix'"
      )
      files.foreach(f => logger.debug(s"  File: ${f.path}"))
      allResults
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to list S3 files at s3://$bucket/$prefix", ex)
        throw new RuntimeException(s"Failed to list S3 files: ${ex.getMessage}", ex)
    }
  }

  override def exists(path: String): Boolean = {
    val (bucket, originalKey) = parseS3Path(path)

    // Apply uniform path flattening for S3Mock compatibility
    val key = flattenPathForS3Mock(originalKey)

    try {
      val request = HeadObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .build()

      s3Client.headObject(request)
      true
    } catch {
      case _: NoSuchKeyException => false
      case ex: Exception =>
        logger.error(s"Failed to check if S3 file exists: s3://$bucket/$key", ex)
        false
    }
  }

  override def getFileInfo(path: String): Option[CloudFileInfo] = {
    val (bucket, key) = parseS3Path(path)

    try {
      val request = HeadObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .build()

      val response = s3Client.headObject(request)

      Some(
        CloudFileInfo(
          path = path,
          size = response.contentLength(),
          modificationTime = response.lastModified().toEpochMilli,
          isDirectory = false
        )
      )
    } catch {
      case _: NoSuchKeyException => None
      case ex: Exception =>
        logger.error(s"Failed to get S3 file info: s3://$bucket/$key", ex)
        None
    }
  }

  override def readFile(path: String): Array[Byte] = {
    val (bucket, originalKey) = parseS3Path(path)

    // Apply uniform path flattening for S3Mock compatibility
    val key = flattenPathForS3Mock(originalKey)

    try {
      logger.debug(s"Reading entire S3 file: s3://$bucket/$key (original: $originalKey)")

      val request = GetObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .build()

      val response = s3Client.getObject(request)
      response.readAllBytes()
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to read S3 file: s3://$bucket/$key", ex)
        throw new RuntimeException(s"Failed to read S3 file: ${ex.getMessage}", ex)
    }
  }

  override def readRange(
    path: String,
    offset: Long,
    length: Long
  ): Array[Byte] = {
    val (bucket, key) = parseS3Path(path)
    val rangeHeader   = s"bytes=$offset-${offset + length - 1}"

    try {
      logger.debug(s"Reading S3 range: s3://$bucket/$key, range=$rangeHeader")

      val request = GetObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .range(rangeHeader)
        .build()

      val response = s3Client.getObject(request)
      response.readAllBytes()
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to read S3 range: s3://$bucket/$key, range=$rangeHeader", ex)
        throw new RuntimeException(s"Failed to read S3 range: ${ex.getMessage}", ex)
    }
  }

  override def openInputStream(path: String): InputStream = {
    val (bucket, key) = parseS3Path(path)

    try {
      val request = GetObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .build()

      s3Client.getObject(request)
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to open S3 input stream: s3://$bucket/$key", ex)
        throw new RuntimeException(s"Failed to open S3 input stream: ${ex.getMessage}", ex)
    }
  }

  override def createOutputStream(path: String): OutputStream =
    // For S3, we need to buffer the output and upload when the stream is closed
    new S3OutputStream(path)

  override def writeFile(path: String, content: Array[Byte]): Unit = {
    val (bucket, originalKey) = parseS3Path(path)

    // Apply uniform path flattening for S3Mock compatibility
    val key           = flattenPathForS3Mock(originalKey)
    val contentLength = content.length.toLong

    try {
      logger.info(s"🔧 S3 WRITE DEBUG - Path: $path")
      logger.info(s"🔧 S3 WRITE DEBUG - Bucket: '$bucket', Key: '$key' (original: '$originalKey')")
      logger.info(s"🔧 S3 WRITE DEBUG - Content length: ${formatBytes(contentLength)}")
      logger.info(s"🔧 S3 WRITE DEBUG - S3Mock mode: $isS3Mock")

      // Ensure bucket exists first
      ensureBucketExists(bucket)

      // Use multipart upload for files larger than threshold
      if (contentLength >= multipartThreshold) {
        logger.info(s"🚀 Using multipart upload for large file: s3://$bucket/$key (${formatBytes(contentLength)})")

        val result = multipartUploader.uploadFile(bucket, key, content)
        logger.info(s"✅ Multipart upload completed: ${result.strategy}, ${result.partCount} parts, ${result.uploadRateMBps}%.2f MB/s")

      } else {
        logger.info(s"📄 Using single-part upload for file: s3://$bucket/$key (${formatBytes(contentLength)})")

        val request = PutObjectRequest
          .builder()
          .bucket(bucket)
          .key(key)
          .contentLength(contentLength)
          .build()

        val requestBody = RequestBody.fromBytes(content)
        s3Client.putObject(request, requestBody)

        logger.info(s"✅ Successfully wrote S3 file: s3://$bucket/$key")
      }

    } catch {
      case ex: Exception =>
        logger.error(s"❌ Failed to write S3 file: s3://$bucket/$key", ex)
        throw new RuntimeException(s"Failed to write S3 file: ${ex.getMessage}", ex)
    }
  }

  override def writeFileIfNotExists(path: String, content: Array[Byte]): Boolean = {
    val (bucket, originalKey) = parseS3Path(path)

    // Apply uniform path flattening for S3Mock compatibility
    val key           = flattenPathForS3Mock(originalKey)
    val contentLength = content.length.toLong

    try {
      logger.info(s"🔒 S3 CONDITIONAL WRITE - Path: $path")
      logger.info(s"🔒 S3 CONDITIONAL WRITE - Bucket: '$bucket', Key: '$key' (original: '$originalKey')")
      logger.info(s"🔒 S3 CONDITIONAL WRITE - Content length: ${formatBytes(contentLength)}")
      logger.info(s"🔒 S3 CONDITIONAL WRITE - Using If-None-Match: * for atomic write protection")

      // Ensure bucket exists first
      ensureBucketExists(bucket)

      // Use S3 Conditional Writes (available in AWS SDK 2.21.0+)
      // If-None-Match: * tells S3 to only write if the object doesn't exist
      // S3 returns 412 Precondition Failed if object already exists
      val request = PutObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .contentLength(contentLength)
        .ifNoneMatch("*") // ✅ S3 Conditional Writes: Only write if object doesn't exist
        .build()

      val requestBody = RequestBody.fromBytes(content)
      s3Client.putObject(request, requestBody)

      logger.info(s"✅ Successfully wrote S3 file (conditional): s3://$bucket/$key")
      true // File was written successfully

    } catch {
      case ex: software.amazon.awssdk.services.s3.model.S3Exception if ex.statusCode() == 412 =>
        // 412 Precondition Failed means the file already exists
        logger.warn(s"⚠️  Conditional write failed - file already exists: s3://$bucket/$key")
        false // File already exists, write was NOT performed

      case ex: Exception =>
        logger.error(s"❌ Failed conditional write to S3: s3://$bucket/$key", ex)
        throw new RuntimeException(s"Failed conditional write to S3: ${ex.getMessage}", ex)
    }
  }

  /** Ensure bucket exists before writing files */
  private def ensureBucketExists(bucket: String): Unit =
    try {
      val bucketExistsRequest = software.amazon.awssdk.services.s3.model.HeadBucketRequest
        .builder()
        .bucket(bucket)
        .build()

      s3Client.headBucket(bucketExistsRequest)
      logger.debug(s"✅ Bucket exists: $bucket")
    } catch {
      case _: software.amazon.awssdk.services.s3.model.NoSuchBucketException =>
        logger.info(s"🔧 Creating missing bucket: $bucket")
        val createBucketRequest = software.amazon.awssdk.services.s3.model.CreateBucketRequest
          .builder()
          .bucket(bucket)
          .build()
        s3Client.createBucket(createBucketRequest)
        logger.info(s"✅ Created S3 bucket: $bucket")
      case ex: Exception =>
        logger.warn(s"⚠️  Could not verify bucket existence for $bucket: ${ex.getMessage}")
      // Continue anyway - the putObject call will fail if bucket really doesn't exist
    }

  override def writeFileFromStream(
    path: String,
    inputStream: InputStream,
    contentLength: Option[Long] = None
  ): Unit = {
    val (bucket, originalKey) = parseS3Path(path)
    // Apply uniform path flattening for S3Mock compatibility
    val key = flattenPathForS3Mock(originalKey)

    try {
      logger.info(s"🔧 S3 STREAMING WRITE - Path: $path")
      logger.info(s"🔧 S3 STREAMING WRITE - Bucket: '$bucket', Key: '$key' (original: '$originalKey')")
      contentLength.foreach(length => logger.info(s"🔧 S3 STREAMING WRITE - Content length: ${formatBytes(length)}"))
      logger.info(s"🔧 S3 STREAMING WRITE - S3Mock mode: $isS3Mock")

      // Ensure bucket exists first
      ensureBucketExists(bucket)

      // Use streaming multipart upload - this is memory efficient for large files
      logger.info(s"🚀 Using streaming multipart upload for file: s3://$bucket/$key")
      val result = multipartUploader.uploadStream(bucket, key, inputStream, contentLength)
      logger.info(s"✅ Streaming upload completed: ${result.strategy}, ${result.partCount} parts, ${result.uploadRateMBps}%.2f MB/s")

    } catch {
      case ex: Exception =>
        logger.error(s"❌ Failed to write S3 file from stream: s3://$bucket/$key", ex)
        throw new RuntimeException(s"Failed to write S3 file from stream: ${ex.getMessage}", ex)
    }
  }

  override def deleteFile(path: String): Boolean = {
    val (bucket, key) = parseS3Path(path)

    try {
      val request = DeleteObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .build()

      s3Client.deleteObject(request)
      true
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to delete S3 file: s3://$bucket/$key", ex)
        false
    }
  }

  override def createDirectory(path: String): Boolean = {
    val (bucket, key) = parseS3Path(path)

    // If the key is empty or just "/", create the bucket
    if (key.isEmpty || key == "/") {
      try {
        // Check if bucket exists
        val bucketExistsRequest = software.amazon.awssdk.services.s3.model.HeadBucketRequest
          .builder()
          .bucket(bucket)
          .build()

        try {
          s3Client.headBucket(bucketExistsRequest)
          logger.debug(s"Bucket already exists: $bucket")
        } catch {
          case _: software.amazon.awssdk.services.s3.model.NoSuchBucketException =>
            // Create bucket
            val createBucketRequest = software.amazon.awssdk.services.s3.model.CreateBucketRequest
              .builder()
              .bucket(bucket)
              .build()
            s3Client.createBucket(createBucketRequest)
            logger.info(s"Created S3 bucket: $bucket")
        }
        true
      } catch {
        case ex: Exception =>
          logger.error(s"Failed to create bucket: $bucket", ex)
          false
      }
    } else {
      // For S3Mock compatibility, we don't create directory placeholders
      // S3Mock stores objects as actual filesystem directories, and creating placeholder objects
      // conflicts with writing actual files later. Instead, we just return true and let
      // S3Mock handle directory creation implicitly when files are written.
      logger.debug(s"Skipping directory placeholder creation for S3Mock compatibility: $path")
      true
    }
  }

  override def readFilesParallel(paths: Seq[String]): Map[String, Array[Byte]] = {
    if (paths.isEmpty) return Map.empty

    logger.info(s"Reading ${paths.size} files in parallel from S3")

    val futures = paths.map { path =>
      CompletableFuture.supplyAsync(
        () =>
          try
            path -> Some(readFile(path))
          catch {
            case ex: Exception =>
              logger.error(s"Failed to read S3 file in parallel: $path", ex)
              path -> None
          },
        executor
      )
    }

    try {
      val results = futures.map(_.get()).collect { case (path, Some(content)) => path -> content }.toMap

      logger.info(s"Successfully read ${results.size} of ${paths.size} files in parallel")
      results
    } catch {
      case ex: Exception =>
        logger.error("Failed to complete parallel S3 file reads", ex)
        Map.empty
    }
  }

  override def existsParallel(paths: Seq[String]): Map[String, Boolean] = {
    if (paths.isEmpty) return Map.empty

    logger.debug(s"Checking existence of ${paths.size} files in parallel")

    val futures = paths.map(path => CompletableFuture.supplyAsync(() => path -> exists(path), executor))

    try
      futures.map(_.get()).toMap
    catch {
      case ex: Exception =>
        logger.error("Failed to complete parallel S3 existence checks", ex)
        paths.map(_ -> false).toMap
    }
  }

  override def getProviderType: String = "s3"

  override def close(): Unit = {
    multipartUploader.shutdown()
    executor.shutdown()
    s3Client.close()
    logger.debug("Closed S3 storage provider")
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

  /**
   * Normalize path for tantivy4java compatibility. Converts s3a:// and s3n:// protocols to s3:// which tantivy4java
   * understands.
   */
  override def normalizePathForTantivy(path: String): String = {
    val protocolNormalized = normalizeProtocolForTantivy(path)

    // Apply path flattening for S3Mock compatibility
    if (isS3Mock) {
      val (bucket, key) = parseS3Path(protocolNormalized)
      val flattenedKey  = flattenPathForS3Mock(key)
      s"s3://$bucket/$flattenedKey"
    } else {
      protocolNormalized
    }
  }

  /** Parse S3 path into bucket and key components */
  private def parseS3Path(path: String): (String, String) = {
    val uri    = URI.create(path)
    val bucket = uri.getHost
    val key    = uri.getPath.stripPrefix("/")
    (bucket, key)
  }

  /**
   * Custom OutputStream for S3 that buffers content and uploads on close. Uses multipart upload for large files
   * (>100MB) for better reliability.
   */
  private class S3OutputStream(path: String) extends OutputStream {
    private val buffer = new ByteArrayOutputStream()
    private var closed = false

    override def write(b: Int): Unit = {
      if (closed) throw new IllegalStateException("Stream is closed")
      buffer.write(b)
    }

    override def write(b: Array[Byte]): Unit = {
      if (closed) throw new IllegalStateException("Stream is closed")
      buffer.write(b)
    }

    override def write(
      b: Array[Byte],
      off: Int,
      len: Int
    ): Unit = {
      if (closed) throw new IllegalStateException("Stream is closed")
      buffer.write(b, off, len)
    }

    override def close(): Unit =
      if (!closed) {
        try {
          val content = buffer.toByteArray
          logger.debug(s"S3OutputStream closing with ${formatBytes(content.length)} buffered")
          writeFile(path, content)
        } finally {
          buffer.close()
          closed = true
        }
      }
  }
}
