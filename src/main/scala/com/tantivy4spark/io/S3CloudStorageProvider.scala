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

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsSessionCredentials, StaticCredentialsProvider, DefaultCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import org.slf4j.LoggerFactory
import java.io.{InputStream, OutputStream, ByteArrayInputStream, ByteArrayOutputStream}
import java.net.URI
import java.util.concurrent.{CompletableFuture, Executors}
import scala.jdk.CollectionConverters._
import scala.util.{Try}

/**
 * High-performance S3 storage provider using AWS SDK directly.
 * Bypasses Hadoop filesystem for better performance and reliability.
 */
class S3CloudStorageProvider(config: CloudStorageConfig) extends CloudStorageProvider {
  
  private val logger = LoggerFactory.getLogger(classOf[S3CloudStorageProvider])
  private val executor = Executors.newCachedThreadPool()
  
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
  private def normalizeProtocolForTantivy(path: String): String = {
    if (path.startsWith("s3a://") || path.startsWith("s3n://")) {
      path.replaceFirst("^s3[an]://", "s3://")
    } else {
      path
    }
  }
  
  // Convert nested paths to flat structure: path/to/file.txt -> path___to___file.txt
  private def flattenPathForS3Mock(key: String): String = {
    if (isS3Mock && key.contains("/")) {
      key.replace("/", S3_MOCK_DIR_SEPARATOR)
    } else {
      key
    }
  }
  
  // Convert flat paths back to nested: path___to___file.txt -> path/to/file.txt
  private def unflattenPathFromS3Mock(key: String): String = {
    if (isS3Mock && key.contains(S3_MOCK_DIR_SEPARATOR)) {
      key.replace(S3_MOCK_DIR_SEPARATOR, "/")
    } else {
      key
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
    
    // Configure credentials - only use DefaultCredentialsProvider if no credentials are configured
    val credentialsProvider = (config.awsAccessKey, config.awsSecretKey, config.awsSessionToken) match {
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
    
    builder.credentialsProvider(credentialsProvider).build()
  }
  
  override def listFiles(path: String, recursive: Boolean = false): Seq[CloudFileInfo] = {
    val (bucket, originalPrefix) = parseS3Path(path)
    
    // Apply uniform path flattening for S3Mock compatibility
    val basePrefix = flattenPathForS3Mock(originalPrefix)
    
    // Add trailing slash only for transaction log directory listings to prevent matching
    // similarly named directories like _transaction_log_backup
    val prefix = if (basePrefix.nonEmpty && 
                     (basePrefix.endsWith("/_transaction_log") || basePrefix == "_transaction_log") &&
                     !basePrefix.endsWith("/")) {
      basePrefix + "/"
    } else {
      basePrefix
    }
    
    try {
      logger.info(s"🔍 S3 LIST DEBUG - Listing S3 files: bucket=$bucket, prefix=$prefix (original: $originalPrefix), recursive=$recursive, isS3Mock=$isS3Mock")
      val request = ListObjectsV2Request.builder()
        .bucket(bucket)
        .prefix(prefix)
        .delimiter(if (recursive || isS3Mock) null else "/") // For S3Mock, always list recursively since we flatten paths
        .build()
      
      val response = s3Client.listObjectsV2(request)
      
      val files = response.contents().asScala.map { s3Object =>
        // Transform the path back to the original format for the caller
        val originalKey = s3Object.key()
        val displayKey = unflattenPathFromS3Mock(originalKey)
        
        logger.debug(s"🔍 S3 LIST ITEM - Original key: $originalKey, Display key: $displayKey")
        
        CloudFileInfo(
          path = s"s3://$bucket/$displayKey",
          size = s3Object.size(),
          modificationTime = s3Object.lastModified().toEpochMilli,
          isDirectory = false
        )
      }.toSeq
      
      // For S3Mock with flattened paths, we don't have real directories
      val directories = if (!recursive && !isS3Mock) {
        response.commonPrefixes().asScala.map { commonPrefix =>
          val displayPrefix = unflattenPathFromS3Mock(commonPrefix.prefix())
          
          CloudFileInfo(
            path = s"s3://$bucket/$displayPrefix",
            size = 0L,
            modificationTime = 0L,
            isDirectory = true
          )
        }.toSeq
      } else Seq.empty
      
      val allResults = files ++ directories
      logger.info(s"🔍 S3 LIST RESULTS - Found ${files.size} files and ${directories.size} directories for prefix '$prefix'")
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
      val request = HeadObjectRequest.builder()
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
      val request = HeadObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build()
      
      val response = s3Client.headObject(request)
      
      Some(CloudFileInfo(
        path = path,
        size = response.contentLength(),
        modificationTime = response.lastModified().toEpochMilli,
        isDirectory = false
      ))
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
      
      val request = GetObjectRequest.builder()
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
  
  override def readRange(path: String, offset: Long, length: Long): Array[Byte] = {
    val (bucket, key) = parseS3Path(path)
    val rangeHeader = s"bytes=$offset-${offset + length - 1}"
    
    try {
      logger.debug(s"Reading S3 range: s3://$bucket/$key, range=$rangeHeader")
      
      val request = GetObjectRequest.builder()
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
      val request = GetObjectRequest.builder()
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
  
  override def createOutputStream(path: String): OutputStream = {
    // For S3, we need to buffer the output and upload when the stream is closed
    new S3OutputStream(path)
  }
  
  override def writeFile(path: String, content: Array[Byte]): Unit = {
    val (bucket, originalKey) = parseS3Path(path)
    
    // Apply uniform path flattening for S3Mock compatibility
    val key = flattenPathForS3Mock(originalKey)
    
    try {
      logger.info(s"🔧 S3 WRITE DEBUG - Path: $path")
      logger.info(s"🔧 S3 WRITE DEBUG - Bucket: '$bucket', Key: '$key' (original: '$originalKey')")
      logger.info(s"🔧 S3 WRITE DEBUG - Content length: ${content.length} bytes")
      logger.info(s"🔧 S3 WRITE DEBUG - S3Mock mode: $isS3Mock")
      
      // Ensure bucket exists first
      ensureBucketExists(bucket)
      
      val request = PutObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .contentLength(content.length.toLong)
        .build()
      
      val requestBody = RequestBody.fromBytes(content)
      s3Client.putObject(request, requestBody)
      
      logger.info(s"✅ Successfully wrote S3 file: s3://$bucket/$key")
    } catch {
      case ex: Exception =>
        logger.error(s"❌ Failed to write S3 file: s3://$bucket/$key", ex)
        throw new RuntimeException(s"Failed to write S3 file: ${ex.getMessage}", ex)
    }
  }
  
  /**
   * Ensure bucket exists before writing files
   */
  private def ensureBucketExists(bucket: String): Unit = {
    try {
      val bucketExistsRequest = software.amazon.awssdk.services.s3.model.HeadBucketRequest.builder()
        .bucket(bucket)
        .build()
      
      s3Client.headBucket(bucketExistsRequest)
      logger.debug(s"✅ Bucket exists: $bucket")
    } catch {
      case _: software.amazon.awssdk.services.s3.model.NoSuchBucketException =>
        logger.info(s"🔧 Creating missing bucket: $bucket")
        val createBucketRequest = software.amazon.awssdk.services.s3.model.CreateBucketRequest.builder()
          .bucket(bucket)
          .build()
        s3Client.createBucket(createBucketRequest)
        logger.info(s"✅ Created S3 bucket: $bucket")
      case ex: Exception =>
        logger.warn(s"⚠️  Could not verify bucket existence for $bucket: ${ex.getMessage}")
        // Continue anyway - the putObject call will fail if bucket really doesn't exist
    }
  }
  
  override def deleteFile(path: String): Boolean = {
    val (bucket, key) = parseS3Path(path)
    
    try {
      val request = DeleteObjectRequest.builder()
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
        val bucketExistsRequest = software.amazon.awssdk.services.s3.model.HeadBucketRequest.builder()
          .bucket(bucket)
          .build()
        
        try {
          s3Client.headBucket(bucketExistsRequest)
          logger.debug(s"Bucket already exists: $bucket")
        } catch {
          case _: software.amazon.awssdk.services.s3.model.NoSuchBucketException =>
            // Create bucket
            val createBucketRequest = software.amazon.awssdk.services.s3.model.CreateBucketRequest.builder()
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
      CompletableFuture.supplyAsync(() => {
        try {
          path -> Some(readFile(path))
        } catch {
          case ex: Exception =>
            logger.error(s"Failed to read S3 file in parallel: $path", ex)
            path -> None
        }
      }, executor)
    }
    
    try {
      val results = futures.map(_.get()).collect {
        case (path, Some(content)) => path -> content
      }.toMap
      
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
    
    val futures = paths.map { path =>
      CompletableFuture.supplyAsync(() => {
        path -> exists(path)
      }, executor)
    }
    
    try {
      futures.map(_.get()).toMap
    } catch {
      case ex: Exception =>
        logger.error("Failed to complete parallel S3 existence checks", ex)
        paths.map(_ -> false).toMap
    }
  }
  
  override def getProviderType: String = "s3"
  
  override def close(): Unit = {
    executor.shutdown()
    s3Client.close()
    logger.debug("Closed S3 storage provider")
  }
  
  /**
   * Normalize path for tantivy4java compatibility.
   * Converts s3a:// and s3n:// protocols to s3:// which tantivy4java understands.
   */
  override def normalizePathForTantivy(path: String): String = {
    val protocolNormalized = normalizeProtocolForTantivy(path)
    
    // Apply path flattening for S3Mock compatibility
    if (isS3Mock) {
      val (bucket, key) = parseS3Path(protocolNormalized)
      val flattenedKey = flattenPathForS3Mock(key)
      s"s3://$bucket/$flattenedKey"
    } else {
      protocolNormalized
    }
  }
  
  /**
   * Parse S3 path into bucket and key components
   */
  private def parseS3Path(path: String): (String, String) = {
    val uri = URI.create(path)
    val bucket = uri.getHost
    val key = uri.getPath.stripPrefix("/")
    (bucket, key)
  }
  
  /**
   * Custom OutputStream for S3 that buffers content and uploads on close
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
    
    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      if (closed) throw new IllegalStateException("Stream is closed")
      buffer.write(b, off, len)
    }
    
    override def close(): Unit = {
      if (!closed) {
        try {
          writeFile(path, buffer.toByteArray)
        } finally {
          buffer.close()
          closed = true
        }
      }
    }
  }
}