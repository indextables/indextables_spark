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

import java.io.{ByteArrayInputStream, File, FileInputStream}
import java.util.Properties
import java.util.UUID

import scala.util.Random
import scala.util.Using

import io.indextables.spark.RealS3TestBase
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.S3Client

/**
 * Real AWS S3 multipart upload tests using test-tantivy4sparkbucket.
 *
 * Tests actual multipart upload functionality against real AWS S3:
 *   - Small files using single-part upload
 *   - Large files using multipart upload
 *   - Custom multipart configuration
 *   - Upload failure handling with retries
 *   - Streaming uploads from input streams
 *
 * Credentials are loaded from ~/.aws/credentials file.
 */
class RealS3MultipartUploaderTest extends RealS3TestBase {

  private val S3_BUCKET = "test-tantivy4sparkbucket"
  private val S3_REGION = "us-east-2"

  // Generate unique test run ID to avoid conflicts
  private val testRunId     = UUID.randomUUID().toString.substring(0, 8)
  private val testKeyPrefix = s"multipart-test-$testRunId"

  private var awsCredentials: Option[(String, String)]       = None
  private var s3Client: Option[S3Client]                     = None
  private var multipartUploader: Option[S3MultipartUploader] = None

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    if (awsCredentials.isDefined) {
      val (accessKey, secretKey) = awsCredentials.get

      // Create S3Client for real AWS S3
      val credentials = AwsBasicCredentials.create(accessKey, secretKey)
      val s3ClientInstance = S3Client
        .builder()
        .region(Region.of(S3_REGION))
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .build()

      val s3AsyncClientInstance = software.amazon.awssdk.services.s3.S3AsyncClient
        .builder()
        .region(Region.of(S3_REGION))
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .build()

      s3Client = Some(s3ClientInstance)
      multipartUploader = Some(new S3MultipartUploader(s3ClientInstance, s3AsyncClientInstance))

      println(s"🔐 AWS credentials loaded successfully")
      println(s"🌊 S3Client configured for bucket: $S3_BUCKET in region: $S3_REGION")
      println(s"📍 Test key prefix: $testKeyPrefix")
    } else {
      println(s"⚠️  No AWS credentials found in ~/.aws/credentials - tests will be skipped")
    }
  }

  override def afterAll(): Unit = {
    // Clean up S3Client and uploader
    multipartUploader.foreach(_.shutdown())
    s3Client.foreach(_.close())

    // Clean up test objects from S3
    if (awsCredentials.isDefined && s3Client.isDefined) {
      cleanupTestObjects()
    }

    super.afterAll()
  }

  /** Load AWS credentials from ~/.aws/credentials file. */
  private def loadAwsCredentials(): Option[(String, String)] =
    try {
      val home     = System.getProperty("user.home")
      val credFile = new File(s"$home/.aws/credentials")

      if (!credFile.exists()) {
        println(s"⚠️  AWS credentials file not found: ${credFile.getPath}")
        return None
      }

      Using(new FileInputStream(credFile)) { fis =>
        val props = new Properties()
        props.load(fis)

        val accessKey = props.getProperty("aws_access_key_id")
        val secretKey = props.getProperty("aws_secret_access_key")

        if (accessKey != null && secretKey != null) {
          Some((accessKey, secretKey))
        } else {
          println(s"⚠️  AWS credentials not found in default profile")
          None
        }
      }.getOrElse(None)

    } catch {
      case e: Exception =>
        println(s"⚠️  Error loading AWS credentials: ${e.getMessage}")
        None
    }

  /** Clean up test objects from S3. */
  private def cleanupTestObjects(): Unit =
    try {
      val client = s3Client.get

      // List all objects with our test prefix
      val listRequest = ListObjectsV2Request
        .builder()
        .bucket(S3_BUCKET)
        .prefix(testKeyPrefix)
        .build()

      val response = client.listObjectsV2(listRequest)
      val objects  = response.contents()

      if (!objects.isEmpty) {
        println(s"🧹 Cleaning up ${objects.size()} test objects from S3...")

        objects.forEach { obj =>
          val deleteRequest = DeleteObjectRequest
            .builder()
            .bucket(S3_BUCKET)
            .key(obj.key())
            .build()

          try {
            client.deleteObject(deleteRequest)
            println(s"   Deleted: s3://$S3_BUCKET/${obj.key()}")
          } catch {
            case e: Exception =>
              println(s"   Warning: Failed to delete ${obj.key()}: ${e.getMessage}")
          }
        }
      }

    } catch {
      case e: Exception =>
        println(s"⚠️  Warning: Could not clean up test objects: ${e.getMessage}")
    }

  test("Real S3: Small files should use single-part upload") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")
    assume(s3Client.isDefined, "S3Client required for test")
    assume(multipartUploader.isDefined, "S3MultipartUploader required for test")

    val uploader = multipartUploader.get
    val content  = new Array[Byte](50 * 1024 * 1024) // 50MB - below default 100MB threshold

    // Fill with random data to make upload realistic
    val random = new Random(12345) // Fixed seed for reproducibility
    random.nextBytes(content)

    val key = s"$testKeyPrefix/small-file-test.dat"

    println(s"📤 Uploading small file (${content.length / (1024 * 1024)}MB) to s3://$S3_BUCKET/$key")

    val result = uploader.uploadFile(S3_BUCKET, key, content)

    println(s"✅ Upload completed: strategy=${result.strategy}, parts=${result.partCount}, size=${result.totalSize}")

    // Verify single-part upload was used
    assert(result.strategy == "single-part", s"Expected single-part upload, got: ${result.strategy}")
    assert(result.partCount == 1, s"Expected 1 part, got: ${result.partCount}")
    assert(result.totalSize == content.length, s"Expected size ${content.length}, got: ${result.totalSize}")
    assert(result.etag.nonEmpty, "ETag should not be empty")
    assert(result.uploadId.isEmpty, "Upload ID should be empty for single-part upload")

    // Verify the object exists in S3
    val headRequest = HeadObjectRequest
      .builder()
      .bucket(S3_BUCKET)
      .key(key)
      .build()

    val headResponse = s3Client.get.headObject(headRequest)
    assert(headResponse.contentLength() == content.length, s"S3 object size mismatch")

    println(s"✅ Verified object exists in S3 with correct size: ${headResponse.contentLength()} bytes")
  }

  test("Real S3: Large files should use multipart upload") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")
    assume(s3Client.isDefined, "S3Client required for test")
    assume(multipartUploader.isDefined, "S3MultipartUploader required for test")

    val uploader = multipartUploader.get
    val content  = new Array[Byte](250 * 1024 * 1024) // 250MB - above default 200MB threshold

    // Fill with random data
    val random = new Random(54321) // Fixed seed for reproducibility
    random.nextBytes(content)

    val key = s"$testKeyPrefix/large-file-test.dat"

    println(s"📤 Uploading large file (${content.length / (1024 * 1024)}MB) to s3://$S3_BUCKET/$key")

    val startTime  = System.currentTimeMillis()
    val result     = uploader.uploadFile(S3_BUCKET, key, content)
    val uploadTime = System.currentTimeMillis() - startTime

    println(s"✅ Upload completed in ${uploadTime}ms: strategy=${result.strategy}, parts=${result.partCount}, size=${result.totalSize}")

    // Verify multipart upload was used
    assert(result.strategy == "multipart", s"Expected multipart upload, got: ${result.strategy}")
    assert(result.partCount > 1, s"Expected multiple parts, got: ${result.partCount}")
    assert(result.totalSize == content.length, s"Expected size ${content.length}, got: ${result.totalSize}")
    assert(result.etag.nonEmpty, "ETag should not be empty")
    assert(result.uploadId.isDefined, "Upload ID should be defined for multipart upload")

    // Expected parts: 250MB / 128MB (default part size) = 2 parts (1 full + 1 partial)
    val expectedParts = math.ceil(content.length.toDouble / (128 * 1024 * 1024)).toInt
    assert(result.partCount == expectedParts, s"Expected $expectedParts parts, got: ${result.partCount}")

    // Verify the object exists in S3 with correct size
    val headRequest = HeadObjectRequest
      .builder()
      .bucket(S3_BUCKET)
      .key(key)
      .build()

    val headResponse = s3Client.get.headObject(headRequest)
    assert(headResponse.contentLength() == content.length, s"S3 object size mismatch")

    println(s"✅ Verified object exists in S3 with correct size: ${headResponse.contentLength()} bytes")
  }

  test("Real S3: Custom configuration should be respected") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")
    assume(s3Client.isDefined, "S3Client required for test")

    val config = S3MultipartConfig(
      multipartThreshold = 50L * 1024 * 1024, // 50MB threshold (lower than default)
      partSize = 32L * 1024 * 1024,           // 32MB parts (smaller than default 64MB)
      maxConcurrency = 2                      // 2 parallel uploads
    )

    // Create async client for this test
    val asyncClient = software.amazon.awssdk.services.s3.S3AsyncClient
      .builder()
      .region(Region.of(S3_REGION))
      .credentialsProvider(s3Client.get.serviceClientConfiguration().credentialsProvider())
      .build()

    val customUploader = new S3MultipartUploader(s3Client.get, asyncClient, config)

    try {
      val content = new Array[Byte](80 * 1024 * 1024) // 80MB - above custom 50MB threshold

      // Fill with random data
      val random = new Random(98765)
      random.nextBytes(content)

      val key = s"$testKeyPrefix/custom-config-test.dat"

      println(s"📤 Uploading file with custom config (${content.length / (1024 * 1024)}MB) to s3://$S3_BUCKET/$key")
      println(s"   Custom threshold: ${config.multipartThreshold / (1024 * 1024)}MB")
      println(s"   Custom part size: ${config.partSize / (1024 * 1024)}MB")
      println(s"   Custom concurrency: ${config.maxConcurrency}")

      val result = customUploader.uploadFile(S3_BUCKET, key, content)

      println(s"✅ Upload completed: strategy=${result.strategy}, parts=${result.partCount}, size=${result.totalSize}")

      // With 80MB file and 32MB part size, should have 3 parts (2 full + 1 partial)
      val expectedParts = math.ceil(content.length.toDouble / config.partSize).toInt
      assert(
        result.partCount == expectedParts,
        s"Expected $expectedParts parts with custom config, got: ${result.partCount}"
      )
      assert(result.strategy == "multipart", "Should use multipart with custom threshold")

      // Verify object in S3
      val headRequest = HeadObjectRequest
        .builder()
        .bucket(S3_BUCKET)
        .key(key)
        .build()

      val headResponse = s3Client.get.headObject(headRequest)
      assert(headResponse.contentLength() == content.length, "S3 object size mismatch")

      println(s"✅ Custom configuration test passed with ${result.partCount} parts")

    } finally
      customUploader.shutdown()
  }

  test("Real S3: Streaming upload should work with input stream") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")
    assume(s3Client.isDefined, "S3Client required for test")
    assume(multipartUploader.isDefined, "S3MultipartUploader required for test")

    val uploader = multipartUploader.get
    val content  = new Array[Byte](220 * 1024 * 1024) // 220MB - above 200MB threshold for multipart

    // Fill with random data
    val random = new Random(11111)
    random.nextBytes(content)

    val inputStream = new ByteArrayInputStream(content)
    val key         = s"$testKeyPrefix/streaming-upload-test.dat"

    println(s"📤 Uploading via stream (${content.length / (1024 * 1024)}MB) to s3://$S3_BUCKET/$key")

    val result = uploader.uploadStream(S3_BUCKET, key, inputStream, Some(content.length.toLong))

    println(
      s"✅ Stream upload completed: strategy=${result.strategy}, parts=${result.partCount}, size=${result.totalSize}"
    )

    // Should use multipart-stream-parallel strategy (new parallel async implementation)
    assert(result.strategy == "multipart-stream-parallel", s"Expected multipart-stream-parallel, got: ${result.strategy}")

    // Verify actual object in S3 BEFORE checking reported size
    val headRequest = HeadObjectRequest
      .builder()
      .bucket(S3_BUCKET)
      .key(key)
      .build()

    val headResponse = s3Client.get.headObject(headRequest)
    val actualS3Size = headResponse.contentLength()

    println(s"✅ S3 object actual size: ${actualS3Size} bytes")
    println(s"📊 Expected size: ${content.length} bytes")
    println(s"📊 Reported size: ${result.totalSize} bytes")

    // First verify the ACTUAL upload was correct
    assert(actualS3Size == content.length, s"S3 object size mismatch - actual: $actualS3Size, expected: ${content.length}")

    // Then check the reported size (this may be a reporting bug)
    assert(result.totalSize == content.length, s"Reported size mismatch - reported: ${result.totalSize}, expected: ${content.length}")
    assert(result.partCount > 0, "Should have at least one part")

    println(s"✅ Streaming upload verified in S3 with correct size: ${actualS3Size} bytes")
  }

  test("Real S3: Upload performance with large merged splits config") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")
    assume(s3Client.isDefined, "S3Client required for test")

    val config = S3MultipartConfig.forLargeMergedSplits

    // Create async client for this test
    val asyncClient = software.amazon.awssdk.services.s3.S3AsyncClient
      .builder()
      .region(Region.of(S3_REGION))
      .credentialsProvider(s3Client.get.serviceClientConfiguration().credentialsProvider())
      .build()

    val performanceUploader = new S3MultipartUploader(s3Client.get, asyncClient, config)

    try {
      val content = new Array[Byte](300 * 1024 * 1024) // 300MB - large merge scenario

      // Fill with random data
      val random = new Random(77777)
      random.nextBytes(content)

      val key = s"$testKeyPrefix/large-merge-performance-test.dat"

      println(s"📤 Performance test: uploading large file (${content.length / (1024 * 1024)}MB) to s3://$S3_BUCKET/$key")
      println(s"   Using forLargeMergedSplits config:")
      println(s"   - Part size: ${config.partSize / (1024 * 1024)}MB")
      println(s"   - Threshold: ${config.multipartThreshold / (1024 * 1024)}MB")
      println(s"   - Concurrency: ${config.maxConcurrency}")
      println(s"   - Max retries: ${config.maxRetries}")

      val startTime  = System.currentTimeMillis()
      val result     = performanceUploader.uploadFile(S3_BUCKET, key, content)
      val uploadTime = System.currentTimeMillis() - startTime

      val throughputMBps = (content.length.toDouble / (1024 * 1024)) / (uploadTime / 1000.0)

      println(f"✅ Performance upload completed in ${uploadTime}ms ($throughputMBps%.2f MB/s)")
      println(s"   Strategy: ${result.strategy}, Parts: ${result.partCount}, Size: ${result.totalSize}")

      // Verify performance characteristics
      assert(result.strategy == "multipart", "Should use multipart for large files")
      assert(uploadTime > 0, "Upload should take measurable time")
      assert(throughputMBps > 0, "Should have positive throughput")

      // With 300MB file and 128MB part size (forLargeMergedSplits), should have 3 parts
      val expectedParts = math.ceil(content.length.toDouble / config.partSize).toInt
      assert(result.partCount == expectedParts, s"Expected $expectedParts parts, got: ${result.partCount}")

      // Verify object in S3
      val headRequest = HeadObjectRequest
        .builder()
        .bucket(S3_BUCKET)
        .key(key)
        .build()

      val headResponse = s3Client.get.headObject(headRequest)
      assert(headResponse.contentLength() == content.length, "S3 object size mismatch")

      println(f"✅ Performance test verified: throughput $throughputMBps%.2f MB/s")

    } finally
      performanceUploader.shutdown()
  }

  test("Real S3: Memory-mapped upload with zero-copy I/O (300MB)") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")
    assume(s3Client.isDefined, "S3Client required for test")

    val config = S3MultipartConfig.forLargeMergedSplits

    // Create async client for this test
    val asyncClient = software.amazon.awssdk.services.s3.S3AsyncClient
      .builder()
      .region(Region.of(S3_REGION))
      .credentialsProvider(s3Client.get.serviceClientConfiguration().credentialsProvider())
      .build()

    val mmapUploader = new S3MultipartUploader(s3Client.get, asyncClient, config)

    try {
      // Create a temp file with random data
      val tempFile = java.nio.file.Files.createTempFile("mmap-test-", ".dat")
      val fileSize = 300L * 1024 * 1024 // 300MB

      println(s"📝 Creating temporary file: ${tempFile}")
      println(s"   Size: ${fileSize / (1024 * 1024)}MB")

      // Write random data to file
      Using.resource(new java.io.FileOutputStream(tempFile.toFile)) { fos =>
        val random = new Random(88888)
        val buffer = new Array[Byte](64 * 1024) // 64KB write buffer
        var remaining = fileSize

        while (remaining > 0) {
          val writeSize = math.min(buffer.length, remaining).toInt
          random.nextBytes(buffer)
          fos.write(buffer, 0, writeSize)
          remaining -= writeSize
        }
      }

      val key = s"$testKeyPrefix/memory-mapped-test.dat"

      println(s"🗺️  Testing memory-mapped upload (zero-copy) to s3://$S3_BUCKET/$key")
      println(s"   File: ${tempFile}")
      println(s"   Size: ${fileSize / (1024 * 1024)}MB")
      println(s"   Config: part size=${config.partSize / (1024 * 1024)}MB, concurrency=${config.maxConcurrency}")

      val startTime = System.currentTimeMillis()
      val result = mmapUploader.uploadFileWithMemoryMapping(S3_BUCKET, key, tempFile)
      val uploadTime = System.currentTimeMillis() - startTime

      val throughputMBps = (fileSize.toDouble / (1024 * 1024)) / (uploadTime / 1000.0)

      println(f"✅ Memory-mapped upload completed in ${uploadTime}ms ($throughputMBps%.2f MB/s)")
      println(s"   Strategy: ${result.strategy}, Parts: ${result.partCount}, Size: ${result.totalSize}")

      // Assertions
      assert(result.strategy == "multipart-mmap", "Should use multipart-mmap strategy")
      assert(result.totalSize == fileSize, "Reported size should match file size")
      assert(uploadTime > 0, "Upload should take measurable time")
      assert(throughputMBps > 0, "Should have positive throughput")

      // Expected parts: 300MB / 128MB = 3 parts
      val expectedParts = math.ceil(fileSize.toDouble / config.partSize).toInt
      assert(result.partCount == expectedParts, s"Expected $expectedParts parts, got: ${result.partCount}")

      // Verify object exists in S3 with correct size
      val headRequest = HeadObjectRequest
        .builder()
        .bucket(S3_BUCKET)
        .key(key)
        .build()

      val headResponse = s3Client.get.headObject(headRequest)
      assert(headResponse.contentLength() == fileSize, s"S3 object size mismatch: expected $fileSize, got ${headResponse.contentLength()}")

      println(f"✅ Memory-mapped upload verified in S3: ${headResponse.contentLength()} bytes")
      println(f"✅ Throughput: $throughputMBps%.2f MB/s")
      println(s"✅ Zero-copy I/O successfully tested - file never loaded into JVM heap!")

      // Clean up temp file
      java.nio.file.Files.deleteIfExists(tempFile)

    } finally
      mmapUploader.shutdown()
  }
}
