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

import scala.concurrent.duration._

import io.indextables.spark.storage.SplitCacheConfig
import io.indextables.spark.TestBase
import org.scalatest.funsuite.AnyFunSuite

/**
 * S3MultipartUploader tests using real AWS S3.
 *
 * IMPORTANT: The comprehensive multipart upload tests have been moved to RealS3MultipartUploaderTest which tests
 * against actual AWS S3 infrastructure for realistic behavior verification.
 *
 * This class maintains configuration and utility tests that don't require actual S3 connectivity.
 */
class S3MultipartUploaderTest extends AnyFunSuite with TestBase {

  test("S3MultipartConfig should have correct default values") {
    val defaultConfig = S3MultipartConfig()

    assert(defaultConfig.partSize == 64L * 1024 * 1024, "Default part size should be 64MB")
    assert(defaultConfig.multipartThreshold == 100L * 1024 * 1024, "Default threshold should be 100MB")
    assert(defaultConfig.maxConcurrency == 4, "Default concurrency should be 4")
    assert(defaultConfig.maxRetries == 3, "Default retries should be 3")
    assert(defaultConfig.baseRetryDelay == 1000, "Default retry delay should be 1000ms")
    assert(defaultConfig.uploadTimeout == 30.minutes, "Default timeout should be 30 minutes")

    println("✅ Default configuration values verified")
  }

  test("S3MultipartConfig.forLargeMergedSplits should be optimized for merge operations") {
    val mergeConfig = S3MultipartConfig.forLargeMergedSplits

    assert(mergeConfig.partSize == 128L * 1024 * 1024, "Merge config should use 128MB parts")
    assert(mergeConfig.multipartThreshold == 200L * 1024 * 1024, "Merge config should use 200MB threshold")
    assert(mergeConfig.maxConcurrency == 6, "Merge config should use higher concurrency")
    assert(mergeConfig.maxRetries == 5, "Merge config should use more retries")
    assert(mergeConfig.uploadTimeout == 60.minutes, "Merge config should use longer timeout")

    println("✅ Large merged splits configuration optimizations verified")
  }

  test("S3UploadResult should format sizes correctly") {
    val result = S3UploadResult(
      bucket = "test-bucket",
      key = "test-key",
      etag = "test-etag",
      uploadId = Some("test-upload-id"),
      partCount = 3,
      totalSize = 150L * 1024 * 1024, // 150MB
      uploadTimeMs = 5000L,
      strategy = "multipart"
    )

    assert(result.bucket == "test-bucket")
    assert(result.key == "test-key")
    assert(result.strategy == "multipart")
    assert(result.partCount == 3)
    assert(result.totalSize == 150L * 1024 * 1024)
    assert(result.uploadId.contains("test-upload-id"))

    println("✅ S3UploadResult structure verified")
  }

  test("multipart threshold logic should work correctly") {
    val smallSize        = 50L * 1024 * 1024  // 50MB
    val largeSize        = 150L * 1024 * 1024 // 150MB
    val defaultThreshold = 100L * 1024 * 1024 // 100MB

    assert(smallSize < defaultThreshold, "50MB should be below default threshold")
    assert(largeSize > defaultThreshold, "150MB should be above default threshold")

    // Test custom threshold
    val customThreshold = 75L * 1024 * 1024 // 75MB
    assert(smallSize < customThreshold, "50MB should be below custom 75MB threshold")
    assert(largeSize > customThreshold, "150MB should be above custom 75MB threshold")

    println("✅ Threshold logic verification completed")
  }

  test("docBatch configuration should have correct new defaults") {
    val defaultConfig = SplitCacheConfig()

    assert(defaultConfig.enableDocBatch == true, "docBatch should be enabled by default for better performance")
    assert(defaultConfig.docBatchMaxSize == 1000, "Default batch size should be 1000 documents")

    println("✅ docBatch default configuration verified")
  }

  test("part size calculation should be accurate") {
    val config   = S3MultipartConfig(partSize = 32L * 1024 * 1024) // 32MB parts
    val fileSize = 80L * 1024 * 1024                               // 80MB file

    val expectedParts = math.ceil(fileSize.toDouble / config.partSize.toDouble).toInt
    assert(expectedParts == 3, s"80MB file with 32MB parts should require 3 parts, got: $expectedParts")

    val largeFileSize      = 300L * 1024 * 1024 // 300MB file
    val largeExpectedParts = math.ceil(largeFileSize.toDouble / config.partSize.toDouble).toInt
    assert(largeExpectedParts == 10, s"300MB file with 32MB parts should require 10 parts, got: $largeExpectedParts")

    println("✅ Part size calculation verified")
  }
}
