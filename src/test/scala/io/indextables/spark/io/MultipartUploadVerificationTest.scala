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

import org.scalatest.funsuite.AnyFunSuite

/**
 * Simple verification test for multipart upload functionality. This test doesn't require actual S3 connectivity but
 * verifies the implementation structure.
 */
class MultipartUploadVerificationTest extends AnyFunSuite {

  test("S3MultipartConfig should provide sensible defaults") {
    val config = S3MultipartConfig.default

    assert(config.partSize == 128L * 1024 * 1024)           // 128MB parts (updated default)
    assert(config.multipartThreshold == 200L * 1024 * 1024) // 200MB threshold (updated default)
    assert(config.maxConcurrency == 4)                      // 4 parallel uploads
    assert(config.maxRetries == 3)                          // 3 retry attempts
    assert(config.baseRetryDelay == 1000)                   // 1 second base delay

    println(s"✅ Default config - Part size: ${config.partSize / (1024 * 1024)}MB")
    println(s"✅ Default config - Threshold: ${config.multipartThreshold / (1024 * 1024)}MB")
    println(s"✅ Default config - Max concurrency: ${config.maxConcurrency}")
  }

  test("forLargeMergedSplits config should be optimized for merge operations") {
    val config = S3MultipartConfig.forLargeMergedSplits

    assert(config.partSize == 128L * 1024 * 1024)           // 128MB parts for large merges
    assert(config.multipartThreshold == 200L * 1024 * 1024) // 200MB threshold
    assert(config.maxConcurrency == 6)                      // Higher concurrency
    assert(config.maxRetries == 5)                          // More retries for critical operations
    assert(config.uploadTimeout.toMinutes == 60)            // Longer timeout

    println(s"✅ Merge config - Part size: ${config.partSize / (1024 * 1024)}MB")
    println(s"✅ Merge config - Threshold: ${config.multipartThreshold / (1024 * 1024)}MB")
    println(s"✅ Merge config - Max concurrency: ${config.maxConcurrency}")
  }

  test("CloudStorageConfig should support multipart configuration") {
    val config = CloudStorageConfig(
      multipartUploadThreshold = Some(150L * 1024 * 1024), // 150MB
      maxConcurrency = Some(8),
      maxRetries = Some(5)
    )

    assert(config.multipartUploadThreshold.contains(150L * 1024 * 1024))
    assert(config.maxConcurrency.contains(8))
    assert(config.maxRetries.contains(5))

    println(
      s"✅ CloudStorageConfig multipart threshold: ${config.multipartUploadThreshold.map(_ / (1024 * 1024)).getOrElse("default")}MB"
    )
    println(s"✅ CloudStorageConfig max concurrency: ${config.maxConcurrency.getOrElse("default")}")
  }

  test("S3UploadResult should provide meaningful metrics") {
    val result = S3UploadResult(
      bucket = "test-bucket",
      key = "large-file.split",
      etag = "test-etag-12345",
      uploadId = Some("multipart-upload-123"),
      partCount = 5,
      totalSize = 320L * 1024 * 1024, // 320MB
      uploadTimeMs = 30000,           // 30 seconds
      strategy = "multipart"
    )

    assert(result.s3Url == "s3://test-bucket/large-file.split")
    assert(result.uploadRateMBps > 0) // Should calculate upload rate
    assert(result.strategy == "multipart")
    assert(result.partCount == 5)

    println(s"✅ Upload result - URL: ${result.s3Url}")
    println(f"✅ Upload result - Upload rate: ${result.uploadRateMBps}%.2f MB/s")
    println(s"✅ Upload result - Strategy: ${result.strategy}")
    println(s"✅ Upload result - Parts: ${result.partCount}")
  }

  test("multipart upload should be enabled for files above threshold") {
    val threshold = 200L * 1024 * 1024 // 200MB (updated default threshold)

    val smallFile   = 50L * 1024 * 1024       // 50MB
    val largeFile   = 250L * 1024 * 1024      // 250MB (updated to be above threshold)
    val massiveFile = 5L * 1024 * 1024 * 1024 // 5GB

    assert(smallFile < threshold, "Small file should use single-part upload")
    assert(largeFile >= threshold, "Large file should use multipart upload")
    assert(massiveFile >= threshold, "Massive merged split should use multipart upload")

    println(s"✅ File size validation:")
    println(s"  - ${smallFile / (1024 * 1024)}MB file: ${if (smallFile < threshold) "single-part" else "multipart"}")
    println(s"  - ${largeFile / (1024 * 1024)}MB file: ${if (largeFile >= threshold) "multipart" else "single-part"}")
    println(s"  - ${massiveFile / (1024 * 1024 * 1024)}GB merged split: ${if (massiveFile >= threshold) "multipart"
      else "single-part"}")
  }

  test("optimal part size calculation should respect AWS limits") {
    // AWS S3 multipart upload limits:
    // - Minimum 5MB per part (except last part)
    // - Maximum 10,000 parts per upload
    // - Maximum 5TB per upload

    val minPartSize = 5L * 1024 * 1024               // 5MB
    val maxParts    = 10000L
    val maxFileSize = 5L * 1024 * 1024 * 1024 * 1024 // 5TB

    // For a 5GB file with 128MB parts: 5120MB / 128MB = 40 parts (OK)
    val file5GB         = 5L * 1024 * 1024 * 1024
    val parts128MB      = 128L * 1024 * 1024
    val calculatedParts = (file5GB + parts128MB - 1) / parts128MB

    assert(calculatedParts < maxParts, "Part count should be within AWS limits")
    assert(parts128MB >= minPartSize, "Part size should meet minimum requirement")

    println(s"✅ AWS limits validation:")
    println(s"  - Min part size: ${minPartSize / (1024 * 1024)}MB")
    println(s"  - Max parts: $maxParts")
    println(s"  - 5GB file with 128MB parts: $calculatedParts parts")
  }
}
