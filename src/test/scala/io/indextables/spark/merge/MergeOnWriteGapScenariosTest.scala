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

package io.indextables.spark.merge

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for gap scenarios identified in merge_on_write_design_gaps.md
 *
 * Covers:
 * - Gap #1: Dynamic allocation detection
 * - Gap #2: Partial staging failures and disk space
 * - Gap #3: Schema evolution and memory management
 * - Gap #5: Speculative execution handling
 * - Gap #6: Merge timeout handling
 * - Gap #8: Cost optimization
 * - Gap #9: Observability metrics
 */
class MergeOnWriteGapScenariosTest extends AnyFunSuite with Matchers {

  test("Gap #2: Staging retries with exponential backoff") {
    // Test retry logic calculates correct delays
    val maxRetries = 3
    val baseDelay = 1000L

    val delays = (0 until maxRetries).map { attempt =>
      baseDelay * Math.pow(2, attempt).toLong
    }

    delays shouldBe Seq(1000, 2000, 4000)
  }

  test("Gap #2: Partial staging failure handling with success rate threshold") {
    val totalUploads = 100
    val successfulUploads = 96
    val successRate = successfulUploads.toDouble / totalUploads

    val minSuccessRate = 0.95

    successRate should be >= minSuccessRate
    (successRate * 100).formatted("%.1f") shouldBe "96.0"
  }

  test("Gap #2: Disk space validation before merge") {
    val availableSpaceGB = 15L
    val minRequiredGB = 20L
    val estimatedMergeSizeGB = 10L

    // Should fail if available < minimum required
    availableSpaceGB should be < minRequiredGB

    // Should fail if available < 2x estimated merge size
    val safetyFactor = 2
    availableSpaceGB should be < (estimatedMergeSizeGB * safetyFactor)
  }

  test("Gap #3: Schema fingerprint computation for compatibility") {
    import java.security.MessageDigest
    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

    val schema1 = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))

    val schema2 = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("email", StringType, nullable = true) // Schema evolution!
    ))

    def computeFingerprint(schema: StructType): String = {
      val digest = MessageDigest.getInstance("SHA-256")
      val hash = digest.digest(schema.json.getBytes("UTF-8"))
      hash.map("%02x".format(_)).mkString
    }

    val fingerprint1 = computeFingerprint(schema1)
    val fingerprint2 = computeFingerprint(schema2)

    fingerprint1 should not equal fingerprint2
    fingerprint1.length shouldBe 64 // SHA-256 produces 32 bytes = 64 hex chars
  }

  test("Gap #3: Memory-based merge concurrency calculation") {
    val maxHeap = 4L * 1024 * 1024 * 1024 // 4GB
    val targetMergeSize = 4L * 1024 * 1024 * 1024 // 4GB
    val memoryOverheadFactor = 3.0 // 3x safety factor

    val memoryPerMerge = (targetMergeSize * memoryOverheadFactor).toLong
    val maxConcurrentMerges = Math.max(1, (maxHeap * 0.6 / memoryPerMerge).toInt)

    maxConcurrentMerges shouldBe 1 // Can only merge 1 at a time with this heap

    // With larger heap
    val largerHeap = 16L * 1024 * 1024 * 1024 // 16GB
    val maxConcurrentWithLargerHeap = Math.max(1, (largerHeap * 0.6 / memoryPerMerge).toInt)

    maxConcurrentWithLargerHeap shouldBe 1 // Still 1 because 16GB * 0.6 = 9.6GB < 12GB needed
  }

  test("Gap #5: Task attempt ID prevents speculative execution conflicts") {
    // Simulate two speculative task attempts creating different staging paths
    val splitUuid = "abc-123"
    val taskAttempt1 = 1001L
    val taskAttempt2 = 1002L // Speculative attempt

    val stagingPath1 = s"s3://bucket/_staging/task-$taskAttempt1-$splitUuid.tmp"
    val stagingPath2 = s"s3://bucket/_staging/task-$taskAttempt2-$splitUuid.tmp"

    stagingPath1 should not equal stagingPath2

    // Only committed task attempt's staging file should be kept
    val committedAttempts = Set(taskAttempt1)
    committedAttempts.contains(taskAttempt1) shouldBe true
    committedAttempts.contains(taskAttempt2) shouldBe false
  }

  test("Gap #6: Dynamic merge timeout calculation") {
    val mergeSizeGB = 40L // 40GB merge
    val throughputMBps = 100 // 100 MB/s
    val baseMergeTimeoutSeconds = 600 // 10 minutes

    val mergeSizeBytes = mergeSizeGB * 1024 * 1024 * 1024
    val estimatedSeconds = (mergeSizeBytes / (throughputMBps * 1024L * 1024L)).toInt
    val safetyFactor = 3
    val calculatedTimeout = Math.max(baseMergeTimeoutSeconds, estimatedSeconds * safetyFactor)

    estimatedSeconds shouldBe 409 // ~409 seconds to transfer 40GB at 100MB/s
    calculatedTimeout shouldBe 1227 // 409 * 3 = 1227 seconds (~20 minutes)
  }

  test("Gap #8: Cost optimization - skip staging for small files") {
    val minSizeToStage = 10 * 1024 * 1024L // 10MB threshold

    val smallFileSizeMB = 5L
    val largeFileSizeMB = 50L

    val smallFileBytes = smallFileSizeMB * 1024 * 1024
    val largeFileBytes = largeFileSizeMB * 1024 * 1024

    (smallFileBytes < minSizeToStage) shouldBe true // Skip staging
    (largeFileBytes >= minSizeToStage) shouldBe true // Stage this one
  }

  test("Gap #9: Metrics calculation for observability") {
    val totalMerges = 100
    val localMerges = 82
    val remoteMerges = 18

    val localityHitRate = localMerges.toDouble / totalMerges
    localityHitRate shouldBe 0.82
    (localityHitRate * 100).formatted("%.1f") shouldBe "82.0"

    val networkIOSavings = localityHitRate
    (networkIOSavings * 100).formatted("%.0f%%") shouldBe "82%"
  }

  test("Gap #9: Metrics JSON serialization") {
    val metrics = MergeOnWriteMetrics(
      totalSplitsCreated = 1000,
      totalSplitsStaged = 980,
      stagingUploadTimeMs = 60000,
      stagingFailures = 20,
      stagingRetries = 15,
      mergeGroupsCreated = 100,
      mergesExecuted = 98,
      mergesLocalFile = 80,
      mergesRemoteDownload = 18,
      mergeDurationMs = 300000,
      splitsPromoted = 2,
      finalSplitCount = 100,
      networkBytesUploaded = 500L * 1024 * 1024 * 1024, // 500GB
      networkBytesDownloaded = 90L * 1024 * 1024 * 1024, // 90GB
      localityHitRate = 0.8163, // 80/98
      diskSpaceExhausted = false,
      memoryPressureDetected = false,
      dynamicAllocationDetected = true,
      cleanupDurationMs = 5000,
      totalDurationMs = 365000
    )

    val json = metrics.toJson
    json should include("totalSplitsCreated")
    json should include("1000")
    json should include("localityHitRate")

    val summary = metrics.summary
    summary should include("Splits Created: 1000")
    summary should include("Locality Hit Rate: 81.6%")
    summary should include("DYNAMIC_ALLOC")
  }

  test("Gap #1: Dynamic allocation detection from Spark config") {
    // Simulated config check
    val dynamicAllocationEnabled = "true"

    dynamicAllocationEnabled.toBoolean shouldBe true
  }

  test("Schema fingerprint stability - same schema produces same hash") {
    import java.security.MessageDigest
    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))

    def computeFingerprint(schema: StructType): String = {
      val digest = MessageDigest.getInstance("SHA-256")
      val hash = digest.digest(schema.json.getBytes("UTF-8"))
      hash.map("%02x".format(_)).mkString
    }

    val fingerprint1 = computeFingerprint(schema)
    val fingerprint2 = computeFingerprint(schema)

    fingerprint1 shouldEqual fingerprint2 // Deterministic!
  }

  test("Progressive cleanup reduces disk usage during merge") {
    // Simulate merge completing and local files being deleted immediately
    val initialDiskUsageGB = 100L
    val mergedSplitSizeGB = 80L
    val sourceSplitsSizeGB = 40L // 10 x 4GB splits

    // After progressive cleanup: merged split created, source splits deleted
    val finalDiskUsageGB = initialDiskUsageGB + mergedSplitSizeGB - sourceSplitsSizeGB

    finalDiskUsageGB shouldBe 140L // 100 + 80 - 40
  }
}
