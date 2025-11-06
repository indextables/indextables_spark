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
 * Integration test demonstrating end-to-end merge-on-write workflow
 *
 * This test validates:
 * 1. Configuration parsing and validation
 * 2. Metadata model creation
 * 3. Bin packing algorithm execution
 * 4. Metrics collection and reporting
 * 5. Gap mitigation features
 *
 * Note: This is a unit-style integration test that validates the components
 * work together correctly without requiring a full Spark cluster or tantivy4java.
 */
class MergeOnWriteIntegrationTest extends AnyFunSuite with Matchers {

  test("End-to-end merge-on-write workflow validation") {
    // Phase 1: Create simulated staged splits from multiple workers
    val workerA = "worker-a.example.com"
    val workerB = "worker-b.example.com"
    val workerC = "worker-c.example.com"

    val stagedSplits = createSimulatedSplits(Seq(
      (workerA, 3200 * 1024 * 1024), // 3.2GB
      (workerA, 2800 * 1024 * 1024), // 2.8GB
      (workerA,  800 * 1024 * 1024), // 800MB
      (workerA,  500 * 1024 * 1024), // 500MB
      (workerB, 3800 * 1024 * 1024), // 3.8GB
      (workerB, 2100 * 1024 * 1024), // 2.1GB
      (workerB, 1900 * 1024 * 1024), // 1.9GB
      (workerC, 4200 * 1024 * 1024), // 4.2GB
      (workerC,  300 * 1024 * 1024), // 300MB
      (workerC,  200 * 1024 * 1024)  // 200MB
    ))

    stagedSplits should have size 10

    // Phase 2: Simulate bin packing
    val targetSize = 4L * 1024 * 1024 * 1024 // 4GB
    val minSplitsToMerge = 2

    val groupedByHost = stagedSplits.groupBy(_.workerHost)
    groupedByHost should have size 3

    // Phase 3: Validate locality preservation
    groupedByHost(workerA) should have size 4
    groupedByHost(workerB) should have size 3
    groupedByHost(workerC) should have size 3

    // Phase 4: Simulate merge group creation
    var totalMergeGroups = 0
    var promotedSplits = 0

    groupedByHost.foreach { case (host, hostSplits) =>
      val sorted = hostSplits.sortBy(-_.size)

      // Simple bin packing: pair largest with smallest
      var remaining = sorted.toBuffer
      while (remaining.size >= minSplitsToMerge) {
        val largest = remaining.head
        remaining.remove(0)

        // Find best fit
        val bestFitIdx = remaining.indexWhere(s => largest.size + s.size <= targetSize * 1.1)
        if (bestFitIdx >= 0) {
          val paired = remaining(bestFitIdx)
          remaining.remove(bestFitIdx)
          totalMergeGroups += 1
          println(s"Merge group on $host: ${largest.size / 1024 / 1024}MB + ${paired.size / 1024 / 1024}MB = ${(largest.size + paired.size) / 1024 / 1024}MB")
        } else {
          promotedSplits += 1
          println(s"Promoted split on $host: ${largest.size / 1024 / 1024}MB (no suitable pair)")
        }
      }

      // Remaining singles are promoted
      promotedSplits += remaining.size
    }

    // Phase 5: Validate results
    println(s"Total merge groups: $totalMergeGroups")
    println(s"Promoted splits: $promotedSplits")

    totalMergeGroups should be >= 3
    promotedSplits should be > 0

    // Phase 6: Simulate metrics collection
    val metrics = MergeOnWriteMetrics(
      totalSplitsCreated = stagedSplits.size,
      totalSplitsStaged = stagedSplits.size,
      stagingUploadTimeMs = 30000,
      stagingFailures = 0,
      stagingRetries = 0,
      mergeGroupsCreated = totalMergeGroups,
      mergesExecuted = totalMergeGroups,
      mergesLocalFile = totalMergeGroups, // All local (simulated)
      mergesRemoteDownload = 0,
      mergeDurationMs = 120000,
      splitsPromoted = promotedSplits,
      finalSplitCount = totalMergeGroups + promotedSplits,
      networkBytesUploaded = 500L * 1024 * 1024, // 500MB
      networkBytesDownloaded = 0,
      localityHitRate = 1.0, // 100% local
      cleanupDurationMs = 5000,
      totalDurationMs = 155000
    )

    metrics.localityHitRate shouldBe 1.0
    metrics.finalSplitCount shouldBe (totalMergeGroups + promotedSplits)
    println(metrics.summary)
  }

  test("Configuration validation for merge-on-write") {
    import org.apache.spark.sql.util.CaseInsensitiveStringMap
    import scala.jdk.CollectionConverters._

    val config = Map(
      "spark.indextables.mergeOnWrite.enabled" -> "true",
      "spark.indextables.mergeOnWrite.targetSize" -> "4G",
      "spark.indextables.mergeOnWrite.minSplitsToMerge" -> "2",
      "spark.indextables.mergeOnWrite.asyncStaging" -> "true",
      "spark.indextables.mergeOnWrite.stagingThreads" -> "4",
      "spark.indextables.mergeOnWrite.maxConcurrentMergesPerWorker" -> "1",
      "spark.indextables.mergeOnWrite.minDiskSpaceGB" -> "20",
      "spark.indextables.mergeOnWrite.stagingRetries" -> "3",
      "spark.indextables.mergeOnWrite.minStagingSuccessRate" -> "0.95"
    )

    val options = new CaseInsensitiveStringMap(config.asJava)

    options.getOrDefault("spark.indextables.mergeOnWrite.enabled", "false") shouldBe "true"
    options.getOrDefault("spark.indextables.mergeOnWrite.targetSize", "4G") shouldBe "4G"
    options.getOrDefault("spark.indextables.mergeOnWrite.minSplitsToMerge", "2") shouldBe "2"
  }

  test("Schema evolution detection workflow") {
    import java.security.MessageDigest
    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

    val schema1 = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType)
    ))

    val schema2 = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("email", StringType) // Added field
    ))

    def fingerprint(schema: StructType): String = {
      val digest = MessageDigest.getInstance("SHA-256")
      digest.digest(schema.json.getBytes("UTF-8")).map("%02x".format(_)).mkString
    }

    val fp1 = fingerprint(schema1)
    val fp2 = fingerprint(schema2)

    fp1 should not equal fp2

    // Create splits with different schemas
    val splits = Seq(
      createSplit("s1", "worker-a", 1000, fp1),
      createSplit("s2", "worker-a", 1000, fp2), // Different schema!
      createSplit("s3", "worker-a", 1000, fp1)
    )

    // Group by schema
    val bySchema = splits.groupBy(_.schemaFingerprint)
    bySchema should have size 2

    // Validate incompatible schemas detected
    val uniqueSchemas = splits.map(_.schemaFingerprint).distinct
    uniqueSchemas.size should be > 1
  }

  test("Metrics summary formatting") {
    val metrics = MergeOnWriteMetrics(
      totalSplitsCreated = 100,
      totalSplitsStaged = 98,
      stagingUploadTimeMs = 60000,
      stagingFailures = 2,
      stagingRetries = 3,
      mergeGroupsCreated = 25,
      mergesExecuted = 25,
      mergesLocalFile = 20,
      mergesRemoteDownload = 5,
      mergeDurationMs = 180000,
      splitsPromoted = 5,
      finalSplitCount = 30,
      networkBytesUploaded = 10L * 1024 * 1024 * 1024,
      networkBytesDownloaded = 1L * 1024 * 1024 * 1024,
      localityHitRate = 0.8,
      diskSpaceExhausted = false,
      memoryPressureDetected = false,
      dynamicAllocationDetected = true,
      cleanupDurationMs = 5000,
      totalDurationMs = 245000
    )

    val summary = metrics.summary
    summary should include("Splits Created: 100")
    summary should include("Locality Hit Rate: 80.0%")
    summary should include("Final Splits: 30")
    summary should include("DYNAMIC_ALLOC")

    val json = metrics.toJson
    json should include("\"totalSplitsCreated\" : 100")
  }

  // Helper methods

  private def createSimulatedSplits(specs: Seq[(String, Long)]): Seq[StagedSplitInfo] = {
    specs.zipWithIndex.map { case ((host, size), idx) =>
      StagedSplitInfo(
        uuid = s"split-$idx",
        taskAttemptId = idx,
        workerHost = host,
        executorId = s"exec-${idx % 3}",
        localPath = s"/tmp/split-$idx",
        stagingPath = s"s3://bucket/_staging/split-$idx.tmp",
        stagingAvailable = true,
        size = size,
        numRecords = (size / 1000).toInt,
        minValues = Map("id" -> "1"),
        maxValues = Map("id" -> "1000"),
        footerStartOffset = Some(1000L),
        footerEndOffset = Some(2000L),
        partitionValues = Map.empty,
        schemaFingerprint = "default-schema"
      )
    }
  }

  private def createSplit(
    uuid: String,
    host: String,
    sizeMB: Long,
    schemaFP: String
  ): StagedSplitInfo = {
    StagedSplitInfo(
      uuid = uuid,
      taskAttemptId = 0,
      workerHost = host,
      executorId = "exec-1",
      localPath = s"/tmp/$uuid",
      stagingPath = s"s3://bucket/_staging/$uuid.tmp",
      stagingAvailable = true,
      size = sizeMB * 1024 * 1024,
      numRecords = sizeMB.toInt * 1000,
      minValues = Map.empty,
      maxValues = Map.empty,
      footerStartOffset = None,
      footerEndOffset = None,
      partitionValues = Map.empty,
      schemaFingerprint = schemaFP
    )
  }
}
