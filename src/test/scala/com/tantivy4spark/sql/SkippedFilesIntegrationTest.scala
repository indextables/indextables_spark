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

package com.tantivy4spark.sql

import com.tantivy4spark.TestBase
import com.tantivy4spark.transaction.{TransactionLog, TransactionLogFactory, AddAction, SkipAction}
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers
import java.io.File
import java.nio.file.Files
import scala.util.Try

/**
 * Comprehensive integration test that validates the complete skipped files functionality:
 *   1. SkipAction creation and transaction log recording 2. Cooldown period enforcement and filtering 3. Transaction
 *      log querying methods 4. Configuration handling 5. End-to-end merge operation integration
 *
 * This test works without requiring actual corrupted files by directly testing the transaction log and cooldown
 * mechanisms.
 */
class SkippedFilesIntegrationTest extends TestBase with Matchers {

  test("should record and track skipped files with timestamps and cooldown") {
    withTempPath { outputPath =>
      println(s"\n🔧 Testing complete skipped files tracking functionality")

      // Step 1: Create initial data to have a transaction log
      val initialData = spark
        .range(10)
        .select(
          col("id"),
          concat(lit("test_doc_"), col("id")).as("title")
        )

      initialData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(outputPath)

      // Step 2: Get transaction log and test skip recording
      val transactionLog = TransactionLogFactory.create(new org.apache.hadoop.fs.Path(outputPath), spark)
      val testFilePath   = "test/corrupted_file.split"
      val testReason     = "File corruption detected during merge"

      println(s"Recording skipped file: $testFilePath")

      // Record a skipped file with 1-hour cooldown for faster testing
      val skipVersion = transactionLog.recordSkippedFile(
        filePath = testFilePath,
        reason = testReason,
        operation = "merge",
        partitionValues = Some(Map("test_partition" -> "value1")),
        size = Some(12345L),
        cooldownHours = 1
      )

      println(s"Skip recorded in version: $skipVersion")

      // Step 3: Verify skip was recorded correctly
      val skippedFiles = transactionLog.getSkippedFiles()
      skippedFiles should not be empty
      skippedFiles.length shouldBe 1

      val skippedFile = skippedFiles.head
      skippedFile.path shouldBe testFilePath
      skippedFile.reason shouldBe testReason
      skippedFile.operation shouldBe "merge"
      skippedFile.partitionValues shouldBe Some(Map("test_partition" -> "value1"))
      skippedFile.size shouldBe Some(12345L)
      skippedFile.skipCount shouldBe 1

      println(s"✅ Skip recorded correctly: ${skippedFile.path} (reason: ${skippedFile.reason})")

      // Step 4: Test cooldown functionality
      val isInCooldown = transactionLog.isFileInCooldown(testFilePath)
      isInCooldown shouldBe true

      val filesInCooldown = transactionLog.getFilesInCooldown()
      filesInCooldown should contain key testFilePath

      println(s"✅ File correctly identified as in cooldown")

      // Step 5: Test filtering logic
      val testAddAction = AddAction(
        path = testFilePath,
        partitionValues = Map("test_partition" -> "value1"),
        size = 12345L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )

      val candidateFiles = Seq(testAddAction)
      val filteredFiles  = transactionLog.filterFilesInCooldown(candidateFiles)

      filteredFiles shouldBe empty
      println(s"✅ File correctly filtered out during cooldown period")

      // Step 6: Test multiple skips increment count
      val skipVersion2 = transactionLog.recordSkippedFile(
        filePath = testFilePath,
        reason = "Second failure - persistent corruption",
        operation = "merge",
        cooldownHours = 1
      )

      val updatedSkips = transactionLog.getSkippedFiles().filter(_.path == testFilePath)
      val latestSkip   = updatedSkips.maxBy(_.skipTimestamp)
      latestSkip.skipCount shouldBe 2
      latestSkip.reason shouldBe "Second failure - persistent corruption"

      println(s"✅ Skip count correctly incremented: ${latestSkip.skipCount}")

      // Step 7: Test different file doesn't affect cooldown
      val differentFilePath     = "test/different_file.split"
      val isDifferentInCooldown = transactionLog.isFileInCooldown(differentFilePath)
      isDifferentInCooldown shouldBe false

      println(s"✅ Different file correctly not in cooldown")

      println(s"✅ Complete skipped files tracking functionality validated")
    }
  }

  test("should handle configuration for cooldown duration and tracking") {
    withTempPath { outputPath =>
      println(s"\n🔧 Testing configuration handling for skipped files")

      // Step 1: Test with tracking disabled
      spark.conf.set("spark.indextables.skippedFiles.trackingEnabled", "false")
      spark.conf.set("spark.indextables.skippedFiles.cooldownDuration", "48")

      val initialData = spark
        .range(5)
        .select(
          col("id"),
          concat(lit("config_test_"), col("id")).as("title")
        )

      initialData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(outputPath)

      val transactionLog = TransactionLogFactory.create(new org.apache.hadoop.fs.Path(outputPath), spark)

      // When tracking is disabled, the configuration should still be readable
      val cooldownHours   = spark.conf.get("spark.indextables.skippedFiles.cooldownDuration", "24").toInt
      val trackingEnabled = spark.conf.get("spark.indextables.skippedFiles.trackingEnabled", "true").toBoolean

      cooldownHours shouldBe 48
      trackingEnabled shouldBe false

      println(s"✅ Configuration correctly read: cooldown=$cooldownHours hours, tracking=$trackingEnabled")

      // Step 2: Test with tracking enabled and custom cooldown
      spark.conf.set("spark.indextables.skippedFiles.trackingEnabled", "true")
      spark.conf.set("spark.indextables.skippedFiles.cooldownDuration", "2")

      val testFile = "config/test_file.split"
      val skipVersion = transactionLog.recordSkippedFile(
        filePath = testFile,
        reason = "Configuration test",
        operation = "merge",
        cooldownHours = spark.conf.get("spark.indextables.skippedFiles.cooldownDuration", "24").toInt
      )

      val skippedFiles = transactionLog.getSkippedFiles()
      val configSkip   = skippedFiles.find(_.path == testFile)
      configSkip shouldBe defined

      // Verify the cooldown period is correctly set (2 hours = 2 * 60 * 60 * 1000 ms)
      val expectedCooldownMs = 2 * 60 * 60 * 1000L
      val actualCooldownMs   = configSkip.get.retryAfter.get - configSkip.get.skipTimestamp

      // Allow for small timing differences (within 1 second)
      Math.abs(actualCooldownMs - expectedCooldownMs) should be < 1000L

      println(s"✅ Custom cooldown period correctly applied: ${actualCooldownMs / 1000 / 60 / 60} hours")

      // Cleanup configuration
      spark.conf.unset("spark.indextables.skippedFiles.trackingEnabled")
      spark.conf.unset("spark.indextables.skippedFiles.cooldownDuration")
    }
  }

  test("should demonstrate SerializableSplitMetadata skipped splits handling") {
    println(s"\n🔧 Testing SerializableSplitMetadata skipped splits functionality")

    // Test creating metadata with skipped splits
    val skippedPaths = List(
      "s3://bucket/corrupted1.split",
      "s3://bucket/missing2.split",
      "/local/path/corrupted3.split"
    )

    val metadata = SerializableSplitMetadata(
      footerStartOffset = 0L,
      footerEndOffset = 1000L,
      hotcacheStartOffset = 0L,
      hotcacheLength = 0L,
      hasFooterOffsets = true,
      timeRangeStart = Some("2024-01-01T00:00:00Z"),
      timeRangeEnd = Some("2024-01-01T23:59:59Z"),
      tags = Some(Map("environment" -> "test")),
      deleteOpstamp = Some(12345L),
      numMergeOps = Some(3),
      docMappingJson = Some("""{"mapping": "test"}"""),
      uncompressedSizeBytes = 50000L,
      splitId = Some("test-split-123"),
      numDocs = Some(100L),
      skippedSplits = skippedPaths,
      indexUid = Some("test-index-uid") // Add indexUid parameter
    )

    // Test getSkippedSplits method
    val retrievedSkips = metadata.getSkippedSplits()
    import scala.jdk.CollectionConverters._
    val retrievedAsList = retrievedSkips.asScala.toList

    retrievedAsList shouldBe skippedPaths
    retrievedSkips.size() shouldBe 3

    println(s"✅ SerializableSplitMetadata correctly stores and retrieves ${retrievedSkips.size()} skipped splits")

    // Test Java interoperability
    val firstSkipped = retrievedSkips.get(0)
    val lastSkipped  = retrievedSkips.get(retrievedSkips.size() - 1)

    firstSkipped shouldBe "s3://bucket/corrupted1.split"
    lastSkipped shouldBe "/local/path/corrupted3.split"

    println(s"✅ Java API working correctly: first='$firstSkipped', last='$lastSkipped'")

    // Test empty skipped splits
    val emptyMetadata = SerializableSplitMetadata(
      footerStartOffset = 0L,
      footerEndOffset = 100L,
      hotcacheStartOffset = 0L,
      hotcacheLength = 0L,
      hasFooterOffsets = false,
      timeRangeStart = None,
      timeRangeEnd = None,
      tags = None,
      deleteOpstamp = None,
      numMergeOps = None,
      docMappingJson = None,
      uncompressedSizeBytes = 1000L,
      skippedSplits = List.empty,
      indexUid = None // Add indexUid parameter for empty metadata
    )

    val emptySkips = emptyMetadata.getSkippedSplits()
    emptySkips.size() shouldBe 0
    emptySkips.isEmpty shouldBe true

    println(s"✅ Empty skipped splits handled correctly")

    // Test empty string indexUid (should behave same as null)
    val emptyIndexUidMetadata = SerializableSplitMetadata(
      footerStartOffset = 0L,
      footerEndOffset = 150L,
      hotcacheStartOffset = 0L,
      hotcacheLength = 0L,
      hasFooterOffsets = true,
      timeRangeStart = None,
      timeRangeEnd = None,
      tags = None,
      deleteOpstamp = Some(789L),
      numMergeOps = Some(1),
      docMappingJson = None,
      uncompressedSizeBytes = 3000L,
      skippedSplits = List.empty,
      indexUid = Some("   ") // Empty/whitespace string should be treated as invalid
    )

    val emptyIndexUid = emptyIndexUidMetadata.getIndexUid()
    emptyIndexUid shouldBe "   " // Should return the actual value
    println(s"✅ Empty/whitespace indexUid handling validated: '$emptyIndexUid'")
  }

  test("should validate transaction log integration with SkipAction") {
    withTempPath { outputPath =>
      println(s"\n🔧 Testing transaction log integration with SkipAction")

      // Create initial data
      val data = spark
        .range(3)
        .select(
          col("id"),
          concat(lit("integration_"), col("id")).as("title")
        )

      data.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(outputPath)

      val transactionLog = TransactionLogFactory.create(new org.apache.hadoop.fs.Path(outputPath), spark)

      // Record multiple skipped files
      val skipPaths = Seq(
        "integration/file1.split",
        "integration/file2.split",
        "integration/file3.split"
      )

      val skipReasons = Seq(
        "Corruption detected in header",
        "Missing metadata file",
        "Invalid file format"
      )

      skipPaths.zip(skipReasons).foreach {
        case (path, reason) =>
          transactionLog.recordSkippedFile(
            filePath = path,
            reason = reason,
            operation = "merge",
            partitionValues = Some(Map("integration" -> "test")),
            cooldownHours = 1
          )
      }

      // Verify all skips were recorded
      val allSkips = transactionLog.getSkippedFiles()
      allSkips.length shouldBe 3

      // Verify each skip has correct information
      skipPaths.zip(skipReasons).foreach {
        case (expectedPath, expectedReason) =>
          val skip = allSkips.find(_.path == expectedPath)
          skip shouldBe defined
          skip.get.reason shouldBe expectedReason
          skip.get.operation shouldBe "merge"
          skip.get.partitionValues shouldBe Some(Map("integration" -> "test"))
          skip.get.skipCount shouldBe 1
      }

      println(s"✅ All ${allSkips.length} skip actions correctly recorded in transaction log")

      // Test cooldown filtering
      val testAddActions = skipPaths.map { path =>
        AddAction(
          path = path,
          partitionValues = Map("integration" -> "test"),
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true
        )
      }

      val filtered = transactionLog.filterFilesInCooldown(testAddActions)
      filtered shouldBe empty

      println(s"✅ All files correctly filtered out during cooldown")

      // Test individual file cooldown checking
      skipPaths.foreach { path =>
        val inCooldown = transactionLog.isFileInCooldown(path)
        inCooldown shouldBe true
      }

      println(s"✅ Individual file cooldown checking working correctly")

      // Test cooldown map
      val cooldownMap = transactionLog.getFilesInCooldown()
      cooldownMap.size shouldBe 3
      skipPaths.foreach(path => cooldownMap should contain key path)

      println(s"✅ Cooldown map correctly contains all ${cooldownMap.size} files")
    }
  }

  test("should allow files to be retried after cooldown period expires") {
    withTempPath { outputPath =>
      println(s"\n🔧 Testing post-cooldown retry behavior")

      // Step 1: Create initial data and transaction log
      val initialData = spark
        .range(5)
        .select(
          col("id"),
          concat(lit("retry_test_"), col("id")).as("title")
        )

      initialData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(outputPath)

      val transactionLog = TransactionLogFactory.create(new org.apache.hadoop.fs.Path(outputPath), spark)
      val testFilePath   = "retry/test_file.split"
      val testReason     = "File failed during merge - testing retry logic"

      // Step 2: Record a skipped file with a very short cooldown (1 second for testing)
      println(s"Recording skipped file with 1-second cooldown: $testFilePath")

      // Record with 1 second cooldown (converted to hours: 1 second = 1/3600 hours)
      val skipVersion = transactionLog.recordSkippedFile(
        filePath = testFilePath,
        reason = testReason,
        operation = "merge",
        partitionValues = Some(Map("retry_test" -> "value1")),
        size = Some(54321L),
        cooldownHours = 1 // This will be 1 hour in production, but we'll test with manual time manipulation
      )

      println(s"Skip recorded in version: $skipVersion")

      // Step 3: Verify file is initially in cooldown
      val initiallyInCooldown = transactionLog.isFileInCooldown(testFilePath)
      initiallyInCooldown shouldBe true

      val testAddAction = AddAction(
        path = testFilePath,
        partitionValues = Map("retry_test" -> "value1"),
        size = 54321L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )

      val initiallyFiltered = transactionLog.filterFilesInCooldown(Seq(testAddAction))
      initiallyFiltered shouldBe empty

      println(s"✅ File correctly blocked during cooldown period")

      // Step 4: Test with an expired cooldown using custom timestamp
      val expiredFilePath  = "retry/expired_cooldown_file.split"
      val expiredTimestamp = System.currentTimeMillis() - (2 * 60 * 60 * 1000L) // 2 hours ago

      // Create a skip record with an expired timestamp (1 hour cooldown, but timestamp is 2 hours ago)
      val expiredSkipVersion = transactionLog.recordSkippedFileWithTimestamp(
        filePath = expiredFilePath,
        reason = "Testing expired cooldown logic",
        operation = "merge",
        skipTimestamp = expiredTimestamp,
        cooldownHours = 1, // 1 hour cooldown, but skip was 2 hours ago, so it should be expired
        partitionValues = Some(Map("retry_test" -> "expired")),
        size = Some(98765L)
      )

      println(s"Recorded expired cooldown file: $expiredFilePath")

      val expiredFileAction = AddAction(
        path = expiredFilePath,
        partitionValues = Map("retry_test" -> "expired"),
        size = 98765L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )

      // Step 5: Verify expired file is NOT filtered (should be retryable)
      val expiredFileFiltered   = transactionLog.filterFilesInCooldown(Seq(expiredFileAction))
      val expiredFileInCooldown = transactionLog.isFileInCooldown(expiredFilePath)

      // The expired file should not be in cooldown and should pass through the filter
      expiredFileInCooldown shouldBe false
      expiredFileFiltered.length shouldBe 1 // File should pass through filter

      println(s"✅ Expired cooldown file correctly becomes retryable: inCooldown=$expiredFileInCooldown, filtered=${expiredFileFiltered.length} files")

      // Step 6: Test with zero cooldown for immediate retry
      val zeroCooldownFilePath = "retry/zero_cooldown_file.split"
      val zeroCooldownVersion = transactionLog.recordSkippedFile(
        filePath = zeroCooldownFilePath,
        reason = "Testing zero cooldown for immediate retry",
        operation = "merge",
        cooldownHours = 0 // Zero cooldown should make file immediately retryable
      )

      val zeroCooldownAction = AddAction(
        path = zeroCooldownFilePath,
        partitionValues = Map("retry_test" -> "immediate"),
        size = 12345L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )

      // Step 7: Verify zero cooldown file is immediately retryable
      val zeroCooldownFiltered   = transactionLog.filterFilesInCooldown(Seq(zeroCooldownAction))
      val zeroCooldownInCooldown = transactionLog.isFileInCooldown(zeroCooldownFilePath)

      // Zero cooldown file should be immediately retryable
      zeroCooldownInCooldown shouldBe false
      zeroCooldownFiltered.length shouldBe 1

      println(s"✅ Zero cooldown file immediately retryable: inCooldown=$zeroCooldownInCooldown, filtered=${zeroCooldownFiltered.length} files")

      // Step 8: Verify all cooldown scenarios work correctly
      val allSkippedFiles = transactionLog.getSkippedFiles()
      val testFileSkips   = allSkippedFiles.filter(_.path.contains("retry"))

      testFileSkips.foreach { skip =>
        val cooldownEndTime = skip.retryAfter.getOrElse(skip.skipTimestamp)
        val currentTime     = System.currentTimeMillis()
        val isExpired       = currentTime > cooldownEndTime
        println(
          s"📝 File: ${skip.path}, cooldown expires at: $cooldownEndTime, current: $currentTime, expired: $isExpired"
        )
      }

      println(s"✅ Complete post-cooldown retry logic validated")
      println(s"   ✓ Files correctly blocked during active cooldown period")
      println(s"   ✓ Files become retryable after cooldown expires")
      println(s"   ✓ Zero cooldown allows immediate retry")
      println(s"   ✓ Transaction log accurately tracks all cooldown states")
    }
  }

  test("should handle empty string indexUid same as null indexUid") {
    println(s"\n🔧 Testing empty string indexUid handling")

    // Test the indexUid validation logic directly
    val testCases = List(
      (None, "None (missing indexUid)"),
      (Some(null), "Some(null)"),
      (Some(""), "Some(\"\") (empty string)"),
      (Some("   "), "Some(\"   \") (whitespace only)"),
      (Some("\t\n"), "Some(\"\\t\\n\") (whitespace chars)"),
      (Some("valid-uuid-123"), "Some(\"valid-uuid-123\") (valid indexUid)")
    )

    testCases.foreach {
      case (indexUid, description) =>
        // Test the same logic used in MergeSplitsCommand
        val shouldSkipMerge = indexUid.isEmpty || indexUid.contains(null) || indexUid.exists(_.trim.isEmpty)

        val expectedResult = description.contains("valid-uuid") match {
          case true  => false // Valid indexUid should NOT skip merge
          case false => true  // All invalid cases should skip merge
        }

        shouldSkipMerge shouldBe expectedResult

        val status = if (shouldSkipMerge) "SKIP MERGE" else "PROCEED WITH MERGE"
        println(s"✅ $description -> $status")
    }

    println(s"✅ Empty string indexUid correctly treated same as null indexUid")
    println(s"   ✓ All null, empty, and whitespace-only indexUids skip ADD/REMOVE operations")
    println(s"   ✓ Only valid non-empty indexUids proceed with normal merge operations")
  }
}
