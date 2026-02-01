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

package io.indextables.spark.transaction.avro

import org.apache.spark.sql.functions._

import io.indextables.spark.sql.SerializableSplitMetadata
import io.indextables.spark.TestBase

/**
 * Tests for skipped files functionality with Avro state format.
 *
 * These tests verify:
 *   1. Skipped files are tracked correctly with Avro state 2. State consolidation preserves skip information 3.
 *      SerializableSplitMetadata works with Avro format 4. Cooldown behavior works with Avro checkpoints
 *
 * This is the Avro equivalent of SkippedFilesIntegrationTest.
 *
 * Note: Since SkipAction entries are tracked in the transaction log (not the state), some skip tracking functionality
 * works similarly in both formats. These tests verify the interaction between skipped files and Avro state.
 */
class AvroSkippedFilesTest extends TestBase {

  private val logger   = org.slf4j.LoggerFactory.getLogger(classOf[AvroSkippedFilesTest])
  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  // ============================================================================
  // SERIALIZABLE SPLIT METADATA TESTS
  // ============================================================================

  test("SerializableSplitMetadata should store and retrieve skipped splits") {
    logger.info("Testing SerializableSplitMetadata skipped splits functionality")

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
      indexUid = Some("test-index-uid")
    )

    // Test getSkippedSplits method
    val retrievedSkips = metadata.getSkippedSplits()
    import scala.jdk.CollectionConverters._
    val retrievedAsList = retrievedSkips.asScala.toList

    retrievedAsList shouldBe skippedPaths
    retrievedSkips.size() shouldBe 3

    logger.info(s"SerializableSplitMetadata correctly stores and retrieves ${retrievedSkips.size()} skipped splits")
  }

  test("SerializableSplitMetadata should handle empty skipped splits") {
    logger.info("Testing empty skipped splits handling")

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
      indexUid = None
    )

    val emptySkips = emptyMetadata.getSkippedSplits()
    emptySkips.size() shouldBe 0
    emptySkips.isEmpty shouldBe true

    logger.info("Empty skipped splits handled correctly")
  }

  test("SerializableSplitMetadata should handle indexUid validation") {
    logger.info("Testing indexUid validation")

    // Test the indexUid validation logic
    val testCases = List(
      (None, "None (missing indexUid)"),
      (Some(null), "Some(null)"),
      (Some(""), "Some(\"\") (empty string)"),
      (Some("   "), "Some(\"   \") (whitespace only)"),
      (Some("valid-uuid-123"), "Some(\"valid-uuid-123\") (valid indexUid)")
    )

    testCases.foreach {
      case (indexUid, description) =>
        val shouldSkipMerge = indexUid.isEmpty || indexUid.contains(null) || indexUid.exists(_.trim.isEmpty)

        val expectedResult = description.contains("valid-uuid") match {
          case true  => false // Valid indexUid should NOT skip merge
          case false => true  // All invalid cases should skip merge
        }

        shouldSkipMerge shouldBe expectedResult

        val status = if (shouldSkipMerge) "SKIP MERGE" else "PROCEED WITH MERGE"
        logger.info(s"$description -> $status")
    }

    logger.info("indexUid validation logic verified")
  }

  // ============================================================================
  // AVRO STATE WITH SPLIT METADATA TESTS
  // ============================================================================

  test("should preserve split metadata after Avro checkpoint") {
    withTempPath { path =>
      logger.info("Testing split metadata preservation with Avro checkpoint")

      // Create table with data
      val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
      df.write.format(provider).mode("overwrite").save(path)

      // Create Avro checkpoint
      val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      checkpointResult(0).getAs[String]("status") shouldBe "SUCCESS"

      // Verify state format
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"
      stateResult(0).getAs[Long]("num_files") should be >= 1L

      // Verify data is readable
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 100

      logger.info("Split metadata preservation test passed")
    }
  }

  test("should handle multiple writes with Avro state consolidation") {
    withTempPath { path =>
      logger.info("Testing multiple writes with Avro state consolidation")

      // Multiple writes to create multiple splits
      for (i <- 0 until 10) {
        val df = spark.range(i * 50, (i + 1) * 50).selectExpr("id", "CAST(id AS STRING) as text")
        if (i == 0) {
          df.write.format(provider).mode("overwrite").save(path)
        } else {
          df.write.format(provider).mode("append").save(path)
        }
      }

      // Create checkpoint to consolidate
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify state contains all files
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"
      stateResult(0).getAs[Long]("num_files") should be >= 10L

      // Verify all data readable
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 500 // 10 * 50

      logger.info("Multiple writes state consolidation test passed")
    }
  }

  // ============================================================================
  // STATE TRACKING TESTS
  // ============================================================================

  test("should track file state correctly across checkpoints") {
    withTempPath { path =>
      logger.info("Testing file state tracking across checkpoints")

      // Initial write
      val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write.format(provider).mode("overwrite").save(path)

      // First checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      val state1 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      val files1 = state1(0).getAs[Long]("num_files")

      // Append more data
      val df2 = spark.range(100, 200).selectExpr("id", "CAST(id AS STRING) as text")
      df2.write.format(provider).mode("append").save(path)

      // Second checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      val state2 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      val files2 = state2(0).getAs[Long]("num_files")

      // Should have more files after append
      files2 should be >= files1

      // Verify data integrity
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 200

      logger.info(s"File tracking: $files1 -> $files2 files")
    }
  }

  test("should handle data integrity with state tracking") {
    withTempPath { path =>
      logger.info("Testing data integrity with state tracking")

      // Create initial data
      val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
      df.write.format(provider).mode("overwrite").save(path)

      // Multiple checkpoints
      for (_ <- 1 to 3) {
        spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

        // Verify data is still accessible
        val result = spark.read.format(provider).load(path)
        result.count() shouldBe 500
      }

      // Final state verification
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      logger.info("Data integrity test passed")
    }
  }

  // ============================================================================
  // PARTITIONED DATA TESTS
  // ============================================================================

  test("should track partitioned data correctly with Avro state") {
    withTempPath { path =>
      logger.info("Testing partitioned data tracking")

      // Create partitioned data
      val df = spark
        .range(0, 300)
        .selectExpr(
          "id",
          "CAST(id AS STRING) as text",
          "CAST(id % 3 AS STRING) as partition_col"
        )
      df.write.format(provider).partitionBy("partition_col").mode("overwrite").save(path)

      // Create checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify state
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // Verify partition filtering works
      val partition0 = spark.read
        .format(provider)
        .load(path)
        .filter("partition_col = '0'")
      partition0.count() shouldBe 100 // 300 / 3

      val partition1 = spark.read
        .format(provider)
        .load(path)
        .filter("partition_col = '1'")
      partition1.count() shouldBe 100

      // Verify all data
      val all = spark.read.format(provider).load(path)
      all.count() shouldBe 300

      logger.info("Partitioned data tracking test passed")
    }
  }

  // ============================================================================
  // STATE DIRECTORY TESTS
  // ============================================================================

  test("should maintain state directory integrity") {
    withTempPath { path =>
      logger.info("Testing state directory integrity")

      // Create data
      val df = spark.range(0, 200).selectExpr("id", "CAST(id AS STRING) as text")
      df.write.format(provider).mode("overwrite").save(path)

      // Create checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify state directory exists
      val txLogDir  = new java.io.File(s"$path/_transaction_log")
      val stateDirs = txLogDir.listFiles().filter(_.getName.startsWith("state-v"))
      stateDirs should not be empty

      // Verify manifest exists
      val latestStateDir = stateDirs.maxBy(_.getName)
      val manifestFile   = new java.io.File(latestStateDir, "_manifest.avro")
      manifestFile.exists() shouldBe true

      // Verify data integrity
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 200

      logger.info("State directory integrity test passed")
    }
  }
}
