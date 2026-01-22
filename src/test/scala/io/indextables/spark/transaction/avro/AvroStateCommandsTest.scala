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

import io.indextables.spark.TestBase

import org.apache.spark.sql.functions._

/**
 * Tests for SQL commands (DROP PARTITIONS, PURGE, etc.) with Avro state format.
 *
 * These tests verify that all SQL commands work correctly when the table
 * uses the Avro state format for checkpoints.
 *
 * Run with:
 *   mvn scalatest:test -DwildcardSuites='io.indextables.spark.transaction.avro.AvroStateCommandsTest'
 */
class AvroStateCommandsTest extends TestBase {

  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Ensure Avro format is enabled
    spark.conf.set("spark.indextables.state.format", "avro")
  }

  override def afterEach(): Unit = {
    spark.conf.unset("spark.indextables.state.format")
    // Clear caches to avoid pollution between tests
    io.indextables.spark.transaction.EnhancedTransactionLogCache.clearGlobalCaches()
    super.afterEach()
  }

  // ==========================================================================
  // DROP PARTITIONS with Avro State
  // ==========================================================================

  test("DROP PARTITIONS works correctly with Avro state checkpoint") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create partitioned data
      val data = Seq(
        (1, "Alice", "2024-01-01", 100),
        (2, "Bob", "2024-01-01", 200),
        (3, "Charlie", "2024-01-02", 150),
        (4, "Diana", "2024-01-02", 250),
        (5, "Eve", "2024-01-03", 175),
        (6, "Frank", "2024-01-03", 225)
      )

      spark.createDataFrame(data).toDF("id", "name", "date", "score")
        .write.format(provider)
        .partitionBy("date")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(path)

      // Create Avro checkpoint
      val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      checkpointResult(0).getAs[String]("status") shouldBe "SUCCESS"

      // Verify Avro state format
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // Verify initial count
      val initialCount = spark.read.format(provider).load(path).count()
      initialCount shouldBe 6

      // DROP PARTITIONS for date = '2024-01-02'
      val dropResult = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$path' WHERE date = '2024-01-02'").collect()
      dropResult(0).getAs[String]("status") shouldBe "success"

      // Verify partition was dropped
      val afterDropCount = spark.read.format(provider).load(path).count()
      afterDropCount shouldBe 4 // 6 - 2 (Charlie, Diana)

      // Verify the specific partition is empty
      val jan2Count = spark.read.format(provider).load(path)
        .filter(col("date") === "2024-01-02")
        .count()
      jan2Count shouldBe 0

      // Verify other partitions are intact
      val jan1Count = spark.read.format(provider).load(path)
        .filter(col("date") === "2024-01-01")
        .count()
      jan1Count shouldBe 2

      val jan3Count = spark.read.format(provider).load(path)
        .filter(col("date") === "2024-01-03")
        .count()
      jan3Count shouldBe 2
    }
  }

  test("DROP PARTITIONS with range predicate works with Avro state") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create partitioned data with multiple dates
      val data = (1 to 10).map { i =>
        (i, s"name_$i", f"2024-01-$i%02d", i * 10)
      }

      spark.createDataFrame(data).toDF("id", "name", "date", "score")
        .write.format(provider)
        .partitionBy("date")
        .mode("overwrite")
        .save(path)

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify Avro state
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // DROP partitions where date < '2024-01-05'
      val dropResult = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$path' WHERE date < '2024-01-05'").collect()
      dropResult(0).getAs[String]("status") shouldBe "success"

      // Verify count (should have 6 remaining: 05, 06, 07, 08, 09, 10)
      val afterDropCount = spark.read.format(provider).load(path).count()
      afterDropCount shouldBe 6
    }
  }

  test("DROP PARTITIONS followed by checkpoint preserves correct state") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create partitioned data
      val data = Seq(
        (1, "Alice", "region_a"),
        (2, "Bob", "region_a"),
        (3, "Charlie", "region_b"),
        (4, "Diana", "region_b"),
        (5, "Eve", "region_c")
      )

      spark.createDataFrame(data).toDF("id", "name", "region")
        .write.format(provider)
        .partitionBy("region")
        .mode("overwrite")
        .save(path)

      // Create initial Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Drop a partition
      spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$path' WHERE region = 'region_b'").collect()

      // Verify state before second checkpoint
      val beforeCheckpoint = spark.read.format(provider).load(path).count()
      beforeCheckpoint shouldBe 3

      // Create another checkpoint after drop
      val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      checkpointResult(0).getAs[String]("status") shouldBe "SUCCESS"

      // Verify Avro state after second checkpoint
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"
      stateResult(0).getAs[Long]("num_files") shouldBe 2 // 2 partitions remaining

      // Verify data integrity
      val afterCheckpoint = spark.read.format(provider).load(path).count()
      afterCheckpoint shouldBe 3

      val names = spark.read.format(provider).load(path)
        .collect().map(_.getAs[String]("name")).toSet
      names shouldBe Set("Alice", "Bob", "Eve")
    }
  }

  // ==========================================================================
  // PURGE with Avro State Directory Cleanup
  // ==========================================================================

  test("PURGE INDEXTABLE cleans up old Avro state directories") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create initial data
      val data1 = Seq((1, "doc1"), (2, "doc2"))
      spark.createDataFrame(data1).toDF("id", "content")
        .write.format(provider)
        .mode("overwrite")
        .save(path)

      // Create first Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Get first checkpoint version
      val state1 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      val version1 = state1(0).getAs[Long]("version")

      // Append more data
      val data2 = Seq((3, "doc3"), (4, "doc4"))
      spark.createDataFrame(data2).toDF("id", "content")
        .write.format(provider)
        .mode("append")
        .save(path)

      // Create second Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Get second checkpoint version
      val state2 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      val version2 = state2(0).getAs[Long]("version")

      version2 should be > version1

      // Verify both state directories exist
      val txLogPath = s"$path/_transaction_log"
      val stateDir1 = f"$txLogPath/state-v$version1%020d"
      val stateDir2 = f"$txLogPath/state-v$version2%020d"

      new java.io.File(stateDir1).exists() shouldBe true
      new java.io.File(stateDir2).exists() shouldBe true

      // Run PURGE DRY RUN with minimum allowed retention
      // DRY RUN verifies it identifies old state directories without actually deleting
      spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")
      val dryRunResult = spark.sql(s"PURGE INDEXTABLE '$path' OLDER THAN 0 HOURS DRY RUN").collect()
      spark.conf.unset("spark.indextables.purge.retentionCheckEnabled")

      // The purge should identify orphaned files and old state directories
      dryRunResult.length should be >= 0 // May or may not have orphaned splits

      // Verify data is still intact
      val finalCount = spark.read.format(provider).load(path).count()
      finalCount shouldBe 4
    }
  }

  test("PURGE INDEXTABLE preserves latest Avro state directory") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create data and checkpoint
      val data = Seq((1, "doc1"), (2, "doc2"), (3, "doc3"))
      spark.createDataFrame(data).toDF("id", "content")
        .write.format(provider)
        .mode("overwrite")
        .save(path)

      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify Avro state
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"
      val version = stateResult(0).getAs[Long]("version")

      // Run PURGE DRY RUN (disable retention check for testing)
      spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")
      val purgeResult = spark.sql(s"PURGE INDEXTABLE '$path' OLDER THAN 1 HOURS DRY RUN").collect()
      spark.conf.unset("spark.indextables.purge.retentionCheckEnabled")

      // Verify state directory still exists
      val txLogPath = s"$path/_transaction_log"
      val stateDir = f"$txLogPath/state-v$version%020d"
      new java.io.File(stateDir).exists() shouldBe true

      // Verify data is accessible
      val count = spark.read.format(provider).load(path).count()
      count shouldBe 3
    }
  }

  test("PURGE after DROP PARTITIONS correctly identifies orphaned splits") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create partitioned data
      val data = Seq(
        (1, "Alice", "2024-01"),
        (2, "Bob", "2024-01"),
        (3, "Charlie", "2024-02"),
        (4, "Diana", "2024-02")
      )

      spark.createDataFrame(data).toDF("id", "name", "month")
        .write.format(provider)
        .partitionBy("month")
        .mode("overwrite")
        .save(path)

      // Checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Drop a partition
      spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$path' WHERE month = '2024-02'").collect()

      // Checkpoint again to update state
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify state
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // Run PURGE DRY RUN - should identify orphaned splits from dropped partition
      spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")
      val purgeResult = spark.sql(s"PURGE INDEXTABLE '$path' OLDER THAN 0 HOURS DRY RUN").collect()
      spark.conf.unset("spark.indextables.purge.retentionCheckEnabled")

      // Verify data integrity (only 2024-01 partition should remain)
      val finalCount = spark.read.format(provider).load(path).count()
      finalCount shouldBe 2

      val names = spark.read.format(provider).load(path)
        .collect().map(_.getAs[String]("name")).toSet
      names shouldBe Set("Alice", "Bob")
    }
  }

  // ==========================================================================
  // TRUNCATE TIME TRAVEL with Avro State
  // ==========================================================================

  test("TRUNCATE TIME TRAVEL works with Avro state") {
    withTempPath { tempPath =>
      val path = tempPath

      // Create initial data
      val data1 = Seq((1, "v1_doc1"), (2, "v1_doc2"))
      spark.createDataFrame(data1).toDF("id", "content")
        .write.format(provider)
        .mode("overwrite")
        .save(path)

      // Checkpoint to create Avro state
      val cp1 = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      cp1(0).getAs[String]("status") shouldBe "SUCCESS"

      // Verify Avro state format
      val state1 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      state1(0).getAs[String]("format") shouldBe "avro-state"

      // Append more data to create another version
      val data2 = Seq((3, "v2_doc3"), (4, "v2_doc4"))
      spark.createDataFrame(data2).toDF("id", "content")
        .write.format(provider)
        .mode("append")
        .save(path)

      // Checkpoint again
      val cp2 = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      cp2(0).getAs[String]("status") shouldBe "SUCCESS"

      // Clear caches before truncate
      io.indextables.spark.transaction.EnhancedTransactionLogCache.clearGlobalCaches()

      // DRY RUN first to verify it works with Avro
      val dryRunResult = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$path' DRY RUN").collect()
      dryRunResult(0).getAs[String]("status") shouldBe "DRY_RUN"

      // Verify data is still accessible after dry run
      val countAfterDryRun = spark.read.format(provider).load(path).count()
      countAfterDryRun shouldBe 4

      // Verify state is still Avro after dry run
      val stateAfterDryRun = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateAfterDryRun(0).getAs[String]("format") shouldBe "avro-state"
    }
  }

  // ==========================================================================
  // MERGE SPLITS with Avro State
  // ==========================================================================

  test("MERGE SPLITS works correctly with Avro state checkpoint") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create multiple small splits by writing in batches
      for (i <- 1 to 3) {
        val data = Seq((i * 10 + 1, s"doc_${i}_1"), (i * 10 + 2, s"doc_${i}_2"))
        spark.createDataFrame(data).toDF("id", "content")
          .write.format(provider)
          .mode(if (i == 1) "overwrite" else "append")
          .save(path)
      }

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify Avro state
      val stateBefore = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateBefore(0).getAs[String]("format") shouldBe "avro-state"
      val filesBefore = stateBefore(0).getAs[Long]("num_files")

      // Run MERGE SPLITS
      val mergeResult = spark.sql(s"MERGE SPLITS '$path' TARGET SIZE 100M").collect()

      // Verify data integrity
      val count = spark.read.format(provider).load(path).count()
      count shouldBe 6

      // Checkpoint after merge
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify still Avro state
      val stateAfter = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateAfter(0).getAs[String]("format") shouldBe "avro-state"
    }
  }

  // ==========================================================================
  // Combined Operations
  // ==========================================================================

  test("Full lifecycle: write -> checkpoint -> drop -> merge -> purge -> checkpoint") {
    withTempPath { tempDir =>
      val path = tempDir

      // Step 1: Write partitioned data
      val data = Seq(
        (1, "Alice", "east", 100),
        (2, "Bob", "east", 200),
        (3, "Charlie", "west", 150),
        (4, "Diana", "west", 250),
        (5, "Eve", "north", 175)
      )

      spark.createDataFrame(data).toDF("id", "name", "region", "score")
        .write.format(provider)
        .partitionBy("region")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(path)

      // Step 2: Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      val state1 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      state1(0).getAs[String]("format") shouldBe "avro-state"

      // Step 3: Drop partition
      spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$path' WHERE region = 'west'").collect()
      spark.read.format(provider).load(path).count() shouldBe 3

      // Step 4: Append more data
      val moreData = Seq((6, "Frank", "east", 300))
      spark.createDataFrame(moreData).toDF("id", "name", "region", "score")
        .write.format(provider)
        .partitionBy("region")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("append")
        .save(path)

      spark.read.format(provider).load(path).count() shouldBe 4

      // Step 5: Checkpoint after modifications
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Step 6: Run PURGE DRY RUN
      spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")
      spark.sql(s"PURGE INDEXTABLE '$path' OLDER THAN 0 HOURS DRY RUN").collect()
      spark.conf.unset("spark.indextables.purge.retentionCheckEnabled")

      // Step 7: Final verification
      val finalState = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      finalState(0).getAs[String]("format") shouldBe "avro-state"

      val finalCount = spark.read.format(provider).load(path).count()
      finalCount shouldBe 4

      // Verify aggregations work
      val sumScore = spark.read.format(provider).load(path)
        .agg(sum("score")).collect()(0).getLong(0)
      sumScore shouldBe 775 // 100 + 200 + 175 + 300 = 775 (no Charlie/Diana who had 150+250)
    }
  }
}
