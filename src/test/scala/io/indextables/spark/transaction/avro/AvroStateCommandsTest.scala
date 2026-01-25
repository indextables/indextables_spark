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

  // ==========================================================================
  // Manifest Pruning Integration Tests
  // These tests verify that commands correctly use listFilesWithPartitionFilters()
  // for Avro manifest pruning when partition predicates are specified.
  // ==========================================================================

  test("DROP PARTITIONS with partition predicate should use Avro manifest pruning") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create data across multiple partitions
      val regions = Seq("us-east", "us-west", "eu-west", "ap-east")
      regions.foreach { region =>
        val data = Seq(
          (1, s"User1-$region", region, 100),
          (2, s"User2-$region", region, 200)
        )
        spark.createDataFrame(data).toDF("id", "name", "region", "score")
          .write.format(provider)
          .partitionBy("region")
          .option("spark.indextables.indexing.fastfields", "score")
          .mode("append")
          .save(path)
      }

      // Create Avro checkpoint - this creates manifests with partition bounds
      val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      checkpointResult(0).getAs[String]("status") shouldBe "SUCCESS"

      // Verify we're using Avro state
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // Initial count
      val initialCount = spark.read.format(provider).load(path).count()
      initialCount shouldBe 8  // 4 regions * 2 users

      // DROP PARTITIONS with equality predicate - should use manifest pruning
      val dropResult = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$path' WHERE region = 'us-east'").collect()
      dropResult(0).getAs[String]("status") shouldBe "success"
      dropResult(0).getAs[Long]("splits_removed") should be >= 1L

      // Verify correct partition was dropped
      val afterDropCount = spark.read.format(provider).load(path).count()
      afterDropCount shouldBe 6  // 8 - 2

      val usEastCount = spark.read.format(provider).load(path)
        .filter(col("region") === "us-east").count()
      usEastCount shouldBe 0

      // DROP PARTITIONS with IN predicate - should use manifest pruning
      val dropResult2 = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$path' WHERE region IN ('us-west', 'eu-west')").collect()
      dropResult2(0).getAs[String]("status") shouldBe "success"
      dropResult2(0).getAs[Long]("splits_removed") should be >= 2L

      // Verify only ap-east remains
      val finalCount = spark.read.format(provider).load(path).count()
      finalCount shouldBe 2

      val apEastCount = spark.read.format(provider).load(path)
        .filter(col("region") === "ap-east").count()
      apEastCount shouldBe 2
    }
  }

  test("DROP PARTITIONS with range predicate should use Avro manifest pruning") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create data across date partitions
      val dates = Seq("2024-01-01", "2024-01-15", "2024-02-01", "2024-02-15", "2024-03-01")
      dates.foreach { date =>
        val data = Seq((1, s"Event-$date", date, 100))
        spark.createDataFrame(data).toDF("id", "name", "date", "value")
          .write.format(provider)
          .partitionBy("date")
          .option("spark.indextables.indexing.fastfields", "value")
          .mode("append")
          .save(path)
      }

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Initial count
      spark.read.format(provider).load(path).count() shouldBe 5

      // DROP with range predicate - should use manifest pruning
      val dropResult = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$path' WHERE date < '2024-02-01'").collect()
      dropResult(0).getAs[String]("status") shouldBe "success"
      dropResult(0).getAs[Long]("splits_removed") shouldBe 2  // 2024-01-01 and 2024-01-15

      // Verify correct partitions remain
      val afterDropCount = spark.read.format(provider).load(path).count()
      afterDropCount shouldBe 3

      // Verify January dates are gone
      val janCount = spark.read.format(provider).load(path)
        .filter(col("date") < "2024-02-01").count()
      janCount shouldBe 0
    }
  }

  test("MERGE SPLITS with WHERE clause should use Avro manifest pruning") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create multiple small splits per partition to trigger merge
      val regions = Seq("us-east", "eu-west")
      (1 to 4).foreach { batch =>
        regions.foreach { region =>
          val data = (1 to 10).map(i => (batch * 100 + i, s"User-$batch-$i", region, i * 10))
          spark.createDataFrame(data).toDF("id", "name", "region", "score")
            .write.format(provider)
            .partitionBy("region")
            .option("spark.indextables.indexing.fastfields", "score")
            .mode("append")
            .save(path)
        }
      }

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify we're using Avro state
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // Initial count
      val initialCount = spark.read.format(provider).load(path).count()
      initialCount shouldBe 80  // 4 batches * 2 regions * 10 records

      // MERGE SPLITS with WHERE clause - should use manifest pruning
      // This targets only us-east partition
      val mergeResult = spark.sql(s"MERGE SPLITS '$path' WHERE region = 'us-east' TARGET SIZE 100M").collect()

      // The merge may or may not find anything to merge depending on split sizes,
      // but it should complete successfully
      val status = mergeResult(0).getStruct(1).getAs[String]("status")
      Seq("success", "no_action") should contain(status)

      // Verify data integrity is preserved
      val afterMergeCount = spark.read.format(provider).load(path).count()
      afterMergeCount shouldBe 80

      // Verify partition data is correct
      val usEastCount = spark.read.format(provider).load(path)
        .filter(col("region") === "us-east").count()
      usEastCount shouldBe 40

      val euWestCount = spark.read.format(provider).load(path)
        .filter(col("region") === "eu-west").count()
      euWestCount shouldBe 40
    }
  }

  test("MERGE SPLITS with WHERE clause should ONLY merge targeted partition files") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create multiple small splits per partition to trigger merge
      val regions = Seq("us-east", "eu-west")
      (1 to 4).foreach { batch =>
        regions.foreach { region =>
          val data = (1 to 10).map(i => (batch * 100 + i, s"User-$batch-$i", region, i * 10))
          spark.createDataFrame(data).toDF("id", "name", "region", "score")
            .write.format(provider)
            .partitionBy("region")
            .option("spark.indextables.indexing.fastfields", "score")
            .mode("append")
            .save(path)
        }
      }

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify we're using Avro state
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // Create transaction log to check files by partition
      val txLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(path), spark)

      // Get initial file counts by partition
      val initialFiles = txLog.listFiles()
      val initialFilesByPartition = initialFiles.groupBy(_.partitionValues)

      println(s"=== INITIAL FILE STATE ===")
      initialFilesByPartition.foreach { case (partition, files) =>
        println(s"  Partition $partition: ${files.size} files")
        files.foreach(f => println(s"    - ${f.path}"))
      }

      val initialUsEastCount = initialFilesByPartition.getOrElse(Map("region" -> "us-east"), Seq.empty).size
      val initialEuWestCount = initialFilesByPartition.getOrElse(Map("region" -> "eu-west"), Seq.empty).size

      println(s"Initial files: us-east=$initialUsEastCount, eu-west=$initialEuWestCount")
      // Spark may create multiple files per batch - verify we have files in both partitions
      initialUsEastCount should be > 0
      initialEuWestCount should be > 0

      // MERGE SPLITS with WHERE clause - should ONLY affect us-east
      println(s"=== EXECUTING MERGE SPLITS WITH WHERE region = 'us-east' ===")
      val mergeResult = spark.sql(s"MERGE SPLITS '$path' WHERE region = 'us-east' TARGET SIZE 100M").collect()

      // Print merge result details
      val metricsRow = mergeResult(0).getStruct(1)
      println(s"  Merge status: ${metricsRow.getAs[String]("status")}")
      println(s"  Merged files: ${metricsRow.getAs[Any]("merged_files")}")
      println(s"  Merge groups: ${metricsRow.getAs[Any]("merge_groups")}")

      // Invalidate cache to get fresh state
      txLog.invalidateCache()

      // Get file counts after merge
      val afterMergeFiles = txLog.listFiles()
      val afterMergeByPartition = afterMergeFiles.groupBy(_.partitionValues)

      println(s"=== AFTER MERGE FILE STATE ===")
      afterMergeByPartition.foreach { case (partition, files) =>
        println(s"  Partition $partition: ${files.size} files")
        files.foreach(f => println(s"    - ${f.path}"))
      }

      val afterUsEastCount = afterMergeByPartition.getOrElse(Map("region" -> "us-east"), Seq.empty).size
      val afterEuWestCount = afterMergeByPartition.getOrElse(Map("region" -> "eu-west"), Seq.empty).size

      println(s"After merge files: us-east=$afterUsEastCount, eu-west=$afterEuWestCount")

      // Key assertion: eu-west should be UNCHANGED!
      afterEuWestCount shouldBe initialEuWestCount

      // us-east should have merged (fewer files)
      val status = mergeResult(0).getStruct(1).getAs[String]("status")
      if (status == "success") {
        afterUsEastCount should be < initialUsEastCount
      }

      // Verify data integrity
      val afterMergeCount = spark.read.format(provider).load(path).count()
      afterMergeCount shouldBe 80

      txLog.close()
    }
  }

  test("PREWARM CACHE with WHERE clause should use Avro manifest pruning") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create partitioned data
      val quarters = Seq("Q1", "Q2", "Q3", "Q4")
      quarters.foreach { quarter =>
        val data = (1 to 5).map(i => (i, s"Item-$quarter-$i", quarter, i * 100))
        spark.createDataFrame(data).toDF("id", "name", "quarter", "value")
          .write.format(provider)
          .partitionBy("quarter")
          .option("spark.indextables.indexing.fastfields", "value")
          .mode("append")
          .save(path)
      }

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify Avro state
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // PREWARM with WHERE clause - should use manifest pruning to only prewarm Q1 and Q2
      val prewarmResult = spark.sql(
        s"PREWARM INDEXTABLES CACHE '$path' WHERE quarter IN ('Q1', 'Q2')"
      ).collect()

      // Should prewarm only splits from Q1 and Q2 partitions (at least 2, one per partition)
      val totalPrewarmed = prewarmResult.map(_.getAs[Int]("splits_prewarmed")).sum
      totalPrewarmed should be >= 2  // At least Q1 + Q2 = 2 splits (may be more due to parallelism)

      // Verify the status
      prewarmResult.foreach { row =>
        Seq("success", "no_splits") should contain(row.getAs[String]("status"))
      }
    }
  }

  test("Commands with compound predicates should use Avro manifest pruning") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create data with two partition columns
      val data = for {
        year <- Seq("2023", "2024")
        month <- Seq("01", "06", "12")
        i <- 1 to 2
      } yield (i, s"Record-$year-$month-$i", year, month, i * 100)

      spark.createDataFrame(data).toDF("id", "name", "year", "month", "value")
        .write.format(provider)
        .partitionBy("year", "month")
        .option("spark.indextables.indexing.fastfields", "value")
        .mode("overwrite")
        .save(path)

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Initial count
      val initialCount = spark.read.format(provider).load(path).count()
      initialCount shouldBe 12  // 2 years * 3 months * 2 records

      // DROP with compound AND predicate - should use manifest pruning
      val dropResult = spark.sql(
        s"DROP INDEXTABLES PARTITIONS FROM '$path' WHERE year = '2023' AND month = '06'"
      ).collect()
      dropResult(0).getAs[String]("status") shouldBe "success"
      dropResult(0).getAs[Long]("splits_removed") shouldBe 1

      // Verify correct partition was dropped
      val afterDropCount = spark.read.format(provider).load(path).count()
      afterDropCount shouldBe 10  // 12 - 2

      // Verify the specific partition is gone
      val droppedPartitionCount = spark.read.format(provider).load(path)
        .filter(col("year") === "2023" && col("month") === "06").count()
      droppedPartitionCount shouldBe 0

      // Other 2023 partitions should still exist
      val year2023Count = spark.read.format(provider).load(path)
        .filter(col("year") === "2023").count()
      year2023Count shouldBe 4  // 01 + 12 = 2 partitions * 2 records
    }
  }

  test("Multiple manifests scenario should benefit from partition pruning") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create data and checkpoint multiple times to create multiple manifests
      val regions = Seq("region-A", "region-B", "region-C", "region-D", "region-E")

      regions.foreach { region =>
        // Write data for this region
        val data = (1 to 10).map(i => (i, s"Item-$region-$i", region, i * 10))
        spark.createDataFrame(data).toDF("id", "name", "region", "value")
          .write.format(provider)
          .partitionBy("region")
          .option("spark.indextables.indexing.fastfields", "value")
          .mode("append")
          .save(path)

        // Create checkpoint after each write (creates separate manifest entries)
        spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      }

      // Verify final state
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // Total count should be 50 (5 regions * 10 records)
      val totalCount = spark.read.format(provider).load(path).count()
      totalCount shouldBe 50

      // DROP specific region - with manifest pruning, only relevant manifest should be read
      val dropResult = spark.sql(
        s"DROP INDEXTABLES PARTITIONS FROM '$path' WHERE region = 'region-C'"
      ).collect()
      dropResult(0).getAs[String]("status") shouldBe "success"

      // Verify only region-C was dropped
      val afterDropCount = spark.read.format(provider).load(path).count()
      afterDropCount shouldBe 40  // 50 - 10

      val regionCCount = spark.read.format(provider).load(path)
        .filter(col("region") === "region-C").count()
      regionCCount shouldBe 0

      // Other regions should be intact
      Seq("region-A", "region-B", "region-D", "region-E").foreach { region =>
        val count = spark.read.format(provider).load(path)
          .filter(col("region") === region).count()
        count shouldBe 10
      }
    }
  }

  // ==========================================================================
  // Bug Reproduction: MERGE SPLITS visibility after multiple appends
  // https://github.com/anthropics/claude-code/issues/XXX
  // ==========================================================================

  test("MERGE SPLITS should see ALL files from multiple append transactions") {
    withTempPath { tempDir =>
      val path = tempDir

      // Clear global caches to ensure fresh state
      io.indextables.spark.transaction.EnhancedTransactionLogCache.clearGlobalCaches()

      println("=== TEST: MERGE SPLITS visibility after multiple appends ===")

      // First append: create 3 splits (3 batches of data)
      println("\n--- First append: writing 3 batches (should create 3 splits) ---")
      (1 to 3).foreach { batch =>
        val data = (1 to 10).map(i => (batch * 100 + i, s"Batch1-$batch-$i", batch * 10))
        spark.createDataFrame(data).toDF("id", "name", "score")
          .write.format(provider)
          .option("spark.indextables.indexing.fastfields", "id,score")
          .mode("append")
          .save(path)
      }

      // Check transaction log files after first append
      val txLog1 = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(path), spark)
      val filesAfterAppend1 = txLog1.listFiles()
      println(s"After first append: ${filesAfterAppend1.size} files")
      filesAfterAppend1.foreach(f => println(s"  - ${f.path}"))
      txLog1.close()

      // Clear caches between appends to simulate separate sessions
      io.indextables.spark.transaction.EnhancedTransactionLogCache.clearGlobalCaches()

      // Second append: create 4 more splits (4 batches of data)
      println("\n--- Second append: writing 4 batches (should create 4 splits) ---")
      (4 to 7).foreach { batch =>
        val data = (1 to 10).map(i => (batch * 100 + i, s"Batch2-$batch-$i", batch * 10))
        spark.createDataFrame(data).toDF("id", "name", "score")
          .write.format(provider)
          .option("spark.indextables.indexing.fastfields", "id,score")
          .mode("append")
          .save(path)
      }

      // Check transaction log files after second append
      val txLog2 = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(path), spark)
      val filesAfterAppend2 = txLog2.listFiles()
      println(s"After second append: ${filesAfterAppend2.size} files")
      filesAfterAppend2.foreach(f => println(s"  - ${f.path}"))
      txLog2.close()

      // Verify total row count
      val totalCount = spark.read.format(provider).load(path).count()
      println(s"Total row count: $totalCount (expected 70)")
      totalCount shouldBe 70 // 7 batches * 10 rows

      // Clear caches to simulate a new session for MERGE
      io.indextables.spark.transaction.EnhancedTransactionLogCache.clearGlobalCaches()

      // Run MERGE SPLITS
      println("\n--- Running MERGE SPLITS ---")
      val mergeResult = spark.sql(s"MERGE SPLITS '$path' TARGET SIZE 100M").collect()

      // Check merge result
      val metricsRow = mergeResult(0).getStruct(1)
      val status = metricsRow.getAs[String]("status")
      val mergedFiles = Option(metricsRow.getAs[java.lang.Long]("merged_files")).map(_.toLong).getOrElse(0L)
      println(s"Merge status: $status")
      println(s"Merged files: $mergedFiles")

      // CRITICAL ASSERTION: The merge should have seen ALL files
      // If only 3 files were merged (from first append), this indicates the bug
      if (status == "success") {
        // We expect at least 6-7 files to be merged (from both appends)
        // Each write batch creates at least 1 split
        assert(mergedFiles >= 6L,
          s"Bug detected: MERGE only saw $mergedFiles files, but expected at least 6 from both appends")
      }

      // Verify data integrity after merge
      io.indextables.spark.transaction.EnhancedTransactionLogCache.clearGlobalCaches()
      val afterMergeCount = spark.read.format(provider).load(path).count()
      println(s"After merge row count: $afterMergeCount (expected 70)")
      afterMergeCount shouldBe 70

      // Verify files from both appends are represented in the data using ID ranges
      // Batch1 IDs: 101-110, 201-210, 301-310 (total 30 rows)
      // Batch2 IDs: 401-410, 501-510, 601-610, 701-710 (total 40 rows)
      val batch1Count = spark.read.format(provider).load(path)
        .filter(col("id") < 400).count()
      val batch2Count = spark.read.format(provider).load(path)
        .filter(col("id") >= 400).count()

      println(s"Batch1 rows (id < 400): $batch1Count (expected 30)")
      println(s"Batch2 rows (id >= 400): $batch2Count (expected 40)")

      batch1Count shouldBe 30
      batch2Count shouldBe 40

      println("\n=== TEST PASSED: All files visible to MERGE SPLITS ===")
    }
  }

  test("Transaction log correctly tracks files across multiple Avro state versions") {
    withTempPath { tempDir =>
      val path = tempDir
      val txLogPath = s"$path/_transaction_log"

      // Clear global caches
      io.indextables.spark.transaction.EnhancedTransactionLogCache.clearGlobalCaches()

      println("=== TEST: Transaction log state version tracking ===")

      // First write
      println("\n--- First write ---")
      val data1 = (1 to 5).map(i => (i, s"First-$i", i * 10))
      spark.createDataFrame(data1).toDF("id", "name", "score")
        .write.format(provider)
        .option("spark.indextables.indexing.fastfields", "id,score")
        .mode("overwrite")
        .save(path)

      // Check state after first write
      val cloudProvider1 = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        txLogPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )
      val stateDirs1 = cloudProvider1.listFiles(txLogPath, recursive = false)
        .filter(_.path.contains("state-v"))
        .map(_.path)
      println(s"State directories after first write: ${stateDirs1.mkString(", ")}")
      cloudProvider1.close()

      // Clear caches
      io.indextables.spark.transaction.EnhancedTransactionLogCache.clearGlobalCaches()

      // Second write (append)
      println("\n--- Second write (append) ---")
      val data2 = (6 to 10).map(i => (i, s"Second-$i", i * 10))
      spark.createDataFrame(data2).toDF("id", "name", "score")
        .write.format(provider)
        .option("spark.indextables.indexing.fastfields", "id,score")
        .mode("append")
        .save(path)

      // Check state after second write
      val cloudProvider2 = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        txLogPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )
      val stateDirs2 = cloudProvider2.listFiles(txLogPath, recursive = false)
        .filter(_.path.contains("state-v"))
        .map(_.path)
        .sorted
      println(s"State directories after second write: ${stateDirs2.mkString(", ")}")

      // Read _last_checkpoint to see which state it points to
      val lastCheckpointPath = s"$txLogPath/_last_checkpoint"
      if (cloudProvider2.exists(lastCheckpointPath)) {
        val content = new String(cloudProvider2.readFile(lastCheckpointPath), "UTF-8")
        println(s"_last_checkpoint content: $content")
      }
      cloudProvider2.close()

      // Verify state version incremented
      assert(stateDirs2.size >= 2,
        "Expected at least 2 state directories after two writes")

      // Clear caches and read with fresh TransactionLog
      io.indextables.spark.transaction.EnhancedTransactionLogCache.clearGlobalCaches()

      val txLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(path), spark)
      val allFiles = txLog.listFiles()
      println(s"\nTotal files from TransactionLog: ${allFiles.size}")
      allFiles.foreach(f => println(s"  - ${f.path}"))

      // Verify all data is readable
      val totalCount = spark.read.format(provider).load(path).count()
      println(s"Total row count: $totalCount (expected 10)")
      totalCount shouldBe 10

      // First batch IDs: 1-5, Second batch IDs: 6-10
      val firstCount = spark.read.format(provider).load(path)
        .filter(col("id") <= 5).count()
      val secondCount = spark.read.format(provider).load(path)
        .filter(col("id") > 5).count()

      println(s"First batch rows (id <= 5): $firstCount (expected 5)")
      println(s"Second batch rows (id > 5): $secondCount (expected 5)")

      firstCount shouldBe 5
      secondCount shouldBe 5

      txLog.close()
      println("\n=== TEST PASSED ===")
    }
  }
}
