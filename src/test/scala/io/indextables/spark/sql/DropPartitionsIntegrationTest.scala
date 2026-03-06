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

package io.indextables.spark.sql

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.{FileSystem, Path}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

/** Integration tests for DROP INDEXTABLES PARTITIONS command. */
class DropPartitionsIntegrationTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: String     = _
  var fs: FileSystem      = _

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("DropPartitionsIntegrationTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.indextables.purge.retentionCheckEnabled", "true")
      .getOrCreate()

    tempDir = Files.createTempDirectory("drop_partitions_test").toString
    fs = new Path(tempDir).getFileSystem(spark.sparkContext.hadoopConfiguration)
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    // Cleanup temp directory
    if (tempDir != null) {
      val dir = new File(tempDir)
      if (dir.exists()) {
        deleteRecursively(dir)
      }
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  test("DROP INDEXTABLES PARTITIONS should logically remove partitions matching equality predicate") {
    val tablePath = s"$tempDir/partitioned_table"

    // Write partitioned data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "Alice", "2023", "01"),
      (2, "Bob", "2023", "01"),
      (3, "Charlie", "2023", "02"),
      (4, "Diana", "2024", "01"),
      (5, "Eve", "2024", "02")
    ).toDF("id", "name", "year", "month")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("year", "month")
      .mode("overwrite")
      .save(tablePath)

    // Verify initial data
    val beforeDrop = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(beforeDrop.count() == 5)

    // Drop partitions for year = '2023'
    val result = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE year = '2023'").collect()
    assert(result.length == 1)
    assert(result(0).getString(1) == "success")
    assert(result(0).getLong(2) == 2) // 2 partitions dropped (01 and 02 for 2023)

    // Verify data after drop - should only see 2024 data
    val afterDrop = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterDrop.count() == 2)
    assert(afterDrop.filter($"year" === "2024").count() == 2)
    assert(afterDrop.filter($"year" === "2023").count() == 0)
  }

  test("DROP INDEXTABLES PARTITIONS should logically remove partitions matching range predicate") {
    val tablePath = s"$tempDir/range_test"

    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "A", "2020"),
      (2, "B", "2021"),
      (3, "C", "2022"),
      (4, "D", "2023"),
      (5, "E", "2024")
    ).toDF("id", "name", "year")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("year")
      .mode("overwrite")
      .save(tablePath)

    // Drop partitions where year < '2022'
    val result = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE year < '2022'").collect()
    assert(result.length == 1)
    assert(result(0).getString(1) == "success")

    // Verify data after drop - should only see 2022, 2023, 2024 data
    val afterDrop = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterDrop.count() == 3)
    assert(afterDrop.filter($"year" >= "2022").count() == 3)
  }

  test("DROP INDEXTABLES PARTITIONS should logically remove partitions matching compound AND predicate") {
    val tablePath = s"$tempDir/compound_and_test"

    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "A", "2023", "Q1"),
      (2, "B", "2023", "Q2"),
      (3, "C", "2023", "Q3"),
      (4, "D", "2024", "Q1"),
      (5, "E", "2024", "Q2")
    ).toDF("id", "name", "year", "quarter")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("year", "quarter")
      .mode("overwrite")
      .save(tablePath)

    // Drop partitions where year = '2023' AND quarter = 'Q1'
    val result =
      spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE year = '2023' AND quarter = 'Q1'").collect()
    assert(result.length == 1)
    assert(result(0).getString(1) == "success")
    assert(result(0).getLong(2) == 1) // Only 1 partition dropped

    // Verify data after drop
    val afterDrop = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterDrop.count() == 4)
    assert(afterDrop.filter($"year" === "2023" && $"quarter" === "Q1").count() == 0)
    assert(afterDrop.filter($"year" === "2023" && $"quarter" =!= "Q1").count() == 2)
  }

  test("DROP INDEXTABLES PARTITIONS should fail when WHERE clause references non-partition columns") {
    val tablePath = s"$tempDir/non_partition_error"

    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "Alice", "2023"),
      (2, "Bob", "2024")
    ).toDF("id", "name", "year")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("year")
      .mode("overwrite")
      .save(tablePath)

    // Try to drop using non-partition column 'name'
    val ex = intercept[Exception] {
      spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE name = 'Alice'").collect()
    }

    assert(ex.getMessage.contains("non-partition") || ex.getMessage.contains("Only partition columns"))
  }

  test("DROP INDEXTABLES PARTITIONS should fail on non-partitioned table") {
    val tablePath = s"$tempDir/non_partitioned"

    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "Alice"),
      (2, "Bob")
    ).toDF("id", "name")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Try to drop partitions from non-partitioned table
    val ex = intercept[Exception] {
      spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE id = 1").collect()
    }

    assert(ex.getMessage.contains("non-partitioned") || ex.getMessage.contains("no partition columns"))
  }

  test("DROP INDEXTABLES PARTITIONS should return no_action when no partitions match") {
    val tablePath = s"$tempDir/no_match_test"

    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "Alice", "2023"),
      (2, "Bob", "2024")
    ).toDF("id", "name", "year")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("year")
      .mode("overwrite")
      .save(tablePath)

    // Drop partitions for year = '2025' (doesn't exist)
    val result = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE year = '2025'").collect()
    assert(result.length == 1)
    assert(result(0).getString(1) == "no_action")
    assert(result(0).getLong(2) == 0) // 0 partitions dropped
    assert(result(0).getLong(3) == 0) // 0 splits removed
  }

  test("DESCRIBE TRANSACTION LOG should show remove actions after DROP PARTITIONS") {
    // Use JSON format since this test validates JSON transaction log structure
    val tablePath = s"$tempDir/describe_test"

    // Set session-level format to ensure DROP PARTITIONS uses JSON format
    spark.conf.set("spark.indextables.state.format", "json")

    try {
      val sparkSession = spark
      import sparkSession.implicits._
      val data = Seq(
        (1, "Alice", "2023"),
        (2, "Bob", "2024")
      ).toDF("id", "name", "year")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.state.format", "json")
        .partitionBy("year")
        .mode("overwrite")
        .save(tablePath)

      // Drop 2023 partitions
      spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE year = '2023'").collect()

      // Use DESCRIBE TRANSACTION LOG to verify remove actions
      // Schema: version, log_file_path, action_type, path, partition_values, ...
      val describeResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' INCLUDE ALL").collect()

      // Should have at least one remove action for the dropped partition
      // action_type is at index 2
      val removeActions = describeResult.filter(_.getString(2) == "remove")
      assert(removeActions.length >= 1, s"Expected at least one remove action, got: ${removeActions.length}")

      // The 'path' column (index 3) should contain the split file path
      val removeActionPath = removeActions(0).getString(3)
      assert(
        removeActionPath != null && removeActionPath.endsWith(".split"),
        s"Expected remove action for a .split file, got: $removeActionPath"
      )
    } finally
      spark.conf.unset("spark.indextables.state.format")
  }

  test("PURGE INDEXTABLE should clean up files from dropped partitions after retention") {
    val tablePath = s"$tempDir/purge_after_drop"

    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "Alice", "2023"),
      (2, "Bob", "2023"),
      (3, "Charlie", "2024")
    ).toDF("id", "name", "year")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("year")
      .mode("overwrite")
      .save(tablePath)

    // Get the split files before drop
    val splitFiles = fs
      .listStatus(new Path(tablePath))
      .flatMap { status =>
        if (status.isDirectory && status.getPath.getName.startsWith("year=")) {
          fs.listStatus(status.getPath).filter(_.getPath.getName.endsWith(".split"))
        } else {
          Array.empty[org.apache.hadoop.fs.FileStatus]
        }
      }

    val year2023Splits = splitFiles.filter(_.getPath.toString.contains("year=2023"))
    assert(year2023Splits.nonEmpty, "Should have splits for 2023 partition")

    // Drop 2023 partitions
    val dropResult = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE year = '2023'").collect()
    assert(dropResult(0).getString(1) == "success")

    // Verify data is logically removed
    val afterDrop = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterDrop.count() == 1)
    assert(afterDrop.filter($"year" === "2023").count() == 0)

    // The files should still physically exist
    val year2023Dir = new Path(s"$tablePath/year=2023")
    assert(fs.exists(year2023Dir) || year2023Splits.exists(s => fs.exists(s.getPath)))

    // Set the modification time of dropped files to be old enough for purge
    // First, find all .split files
    val allSplitFiles = fs
      .listStatus(new Path(tablePath))
      .flatMap { status =>
        if (status.isDirectory && status.getPath.getName.startsWith("year=")) {
          fs.listStatus(status.getPath)
        } else {
          Array.empty[org.apache.hadoop.fs.FileStatus]
        }
      }

    // Set modification time to 8 days ago for 2023 partition files
    val oldTime = System.currentTimeMillis() - (8L * 24 * 60 * 60 * 1000)
    allSplitFiles.filter(_.getPath.toString.contains("year=2023")).foreach { status =>
      fs.setTimes(status.getPath, oldTime, -1)
    }

    // Run PURGE to clean up orphaned files
    val purgeResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    assert(purgeResult.length == 1)

    // Verify the 2024 data is still intact
    val afterPurge = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterPurge.count() == 1)
    assert(afterPurge.filter($"year" === "2024").count() == 1)
  }

  test("DROP INDEXTABLES PARTITIONS with OR compound predicate") {
    val tablePath = s"$tempDir/or_compound_test"

    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "A", "2022"),
      (2, "B", "2023"),
      (3, "C", "2024"),
      (4, "D", "2025")
    ).toDF("id", "name", "year")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("year")
      .mode("overwrite")
      .save(tablePath)

    // Drop partitions where year = '2022' OR year = '2024'
    val result =
      spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE year = '2022' OR year = '2024'").collect()
    assert(result.length == 1)
    assert(result(0).getString(1) == "success")
    assert(result(0).getLong(2) == 2) // 2 partitions dropped

    // Verify data after drop - should only see 2023 and 2025 data
    val afterDrop = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterDrop.count() == 2)
    assert(afterDrop.filter($"year" === "2023").count() == 1)
    assert(afterDrop.filter($"year" === "2025").count() == 1)
  }

  test("DROP INDEXTABLES PARTITIONS with BETWEEN predicate") {
    val tablePath = s"$tempDir/between_test"

    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "A", 1),
      (2, "B", 2),
      (3, "C", 3),
      (4, "D", 4),
      (5, "E", 5),
      (6, "F", 6)
    ).toDF("id", "name", "month")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("month")
      .mode("overwrite")
      .save(tablePath)

    // Drop partitions where month BETWEEN 2 AND 4
    val result = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE month BETWEEN 2 AND 4").collect()
    assert(result.length == 1)
    assert(result(0).getString(1) == "success")

    // Verify data after drop - should only see months 1, 5, 6
    val afterDrop = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterDrop.count() == 3)
    assert(afterDrop.filter($"month" === 1).count() == 1)
    assert(afterDrop.filter($"month" === 5).count() == 1)
    assert(afterDrop.filter($"month" === 6).count() == 1)
  }

  test("DROP PARTITIONS should work with separate inserts to different partitions") {
    // Bug reproduction: https://github.com/...
    // 1. Create table with partition key
    // 2. Insert to partition 01
    // 3. Insert to partition 02 (separate transaction)
    // 4. Try to drop partition 02 - should work, but bug shows "no_action" "table is empty"
    // 5. Try to drop partition 01 - success but partition 02 also becomes unreadable

    val tablePath = s"$tempDir/separate_inserts_bug"

    val sparkSession = spark
    import sparkSession.implicits._

    // First insert: partition 01
    println("=== Insert 1: partition=01 ===")
    val data1 = Seq(
      (1, "Alice", "01"),
      (2, "Bob", "01")
    ).toDF("id", "name", "partition_key")

    data1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("partition_key")
      .mode("overwrite")
      .save(tablePath)

    // Verify first insert
    val count1 = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath).count()
    println(s"After insert 1: count=$count1")
    assert(count1 == 2, s"Expected 2 records after first insert, got $count1")

    // Second insert: partition 02 (separate transaction)
    println("=== Insert 2: partition=02 ===")
    val data2 = Seq(
      (3, "Charlie", "02"),
      (4, "Diana", "02")
    ).toDF("id", "name", "partition_key")

    data2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("partition_key")
      .mode("append")
      .save(tablePath)

    // Verify second insert
    val count2 = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath).count()
    println(s"After insert 2: count=$count2")
    assert(count2 == 4, s"Expected 4 records after second insert, got $count2")

    // Verify both partitions exist
    val df               = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    val partition01Count = df.filter($"partition_key" === "01").count()
    val partition02Count = df.filter($"partition_key" === "02").count()
    println(s"Partition counts: 01=$partition01Count, 02=$partition02Count")
    assert(partition01Count == 2, s"Expected 2 in partition 01, got $partition01Count")
    assert(partition02Count == 2, s"Expected 2 in partition 02, got $partition02Count")

    // Now try to drop partition 02
    println("=== Drop partition 02 ===")
    val dropResult = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE partition_key = '02'").collect()
    dropResult.foreach(r => println(s"Drop result: ${r.mkString(", ")}"))

    val dropStatus  = dropResult(0).getString(1)
    val dropMessage = dropResult(0).getString(5)
    println(s"Drop status: $dropStatus, message: $dropMessage")

    // BUG: User reported getting "no_action" "table is empty" here
    assert(dropStatus == "success", s"Expected 'success' but got '$dropStatus' with message: $dropMessage")
    assert(dropResult(0).getLong(2) == 1, s"Expected 1 partition dropped, got ${dropResult(0).getLong(2)}")

    // Verify partition 02 is dropped but partition 01 still exists
    val afterDropCount =
      spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath).count()
    println(s"After drop 02: count=$afterDropCount")
    assert(afterDropCount == 2, s"Expected 2 records after dropping partition 02, got $afterDropCount")

    val afterDrop01 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .filter($"partition_key" === "01")
      .count()
    val afterDrop02 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .filter($"partition_key" === "02")
      .count()
    println(s"After drop: partition 01=$afterDrop01, partition 02=$afterDrop02")

    assert(afterDrop01 == 2, s"Partition 01 should still have 2 records, got $afterDrop01")
    assert(afterDrop02 == 0, s"Partition 02 should be empty, got $afterDrop02")

    println("=== Test passed: DROP PARTITIONS works correctly with separate inserts ===")
  }

  test("DROP PARTITIONS with Avro checkpoint should work with separate inserts to different partitions") {
    // Same bug but with explicit Avro checkpoint creation between inserts
    val tablePath = s"$tempDir/avro_separate_inserts_bug"

    val sparkSession = spark
    import sparkSession.implicits._

    // Enable Avro state format
    spark.conf.set("spark.indextables.state.format", "avro")

    try {
      // First insert: partition 01
      println("=== [AVRO] Insert 1: partition=01 ===")
      val data1 = Seq(
        (1, "Alice", "01"),
        (2, "Bob", "01")
      ).toDF("id", "name", "partition_key")

      data1.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("partition_key")
        .mode("overwrite")
        .save(tablePath)

      // Create Avro checkpoint after first insert
      println("=== [AVRO] Creating checkpoint after insert 1 ===")
      spark.sql(s"CHECKPOINT INDEXTABLES '$tablePath'").collect()

      // Verify checkpoint format
      val stateResult1 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$tablePath'").collect()
      println(s"State format after insert 1: ${stateResult1(0).getAs[String]("format")}")

      // Verify first insert
      val count1 = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath).count()
      println(s"[AVRO] After insert 1: count=$count1")
      assert(count1 == 2, s"Expected 2 records after first insert, got $count1")

      // Second insert: partition 02 (separate transaction)
      println("=== [AVRO] Insert 2: partition=02 ===")
      val data2 = Seq(
        (3, "Charlie", "02"),
        (4, "Diana", "02")
      ).toDF("id", "name", "partition_key")

      data2.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("partition_key")
        .mode("append")
        .save(tablePath)

      // Create Avro checkpoint after second insert
      println("=== [AVRO] Creating checkpoint after insert 2 ===")
      spark.sql(s"CHECKPOINT INDEXTABLES '$tablePath'").collect()

      // Verify checkpoint format
      val stateResult2 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$tablePath'").collect()
      println(s"State format after insert 2: ${stateResult2(0).getAs[String]("format")}")

      // Verify second insert
      val count2 = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath).count()
      println(s"[AVRO] After insert 2: count=$count2")
      assert(count2 == 4, s"Expected 4 records after second insert, got $count2")

      // Verify both partitions exist
      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val partition01Count = df.filter($"partition_key" === "01").count()
      val partition02Count = df.filter($"partition_key" === "02").count()
      println(s"[AVRO] Partition counts: 01=$partition01Count, 02=$partition02Count")
      assert(partition01Count == 2, s"Expected 2 in partition 01, got $partition01Count")
      assert(partition02Count == 2, s"Expected 2 in partition 02, got $partition02Count")

      // Now try to drop partition 02
      println("=== [AVRO] Drop partition 02 ===")
      val dropResult = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE partition_key = '02'").collect()
      dropResult.foreach(r => println(s"[AVRO] Drop result: ${r.mkString(", ")}"))

      val dropStatus  = dropResult(0).getString(1)
      val dropMessage = dropResult(0).getString(5)
      println(s"[AVRO] Drop status: $dropStatus, message: $dropMessage")

      // BUG: User reported getting "no_action" "table is empty" here
      assert(dropStatus == "success", s"Expected 'success' but got '$dropStatus' with message: $dropMessage")
      assert(dropResult(0).getLong(2) == 1, s"Expected 1 partition dropped, got ${dropResult(0).getLong(2)}")

      // Verify partition 02 is dropped but partition 01 still exists
      val afterDropCount =
        spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath).count()
      println(s"[AVRO] After drop 02: count=$afterDropCount")
      assert(afterDropCount == 2, s"Expected 2 records after dropping partition 02, got $afterDropCount")

      val afterDrop01 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)
        .filter($"partition_key" === "01")
        .count()
      val afterDrop02 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)
        .filter($"partition_key" === "02")
        .count()
      println(s"[AVRO] After drop: partition 01=$afterDrop01, partition 02=$afterDrop02")

      assert(afterDrop01 == 2, s"Partition 01 should still have 2 records, got $afterDrop01")
      assert(afterDrop02 == 0, s"Partition 02 should be empty, got $afterDrop02")

      println("=== [AVRO] Test passed: DROP PARTITIONS works correctly with separate inserts ===")
    } finally
      spark.conf.unset("spark.indextables.state.format")
  }

  // ==========================================================================
  // IT-036: Numeric Partition Predicate Regression Tests
  // These tests verify correct numeric comparison for partition predicates.
  // With the old code (lexicographic), these would fail because "10" < "2".
  // ==========================================================================

  test("IT-036 REGRESSION: DROP PARTITIONS with numeric month > 9 should correctly identify months 10-12") {
    val tablePath = s"$tempDir/it036_numeric_gt"

    val sparkSession = spark
    import sparkSession.implicits._

    // Write data with integer month partitions 1-12
    val data = (1 to 12).map(m => (m, s"data_$m", m)).toDF("id", "name", "month")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("month")
      .mode("overwrite")
      .save(tablePath)

    // Verify initial data
    val beforeDrop = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(beforeDrop.count() == 12)

    // Drop partitions where month > 9 — should remove months 10, 11, 12
    // BUG: With lexicographic comparison, "10" < "9" so months 10-12 would NOT match month > 9
    val result = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE month > 9").collect()
    assert(result.length == 1)
    assert(result(0).getString(1) == "success")
    assert(result(0).getLong(2) == 3, s"Expected 3 partitions dropped (10, 11, 12), got ${result(0).getLong(2)}")

    // Verify data after drop - should only see months 1-9
    val afterDrop = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterDrop.count() == 9, s"Expected 9 records after drop, got ${afterDrop.count()}")

    // Verify each remaining month
    for (m <- 1 to 9)
      assert(afterDrop.filter($"month" === m).count() == 1, s"Month $m should still exist")
    for (m <- 10 to 12)
      assert(afterDrop.filter($"month" === m).count() == 0, s"Month $m should be dropped")
  }

  test("IT-036 REGRESSION: DROP PARTITIONS with numeric BETWEEN 2 AND 11 should correctly match months 2-11") {
    val tablePath = s"$tempDir/it036_numeric_between"

    val sparkSession = spark
    import sparkSession.implicits._

    // Write data with integer month partitions 1-12
    val data = (1 to 12).map(m => (m, s"data_$m", m)).toDF("id", "name", "month")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("month")
      .mode("overwrite")
      .save(tablePath)

    // Drop partitions where month BETWEEN 2 AND 11
    // BUG: With lexicographic comparison, "10" < "2" so months 10-11 would be missed
    val result = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE month BETWEEN 2 AND 11").collect()
    assert(result.length == 1)
    assert(result(0).getString(1) == "success")
    assert(result(0).getLong(2) == 10, s"Expected 10 partitions dropped (months 2-11), got ${result(0).getLong(2)}")

    // Verify data after drop - should only see months 1 and 12
    val afterDrop = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterDrop.count() == 2, s"Expected 2 records after drop, got ${afterDrop.count()}")
    assert(afterDrop.filter($"month" === 1).count() == 1, "Month 1 should still exist")
    assert(afterDrop.filter($"month" === 12).count() == 1, "Month 12 should still exist")
  }

  test("IT-036 REGRESSION: DROP PARTITIONS with numeric month < 10 should correctly match months 1-9 only") {
    val tablePath = s"$tempDir/it036_numeric_lt"

    val sparkSession = spark
    import sparkSession.implicits._

    // Write data with integer month partitions 1-12
    val data = (1 to 12).map(m => (m, s"data_$m", m)).toDF("id", "name", "month")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("month")
      .mode("overwrite")
      .save(tablePath)

    // Drop partitions where month < 10 — should remove months 1-9
    // BUG: With lexicographic comparison, "10" < "10" is false (correct),
    // but "2" < "10" is ALSO false because '2' > '1'
    val result = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE month < 10").collect()
    assert(result.length == 1)
    assert(result(0).getString(1) == "success")
    assert(result(0).getLong(2) == 9, s"Expected 9 partitions dropped (months 1-9), got ${result(0).getLong(2)}")

    // Verify data after drop - should only see months 10, 11, 12
    val afterDrop = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterDrop.count() == 3, s"Expected 3 records after drop, got ${afterDrop.count()}")
    for (m <- 10 to 12)
      assert(afterDrop.filter($"month" === m).count() == 1, s"Month $m should still exist")
  }

  test("Multiple DROP INDEXTABLES PARTITIONS operations should be cumulative") {
    val tablePath = s"$tempDir/cumulative_test"

    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "A", "2020"),
      (2, "B", "2021"),
      (3, "C", "2022"),
      (4, "D", "2023"),
      (5, "E", "2024")
    ).toDF("id", "name", "year")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("year")
      .mode("overwrite")
      .save(tablePath)

    // First drop: 2020
    val result1 = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE year = '2020'").collect()
    assert(result1(0).getString(1) == "success")

    // Verify
    val afterDrop1 = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterDrop1.count() == 4)

    // Second drop: 2021
    val result2 = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE year = '2021'").collect()
    assert(result2(0).getString(1) == "success")

    // Verify
    val afterDrop2 = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterDrop2.count() == 3)

    // Third drop: 2022
    val result3 = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE year = '2022'").collect()
    assert(result3(0).getString(1) == "success")

    // Verify final state
    val afterDrop3 = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterDrop3.count() == 2)
    assert(afterDrop3.filter($"year" === "2023").count() == 1)
    assert(afterDrop3.filter($"year" === "2024").count() == 1)
  }
}
