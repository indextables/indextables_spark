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
    } finally {
      spark.conf.unset("spark.indextables.state.format")
    }
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
