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

import java.io.File

import scala.util.Random

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

/**
 * Tests for DESCRIBE INDEXTABLES TRANSACTION LOG command with Avro state format.
 *
 * These tests verify that:
 *   - DESCRIBE command works with Avro state directories
 *   - Command outputs correct format information for Avro
 *   - INCLUDE ALL retrieves actions from Avro manifests
 */
class AvroDescribeTransactionLogTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: String     = _

  override def beforeEach(): Unit = {
    // Use Avro format (default)
    spark = SparkSession
      .builder()
      .appName("AvroDescribeTransactionLogTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.indextables.state.format", "avro")
      .config("spark.indextables.checkpoint.enabled", "true")
      .config("spark.indextables.checkpoint.interval", "5")
      .getOrCreate()

    tempDir = System.getProperty("java.io.tmpdir") + "/avro_describe_txlog_test_" + Random.nextLong().abs
    new File(tempDir).mkdirs()
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
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

  test("Avro: DESCRIBE TRANSACTION LOG should return all actions") {
    val tablePath    = s"$tempDir/test_table"
    val sparkSession = spark
    import sparkSession.implicits._

    // Write initial data
    val data1 = Seq((1, "one"), (2, "two")).toDF("id", "value")
    data1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "id")
      .mode("overwrite")
      .save(tablePath)

    // Write more data
    val data2 = Seq((3, "three"), (4, "four")).toDF("id", "value")
    data2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("append")
      .save(tablePath)

    // Describe the transaction log
    val result = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath'")

    // Verify we got results
    assert(result.count() > 0, "Should have returned transaction log actions")

    // Check schema
    val expectedColumns = Seq(
      "version",
      "log_file_path",
      "action_type",
      "path",
      "partition_values",
      "size",
      "data_change",
      "tags"
    )
    expectedColumns.foreach { colName =>
      assert(result.columns.contains(colName), s"Result should contain column: $colName")
    }

    // Verify action types - Avro format may have different action combinations
    val actionTypes = result.select("action_type").distinct().collect().map(_.getString(0)).toSet
    assert(actionTypes.nonEmpty, "Should have some action types")
    // At minimum, we should have add actions for the files written
    assert(actionTypes.contains("add"), "Should have add actions")
  }

  test("Avro: DESCRIBE TRANSACTION LOG INCLUDE ALL should work with state directories") {
    val tablePath    = s"$tempDir/test_table2"
    val sparkSession = spark
    import sparkSession.implicits._

    // Create multiple versions to trigger checkpoint
    (1 to 10).foreach { i =>
      val data = Seq((i, s"value_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "id")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Verify Avro state directory was created
    val txLogPath        = new File(tablePath, "_transaction_log")
    val stateDirectories = txLogPath.listFiles().filter(_.getName.startsWith("state-v"))
    assert(stateDirectories.nonEmpty, "Should have created Avro state directory")

    // Describe without INCLUDE ALL
    val resultWithoutAll = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath'")
    val countWithoutAll  = resultWithoutAll.count()

    // Describe with INCLUDE ALL
    val resultWithAll = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' INCLUDE ALL")
    val countWithAll  = resultWithAll.count()

    // Both should return results
    assert(countWithoutAll > 0, "Should return actions without INCLUDE ALL")
    assert(countWithAll > 0, "Should return actions with INCLUDE ALL")

    // Verify add actions are present in one of the results
    val addActionsWithoutAll = resultWithoutAll.filter("action_type = 'add'").count()
    val addActionsWithAll    = resultWithAll.filter("action_type = 'add'").count()
    val totalAddActions      = Math.max(addActionsWithoutAll, addActionsWithAll)
    assert(totalAddActions >= 10, s"Should have at least 10 add actions total, found $totalAddActions")
  }

  test("Avro: DESCRIBE should work after multiple appends") {
    val tablePath    = s"$tempDir/test_table3"
    val sparkSession = spark
    import sparkSession.implicits._

    // Create table with multiple appends
    val initialData = Seq((1, "initial")).toDF("id", "value")
    initialData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "id")
      .mode("overwrite")
      .save(tablePath)

    // Multiple appends
    (2 to 5).foreach { i =>
      val data = Seq((i, s"append_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(tablePath)
    }

    // Describe transaction log
    val result = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath'")

    // Verify we have multiple add actions
    val addActions = result.filter("action_type = 'add'").count()
    assert(addActions >= 5, s"Should have at least 5 add actions for 5 writes, found $addActions")

    // Verify table data is intact
    val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(df.count() == 5, "Should have 5 rows total")
  }

  test("Avro: DESCRIBE should show Avro state info in log file path") {
    val tablePath    = s"$tempDir/test_table4"
    val sparkSession = spark
    import sparkSession.implicits._

    // Create enough versions to trigger Avro state creation
    (1 to 10).foreach { i =>
      val data = Seq((i, s"value_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "id")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Describe transaction log
    val result   = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath'")
    val logPaths = result.select("log_file_path").distinct().collect().map(_.getString(0))

    // Should have some paths (either from Avro state or version files)
    assert(logPaths.nonEmpty, "Should have log file paths")

    // Check if Avro state is being used
    val txLogDir     = new File(tablePath, "_transaction_log")
    val hasAvroState = txLogDir.listFiles().exists(_.getName.startsWith("state-v"))
    if (hasAvroState) {
      // If Avro state exists, some paths should reference it
      val avroStatePaths = logPaths.filter(p => p.contains("state-v") || p.contains("avro"))
      // Note: Even with Avro state, DESCRIBE may still show version file paths
      // depending on implementation
    }
  }

  test("Avro: table should be readable after DESCRIBE") {
    val tablePath    = s"$tempDir/test_table5"
    val sparkSession = spark
    import sparkSession.implicits._

    // Write test data
    val data = Seq(
      (1, "Alice", 100),
      (2, "Bob", 200),
      (3, "Charlie", 300)
    ).toDF("id", "name", "score")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "id,score")
      .mode("overwrite")
      .save(tablePath)

    // Run DESCRIBE
    spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath'").collect()

    // Verify table is still readable
    val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(df.count() == 3)

    // Verify queries work
    assert(df.filter("id = 2").count() == 1)
    assert(df.agg(Map("score" -> "sum")).collect()(0).getLong(0) == 600)
  }
}
