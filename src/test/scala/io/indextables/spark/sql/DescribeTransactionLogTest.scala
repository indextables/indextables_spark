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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.SparkSession
import java.io.File
import scala.util.Random

/**
 * Tests for DESCRIBE INDEXTABLES TRANSACTION LOG command.
 */
class DescribeTransactionLogTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: String = _

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .appName("DescribeTransactionLogTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.indextables.checkpoint.enabled", "true")
      .config("spark.indextables.checkpoint.interval", "10")
      .getOrCreate()

    tempDir = System.getProperty("java.io.tmpdir") + "/describe_txlog_test_" + Random.nextLong().abs
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

  test("DESCRIBE INDEXTABLES TRANSACTION LOG should return all actions") {
    val tablePath = s"$tempDir/test_table"
    val sparkSession = spark
    import sparkSession.implicits._

    // Write initial data
    val data1 = Seq((1, "one"), (2, "two")).toDF("id", "value")
    data1.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Write more data
    val data2 = Seq((3, "three"), (4, "four")).toDF("id", "value")
    data2.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("append")
      .save(tablePath)

    // Describe the transaction log
    val result = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath'")

    // Verify we got results
    assert(result.count() > 0, "Should have returned transaction log actions")

    // Check schema
    val expectedColumns = Seq(
      "version", "log_file_path", "action_type", "path", "partition_values",
      "size", "data_change", "tags"
    )
    expectedColumns.foreach { colName =>
      assert(result.columns.contains(colName), s"Result should contain column: $colName")
    }

    // Verify action types
    val actionTypes = result.select("action_type").distinct().collect().map(_.getString(0)).toSet
    assert(actionTypes.contains("protocol"), "Should have protocol actions")
    assert(actionTypes.contains("metadata"), "Should have metadata actions")
    assert(actionTypes.contains("add"), "Should have add actions")

    // Show results for manual inspection
    println("\n=== Transaction Log Contents ===")
    result.select("version", "action_type", "path", "size", "data_change").show(false)
  }

  test("DESCRIBE INDEXTABLES TRANSACTION LOG INCLUDE ALL should return all versions") {
    val tablePath = s"$tempDir/test_table2"
    val sparkSession = spark
    import sparkSession.implicits._

    // Create multiple versions
    (1 to 15).foreach { i =>
      val data = Seq((i, s"value_$i")).toDF("id", "value")
      data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Describe without INCLUDE ALL
    val resultWithoutAll = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath'")
    val countWithoutAll = resultWithoutAll.count()

    // Describe with INCLUDE ALL
    val resultWithAll = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' INCLUDE ALL")
    val countWithAll = resultWithAll.count()

    println(s"Without INCLUDE ALL: $countWithoutAll rows")
    println(s"With INCLUDE ALL: $countWithAll rows")

    // INCLUDE ALL should return more rows (or equal if no checkpoint)
    assert(countWithAll >= countWithoutAll,
      "INCLUDE ALL should return at least as many rows as without INCLUDE ALL")

    // Verify versions are present
    val versions = resultWithAll.select("version").distinct().collect().map(_.getLong(0)).sorted
    println(s"Versions found: ${versions.mkString(", ")}")
    assert(versions.contains(0L), "Should include version 0 (initial protocol/metadata)")
  }

  test("DESCRIBE should handle empty transaction log gracefully") {
    val tablePath = s"$tempDir/nonexistent_table"

    // Try to describe a non-existent table - should return empty results
    val result = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath'")
    val count = result.count()

    // Should return zero rows for non-existent table
    assert(count == 0, s"Expected 0 rows for non-existent table, got $count")
    println(s"Empty transaction log returned $count rows (expected 0)")
  }

  test("DESCRIBE should show RemoveAction after overwrite") {
    val tablePath = s"$tempDir/test_table3"
    val sparkSession = spark
    import sparkSession.implicits._

    // Write initial data
    val data1 = Seq((1, "one"), (2, "two")).toDF("id", "value")
    data1.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Overwrite with new data
    val data2 = Seq((10, "ten"), (20, "twenty")).toDF("id", "value")
    data2.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Describe with INCLUDE ALL to see all history
    val result = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' INCLUDE ALL")

    // Check for remove actions
    val actionTypes = result.select("action_type").collect().map(_.getString(0))
    val hasRemove = actionTypes.contains("remove")

    println(s"\n=== Actions after overwrite ===")
    result.select("version", "action_type", "path").show(false)

    // After overwrite, we should see REMOVE actions for old files
    assert(hasRemove, "Should have remove actions after overwrite")
  }
}
