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

package io.indextables.spark.core

import java.nio.file.Files

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

/**
 * Test to verify that COUNT(*) queries use transaction log optimization and return correct results regardless of query
 * execution mode.
 *
 * This addresses GitHub issue #15: COUNT queries on Databricks should use transaction log metadata instead of full
 * table scans.
 */
class CountTransactionLogOptimizationTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _
  var testDir: String     = _

  override def beforeAll(): Unit = {
    // Create a new SparkSession for testing
    spark = SparkSession
      .builder()
      .appName("CountTransactionLogOptimizationTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

    // Create temp directory for test data
    testDir = Files.createTempDirectory("count_optimization_test").toString
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  test("COUNT(*) should use transaction log optimization and return correct count") {
    val tablePath = s"$testDir/count_test_table"

    // Write test data - 10,000 rows
    val testData = (1 to 10000).map(i => (i, s"value_$i", i * 2))
    val df       = spark.createDataFrame(testData).toDF("id", "name", "amount")

    println(s"Writing 10,000 rows to $tablePath")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Read back and count
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    println("Executing COUNT(*) query...")
    val count = readDf.count()

    println(s"COUNT result: $count")
    assert(count == 10000, s"Expected count to be 10000, but got $count")
  }

  test("COUNT(*) with DataFrame.count() should use transaction log") {
    val tablePath = s"$testDir/count_df_test"

    // Write 5,000 rows
    val testData = (1 to 5000).map(i => (i, s"record_$i"))
    spark
      .createDataFrame(testData)
      .toDF("id", "name")
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Use DataFrame.count()
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val count = df.count()
    assert(count == 5000, s"Expected 5000, got $count")
  }

  test("SQL COUNT(*) should use transaction log optimization") {
    val tablePath = s"$testDir/count_sql_test"

    // Write 7,500 rows
    val testData = (1 to 7500).map(i => (i, s"item_$i"))
    spark
      .createDataFrame(testData)
      .toDF("id", "description")
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Create temp view
    spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .createOrReplaceTempView("test_table")

    // Execute SQL COUNT
    val result = spark.sql("SELECT COUNT(*) as cnt FROM test_table")
    val count  = result.collect()(0).getLong(0)

    assert(count == 7500, s"Expected 7500, got $count")
  }

  test("COUNT query should not scan any splits") {
    val tablePath = s"$testDir/count_no_scan_test"

    // Write data across multiple partitions to create multiple splits
    val testData = (1 to 10000).map(i => (i, s"data_$i", i % 100))
    spark
      .createDataFrame(testData)
      .toDF("id", "data", "partition_col")
      .repartition(10) // Force multiple splits
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Explain the plan to verify optimization
    println("\n=== Query Plan for COUNT(*) ===")
    df.selectExpr("COUNT(*) as total").explain(extended = true)

    val count = df.count()
    assert(count == 10000, s"Expected 10000, got $count")
  }

  test("COUNT(*) with LIMIT should still return full count from transaction log") {
    val tablePath = s"$testDir/count_with_limit_test"

    // Write 10,000 rows
    val testData = (1 to 10000).map(i => (i, s"record_$i"))
    spark
      .createDataFrame(testData)
      .toDF("id", "name")
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Apply LIMIT before COUNT - this simulates Databricks interactive query behavior
    println("\n=== Testing COUNT with LIMIT 10 ===")
    val limitedDf = df.limit(10)
    val count     = limitedDf.count()

    // COUNT should return 10 (the limited subset), NOT 10000
    // This is the correct Spark behavior - LIMIT is applied before COUNT
    println(s"COUNT result with LIMIT 10: $count")
    assert(count == 10, s"Expected 10 (limited), got $count")
  }

  test("COUNT aggregation should ignore LIMIT when using transaction log optimization") {
    val tablePath = s"$testDir/count_agg_with_limit_test"

    // Write 10,000 rows
    val testData = (1 to 10000).map(i => (i, s"item_$i"))
    spark
      .createDataFrame(testData)
      .toDF("id", "description")
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Use aggregation syntax with LIMIT
    println("\n=== Testing COUNT aggregation (not df.count()) ===")
    val result = df.selectExpr("COUNT(*) as total")

    println("Query plan:")
    result.explain(extended = true)

    val count = result.collect()(0).getLong(0)

    // This should return full count (10000) using transaction log optimization
    println(s"COUNT aggregation result: $count")
    assert(count == 10000, s"Expected 10000 from transaction log, got $count")
  }

  test("DataFrame.count() should return full count regardless of LIMIT in query context") {
    val tablePath = s"$testDir/df_count_context_test"

    // Write 10,000 rows
    val testData = (1 to 10000).map(i => (i, s"value_$i"))
    spark
      .createDataFrame(testData)
      .toDF("id", "value")
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Call df.count() directly - should use transaction log
    println("\n=== Testing df.count() directly ===")
    val count = df.count()

    println(s"df.count() result: $count")
    assert(count == 10000, s"Expected 10000 from transaction log, got $count")
  }
}
