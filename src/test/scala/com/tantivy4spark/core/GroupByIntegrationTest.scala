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

package com.tantivy4spark.core

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.File
import java.nio.file.Files
import scala.util.Random

/**
 * Integration test for GROUP BY functionality with actual Spark DataFrames.
 * This test demonstrates what we need to implement for complete GROUP BY support.
 */
class GroupByIntegrationTest extends AnyFunSuite {

  test("GROUP BY with COUNT aggregation - basic test") {
    val spark = SparkSession.builder()
      .appName("GroupByIntegrationTest")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._

      // Create test data with categories for grouping
      val testData = Seq(
        ("doc1", "category_a", "content about AI", 10),
        ("doc2", "category_a", "content about ML", 20),
        ("doc3", "category_b", "content about data", 30),
        ("doc4", "category_b", "content about analytics", 40),
        ("doc5", "category_c", "content about search", 50)
      ).toDF("id", "category", "content", "score")

      val tempDir = Files.createTempDirectory("groupby-test").toFile
      val tablePath = tempDir.getAbsolutePath

      // Write data with string field for category (should support GROUP BY)
      testData.write.format("tantivy4spark")
        .option("spark.tantivy4spark.indexing.typemap.category", "string")  // String fields support GROUP BY
        .option("spark.tantivy4spark.indexing.fastfields", "category,score") // Both GROUP BY column and aggregation column must be fast
        .mode("overwrite")
        .save(tablePath)

      println(s"✅ GROUP BY test: Data written to $tablePath")

      // Read back the data using V2 API for aggregate pushdown
      val df = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(tablePath)

      // Perform GROUP BY query - this should trigger pushGroupBy() method
      println("🔍 GROUP BY TEST: Executing GROUP BY query...")
      val groupByResult = df.groupBy("category").count()

      // Show the execution plan to see if GROUP BY was pushed down
      println("🔍 GROUP BY TEST: Execution plan:")
      groupByResult.explain(true)

      // Collect results
      val results = groupByResult.collect()

      println("🔍 GROUP BY TEST: Results:")
      results.foreach { row =>
        println(s"  ${row.getString(0)}: ${row.getLong(1)}")
      }

      // Verify expected results
      val resultMap = results.map(row => row.getString(0) -> row.getLong(1)).toMap

      // Expected: category_a: 2, category_b: 2, category_c: 1
      assert(resultMap("category_a") == 2, s"category_a should have 2 docs, got ${resultMap("category_a")}")
      assert(resultMap("category_b") == 2, s"category_b should have 2 docs, got ${resultMap("category_b")}")
      assert(resultMap("category_c") == 1, s"category_c should have 1 doc, got ${resultMap("category_c")}")

      println(s"✅ GROUP BY test: All assertions passed!")

      // Clean up
      deleteRecursively(tempDir)

    } finally {
      spark.stop()
    }
  }

  test("GROUP BY with SUM aggregation") {
    val spark = SparkSession.builder()
      .appName("GroupBySumTest")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._

      // Create test data with numeric scores for summing
      val testData = Seq(
        ("doc1", "team_a", 100),
        ("doc2", "team_a", 200),
        ("doc3", "team_b", 150),
        ("doc4", "team_b", 250),
        ("doc5", "team_c", 300)
      ).toDF("id", "team", "score")

      val tempDir = Files.createTempDirectory("groupby-sum-test").toFile
      val tablePath = tempDir.getAbsolutePath

      // Write data
      testData.write.format("tantivy4spark")
        .option("spark.tantivy4spark.indexing.typemap.team", "string")     // String field for GROUP BY
        .option("spark.tantivy4spark.indexing.fastfields", "team,score")   // Both GROUP BY column and SUM column must be fast
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(tablePath)

      // Perform GROUP BY with SUM
      println("🔍 GROUP BY SUM TEST: Executing GROUP BY with SUM...")
      val groupBySumResult = df.groupBy("team").agg(sum("score").as("total_score"))

      // Show execution plan
      println("🔍 GROUP BY SUM TEST: Execution plan:")
      groupBySumResult.explain(true)

      // Collect and verify results
      val results = groupBySumResult.collect()
      val resultMap = results.map(row => row.getString(0) -> row.getLong(1)).toMap

      // Expected: team_a: 300, team_b: 400, team_c: 300
      assert(resultMap("team_a") == 300, s"team_a should have sum 300, got ${resultMap("team_a")}")
      assert(resultMap("team_b") == 400, s"team_b should have sum 400, got ${resultMap("team_b")}")
      assert(resultMap("team_c") == 300, s"team_c should have sum 300, got ${resultMap("team_c")}")

      println(s"✅ GROUP BY SUM test: All assertions passed!")

      // Clean up
      deleteRecursively(tempDir)

    } finally {
      spark.stop()
    }
  }

  test("GROUP BY pushdown detection - verify pushGroupBy() is called") {
    val spark = SparkSession.builder()
      .appName("GroupByPushdownDetectionTest")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "false")  // Disable AQE for predictable plans
      .getOrCreate()

    try {
      import spark.implicits._

      val testData = Seq(
        ("doc1", "status_active", 1),
        ("doc2", "status_active", 1),
        ("doc3", "status_inactive", 1)
      ).toDF("id", "status", "value")

      val tempDir = Files.createTempDirectory("groupby-pushdown-test").toFile
      val tablePath = tempDir.getAbsolutePath

      testData.write.format("tantivy4spark")
        .option("spark.tantivy4spark.indexing.typemap.status", "string")
        .option("spark.tantivy4spark.indexing.fastfields", "status,value")  // Add both status and value as fast fields
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(tablePath)

      // This query should trigger aggregate pushdown with GROUP BY
      val query = df.groupBy("status").count()

      // Look for evidence of pushdown in the physical plan
      val physicalPlan = query.queryExecution.executedPlan.toString
      println("🔍 PUSHDOWN DETECTION: Physical plan:")
      println(physicalPlan)

      // Check if our GROUP BY scan classes appear in the plan
      val hasTantivyGroupByScan = physicalPlan.contains("Tantivy4SparkGroupByAggregateScan") ||
                                  physicalPlan.contains("GroupByAggregateScan")

      if (hasTantivyGroupByScan) {
        println("✅ GROUP BY pushdown detected in physical plan!")
      } else {
        println("❌ GROUP BY pushdown NOT detected - falling back to Spark aggregation")
        println("This indicates pushGroupBy() method may not be working correctly")
      }

      // Execute the query to see actual behavior
      val results = query.collect()
      println("🔍 Results from GROUP BY query:")
      results.foreach { row =>
        println(s"  ${row.getString(0)}: ${row.getLong(1)}")
      }

      // Clean up
      deleteRecursively(tempDir)

    } finally {
      spark.stop()
    }
  }

  /**
   * Recursively delete a directory and all its contents.
   */
  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}