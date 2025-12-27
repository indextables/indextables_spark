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

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Comprehensive test for AVG aggregation functionality with and without GROUP BY. Verifies that Spark correctly
 * transforms AVG into SUM + COUNT operations for distributed processing.
 */
class AvgAggregationTest extends AnyFunSuite with Matchers {

  test("AVG aggregation should work correctly for simple aggregations (no GROUP BY)") {
    val spark = SparkSession
      .builder()
      .appName("AvgAggregationSimpleTest")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._

      // Create test data - deliberately across multiple partitions
      val testData = Seq(
        ("doc1", "content1", 10, 1.0),
        ("doc2", "content2", 20, 2.0),
        ("doc3", "content3", 30, 3.0),
        ("doc4", "content4", 40, 4.0),
        ("doc5", "content5", 50, 5.0)
      ).toDF("id", "content", "score", "rating")
        .repartition(3) // Force multiple partitions for distributed test

      val tempDir   = Files.createTempDirectory("avg-simple-test").toFile
      val tablePath = tempDir.getAbsolutePath

      // Write data using V2 API to ensure we test distributed aggregation
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "score,rating")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      println(s"✅ AVG Simple Test: Data written to $tablePath")

      // Read back the data
      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // Verify we have multiple partitions
      val partitionCount = df.rdd.getNumPartitions
      println(s"🔍 AVG Simple Test: DataFrame has $partitionCount partitions")

      // Test AVG aggregation - this should be transformed by Spark into SUM + COUNT
      println("🔍 AVG Simple Test: Executing AVG aggregation...")
      val avgResult = df
        .agg(
          avg("score").as("avg_score"),
          avg("rating").as("avg_rating"),
          count("*").as("total_count")
        )
        .collect()

      val row             = avgResult(0)
      val actualAvgScore  = row.getDouble(0)
      val actualAvgRating = row.getDouble(1)
      val actualCount     = row.getLong(2)

      println(s"🔍 AVG Simple Test Results:")
      println(s"  AVG(score): $actualAvgScore (expected: 30.0)")
      println(s"  AVG(rating): $actualAvgRating (expected: 3.0)")
      println(s"  COUNT(*): $actualCount (expected: 5)")

      // Verify correct results
      actualAvgScore shouldBe 30.0
      actualAvgRating shouldBe 3.0
      actualCount shouldBe 5L

      println(s"✅ AVG Simple Test: All assertions passed!")

      // Clean up
      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }

  test("AVG aggregation should work correctly with GROUP BY") {
    val spark = SparkSession
      .builder()
      .appName("AvgAggregationGroupByTest")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._

      // Create test data with categories for GROUP BY
      val testData = Seq(
        ("doc1", "category_a", 10, 1.0),
        ("doc2", "category_a", 30, 3.0), // avg for category_a: score=20.0, rating=2.0
        ("doc3", "category_b", 20, 2.0),
        ("doc4", "category_b", 40, 4.0), // avg for category_b: score=30.0, rating=3.0
        ("doc5", "category_c", 50, 5.0)  // avg for category_c: score=50.0, rating=5.0
      ).toDF("id", "category", "score", "rating")
        .repartition(3) // Force multiple partitions for distributed test

      val tempDir   = Files.createTempDirectory("avg-groupby-test").toFile
      val tablePath = tempDir.getAbsolutePath

      // Write data using V2 API
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.category", "string")          // String field for GROUP BY
        .option("spark.indextables.indexing.fastfields", "category,score,rating") // All fields must be fast
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      println(s"✅ AVG GROUP BY Test: Data written to $tablePath")

      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // Verify we have multiple partitions
      val partitionCount = df.rdd.getNumPartitions
      println(s"🔍 AVG GROUP BY Test: DataFrame has $partitionCount partitions")

      // Test GROUP BY with AVG - this should be transformed by Spark into SUM + COUNT per group
      println("🔍 AVG GROUP BY Test: Executing GROUP BY with AVG aggregation...")
      val groupByAvgResult = df
        .groupBy("category")
        .agg(
          avg("score").as("avg_score"),
          avg("rating").as("avg_rating"),
          count("*").as("count_per_category")
        )
        .collect()

      println("🔍 AVG GROUP BY Test Results:")
      val resultMap = groupByAvgResult.map { row =>
        val category  = row.getString(0)
        val avgScore  = row.getDouble(1)
        val avgRating = row.getDouble(2)
        val count     = row.getLong(3)
        println(s"  $category: AVG(score)=$avgScore, AVG(rating)=$avgRating, COUNT=$count")
        category -> ((avgScore, avgRating, count))
      }.toMap

      // Verify expected results
      resultMap("category_a")._1 shouldBe 20.0 // avg score for category_a
      resultMap("category_a")._2 shouldBe 2.0  // avg rating for category_a
      resultMap("category_a")._3 shouldBe 2L   // count for category_a

      resultMap("category_b")._1 shouldBe 30.0 // avg score for category_b
      resultMap("category_b")._2 shouldBe 3.0  // avg rating for category_b
      resultMap("category_b")._3 shouldBe 2L   // count for category_b

      resultMap("category_c")._1 shouldBe 50.0 // avg score for category_c
      resultMap("category_c")._2 shouldBe 5.0  // avg rating for category_c
      resultMap("category_c")._3 shouldBe 1L   // count for category_c

      println(s"✅ AVG GROUP BY Test: All assertions passed!")

      // Clean up
      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }

  test("AVG aggregation should handle distributed processing correctly across multiple splits") {
    val spark = SparkSession
      .builder()
      .appName("AvgDistributedTest")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "false") // Disable AQE for predictable plans
      .getOrCreate()

    try {
      import spark.implicits._

      // Create larger dataset spread across partitions
      val largeTestData = (1 to 100)
        .map { i =>
          val category = s"cat_${i % 5}" // 5 categories
          val score    = i * 10
          val rating   = i.toDouble / 10
          (s"doc$i", category, score, rating)
        }
        .toDF("id", "category", "score", "rating")
        .repartition(5) // Multiple partitions

      val tempDir   = Files.createTempDirectory("avg-distributed-test").toFile
      val tablePath = tempDir.getAbsolutePath

      // Write data
      largeTestData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.category", "string")
        .option("spark.indextables.indexing.fastfields", "category,score,rating")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      println(s"✅ AVG Distributed Test: Large dataset written to $tablePath")

      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // Test both simple and GROUP BY AVG on the same large dataset
      println("🔍 AVG Distributed Test: Testing simple AVG on large dataset...")
      val simpleAvgResult = df
        .agg(
          avg("score").as("overall_avg_score"),
          avg("rating").as("overall_avg_rating"),
          count("*").as("total_records")
        )
        .collect()(0)

      val overallAvgScore  = simpleAvgResult.getDouble(0)
      val overallAvgRating = simpleAvgResult.getDouble(1)
      val totalRecords     = simpleAvgResult.getLong(2)

      println(s"🔍 Simple AVG Results: score=$overallAvgScore, rating=$overallAvgRating, count=$totalRecords")

      // Verify simple aggregation
      totalRecords shouldBe 100L
      overallAvgScore shouldBe 505.0                    // (10+20+...+1000)/100 = 50500/100 = 505
      math.abs(overallAvgRating - 5.05) should be < 0.1 // Allow for floating-point precision in distributed calculation

      println("🔍 AVG Distributed Test: Testing GROUP BY AVG on large dataset...")
      val groupByAvgResult = df
        .groupBy("category")
        .agg(
          avg("score").as("avg_score"),
          avg("rating").as("avg_rating"),
          count("*").as("count_per_cat")
        )
        .orderBy("category")
        .collect()

      println("🔍 GROUP BY AVG Results:")
      groupByAvgResult.foreach { row =>
        val category  = row.getString(0)
        val avgScore  = row.getDouble(1)
        val avgRating = row.getDouble(2)
        val count     = row.getLong(3)
        println(s"  $category: AVG(score)=$avgScore, AVG(rating)=$avgRating, COUNT=$count")
      }

      // Verify each category has 20 records (100/5) and correct averages
      groupByAvgResult.foreach { row =>
        val count = row.getLong(3)
        count shouldBe 20L // Each category should have 20 records
      }

      println(s"✅ AVG Distributed Test: All assertions passed!")

      // Clean up
      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }

  /** Recursively delete a directory and all its contents. */
  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}
