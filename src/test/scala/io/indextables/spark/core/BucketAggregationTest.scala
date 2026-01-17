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
import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for bucket aggregation functions (DateHistogram, Histogram, Range). These functions enable time-series analysis
 * and numeric distribution analysis using tantivy4java's bucket aggregation capabilities.
 */
class BucketAggregationTest extends AnyFunSuite with Matchers {

  def createTestSession(): SparkSession =
    SparkSession
      .builder()
      .appName("BucketAggregationTest")
      .master("local[*]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

  test("Histogram aggregation should bucket numeric values with COUNT") {
    val spark = createTestSession()

    try {
      import spark.implicits._

      // Create test data with numeric values spanning multiple buckets
      val testData = Seq(
        ("prod1", 15.0, 10),
        ("prod2", 25.0, 20),
        ("prod3", 35.0, 30),
        ("prod4", 55.0, 40),
        ("prod5", 75.0, 50),
        ("prod6", 85.0, 60),
        ("prod7", 95.0, 70),
        ("prod8", 150.0, 80)
      ).toDF("name", "price", "quantity")

      val tempDir   = Files.createTempDirectory("histogram-test").toFile
      val tablePath = tempDir.getAbsolutePath

      // Write data with fast fields for aggregation
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "price,quantity")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      println(s"Histogram Test: Data written to $tablePath")

      // Read back the data
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // Test histogram aggregation with interval of 50
      // Expected buckets: 0-50 (4 items), 50-100 (3 items), 100-150 (0 items), 150-200 (1 item)
      println("Histogram Test: Executing histogram aggregation with interval=50...")

      // Register the DataFrame as a temp view for SQL access
      df.createOrReplaceTempView("products")

      val histResult = spark.sql(
        """
          |SELECT indextables_histogram(price, 50.0) as price_bucket, COUNT(*) as cnt
          |FROM products
          |GROUP BY indextables_histogram(price, 50.0)
          |ORDER BY price_bucket
          |""".stripMargin
      )

      println("Histogram Test: Results:")
      histResult.show()

      // Verify results
      val rows = histResult.collect()
      rows.length should be > 0

      // Verify bucket contents
      // Expected: 0.0-50.0 has prices 15, 25, 35 (3 items)
      //           50.0-100.0 has prices 55, 75, 85, 95 (4 items)
      //           150.0-200.0 has price 150 (1 item)
      val bucket0   = rows.find(_.getAs[Double]("price_bucket") == 0.0)
      val bucket50  = rows.find(_.getAs[Double]("price_bucket") == 50.0)
      val bucket150 = rows.find(_.getAs[Double]("price_bucket") == 150.0)

      bucket0 shouldBe defined
      bucket0.get.getAs[Long]("cnt") shouldBe 3

      bucket50 shouldBe defined
      bucket50.get.getAs[Long]("cnt") shouldBe 4

      bucket150 shouldBe defined
      bucket150.get.getAs[Long]("cnt") shouldBe 1

      println(s"Histogram Test: Found ${rows.length} buckets with correct counts")

      // Clean up
      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }

  test("DateHistogram aggregation should bucket timestamps with COUNT") {
    val spark = createTestSession()

    try {
      import spark.implicits._

      // Create test data with timestamps spanning multiple days
      val baseTime = System.currentTimeMillis()
      val dayMs    = 24 * 60 * 60 * 1000L

      val testData = Seq(
        ("event1", new Timestamp(baseTime), 100),
        ("event2", new Timestamp(baseTime + 1000), 200),
        ("event3", new Timestamp(baseTime + dayMs), 300),
        ("event4", new Timestamp(baseTime + dayMs + 1000), 400),
        ("event5", new Timestamp(baseTime + 2 * dayMs), 500),
        ("event6", new Timestamp(baseTime + 3 * dayMs), 600)
      ).toDF("name", "event_time", "value")

      val tempDir   = Files.createTempDirectory("date-histogram-test").toFile
      val tablePath = tempDir.getAbsolutePath

      // Write data with fast fields
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "event_time,value")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      println(s"DateHistogram Test: Data written to $tablePath")

      // Read back the data
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // Test date histogram aggregation with 1-day interval
      println("DateHistogram Test: Executing date histogram aggregation with interval='1d'...")

      // Register the DataFrame as a temp view for SQL access
      df.createOrReplaceTempView("events")

      val dateHistResult = spark.sql(
        """
          |SELECT indextables_date_histogram(event_time, '1d') as day_bucket, COUNT(*) as cnt
          |FROM events
          |GROUP BY indextables_date_histogram(event_time, '1d')
          |ORDER BY day_bucket
          |""".stripMargin
      )

      println("DateHistogram Test: Results:")
      dateHistResult.show()

      // Verify results
      val rows = dateHistResult.collect()
      rows.length should be > 0

      println(s"DateHistogram Test: Found ${rows.length} daily buckets")

      // Clean up
      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }

  test("Range aggregation should create custom buckets with COUNT") {
    val spark = createTestSession()

    try {
      import spark.implicits._

      // Create test data with numeric values
      val testData = Seq(
        ("item1", 10.0),
        ("item2", 25.0),
        ("item3", 35.0),
        ("item4", 75.0),
        ("item5", 150.0),
        ("item6", 250.0)
      ).toDF("name", "price")

      val tempDir   = Files.createTempDirectory("range-test").toFile
      val tablePath = tempDir.getAbsolutePath

      // Write data with fast fields
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "price")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      println(s"Range Test: Data written to $tablePath")

      // Read back the data
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // Test range aggregation with custom buckets:
      // cheap: < 50, mid: 50-100, expensive: >= 100
      println("Range Test: Executing range aggregation...")

      // Register the DataFrame as a temp view for SQL access
      df.createOrReplaceTempView("items")

      val rangeResult = spark.sql(
        """
          |SELECT
          |  indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL) as price_tier,
          |  COUNT(*) as cnt
          |FROM items
          |GROUP BY indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL)
          |ORDER BY price_tier
          |""".stripMargin
      )

      println("Range Test: Results:")
      rangeResult.show()

      // Verify results
      val rows = rangeResult.collect()
      rows.length should be > 0

      println(s"Range Test: Found ${rows.length} range buckets")

      // Clean up
      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }

  test("Histogram aggregation with SUM sub-aggregation should work") {
    val spark = createTestSession()

    try {
      import spark.implicits._

      // Create test data with numeric values
      val testData = Seq(
        ("prod1", 15.0, 10),
        ("prod2", 25.0, 20),
        ("prod3", 35.0, 30),
        ("prod4", 55.0, 40),
        ("prod5", 75.0, 50)
      ).toDF("name", "price", "quantity")

      val tempDir   = Files.createTempDirectory("histogram-sum-test").toFile
      val tablePath = tempDir.getAbsolutePath

      // Write data with fast fields
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "price,quantity")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      println(s"Histogram+SUM Test: Data written to $tablePath")

      // Read back the data
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // Test histogram aggregation with SUM sub-aggregation
      println("Histogram+SUM Test: Executing histogram aggregation with SUM...")

      // Register the DataFrame as a temp view for SQL access
      df.createOrReplaceTempView("products_sum")

      val histResult = spark.sql(
        """
          |SELECT indextables_histogram(price, 50.0) as price_bucket, COUNT(*) as cnt, SUM(quantity) as total_qty
          |FROM products_sum
          |GROUP BY indextables_histogram(price, 50.0)
          |ORDER BY price_bucket
          |""".stripMargin
      )

      println("Histogram+SUM Test: Results:")
      histResult.show()

      // Verify results
      val rows = histResult.collect()
      rows.length should be > 0

      // Verify bucket contents with SUM
      // Expected: 0.0-50.0 has prices 15, 25, 35 with quantities 10, 20, 30 (sum=60)
      //           50.0-100.0 has prices 55, 75 with quantities 40, 50 (sum=90)
      val bucket0  = rows.find(_.getAs[Double]("price_bucket") == 0.0)
      val bucket50 = rows.find(_.getAs[Double]("price_bucket") == 50.0)

      bucket0 shouldBe defined
      bucket0.get.getAs[Long]("cnt") shouldBe 3
      bucket0.get.getAs[Long]("total_qty") shouldBe 60

      bucket50 shouldBe defined
      bucket50.get.getAs[Long]("cnt") shouldBe 2
      bucket50.get.getAs[Long]("total_qty") shouldBe 90

      println(s"Histogram+SUM Test: Found ${rows.length} buckets with correct SUM values")

      // Clean up
      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}
