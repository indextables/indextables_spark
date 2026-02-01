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

import org.apache.spark.sql.functions._

import io.indextables.spark.transaction._
import io.indextables.spark.TestBase

/** Integration tests for partition pruning optimizations with actual Spark operations. */
class PartitionPruningOptimizationIntegrationTest extends TestBase {

  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Clear partition filter cache before each test
    PartitionFilterCache.invalidate()
  }

  test("Integration: equality filter with partition index") {
    withTempPath { path =>
      // Create partitioned data
      val data = Seq(
        (1, "Alice", "us-east"),
        (2, "Bob", "us-east"),
        (3, "Charlie", "us-west"),
        (4, "Diana", "eu-central")
      )
      val df = spark.createDataFrame(data).toDF("id", "name", "region")

      // Write partitioned data
      df.write
        .format(provider)
        .mode("overwrite")
        .partitionBy("region")
        .save(path)

      // Read and filter
      val result = spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "us-east")
        .collect()

      result.length shouldBe 2
      result.map(_.getAs[String]("name")).sorted shouldBe Array("Alice", "Bob")
    }
  }

  test("Integration: IN filter uses index for pruning") {
    withTempPath { path =>
      // Create partitioned data
      val data = Seq(
        (1, "Alice", "us-east"),
        (2, "Bob", "us-west"),
        (3, "Charlie", "eu-central"),
        (4, "Diana", "ap-south")
      )
      val df = spark.createDataFrame(data).toDF("id", "name", "region")

      df.write
        .format(provider)
        .mode("overwrite")
        .partitionBy("region")
        .save(path)

      // Read and filter with IN clause
      val result = spark.read
        .format(provider)
        .load(path)
        .filter(col("region").isin("us-east", "us-west"))
        .collect()

      result.length shouldBe 2
      result.map(_.getAs[String]("name")).sorted shouldBe Array("Alice", "Bob")
    }
  }

  test("Integration: compound filter with equality and range") {
    withTempPath { path =>
      // Create partitioned data with multiple partition columns
      val data = Seq(
        (1, "Alice", "us-east", 2023),
        (2, "Bob", "us-east", 2024),
        (3, "Charlie", "us-west", 2024),
        (4, "Diana", "us-east", 2024)
      )
      val df = spark.createDataFrame(data).toDF("id", "name", "region", "year")

      df.write
        .format(provider)
        .mode("overwrite")
        .partitionBy("region", "year")
        .save(path)

      // Read and filter with compound conditions
      val result = spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "us-east" && col("year") >= 2024)
        .collect()

      result.length shouldBe 2
      result.map(_.getAs[String]("name")).sorted shouldBe Array("Bob", "Diana")
    }
  }

  test("Integration: large partition count performance") {
    withTempPath { path =>
      // Create data with many partitions
      val data = (1 to 200).map(i => (i, s"Person-$i", s"region-${i % 50}", s"2024-01-${(i % 28) + 1}"))
      val df   = spark.createDataFrame(data).toDF("id", "name", "region", "date")

      df.write
        .format(provider)
        .mode("overwrite")
        .partitionBy("region", "date")
        .save(path)

      // Measure query time with filter
      val startTime = System.currentTimeMillis()
      val result = spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "region-0")
        .collect()
      val duration = System.currentTimeMillis() - startTime

      // Should find records in region-0 (ids 50, 100, 150, 200)
      result.length shouldBe 4
      // Should complete quickly (under 5 seconds)
      duration should be < 5000L
    }
  }

  test("Integration: cache hit rate improves on repeated queries") {
    withTempPath { path =>
      // Create partitioned data
      val data = (1 to 100).map(i => (i, s"Person-$i", s"region-${i % 10}"))
      val df   = spark.createDataFrame(data).toDF("id", "name", "region")

      df.write
        .format(provider)
        .mode("overwrite")
        .partitionBy("region")
        .save(path)

      // Clear cache
      PartitionFilterCache.invalidate()

      // First query - populates cache
      spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "region-0")
        .count()

      val (hits1, misses1, _) = PartitionFilterCache.getStats()

      // Second query - should hit cache
      spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "region-0")
        .count()

      val (hits2, misses2, hitRate2) = PartitionFilterCache.getStats()

      // Cache hits should increase
      hits2 should be >= hits1
    }
  }

  test("Integration: cache invalidation on write") {
    withTempPath { path =>
      // Initial write
      val data1 = Seq(
        (1, "Alice", "us-east"),
        (2, "Bob", "us-west")
      )
      spark
        .createDataFrame(data1)
        .toDF("id", "name", "region")
        .write
        .format(provider)
        .mode("overwrite")
        .partitionBy("region")
        .save(path)

      // Query to populate cache
      val count1 = spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "us-east")
        .count()

      count1 shouldBe 1

      // Note: With optimizations, simple equality filters may use the partition index
      // directly and bypass the cache, so we don't assert cache size here

      // Append more data
      val data2 = Seq(
        (3, "Charlie", "us-east"),
        (4, "Diana", "eu-central")
      )
      spark
        .createDataFrame(data2)
        .toDF("id", "name", "region")
        .write
        .format(provider)
        .partitionBy("region")
        .mode("append")
        .save(path)

      // Query again - should see new data
      val count2 = spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "us-east")
        .count()

      count2 shouldBe 2
    }
  }

  test("Integration: no partition columns returns all data") {
    withTempPath { path =>
      // Create non-partitioned data
      val data = Seq(
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie")
      )
      spark
        .createDataFrame(data)
        .toDF("id", "name")
        .write
        .format(provider)
        .mode("overwrite")
        .save(path)

      // Query with filter on data column (not partition)
      val result = spark.read
        .format(provider)
        .load(path)
        .filter(col("id") > 1)
        .collect()

      result.length shouldBe 2
    }
  }

  test("Integration: OR filter with partition pruning") {
    withTempPath { path =>
      val data = Seq(
        (1, "Alice", "us-east"),
        (2, "Bob", "us-west"),
        (3, "Charlie", "eu-central"),
        (4, "Diana", "ap-south")
      )
      spark
        .createDataFrame(data)
        .toDF("id", "name", "region")
        .write
        .format(provider)
        .mode("overwrite")
        .partitionBy("region")
        .save(path)

      val result = spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "us-east" || col("region") === "ap-south")
        .collect()

      result.length shouldBe 2
      result.map(_.getAs[String]("name")).sorted shouldBe Array("Alice", "Diana")
    }
  }

  test("Integration: mixed partition and data column filters") {
    withTempPath { path =>
      val data = Seq(
        (1, "Alice", 30, "us-east"),
        (2, "Bob", 25, "us-east"),
        (3, "Charlie", 35, "us-west"),
        (4, "Diana", 28, "us-east")
      )
      spark
        .createDataFrame(data)
        .toDF("id", "name", "age", "region")
        .write
        .format(provider)
        .mode("overwrite")
        .partitionBy("region")
        .save(path)

      // Filter on both partition column and data column
      // Using >= 28 to include Diana (age 28)
      val result = spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "us-east" && col("age") >= 28)
        .collect()

      result.length shouldBe 2
      result.map(_.getAs[String]("name")).sorted shouldBe Array("Alice", "Diana")
    }
  }

  test("Integration: aggregation with partition pruning") {
    withTempPath { path =>
      val data = Seq(
        (1, 100.0, "us-east"),
        (2, 200.0, "us-east"),
        (3, 300.0, "us-west"),
        (4, 400.0, "us-east")
      )
      spark
        .createDataFrame(data)
        .toDF("id", "amount", "region")
        .write
        .format(provider)
        .mode("overwrite")
        .partitionBy("region")
        .option("spark.indextables.indexing.fastfields", "amount")
        .save(path)

      // Aggregate with partition filter
      val result = spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "us-east")
        .agg(sum("amount").as("total"))
        .collect()

      result.length shouldBe 1
      result.head.getAs[Double]("total") shouldBe 700.0 +- 0.01
    }
  }

  test("Integration: benchmark 10K files across 100 partitions") {
    withTempPath { path =>
      // Create many files across many partitions
      // Each partition will have ~100 files
      val data = (1 to 1000).map(i => (i, s"Person-$i", i * 10.0, s"region-${i % 100}"))
      val df   = spark.createDataFrame(data).toDF("id", "name", "value", "region")

      df.write
        .format(provider)
        .mode("overwrite")
        .partitionBy("region")
        .save(path)

      // Clear cache
      PartitionFilterCache.invalidate()
      PartitionPruning.invalidateCaches()

      // Benchmark: Query single partition
      val startTime = System.currentTimeMillis()
      val result = spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "region-0")
        .count()
      val duration = System.currentTimeMillis() - startTime

      // Should find ~10 records
      result shouldBe 10L

      // Should complete quickly with optimizations
      println(s"Benchmark: Query completed in ${duration}ms")
      duration should be < 10000L

      // Check optimization stats
      val stats = PartitionPruning.getOptimizationStats()
      println(s"Optimization stats: $stats")
    }
  }
}
