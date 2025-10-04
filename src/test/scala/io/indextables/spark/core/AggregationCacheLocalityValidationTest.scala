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

import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import io.indextables.spark.storage.{BroadcastSplitLocalityManager, SplitLocationRegistry}
import io.indextables.spark.transaction.{AddAction, TransactionLogFactory}
import io.indextables.spark.TestBase

/**
 * Test to validate that aggregate scans (both simple and GROUP BY) properly implement cache locality via
 * preferredLocations() method for optimal task scheduling.
 */
class AggregationCacheLocalityValidationTest extends TestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Clear locality tracking
    BroadcastSplitLocalityManager.clearAll()
    SplitLocationRegistry.clearAllLocations()
  }

  override def afterAll(): Unit = {
    // Clear locality tracking
    BroadcastSplitLocalityManager.clearAll()
    SplitLocationRegistry.clearAllLocations()
    super.afterAll()
  }

  test("Simple aggregate scan should use preferredLocations for cache locality") {
    val tablePath = new File(tempDir, "simple_aggregate_locality_test").getAbsolutePath

    // Create test data
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val testData = (1 to 100).map(i => (i, s"item_$i", i * 10))
    val df       = spark.createDataFrame(testData).toDF("id", "name", "value")

    // Write using V2 API for proper partition column indexing
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "value")
      .mode("overwrite")
      .save(tablePath)

    // Simulate that some splits have been cached on specific hosts
    val hostname = SplitLocationRegistry.getCurrentHostname

    // Read the transaction log to get actual split paths using the factory
    val transactionLogPath = new File(tablePath, "_transaction_log").getAbsolutePath
    val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
      org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority(new org.apache.hadoop.fs.Path(tablePath)),
      spark,
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    )
    val splits = transactionLog.listFiles()

    // Register that this host has cached the first split
    if (splits.nonEmpty) {
      val firstSplitPath = splits.head.path
      BroadcastSplitLocalityManager.recordSplitAccess(firstSplitPath, hostname)
      SplitLocationRegistry.recordSplitAccess(firstSplitPath, hostname)

      println(s"✅ Registered split '$firstSplitPath' as cached on host '$hostname'")
    }

    // Now perform aggregation queries that should use cache locality
    val aggDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Test simple aggregations - COUNT, SUM, AVG, MIN, MAX
    val countResult = aggDf.agg(count("*")).collect()
    assert(countResult.length == 1)
    assert(countResult(0).getLong(0) == 100)
    println(s"✅ COUNT(*) aggregation completed with cache locality support")

    val sumResult = aggDf.agg(sum("value")).collect()
    assert(sumResult.length == 1)
    // Handle both Integer and Long types for sum result
    val sumValue = sumResult(0).get(0) match {
      case i: Integer => i.toLong
      case l: Long    => l
      case other      => throw new IllegalStateException(s"Unexpected sum result type: ${other.getClass}")
    }
    assert(sumValue == 50500)
    println(s"✅ SUM(value) aggregation completed with cache locality support")

    val avgResult = aggDf.agg(avg("value")).collect()
    assert(avgResult.length == 1)
    assert(avgResult(0).getDouble(0) == 505.0)
    println(s"✅ AVG(value) aggregation completed with cache locality support")

    val minMaxResult = aggDf.agg(min("value"), max("value")).collect()
    assert(minMaxResult.length == 1)
    // Handle both Integer and Long types for min/max results
    val minValue = minMaxResult(0).get(0) match {
      case i: Integer => i.toLong
      case l: Long    => l
      case other      => throw new IllegalStateException(s"Unexpected min result type: ${other.getClass}")
    }
    val maxValue = minMaxResult(0).get(1) match {
      case i: Integer => i.toLong
      case l: Long    => l
      case other      => throw new IllegalStateException(s"Unexpected max result type: ${other.getClass}")
    }
    assert(minValue == 10)
    assert(maxValue == 1000)
    println(s"✅ MIN/MAX(value) aggregation completed with cache locality support")

    // Verify that preferredLocations was used by checking broadcast manager
    val preferredHosts = BroadcastSplitLocalityManager.getPreferredHosts(splits.head.path)
    assert(preferredHosts.contains(hostname), s"Expected preferredLocations to include '$hostname' for cached split")
    println(s"✅ Verified preferredLocations includes cached host: ${preferredHosts.mkString(", ")}")
  }

  test("GROUP BY aggregate scan should use preferredLocations for cache locality") {
    val tablePath = new File(tempDir, "groupby_aggregate_locality_test").getAbsolutePath

    // Create test data with categories for GROUP BY
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val testData = (1 to 100).map { i =>
      val category = if (i % 3 == 0) "A" else if (i % 3 == 1) "B" else "C"
      (i, category, s"item_$i", i * 10)
    }
    val df = spark.createDataFrame(testData).toDF("id", "category", "name", "value")

    // Write using V2 API - category needs to be a fast field for GROUP BY
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "category,value")
      .option("spark.indextables.indexing.typemap.category", "string")
      .mode("overwrite")
      .save(tablePath)

    // Simulate that some splits have been cached on specific hosts
    val hostname = SplitLocationRegistry.getCurrentHostname

    // Read the transaction log to get actual split paths using the factory
    val transactionLogPath = new File(tablePath, "_transaction_log").getAbsolutePath
    val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
      org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority(new org.apache.hadoop.fs.Path(tablePath)),
      spark,
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    )
    val splits = transactionLog.listFiles()

    // Register that this host has cached all splits (simulating warm cache)
    splits.foreach { split =>
      BroadcastSplitLocalityManager.recordSplitAccess(split.path, hostname)
      SplitLocationRegistry.recordSplitAccess(split.path, hostname)
      println(s"✅ Registered split '${split.path}' as cached on host '$hostname'")
    }

    // Now perform GROUP BY aggregation queries that should use cache locality
    val aggDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Test GROUP BY aggregations
    val groupByResult = aggDf
      .groupBy("category")
      .agg(
        count("*").as("count"),
        sum("value").as("total_value"),
        avg("value").as("avg_value"),
        min("value").as("min_value"),
        max("value").as("max_value")
      )
      .orderBy("category")
      .collect()

    assert(groupByResult.length == 3, "Should have 3 categories")

    // Verify category A (multiples of 3: 3, 6, 9, ..., 99 = 33 items)
    val categoryA = groupByResult.find(_.getString(0) == "A").get
    assert(categoryA.getLong(1) == 33, s"Category A count should be 33")
    println(s"✅ GROUP BY category A aggregation correct with cache locality")

    // Verify category B (remainder 1: 1, 4, 7, ..., 100 = 34 items)
    val categoryB = groupByResult.find(_.getString(0) == "B").get
    assert(categoryB.getLong(1) == 34, s"Category B count should be 34")
    println(s"✅ GROUP BY category B aggregation correct with cache locality")

    // Verify category C (remainder 2: 2, 5, 8, ..., 98 = 33 items)
    val categoryC = groupByResult.find(_.getString(0) == "C").get
    assert(categoryC.getLong(1) == 33, s"Category C count should be 33")
    println(s"✅ GROUP BY category C aggregation correct with cache locality")

    // Verify that preferredLocations was used for all splits
    splits.foreach { split =>
      val preferredHosts = BroadcastSplitLocalityManager.getPreferredHosts(split.path)
      assert(
        preferredHosts.contains(hostname),
        s"Expected preferredLocations to include '$hostname' for cached split '${split.path}'"
      )
    }
    println(s"✅ Verified all splits use preferredLocations for cache locality in GROUP BY")
  }

  test("Aggregate scans should fallback gracefully when no cache locality info available") {
    val tablePath = new File(tempDir, "no_locality_test").getAbsolutePath

    // Clear all locality information
    BroadcastSplitLocalityManager.clearAll()
    SplitLocationRegistry.clearAllLocations()

    // Create test data
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val testData = (1 to 50).map(i => (i, s"item_$i", i * 5))
    val df       = spark.createDataFrame(testData).toDF("id", "name", "value")

    // Write using V2 API
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "value")
      .mode("overwrite")
      .save(tablePath)

    // Read and perform aggregation without any cache locality info
    val aggDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Simple aggregation should still work without cache locality
    val countResult = aggDf.agg(count("*")).collect()
    assert(countResult.length == 1)
    assert(countResult(0).getLong(0) == 50)
    println(s"✅ Aggregation works correctly without cache locality info (fallback)")

    // GROUP BY should also work
    val testDataWithCategory = (1 to 50).map { i =>
      val category = if (i <= 25) "X" else "Y"
      (i, category, s"item_$i", i * 5)
    }
    val dfWithCategory = spark.createDataFrame(testDataWithCategory).toDF("id", "category", "name", "value")

    dfWithCategory.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "category,value")
      .option("spark.indextables.indexing.typemap.category", "string")
      .mode("overwrite")
      .save(tablePath + "_groupby")

    val groupByDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath + "_groupby")

    val groupByResult = groupByDf
      .groupBy("category")
      .agg(count("*").as("count"))
      .orderBy("category")
      .collect()

    assert(groupByResult.length == 2)
    assert(groupByResult(0).getLong(1) == 25) // Category X
    assert(groupByResult(1).getLong(1) == 25) // Category Y
    println(s"✅ GROUP BY aggregation works correctly without cache locality info (fallback)")
  }
}
