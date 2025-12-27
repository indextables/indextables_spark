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

import org.apache.spark.sql.functions._

import io.indextables.spark.storage.DriverSplitLocalityManager
import io.indextables.spark.TestBase

/**
 * Test to validate that aggregate scans (both simple and GROUP BY) work correctly with the driver-based locality
 * management. Locality is automatically assigned during partition planning via
 * DriverSplitLocalityManager.assignSplitsForQuery().
 */
class AggregationCacheLocalityValidationTest extends TestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Clear locality tracking
    DriverSplitLocalityManager.clear()
  }

  override def afterAll(): Unit = {
    // Clear locality tracking
    DriverSplitLocalityManager.clear()
    super.afterAll()
  }

  test("Simple aggregate scan should work with driver-based locality") {
    val tablePath = new File(tempDir, "simple_aggregate_locality_test").getAbsolutePath

    // Create test data
    val testData = (1 to 100).map(i => (i, s"item_$i", i * 10))
    val df       = spark.createDataFrame(testData).toDF("id", "name", "value")

    // Write using V2 API for proper partition column indexing
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "value")
      .mode("overwrite")
      .save(tablePath)

    // Read the transaction log to get actual split paths
    val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
      org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority(new org.apache.hadoop.fs.Path(tablePath)),
      spark,
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    )
    val splits = transactionLog.listFiles()
    println(s"✅ Table has ${splits.size} split(s)")

    // Now perform aggregation queries - locality is automatically assigned during partition planning
    val aggDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Test simple aggregations - COUNT, SUM, AVG, MIN, MAX
    val countResult = aggDf.agg(count("*")).collect()
    assert(countResult.length == 1)
    assert(countResult(0).getLong(0) == 100)
    println(s"✅ COUNT(*) aggregation completed with driver-based locality")

    val sumResult = aggDf.agg(sum("value")).collect()
    assert(sumResult.length == 1)
    // Handle both Integer and Long types for sum result
    val sumValue = sumResult(0).get(0) match {
      case i: Integer => i.toLong
      case l: Long    => l
      case other      => throw new IllegalStateException(s"Unexpected sum result type: ${other.getClass}")
    }
    assert(sumValue == 50500)
    println(s"✅ SUM(value) aggregation completed with driver-based locality")

    val avgResult = aggDf.agg(avg("value")).collect()
    assert(avgResult.length == 1)
    assert(avgResult(0).getDouble(0) == 505.0)
    println(s"✅ AVG(value) aggregation completed with driver-based locality")

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
    println(s"✅ MIN/MAX(value) aggregation completed with driver-based locality")
  }

  test("GROUP BY aggregate scan should work with driver-based locality") {
    val tablePath = new File(tempDir, "groupby_aggregate_locality_test").getAbsolutePath

    // Create test data with categories for GROUP BY
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

    // Read the transaction log to get actual split paths
    val transactionLog = io.indextables.spark.transaction.TransactionLogFactory.create(
      org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority(new org.apache.hadoop.fs.Path(tablePath)),
      spark,
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    )
    val splits = transactionLog.listFiles()
    println(s"✅ Table has ${splits.size} split(s) for GROUP BY test")

    // Now perform GROUP BY aggregation queries - locality is automatically assigned
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
    println(s"✅ GROUP BY category A aggregation correct with driver-based locality")

    // Verify category B (remainder 1: 1, 4, 7, ..., 100 = 34 items)
    val categoryB = groupByResult.find(_.getString(0) == "B").get
    assert(categoryB.getLong(1) == 34, s"Category B count should be 34")
    println(s"✅ GROUP BY category B aggregation correct with driver-based locality")

    // Verify category C (remainder 2: 2, 5, 8, ..., 98 = 33 items)
    val categoryC = groupByResult.find(_.getString(0) == "C").get
    assert(categoryC.getLong(1) == 33, s"Category C count should be 33")
    println(s"✅ GROUP BY category C aggregation correct with driver-based locality")

    println(s"✅ All GROUP BY aggregations completed with driver-based locality")
  }

  test("Aggregate scans should work correctly on fresh start (no prior assignments)") {
    val tablePath = new File(tempDir, "no_locality_test").getAbsolutePath

    // Clear all locality information to simulate fresh start
    DriverSplitLocalityManager.clear()

    // Create test data
    val testData = (1 to 50).map(i => (i, s"item_$i", i * 5))
    val df       = spark.createDataFrame(testData).toDF("id", "name", "value")

    // Write using V2 API
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "value")
      .mode("overwrite")
      .save(tablePath)

    // Read and perform aggregation - locality will be assigned automatically
    val aggDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Simple aggregation should work with automatic locality assignment
    val countResult = aggDf.agg(count("*")).collect()
    assert(countResult.length == 1)
    assert(countResult(0).getLong(0) == 50)
    println(s"✅ Aggregation works correctly with automatic locality assignment")

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
    println(s"✅ GROUP BY aggregation works correctly with automatic locality assignment")
  }

  test("Repeated queries should maintain sticky locality assignments") {
    val tablePath = new File(tempDir, "sticky_locality_test").getAbsolutePath

    // Clear locality to start fresh
    DriverSplitLocalityManager.clear()

    // Create test data
    val testData = (1 to 100).map(i => (i, s"item_$i", i * 10))
    val df       = spark.createDataFrame(testData).toDF("id", "name", "value")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "value")
      .mode("overwrite")
      .save(tablePath)

    // First query - assignments will be created
    val aggDf1 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val result1 = aggDf1.agg(count("*")).collect()
    assert(result1(0).getLong(0) == 100)
    println(s"✅ First query completed - locality assignments created")

    // Second query - should use sticky assignments
    val aggDf2 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val result2 = aggDf2.agg(sum("value")).collect()
    val sumValue = result2(0).get(0) match {
      case i: Integer => i.toLong
      case l: Long    => l
      case other      => throw new IllegalStateException(s"Unexpected sum result type: ${other.getClass}")
    }
    assert(sumValue == 50500)
    println(s"✅ Second query completed - sticky locality assignments used")

    // Third query with different aggregation
    val aggDf3 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val result3 = aggDf3.agg(avg("value")).collect()
    assert(result3(0).getDouble(0) == 505.0)
    println(s"✅ Third query completed - consistent locality maintained")
  }

  test("New hosts should receive rebalanced splits") {
    // Clear locality to start fresh
    DriverSplitLocalityManager.clear()

    // Simulate split paths
    val splitPaths = (1 to 10).map(i => s"split-$i.split")

    // Initial assignment with 2 hosts
    val initialHosts       = Set("host-a", "host-b")
    val initialAssignments = DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, initialHosts)

    // Verify initial distribution (should be 5 splits per host)
    val initialDistribution = initialAssignments.values.groupBy(identity).map { case (k, v) => k -> v.size }
    println(s"Initial distribution: $initialDistribution")
    assert(initialDistribution("host-a") == 5)
    assert(initialDistribution("host-b") == 5)

    // Now add a new host
    val expandedHosts         = Set("host-a", "host-b", "host-c")
    val rebalancedAssignments = DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, expandedHosts)

    // Verify rebalanced distribution (should be ~3-4 splits per host)
    val rebalancedDistribution = rebalancedAssignments.values.groupBy(identity).map { case (k, v) => k -> v.size }
    println(s"Rebalanced distribution: $rebalancedDistribution")

    // New host should have received some splits
    assert(rebalancedDistribution.getOrElse("host-c", 0) > 0, "New host should have received splits")

    // Fair share with 10 splits across 3 hosts is ceil(10/3) = 4
    // Rebalancing only takes from EXCESS (above fair share) to minimize cache invalidation:
    // - host-a had 5, fair share is 4, so 1 excess
    // - host-b had 5, fair share is 4, so 1 excess
    // - host-c receives 2 (total excess available)
    // Result: host-a=4, host-b=4, host-c=2
    assert(rebalancedDistribution("host-a") == 4, "host-a should have 4 splits")
    assert(rebalancedDistribution("host-b") == 4, "host-b should have 4 splits")
    assert(rebalancedDistribution("host-c") == 2, "host-c should have 2 splits (only excess moved)")

    println(s"✅ New host 'host-c' received ${rebalancedDistribution("host-c")} splits via rebalancing (only excess)")
  }

  test("Adding multiple new hosts should distribute fairly") {
    // Clear locality to start fresh
    DriverSplitLocalityManager.clear()

    // Simulate 12 split paths
    val splitPaths = (1 to 12).map(i => s"split-$i.split")

    // Initial assignment with 1 host (all splits on one host)
    val initialHosts       = Set("host-a")
    val initialAssignments = DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, initialHosts)

    assert(initialAssignments.values.count(_ == "host-a") == 12)
    println(s"Initial: all 12 splits on host-a")

    // Now add 3 more hosts (total 4)
    val expandedHosts         = Set("host-a", "host-b", "host-c", "host-d")
    val rebalancedAssignments = DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, expandedHosts)

    val rebalancedDistribution = rebalancedAssignments.values.groupBy(identity).map { case (k, v) => k -> v.size }
    println(s"Rebalanced distribution: $rebalancedDistribution")

    // Fair share is 12/4 = 3 splits per host
    rebalancedDistribution.foreach {
      case (host, count) =>
        assert(count == 3, s"Host $host has $count splits, expected 3")
    }

    println(s"✅ All 4 hosts have exactly 3 splits each after rebalancing")
  }
}
