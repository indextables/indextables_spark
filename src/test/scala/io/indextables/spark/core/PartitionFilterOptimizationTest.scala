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

import io.indextables.spark.TestBase
import org.scalatest.BeforeAndAfterEach

/**
 * Tests for partition filter optimization.
 *
 * Partition filters are already handled by partition pruning and should NOT be
 * sent to Tantivy for query execution. This avoids unnecessary work in Tantivy,
 * especially for expensive range queries (>, <, between).
 *
 * These tests verify:
 * - Partition filters are excluded from Tantivy queries
 * - Non-partition filters are still pushed down to Tantivy
 * - Query results remain correct despite filter optimization
 */
class PartitionFilterOptimizationTest extends TestBase with BeforeAndAfterEach {

  test("should return correct results with partition equality filter") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data with partition column
    val df = Seq(
      (1, "alice", "2024-01-01"),
      (2, "bob", "2024-01-01"),
      (3, "charlie", "2024-01-02"),
      (4, "diana", "2024-01-02"),
      (5, "eve", "2024-01-03")
    ).toDF("id", "name", "date")

    val tablePath = s"$tempDir/partition_eq_test"

    // Write with partitioning
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .partitionBy("date")
      .save(tablePath)

    // Read back with partition filter
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      .filter($"date" === "2024-01-01")
      .select("id", "name")
      .orderBy("id")
      .collect()

    // Verify results - should only have records from 2024-01-01 partition
    result.length shouldBe 2
    result(0).getInt(0) shouldBe 1
    result(0).getString(1) shouldBe "alice"
    result(1).getInt(0) shouldBe 2
    result(1).getString(1) shouldBe "bob"
  }

  test("should return correct results with partition range filter") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data with partition column
    val df = Seq(
      (1, "alice", "2024-01"),
      (2, "bob", "2024-01"),
      (3, "charlie", "2024-02"),
      (4, "diana", "2024-02"),
      (5, "eve", "2024-03")
    ).toDF("id", "name", "month")

    val tablePath = s"$tempDir/partition_range_test"

    // Write with partitioning
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .partitionBy("month")
      .save(tablePath)

    // Read back with partition range filter (>, <, between)
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      .filter($"month" >= "2024-02")
      .select("id", "name")
      .orderBy("id")
      .collect()

    // Verify results - should have records from 2024-02 and 2024-03
    result.length shouldBe 3
    result(0).getInt(0) shouldBe 3
    result(1).getInt(0) shouldBe 4
    result(2).getInt(0) shouldBe 5
  }

  test("should handle mixed partition and non-partition filters") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data with partition column and data columns
    val df = Seq(
      (1, "alice", 100, "2024-01"),
      (2, "bob", 200, "2024-01"),
      (3, "charlie", 150, "2024-02"),
      (4, "diana", 300, "2024-02"),
      (5, "eve", 250, "2024-03")
    ).toDF("id", "name", "score", "month")

    val tablePath = s"$tempDir/mixed_filter_test"

    // Write with partitioning
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .partitionBy("month")
      .save(tablePath)

    // Read back with mixed filters: partition filter + non-partition filter
    // Partition filter (month === "2024-02") should be excluded from Tantivy
    // Non-partition filter (score > 200) should be sent to Tantivy
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      .filter($"month" === "2024-02" && $"score" > 200)
      .select("id", "name", "score")
      .collect()

    // Verify results - should only have diana (score 300 from month 2024-02)
    result.length shouldBe 1
    result(0).getInt(0) shouldBe 4
    result(0).getString(1) shouldBe "diana"
    result(0).getInt(2) shouldBe 300
  }

  test("should handle IN filter on partition column") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data
    val df = Seq(
      (1, "alice", "A"),
      (2, "bob", "B"),
      (3, "charlie", "C"),
      (4, "diana", "A"),
      (5, "eve", "B")
    ).toDF("id", "name", "category")

    val tablePath = s"$tempDir/partition_in_test"

    // Write with partitioning
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .partitionBy("category")
      .save(tablePath)

    // Read back with IN filter on partition column
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      .filter($"category".isin("A", "B"))
      .select("id", "name")
      .orderBy("id")
      .collect()

    // Verify results - should have records from categories A and B
    result.length shouldBe 4
    val ids = result.map(_.getInt(0)).toSet
    ids shouldBe Set(1, 2, 4, 5)
  }

  test("should work with non-partitioned table (no filter exclusion)") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data (no partitioning)
    val df = Seq(
      (1, "alice", 100),
      (2, "bob", 200),
      (3, "charlie", 150)
    ).toDF("id", "name", "score")

    val tablePath = s"$tempDir/non_partitioned_test"

    // Write WITHOUT partitioning
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Read back with filters - all should go to Tantivy
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      .filter($"score" >= 150)
      .select("id", "name")
      .orderBy("id")
      .collect()

    // Verify results
    result.length shouldBe 2
    result(0).getInt(0) shouldBe 2
    result(1).getInt(0) shouldBe 3
  }

  test("should handle complex filter with OR across partition and non-partition columns") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data
    val df = Seq(
      (1, "alice", 100, "A"),
      (2, "bob", 200, "B"),
      (3, "charlie", 300, "A"),
      (4, "diana", 150, "B"),
      (5, "eve", 250, "C")
    ).toDF("id", "name", "score", "category")

    val tablePath = s"$tempDir/complex_or_filter_test"

    // Write with partitioning
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .partitionBy("category")
      .save(tablePath)

    // OR filter that mixes partition and non-partition columns should NOT be excluded
    // because excluding it would be incorrect (score > 250 OR category = 'A')
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      .filter($"score" > 250 || $"category" === "A")
      .select("id", "name")
      .orderBy("id")
      .collect()

    // Verify results - should have alice (cat A), charlie (cat A, score 300)
    result.length shouldBe 2
    result(0).getInt(0) shouldBe 1  // alice (category A)
    result(1).getInt(0) shouldBe 3  // charlie (category A, and score > 250)
  }

  test("should handle aggregations with partition filter") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data
    val df = Seq(
      (1, "alice", 100, "2024-01"),
      (2, "bob", 200, "2024-01"),
      (3, "charlie", 300, "2024-02"),
      (4, "diana", 400, "2024-02")
    ).toDF("id", "name", "score", "month")

    val tablePath = s"$tempDir/agg_with_partition_filter_test"

    // Write with partitioning
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.indexing.fastfields", "score")
      .partitionBy("month")
      .save(tablePath)

    // Aggregate with partition filter
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      .filter($"month" === "2024-02")
      .agg(
        count("*").as("count"),
        sum("score").as("total"),
        avg("score").as("average")
      )
      .collect()

    // Verify aggregation results for 2024-02 partition
    result.length shouldBe 1
    result(0).getLong(0) shouldBe 2      // count
    result(0).getLong(1) shouldBe 700    // sum(300 + 400)
    result(0).getDouble(2) shouldBe 350.0 // avg
  }

  test("should handle between range filter on partition column") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data
    val df = Seq(
      (1, "alice", "2024-01-01"),
      (2, "bob", "2024-01-15"),
      (3, "charlie", "2024-02-01"),
      (4, "diana", "2024-02-15"),
      (5, "eve", "2024-03-01")
    ).toDF("id", "name", "date")

    val tablePath = s"$tempDir/partition_between_test"

    // Write with partitioning
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .partitionBy("date")
      .save(tablePath)

    // Between range filter on partition column
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      .filter($"date" >= "2024-01-15" && $"date" <= "2024-02-15")
      .select("id", "name")
      .orderBy("id")
      .collect()

    // Verify results - should have bob, charlie, diana
    result.length shouldBe 3
    result(0).getInt(0) shouldBe 2  // bob
    result(1).getInt(0) shouldBe 3  // charlie
    result(2).getInt(0) shouldBe 4  // diana
  }

  test("should return correct results with timestamp range filter within statistics bounds") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    import java.sql.Timestamp
    import java.time.Instant

    // Create test data with timestamps in a narrow range
    val df = Seq(
      (1, "alice", Timestamp.from(Instant.parse("2024-02-01T10:00:00.000Z"))),
      (2, "bob", Timestamp.from(Instant.parse("2024-02-15T12:00:00.000Z"))),
      (3, "charlie", Timestamp.from(Instant.parse("2024-03-01T14:00:00.000Z")))
    ).toDF("id", "name", "created_at")

    val tablePath = s"$tempDir/timestamp_stats_test"

    // Write - statistics will have min=2024-02-01, max=2024-03-01
    df.coalesce(1).write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Query with range filter that covers ALL data in the split
    // Filter: created_at >= 2024-01-01 (all data is after this)
    // Since splitMin (2024-02-01) > filterVal (2024-01-01), filter should be redundant
    val filterVal = Timestamp.from(Instant.parse("2024-01-01T00:00:00.000Z"))
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      .filter($"created_at" >= filterVal)
      .select("id", "name")
      .orderBy("id")
      .collect()

    // All 3 records should be returned
    result.length shouldBe 3
    result(0).getInt(0) shouldBe 1
    result(1).getInt(0) shouldBe 2
    result(2).getInt(0) shouldBe 3
  }

  test("should return correct results with date range filter within statistics bounds") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    import java.sql.Date

    // Create test data with dates
    val df = Seq(
      (1, "alice", Date.valueOf("2024-02-01")),
      (2, "bob", Date.valueOf("2024-02-15")),
      (3, "charlie", Date.valueOf("2024-03-01"))
    ).toDF("id", "name", "event_date")

    val tablePath = s"$tempDir/date_stats_test"

    // Write - statistics will have min=2024-02-01, max=2024-03-01
    df.coalesce(1).write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Query with range filter where max < filter value
    // Filter: event_date <= 2024-12-31 (all data is before this)
    // Since splitMax (2024-03-01) <= filterVal (2024-12-31), filter should be redundant
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      .filter($"event_date" <= Date.valueOf("2024-12-31"))
      .select("id", "name")
      .orderBy("id")
      .collect()

    // All 3 records should be returned
    result.length shouldBe 3
    result(0).getInt(0) shouldBe 1
    result(1).getInt(0) shouldBe 2
    result(2).getInt(0) shouldBe 3
  }

  test("should apply filter when statistics show data outside range") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    import java.sql.Timestamp
    import java.time.Instant

    // Create test data with timestamps
    val df = Seq(
      (1, "alice", Timestamp.from(Instant.parse("2024-01-01T10:00:00.000Z"))),
      (2, "bob", Timestamp.from(Instant.parse("2024-02-15T12:00:00.000Z"))),
      (3, "charlie", Timestamp.from(Instant.parse("2024-03-01T14:00:00.000Z")))
    ).toDF("id", "name", "created_at")

    val tablePath = s"$tempDir/timestamp_filter_needed_test"

    // Write
    df.coalesce(1).write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Query with range filter that excludes some data
    // Filter: created_at >= 2024-02-01 (some data is before this)
    // Since splitMin (2024-01-01) < filterVal (2024-02-01), filter should NOT be redundant
    val filterVal = Timestamp.from(Instant.parse("2024-02-01T00:00:00.000Z"))
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      .filter($"created_at" >= filterVal)
      .select("id", "name")
      .orderBy("id")
      .collect()

    // Only bob and charlie should be returned (alice is filtered out)
    result.length shouldBe 2
    result(0).getInt(0) shouldBe 2  // bob
    result(1).getInt(0) shouldBe 3  // charlie
  }

  test("should handle between range on timestamp with statistics optimization") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    import java.sql.Timestamp
    import java.time.Instant

    // Create test data with timestamps in a narrow range
    val df = Seq(
      (1, "alice", Timestamp.from(Instant.parse("2024-02-10T10:00:00.000Z"))),
      (2, "bob", Timestamp.from(Instant.parse("2024-02-15T12:00:00.000Z"))),
      (3, "charlie", Timestamp.from(Instant.parse("2024-02-20T14:00:00.000Z")))
    ).toDF("id", "name", "created_at")

    val tablePath = s"$tempDir/timestamp_between_stats_test"

    // Write - statistics will have min=2024-02-10, max=2024-02-20
    df.coalesce(1).write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Query with BETWEEN that covers ALL data
    // Filter: created_at BETWEEN 2024-01-01 AND 2024-12-31
    // All data is within this range, so both bounds should be redundant
    val startFilter = Timestamp.from(Instant.parse("2024-01-01T00:00:00.000Z"))
    val endFilter = Timestamp.from(Instant.parse("2024-12-31T23:59:59.000Z"))
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      .filter($"created_at" >= startFilter && $"created_at" <= endFilter)
      .select("id", "name")
      .orderBy("id")
      .collect()

    // All 3 records should be returned
    result.length shouldBe 3
    result(0).getInt(0) shouldBe 1
    result(1).getInt(0) shouldBe 2
    result(2).getInt(0) shouldBe 3
  }
}
