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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.functions._

import io.indextables.spark.TestBase
import org.scalatest.matchers.should.Matchers._

/**
 * Test suite for GROUP BY queries on tables with Date/Timestamp partition columns.
 *
 * This tests the fix for ClassCastException that occurred when running GROUP BY queries
 * on tables with DateType or TimestampType partition columns.
 *
 * Root cause: Missing type conversion in IndexTables4SparkGroupByAggregateScan and
 * TransactionLogCountScan - values fell through to default case returning UTF8String,
 * but the schema declared the proper type (DateType expects Int, TimestampType expects Long).
 */
class PartitionColumnGroupByTest extends TestBase {

  test("GROUP BY DateType partition column should work correctly") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = s"$tempDir/date_partition_test"

      // Create test data with Date partition column
      val data = Seq(
        ("doc1", "content1", 100, Date.valueOf("2024-01-01")),
        ("doc2", "content2", 200, Date.valueOf("2024-01-01")),
        ("doc3", "content3", 300, Date.valueOf("2024-01-02")),
        ("doc4", "content4", 400, Date.valueOf("2024-01-02")),
        ("doc5", "content5", 500, Date.valueOf("2024-01-03"))
      ).toDF("id", "content", "value", "part_date")

      // Write partitioned by Date column
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("part_date")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // GROUP BY DateType partition column
      println("TEST: GROUP BY DateType partition column")
      val result = df
        .groupBy("part_date")
        .agg(count("*").as("count"))
        .orderBy("part_date")
        .collect()

      println(s"Results:")
      result.foreach(row => println(s"  ${row.getDate(0)} => ${row.getLong(1)}"))

      result.length should be(3)
      result(0).getDate(0) should be(Date.valueOf("2024-01-01"))
      result(0).getLong(1) should be(2)

      result(1).getDate(0) should be(Date.valueOf("2024-01-02"))
      result(1).getLong(1) should be(2)

      result(2).getDate(0) should be(Date.valueOf("2024-01-03"))
      result(2).getLong(1) should be(1)
    }
  }

  test("GROUP BY TimestampType partition column should work correctly") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = s"$tempDir/timestamp_partition_test"

      // Create test data with Timestamp partition column
      val ts1 = Timestamp.valueOf("2024-01-01 10:00:00")
      val ts2 = Timestamp.valueOf("2024-01-01 11:00:00")
      val ts3 = Timestamp.valueOf("2024-01-02 10:00:00")

      val data = Seq(
        ("doc1", "content1", 100, ts1),
        ("doc2", "content2", 200, ts1),
        ("doc3", "content3", 300, ts2),
        ("doc4", "content4", 400, ts3),
        ("doc5", "content5", 500, ts3)
      ).toDF("id", "content", "value", "part_ts")

      // Write partitioned by Timestamp column
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("part_ts")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // GROUP BY TimestampType partition column
      println("TEST: GROUP BY TimestampType partition column")
      val result = df
        .groupBy("part_ts")
        .agg(count("*").as("count"))
        .orderBy("part_ts")
        .collect()

      println(s"Results:")
      result.foreach(row => println(s"  ${row.getTimestamp(0)} => ${row.getLong(1)}"))

      result.length should be(3)
      result(0).getTimestamp(0) should be(ts1)
      result(0).getLong(1) should be(2)

      result(1).getTimestamp(0) should be(ts2)
      result(1).getLong(1) should be(1)

      result(2).getTimestamp(0) should be(ts3)
      result(2).getLong(1) should be(2)
    }
  }

  test("GROUP BY multiple partition columns including DateType should work") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = s"$tempDir/multi_partition_date_test"

      // Create test data with Date + String partition columns
      val data = Seq(
        ("doc1", "content1", 100, Date.valueOf("2024-01-01"), "A"),
        ("doc2", "content2", 200, Date.valueOf("2024-01-01"), "A"),
        ("doc3", "content3", 300, Date.valueOf("2024-01-01"), "B"),
        ("doc4", "content4", 400, Date.valueOf("2024-01-02"), "A"),
        ("doc5", "content5", 500, Date.valueOf("2024-01-02"), "B"),
        ("doc6", "content6", 600, Date.valueOf("2024-01-02"), "B")
      ).toDF("id", "content", "value", "part_date", "part_category")

      // Write partitioned by Date + String
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("part_date", "part_category")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // GROUP BY both partition columns
      println("TEST: GROUP BY Date + String partition columns")
      val result = df
        .groupBy("part_date", "part_category")
        .agg(count("*").as("count"))
        .orderBy("part_date", "part_category")
        .collect()

      println(s"Results:")
      result.foreach(row => println(s"  ${row.getDate(0)}, ${row.getString(1)} => ${row.getLong(2)}"))

      result.length should be(4)
      result(0).getDate(0) should be(Date.valueOf("2024-01-01"))
      result(0).getString(1) should be("A")
      result(0).getLong(2) should be(2)

      result(1).getDate(0) should be(Date.valueOf("2024-01-01"))
      result(1).getString(1) should be("B")
      result(1).getLong(2) should be(1)

      result(2).getDate(0) should be(Date.valueOf("2024-01-02"))
      result(2).getString(1) should be("A")
      result(2).getLong(2) should be(1)

      result(3).getDate(0) should be(Date.valueOf("2024-01-02"))
      result(3).getString(1) should be("B")
      result(3).getLong(2) should be(2)
    }
  }

  test("GROUP BY IntegerType partition column should work correctly") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = s"$tempDir/int_partition_test"

      // Create test data with Integer partition column
      val data = Seq(
        ("doc1", "content1", 100, 2024),
        ("doc2", "content2", 200, 2024),
        ("doc3", "content3", 300, 2023),
        ("doc4", "content4", 400, 2023),
        ("doc5", "content5", 500, 2022)
      ).toDF("id", "content", "value", "part_year")

      // Write partitioned by Integer column
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("part_year")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // GROUP BY IntegerType partition column
      println("TEST: GROUP BY IntegerType partition column")
      val result = df
        .groupBy("part_year")
        .agg(count("*").as("count"))
        .orderBy("part_year")
        .collect()

      println(s"Results:")
      result.foreach(row => println(s"  ${row.getInt(0)} => ${row.getLong(1)}"))

      result.length should be(3)
      result(0).getInt(0) should be(2022)
      result(0).getLong(1) should be(1)

      result(1).getInt(0) should be(2023)
      result(1).getLong(1) should be(2)

      result(2).getInt(0) should be(2024)
      result(2).getLong(1) should be(2)
    }
  }

  test("GROUP BY LongType partition column should work correctly") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = s"$tempDir/long_partition_test"

      // Create test data with Long partition column
      val data = Seq(
        ("doc1", "content1", 100, 1000000000001L),
        ("doc2", "content2", 200, 1000000000001L),
        ("doc3", "content3", 300, 1000000000002L),
        ("doc4", "content4", 400, 1000000000002L),
        ("doc5", "content5", 500, 1000000000003L)
      ).toDF("id", "content", "value", "part_id")

      // Write partitioned by Long column
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("part_id")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // GROUP BY LongType partition column
      println("TEST: GROUP BY LongType partition column")
      val result = df
        .groupBy("part_id")
        .agg(count("*").as("count"))
        .orderBy("part_id")
        .collect()

      println(s"Results:")
      result.foreach(row => println(s"  ${row.getLong(0)} => ${row.getLong(1)}"))

      result.length should be(3)
      result(0).getLong(0) should be(1000000000001L)
      result(0).getLong(1) should be(2)

      result(1).getLong(0) should be(1000000000002L)
      result(1).getLong(1) should be(2)

      result(2).getLong(0) should be(1000000000003L)
      result(2).getLong(1) should be(1)
    }
  }

  test("GROUP BY StringType partition column should work correctly") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = s"$tempDir/string_partition_test"

      // Create test data with String partition column
      val data = Seq(
        ("doc1", "content1", 100, "region_a"),
        ("doc2", "content2", 200, "region_a"),
        ("doc3", "content3", 300, "region_b"),
        ("doc4", "content4", 400, "region_b"),
        ("doc5", "content5", 500, "region_c")
      ).toDF("id", "content", "value", "part_region")

      // Write partitioned by String column
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("part_region")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // GROUP BY StringType partition column
      println("TEST: GROUP BY StringType partition column")
      val result = df
        .groupBy("part_region")
        .agg(count("*").as("count"))
        .orderBy("part_region")
        .collect()

      println(s"Results:")
      result.foreach(row => println(s"  ${row.getString(0)} => ${row.getLong(1)}"))

      result.length should be(3)
      result(0).getString(0) should be("region_a")
      result(0).getLong(1) should be(2)

      result(1).getString(0) should be("region_b")
      result(1).getLong(1) should be(2)

      result(2).getString(0) should be("region_c")
      result(2).getLong(1) should be(1)
    }
  }

  test("GROUP BY Date partition + non-partition column should work (tantivy path)") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = s"$tempDir/date_and_nonpart_test"

      // Create test data with Date partition column and non-partition category
      val data = Seq(
        ("doc1", "cat_a", 100, Date.valueOf("2024-01-01")),
        ("doc2", "cat_a", 200, Date.valueOf("2024-01-01")),
        ("doc3", "cat_b", 300, Date.valueOf("2024-01-01")),
        ("doc4", "cat_a", 400, Date.valueOf("2024-01-02")),
        ("doc5", "cat_b", 500, Date.valueOf("2024-01-02"))
      ).toDF("id", "category", "value", "part_date")

      // Write partitioned by Date only
      // When using tantivy GROUP BY (with non-partition columns), ALL GROUP BY columns need to be fast
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("part_date")
        .option("spark.indextables.indexing.fastfields", "category,part_date") // both columns need to be fast for GROUP BY
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // GROUP BY Date partition + non-partition column - uses tantivy aggregation
      println("TEST: GROUP BY Date partition + non-partition column")
      val result = df
        .groupBy("part_date", "category")
        .agg(count("*").as("count"))
        .orderBy("part_date", "category")
        .collect()

      println(s"Results:")
      result.foreach(row => println(s"  ${row.getDate(0)}, ${row.getString(1)} => ${row.getLong(2)}"))

      result.length should be(4)
      result(0).getDate(0) should be(Date.valueOf("2024-01-01"))
      result(0).getString(1) should be("cat_a")
      result(0).getLong(2) should be(2)

      result(1).getDate(0) should be(Date.valueOf("2024-01-01"))
      result(1).getString(1) should be("cat_b")
      result(1).getLong(2) should be(1)

      result(2).getDate(0) should be(Date.valueOf("2024-01-02"))
      result(2).getString(1) should be("cat_a")
      result(2).getLong(2) should be(1)

      result(3).getDate(0) should be(Date.valueOf("2024-01-02"))
      result(3).getString(1) should be("cat_b")
      result(3).getLong(2) should be(1)
    }
  }

  test("Simple COUNT(*) with DateType partition filter should work") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = s"$tempDir/date_filter_count_test"

      // Create test data with Date partition column
      val data = Seq(
        ("doc1", "content1", 100, Date.valueOf("2024-01-01")),
        ("doc2", "content2", 200, Date.valueOf("2024-01-01")),
        ("doc3", "content3", 300, Date.valueOf("2024-01-02")),
        ("doc4", "content4", 400, Date.valueOf("2024-01-02")),
        ("doc5", "content5", 500, Date.valueOf("2024-01-03"))
      ).toDF("id", "content", "value", "part_date")

      // Write partitioned by Date column
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("part_date")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // Simple COUNT(*) with Date partition filter
      println("TEST: COUNT(*) with Date partition filter")
      val result = df
        .filter($"part_date" === Date.valueOf("2024-01-01"))
        .agg(count("*").as("count"))
        .collect()

      println(s"Result: ${result(0).getLong(0)}")
      result(0).getLong(0) should be(2)
    }
  }
}
