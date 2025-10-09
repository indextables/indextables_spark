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

import java.io.File

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import io.indextables.spark.TestBase

/**
 * Test suite demonstrating aggregate pushdown failures with partitioned tables.
 *
 * BUG: When using V2 DataSource API with partitioned tables, distinct().collect()
 * triggers aggregate pushdown that fails with:
 * "The data source returns unexpected number of columns"
 *
 * This test suite will FAIL until the aggregate pushdown implementation is fixed
 * to properly handle partition columns.
 *
 * Expected behavior: distinct().collect() should work on partitioned tables just
 * like it does on non-partitioned tables.
 */
class PartitionedTableAggregatePushdownIssue
    extends AnyFunSuite
    with TestBase
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  test("distinct().collect() should work on partitioned tables") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    import org.apache.spark.sql.functions._

    withTempPath { tempDir =>
      val tablePath = new File(tempDir, "partitioned_table").getAbsolutePath

      println("=" * 80)
      println("BUG REPRODUCTION: distinct().collect() on partitioned tables")
      println("=" * 80)

      // Create partitioned table
      println("\n1. Creating partitioned table with 4 partitions...")
      val data = Seq(
        ("2024-01-01", 10, "doc1", "content1", 100),
        ("2024-01-01", 11, "doc2", "content2", 200),
        ("2024-01-02", 10, "doc3", "content3", 300),
        ("2024-01-02", 11, "doc4", "content4", 400)
      ).toDF("load_date", "load_hour", "id", "content", "score")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("load_date", "load_hour")
        .option("spark.indextables.indexing.fastfields", "score,load_date,load_hour")
        .mode("overwrite")
        .save(tablePath)
      println("✅ Table created successfully")

      // Read the table
      println("\n2. Reading partitioned table...")
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)
      println("✅ Table loaded successfully")

      // This should work but currently fails due to aggregate pushdown bug
      println("\n3. Attempting distinct().collect() on partition columns...")
      println("   Expected: Should return 4 unique partition combinations")
      println("   Actual: Will fail with 'unexpected number of columns' error")
      println()

      val distinctPartitions = df.select("load_date", "load_hour").distinct().collect()

      // If we get here, the bug is fixed!
      println(s"✅ SUCCESS: distinct().collect() returned ${distinctPartitions.length} rows")
      assert(distinctPartitions.length === 4,
        s"Expected 4 unique partitions, got ${distinctPartitions.length}")

      // Verify all expected partitions are present
      val partitionSet = distinctPartitions.map(r => (r.getString(0), r.getInt(1))).toSet
      val expectedPartitions = Set(
        ("2024-01-01", 10),
        ("2024-01-01", 11),
        ("2024-01-02", 10),
        ("2024-01-02", 11)
      )
      assert(partitionSet === expectedPartitions,
        s"Partition values don't match. Expected: $expectedPartitions, Got: $partitionSet")

      println("=" * 80)
      println("BUG IS FIXED: distinct().collect() now works on partitioned tables!")
      println("=" * 80)
    }
  }

  test("distinct().count() should work on partitioned tables") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    import org.apache.spark.sql.functions._

    withTempPath { tempDir =>
      val tablePath = new File(tempDir, "partitioned_table").getAbsolutePath

      println("=" * 80)
      println("BUG REPRODUCTION: distinct().count() on partitioned tables")
      println("=" * 80)

      // Create partitioned table
      println("\n1. Creating partitioned table with 4 partitions...")
      val data = Seq(
        ("2024-01-01", 10, "doc1", "content1", 100),
        ("2024-01-01", 11, "doc2", "content2", 200),
        ("2024-01-02", 10, "doc3", "content3", 300),
        ("2024-01-02", 11, "doc4", "content4", 400)
      ).toDF("load_date", "load_hour", "id", "content", "score")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("load_date", "load_hour")
        .option("spark.indextables.indexing.fastfields", "score,load_date,load_hour")
        .mode("overwrite")
        .save(tablePath)
      println("✅ Table created successfully")

      // Read the table
      println("\n2. Reading partitioned table...")
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)
      println("✅ Table loaded successfully")

      // This should work but currently fails due to aggregate pushdown bug
      println("\n3. Attempting distinct().count() on partition columns...")
      println("   Expected: Should return 4 (number of unique partition combinations)")
      println("   Actual: Will fail with 'unexpected number of columns' error")
      println()

      val distinctCount = df.select("load_date", "load_hour").distinct().count()

      // If we get here, the bug is fixed!
      println(s"✅ SUCCESS: distinct().count() returned $distinctCount")
      assert(distinctCount === 4,
        s"Expected 4 unique partitions, got $distinctCount")

      println("=" * 80)
      println("BUG IS FIXED: distinct().count() now works on partitioned tables!")
      println("=" * 80)
    }
  }
}
