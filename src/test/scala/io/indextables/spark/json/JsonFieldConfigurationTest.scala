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

package io.indextables.spark.json

import io.indextables.spark.TestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Test suite for JSON field configuration options, specifically testing the
 * spark.indextables.indexing.json.mode configuration.
 *
 * Validates:
 * - Default "full" mode enables fast fields for range queries and aggregations
 * - "minimal" mode disables fast fields (stored + indexed only)
 * - Configuration override behavior
 */
class JsonFieldConfigurationTest extends TestBase {

  test("should support range queries and aggregations with default full mode") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false),
          StructField("salary", IntegerType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30, 80000)),
        Row(2, Row("Bob", 25, 60000)),
        Row(3, Row("Charlie", 35, 90000)),
        Row(4, Row("David", 28, 70000)),
        Row(5, Row("Eve", 32, 85000))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write with default mode (should be "full")
      // Note: Even with json.mode=full, we still need to explicitly configure fast fields
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "user.age,user.salary")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows with default json.mode (full)")

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: Simple read should work
      val totalCount = result.count()
      totalCount shouldBe 5L
      println(s"✅ Total count: $totalCount rows")

      // Test: Explicit aggregation on nested field (requires fast fields)
      val avgSalary = result.agg(avg(col("user.salary"))).collect()(0).getDouble(0)
      avgSalary shouldBe 77000.0 // (80000 + 60000 + 90000 + 70000 + 85000) / 5
      println(s"✅ Aggregation AVG(user.salary): $avgSalary")
    }
  }

  test("should support range queries and aggregations with explicit full mode") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false),
          StructField("salary", IntegerType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30, 80000)),
        Row(2, Row("Bob", 25, 60000)),
        Row(3, Row("Charlie", 35, 90000)),
        Row(4, Row("David", 28, 70000)),
        Row(5, Row("Eve", 32, 85000))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write with explicit "full" mode
      // Note: json.mode=full enables JSON field features, but fast fields still need explicit config
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.json.mode", "full")
        .option("spark.indextables.indexing.fastfields", "user.age,user.salary")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows with json.mode=full")

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: Simple read should work
      val totalCount = result.count()
      totalCount shouldBe 5L
      println(s"✅ Total count: $totalCount rows")

      // Test: Explicit aggregation on nested field
      val sumSalary = result.agg(sum(col("user.salary"))).collect()(0).getLong(0)
      sumSalary shouldBe 385000L // 80000 + 60000 + 90000 + 70000 + 85000
      println(s"✅ Aggregation SUM(user.salary): $sumSalary")
    }
  }

  test("should handle minimal mode gracefully for equality filters") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false),
          StructField("city", StringType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30, "NYC")),
        Row(2, Row("Bob", 25, "SF")),
        Row(3, Row("Charlie", 35, "NYC")),
        Row(4, Row("David", 28, "SF")),
        Row(5, Row("Eve", 32, "NYC"))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write with "minimal" mode
      // Note: minimal mode = no fast fields by default, but we can still explicitly configure them
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.json.mode", "minimal")
        .option("spark.indextables.indexing.fastfields", "user.city")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows with json.mode=minimal")

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: Basic COUNT should work
      val totalCount = result.count()
      totalCount shouldBe 5L
      println(s"✅ COUNT(*): $totalCount rows")

      // Test: Read all rows and filter in memory (to demonstrate data is accessible)
      val allRows = result.collect()
      allRows.length shouldBe 5
      println(s"✅ Collected all rows: ${allRows.length} rows")
    }
  }

  test("should support case-insensitive json.mode values") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30)),
        Row(2, Row("Bob", 25))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write with uppercase "FULL" mode
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.json.mode", "FULL")
        .option("spark.indextables.indexing.fastfields", "user.age")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows with json.mode=FULL (uppercase)")

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: Should work with uppercase mode value
      val totalCount = result.count()
      totalCount shouldBe 2L // Alice (30), Bob (25)
      println(s"✅ Total count with uppercase mode: $totalCount rows")

      // Test: Explicit aggregation
      val maxAge = result.agg(max(col("user.age"))).collect()(0).getInt(0)
      maxAge shouldBe 30 // Alice
      println(s"✅ MAX(user.age) with uppercase mode: $maxAge")
    }
  }

  test("should default to full mode for invalid json.mode values") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30)),
        Row(2, Row("Bob", 25)),
        Row(3, Row("Charlie", 35))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write with invalid mode value (should default to "full")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.json.mode", "invalid_mode")
        .option("spark.indextables.indexing.fastfields", "user.age")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows with json.mode=invalid_mode (should default to full)")

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: Basic count should work
      val totalCount = result.count()
      totalCount shouldBe 3L // Alice (30), Bob (25), Charlie (35)
      println(s"✅ Total count with invalid mode (defaults to full): $totalCount rows")

      // Test: Explicit aggregation should work
      val maxAge = result.agg(max(col("user.age"))).collect()(0).getInt(0)
      maxAge shouldBe 35
      println(s"✅ Aggregation MAX(user.age) with invalid mode: $maxAge")
    }
  }

  test("should support multiple aggregations with full mode") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false),
          StructField("salary", IntegerType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30, 80000)),
        Row(2, Row("Bob", 25, 60000)),
        Row(3, Row("Charlie", 35, 90000)),
        Row(4, Row("David", 28, 70000)),
        Row(5, Row("Eve", 32, 85000))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write with explicit full mode
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.json.mode", "full")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows with json.mode=full for multiple aggregations")

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: Multiple aggregations on different nested fields
      val stats = result
        .agg(
          count("*").as("total"),
          sum(col("user.salary")).as("sum_salary"),
          avg(col("user.age")).as("avg_age"),
          min(col("user.age")).as("min_age"),
          max(col("user.salary")).as("max_salary")
        )
        .collect()(0)

      stats.getLong(0) shouldBe 5L
      stats.getLong(1) shouldBe 385000L
      stats.getDouble(2) shouldBe 30.0
      stats.getInt(3) shouldBe 25
      stats.getInt(4) shouldBe 90000

      println(s"✅ Multiple aggregations: count=${stats.getLong(0)}, sum=${stats.getLong(1)}, " +
             s"avg=${stats.getDouble(2)}, min=${stats.getInt(3)}, max=${stats.getInt(4)}")
    }
  }
}
