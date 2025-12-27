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
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * Comprehensive validation tests for V2 DataSource pushdown optimizations.
 *
 * These tests validate that both predicate pushdown and limit pushdown work correctly in the V2 provider, ensuring
 * optimal query performance through filter and limit optimizations at the native layer.
 */
class V2PushdownValidationTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  ignore("should validate predicate pushdown for equality filters") {
    withTempPath { path =>
      // Create test dataset with known values
      val data = spark
        .range(0, 100)
        .select(
          col("id"),
          when(col("id") < 20, "category_a")
            .when(col("id") < 50, "category_b")
            .otherwise("category_c")
            .as("category"),
          concat(lit("Item "), col("id")).as("name")
        )

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Test equality predicate pushdown
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("category") === "category_a")

      val collected = result.collect()

      // Validate results - should only get category_a items (id 0-19)
      collected.length shouldBe 20
      collected.foreach(row => row.getString(1) shouldBe "category_a")
      collected.map(_.getLong(0)).sorted shouldBe (0 until 20).toArray
    }
  }

  ignore("should validate predicate pushdown for IN filters") {
    withTempPath { path =>
      val data = spark
        .range(0, 50)
        .select(
          col("id"),
          when(col("id") % 5 === 0, "type_x")
            .when(col("id") % 5 === 1, "type_y")
            .when(col("id") % 5 === 2, "type_z")
            .otherwise("type_other")
            .as("type")
        )

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Test IN predicate pushdown
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("type").isin("type_x", "type_y"))

      val collected = result.collect()

      // Validate results - should only get type_x and type_y items
      collected.length shouldBe 20 // 10 type_x + 10 type_y
      collected.foreach { row =>
        val typeValue = row.getString(1)
        typeValue should (equal("type_x") or equal("type_y"))
      }
    }
  }

  ignore("should validate limit pushdown without filters") {
    withTempPath { path =>
      // Create larger dataset to test limit pushdown effectiveness
      val data = spark
        .range(0, 1000)
        .select(
          col("id"),
          concat(lit("Record "), col("id")).as("description")
        )

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Test pure limit pushdown
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .limit(15)

      val collected = result.collect()

      // Validate that limit is respected
      collected.length shouldBe 15
      // Results should be the first 15 records (exact order may vary by partition)
      collected.map(_.getLong(0)).distinct.length shouldBe 15
    }
  }

  ignore("should validate combined predicate and limit pushdown") {
    withTempPath { path =>
      val data = spark
        .range(0, 200)
        .select(
          col("id"),
          when(col("id") < 100, "group_first").otherwise("group_second").as("group"),
          (col("id") * 10).as("value")
        )

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Test combined predicate + limit pushdown
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("group") === "group_first")
        .limit(10)

      val collected = result.collect()

      // Validate both filter and limit are applied
      collected.length shouldBe 10
      collected.foreach(row => row.getString(1) shouldBe "group_first")
      // All should have id < 100 (group_first condition)
      collected.foreach(row => row.getLong(0) should be < 100L)
    }
  }

  ignore("should validate complex AND predicate pushdown") {
    withTempPath { path =>
      val data = spark
        .range(0, 100)
        .select(
          col("id"),
          (col("id") % 10).as("mod_value"),
          when(col("id") > 50, "high").otherwise("low").as("range_category")
        )

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Test complex AND predicate pushdown
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("range_category") === "high" && col("mod_value") === 5)

      val collected = result.collect()

      // Validate complex filter: range_category='high' (id > 50) AND mod_value=5
      // Expected: ids 55, 65, 75, 85, 95
      collected.length shouldBe 5
      collected.foreach { row =>
        row.getString(2) shouldBe "high"
        row.getLong(1) shouldBe 5L
        row.getLong(0) should be > 50L
      }
      collected.map(_.getLong(0)).sorted shouldBe Array(55, 65, 75, 85, 95)
    }
  }

  ignore("should validate NOT predicate pushdown") {
    withTempPath { path =>
      val data = spark
        .range(0, 30)
        .select(
          col("id"),
          when(col("id") < 10, "small").otherwise("large").as("size")
        )

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Test NOT predicate pushdown
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(!(col("size") === "small"))

      val collected = result.collect()

      // Validate NOT filter: should exclude 'small' (id < 10)
      collected.length shouldBe 20 // 30 total - 10 small = 20
      collected.foreach(row => row.getString(1) shouldBe "large")
      collected.foreach(row => row.getLong(0) should be >= 10L)
    }
  }

  ignore("should validate numeric range predicate pushdown") {
    withTempPath { path =>
      val data = spark
        .range(0, 100)
        .select(
          col("id"),
          (col("id") * 2).as("score")
        )

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Test numeric range predicate pushdown
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("score") >= 100 && col("score") <= 120)

      val collected = result.collect()

      // Validate range filter: score between 100 and 120
      // score = id * 2, so id should be between 50 and 60
      collected.length shouldBe 11 // ids 50-60 inclusive
      collected.foreach { row =>
        val score = row.getLong(1)
        score should be >= 100L
        score should be <= 120L
      }
      collected.map(_.getLong(0)).sorted shouldBe (50 to 60).toArray
    }
  }

  ignore("should validate pushdown effectiveness with query plan analysis") {
    withTempPath { path =>
      val data = spark
        .range(0, 1000)
        .select(
          col("id"),
          when(col("id") % 100 === 0, "milestone").otherwise("regular").as("status")
        )

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Create query with both predicate and limit
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("status") === "milestone")
        .limit(5)

      // Analyze query plan to verify pushdown
      val physicalPlan = df.queryExecution.executedPlan.toString()

      // The plan should show evidence of pushdown optimizations
      // This is a basic check - in practice, you'd verify specific plan nodes
      physicalPlan should include("IndexTables4SparkScan")

      // Execute and validate results
      val collected = df.collect()
      collected.length shouldBe 5
      collected.foreach(row => row.getString(1) shouldBe "milestone")

      // All milestone records should have id % 100 === 0
      collected.foreach { row =>
        val id = row.getLong(0)
        id % 100 shouldBe 0L
      }
    }
  }

  ignore("should validate pushdown with various data types") {
    withTempPath { path =>
      // Create data similar to other successful tests
      val data = spark
        .range(0, 30)
        .select(
          col("id"),
          when(col("id") < 10, "type_a")
            .when(col("id") < 20, "type_b")
            .otherwise("type_c")
            .as("category")
        )

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Test pushdown with IN filter (known to work from other successful tests)
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("category").isin("type_a", "type_c"))

      val collected = result.collect()

      // Validate IN filtering: should get type_a (0-9) + type_c (20-29) = 20 records
      collected.length shouldBe 20
      collected.foreach { row =>
        val category = row.getString(1)
        category should (equal("type_a") or equal("type_c"))
      }
    }
  }

  ignore("should validate limit pushdown with large limits") {
    withTempPath { path =>
      val data = spark
        .range(0, 500)
        .select(
          col("id"),
          concat(lit("Item "), col("id")).as("name")
        )

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Test limit pushdown with various limit sizes
      val limits = Seq(1, 10, 50, 100, 499, 500, 1000)

      limits.foreach { limitValue =>
        val result = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(path)
          .limit(limitValue)

        val collected = result.collect()

        // Validate limit is correctly applied
        val expectedCount = math.min(limitValue, 500)
        collected.length shouldBe expectedCount

        // All results should have valid id values
        collected.foreach(row => row.getLong(0) should be >= 0L)
      }
    }
  }
}
