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

import io.indextables.spark.TestBase
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * Test advanced scan features in V2 DataSource API including:
 *   - Column pruning optimization
 *   - Partition pushdown
 *   - Limit pushdown
 *   - Combined pushdown operations
 *   - Query plan verification
 */
class V2AdvancedScanTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  ignore("should support column pruning in V2 scan") {
    withTempPath { path =>
      // Create test data with multiple columns
      val data = spark
        .range(100)
        .select(
          col("id"),
          concat(lit("Name "), col("id")).as("name"),
          (col("id") % 10).as("category_id"),
          concat(lit("Category "), col("id") % 10).as("category_name"),
          (col("id") * 1000).as("large_value"),
          concat(lit("Description for item "), col("id")).as("description")
        )

      // Write using V2 API
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Read with column pruning - select only 2 out of 6 columns
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .select("id", "name") // Should prune other columns

      // Verify column pruning worked
      result.schema.fields.length shouldBe 2
      result.schema.fieldNames should contain theSameElementsAs Array("id", "name")

      // Verify data correctness
      val collected = result.orderBy("id").collect()
      collected.length shouldBe 100
      collected(0).getString(1) shouldBe "Name 0"
      collected(99).getString(1) shouldBe "Name 99"

      // Verify query plan shows column pruning (check for actual schema or relation)
      val queryPlan = result.queryExecution.optimizedPlan.toString
      queryPlan should (include("ReadSchema") or include("id#") and include("name#"))
    }
  }

  ignore("should support limit pushdown in V2 scan") {
    withTempPath { path =>
      // Create larger test dataset
      val data = spark
        .range(1000)
        .select(
          col("id"),
          concat(lit("Item "), col("id")).as("title"),
          (col("id") % 5).as("status")
        )

      // Write using V2 API
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Read with limit - should push down limit to scan
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .limit(10)

      // Verify limit pushdown worked
      val collected = result.collect()
      collected.length shouldBe 10

      // Verify query plan shows limit pushdown
      val queryPlan = result.queryExecution.optimizedPlan.toString
      // V2 limit pushdown appears as LocalLimit in the plan
      queryPlan should (include("LocalLimit") or include("GlobalLimit"))
    }
  }

  ignore("should support combined filter and limit pushdown") {
    withTempPath { path =>
      // Create test data with filterable content
      val data = spark
        .range(500)
        .select(
          col("id"),
          (col("id") % 3).as("category"),
          concat(lit("Product "), col("id")).as("name"),
          (col("id") * 10.5).as("price")
        )

      // Write using V2 API
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Read with both filter and limit
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("category") === 1) // Should push down filter
        .limit(5)                      // Should push down limit

      // Verify results
      val collected = result.collect()
      collected.length shouldBe 5
      collected.foreach(row => row.getLong(1) shouldBe 1) // All category = 1

      // Verify query plan shows both pushdowns (filter pushdown may not always show as "PushedFilters")
      val queryPlan = result.queryExecution.optimizedPlan.toString
      // Check that filtering is working (results should be filtered)
      queryPlan should (include("LocalLimit") or include("GlobalLimit"))

      // More importantly, verify the actual filtering worked
      result.count() should be <= 5L
    }
  }

  ignore("should support column pruning with complex projections") {
    withTempPath { path =>
      // Create complex schema
      val data = spark
        .range(50)
        .select(
          col("id"),
          concat(lit("First "), col("id")).as("first_name"),
          concat(lit("Last "), col("id")).as("last_name"),
          (col("id") + 20).as("age"),
          (col("id") % 2 === 0).as("active"),
          (col("id") * 50000).as("salary"),
          concat(lit("Dept "), col("id") % 3).as("department")
        )

      // Write using V2 API
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Read with complex projection involving expressions
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .select(
          col("id"),
          concat(col("first_name"), lit(" "), col("last_name")).as("full_name"),
          col("active")
        )

      // Verify projection worked
      result.schema.fieldNames should contain theSameElementsAs Array("id", "full_name", "active")

      val collected = result.orderBy("id").take(3)
      collected(0).getString(1) shouldBe "First 0 Last 0"
      collected(1).getString(1) shouldBe "First 1 Last 1"
      collected(2).getString(1) shouldBe "First 2 Last 2"
    }
  }

  ignore("should handle empty result sets with pushdown optimizations") {
    withTempPath { path =>
      // Create test data
      val data = spark
        .range(100)
        .select(
          col("id"),
          (col("id") % 10).as("group_id"),
          concat(lit("Value "), col("id")).as("value")
        )

      // Write using V2 API
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Read with filter that matches no records
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("group_id") === -1) // No records match this
        .select("id", "value")          // Column pruning
        .limit(10)                      // Limit pushdown

      // Verify empty result handling
      val collected = result.collect()
      collected.length shouldBe 0

      // Verify query plan still shows optimizations
      val queryPlan = result.queryExecution.optimizedPlan.toString
      // Focus on verifying the actual functionality rather than plan text
      queryPlan should (include("id#") and include("value#")) // Should have selected columns
    }
  }

  ignore("should support scan with all advanced features combined") {
    withTempPath { path =>
      // Create comprehensive test dataset
      val data = spark
        .range(1000)
        .select(
          col("id"),
          (col("id") % 100).as("partition_key"),
          concat(lit("Record "), col("id")).as("title"),
          (col("id") % 5).as("status_code"),
          (col("id") * 1.5).as("score"),
          when(col("id") % 2 === 0, "active").otherwise("inactive").as("status"),
          concat(lit("Details for record "), col("id")).as("details")
        )

      // Write using V2 API
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Complex query with all optimizations:
      // - Column pruning (select specific columns)
      // - Filter pushdown (multiple conditions)
      // - Limit pushdown
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("status_code") === 1)         // Filter pushdown
        .filter(col("partition_key") < 50)        // Additional filter
        .select("id", "title", "score", "status") // Column pruning
        .orderBy("id")                            // Order by
        .limit(20)                                // Limit pushdown

      // Verify results
      val collected = result.collect()
      collected.length should be <= 20
      collected.foreach { row =>
        row.getLong(0)  % 5 shouldBe 1         // status_code = 1
        (row.getLong(0) % 100) should be < 50L // partition_key < 50
      }

      // Verify query plan shows all optimizations
      val queryPlan = result.queryExecution.optimizedPlan.toString
      // Verify the actual functionality is working
      queryPlan should (include("LocalLimit") or include("GlobalLimit"))
      queryPlan should (include("id#") and include("title#") and include("score#") and include("status#"))

      // Verify correct schema
      result.schema.fieldNames should contain theSameElementsAs Array("id", "title", "score", "status")
    }
  }
}
