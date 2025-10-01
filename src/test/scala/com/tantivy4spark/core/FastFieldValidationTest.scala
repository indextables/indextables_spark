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

package com.tantivy4spark.core

import com.tantivy4spark.TestBase
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/** Test to verify that COUNT aggregations with filters require fast field configuration. */
class FastFieldValidationTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  test("COUNT aggregation without filters should work without fast field configuration") {
    withTempPath { path =>
      // Create test data
      val data = spark
        .createDataFrame(
          Seq(
            ("id1", "batch1", "content1"),
            ("id2", "batch2", "content2"),
            ("id3", "batch1", "content3")
          )
        )
        .toDF("id", "batch", "content")

      // Write data WITHOUT configuring batch as fast field
      data.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Read data back
      val result = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(path)

      // This should work - unfiltered count (uses transaction log optimization)
      val totalCount = result.count()
      totalCount shouldBe 3
      println(s"✅ Unfiltered count works without fast fields: $totalCount")
    }
  }

  test("COUNT aggregation with filter should require fast field configuration") {
    withTempPath { path =>
      // Create test data
      val data = spark
        .createDataFrame(
          Seq(
            ("id1", "batch1", "content1"),
            ("id2", "batch2", "content2"),
            ("id3", "batch1", "content3")
          )
        )
        .toDF("id", "batch", "content")

      // Write data WITHOUT configuring batch as fast field (explicitly exclude it from auto-fast-field)
      data.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.indextables.indexing.nonfastfields", "batch")
        .mode("overwrite")
        .save(path)

      // Read data back
      val result = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(path)

      // This should FAIL - filtered count without fast field configuration
      val exception = intercept[IllegalArgumentException] {
        val batch1Count = result.filter(col("batch") === "batch1").count()
        println(s"Batch1 count (should fail): $batch1Count")
      }

      // Verify the error message mentions fast fields
      exception.getMessage should include("fast field configuration")
      exception.getMessage should include("batch")
      println(s"✅ Test passed - got expected error for missing fast field: ${exception.getMessage}")
    }
  }

  test("COUNT aggregation with filter should work WITH fast field configuration") {
    withTempPath { path =>
      // Create test data
      val data = spark
        .createDataFrame(
          Seq(
            ("id1", "batch1", "content1"),
            ("id2", "batch2", "content2"),
            ("id3", "batch1", "content3")
          )
        )
        .toDF("id", "batch", "content")

      // Write data WITH batch configured as fast field
      data.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "batch")
        .mode("overwrite")
        .save(path)

      // Read data back
      val result = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(path)

      // This should work - filtered count with fast field configuration
      val batch1Count = result.filter(col("batch") === "batch1").count()
      batch1Count shouldBe 2
      println(s"✅ Filtered count works with fast field: $batch1Count")

      val batch2Count = result.filter(col("batch") === "batch2").count()
      batch2Count shouldBe 1
      println(s"✅ Another filtered count works with fast field: $batch2Count")
    }
  }

  test("COUNT aggregation with multiple filter fields should validate all fast fields") {
    withTempPath { path =>
      // Create test data with multiple filterable fields
      val data = spark
        .createDataFrame(
          Seq(
            ("id1", "batch1", "type1", "content1"),
            ("id2", "batch2", "type2", "content2"),
            ("id3", "batch1", "type1", "content3"),
            ("id4", "batch2", "type1", "content4")
          )
        )
        .toDF("id", "batch", "type", "content")

      // Write data with only one fast field configured (explicitly exclude type from auto-fast-field)
      data.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "batch")   // Only batch, not type
        .option("spark.indextables.indexing.nonfastfields", "type") // Explicitly exclude type
        .mode("overwrite")
        .save(path)

      // Read data back
      val result = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(path)

      // This should work - filter only on fast field
      val batch1Count = result.filter(col("batch") === "batch1").count()
      batch1Count shouldBe 2
      println(s"✅ Filter on fast field works: $batch1Count")

      // This should FAIL - filter on non-fast field
      val exception = intercept[IllegalArgumentException] {
        val type1Count = result.filter(col("type") === "type1").count()
        println(s"Type1 count (should fail): $type1Count")
      }

      exception.getMessage should include("fast field configuration")
      exception.getMessage should include("type")
      println(s"✅ Test passed - got expected error for non-fast field: ${exception.getMessage}")
    }
  }
}
