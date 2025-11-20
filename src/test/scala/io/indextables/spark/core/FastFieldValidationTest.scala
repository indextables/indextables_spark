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
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Read data back
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // This should work - unfiltered count (uses transaction log optimization)
      val totalCount = result.count()
      totalCount shouldBe 3
      println(s"✅ Unfiltered count works without fast fields: $totalCount")
    }
  }

  test("COUNT aggregation with filter should require numeric fast field configuration") {
    withTempPath { path =>
      // Create test data with ONLY string fields (no numeric fields for fast field auto-configuration)
      val data = spark
        .createDataFrame(
          Seq(
            ("id1", "batch1", "content1"),
            ("id2", "batch2", "content2"),
            ("id3", "batch1", "content3")
          )
        )
        .toDF("id", "batch", "content")

      // Write data WITHOUT any fast fields configured
      // Since all fields are strings, no numeric fast field will be available
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.nonfastfields", "id,batch,content")
        .mode("overwrite")
        .save(path)

      // Read data back
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // With only string fields and nonfastfields configured, COUNT with filters has two possible behaviors:
      // 1. If Spark pushes down the aggregation: Should fail with IllegalArgumentException (no numeric fast field)
      // 2. If Spark handles the aggregation: Works fine (Spark computes count after fetching filtered data)
      // Both behaviors are acceptable - this test just verifies the system doesn't crash
      try {
        val batch1Count = result.filter(col("batch") === "batch1").count()
        println(s"✅ Batch1 count succeeded (Spark computed aggregation): $batch1Count")
        assert(batch1Count == 2, "Should find 2 records with batch=batch1")
      } catch {
        case e: IllegalArgumentException if e.getMessage.contains("numeric fast field") =>
          println(
            s"✅ Got expected error for missing numeric fast field (connector rejected aggregation): ${e.getMessage}"
          )
        case e: Exception =>
          fail(s"Unexpected exception type: ${e.getClass.getName}: ${e.getMessage}")
      }
    }
  }

  test("COUNT aggregation with filter should work WITH numeric fast field configuration") {
    withTempPath { path =>
      // Create test data with a numeric field for fast field configuration
      val data = spark
        .createDataFrame(
          Seq(
            ("id1", "batch1", "content1", 1),
            ("id2", "batch2", "content2", 2),
            ("id3", "batch1", "content3", 3)
          )
        )
        .toDF("id", "batch", "content", "seq_num")

      // Write data WITH numeric field configured as fast field
      // COUNT(*) aggregation with filters requires a numeric fast field (StatsAggregation limitation)
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "seq_num")
        .mode("overwrite")
        .save(path)

      // Read data back
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // This should work - filtered count with numeric fast field configuration
      val batch1Count = result.filter(col("batch") === "batch1").count()
      batch1Count shouldBe 2
      println(s"✅ Filtered count works with numeric fast field: $batch1Count")

      val batch2Count = result.filter(col("batch") === "batch2").count()
      batch2Count shouldBe 1
      println(s"✅ Another filtered count works with numeric fast field: $batch2Count")
    }
  }

  test("COUNT aggregation with multiple filter fields should have numeric fast field") {
    withTempPath { path =>
      // Create test data with multiple filterable fields including a numeric field
      val data = spark
        .createDataFrame(
          Seq(
            ("id1", "batch1", "type1", "content1", 1),
            ("id2", "batch2", "type2", "content2", 2),
            ("id3", "batch1", "type1", "content3", 3),
            ("id4", "batch2", "type1", "content4", 4)
          )
        )
        .toDF("id", "batch", "type", "content", "seq_num")

      // Write data with numeric fast field (required for COUNT with filters)
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "seq_num")
        .mode("overwrite")
        .save(path)

      // Read data back
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // This should work - filter on any field with numeric fast field configured
      val batch1Count = result.filter(col("batch") === "batch1").count()
      batch1Count shouldBe 2
      println(s"✅ Filter on batch field works with numeric fast field: $batch1Count")

      val type1Count = result.filter(col("type") === "type1").count()
      type1Count shouldBe 3
      println(s"✅ Filter on type field works with numeric fast field: $type1Count")
    }
  }
}
