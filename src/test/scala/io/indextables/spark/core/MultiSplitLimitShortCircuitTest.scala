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

/**
 * Tests for the multi-split limit short-circuit behavior.
 *
 * When splitsPerTask > 1, multiple splits are bundled into a single task. The MultiSplitPartitionReader should stop
 * processing splits once the pushed limit has been satisfied.
 */
class MultiSplitLimitShortCircuitTest extends TestBase {

  private val FORMAT = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  /** Write multiple batches to the same path to create multiple splits. */
  private def writeMultipleSplits(
    path: String,
    batchCount: Int,
    rowsPerBatch: Int
  ): Unit =
    (0 until batchCount).foreach { batch =>
      val startId = batch * rowsPerBatch
      val data = spark
        .range(startId, startId + rowsPerBatch)
        .select(
          col("id"),
          (col("id") % 10).cast("string").as("category"),
          lit(s"batch_$batch").as("batch_label")
        )

      data.write
        .format(FORMAT)
        .mode("append")
        .save(path)
    }

  test("multi-split reader respects pushed LIMIT and returns exact row count") {
    withTempPath { tempPath =>
      // Create 3 splits, each with 100 rows (300 total)
      writeMultipleSplits(tempPath, batchCount = 3, rowsPerBatch = 100)

      // Force multi-split mode with all splits in one task
      val df = spark.read
        .format(FORMAT)
        .option("spark.indextables.read.splitsPerTask", "3")
        .option("spark.indextables.read.defaultLimit", "10000")
        .load(tempPath)

      val result = df.limit(10).collect()
      result.length shouldBe 10
    }
  }

  test("multi-split reader with limit smaller than single split returns correct count") {
    withTempPath { tempPath =>
      // Create 3 splits, each with 100 rows
      writeMultipleSplits(tempPath, batchCount = 3, rowsPerBatch = 100)

      val df = spark.read
        .format(FORMAT)
        .option("spark.indextables.read.splitsPerTask", "3")
        .option("spark.indextables.read.defaultLimit", "10000")
        .load(tempPath)

      // Limit smaller than one split's worth of rows
      val result = df.limit(50).collect()
      result.length shouldBe 50
    }
  }

  test("multi-split reader returns all rows when limit exceeds total") {
    withTempPath { tempPath =>
      // Create 3 splits, each with 50 rows (150 total)
      writeMultipleSplits(tempPath, batchCount = 3, rowsPerBatch = 50)

      val df = spark.read
        .format(FORMAT)
        .option("spark.indextables.read.splitsPerTask", "3")
        .option("spark.indextables.read.defaultLimit", "10000")
        .load(tempPath)

      // Limit larger than total data
      val result = df.limit(500).collect()
      result.length shouldBe 150
    }
  }

  test("multi-split reader with filters and limit") {
    withTempPath { tempPath =>
      // Create 3 splits, each with 100 rows (300 total, 30 per category)
      writeMultipleSplits(tempPath, batchCount = 3, rowsPerBatch = 100)

      val df = spark.read
        .format(FORMAT)
        .option("spark.indextables.read.splitsPerTask", "3")
        .option("spark.indextables.read.defaultLimit", "10000")
        .load(tempPath)

      // Filter + limit: category "0" appears for id % 10 == 0 → 10 per batch, 30 total
      val result = df.filter(col("category") === "0").limit(5).collect()
      result.length shouldBe 5
      result.foreach(row => row.getString(row.fieldIndex("category")) shouldBe "0")
    }
  }

  test("multi-split reader with SQL LIMIT") {
    withTempPath { tempPath =>
      // Create 4 splits, each with 75 rows (300 total)
      writeMultipleSplits(tempPath, batchCount = 4, rowsPerBatch = 75)

      spark.read
        .format(FORMAT)
        .option("spark.indextables.read.splitsPerTask", "4")
        .option("spark.indextables.read.defaultLimit", "10000")
        .load(tempPath)
        .createOrReplaceTempView("multi_split_table")

      val result = spark.sql("SELECT * FROM multi_split_table LIMIT 20").collect()
      result.length shouldBe 20
    }
  }

}
