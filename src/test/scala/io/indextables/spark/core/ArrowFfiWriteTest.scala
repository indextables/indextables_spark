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

import io.indextables.spark.write.ArrowFfiWriteConfig
import io.indextables.spark.TestBase

class ArrowFfiWriteTest extends TestBase {

  private val FORMAT = INDEXTABLES_FORMAT

  // ===== Arrow FFI (default path) =====

  test("default write uses Arrow FFI path") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_default"
      val df        = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")

      // No explicit option — Arrow FFI is the default
      df.write
        .mode("overwrite")
        .format(FORMAT)
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 100

      val ids = result.select("id").collect().map(_.getLong(0)).sorted
      ids shouldBe (0L until 100L).toArray
    }
  }

  test("Arrow FFI write + read roundtrip with all primitive types") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_arrow_all_types"
      val df = spark
        .range(0, 50)
        .selectExpr(
          "id",
          "CAST(id AS STRING) as name",
          "CAST(id AS INT) as int_val",
          "CAST(id AS DOUBLE) as double_val",
          "CAST(id % 2 = 0 AS BOOLEAN) as active",
          "CAST('2024-01-01' AS TIMESTAMP) as ts"
        )

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 50

      val ids = result.select("id").collect().map(_.getLong(0)).sorted
      ids shouldBe (0L until 50L).toArray
    }
  }

  test("Arrow FFI write with custom batch size") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_arrow_batch_size"
      val df        = spark.range(0, 200).selectExpr("id", "CAST(id AS STRING) as text")

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .option(ArrowFfiWriteConfig.KEY_BATCH_SIZE, "32") // Small batch size to test multiple flushes
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 200
    }
  }

  // ===== Arrow FFI Partitioned Writes =====

  test("Arrow FFI partitioned write with single partition column") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_arrow_partitioned"
      val df = spark
        .range(0, 100)
        .selectExpr(
          "id",
          "CAST(id AS STRING) as text",
          "CAST(id % 5 AS STRING) as category"
        )

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .partitionBy("category")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 100

      // Verify partition pruning works
      val filtered = result.filter("category = '0'")
      filtered.count() shouldBe 20
    }
  }

  test("Arrow FFI partitioned write with multiple partition columns") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_arrow_multi_part"
      val df = spark
        .range(0, 100)
        .selectExpr(
          "id",
          "CAST(id AS STRING) as text",
          "CAST(id % 3 AS STRING) as region",
          "CAST(id % 2 AS STRING) as type"
        )

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .partitionBy("region", "type")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 100
    }
  }

  // ===== Arrow FFI Statistics & Filters =====

  test("Arrow FFI statistics enable data skipping") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_arrow_stats"
      val df = spark
        .range(0, 1000)
        .selectExpr("id", "CAST(id AS STRING) as text")

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 1000

      // Filter pushdown should work with stats (use collect to avoid aggregate pushdown issues)
      val filtered = result.filter("id = 500").collect()
      filtered.length shouldBe 1
      filtered.head.getLong(0) shouldBe 500L
    }
  }

  // ===== Arrow FFI JSON/Complex Fields =====

  test("Arrow FFI JSON fields (Struct) write and read correctly") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_arrow_json"
      val df = spark.sql(
        """SELECT
          |  id,
          |  named_struct('x', CAST(id AS INT), 'y', CAST(id AS STRING)) as obj
          |FROM range(50)""".stripMargin
      )

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 50
    }
  }

  // ===== Arrow FFI + OptimizedWrite =====

  test("OptimizedWrite + Arrow FFI combination") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_arrow_optimized"
      val df        = spark.range(0, 200).selectExpr("id", "CAST(id AS STRING) as text")

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .option("spark.indextables.write.optimizeWrite.enabled", "true")
        .option("spark.indextables.write.optimizeWrite.targetSplitSize", "1G")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 200
    }
  }

}
