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
import io.indextables.spark.write.ArrowFfiWriteConfig

class ArrowFfiWriteTest extends TestBase {

  private val FORMAT = "io.indextables.spark.core.IndexTables4SparkTableProvider"

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

  // ===== Parity Test =====

  test("Arrow FFI and TANT batch produce identical query results") {
    withTempPath { path =>
      val tantPath  = s"file://$path/tant_path"
      val arrowPath = s"file://$path/arrow_path"

      val df = spark
        .range(0, 500)
        .selectExpr(
          "id",
          "CAST(id AS STRING) as name",
          "CAST(id * 1.5 AS DOUBLE) as score",
          "CAST(id % 2 = 0 AS BOOLEAN) as active"
        )

      // Write with TANT batch (explicitly disabled)
      df.write
        .mode("overwrite")
        .format(FORMAT)
        .option(ArrowFfiWriteConfig.KEY_ENABLED, "false")
        .save(tantPath)

      // Write with Arrow FFI (default)
      df.write
        .mode("overwrite")
        .format(FORMAT)
        .save(arrowPath)

      val tantResult  = spark.read.format(FORMAT).load(tantPath)
      val arrowResult = spark.read.format(FORMAT).load(arrowPath)

      // Verify row counts match
      tantResult.count() shouldBe arrowResult.count()

      // Verify data matches
      val tantIds  = tantResult.select("id").collect().map(_.getLong(0)).sorted
      val arrowIds = arrowResult.select("id").collect().map(_.getLong(0)).sorted
      tantIds shouldBe arrowIds

      // Verify filter works identically (use collect to avoid aggregate pushdown issues)
      val tantFiltered  = tantResult.filter("id > 400").collect().length
      val arrowFiltered = arrowResult.filter("id > 400").collect().length
      tantFiltered shouldBe arrowFiltered
    }
  }

  // ===== TANT Batch Path (explicit opt-out) =====

  test("TANT batch: non-partitioned write + read roundtrip") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_tant_basic"
      val df        = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .option(ArrowFfiWriteConfig.KEY_ENABLED, "false")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 100

      val ids = result.select("id").collect().map(_.getLong(0)).sorted
      ids shouldBe (0L until 100L).toArray
    }
  }

  test("TANT batch: all primitive types roundtrip") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_tant_all_types"
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
        .option(ArrowFfiWriteConfig.KEY_ENABLED, "false")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 50

      val ids = result.select("id").collect().map(_.getLong(0)).sorted
      ids shouldBe (0L until 50L).toArray
    }
  }

  test("TANT batch: partitioned write with single partition column") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_tant_partitioned"
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
        .option(ArrowFfiWriteConfig.KEY_ENABLED, "false")
        .partitionBy("category")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 100

      val filtered = result.filter("category = '0'")
      filtered.count() shouldBe 20
    }
  }

  test("TANT batch: partitioned write with multiple partition columns") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_tant_multi_part"
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
        .option(ArrowFfiWriteConfig.KEY_ENABLED, "false")
        .partitionBy("region", "type")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 100
    }
  }

  test("TANT batch: statistics enable data skipping") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_tant_stats"
      val df = spark
        .range(0, 1000)
        .selectExpr("id", "CAST(id AS STRING) as text")

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .option(ArrowFfiWriteConfig.KEY_ENABLED, "false")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 1000

      val filtered = result.filter("id = 500").collect()
      filtered.length shouldBe 1
      filtered.head.getLong(0) shouldBe 500L
    }
  }

  test("TANT batch: JSON fields (Struct) write and read correctly") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_tant_json"
      val df = spark.sql(
        """SELECT
          |  id,
          |  named_struct('x', CAST(id AS INT), 'y', CAST(id AS STRING)) as obj
          |FROM range(50)""".stripMargin
      )

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .option(ArrowFfiWriteConfig.KEY_ENABLED, "false")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 50
    }
  }

  test("Arrow FFI vs TANT batch: statistics parity") {
    import io.indextables.spark.transaction.TransactionLog
    import org.apache.spark.sql.util.CaseInsensitiveStringMap
    import scala.jdk.CollectionConverters._

    withTempPath { path =>
      val arrowPath = s"file://$path/test_stats_arrow"
      val tantPath  = s"file://$path/test_stats_tant"
      val df = spark.range(0, 100).selectExpr("id", "CAST(id * 10 AS LONG) as score", "CAST(id AS STRING) as name")

      // Write with both paths
      df.write.mode("overwrite").format(FORMAT).save(arrowPath)
      df.write.mode("overwrite").format(FORMAT).option(ArrowFfiWriteConfig.KEY_ENABLED, "false").save(tantPath)

      def getActions(tablePath: String) = {
        val txLog = new TransactionLog(
          new org.apache.hadoop.fs.Path(tablePath), spark,
          new CaseInsensitiveStringMap(Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava))
        txLog.listFiles()
      }

      val arrowActions = getActions(arrowPath)
      val tantActions  = getActions(tantPath)

      // Both paths should write all 100 records
      arrowActions.flatMap(_.numRecords).sum shouldBe 100
      tantActions.flatMap(_.numRecords).sum shouldBe 100

      // Compute global min/max across all splits for each path
      def globalMin(actions: Seq[io.indextables.spark.transaction.AddAction], col: String): String =
        actions.flatMap(_.minValues).flatMap(_.get(col)).min
      def globalMax(actions: Seq[io.indextables.spark.transaction.AddAction], col: String): String =
        actions.flatMap(_.maxValues).flatMap(_.get(col)).max

      // All splits should have statistics
      arrowActions.foreach { a =>
        a.minValues shouldBe defined
        a.maxValues shouldBe defined
        a.docMappingJson shouldBe defined
        a.footerStartOffset shouldBe defined
        a.footerEndOffset shouldBe defined
      }

      // Global min/max should match between paths
      globalMin(arrowActions, "id") shouldBe globalMin(tantActions, "id")
      globalMax(arrowActions, "id") shouldBe globalMax(tantActions, "id")
      globalMin(arrowActions, "score") shouldBe globalMin(tantActions, "score")
      globalMax(arrowActions, "score") shouldBe globalMax(tantActions, "score")
    }
  }

  test("TANT batch: OptimizedWrite combination") {
    withTempPath { path =>
      val tablePath = s"file://$path/test_tant_optimized"
      val df        = spark.range(0, 200).selectExpr("id", "CAST(id AS STRING) as text")

      df.write
        .mode("overwrite")
        .format(FORMAT)
        .option(ArrowFfiWriteConfig.KEY_ENABLED, "false")
        .option("spark.indextables.write.optimizeWrite.enabled", "true")
        .option("spark.indextables.write.optimizeWrite.targetSplitSize", "1G")
        .save(tablePath)

      val result = spark.read.format(FORMAT).load(tablePath)
      result.count() shouldBe 200
    }
  }
}
