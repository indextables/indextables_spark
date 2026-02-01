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
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.avro.{StateConfig, StateWriter, StreamingStateReader}
import io.indextables.spark.TestBase

/**
 * End-to-end integration tests for Avro state format.
 *
 * These tests verify that the Avro state format works correctly with actual Spark read/write operations, including:
 *   - Writing data and creating Avro checkpoints
 *   - Reading data from Avro state format
 *   - Partition filtering with Avro manifest pruning
 *   - Migration from JSON to Avro format
 *   - DESCRIBE STATE command
 */
class AvroStateIntegrationTest extends TestBase {

  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  test("E2E: write data and create Avro checkpoint") {
    withTempPath { path =>
      // Write test data
      val data = (1 to 100).map(i => (i, s"name_$i", i * 10.0))
      val df   = spark.createDataFrame(data).toDF("id", "name", "score")

      df.write
        .format(provider)
        .mode("overwrite")
        .save(path)

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify checkpoint was created in Avro format
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult.length shouldBe 1
      stateResult(0).getAs[String]("format") shouldBe "avro-state"
      stateResult(0).getAs[Long]("num_files") should be > 0L

      // Verify data can be read
      val readDf = spark.read.format(provider).load(path)
      readDf.count() shouldBe 100
    }
  }

  test("E2E: read data from Avro state format") {
    withTempPath { path =>
      // Write test data
      val data = Seq(
        (1, "Alice", 100.0),
        (2, "Bob", 200.0),
        (3, "Charlie", 300.0)
      )
      val df = spark.createDataFrame(data).toDF("id", "name", "score")

      df.write
        .format(provider)
        .mode("overwrite")
        .save(path)

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Read and verify
      val readDf  = spark.read.format(provider).load(path)
      val results = readDf.orderBy("id").collect()

      results.length shouldBe 3
      results(0).getAs[String]("name") shouldBe "Alice"
      results(1).getAs[String]("name") shouldBe "Bob"
      results(2).getAs[String]("name") shouldBe "Charlie"
    }
  }

  test("E2E: partitioned data with Avro state") {
    withTempPath { path =>
      // Create partitioned data
      val data = Seq(
        (1, "Alice", "2024-01-01"),
        (2, "Bob", "2024-01-01"),
        (3, "Charlie", "2024-01-02"),
        (4, "Diana", "2024-01-02"),
        (5, "Eve", "2024-01-03")
      )
      val df = spark.createDataFrame(data).toDF("id", "name", "date")

      // Write partitioned data
      df.write
        .format(provider)
        .mode("overwrite")
        .partitionBy("date")
        .save(path)

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify checkpoint format
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // Read all data
      val allData = spark.read.format(provider).load(path)
      allData.count() shouldBe 5

      // Read with partition filter
      val filtered = spark.read
        .format(provider)
        .load(path)
        .filter(col("date") === "2024-01-01")
        .collect()

      filtered.length shouldBe 2
      filtered.map(_.getAs[String]("name")).sorted shouldBe Array("Alice", "Bob")
    }
  }

  test("E2E: partition filter pushdown with Avro manifest pruning") {
    withTempPath { path =>
      // Create data with multiple partitions to test manifest pruning
      val data = (1 to 100).map { i =>
        val region = s"region_${i % 10}"
        (i, s"name_$i", region)
      }
      val df = spark.createDataFrame(data).toDF("id", "name", "region")

      df.write
        .format(provider)
        .mode("overwrite")
        .partitionBy("region")
        .save(path)

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Query single partition - should benefit from manifest pruning
      val singlePartition = spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "region_0")
        .collect()

      singlePartition.length shouldBe 10

      // Query multiple partitions with IN filter
      val multiPartition = spark.read
        .format(provider)
        .load(path)
        .filter(col("region").isin("region_0", "region_1", "region_2"))
        .collect()

      multiPartition.length shouldBe 30
    }
  }

  test("E2E: incremental writes after Avro checkpoint") {
    withTempPath { path =>
      // Write initial data
      val data1 = Seq((1, "Alice"), (2, "Bob"))
      val df1   = spark.createDataFrame(data1).toDF("id", "name")

      df1.write
        .format(provider)
        .mode("overwrite")
        .save(path)

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Write more data (append)
      val data2 = Seq((3, "Charlie"), (4, "Diana"))
      val df2   = spark.createDataFrame(data2).toDF("id", "name")

      df2.write
        .format(provider)
        .mode("append")
        .save(path)

      // Read all data (checkpoint + incremental)
      val readDf = spark.read.format(provider).load(path)
      readDf.count() shouldBe 4

      val names = readDf.select("name").collect().map(_.getString(0)).sorted
      names shouldBe Array("Alice", "Bob", "Charlie", "Diana")
    }
  }

  test("E2E: aggregation queries with Avro state") {
    withTempPath { path =>
      // Write test data
      val data = (1 to 100).map(i => (i, s"name_$i", i.toDouble))
      val df   = spark.createDataFrame(data).toDF("id", "name", "score")

      df.write
        .format(provider)
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(path)

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Run aggregation
      val readDf = spark.read.format(provider).load(path)
      val aggResult = readDf
        .agg(
          count("*").as("cnt"),
          sum("score").as("total"),
          avg("score").as("avg_score")
        )
        .collect()

      aggResult.length shouldBe 1
      aggResult(0).getAs[Long]("cnt") shouldBe 100
      aggResult(0).getAs[Double]("total") shouldBe 5050.0
      aggResult(0).getAs[Double]("avg_score") shouldBe 50.5
    }
  }

  test("E2E: DESCRIBE STATE shows correct format and metrics") {
    withTempPath { path =>
      // Write test data
      val data = (1 to 50).map(i => (i, s"name_$i"))
      val df   = spark.createDataFrame(data).toDF("id", "name")

      df.write
        .format(provider)
        .mode("overwrite")
        .save(path)

      // Before explicit checkpoint - format depends on whether auto-checkpoint was triggered
      val beforeCheckpoint = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      beforeCheckpoint.length shouldBe 1
      // Format could be 'none' if no checkpoint exists yet, 'json'/'json-multipart' for JSON format,
      // or 'avro-state' if Avro is the default and auto-checkpoint happened
      val formatBefore = beforeCheckpoint(0).getAs[String]("format")
      val validFormats = Set("none", "json", "json-multipart", "avro-state")
      validFormats.contains(formatBefore) shouldBe true

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // After checkpoint - should show 'avro-state'
      val afterCheckpoint = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      afterCheckpoint.length shouldBe 1
      afterCheckpoint(0).getAs[String]("format") shouldBe "avro-state"
      afterCheckpoint(0).getAs[Long]("num_files") should be > 0L
      afterCheckpoint(0).getAs[Int]("num_manifests") should be >= 1
      afterCheckpoint(0).getAs[Boolean]("needs_compaction") shouldBe false
    }
  }

  test("E2E: multiple checkpoints preserve data integrity") {
    withTempPath { path =>
      // Write initial data
      val data1 = (1 to 10).map(i => (i, s"name_$i"))
      spark.createDataFrame(data1).toDF("id", "name").write.format(provider).mode("overwrite").save(path)

      // First checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Append more data
      val data2 = (11 to 20).map(i => (i, s"name_$i"))
      spark.createDataFrame(data2).toDF("id", "name").write.format(provider).mode("append").save(path)

      // Second checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Append even more data
      val data3 = (21 to 30).map(i => (i, s"name_$i"))
      spark.createDataFrame(data3).toDF("id", "name").write.format(provider).mode("append").save(path)

      // Third checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify all data is present
      val readDf = spark.read.format(provider).load(path)
      readDf.count() shouldBe 30

      // Verify state is correct
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"
      stateResult(0).getAs[Long]("num_files") should be > 0L
    }
  }

  test("E2E: filter pushdown works with Avro state") {
    withTempPath { path =>
      // Write test data
      val data = Seq(
        (1, "Alice", 100),
        (2, "Bob", 200),
        (3, "Charlie", 300),
        (4, "Diana", 400),
        (5, "Eve", 500)
      )
      val df = spark.createDataFrame(data).toDF("id", "name", "score")

      df.write
        .format(provider)
        .mode("overwrite")
        .save(path)

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Test various filter types
      val readDf = spark.read.format(provider).load(path)

      // Equality filter
      readDf.filter(col("name") === "Alice").count() shouldBe 1

      // Range filter
      readDf.filter(col("score") > 200).count() shouldBe 3

      // Combined filters
      readDf.filter(col("score") >= 200 && col("score") <= 400).count() shouldBe 3
    }
  }

  test("E2E: large dataset with Avro state") {
    withTempPath { path =>
      // Write larger dataset
      val data = (1 to 1000).map(i => (i, s"name_$i", i % 100, i * 1.5))
      val df   = spark.createDataFrame(data).toDF("id", "name", "category", "value")

      df.write
        .format(provider)
        .mode("overwrite")
        .partitionBy("category")
        .save(path)

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify checkpoint
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // Read and verify count
      val readDf = spark.read.format(provider).load(path)
      readDf.count() shouldBe 1000

      // Query specific partition
      val partitionData = readDf.filter(col("category") === 0).count()
      partitionData shouldBe 10 // 1000 / 100 categories = 10 per category
    }
  }
}
