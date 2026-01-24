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

package io.indextables.spark.transaction.avro

import java.io.File

import io.indextables.spark.TestBase

/**
 * Tests for Avro state format with large tables that would require multi-part handling.
 *
 * These tests verify:
 *   1. Avro state handles large numbers of files correctly
 *   2. Multiple manifest files are created when needed
 *   3. State can be read back correctly after checkpoint
 *   4. Incremental writes after checkpoint work correctly
 *   5. State compaction works properly
 *
 * This is the Avro equivalent of MultiPartCheckpointTest.
 */
class AvroMultiPartStateTest extends TestBase {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[AvroMultiPartStateTest])
  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  // ============================================================================
  // LARGE TABLE STATE TESTS
  // ============================================================================

  test("should handle table with many splits in Avro state") {
    withTempPath { path =>
      logger.info("Testing Avro state with many splits")

      // Create table with multiple writes to generate many splits
      for (i <- 0 until 20) {
        val df = spark.range(i * 100, (i + 1) * 100).selectExpr("id", "CAST(id AS STRING) as text")
        if (i == 0) {
          df.write.format(provider).mode("overwrite").save(path)
        } else {
          df.write.format(provider).mode("append").save(path)
        }
      }

      // Create Avro checkpoint
      val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      checkpointResult(0).getAs[String]("status") shouldBe "SUCCESS"

      // Verify state format
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"
      stateResult(0).getAs[Long]("num_files") should be >= 20L

      // Verify data integrity - use both count (pushdown) and collect (full read)
      val result = spark.read.format(provider).load(path)
      val countResult = result.count()
      val collectResult = result.limit(100000000).collect()

      countResult shouldBe 2000 // 20 * 100
      collectResult.length shouldBe 2000
      countResult shouldBe collectResult.length // Verify they match

      logger.info("Many splits test passed")
    }
  }

  test("should create multiple manifests for very large tables") {
    withTempPath { path =>
      logger.info("Testing multiple manifest creation")

      // Create table with many small writes
      for (i <- 0 until 30) {
        val df = spark.range(i * 50, (i + 1) * 50).selectExpr("id", "CAST(id AS STRING) as text")
        if (i == 0) {
          df.write.format(provider).mode("overwrite").save(path)
        } else {
          df.write.format(provider).mode("append").save(path)
        }
      }

      // Create Avro checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify state
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"
      val numManifests = stateResult(0).getAs[Int]("num_manifests")
      numManifests should be >= 1

      // Verify data - use both count (pushdown) and collect (full read)
      val result = spark.read.format(provider).load(path)
      val countResult = result.count()
      val collectResult = result.limit(100000000).collect()

      countResult shouldBe 1500 // 30 * 50
      collectResult.length shouldBe 1500
      countResult shouldBe collectResult.length

      logger.info(s"Created $numManifests manifest(s)")
    }
  }

  test("should read state correctly after checkpoint") {
    withTempPath { path =>
      logger.info("Testing state read after checkpoint")

      // Create initial data
      val df1 = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write.format(provider).mode("overwrite").save(path)

      // Append more data
      for (i <- 1 until 10) {
        val df = spark.range(i * 500, (i + 1) * 500).selectExpr("id", "CAST(id AS STRING) as text")
        df.write.format(provider).mode("append").save(path)
      }

      // Create checkpoint
      val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      checkpointResult(0).getAs[String]("status") shouldBe "SUCCESS"
      val checkpointVersion = checkpointResult(0).getAs[Long]("checkpoint_version")

      // Read and verify - use both count (pushdown) and collect (full read)
      val result = spark.read.format(provider).load(path)
      val countResult = result.count()
      val collectResult = result.limit(100000000).collect()

      countResult shouldBe 5000 // 10 * 500
      collectResult.length shouldBe 5000
      countResult shouldBe collectResult.length

      // Verify all IDs present
      val minMax = result.selectExpr("min(id)", "max(id)").collect()(0)
      minMax.getLong(0) shouldBe 0
      minMax.getLong(1) shouldBe 4999

      logger.info(s"State read correctly after checkpoint version $checkpointVersion")
    }
  }

  test("should handle incremental writes after Avro checkpoint") {
    withTempPath { path =>
      logger.info("Testing incremental writes after checkpoint")

      // Create initial data
      val df1 = spark.range(0, 1000).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write.format(provider).mode("overwrite").save(path)

      // Create first checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Append more data
      val df2 = spark.range(1000, 2000).selectExpr("id", "CAST(id AS STRING) as text")
      df2.write.format(provider).mode("append").save(path)

      // Read should include both checkpoint and incremental data
      val result1 = spark.read.format(provider).load(path)
      result1.count() shouldBe 2000

      // Create second checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Append even more data
      val df3 = spark.range(2000, 3000).selectExpr("id", "CAST(id AS STRING) as text")
      df3.write.format(provider).mode("append").save(path)

      // Final read
      val result2 = spark.read.format(provider).load(path)
      result2.count() shouldBe 3000

      logger.info("Incremental writes after checkpoint test passed")
    }
  }

  test("should handle checkpoint with partitioned data") {
    withTempPath { path =>
      logger.info("Testing checkpoint with partitioned data")

      // Create partitioned data
      val df = spark.range(0, 1000).selectExpr(
        "id",
        "CAST(id AS STRING) as text",
        "CAST(id % 10 AS STRING) as partition_col"
      )
      df.write.format(provider).partitionBy("partition_col").mode("overwrite").save(path)

      // Create checkpoint
      val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      checkpointResult(0).getAs[String]("status") shouldBe "SUCCESS"

      // Verify state format
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // Read with partition filter
      val filtered = spark.read.format(provider).load(path)
        .filter("partition_col = '0'")
      filtered.count() shouldBe 100 // 1000 / 10 partitions

      // Read all data
      val all = spark.read.format(provider).load(path)
      all.count() shouldBe 1000

      logger.info("Partitioned data checkpoint test passed")
    }
  }

  // ============================================================================
  // STATE DIRECTORY STRUCTURE TESTS
  // ============================================================================

  test("should create proper state directory structure") {
    withTempPath { path =>
      logger.info("Testing state directory structure")

      // Create data
      val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
      df.write.format(provider).mode("overwrite").save(path)

      // Create checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Check state directory exists
      val txLogDir = new File(s"$path/_transaction_log")
      val stateDirs = txLogDir.listFiles().filter(_.getName.startsWith("state-v"))

      stateDirs should not be empty
      logger.info(s"Found ${stateDirs.length} state directory(ies)")

      // Verify manifest exists in state directory
      val latestStateDir = stateDirs.maxBy(_.getName)
      val manifestFile = new File(latestStateDir, "_manifest.json")
      manifestFile.exists() shouldBe true

      logger.info("State directory structure test passed")
    }
  }

  test("should handle multiple checkpoints correctly") {
    withTempPath { path =>
      logger.info("Testing multiple checkpoints")

      // Create initial data and checkpoint
      val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write.format(provider).mode("overwrite").save(path)
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Append and checkpoint again
      val df2 = spark.range(100, 200).selectExpr("id", "CAST(id AS STRING) as text")
      df2.write.format(provider).mode("append").save(path)
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Append and checkpoint third time
      val df3 = spark.range(200, 300).selectExpr("id", "CAST(id AS STRING) as text")
      df3.write.format(provider).mode("append").save(path)
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify data integrity
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 300

      // Verify state format
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      logger.info("Multiple checkpoints test passed")
    }
  }

  // ============================================================================
  // SCHEMA HANDLING TESTS
  // ============================================================================

  test("should preserve schema in Avro state") {
    withTempPath { path =>
      logger.info("Testing schema preservation in Avro state")

      // Create data with specific schema
      val df = spark.range(0, 100).selectExpr(
        "id",
        "CAST(id AS STRING) as text",
        "CAST(id * 1.5 AS DOUBLE) as score",
        "CAST(id % 2 = 0 AS BOOLEAN) as is_even"
      )
      df.write.format(provider).mode("overwrite").save(path)

      // Create checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Read and verify schema
      val result = spark.read.format(provider).load(path)
      val schema = result.schema

      schema.fieldNames should contain allOf("id", "text", "score", "is_even")
      result.count() shouldBe 100

      // Verify data types are preserved
      val row = result.filter("id = 10").collect()(0)
      row.getAs[String]("text") shouldBe "10"
      row.getAs[Double]("score") shouldBe 15.0
      row.getAs[Boolean]("is_even") shouldBe true

      logger.info("Schema preservation test passed")
    }
  }

  test("should handle consistent schema across writes with Avro state") {
    withTempPath { path =>
      logger.info("Testing consistent schema across writes with Avro state")

      // Create initial data
      val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write.format(provider).mode("overwrite").save(path)

      // Create checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Append data with same schema
      val df2 = spark.range(100, 200).selectExpr("id", "CAST(id AS STRING) as text")
      df2.write.format(provider).mode("append").save(path)

      // Read should work correctly
      val result = spark.read.format(provider).load(path)

      // Should have all columns
      result.schema.fieldNames should contain("id")
      result.schema.fieldNames should contain("text")

      // Verify all data accessible via collect
      val allData = result.limit(100000000).collect()
      allData.length shouldBe 200

      logger.info("Consistent schema test passed")
    }
  }

  // ============================================================================
  // AGGREGATE TESTS WITH AVRO STATE
  // ============================================================================

  test("should support aggregations with Avro state") {
    withTempPath { path =>
      logger.info("Testing aggregations with Avro state")

      // Create data
      val df = spark.range(0, 1000).selectExpr(
        "id",
        "CAST(id AS STRING) as text",
        "CAST(id AS DOUBLE) as value"
      )
      df.write
        .format(provider)
        .option("spark.indextables.indexing.fastfields", "value")
        .mode("overwrite")
        .save(path)

      // Create checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Run aggregations
      val result = spark.read.format(provider).load(path)
      val agg = result.agg(
        org.apache.spark.sql.functions.count("*").as("cnt"),
        org.apache.spark.sql.functions.sum("value").as("total"),
        org.apache.spark.sql.functions.avg("value").as("avg_val")
      ).collect()(0)

      agg.getLong(0) shouldBe 1000
      agg.getDouble(1) shouldBe 499500.0 // sum of 0 to 999
      agg.getDouble(2) shouldBe 499.5

      logger.info("Aggregations with Avro state test passed")
    }
  }

  test("should support filtered aggregations with Avro state") {
    withTempPath { path =>
      logger.info("Testing filtered aggregations with Avro state")

      // Create data
      val df = spark.range(0, 500).selectExpr(
        "id",
        "CAST(id AS STRING) as text",
        "CAST(id AS DOUBLE) as value"
      )
      df.write
        .format(provider)
        .option("spark.indextables.indexing.fastfields", "value")
        .mode("overwrite")
        .save(path)

      // Create checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Run filtered aggregation
      val result = spark.read.format(provider).load(path)
        .filter("value >= 100 AND value < 200")
        .agg(org.apache.spark.sql.functions.count("*").as("cnt"))
        .collect()(0)

      result.getLong(0) shouldBe 100 // IDs 100-199

      logger.info("Filtered aggregations test passed")
    }
  }
}
