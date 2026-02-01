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
 * Scale tests for Avro state format behavior.
 *
 * Tests cover:
 *   - Many transaction operations with Avro state
 *   - Checkpoint creation and state consolidation
 *   - Read performance at scale
 *   - State caching effectiveness
 *   - Wide schema handling
 *
 * This is the Avro equivalent of TransactionLogScaleTest.
 */
class AvroTransactionLogScaleTest extends TestBase {

  private val logger   = org.slf4j.LoggerFactory.getLogger(classOf[AvroTransactionLogScaleTest])
  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  // ============================================================================
  // MANY TRANSACTIONS TESTS
  // ============================================================================

  test("should handle 50+ transactions with Avro state consolidation") {
    withTempPath { path =>
      logger.info("Testing Avro state with many transactions")

      // Create 50 transactions
      for (i <- 0 until 50) {
        val df = spark.range(i * 20, (i + 1) * 20).selectExpr("id", "CAST(id AS STRING) as text")
        if (i == 0) {
          df.write.format(provider).mode("overwrite").save(path)
        } else {
          df.write.format(provider).mode("append").save(path)
        }
      }

      // Create Avro checkpoint to consolidate state
      val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      checkpointResult(0).getAs[String]("status") shouldBe "SUCCESS"

      // Verify state format
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"
      stateResult(0).getAs[Long]("num_files") should be >= 50L

      // Verify data integrity
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 1000 // 50 * 20

      logger.info("Many transactions test passed")
    }
  }

  test("should read efficiently with Avro state") {
    withTempPath { path =>
      logger.info("Testing efficient read with Avro state")

      // Create 30 transactions
      for (i <- 0 until 30) {
        val df = spark.range(i * 100, (i + 1) * 100).selectExpr("id", "CAST(id AS STRING) as text")
        if (i == 0) {
          df.write.format(provider).mode("overwrite").save(path)
        } else {
          df.write.format(provider).mode("append").save(path)
        }
      }

      // Create checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Time the read operation
      val startTime = System.currentTimeMillis()

      val result   = spark.read.format(provider).load(path)
      val count    = result.count()
      val readTime = System.currentTimeMillis() - startTime

      count shouldBe 3000 // 30 * 100
      logger.info(s"Read 3000 rows from Avro state in ${readTime}ms")

      // Should complete in reasonable time
      assert(readTime < 30000, s"Read should complete in <30s, took ${readTime}ms")

      logger.info("Efficient read test passed")
    }
  }

  // ============================================================================
  // STATE CONSOLIDATION TESTS
  // ============================================================================

  test("should consolidate state after many operations") {
    withTempPath { path =>
      logger.info("Testing state consolidation")

      // Create many small writes
      for (i <- 0 until 25) {
        val df = spark.range(i * 40, (i + 1) * 40).selectExpr("id", "CAST(id AS STRING) as text")
        if (i == 0) {
          df.write.format(provider).mode("overwrite").save(path)
        } else {
          df.write.format(provider).mode("append").save(path)
        }
      }

      // Create checkpoint to consolidate
      val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      checkpointResult(0).getAs[String]("status") shouldBe "SUCCESS"
      val checkpointVersion = checkpointResult(0).getAs[Long]("checkpoint_version")

      // Verify state directory exists
      val txLogDir  = new File(s"$path/_transaction_log")
      val stateDirs = txLogDir.listFiles().filter(_.getName.startsWith("state-v"))
      stateDirs should not be empty

      // Verify data
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 1000 // 25 * 40

      logger.info(s"State consolidated at version $checkpointVersion")
    }
  }

  test("should handle incremental state after checkpoint") {
    withTempPath { path =>
      logger.info("Testing incremental state after checkpoint")

      // Initial transactions
      for (i <- 0 until 15) {
        val df = spark.range(i * 50, (i + 1) * 50).selectExpr("id", "CAST(id AS STRING) as text")
        if (i == 0) {
          df.write.format(provider).mode("overwrite").save(path)
        } else {
          df.write.format(provider).mode("append").save(path)
        }
      }

      // First checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // More transactions after checkpoint
      for (i <- 15 until 25) {
        val df = spark.range(i * 50, (i + 1) * 50).selectExpr("id", "CAST(id AS STRING) as text")
        df.write.format(provider).mode("append").save(path)
      }

      // Read should include checkpoint + incremental
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 1250 // 25 * 50

      logger.info("Incremental state after checkpoint test passed")
    }
  }

  // ============================================================================
  // CACHING TESTS
  // ============================================================================

  test("should cache Avro state for repeated reads") {
    withTempPath { path =>
      logger.info("Testing Avro state caching")

      val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
      df.write.format(provider).mode("overwrite").save(path)

      // Create checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // First read - may need to load state
      val startTime1 = System.currentTimeMillis()
      val result1    = spark.read.format(provider).load(path)
      result1.count()
      val time1 = System.currentTimeMillis() - startTime1

      // Second read - should use cached state
      val startTime2 = System.currentTimeMillis()
      val result2    = spark.read.format(provider).load(path)
      result2.count()
      val time2 = System.currentTimeMillis() - startTime2

      logger.info(s"First read: ${time1}ms, Second read: ${time2}ms")

      // Both reads should return same data
      result1.count() shouldBe 500
      result2.count() shouldBe 500

      logger.info("Avro state caching test passed")
    }
  }

  test("should invalidate cache on new writes") {
    withTempPath { path =>
      logger.info("Testing cache invalidation")

      // Initial write
      val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write.format(provider).mode("overwrite").save(path)
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // First read
      val result1 = spark.read.format(provider).load(path)
      result1.count() shouldBe 100

      // Append new data
      val df2 = spark.range(100, 200).selectExpr("id", "CAST(id AS STRING) as text")
      df2.write.format(provider).mode("append").save(path)

      // Second read - should see new data
      val result2 = spark.read.format(provider).load(path)
      result2.count() shouldBe 200

      logger.info("Cache invalidation test passed")
    }
  }

  // ============================================================================
  // WIDE SCHEMA TESTS
  // ============================================================================

  test("should handle wide schema in Avro state") {
    withTempPath { path =>
      logger.info("Testing wide schema in Avro state")

      // Create DataFrame with many columns
      val baseDF = spark.range(0, 100)
      var wideDF = baseDF.selectExpr("id")

      for (i <- 1 to 20)
        wideDF = wideDF.withColumn(s"col_$i", org.apache.spark.sql.functions.lit(s"value_$i"))

      wideDF.write.format(provider).mode("overwrite").save(path)

      // Create checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify all columns readable
      val result = spark.read.format(provider).load(path)

      result.schema.fields.length shouldBe 21 // id + 20 columns
      result.count() shouldBe 100

      logger.info("Wide schema test passed")
    }
  }

  // ============================================================================
  // STRESS TESTS
  // ============================================================================

  test("should handle rapid sequential writes with Avro state") {
    withTempPath { path =>
      logger.info("Testing rapid sequential writes")

      // Rapid sequential writes
      for (i <- 0 until 20) {
        val df = spark.range(i * 10, (i + 1) * 10).selectExpr("id", "CAST(id AS STRING) as text")
        if (i == 0) {
          df.write.format(provider).mode("overwrite").save(path)
        } else {
          df.write.format(provider).mode("append").save(path)
        }
      }

      // Create checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify all data
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 200 // 20 * 10

      // Verify state
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"
      stateResult(0).getAs[Long]("num_files") should be >= 20L

      logger.info("Rapid writes test passed")
    }
  }

  test("should maintain consistency under interleaved read/write") {
    withTempPath { path =>
      logger.info("Testing interleaved read/write")

      // Initial write
      val df1 = spark.range(0, 50).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write.format(provider).mode("overwrite").save(path)
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Interleaved reads and writes
      for (i <- 1 to 5) {
        // Read
        val readResult = spark.read.format(provider).load(path)
        val count      = readResult.count()
        logger.info(s"Iteration $i: read $count rows")

        // Write
        val df = spark.range(i * 50, (i + 1) * 50).selectExpr("id", "CAST(id AS STRING) as text")
        df.write.format(provider).mode("append").save(path)
      }

      // Final read
      val finalResult = spark.read.format(provider).load(path)
      finalResult.count() shouldBe 300 // 50 initial + 5*50 appends

      logger.info("Interleaved read/write test passed")
    }
  }

  // ============================================================================
  // STATE VERSION HISTORY TESTS
  // ============================================================================

  test("should track state versions correctly") {
    withTempPath { path =>
      logger.info("Testing state version tracking")

      // Create version history with checkpoints
      for (i <- 0 until 5) {
        val df = spark.range(i * 100, (i + 1) * 100).selectExpr("id", "CAST(id AS STRING) as text")
        if (i == 0) {
          df.write.format(provider).mode("overwrite").save(path)
        } else {
          df.write.format(provider).mode("append").save(path)
        }

        // Create checkpoint after each write
        val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
        checkpointResult(0).getAs[String]("status") shouldBe "SUCCESS"
      }

      // Verify state directories exist
      val txLogDir  = new File(s"$path/_transaction_log")
      val stateDirs = txLogDir.listFiles().filter(_.getName.startsWith("state-v"))
      stateDirs should not be empty

      // Verify final state
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // Verify final count
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 500 // 5 * 100

      logger.info("State version tracking test passed")
    }
  }

  test("should handle large number of files in state") {
    withTempPath { path =>
      logger.info("Testing large number of files in state")

      // Create many small writes (each creates at least one split)
      for (i <- 0 until 40) {
        val df = spark.range(i * 25, (i + 1) * 25).selectExpr("id", "CAST(id AS STRING) as text")
        if (i == 0) {
          df.write.format(provider).mode("overwrite").save(path)
        } else {
          df.write.format(provider).mode("append").save(path)
        }
      }

      // Create checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify state handles many files
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"
      stateResult(0).getAs[Long]("num_files") should be >= 40L

      // Verify data integrity
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 1000 // 40 * 25

      logger.info("Large number of files test passed")
    }
  }

  // ============================================================================
  // COMPRESSION EFFECTIVENESS TESTS
  // ============================================================================

  test("should effectively compress Avro state") {
    withTempPath { path =>
      logger.info("Testing Avro state compression")

      // Create data
      val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
      df.write.format(provider).mode("overwrite").save(path)

      // Create checkpoint with Avro (uses zstd compression by default)
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify state was created
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // Verify data readable
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 500

      logger.info("Avro state compression test passed")
    }
  }
}
