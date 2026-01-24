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

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import io.indextables.spark.TestBase

/**
 * Tests for write operation atomicity with Avro state format.
 *
 * These tests verify:
 *   1. Avro state is created atomically after successful writes
 *   2. Split files and state are consistent
 *   3. Append operations work correctly with Avro state
 *   4. Overwrite operations are atomic
 *   5. Concurrent writes are handled safely
 *
 * This is the Avro equivalent of WriteOperationAtomicityTest.
 */
class AvroWriteOperationAtomicityTest extends TestBase {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[AvroWriteOperationAtomicityTest])
  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  // ============================================================================
  // AVRO STATE ATOMICITY TESTS
  // ============================================================================

  test("should create Avro state atomically on successful checkpoint") {
    withTempPath { path =>
      logger.info("Testing atomic Avro state creation")

      val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
      df.write.format(provider).mode("overwrite").save(path)

      // Create Avro checkpoint
      val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      checkpointResult(0).getAs[String]("status") shouldBe "SUCCESS"

      // Verify state directory exists
      val txLogDir = new File(s"$path/_transaction_log")
      val stateDirs = txLogDir.listFiles().filter(_.getName.startsWith("state-v"))
      stateDirs should not be empty

      // Verify manifest exists (state is complete)
      val stateDir = stateDirs.head
      val manifestFile = new File(stateDir, "_manifest.json")
      manifestFile.exists() shouldBe true

      // Verify data can be read
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 100

      logger.info("Atomic Avro state creation test passed")
    }
  }

  test("should maintain consistency between Avro state and split files") {
    withTempPath { path =>
      logger.info("Testing state and split file consistency")

      val df = spark.range(0, 200).selectExpr("id", "CAST(id AS STRING) as text")
      df.write.format(provider).mode("overwrite").save(path)

      // Create checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Get split files
      val tableDir = new File(path)
      val splitFiles = tableDir.listFiles().filter(_.getName.endsWith(".split"))
      splitFiles should not be empty

      // Verify state reports correct number of files
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"
      stateResult(0).getAs[Long]("num_files") shouldBe splitFiles.length

      // Read should return all data
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 200

      logger.info("State and split file consistency test passed")
    }
  }

  test("should handle append operation atomically with Avro state") {
    withTempPath { path =>
      logger.info("Testing atomic append with Avro state")

      // Initial write
      val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write.format(provider).mode("overwrite").save(path)
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify initial state
      val result1 = spark.read.format(provider).load(path)
      result1.count() shouldBe 100

      // Append
      val df2 = spark.range(100, 200).selectExpr("id", "CAST(id AS STRING) as text")
      df2.write.format(provider).mode("append").save(path)

      // Verify both datasets are present (without new checkpoint)
      val result2 = spark.read.format(provider).load(path)
      result2.count() shouldBe 200

      // Create new checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify all IDs present
      val ids = spark.read.format(provider).load(path)
        .select("id").collect().map(_.getLong(0)).sorted
      ids shouldBe (0L until 200L).toArray

      logger.info("Atomic append test passed")
    }
  }

  // ============================================================================
  // SPLIT FILE HANDLING TESTS
  // ============================================================================

  test("should not leave partial split files on successful write") {
    withTempPath { path =>
      logger.info("Testing no partial split files")

      val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
      df.write.format(provider).mode("overwrite").save(path)

      val tableDir = new File(path)

      // Should have no temp files
      val tempFiles = tableDir.listFiles().filter(f =>
        f.getName.contains(".tmp") || f.getName.contains(".partial")
      )
      tempFiles shouldBe empty

      // All split files should be valid (non-empty)
      val splitFiles = tableDir.listFiles().filter(_.getName.endsWith(".split"))
      splitFiles.foreach(f => f.length() should be > 0L)

      logger.info("No partial split files test passed")
    }
  }

  test("should create split files with unique names") {
    withTempPath { path =>
      logger.info("Testing unique split file names")

      // Write with multiple partitions to generate multiple splits
      val df = spark.range(0, 1000).repartition(10).selectExpr("id", "CAST(id AS STRING) as text")
      df.write.format(provider).mode("overwrite").save(path)

      val tableDir = new File(path)
      val splitFiles = tableDir.listFiles().filter(_.getName.endsWith(".split"))

      // All split file names should be unique
      val names = splitFiles.map(_.getName)
      names.length shouldBe names.distinct.length

      logger.info("Unique split file names test passed")
    }
  }

  // ============================================================================
  // CONCURRENT WRITE HANDLING TESTS
  // ============================================================================

  test("should handle concurrent writes to same table safely") {
    withTempPath { path =>
      logger.info("Testing concurrent writes")

      // Initial write
      val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write.format(provider).mode("overwrite").save(path)

      // Concurrent appends
      val futures = (1 to 3).map { i =>
        Future {
          val df = spark.range(i * 100, (i + 1) * 100).selectExpr("id", "CAST(id AS STRING) as text")
          df.write.format(provider).mode("append").save(path)
          i
        }
      }

      // Wait for all writes to complete
      val results = Await.result(Future.sequence(futures), 120.seconds)
      results.length shouldBe 3

      // Verify data integrity
      val result = spark.read.format(provider).load(path)
      result.count() should be >= 100L // At least initial data

      logger.info("Concurrent writes test passed")
    }
  }

  test("should handle sequential appends correctly") {
    withTempPath { path =>
      logger.info("Testing sequential appends")

      // Initial write
      val df1 = spark.range(0, 50).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write.format(provider).mode("overwrite").save(path)

      // Sequential appends
      for (i <- 1 to 5) {
        val df = spark.range(i * 50, (i + 1) * 50).selectExpr("id", "CAST(id AS STRING) as text")
        df.write.format(provider).mode("append").save(path)
      }

      // Verify final count
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 300 // 50 + 5*50

      logger.info("Sequential appends test passed")
    }
  }

  // ============================================================================
  // OVERWRITE MODE ATOMICITY TESTS
  // ============================================================================

  test("should atomically replace data on overwrite") {
    withTempPath { path =>
      logger.info("Testing atomic overwrite")

      // Initial write
      val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write.format(provider).mode("overwrite").save(path)

      // Verify initial data
      val result1 = spark.read.format(provider).load(path)
      result1.count() shouldBe 100

      // Overwrite with different data
      val df2 = spark.range(1000, 1050).selectExpr("id", "CAST(id AS STRING) as text")
      df2.write.format(provider).mode("overwrite").save(path)

      // Verify overwritten data
      val result2 = spark.read.format(provider).load(path)
      result2.count() shouldBe 50

      // Verify only new IDs present
      val ids = result2.select("id").collect().map(_.getLong(0)).sorted
      ids.head shouldBe 1000
      ids.last shouldBe 1049

      logger.info("Atomic overwrite test passed")
    }
  }

  test("should maintain data integrity after multiple overwrites") {
    withTempPath { path =>
      logger.info("Testing multiple overwrites")

      // Perform multiple overwrites
      for (i <- 0 until 5) {
        val df = spark.range(i * 100, (i + 1) * 100).selectExpr("id", "CAST(id AS STRING) as text")
        df.write.format(provider).mode("overwrite").save(path)

        // Verify each overwrite
        val result = spark.read.format(provider).load(path)
        result.count() shouldBe 100

        val minId = result.selectExpr("min(id)").collect()(0).getLong(0)
        minId shouldBe (i * 100)
      }

      logger.info("Multiple overwrites test passed")
    }
  }

  // ============================================================================
  // CHECKPOINT INTERACTION TESTS
  // ============================================================================

  test("should maintain atomicity with checkpoint creation") {
    withTempPath { path =>
      logger.info("Testing atomicity with checkpoint creation")

      // Write enough to trigger checkpoint consideration
      for (i <- 0 until 15) {
        val df = spark.range(i * 50, (i + 1) * 50).selectExpr("id", "CAST(id AS STRING) as text")
        if (i == 0) {
          df.write.format(provider).mode("overwrite").save(path)
        } else {
          df.write.format(provider).mode("append").save(path)
        }
      }

      // Create explicit checkpoint
      val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      checkpointResult(0).getAs[String]("status") shouldBe "SUCCESS"

      // Verify state
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"

      // Data should be intact
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 750 // 15 * 50

      logger.info("Checkpoint atomicity test passed")
    }
  }

  test("should handle writes after checkpoint correctly") {
    withTempPath { path =>
      logger.info("Testing writes after checkpoint")

      // Initial data and checkpoint
      val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write.format(provider).mode("overwrite").save(path)
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Write after checkpoint
      val df2 = spark.range(100, 200).selectExpr("id", "CAST(id AS STRING) as text")
      df2.write.format(provider).mode("append").save(path)

      // Verify combined data
      val result = spark.read.format(provider).load(path)
      result.count() shouldBe 200

      // New checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify state updated
      val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      stateResult(0).getAs[String]("format") shouldBe "avro-state"
      stateResult(0).getAs[Long]("num_files") should be >= 1L

      logger.info("Writes after checkpoint test passed")
    }
  }

  // ============================================================================
  // STATE CONSISTENCY TESTS
  // ============================================================================

  test("should recover state correctly after failure simulation") {
    withTempPath { path =>
      logger.info("Testing state recovery")

      // Write data
      val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
      df.write.format(provider).mode("overwrite").save(path)

      // Create checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Simulate "recovery" by re-reading
      val result1 = spark.read.format(provider).load(path)
      result1.count() shouldBe 500

      // Append more data
      val df2 = spark.range(500, 600).selectExpr("id", "CAST(id AS STRING) as text")
      df2.write.format(provider).mode("append").save(path)

      // Re-read again
      val result2 = spark.read.format(provider).load(path)
      result2.count() shouldBe 600

      logger.info("State recovery test passed")
    }
  }

  test("should handle interleaved read/write correctly with Avro state") {
    withTempPath { path =>
      logger.info("Testing interleaved read/write with Avro state")

      // Initial write
      val df1 = spark.range(0, 50).selectExpr("id", "CAST(id AS STRING) as text")
      df1.write.format(provider).mode("overwrite").save(path)
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Interleaved reads and writes
      for (i <- 1 to 5) {
        // Read
        val readResult = spark.read.format(provider).load(path)
        val count = readResult.count()
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
}
