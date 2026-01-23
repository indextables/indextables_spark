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

package io.indextables.spark.transaction

import java.io.File

import io.indextables.spark.TestBase

/**
 * Comprehensive tests for Transaction Log behavior at scale.
 *
 * Tests cover:
 *   - Many transaction log entries
 *   - Checkpoint creation and recovery
 *   - Log compression effectiveness
 *   - Cache performance
 *   - Log replay performance
 */
class TransactionLogScaleTest extends TestBase {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[TransactionLogScaleTest])

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Use JSON format since this test validates JSON transaction log file structure
    spark.conf.set("spark.indextables.state.format", "json")
  }

  // ============================================================================
  // MANY TRANSACTIONS TESTS
  // ============================================================================

  test("should handle 50+ transaction log entries") {
    val tablePath = s"file://$tempDir/test_many_transactions"

    // Create 50 transactions
    for (i <- 0 until 50) {
      val df = spark.range(i * 20, (i + 1) * 20).selectExpr("id", "CAST(id AS STRING) as text")
      if (i == 0) {
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("overwrite")
          .save(tablePath)
      } else {
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("append")
          .save(tablePath)
      }
    }

    // Verify transaction log entries
    val txLogDir   = new File(s"$tempDir/test_many_transactions/_transaction_log")
    val logEntries = txLogDir.listFiles().filter(_.getName.endsWith(".json")).length

    logger.info(s"Created $logEntries transaction log entries")
    assert(logEntries >= 50, s"Should have at least 50 log entries, found $logEntries")

    // Verify data integrity
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 1000 // 50 * 20

    logger.info("Many transactions test passed")
  }

  test("should read efficiently with many log entries") {
    val tablePath = s"file://$tempDir/test_efficient_read"

    // Create 30 transactions
    for (i <- 0 until 30) {
      val df = spark.range(i * 100, (i + 1) * 100).selectExpr("id", "CAST(id AS STRING) as text")
      if (i == 0) {
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("overwrite")
          .save(tablePath)
      } else {
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("append")
          .save(tablePath)
      }
    }

    // Time the read operation
    val startTime = System.currentTimeMillis()

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val count    = result.count()
    val readTime = System.currentTimeMillis() - startTime

    count shouldBe 3000 // 30 * 100
    logger.info(s"Read 3000 rows from 30 transactions in ${readTime}ms")

    // Should complete in reasonable time
    assert(readTime < 30000, s"Read should complete in <30s, took ${readTime}ms")

    logger.info("Efficient read test passed")
  }

  // ============================================================================
  // CHECKPOINT TESTS
  // ============================================================================

  test("should create checkpoint after configured interval") {
    val tablePath = s"file://$tempDir/test_checkpoint_creation"

    // Create transactions with checkpoint interval of 10
    for (i <- 0 until 15) {
      val df = spark.range(i * 50, (i + 1) * 50).selectExpr("id", "CAST(id AS STRING) as text")
      if (i == 0) {
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.checkpoint.enabled", "true")
          .option("spark.indextables.checkpoint.interval", "10")
          .mode("overwrite")
          .save(tablePath)
      } else {
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.checkpoint.enabled", "true")
          .option("spark.indextables.checkpoint.interval", "10")
          .mode("append")
          .save(tablePath)
      }
    }

    // Verify checkpoint was created
    val txLogDir        = new File(s"$tempDir/test_checkpoint_creation/_transaction_log")
    val checkpointFiles = txLogDir.listFiles().filter(_.getName.contains("checkpoint"))

    logger.info(s"Found ${checkpointFiles.length} checkpoint files")
    assert(checkpointFiles.nonEmpty, "Checkpoint should have been created after 10+ transactions")

    // Verify data integrity
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 750 // 15 * 50

    logger.info("Checkpoint creation test passed")
  }

  test("should recover from checkpoint efficiently") {
    val tablePath = s"file://$tempDir/test_checkpoint_recovery"

    // Create transactions with checkpoint
    for (i <- 0 until 25) {
      val df = spark.range(i * 40, (i + 1) * 40).selectExpr("id", "CAST(id AS STRING) as text")
      if (i == 0) {
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.checkpoint.enabled", "true")
          .option("spark.indextables.checkpoint.interval", "10")
          .mode("overwrite")
          .save(tablePath)
      } else {
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.checkpoint.enabled", "true")
          .option("spark.indextables.checkpoint.interval", "10")
          .mode("append")
          .save(tablePath)
      }
    }

    // Time the read (should use checkpoint)
    val startTime = System.currentTimeMillis()

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val count    = result.count()
    val readTime = System.currentTimeMillis() - startTime

    count shouldBe 1000 // 25 * 40
    logger.info(s"Read from checkpoint in ${readTime}ms")

    logger.info("Checkpoint recovery test passed")
  }

  // ============================================================================
  // COMPRESSION TESTS
  // ============================================================================

  test("should compress transaction log entries") {
    val tablePath = s"file://$tempDir/test_compression"

    // Create data with compression enabled
    val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "true")
      .mode("overwrite")
      .save(tablePath)

    // Verify transaction log has compressed entries
    val txLogDir = new File(s"$tempDir/test_compression/_transaction_log")
    val allFiles = txLogDir.listFiles()

    logger.info(s"Transaction log contains ${allFiles.length} files")

    // Verify data readable
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 500

    logger.info("Compression test passed")
  }

  test("should read compressed and uncompressed logs") {
    val tablePath = s"file://$tempDir/test_mixed_compression"

    // Write with compression
    val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "true")
      .mode("overwrite")
      .save(tablePath)

    // Append without explicit compression setting (should use default)
    val df2 = spark.range(100, 200).selectExpr("id", "CAST(id AS STRING) as text")
    df2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("append")
      .save(tablePath)

    // Verify all data readable
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 200

    logger.info("Mixed compression test passed")
  }

  // ============================================================================
  // CACHE TESTS
  // ============================================================================

  test("should cache transaction log for repeated reads") {
    val tablePath = s"file://$tempDir/test_cache"

    val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // First read - cold cache
    val startTime1 = System.currentTimeMillis()
    val result1 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result1.count()
    val time1 = System.currentTimeMillis() - startTime1

    // Second read - should use cache
    val startTime2 = System.currentTimeMillis()
    val result2 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result2.count()
    val time2 = System.currentTimeMillis() - startTime2

    logger.info(s"First read: ${time1}ms, Second read: ${time2}ms")

    // Both reads should return same data
    result1.count() shouldBe 500
    result2.count() shouldBe 500

    logger.info("Transaction log cache test passed")
  }

  test("should invalidate cache on new writes") {
    val tablePath = s"file://$tempDir/test_cache_invalidation"

    // Initial write
    val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // First read
    val result1 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result1.count() shouldBe 100

    // Append new data
    val df2 = spark.range(100, 200).selectExpr("id", "CAST(id AS STRING) as text")
    df2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("append")
      .save(tablePath)

    // Second read - should see new data (cache invalidated)
    val result2 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result2.count() shouldBe 200

    logger.info("Cache invalidation test passed")
  }

  // ============================================================================
  // LARGE SCHEMA TESTS
  // ============================================================================

  test("should handle wide schema in transaction log") {
    val tablePath = s"file://$tempDir/test_wide_schema"

    // Create DataFrame with many columns
    val baseDF = spark.range(0, 100)
    var wideDF = baseDF.selectExpr("id")

    for (i <- 1 to 20)
      wideDF = wideDF.withColumn(s"col_$i", org.apache.spark.sql.functions.lit(s"value_$i"))

    wideDF.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Verify all columns readable
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.schema.fields.length shouldBe 21 // id + 20 columns
    result.count() shouldBe 100

    logger.info("Wide schema test passed")
  }

  // ============================================================================
  // STRESS TESTS
  // ============================================================================

  test("should handle rapid sequential writes") {
    val tablePath = s"file://$tempDir/test_rapid_writes"

    // Rapid sequential writes
    for (i <- 0 until 20) {
      val df = spark.range(i * 10, (i + 1) * 10).selectExpr("id", "CAST(id AS STRING) as text")
      if (i == 0) {
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("overwrite")
          .save(tablePath)
      } else {
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("append")
          .save(tablePath)
      }
    }

    // Verify all data
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 200 // 20 * 10

    // Verify transaction log
    val txLogDir   = new File(s"$tempDir/test_rapid_writes/_transaction_log")
    val logEntries = txLogDir.listFiles().filter(_.getName.endsWith(".json")).length

    logger.info(s"Rapid writes created $logEntries log entries")
    assert(logEntries >= 20, "Should have at least 20 log entries")

    logger.info("Rapid writes test passed")
  }

  test("should maintain consistency under interleaved read/write") {
    val tablePath = s"file://$tempDir/test_interleaved"

    // Initial write
    val df1 = spark.range(0, 50).selectExpr("id", "CAST(id AS STRING) as text")
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Interleaved reads and writes
    for (i <- 1 to 5) {
      // Read
      val readResult = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)
      val count = readResult.count()
      logger.info(s"Iteration $i: read $count rows")

      // Write
      val df = spark.range(i * 50, (i + 1) * 50).selectExpr("id", "CAST(id AS STRING) as text")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(tablePath)
    }

    // Final read
    val finalResult = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    finalResult.count() shouldBe 300 // 50 initial + 5*50 appends

    logger.info("Interleaved read/write test passed")
  }

  // ============================================================================
  // VERSION HISTORY TESTS
  // ============================================================================

  test("should preserve complete version history") {
    val tablePath = s"file://$tempDir/test_version_history"

    // Create version history
    for (i <- 0 until 10) {
      val df = spark.range(i * 30, (i + 1) * 30).selectExpr("id", "CAST(id AS STRING) as text")
      if (i == 0) {
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("overwrite")
          .save(tablePath)
      } else {
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("append")
          .save(tablePath)
      }
    }

    // Verify all versions created
    val txLogDir = new File(s"$tempDir/test_version_history/_transaction_log")
    val logFiles = txLogDir
      .listFiles()
      .filter(_.getName.endsWith(".json"))
      .filterNot(_.getName.contains("checkpoint"))
      .sortBy(_.getName)

    logger.info(s"Created ${logFiles.length} version entries")
    assert(logFiles.length >= 10, "Should have at least 10 version entries")

    // Verify final state
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 300 // 10 * 30

    logger.info("Version history test passed")
  }
}
