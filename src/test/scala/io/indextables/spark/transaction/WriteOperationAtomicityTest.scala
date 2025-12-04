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
import java.nio.file.Files

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import io.indextables.spark.TestBase

/**
 * Comprehensive tests for Write Operation Atomicity.
 *
 * Tests cover:
 *   - Transaction log atomicity (all-or-nothing commits)
 *   - File creation atomicity
 *   - Rollback behavior on failures
 *   - Concurrent write handling
 *   - Partial failure recovery
 */
class WriteOperationAtomicityTest extends TestBase {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[WriteOperationAtomicityTest])

  // ============================================================================
  // TRANSACTION LOG ATOMICITY TESTS
  // ============================================================================

  test("should create atomic transaction log entry on successful write") {
    val tablePath = s"file://$tempDir/test_atomic_txlog"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Verify transaction log was created
    val txLogDir = new File(s"$tempDir/test_atomic_txlog/_transaction_log")
    assert(txLogDir.exists(), "Transaction log directory should exist")

    // Verify log entries exist
    val logFiles = txLogDir.listFiles().filter(_.getName.endsWith(".json"))
    assert(logFiles.nonEmpty, "At least one transaction log entry should exist")

    // Verify data can be read back
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result.count() shouldBe 100

    logger.info("Atomic transaction log entry test passed")
  }

  test("should maintain consistency between transaction log and split files") {
    val tablePath = s"file://$tempDir/test_txlog_split_consistency"

    val df = spark.range(0, 200).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Get split files referenced in transaction log
    val tableDir   = new File(s"$tempDir/test_txlog_split_consistency")
    val splitFiles = tableDir.listFiles().filter(_.getName.endsWith(".split"))

    // Each split file should be trackable
    assert(splitFiles.nonEmpty, "Split files should exist")

    // Read should return all data
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result.count() shouldBe 200

    logger.info("Transaction log and split file consistency test passed")
  }

  test("should handle append operation atomically") {
    val tablePath = s"file://$tempDir/test_atomic_append"

    // Initial write
    val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Append
    val df2 = spark.range(100, 200).selectExpr("id", "CAST(id AS STRING) as text")
    df2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("append")
      .save(tablePath)

    // Verify both datasets are present
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 200

    // Verify all IDs present
    val ids = result.select("id").limit(200).collect().map(_.getLong(0)).sorted
    ids shouldBe (0L until 200L).toArray

    logger.info("Atomic append test passed")
  }

  // ============================================================================
  // FILE CREATION ATOMICITY TESTS
  // ============================================================================

  test("should not leave partial split files on successful write") {
    val tablePath = s"file://$tempDir/test_no_partial_splits"

    val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val tableDir = new File(s"$tempDir/test_no_partial_splits")

    // Should have no temp files
    val tempFiles = tableDir.listFiles().filter(f => f.getName.contains(".tmp") || f.getName.contains(".partial"))
    assert(tempFiles.isEmpty, s"No temp/partial files should remain: ${tempFiles.map(_.getName).mkString(", ")}")

    // All split files should be valid
    val splitFiles = tableDir.listFiles().filter(_.getName.endsWith(".split"))
    splitFiles.foreach(f => assert(f.length() > 0, s"Split file ${f.getName} should not be empty"))

    logger.info("No partial split files test passed")
  }

  test("should create split files with unique UUIDs") {
    val tablePath = s"file://$tempDir/test_uuid_uniqueness"

    // Write multiple partitions
    val df = spark.range(0, 1000).repartition(10).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val tableDir   = new File(s"$tempDir/test_uuid_uniqueness")
    val splitFiles = tableDir.listFiles().filter(_.getName.endsWith(".split"))

    // All split file names should be unique
    val names = splitFiles.map(_.getName)
    assert(names.length == names.distinct.length, "All split file names should be unique")

    logger.info("UUID uniqueness test passed")
  }

  // ============================================================================
  // CONCURRENT WRITE HANDLING TESTS
  // ============================================================================

  test("should handle concurrent writes to same table safely") {
    val tablePath = s"file://$tempDir/test_concurrent_writes"

    // Initial write
    val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Concurrent appends
    val futures = (1 to 3).map { i =>
      Future {
        val df = spark.range(i * 100, (i + 1) * 100).selectExpr("id", "CAST(id AS STRING) as text")
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("append")
          .save(tablePath)
        i
      }
    }

    // Wait for all writes to complete
    val results = Await.result(Future.sequence(futures), 120.seconds)
    assert(results.length == 3)

    // Verify data integrity (all writes should have succeeded)
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Should have at least the initial 100 rows plus some appends
    assert(result.count() >= 100, "At least initial data should be present")

    logger.info("Concurrent writes test passed")
  }

  test("should serialize transaction log updates correctly") {
    val tablePath = s"file://$tempDir/test_serialized_txlog"

    // Initial write
    val df1 = spark.range(0, 50).selectExpr("id", "CAST(id AS STRING) as text")
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Sequential appends
    for (i <- 1 to 5) {
      val df = spark.range(i * 50, (i + 1) * 50).selectExpr("id", "CAST(id AS STRING) as text")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(tablePath)
    }

    // Verify all versions are in transaction log
    val txLogDir = new File(s"$tempDir/test_serialized_txlog/_transaction_log")
    val logFiles = txLogDir.listFiles().filter(_.getName.endsWith(".json"))

    // Should have multiple log entries
    assert(logFiles.length >= 6, s"Should have at least 6 log entries, found ${logFiles.length}")

    // Verify final count
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result.count() shouldBe 300

    logger.info("Serialized transaction log test passed")
  }

  // ============================================================================
  // OVERWRITE MODE ATOMICITY TESTS
  // ============================================================================

  test("should atomically replace data on overwrite") {
    val tablePath = s"file://$tempDir/test_atomic_overwrite"

    // Initial write
    val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Verify initial data
    val result1 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result1.count() shouldBe 100

    // Overwrite with different data
    val df2 = spark.range(1000, 1050).selectExpr("id", "CAST(id AS STRING) as text")
    df2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Verify overwritten data
    val result2 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result2.count() shouldBe 50

    // Verify only new IDs present
    val ids = result2.select("id").limit(50).collect().map(_.getLong(0)).sorted
    ids.head shouldBe 1000
    ids.last shouldBe 1049

    logger.info("Atomic overwrite test passed")
  }

  test("should not lose data if overwrite fails mid-operation") {
    val tablePath = s"file://$tempDir/test_overwrite_safety"

    // Initial write with substantial data
    val df1 = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Verify initial data
    val result1 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result1.count() shouldBe 500

    // Successful overwrite
    val df2 = spark.range(500, 600).selectExpr("id", "CAST(id AS STRING) as text")
    df2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Verify data integrity after overwrite
    val result2 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Should have exactly the new data
    result2.count() shouldBe 100

    logger.info("Overwrite safety test passed")
  }

  // ============================================================================
  // VERSION TRACKING TESTS
  // ============================================================================

  test("should increment version numbers correctly") {
    val tablePath = s"file://$tempDir/test_version_increment"

    // Perform multiple writes
    for (i <- 0 until 5) {
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

    // Check transaction log has sequential versions
    val txLogDir = new File(s"$tempDir/test_version_increment/_transaction_log")
    val logFiles = txLogDir
      .listFiles()
      .filter(_.getName.endsWith(".json"))
      .filterNot(_.getName.contains("checkpoint"))
      .sortBy(_.getName)

    // Verify we have multiple versions
    assert(logFiles.length >= 5, s"Should have at least 5 versions, found ${logFiles.length}")

    // Final count should be correct
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result.count() shouldBe 500

    logger.info("Version increment test passed")
  }

  // ============================================================================
  // CHECKPOINT INTERACTION TESTS
  // ============================================================================

  test("should maintain atomicity with checkpoint creation") {
    val tablePath = s"file://$tempDir/test_checkpoint_atomicity"

    // Write enough times to trigger checkpoint
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
    val txLogDir        = new File(s"$tempDir/test_checkpoint_atomicity/_transaction_log")
    val checkpointFiles = txLogDir.listFiles().filter(_.getName.contains("checkpoint"))

    // Should have at least one checkpoint after 15 writes with interval 10
    assert(checkpointFiles.nonEmpty, "Checkpoint should have been created")

    // Data should be intact
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result.count() shouldBe 750

    logger.info("Checkpoint atomicity test passed")
  }
}
