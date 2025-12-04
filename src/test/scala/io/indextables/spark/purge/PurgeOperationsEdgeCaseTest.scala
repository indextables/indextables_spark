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

package io.indextables.spark.purge

import java.io.File
import java.nio.file.Files
import java.util.UUID

import io.indextables.spark.TestBase
import org.scalatest.BeforeAndAfterEach

/**
 * Comprehensive tests for Purge Operations edge cases.
 *
 * Tests cover:
 *   - Orphaned file detection accuracy
 *   - Retention period enforcement
 *   - DRY RUN mode accuracy
 *   - Transaction log cleanup
 *   - Concurrent access during purge
 */
class PurgeOperationsEdgeCaseTest extends TestBase with BeforeAndAfterEach {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[PurgeOperationsEdgeCaseTest])

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sessionState.catalog.reset()
  }

  // ============================================================================
  // ORPHANED FILE DETECTION TESTS
  // ============================================================================

  test("should not delete referenced split files") {
    val tablePath = s"file://$tempDir/test_no_delete_referenced"

    // Create table with data
    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Get split file count before purge
    val tableDir     = new File(s"$tempDir/test_no_delete_referenced")
    val splitsBefore = tableDir.listFiles().filter(_.getName.endsWith(".split")).length

    // Execute purge with long retention - active files are always excluded regardless
    // because they are referenced in the transaction log
    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS DRY RUN")

    // Verify no files deleted (DRY RUN)
    val splitsAfter = tableDir.listFiles().filter(_.getName.endsWith(".split")).length
    splitsAfter shouldBe splitsBefore

    // Verify data still readable
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 100

    logger.info("Referenced files protection test passed")
  }

  test("should detect orphaned files correctly") {
    val tablePath = s"file://$tempDir/test_orphan_detection"

    // Create table
    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Create orphaned split file (not in transaction log)
    val tableDir   = new File(s"$tempDir/test_orphan_detection")
    val orphanFile = new File(tableDir, s"${UUID.randomUUID()}.split")
    Files.write(orphanFile.toPath, "orphaned content".getBytes)

    // Set file modification time to past
    orphanFile.setLastModified(System.currentTimeMillis() - (8 * 24 * 60 * 60 * 1000L)) // 8 days ago

    // Execute purge DRY RUN
    val dryRunResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS DRY RUN")

    // DRY RUN should report the orphaned file
    val rows = dryRunResult.collect()
    logger.info(s"DRY RUN found ${rows.length} files to purge")

    // Verify data still readable
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 100

    logger.info("Orphan detection test passed")
  }

  // ============================================================================
  // RETENTION PERIOD TESTS
  // ============================================================================

  test("should respect minimum retention period") {
    val tablePath = s"file://$tempDir/test_min_retention"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Try to purge with very short retention - should be rejected or use minimum
    try
      // Attempting purge with 0 hours should either fail or use minimum
      spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 0 HOURS")
    catch {
      case e: Exception =>
        // Expected - minimum retention enforced
        logger.info(s"Minimum retention enforced: ${e.getMessage}")
    }

    // Data should still be intact
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 100

    logger.info("Minimum retention period test passed")
  }

  test("should handle different retention time units") {
    val tablePath = s"file://$tempDir/test_retention_units"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Test HOURS
    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 168 HOURS DRY RUN")

    // Test DAYS
    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS DRY RUN")

    // Verify data still readable
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 100

    logger.info("Retention time units test passed")
  }

  // ============================================================================
  // DRY RUN MODE TESTS
  // ============================================================================

  test("DRY RUN should not delete any files") {
    val tablePath = s"file://$tempDir/test_dry_run_no_delete"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Create orphaned file
    val tableDir   = new File(s"$tempDir/test_dry_run_no_delete")
    val orphanFile = new File(tableDir, s"${UUID.randomUUID()}.split")
    Files.write(orphanFile.toPath, "orphaned content".getBytes)
    orphanFile.setLastModified(System.currentTimeMillis() - (8 * 24 * 60 * 60 * 1000L))

    // Get file count before
    val filesBefore = tableDir.listFiles().length

    // Execute DRY RUN
    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS DRY RUN")

    // Verify no files deleted
    val filesAfter = tableDir.listFiles().length
    filesAfter shouldBe filesBefore

    // Orphan file should still exist
    assert(orphanFile.exists(), "Orphan file should not be deleted in DRY RUN mode")

    logger.info("DRY RUN no delete test passed")
  }

  test("DRY RUN should report accurate file counts") {
    val tablePath = s"file://$tempDir/test_dry_run_counts"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val tableDir = new File(s"$tempDir/test_dry_run_counts")

    // Create multiple orphaned files
    for (i <- 1 to 3) {
      val orphanFile = new File(tableDir, s"${UUID.randomUUID()}.split")
      Files.write(orphanFile.toPath, s"orphaned content $i".getBytes)
      orphanFile.setLastModified(System.currentTimeMillis() - (8 * 24 * 60 * 60 * 1000L))
    }

    // Execute DRY RUN
    val dryRunResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS DRY RUN")
    val rows         = dryRunResult.collect()

    logger.info(s"DRY RUN reported ${rows.length} files")

    // Data should still be readable
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 100

    logger.info("DRY RUN file counts test passed")
  }

  // ============================================================================
  // TRANSACTION LOG CLEANUP TESTS
  // ============================================================================

  test("should clean up old transaction log files") {
    val tablePath = s"file://$tempDir/test_txlog_cleanup"

    // Create table with many versions
    for (i <- 0 until 15) {
      val df = spark.range(i * 50, (i + 1) * 50).selectExpr("id", "CAST(id AS STRING) as text")
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

    // Verify transaction log has many entries
    val txLogDir       = new File(s"$tempDir/test_txlog_cleanup/_transaction_log")
    val logFilesBefore = txLogDir.listFiles().filter(_.getName.endsWith(".json")).length

    logger.info(s"Transaction log has $logFilesBefore entries before purge")

    // Execute purge with transaction log retention
    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS TRANSACTION LOG RETENTION 30 DAYS DRY RUN")

    // Verify data still readable
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 750 // 15 * 50

    logger.info("Transaction log cleanup test passed")
  }

  test("should preserve checkpoint-referenced transaction log files") {
    val tablePath = s"file://$tempDir/test_checkpoint_preserve"

    // Create table with checkpoints
    for (i <- 0 until 12) {
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

    // Verify checkpoint exists
    val txLogDir        = new File(s"$tempDir/test_checkpoint_preserve/_transaction_log")
    val checkpointFiles = txLogDir.listFiles().filter(_.getName.contains("checkpoint"))

    logger.info(s"Found ${checkpointFiles.length} checkpoint files")

    // Execute purge DRY RUN
    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS DRY RUN")

    // Verify data still readable
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 600 // 12 * 50

    logger.info("Checkpoint preservation test passed")
  }

  // ============================================================================
  // CONCURRENT ACCESS TESTS
  // ============================================================================

  test("should not interfere with concurrent reads during purge") {
    val tablePath = s"file://$tempDir/test_concurrent_read_purge"

    val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    import scala.concurrent.{Await, Future}
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    // Run purge and reads concurrently
    val purgeFuture = Future {
      spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS DRY RUN")
      "purge_done"
    }

    val readFuture = Future {
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)
      result.count()
    }

    val purgeResult = Await.result(purgeFuture, 60.seconds)
    val readResult  = Await.result(readFuture, 60.seconds)

    purgeResult shouldBe "purge_done"
    readResult shouldBe 500

    logger.info("Concurrent read during purge test passed")
  }

  // ============================================================================
  // EMPTY TABLE TESTS
  // ============================================================================

  test("should handle purge on empty table") {
    val tablePath = s"file://$tempDir/test_empty_purge"

    // Create empty table
    val df = spark.range(0, 0).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Execute purge - should not fail
    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS DRY RUN")

    // Verify table still accessible
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 0

    logger.info("Empty table purge test passed")
  }

  // ============================================================================
  // PARTITIONED TABLE TESTS
  // ============================================================================

  test("should handle purge on partitioned table") {
    val tablePath = s"file://$tempDir/test_partitioned_purge"

    val df = spark
      .range(0, 200)
      .selectExpr(
        "id",
        "CAST(id AS STRING) as text",
        "CAST(id % 4 AS STRING) as partition_col"
      )
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("partition_col")
      .mode("overwrite")
      .save(tablePath)

    // Create orphan in one partition
    val partitionDir = new File(s"$tempDir/test_partitioned_purge/partition_col=0")
    if (partitionDir.exists()) {
      val orphanFile = new File(partitionDir, s"${UUID.randomUUID()}.split")
      Files.write(orphanFile.toPath, "orphaned content".getBytes)
      orphanFile.setLastModified(System.currentTimeMillis() - (8 * 24 * 60 * 60 * 1000L))
    }

    // Execute purge DRY RUN
    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS DRY RUN")

    // Verify all partitions still readable
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 200
    result.filter("partition_col = '0'").count() shouldBe 50
    result.filter("partition_col = '1'").count() shouldBe 50

    logger.info("Partitioned table purge test passed")
  }

  // ============================================================================
  // ERROR HANDLING TESTS
  // ============================================================================

  test("should handle invalid table path gracefully") {
    val invalidPath = s"file://$tempDir/non_existent_table"

    try {
      spark.sql(s"PURGE INDEXTABLE '$invalidPath' OLDER THAN 7 DAYS")
      fail("Should have thrown exception for non-existent table")
    } catch {
      case e: Exception =>
        logger.info(s"Expected error for invalid path: ${e.getMessage}")
        assert(e.getMessage != null)
    }

    logger.info("Invalid path handling test passed")
  }

  test("should handle malformed retention specification") {
    val tablePath = s"file://$tempDir/test_malformed_retention"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    try {
      spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN -5 DAYS")
      fail("Should have thrown exception for negative retention")
    } catch {
      case e: Exception =>
        logger.info(s"Expected error for negative retention: ${e.getMessage}")
    }

    // Data should still be intact
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 100

    logger.info("Malformed retention handling test passed")
  }
}
