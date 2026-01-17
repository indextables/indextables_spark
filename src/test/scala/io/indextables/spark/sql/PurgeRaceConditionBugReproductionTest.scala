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

package io.indextables.spark.sql

import java.io.File
import java.util.UUID

import scala.util.Random

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.{FileSystem, Path}

import io.indextables.spark.transaction.{TransactionLog, TransactionLogFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

/**
 * Bug reproduction tests for PURGE INDEXTABLE race condition.
 *
 * These tests reproduce the bug where:
 *   1. `getTransactionLogVersionsToDelete()` calculates which versions to delete at time T1 2.
 *      `cleanupOldTransactionLogFiles()` runs at time T2 and INDEPENDENTLY recalculates 3. Due to time passing, more
 *      versions may qualify for deletion at T2 4. `getAllFilesFromVersions()` tries to read versions that were
 *      calculated as "to keep" but deleted 5. Files from those versions are not included in valid set -> deleted ->
 *      DATA CORRUPTION
 *
 * EXPECTED BEHAVIOR:
 *   - With current BUGGY code: Tests should FAIL
 *   - After fix is applied: Tests should PASS
 */
class PurgeRaceConditionBugReproductionTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  var fs: FileSystem      = _
  var tempDir: String     = _

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .appName("PurgeRaceConditionBugReproductionTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.indextables.checkpoint.enabled", "true")
      .config("spark.indextables.checkpoint.interval", "10")
      .getOrCreate()

    fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    tempDir = System.getProperty("java.io.tmpdir") + "/purge_race_bug_repro_" + Random.nextLong().abs
    new File(tempDir).mkdirs()
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    if (tempDir != null) {
      deleteRecursively(new File(tempDir))
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  /**
   * BUG REPRODUCTION TEST 1: Race condition at retention boundary
   *
   * This test reproduces the exact scenario reported by the user:
   *   - Table with date/hour partitions
   *   - ~100 transactions
   *   - PURGE with 2 day retention
   *   - Files still in active transaction get deleted
   *
   * EXPECTED: FAILS with current buggy code, PASSES after fix
   */
  test("BUG REPRO: PURGE should NOT delete files referenced in current transaction state") {
    val tablePath    = s"$tempDir/race_bug_test1"
    val sparkSession = spark
    import sparkSession.implicits._

    // Step 1: Create partitioned table similar to user's setup (date/hour partitions)
    val initialData = Seq(
      ("2024-01-01", 10, "initial1"),
      ("2024-01-01", 11, "initial2"),
      ("2024-01-02", 10, "initial3")
    ).toDF("date", "hour", "value")

    initialData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("date", "hour")
      .mode("overwrite")
      .save(tablePath)

    // Step 2: Create many transactions (like user's ~100)
    for (i <- 1 to 100) {
      val newData = Seq((s"2024-01-${(i % 28) + 1}", i % 24, s"value_$i")).toDF("date", "hour", "value")
      newData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("date", "hour")
        .mode("append")
        .save(tablePath)
    }

    // Step 3: Get files that are in the CURRENT transaction state (should NEVER be deleted)
    val txLog             = getTransactionLog(tablePath)
    val currentStateFiles = txLog.listFiles().map(_.path).toSet
    val currentRowCount = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()

    assert(currentStateFiles.nonEmpty, "Should have files in current state")
    assert(currentRowCount > 0, "Should have rows in table")

    println(s"Current state has ${currentStateFiles.size} files and $currentRowCount rows")

    // Step 4: Age transaction log files to trigger the race condition
    // Set files to be RIGHT AT the retention boundary (48 hours)
    // Some files are set to be just INSIDE the boundary (won't be deleted at T1)
    // But time passes between T1 and T2, potentially causing them to be deleted at T2
    ageTransactionLogFilesToBoundary(tablePath, retentionHours = 48)

    // Step 5: Run PURGE with same parameters as user
    val result = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 2 DAYS TRANSACTION LOG RETENTION 2 DAYS").collect()
    val metrics              = result(0).getStruct(1)
    val orphanedFilesDeleted = metrics.getLong(2)
    val txLogsDeleted        = metrics.getLong(5)

    println(s"PURGE deleted $orphanedFilesDeleted orphaned files and $txLogsDeleted transaction logs")

    // Step 6: CRITICAL CHECK - verify NO files from current state were deleted
    val filesOnDisk = listSplitFilesOnDisk(tablePath)
    val missingFiles = currentStateFiles.filterNot { filePath =>
      val filename = extractFilename(filePath)
      filesOnDisk.exists(_.endsWith(filename))
    }

    // THIS ASSERTION SHOULD FAIL WITH THE CURRENT BUGGY CODE
    assert(
      missingFiles.isEmpty,
      s"BUG DETECTED: ${missingFiles.size} files from current transaction state were deleted!\n" +
        s"Missing files: ${missingFiles.take(5).mkString(", ")}${if (missingFiles.size > 5) "..." else ""}"
    )

    // Step 7: Verify table is still fully readable with same row count
    val postPurgeRowCount = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()

    // THIS SHOULD ALSO FAIL - row count will be lower due to missing files
    assert(
      postPurgeRowCount == currentRowCount,
      s"BUG DETECTED: Row count changed from $currentRowCount to $postPurgeRowCount after PURGE"
    )

    println(s"Test PASSED: All ${currentStateFiles.size} files preserved, row count stable at $currentRowCount")
  }

  /**
   * BUG REPRODUCTION TEST 2: Multiple consecutive PURGE operations
   *
   * Reproduces user's observation:
   *   - First run: "strangely small number" of files deleted
   *   - Second run: deleted files still referenced in active transaction
   *   - Third run: deleted a few more files
   *
   * EXPECTED: FAILS with current buggy code, PASSES after fix
   */
  test("BUG REPRO: Multiple PURGE runs should not cause progressive corruption") {
    val tablePath    = s"$tempDir/multi_purge_test"
    val sparkSession = spark
    import sparkSession.implicits._

    // Create partitioned table with many transactions
    val initialData = Seq(("2024-01-01", 10, "init")).toDF("date", "hour", "value")
    initialData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("date", "hour")
      .mode("overwrite")
      .save(tablePath)

    for (i <- 1 to 100) {
      val newData = Seq((s"2024-01-${(i % 28) + 1}", i % 24, s"val_$i")).toDF("date", "hour", "value")
      newData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("date", "hour")
        .mode("append")
        .save(tablePath)
    }

    // Get baseline row count
    val baselineRowCount = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()

    println(s"Baseline row count: $baselineRowCount")

    // Age files to retention boundary
    ageTransactionLogFilesToBoundary(tablePath, retentionHours = 48)

    // Run PURGE 3 times like user did
    for (run <- 1 to 3) {
      val result = spark
        .sql(
          s"PURGE INDEXTABLE '$tablePath' OLDER THAN 2 DAYS TRANSACTION LOG RETENTION 2 DAYS"
        )
        .collect()

      val metrics         = result(0).getStruct(1)
      val orphanedDeleted = metrics.getLong(2)
      val txLogsDeleted   = metrics.getLong(5)

      val rowCountAfterRun = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)
        .count()

      println(s"Run $run: Deleted $orphanedDeleted orphans, $txLogsDeleted tx logs. Row count: $rowCountAfterRun")

      // Check that row count NEVER decreased (would indicate corruption)
      // THIS SHOULD FAIL ON RUN 2 OR 3 WITH THE BUGGY CODE
      assert(
        rowCountAfterRun == baselineRowCount,
        s"BUG DETECTED on run $run: Row count dropped from $baselineRowCount to $rowCountAfterRun"
      )
    }

    println(s"Test PASSED: Row count stable at $baselineRowCount after 3 PURGE runs")
  }

  /**
   * BUG REPRODUCTION TEST 3: Verifies versionsToDelete is used consistently
   *
   * This test directly checks that when a transaction log version is calculated to be kept, it is NOT deleted by the
   * cleanup operation.
   *
   * EXPECTED: FAILS with current buggy code, PASSES after fix
   */
  test("BUG REPRO: Transaction log versions calculated as 'keep' should not be deleted") {
    val tablePath    = s"$tempDir/consistent_delete_test"
    val sparkSession = spark
    import sparkSession.implicits._

    // Create table with transactions
    val data = Seq(("2024-01-01", 10, "v")).toDF("date", "hour", "value")
    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("date", "hour")
      .mode("overwrite")
      .save(tablePath)

    for (i <- 1 to 50) {
      val newData = Seq((s"2024-01-${(i % 28) + 1}", i % 24, s"val_$i")).toDF("date", "hour", "value")
      newData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("date", "hour")
        .mode("append")
        .save(tablePath)
    }

    // Get transaction log versions before
    val txLog          = getTransactionLog(tablePath)
    val versionsBefore = txLog.getVersions().toSet

    println(s"Versions before PURGE: ${versionsBefore.size}")

    // Get files referenced in the current state
    val currentFiles = txLog.listFiles().map(_.path).toSet

    // Age some files to be at the boundary
    ageTransactionLogFilesToBoundary(tablePath, retentionHours = 48)

    // Run PURGE
    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 2 DAYS TRANSACTION LOG RETENTION 2 DAYS")

    // Verify all current files still exist on disk
    val filesOnDisk = listSplitFilesOnDisk(tablePath)
    val missingFiles = currentFiles.filterNot { filePath =>
      val filename = extractFilename(filePath)
      filesOnDisk.exists(_.endsWith(filename))
    }

    assert(
      missingFiles.isEmpty,
      s"BUG DETECTED: ${missingFiles.size} files from current state are missing!\n" +
        s"Files: ${missingFiles.mkString(", ")}"
    )

    // Verify table is readable
    val rowCount = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()

    assert(rowCount > 0, "Table should still be readable after PURGE")

    println(s"Test PASSED: All current files preserved, $rowCount rows readable")
  }

  /**
   * BUG REPRODUCTION TEST 4: Partitioned table with nested directories
   *
   * Tests that PURGE correctly handles partitioned tables with nested directory structures like date=X/hour=Y.
   *
   * EXPECTED: FAILS with current buggy code if race condition triggers, PASSES after fix
   */
  test("BUG REPRO: PURGE handles nested partition directories correctly") {
    val tablePath    = s"$tempDir/nested_partition_test"
    val sparkSession = spark
    import sparkSession.implicits._

    // Create nested partition structure (date/hour like user's table)
    val data = Seq(
      ("2024-01-01", 10, "value1"),
      ("2024-01-01", 11, "value2"),
      ("2024-01-02", 10, "value3"),
      ("2024-01-02", 11, "value4")
    ).toDF("date", "hour", "value")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("date", "hour")
      .mode("overwrite")
      .save(tablePath)

    // Add more data across partitions
    for (i <- 1 to 50) {
      val newData = Seq((s"2024-01-${(i % 28) + 1}", i % 24, s"val_$i")).toDF("date", "hour", "value")
      newData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("date", "hour")
        .mode("append")
        .save(tablePath)
    }

    // Get baseline
    val baselineRowCount = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()

    val txLog        = getTransactionLog(tablePath)
    val currentFiles = txLog.listFiles().map(_.path).toSet

    println(s"Baseline: $baselineRowCount rows, ${currentFiles.size} files across partitions")

    // Age files
    ageTransactionLogFilesToBoundary(tablePath, retentionHours = 48)

    // Run PURGE
    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 2 DAYS TRANSACTION LOG RETENTION 2 DAYS")

    // Verify all partitioned files still exist
    val filesOnDisk = listSplitFilesOnDisk(tablePath)
    val missingFiles = currentFiles.filterNot { filePath =>
      val filename = extractFilename(filePath)
      filesOnDisk.exists(_.endsWith(filename))
    }

    assert(
      missingFiles.isEmpty,
      s"BUG DETECTED: ${missingFiles.size} partitioned files are missing!\n" +
        s"Files: ${missingFiles.take(5).mkString(", ")}"
    )

    // Verify data integrity
    val postPurgeRowCount = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()

    assert(
      postPurgeRowCount == baselineRowCount,
      s"BUG DETECTED: Row count changed from $baselineRowCount to $postPurgeRowCount"
    )

    println(s"Test PASSED: Nested partitions handled correctly, $postPurgeRowCount rows intact")
  }

  // ============ Helper Methods ============

  private def getTransactionLog(tablePath: String): TransactionLog = {
    val emptyMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    TransactionLogFactory.create(new Path(tablePath), spark, emptyMap)
  }

  /**
   * Ages BOTH transaction log files AND split files to be at the retention boundary.
   *
   * CRITICAL: Both must be aged to reproduce the bug!
   *   - Transaction log files: controls which tx logs are deleted
   *   - Split files: controls which orphaned files are eligible for deletion
   *
   * In production, both would be old (data written days ago). The race condition occurs when:
   *   - Some tx log files are set to be just INSIDE the boundary at T1
   *   - Time passes between getTransactionLogVersionsToDelete() and cleanupOldTransactionLogFiles()
   *   - At T2, those files may now be OUTSIDE the boundary and get deleted
   *   - getAllFilesFromVersions() tries to read them but they're gone -> files become orphaned
   *   - Those orphaned files are also old, so they get deleted -> DATA CORRUPTION
   */
  private def ageTransactionLogFilesToBoundary(tablePath: String, retentionHours: Long): Unit = {
    val txLogPath = new Path(tablePath, "_transaction_log")
    if (!fs.exists(txLogPath)) return

    val boundaryTime = System.currentTimeMillis() - (retentionHours * 3600 * 1000)

    // 1. Age transaction log files
    val txLogFiles = fs
      .listStatus(txLogPath)
      .filter(_.getPath.getName.endsWith(".json"))
      .filterNot(_.getPath.getName.contains("checkpoint"))
      .sortBy(_.getPath.getName)

    // Set files at different times around the boundary
    // Earlier versions: outside boundary (will be deleted)
    // Later versions: at or just inside boundary (race condition target)
    txLogFiles.zipWithIndex.foreach {
      case (file, idx) =>
        val fraction = idx.toDouble / math.max(txLogFiles.length - 1, 1)
        // Files range from 10 minutes outside boundary to 1 second inside boundary
        val offset = if (fraction < 0.7) {
          // Early versions: clearly outside boundary (10 min to 1 hour old beyond boundary)
          -((1 - fraction) * 60 * 60 * 1000).toLong
        } else {
          // Later versions: RIGHT AT the boundary (within 10 seconds)
          // This is where the race condition can strike
          ((1 - fraction) * 10 * 1000).toLong
        }
        val newTime = boundaryTime + offset
        fs.setTimes(file.getPath, newTime, -1)
    }

    println(s"Aged ${txLogFiles.length} transaction log files around ${retentionHours}h boundary")

    // 2. Age SPLIT files - CRITICAL for reproducing the bug!
    // Without this, orphaned files are protected by the retention filter
    val splitFiles = listFilesRecursively(new Path(tablePath))
      .filter(_.endsWith(".split"))

    val oldTime = boundaryTime - (60 * 60 * 1000) // 1 hour before boundary (definitely old enough)
    splitFiles.foreach(splitPath => fs.setTimes(new Path(splitPath), oldTime, -1))

    println(s"Aged ${splitFiles.length} split files to be older than ${retentionHours}h boundary")
  }

  private def listSplitFilesOnDisk(tablePath: String): Seq[String] =
    listFilesRecursively(new Path(tablePath))
      .filter(_.endsWith(".split"))

  private def listFilesRecursively(path: Path): Seq[String] = {
    if (!fs.exists(path)) return Seq.empty

    val statuses = fs.listStatus(path)
    statuses.flatMap { status =>
      if (status.isDirectory && !status.getPath.getName.startsWith("_")) {
        listFilesRecursively(status.getPath)
      } else if (status.isFile) {
        Seq(status.getPath.toString)
      } else {
        Seq.empty
      }
    }.toSeq
  }

  private def extractFilename(path: String): String = path.split('/').last
}
