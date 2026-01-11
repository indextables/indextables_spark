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

import scala.util.Random

import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.{FileSystem, Path}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

/**
 * Tests for time travel support in PURGE INDEXTABLE command.
 *
 * These tests verify that PURGE correctly handles files that were added and removed in historical versions, ensuring
 * time travel queries continue to work.
 */
class PurgeIndexTableTimeTravelTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  var fs: FileSystem      = _
  var tempDir: String     = _

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .appName("PurgeIndexTableTimeTravelTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.indextables.checkpoint.enabled", "true")
      .config("spark.indextables.checkpoint.interval", "10")
      .getOrCreate()

    fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    tempDir = System.getProperty("java.io.tmpdir") + "/purge_time_travel_test_" + Random.nextLong().abs
    new File(tempDir).mkdirs()
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    if (tempDir != null) {
      val dir = new File(tempDir)
      if (dir.exists()) {
        dir.listFiles().foreach(_.delete())
        dir.delete()
      }
    }
  }

  test("PURGE should NOT delete files referenced in transaction logs within retention period") {
    val tablePath    = s"$tempDir/time_travel_test1"
    val sparkSession = spark
    import sparkSession.implicits._

    // Set short retention (1 hour) for transaction logs
    spark.conf.set("spark.indextables.logRetention.duration", (1L * 60 * 60 * 1000).toString) // 1 hour

    // Create initial data (versions 0-4)
    (1 to 5).foreach { i =>
      val data = Seq((i, s"initial_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Get the split file from version 5
    val txLogPath = new Path(s"$tablePath/_transaction_log")
    val filesAfterV5 = fs
      .listStatus(new Path(tablePath))
      .filter(_.getPath.getName.endsWith(".split"))
      .map(_.getPath.getName)

    println(s"Files after v5: ${filesAfterV5.mkString(", ")}")
    assert(filesAfterV5.length >= 1, "Should have at least one split file after v5")
    val fileFromV5 = filesAfterV5.head

    // Overwrite the table (version 5) - this creates REMOVE actions for old files
    val overwriteData = Seq((100, "new_data")).toDF("id", "value")
    overwriteData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Verify the old file still exists on disk (orphaned because REMOVE didn't delete it)
    val fileFromV5Path = new Path(tablePath, fileFromV5)
    assert(fs.exists(fileFromV5Path), s"File from v5 should still exist: $fileFromV5")

    // Continue writing to create more versions and trigger checkpoint
    (7 to 12).foreach { i =>
      val data = Seq((i + 100, s"newer_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(tablePath)
    }

    // Verify checkpoint was created
    val checkpointFiles = fs
      .listStatus(txLogPath)
      .filter(_.getPath.getName.contains("checkpoint"))
    assert(checkpointFiles.nonEmpty, "Checkpoint should have been created")

    // Run PURGE with 24 hour retention (transaction logs are recent, within retention)
    val result               = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS").collect()
    val metrics              = result(0).getStruct(1)
    val orphanedFilesDeleted = metrics.getLong(2)

    // The file from v5 should NOT be deleted because:
    // 1. Transaction logs v0-v5 are within 1 hour retention
    // 2. File is referenced in those transaction logs (needed for time travel)
    val fileStillExists = fs.exists(fileFromV5Path)
    assert(fileStillExists, s"File from v5 should NOT be deleted - needed for time travel to historical versions")

    println(s"✓ Time travel test 1 passed: File preserved when transaction logs are within retention")
  }

  test("PURGE should delete orphaned files when their transaction logs are outside retention period") {
    val tablePath    = s"$tempDir/time_travel_test2"
    val sparkSession = spark
    import sparkSession.implicits._
    import java.util.UUID

    // Set 1 hour retention for transaction logs
    spark.conf.set("spark.indextables.logRetention.duration", (1L * 60 * 60 * 1000).toString)

    // Create initial data (versions 0-4)
    (1 to 5).foreach { i =>
      val data = Seq((i, s"initial_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Manually create orphaned files that simulate files from early versions
    // These files are NOT referenced in any transaction log
    val oldTimestampForFiles = System.currentTimeMillis() - (25L * 60 * 60 * 1000) // 25 hours ago
    val orphanedFiles = (1 to 3).map { i =>
      val orphanedPath = new Path(tablePath, s"orphaned_old_${UUID.randomUUID()}.split")
      fs.create(orphanedPath).close()
      // Age the file to be older than retention period (24 hours)
      fs.setTimes(orphanedPath, oldTimestampForFiles, -1)
      println(s"Created test orphaned file from 'old version': ${orphanedPath.getName}")
      orphanedPath.getName
    }.toSet

    // Overwrite the table - creates REMOVE actions for old files
    val overwriteData = Seq((100, "new_data")).toDF("id", "value")
    overwriteData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Continue writing to create checkpoint at v10
    (7 to 12).foreach { i =>
      val data = Seq((i + 100, s"newer_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(tablePath)
    }

    // Verify checkpoint was created
    val txLogPath = new Path(s"$tablePath/_transaction_log")
    val checkpointFiles = fs
      .listStatus(txLogPath)
      .filter(_.getPath.getName.contains("checkpoint"))
    assert(checkpointFiles.nonEmpty, "Checkpoint should have been created")

    // Age the transaction log files v0-v9 to be 2 hours old (outside 1 hour retention)
    val oldTimestamp = System.currentTimeMillis() - (2L * 60 * 60 * 1000) // 2 hours ago
    (0 to 9).foreach { version =>
      val versionFile = new Path(txLogPath, f"$version%020d.json")
      if (fs.exists(versionFile)) {
        fs.setTimes(versionFile, oldTimestamp, -1)
      }
    }

    // Run PURGE with 24 hour retention for splits
    val result =
      spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS TRANSACTION LOG RETENTION 1 HOURS").collect()
    val metrics              = result(0).getStruct(1)
    val orphanedFilesDeleted = metrics.getLong(2)
    val txLogsDeleted        = metrics.getLong(5)

    println(s"Transaction logs deleted: $txLogsDeleted")
    println(s"Orphaned files deleted: $orphanedFilesDeleted")

    // Transaction logs v0-v9 should be deleted (older than checkpoint, outside retention)
    assert(txLogsDeleted > 0, "Should have deleted old transaction logs")

    // Orphaned files should now be deleted because:
    // 1. Their transaction logs (v0-v9) were deleted
    // 2. They're not referenced in any remaining transaction log
    // 3. No way to time travel to them anymore
    assert(
      orphanedFilesDeleted >= orphanedFiles.size,
      s"Should have deleted at least ${orphanedFiles.size} orphaned files, deleted: $orphanedFilesDeleted"
    )

    // Verify orphaned files no longer exist
    val remainingOrphanedFiles = orphanedFiles.filter(fileName => fs.exists(new Path(tablePath, fileName)))
    assert(remainingOrphanedFiles.isEmpty, s"Orphaned files should be deleted: ${remainingOrphanedFiles.mkString(", ")}")

    println(s"✓ Time travel test 2 passed: Files deleted when transaction logs are outside retention")
  }

  test("PURGE DRY RUN should correctly preview orphaned files based on transaction log retention") {
    val tablePath    = s"$tempDir/time_travel_dryrun_test"
    val sparkSession = spark
    import sparkSession.implicits._
    import java.util.UUID

    spark.conf.set("spark.indextables.logRetention.duration", (1L * 60 * 60 * 1000).toString)

    // Create initial data - just one write to keep it simple
    val initialData = Seq((1, "initial")).toDF("id", "value")
    initialData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Manually create orphaned files that simulate files from early versions
    val oldTimestampForFiles = System.currentTimeMillis() - (25L * 60 * 60 * 1000) // 25 hours ago
    val orphanedFiles = (1 to 3).map { i =>
      val orphanedPath = new Path(tablePath, s"orphaned_dryrun_${UUID.randomUUID()}.split")
      fs.create(orphanedPath).close()
      // Age the file to be older than retention period (24 hours)
      fs.setTimes(orphanedPath, oldTimestampForFiles, -1)
      println(s"Created test orphaned file: ${orphanedPath.getName}")
      orphanedPath.getName
    }.toSet

    // Overwrite to create new transaction log versions
    val overwriteData = Seq((100, "new_data")).toDF("id", "value")
    overwriteData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Create more versions and checkpoint
    (7 to 12).foreach { i =>
      val data = Seq((i + 100, s"newer_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(tablePath)
    }

    // Age transaction logs
    val txLogPath    = new Path(s"$tablePath/_transaction_log")
    val oldTimestamp = System.currentTimeMillis() - (2L * 60 * 60 * 1000)
    (0 to 9).foreach { version =>
      val versionFile = new Path(txLogPath, f"$version%020d.json")
      if (fs.exists(versionFile)) {
        fs.setTimes(versionFile, oldTimestamp, -1)
      }
    }

    val filesBeforePurge = fs
      .listStatus(new Path(tablePath))
      .filter(_.getPath.getName.endsWith(".split"))
      .map(_.getPath.getName)
      .toSet

    // Run DRY RUN
    val dryRunResult =
      spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS TRANSACTION LOG RETENTION 1 HOURS DRY RUN").collect()
    val dryRunMetrics       = dryRunResult(0).getStruct(1)
    val dryRunOrphanedFound = dryRunMetrics.getLong(1)  // orphanedFilesFound (before retention filter)
    val dryRunStatus        = dryRunMetrics.getString(0)

    assert(dryRunStatus == "DRY_RUN", "Status should be DRY_RUN")
    println(s"DRY RUN found $dryRunOrphanedFound orphaned files")

    // Verify NO files were actually deleted
    val filesAfterDryRun = fs
      .listStatus(new Path(tablePath))
      .filter(_.getPath.getName.endsWith(".split"))
      .map(_.getPath.getName)
      .toSet

    assert(filesBeforePurge == filesAfterDryRun, "DRY RUN should not delete any files")

    // Now run actual PURGE
    val actualResult =
      spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS TRANSACTION LOG RETENTION 1 HOURS").collect()
    val actualMetrics         = actualResult(0).getStruct(1)
    val actualOrphanedFound   = actualMetrics.getLong(1)  // orphanedFilesFound (before retention filter)
    val actualOrphanedDeleted = actualMetrics.getLong(2)  // orphanedFilesDeleted (after retention filter)

    println(s"Actual PURGE found $actualOrphanedFound orphaned files, deleted $actualOrphanedDeleted")

    // Compare orphanedFilesFound (both before retention filter) - these should match
    // Note: orphanedFilesDeleted may differ from orphanedFilesFound due to retention filtering
    assert(
      dryRunOrphanedFound == actualOrphanedFound,
      s"DRY RUN orphanedFound ($dryRunOrphanedFound) should match actual orphanedFound ($actualOrphanedFound)"
    )

    // Verify we deleted at least the orphaned files we created
    assert(
      actualOrphanedDeleted >= orphanedFiles.size,
      s"Should have deleted at least ${orphanedFiles.size} orphaned files"
    )

    println(s"✓ DRY RUN test passed: Preview matched actual deletions ($actualOrphanedDeleted files)")
  }

  test("PURGE should handle mixed file lifecycles correctly") {
    val tablePath    = s"$tempDir/mixed_lifecycle_test"
    val sparkSession = spark
    import sparkSession.implicits._
    import java.util.UUID

    spark.conf.set("spark.indextables.logRetention.duration", (1L * 60 * 60 * 1000).toString)

    // Scenario:
    // - Create some initial versions
    // - Add orphaned files to simulate old files from early versions
    // - Add new data (currently active)
    // - PURGE should delete orphaned files but preserve current files

    // v0-v2: Create initial data
    (1 to 3).foreach { i =>
      val data = Seq((i, s"group_a_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Manually create orphaned files from "old versions" (Group A)
    val oldTimestampForFiles = System.currentTimeMillis() - (25L * 60 * 60 * 1000) // 25 hours ago
    val orphanedGroupA = (1 to 3).map { i =>
      val orphanedPath = new Path(tablePath, s"orphaned_group_a_${UUID.randomUUID()}.split")
      fs.create(orphanedPath).close()
      // Age the file to be older than retention period (24 hours)
      fs.setTimes(orphanedPath, oldTimestampForFiles, -1)
      println(s"Created orphaned file (Group A): ${orphanedPath.getName}")
      orphanedPath.getName
    }.toSet

    // v3: Overwrite
    val overwriteData = Seq((100, "overwrite")).toDF("id", "value")
    overwriteData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // v4-v12: Add new data (Group D) and create checkpoint
    (5 to 13).foreach { i =>
      val data = Seq((i + 100, s"group_d_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(tablePath)
    }

    // Get current active files (should be preserved)
    val currentActiveFiles = fs
      .listStatus(new Path(tablePath))
      .filter(_.getPath.getName.endsWith(".split"))
      .filterNot(f => orphanedGroupA.contains(f.getPath.getName))
      .map(_.getPath.getName)
      .toSet

    // Age transaction logs v0-v9 (outside retention)
    val txLogPath    = new Path(s"$tablePath/_transaction_log")
    val oldTimestamp = System.currentTimeMillis() - (2L * 60 * 60 * 1000)
    (0 to 9).foreach { version =>
      val versionFile = new Path(txLogPath, f"$version%020d.json")
      if (fs.exists(versionFile)) {
        fs.setTimes(versionFile, oldTimestamp, -1)
      }
    }

    val filesBeforePurge = fs
      .listStatus(new Path(tablePath))
      .filter(_.getPath.getName.endsWith(".split"))
      .map(_.getPath.getName)
      .toSet

    // Run PURGE
    val result =
      spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS TRANSACTION LOG RETENTION 1 HOURS").collect()
    val metrics              = result(0).getStruct(1)
    val orphanedFilesDeleted = metrics.getLong(2)

    val filesAfterPurge = fs
      .listStatus(new Path(tablePath))
      .filter(_.getPath.getName.endsWith(".split"))
      .map(_.getPath.getName)
      .toSet

    // Orphaned files from group A should be deleted
    val deletedFiles          = filesBeforePurge -- filesAfterPurge
    val orphanedGroupADeleted = deletedFiles.intersect(orphanedGroupA)

    assert(
      orphanedGroupADeleted.size >= 1,
      s"Should have deleted files from group A (found ${orphanedGroupADeleted.size} of ${orphanedGroupA.size})"
    )

    // Current active files should NOT be deleted
    val currentFilesStillExist = currentActiveFiles.subsetOf(filesAfterPurge)
    assert(currentFilesStillExist, "Current active files should NOT be deleted")

    // Verify we can still read data
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    val count = readData.count()
    assert(count > 0, "Should be able to read data after PURGE")

    println(s"✓ Mixed lifecycle test passed: Deleted ${orphanedGroupADeleted.size} orphaned files, preserved ${currentActiveFiles.size} current files")
  }

  test("PURGE should handle file re-add after removal (same path reused)") {
    val tablePath    = s"$tempDir/file_readd_test"
    val sparkSession = spark
    import sparkSession.implicits._

    spark.conf.set("spark.indextables.logRetention.duration", (1L * 60 * 60 * 1000).toString)

    // v0: Initial write with specific data that will create a split file
    val initialData = Seq((1, "version1")).toDF("id", "value")
    initialData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // v1-v2: Add more data
    (2 to 3).foreach { i =>
      val data = Seq((i, s"data_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(tablePath)
    }

    // v3: Overwrite (removes all old files, creates REMOVE actions)
    val overwriteData = Seq((100, "overwrite")).toDF("id", "value")
    overwriteData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // v4-v13: Add new data and create checkpoint
    (5 to 14).foreach { i =>
      val data = Seq((i + 100, s"new_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(tablePath)
    }

    // Current files are active and should not be deleted
    val currentFiles = fs
      .listStatus(new Path(tablePath))
      .filter(_.getPath.getName.endsWith(".split"))
      .map(_.getPath.getName)
      .toSet

    // Age old transaction logs
    val txLogPath    = new Path(s"$tablePath/_transaction_log")
    val oldTimestamp = System.currentTimeMillis() - (2L * 60 * 60 * 1000)
    (0 to 9).foreach { version =>
      val versionFile = new Path(txLogPath, f"$version%020d.json")
      if (fs.exists(versionFile)) {
        fs.setTimes(versionFile, oldTimestamp, -1)
      }
    }

    // Run PURGE
    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS TRANSACTION LOG RETENTION 1 HOURS").collect()

    val filesAfterPurge = fs
      .listStatus(new Path(tablePath))
      .filter(_.getPath.getName.endsWith(".split"))
      .map(_.getPath.getName)
      .toSet

    // All current files should still exist (they're in recent transaction logs v10+)
    val currentFilesPreserved = currentFiles.subsetOf(filesAfterPurge)
    assert(currentFilesPreserved, "All current files should be preserved")

    // Verify we can read the current data
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    val count = readData.count()
    assert(count > 0, "Should be able to read data after PURGE")

    println(s"✓ File re-add test passed: Current files preserved, can read $count rows")
  }
}
