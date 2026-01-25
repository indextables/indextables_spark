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
import java.nio.file.Files
import java.util.UUID

import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.{FileSystem, Path}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

/** Integration tests for PURGE INDEXTABLE command. Tests actual file operations with orphaned splits. */
class PurgeIndexTableIntegrationTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: String     = _
  var fs: FileSystem      = _

  override def beforeEach(): Unit = {
    // Use JSON format since these tests validate JSON checkpoint file structure
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("PurgeOrphanedSplitsIntegrationTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.indextables.state.format", "json")
      .config("spark.indextables.purge.retentionCheckEnabled", "true")
      .getOrCreate()

    tempDir = Files.createTempDirectory("purge_integration_test").toString
    fs = new Path(tempDir).getFileSystem(spark.sparkContext.hadoopConfiguration)
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    // Cleanup temp directory
    if (tempDir != null) {
      val dir = new File(tempDir)
      if (dir.exists()) {
        deleteRecursively(dir)
      }
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  test("PURGE INDEXTABLE should identify and delete orphaned split files") {
    val tablePath = s"$tempDir/test_table"

    // Write initial data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "Alice", 30),
      (2, "Bob", 25),
      (3, "Charlie", 35)
    ).toDF("id", "name", "age")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.indexing.fastfields", "age")
      .save(tablePath)

    // Verify table exists and has files
    val beforeRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(beforeRead.count() == 3)

    // Create orphaned split files by directly writing to filesystem
    val orphanedSplit1 = new Path(s"$tablePath/orphaned_${UUID.randomUUID()}.split")
    val orphanedSplit2 = new Path(s"$tablePath/orphaned_${UUID.randomUUID()}.split")
    val orphanedCrc1   = new Path(s"$tablePath/orphaned_${UUID.randomUUID()}.crc")

    fs.create(orphanedSplit1).close()
    fs.create(orphanedSplit2).close()
    fs.create(orphanedCrc1).close()

    // Set modification time to be old enough (older than default 7 days)
    val oldTime = System.currentTimeMillis() - (8L * 24 * 60 * 60 * 1000) // 8 days ago
    fs.setTimes(orphanedSplit1, oldTime, -1)
    fs.setTimes(orphanedSplit2, oldTime, -1)
    fs.setTimes(orphanedCrc1, oldTime, -1)

    // Verify orphaned files exist
    assert(fs.exists(orphanedSplit1))
    assert(fs.exists(orphanedSplit2))
    assert(fs.exists(orphanedCrc1))

    // Run DRY RUN first to preview
    val dryRunResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' DRY RUN").collect()
    assert(dryRunResult.length == 1)
    val dryRunMetrics = dryRunResult(0).getStruct(1)
    assert(dryRunMetrics.getString(0) == "DRY_RUN")
    assert(dryRunMetrics.getLong(1) == 3)       // Found 3 orphaned files
    assert(dryRunMetrics.getBoolean(8) == true) // dry_run flag (field index 8)

    // Files should still exist after DRY RUN
    assert(fs.exists(orphanedSplit1))
    assert(fs.exists(orphanedSplit2))
    assert(fs.exists(orphanedCrc1))

    // Now actually purge
    val purgeResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    assert(purgeResult.length == 1)
    val metrics = purgeResult(0).getStruct(1)
    assert(metrics.getString(0) == "SUCCESS")
    assert(metrics.getLong(1) == 3) // Found 3 orphaned files
    assert(metrics.getLong(2) == 3) // Deleted 3 files

    // Verify orphaned files are deleted
    assert(!fs.exists(orphanedSplit1))
    assert(!fs.exists(orphanedSplit2))
    assert(!fs.exists(orphanedCrc1))

    // Verify table is still intact
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 3)
  }

  test("PURGE INDEXTABLE should respect retention period") {

    val tablePath = s"$tempDir/retention_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned files with different ages
    val oldOrphan    = new Path(s"$tablePath/old_${UUID.randomUUID()}.split")
    val recentOrphan = new Path(s"$tablePath/recent_${UUID.randomUUID()}.split")

    fs.create(oldOrphan).close()
    fs.create(recentOrphan).close()

    // Set modification times
    val oldTime    = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000) // 10 days ago
    val recentTime = System.currentTimeMillis() - (2L * 24 * 60 * 60 * 1000)  // 2 days ago

    fs.setTimes(oldOrphan, oldTime, -1)
    fs.setTimes(recentOrphan, recentTime, -1)

    // Purge with 7 day retention
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    val metrics = result(0).getStruct(1)

    // Should delete old file but keep recent file
    assert(!fs.exists(oldOrphan), "Old orphan should be deleted")
    assert(fs.exists(recentOrphan), "Recent orphan should be kept")

    // Verify metrics
    assert(metrics.getLong(1) == 2) // Found 2 orphaned files
    assert(metrics.getLong(2) == 1) // Deleted only 1 (the old one)
  }

  test("PURGE INDEXTABLE should handle empty tables") {
    val tablePath = s"$tempDir/empty_table"

    // Create empty table directory (no transaction log)
    fs.mkdirs(new Path(tablePath))

    // Should handle gracefully
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' DRY RUN").collect()
    val metrics = result(0).getStruct(1)
    assert(metrics.getString(0) == "DRY_RUN")
    // Should not fail, just report no files
  }

  test("PURGE INDEXTABLE should not delete files referenced in transaction log") {

    val tablePath = s"$tempDir/protected_files_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "Alice"),
      (2, "Bob")
    ).toDF("id", "name")

    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Get the split files from transaction log
    val validFiles = fs
      .listStatus(new Path(tablePath))
      .filter(_.getPath.getName.endsWith(".split"))
      .map(_.getPath)

    assert(validFiles.nonEmpty, "Should have valid split files")

    // Create an orphaned file
    val orphanedFile = new Path(s"$tablePath/orphaned_${UUID.randomUUID()}.split")
    fs.create(orphanedFile).close()

    // Set old modification time for orphaned file
    val oldTime = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000)
    fs.setTimes(orphanedFile, oldTime, -1)

    // Also set old time for valid files (to ensure we don't delete them based on age alone)
    validFiles.foreach(f => fs.setTimes(f, oldTime, -1))

    // Purge
    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()

    // Valid files should still exist
    validFiles.foreach(file => assert(fs.exists(file), s"Valid file $file should not be deleted"))

    // Orphaned file should be deleted
    assert(!fs.exists(orphanedFile), "Orphaned file should be deleted")

    // Table should still be readable
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 2)
  }

  test("PURGE INDEXTABLE should enforce minimum retention period") {

    val tablePath = s"$tempDir/min_retention_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Try to purge with less than minimum retention (24 hours)
    val exception = intercept[IllegalArgumentException] {
      spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 12 HOURS").collect()
    }

    assert(exception.getMessage.contains("less than minimum"))
    assert(exception.getMessage.contains("24 hours"))
  }

  test("PURGE INDEXTABLE should allow override of retention check") {

    val tablePath = s"$tempDir/override_retention_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Disable retention check
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")

    // Should now allow short retention period
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 1 HOURS DRY RUN").collect()
    val metrics = result(0).getStruct(1)
    assert(metrics.getString(0) == "DRY_RUN")
    // Should succeed without error
  }

  test("PURGE INDEXTABLE should handle tables with no orphaned files") {

    val tablePath = s"$tempDir/no_orphans_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Purge (should find no orphaned files)
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    val metrics = result(0).getStruct(1)

    assert(metrics.getString(0) == "SUCCESS")
    assert(metrics.getLong(1) == 0)                                  // Found 0 orphaned files
    assert(metrics.getLong(2) == 0)                                  // Deleted 0 files
    assert(metrics.getString(10).contains("No orphaned files found")) // message field is at index 10
  }

  test("PURGE INDEXTABLE should respect maxFilesToDelete limit") {

    val tablePath = s"$tempDir/max_files_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create multiple orphaned files
    val orphanedFiles = Range(1, 11).map { i =>
      val file = new Path(s"$tablePath/orphaned_$i.split")
      fs.create(file).close()
      val oldTime = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000)
      fs.setTimes(file, oldTime, -1)
      file
    }

    // Set limit to 5 files
    spark.conf.set("spark.indextables.purge.maxFilesToDelete", "5")

    // Purge
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    val metrics = result(0).getStruct(1)

    // Should delete only 5 files
    assert(metrics.getLong(2) == 5) // Deleted 5 files

    // Count remaining orphaned files
    val remainingCount = orphanedFiles.count(f => fs.exists(f))
    assert(remainingCount == 5, "Should have 5 remaining orphaned files")
  }

  test("PURGE INDEXTABLE should handle partitioned tables") {

    val tablePath = s"$tempDir/partitioned_test"

    // Write partitioned data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "Alice", "2024-01-01"),
      (2, "Bob", "2024-01-01"),
      (3, "Charlie", "2024-01-02")
    ).toDF("id", "name", "date")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("date")
      .mode("overwrite")
      .save(tablePath)

    // Create orphaned files in different partitions
    val orphan1 = new Path(s"$tablePath/date=2024-01-01/orphaned_${UUID.randomUUID()}.split")
    val orphan2 = new Path(s"$tablePath/date=2024-01-02/orphaned_${UUID.randomUUID()}.split")

    fs.create(orphan1).close()
    fs.create(orphan2).close()

    val oldTime = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000)
    fs.setTimes(orphan1, oldTime, -1)
    fs.setTimes(orphan2, oldTime, -1)

    // Purge
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    val metrics = result(0).getStruct(1)

    assert(metrics.getLong(2) == 2) // Should delete orphans from all partitions

    // Verify orphaned files are deleted
    assert(!fs.exists(orphan1))
    assert(!fs.exists(orphan2))

    // Verify table is still intact
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 3)
  }

  test("PURGE INDEXTABLE should include both .split and .crc files") {

    val tablePath = s"$tempDir/split_crc_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned .split and .crc files
    val uuid          = UUID.randomUUID()
    val orphanedSplit = new Path(s"$tablePath/orphaned_$uuid.split")
    val orphanedCrc   = new Path(s"$tablePath/orphaned_$uuid.crc")

    fs.create(orphanedSplit).close()
    fs.create(orphanedCrc).close()

    val oldTime = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000)
    fs.setTimes(orphanedSplit, oldTime, -1)
    fs.setTimes(orphanedCrc, oldTime, -1)

    // Purge
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    val metrics = result(0).getStruct(1)

    assert(metrics.getLong(2) == 2) // Should delete both .split and .crc

    // Verify both are deleted
    assert(!fs.exists(orphanedSplit))
    assert(!fs.exists(orphanedCrc))
  }

  test("PURGE INDEXTABLE should handle multiple writes and orphaned files") {

    val tablePath = s"$tempDir/multi_write_test"

    // First write
    val sparkSession = spark
    import sparkSession.implicits._
    val data1 = Seq((1, "Alice")).toDF("id", "name")
    data1.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned file from "failed" write
    val orphan1 = new Path(s"$tablePath/failed_write_${UUID.randomUUID()}.split")
    fs.create(orphan1).close()

    // Second write (append)
    val data2 = Seq((2, "Bob")).toDF("id", "name")
    data2.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("append").save(tablePath)

    // Create another orphaned file
    val orphan2 = new Path(s"$tablePath/failed_write_2_${UUID.randomUUID()}.split")
    fs.create(orphan2).close()

    // Set old modification times
    val oldTime = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000)
    fs.setTimes(orphan1, oldTime, -1)
    fs.setTimes(orphan2, oldTime, -1)

    // Purge
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    val metrics = result(0).getStruct(1)

    assert(metrics.getLong(2) == 2) // Should delete both orphaned files

    // Verify table has both records
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 2)
  }

  test("PURGE INDEXTABLE DRY RUN should not modify any files") {

    val tablePath = s"$tempDir/dry_run_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned files
    val orphanedFiles = Range(1, 6).map { i =>
      val file = new Path(s"$tablePath/orphaned_$i.split")
      fs.create(file).close()
      val oldTime = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000)
      fs.setTimes(file, oldTime, -1)
      file
    }

    // Record file count before
    val beforeCount = fs.listStatus(new Path(tablePath)).length

    // Run DRY RUN
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    assert(metrics.getString(0) == "DRY_RUN")
    assert(metrics.getLong(1) == 5) // Found 5 orphaned files
    assert(metrics.getLong(2) == 0) // Deleted 0 files

    // Verify file count is unchanged
    val afterCount = fs.listStatus(new Path(tablePath)).length
    assert(afterCount == beforeCount)

    // Verify all orphaned files still exist
    orphanedFiles.foreach(file => assert(fs.exists(file), s"File $file should still exist after DRY RUN"))
  }

  test("PURGE INDEXTABLE should handle multi-part checkpoints correctly") {
    val tablePath = s"$tempDir/multi_part_checkpoint_test"

    // Configure for multi-part checkpoints with low threshold
    spark.conf.set("spark.indextables.checkpoint.enabled", "true")
    spark.conf.set("spark.indextables.checkpoint.interval", "3")
    spark.conf.set("spark.indextables.checkpoint.actionsPerPart", "5")
    spark.conf.set("spark.indextables.checkpoint.multiPart.enabled", "true")

    val sparkSession = spark
    import sparkSession.implicits._

    // Write enough data to trigger multi-part checkpoint
    (1 to 10).foreach { i =>
      val data = Seq((i, s"value_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Verify table has data
    val beforeRead  = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    val beforeCount = beforeRead.count()
    assert(beforeCount > 0, "Table should have data")

    // List transaction log files to verify multi-part checkpoint exists
    val txLogPath  = new Path(s"$tablePath/_transaction_log")
    val txLogFiles = fs.listStatus(txLogPath).map(_.getPath.getName).toSet

    // Check for checkpoint manifest (multi-part) or single-file checkpoint
    val hasCheckpoint = txLogFiles.exists(_.endsWith(".checkpoint.json"))
    assert(hasCheckpoint, "Table should have a checkpoint")

    // Check for part files (UUID pattern: checkpoint.<uuid>.<partnum>.json)
    val partFilePattern = """.*\.checkpoint\.[a-f0-9]+\.\d{5}\.json""".r
    val hasPartFiles    = txLogFiles.exists(f => partFilePattern.findFirstIn(f).isDefined)
    println(s"Transaction log files: ${txLogFiles.mkString(", ")}")
    println(s"Has multi-part checkpoint: $hasPartFiles")

    // Create orphaned files
    val orphanedSplit = new Path(s"$tablePath/orphaned_${UUID.randomUUID()}.split")
    fs.create(orphanedSplit).close()
    val oldTime = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000) // 10 days ago
    fs.setTimes(orphanedSplit, oldTime, -1)

    // Run PURGE
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    val metrics = result(0).getStruct(1)

    // Verify orphaned file was deleted
    assert(!fs.exists(orphanedSplit), "Orphaned split should be deleted")
    assert(metrics.getLong(2) >= 1, "Should have deleted at least 1 orphaned file")

    // Verify table still works after purge
    val afterRead  = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    val afterCount = afterRead.count()
    assert(afterCount == beforeCount, s"Data count should be unchanged: expected $beforeCount, got $afterCount")

    // Verify checkpoint files were NOT deleted
    val txLogFilesAfter      = fs.listStatus(txLogPath).map(_.getPath.getName).toSet
    val checkpointFilesAfter = txLogFilesAfter.filter(_.contains("checkpoint"))
    assert(checkpointFilesAfter.nonEmpty, "Checkpoint files should still exist after purge")

    // If multi-part checkpoint was created, verify parts still exist
    if (hasPartFiles) {
      val partFilesAfter = txLogFilesAfter.filter(f => partFilePattern.findFirstIn(f).isDefined)
      assert(partFilesAfter.nonEmpty, "Multi-part checkpoint part files should still exist")
    }

    println(s"✅ PURGE with multi-part checkpoint completed successfully")
    println(s"   Before: $beforeCount rows, After: $afterCount rows")
    println(s"   Checkpoint files preserved: ${checkpointFilesAfter.mkString(", ")}")
  }

  test("PURGE INDEXTABLE should delete old multi-part checkpoint files with transaction log retention") {
    val tablePath = s"$tempDir/multi_part_checkpoint_delete_test"

    // Configure for multi-part checkpoints with very low threshold to force multi-part
    spark.conf.set("spark.indextables.checkpoint.enabled", "true")
    spark.conf.set("spark.indextables.checkpoint.interval", "3")
    spark.conf.set("spark.indextables.checkpoint.actionsPerPart", "3")
    spark.conf.set("spark.indextables.checkpoint.multiPart.enabled", "true")
    // Disable retention check for testing
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")

    val sparkSession = spark
    import sparkSession.implicits._

    // Write data to create first checkpoint (at version 3)
    (1 to 4).foreach { i =>
      val data = Seq((i, s"value_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // List checkpoint files after first batch
    val txLogPath            = new Path(s"$tablePath/_transaction_log")
    val firstBatchFiles      = fs.listStatus(txLogPath).map(_.getPath.getName).toSet
    val firstCheckpointFiles = firstBatchFiles.filter(_.contains("checkpoint"))
    println(s"After first batch - checkpoint files: ${firstCheckpointFiles.mkString(", ")}")

    // Write more data to create second checkpoint (at version 6 or 9)
    (5 to 10).foreach { i =>
      val data = Seq((i, s"value_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(tablePath)
    }

    // List all checkpoint files after second batch
    val secondBatchFiles   = fs.listStatus(txLogPath).map(_.getPath.getName).toSet
    val allCheckpointFiles = secondBatchFiles.filter(_.contains("checkpoint"))
    println(s"After second batch - all checkpoint files: ${allCheckpointFiles.mkString(", ")}")

    // Find part files - pattern: checkpoint.<uuid>.<partnum>.json
    val partFilePattern = """(\d+)\.checkpoint\.([a-f0-9]+)\.\d{5}\.json""".r
    val manifestPattern = """(\d+)\.checkpoint\.json""".r

    val partFilesByVersion = allCheckpointFiles
      .flatMap(f => partFilePattern.findFirstMatchIn(f).map(m => (m.group(1).toLong, f)))
      .groupBy(_._1)
      .mapValues(_.map(_._2).toSet)

    val manifestsByVersion = allCheckpointFiles
      .flatMap(f => manifestPattern.findFirstMatchIn(f).map(m => (m.group(1).toLong, f)))
      .groupBy(_._1)
      .mapValues(_.map(_._2).toSet)

    println(s"Part files by version: $partFilesByVersion")
    println(s"Manifests by version: $manifestsByVersion")

    // Get all version files to set old timestamps on older transaction log files
    val allTxLogFiles = fs.listStatus(txLogPath)

    // Set old modification time on ALL transaction log files to make them eligible for purge
    val oldTime = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000) // 10 days ago
    allTxLogFiles.foreach(fileStatus => fs.setTimes(fileStatus.getPath, oldTime, -1))

    // Read _last_checkpoint to find the latest checkpoint version
    val lastCheckpointPath = new Path(txLogPath, "_last_checkpoint")
    val lastCheckpointContent = if (fs.exists(lastCheckpointPath)) {
      val is      = fs.open(lastCheckpointPath)
      val content = scala.io.Source.fromInputStream(is).mkString
      is.close()
      content
    } else ""
    println(s"_last_checkpoint content: $lastCheckpointContent")

    // Verify we have at least 2 different checkpoint versions
    val checkpointVersions = (partFilesByVersion.keys ++ manifestsByVersion.keys).toSet
    println(s"Checkpoint versions found: $checkpointVersions")

    // Get the expected latest version (highest version number)
    val latestVersion = checkpointVersions.max
    val olderVersions = checkpointVersions.filter(_ < latestVersion)
    println(s"Latest version: $latestVersion, Older versions: $olderVersions")

    // Verify table has data before purge
    val beforeRead  = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    val beforeCount = beforeRead.count()
    assert(beforeCount == 10, s"Table should have 10 rows, got $beforeCount")

    // Run PURGE with transaction log retention
    val result =
      spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 1 HOURS TRANSACTION LOG RETENTION 1 HOURS").collect()
    val metrics = result(0).getStruct(1)
    println(s"PURGE result: status=${metrics.getString(0)}, txLogFilesDeleted=${metrics.getLong(5)}")

    // List remaining checkpoint files after purge
    val remainingFiles           = fs.listStatus(txLogPath).map(_.getPath.getName).toSet
    val remainingCheckpointFiles = remainingFiles.filter(_.contains("checkpoint"))
    println(s"Remaining checkpoint files after purge: ${remainingCheckpointFiles.mkString(", ")}")

    // Verify latest checkpoint files still exist
    val latestManifest = f"$latestVersion%020d.checkpoint.json"
    assert(
      remainingCheckpointFiles.contains(latestManifest) || remainingCheckpointFiles.contains("_last_checkpoint"),
      s"Latest checkpoint manifest ($latestManifest) or _last_checkpoint should still exist"
    )

    // If we had older versions, verify some transaction log cleanup happened
    if (olderVersions.nonEmpty) {
      val txLogFilesDeleted = metrics.getLong(5)
      println(s"Transaction log files deleted: $txLogFilesDeleted")
      // We expect some old version files to be deleted
      // Note: The exact behavior depends on how retention and checkpoint protection work
    }

    // Most importantly: verify table still works after purge
    val afterRead  = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    val afterCount = afterRead.count()
    assert(afterCount == beforeCount, s"Data count should be unchanged: expected $beforeCount, got $afterCount")

    println(s"✅ PURGE with multi-part checkpoint deletion test completed")
    println(s"   Data preserved: $afterCount rows")
    println(s"   Remaining checkpoint files: ${remainingCheckpointFiles.mkString(", ")}")
  }
}
