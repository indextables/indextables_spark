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
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("PurgeOrphanedSplitsIntegrationTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
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
    assert(dryRunMetrics.getBoolean(6) == true) // dry_run flag (field index 6)

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
    assert(metrics.getString(8).contains("No orphaned files found")) // message field is at index 8
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
}
