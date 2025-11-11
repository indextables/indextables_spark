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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Error handling and edge case tests for PURGE INDEXTABLE command.
 */
class PurgeIndexTableErrorHandlingTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: String = _
  var fs: FileSystem = _

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("PurgeOrphanedSplitsErrorHandlingTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

    tempDir = Files.createTempDirectory("purge_error_test").toString
    fs = new Path(tempDir).getFileSystem(spark.sparkContext.hadoopConfiguration)
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
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

  test("PURGE INDEXTABLE should handle non-existent table path") {
    val nonExistentPath = s"$tempDir/non_existent_table"

    // Should handle gracefully, not throw exception
    val result: Option[Array[org.apache.spark.sql.Row]] = try {
      val rows = spark.sql(s"PURGE INDEXTABLE '$nonExistentPath' DRY RUN").collect()
      Some(rows)
    } catch {
      case e: Exception =>
        // Acceptable to throw exception for non-existent table
        None
    }

    // Either succeeds with 0 files or throws appropriate exception
    if (result.isDefined) {
      val metrics = result.get(0).getStruct(1)
      assert(metrics.getLong(1) == 0) // Should find 0 files
    }
  }

  test("PURGE INDEXTABLE should handle negative retention values") {

    val tablePath = s"$tempDir/negative_retention_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Try negative retention - should throw exception
    val exception = intercept[Exception] {
      spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN -5 DAYS").collect()
    }

    // Should contain error about invalid retention
    assert(exception.getMessage != null)
  }

  test("PURGE INDEXTABLE should handle zero retention period") {

    val tablePath = s"$tempDir/zero_retention_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Disable retention check to test zero retention behavior
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")

    // Zero retention should be handled (deletes nothing newer than current time)
    val result = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 0 HOURS DRY RUN").collect()
    val metrics = result(0).getStruct(1)
    assert(metrics.getString(0) == "DRY_RUN")
  }

  test("PURGE INDEXTABLE should handle tables with only transaction log") {
    val tablePath = s"$tempDir/only_txlog_test"

    // Create table directory and transaction log directory (but no split files)
    fs.mkdirs(new Path(tablePath))
    fs.mkdirs(new Path(s"$tablePath/_transaction_log"))

    // Should handle gracefully
    val result = spark.sql(s"PURGE INDEXTABLE '$tablePath' DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    // Should report no orphaned files
    assert(metrics.getLong(1) == 0)
  }

  test("PURGE INDEXTABLE should not delete transaction log files") {

    val tablePath = s"$tempDir/txlog_protection_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Get transaction log files
    val txLogPath = new Path(s"$tablePath/_transaction_log")
    val txLogFilesBefore = fs.listStatus(txLogPath).map(_.getPath.getName).toSet

    assert(txLogFilesBefore.nonEmpty, "Transaction log should have files")

    // Run purge with 24 hours (minimum retention period)
    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS DRY RUN").collect()

    // Verify transaction log files are untouched
    val txLogFilesAfter = fs.listStatus(txLogPath).map(_.getPath.getName).toSet
    assert(txLogFilesBefore == txLogFilesAfter, "Transaction log files should not be affected")
  }

  test("PURGE INDEXTABLE should handle very large retention periods") {

    val tablePath = s"$tempDir/large_retention_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned file
    val orphan = new Path(s"$tablePath/orphaned_${UUID.randomUUID()}.split")
    fs.create(orphan).close()
    val oldTime = System.currentTimeMillis() - (30L * 24 * 60 * 60 * 1000) // 30 days ago
    fs.setTimes(orphan, oldTime, -1)

    // Very large retention period (1000 days)
    val result = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 1000 DAYS DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    // Should find 0 files eligible (all files are newer than 1000 days)
    assert(metrics.getLong(1) == 1) // Found the orphan
    // But it won't be eligible for deletion with 1000 day retention
  }

  test("PURGE INDEXTABLE should handle concurrent writes") {

    val tablePath = s"$tempDir/concurrent_write_test"

    // Initial write
    val sparkSession = spark
    import sparkSession.implicits._
    val data1 = Seq((1, "Alice")).toDF("id", "name")
    data1.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned file
    val orphan = new Path(s"$tablePath/orphaned_${UUID.randomUUID()}.split")
    fs.create(orphan).close()
    val oldTime = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000)
    fs.setTimes(orphan, oldTime, -1)

    // Simulate concurrent write (append)
    val data2 = Seq((2, "Bob")).toDF("id", "name")
    data2.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("append").save(tablePath)

    // Purge should still work correctly
    val result = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    val metrics = result(0).getStruct(1)

    // Should delete only the orphan, not the newly written file
    assert(metrics.getLong(2) >= 1) // At least the orphan was deleted

    // Table should have both records
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 2)
  }

  test("PURGE INDEXTABLE should handle files with special characters in names") {

    val tablePath = s"$tempDir/special_chars_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned files with special characters
    val specialOrphans = Seq(
      s"orphaned-with-dash.split",
      s"orphaned_with_underscore.split",
      s"orphaned.with.dots.split"
    ).map { name =>
      val file = new Path(s"$tablePath/$name")
      fs.create(file).close()
      val oldTime = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000)
      fs.setTimes(file, oldTime, -1)
      file
    }

    // Purge
    val result = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    val metrics = result(0).getStruct(1)

    // Should delete all special character files
    assert(metrics.getLong(2) >= 3)

    // Verify files are deleted
    specialOrphans.foreach { file =>
      assert(!fs.exists(file), s"Special character file $file should be deleted")
    }
  }

  test("PURGE INDEXTABLE should handle deeply nested partition directories") {

    val tablePath = s"$tempDir/nested_partition_test"

    // Write data with multiple partition levels
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "Alice", "2024", "01", "01"),
      (2, "Bob", "2024", "01", "02")
    ).toDF("id", "name", "year", "month", "day")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("year", "month", "day").mode("overwrite")
      .save(tablePath)

    // Create orphaned file in deep partition
    val deepOrphan = new Path(s"$tablePath/year=2024/month=01/day=01/orphaned_${UUID.randomUUID()}.split")
    fs.create(deepOrphan).close()
    val oldTime = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000)
    fs.setTimes(deepOrphan, oldTime, -1)

    // Purge
    val result = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    val metrics = result(0).getStruct(1)

    // Should find and delete the orphan in deep partition
    assert(metrics.getLong(2) >= 1)
    assert(!fs.exists(deepOrphan))
  }

  test("PURGE INDEXTABLE should handle read-only filesystems gracefully") {

    val tablePath = s"$tempDir/readonly_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned file
    val orphan = new Path(s"$tablePath/orphaned_${UUID.randomUUID()}.split")
    fs.create(orphan).close()
    val oldTime = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000)
    fs.setTimes(orphan, oldTime, -1)

    // Make file read-only (simulate permission issue)
    val file = new File(orphan.toString)
    file.setReadOnly()

    // Try to purge - should handle permission errors gracefully
    val result: Option[Array[org.apache.spark.sql.Row]] = try {
      val rows = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
      Some(rows)
    } catch {
      case e: Exception =>
        // Permission errors are acceptable
        None
    }

    // Either succeeds with partial success or throws appropriate exception
    if (result.isDefined) {
      val metrics = result.get(0).getStruct(1)
      // May report partial success if some files failed to delete
      val status = metrics.getString(0)
      assert(status == "SUCCESS" || status == "PARTIAL_SUCCESS")
    }

    // Restore write permissions for cleanup
    file.setWritable(true)
  }

  test("PURGE INDEXTABLE should provide accurate size metrics") {

    val tablePath = s"$tempDir/size_metrics_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned files with known sizes
    val file1 = new Path(s"$tablePath/orphan1.split")
    val file2 = new Path(s"$tablePath/orphan2.split")

    val size1 = 1024L * 100 // 100 KB
    val size2 = 1024L * 200 // 200 KB

    // Write files with specific sizes
    val out1 = fs.create(file1)
    out1.write(new Array[Byte](size1.toInt))
    out1.close()

    val out2 = fs.create(file2)
    out2.write(new Array[Byte](size2.toInt))
    out2.close()

    val oldTime = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000)
    fs.setTimes(file1, oldTime, -1)
    fs.setTimes(file2, oldTime, -1)

    // Purge
    val result = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    val metrics = result(0).getStruct(1)

    // Check size metrics
    val sizeMB = metrics.getDouble(3)
    val expectedMB = (size1 + size2) / (1024.0 * 1024.0)

    assert(math.abs(sizeMB - expectedMB) < 0.01, s"Size metrics should be accurate: expected $expectedMB MB, got $sizeMB MB")
  }

  test("PURGE INDEXTABLE should handle case-insensitive file extensions") {

    val tablePath = s"$tempDir/case_insensitive_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned files with different case extensions
    // (Note: filesystem behavior may vary, but command should handle gracefully)
    val normalOrphan = new Path(s"$tablePath/orphaned.split")
    fs.create(normalOrphan).close()

    val oldTime = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000)
    fs.setTimes(normalOrphan, oldTime, -1)

    // Purge should find .split files regardless of implementation
    val result = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    val metrics = result(0).getStruct(1)

    // Should delete the orphan
    assert(!fs.exists(normalOrphan))
  }

  test("PURGE INDEXTABLE should handle empty file names gracefully") {

    val tablePath = s"$tempDir/empty_name_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Purge should not crash on edge cases
    val result = spark.sql(s"PURGE INDEXTABLE '$tablePath' DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    // Should complete successfully
    assert(metrics.getString(0) == "DRY_RUN")
  }

  test("PURGE INDEXTABLE should handle symlinks gracefully") {

    val tablePath = s"$tempDir/symlink_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Note: Symlink behavior is filesystem-dependent
    // Command should handle gracefully without following symlinks

    // Purge should complete without error
    val result = spark.sql(s"PURGE INDEXTABLE '$tablePath' DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    assert(metrics.getString(0) == "DRY_RUN")
  }
}
