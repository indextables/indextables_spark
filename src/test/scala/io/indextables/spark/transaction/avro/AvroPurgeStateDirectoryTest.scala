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
import java.nio.file.Files
import java.util.UUID

import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.{FileSystem, Path}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

/**
 * Tests for purge operations with Avro state format.
 *
 * These tests verify that:
 *   - PURGE INDEXTABLE works correctly with Avro state directories
 *   - Orphaned split files are identified and deleted
 *   - Avro state directories are preserved during purge
 *   - Retention periods are respected for Avro format
 */
class AvroPurgeStateDirectoryTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: String     = _
  var fs: FileSystem      = _

  override def beforeEach(): Unit = {
    // Use Avro format (default)
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("AvroPurgeStateDirectoryTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.indextables.state.format", "avro")
      .config("spark.indextables.purge.retentionCheckEnabled", "true")
      .config("spark.indextables.checkpoint.enabled", "true")
      .config("spark.indextables.checkpoint.interval", "1")
      .getOrCreate()

    tempDir = Files.createTempDirectory("avro_purge_test").toString
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

  test("Avro state: PURGE should identify and delete orphaned split files") {
    val tablePath = s"$tempDir/test_table"

    // Write initial data with Avro format
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
      .option("spark.indextables.indexing.fastfields", "id,age")
      .save(tablePath)

    // Verify table exists and has files
    val beforeRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(beforeRead.count() == 3)

    // Verify Avro state directory was created
    val txLogPath = new File(tablePath, "_transaction_log")
    val stateDirectories = txLogPath.listFiles().filter(_.getName.startsWith("state-v"))
    assert(stateDirectories.nonEmpty, "Should have created Avro state directory")

    // Create orphaned split files by directly writing to filesystem
    val orphanedSplit1 = new Path(s"$tablePath/orphaned_${UUID.randomUUID()}.split")
    val orphanedSplit2 = new Path(s"$tablePath/orphaned_${UUID.randomUUID()}.split")

    fs.create(orphanedSplit1).close()
    fs.create(orphanedSplit2).close()

    // Set modification time to be old enough (older than default 7 days)
    val oldTime = System.currentTimeMillis() - (8L * 24 * 60 * 60 * 1000) // 8 days ago
    fs.setTimes(orphanedSplit1, oldTime, -1)
    fs.setTimes(orphanedSplit2, oldTime, -1)

    // Verify orphaned files exist
    assert(fs.exists(orphanedSplit1))
    assert(fs.exists(orphanedSplit2))

    // Run DRY RUN first to preview
    val dryRunResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' DRY RUN").collect()
    assert(dryRunResult.length == 1)
    val dryRunMetrics = dryRunResult(0).getStruct(1)
    assert(dryRunMetrics.getString(0) == "DRY_RUN")
    assert(dryRunMetrics.getLong(1) >= 2, s"Should find at least 2 orphaned files, found ${dryRunMetrics.getLong(1)}")

    // Files should still exist after DRY RUN
    assert(fs.exists(orphanedSplit1))
    assert(fs.exists(orphanedSplit2))

    // Now actually purge
    val purgeResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    assert(purgeResult.length == 1)
    val metrics = purgeResult(0).getStruct(1)
    assert(metrics.getString(0) == "SUCCESS")
    assert(metrics.getLong(1) >= 2, s"Should find at least 2 orphaned files, found ${metrics.getLong(1)}")
    assert(metrics.getLong(2) >= 2, s"Should delete at least 2 files, deleted ${metrics.getLong(2)}")

    // Verify orphaned files are deleted
    assert(!fs.exists(orphanedSplit1))
    assert(!fs.exists(orphanedSplit2))

    // Verify table is still intact
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 3)

    // Verify Avro state directory still exists
    val stateDirectoriesAfter = txLogPath.listFiles().filter(_.getName.startsWith("state-v"))
    assert(stateDirectoriesAfter.nonEmpty, "Avro state directory should be preserved after purge")
  }

  test("Avro state: PURGE should preserve state directories during cleanup") {
    val tablePath = s"$tempDir/preserve_state_test"

    // Write initial data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test1"), (2, "test2")).toDF("id", "content")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Append more data to create additional state
    val data2 = Seq((3, "test3"), (4, "test4")).toDF("id", "content")
    data2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("append")
      .save(tablePath)

    // Get state directory count before purge
    val txLogPath = new File(tablePath, "_transaction_log")
    val stateDirectoriesBefore = txLogPath.listFiles().filter(_.getName.startsWith("state-v"))

    // Run purge (no orphans expected)
    val purgeResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    assert(purgeResult.length == 1)
    val metrics = purgeResult(0).getStruct(1)
    assert(metrics.getString(0) == "SUCCESS" || metrics.getString(0) == "NO_ORPHANS")

    // Verify state directories are preserved
    val stateDirectoriesAfter = txLogPath.listFiles().filter(_.getName.startsWith("state-v"))
    assert(
      stateDirectoriesAfter.length >= stateDirectoriesBefore.length,
      "State directories should be preserved during purge"
    )

    // Verify data is intact
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 4)
  }

  test("Avro state: PURGE should respect retention period") {
    val tablePath = s"$tempDir/retention_test"

    // Write initial data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "content")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.indexing.fastfields", "id")
      .save(tablePath)

    // Create orphaned files with different ages
    val recentOrphan = new Path(s"$tablePath/recent_orphan.split")
    val oldOrphan    = new Path(s"$tablePath/old_orphan.split")

    fs.create(recentOrphan).close()
    fs.create(oldOrphan).close()

    // Set times - recent orphan is 1 day old, old orphan is 10 days old
    val oneDayAgo  = System.currentTimeMillis() - (1L * 24 * 60 * 60 * 1000)
    val tenDaysAgo = System.currentTimeMillis() - (10L * 24 * 60 * 60 * 1000)

    fs.setTimes(recentOrphan, oneDayAgo, -1)
    fs.setTimes(oldOrphan, tenDaysAgo, -1)

    // Purge with 7 day retention - should delete old orphan but not recent
    val purgeResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()
    assert(purgeResult.length == 1)
    val metrics = purgeResult(0).getStruct(1)
    assert(metrics.getString(0) == "SUCCESS")
    // Should find and delete at least the old orphan (may also delete old tx log files)
    assert(metrics.getLong(1) >= 1, s"Should find at least 1 old orphaned file, found ${metrics.getLong(1)}")
    assert(metrics.getLong(2) >= 1, s"Should delete at least 1 file, deleted ${metrics.getLong(2)}")

    // Recent orphan should still exist (it's only 1 day old)
    assert(fs.exists(recentOrphan), "Recent orphan should not be deleted")
    assert(!fs.exists(oldOrphan), "Old orphan should be deleted")
  }

  test("Avro state: table should work after purge operation") {
    val tablePath = s"$tempDir/after_purge_test"

    // Write initial data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "Alice", 100),
      (2, "Bob", 200),
      (3, "Charlie", 300)
    ).toDF("id", "name", "value")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.indexing.fastfields", "id,value")
      .save(tablePath)

    // Create some orphans and purge them
    val orphan = new Path(s"$tablePath/orphan_${UUID.randomUUID()}.split")
    fs.create(orphan).close()
    val oldTime = System.currentTimeMillis() - (8L * 24 * 60 * 60 * 1000)
    fs.setTimes(orphan, oldTime, -1)

    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()

    // Verify all operations work after purge
    val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

    // Count
    assert(df.count() == 3)

    // Filter
    assert(df.filter("id > 1").count() == 2)

    // Aggregation
    val sum = df.agg(Map("value" -> "sum")).collect()(0).getLong(0)
    assert(sum == 600)

    // Append more data
    val newData = Seq((4, "Diana", 400)).toDF("id", "name", "value")
    newData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("append")
      .save(tablePath)

    // Verify append worked
    val afterAppend = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterAppend.count() == 4)
  }

  test("Avro state: _last_checkpoint should reference avro-state format after purge") {
    val tablePath = s"$tempDir/checkpoint_format_test"

    // Write data with checkpoint
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "content")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Run purge
    spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 7 DAYS").collect()

    // Verify _last_checkpoint still references Avro format
    val lastCheckpointFile = new File(tablePath, "_transaction_log/_last_checkpoint")
    if (lastCheckpointFile.exists()) {
      val content = scala.io.Source.fromFile(lastCheckpointFile).mkString
      assert(
        content.contains("avro-state") || content.contains("stateDir"),
        s"_last_checkpoint should reference Avro state format after purge. Content: $content"
      )
    }

    // Table should still work
    val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(df.count() == 1)
  }
}
