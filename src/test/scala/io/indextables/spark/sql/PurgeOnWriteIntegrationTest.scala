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

import io.indextables.spark.purge.PurgeOnWriteTransactionCounter
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

import java.io.File
import java.nio.file.Files
import java.util.UUID

/**
 * Integration tests for purge-on-write feature.
 *
 * Tests cover:
 * - Trigger after N writes
 * - Trigger after merge-on-write
 * - Configuration propagation
 * - Split file cleanup
 * - Transaction log cleanup
 * - Counter reset after purge
 */
class PurgeOnWriteIntegrationTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  var testBasePath: String = _
  var fs: FileSystem = _

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("PurgeOnWriteIntegrationTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.indextables.purge.retentionCheckEnabled", "true")
      .getOrCreate()

    testBasePath = Files.createTempDirectory("purge_on_write_test").toString
    fs = new Path(testBasePath).getFileSystem(spark.sparkContext.hadoopConfiguration)

    // Clear all transaction counters
    PurgeOnWriteTransactionCounter.clearAll()

    // Reset all config
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "0")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "168")
    spark.conf.set("spark.indextables.purgeOnWrite.txLogRetentionHours", "720")
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "true")
  }

  override def afterEach(): Unit = {
    try {
      if (fs != null && testBasePath != null) {
        fs.delete(new Path(testBasePath), true)
      }
    } finally {
      PurgeOnWriteTransactionCounter.clearAll()
      if (spark != null) {
        spark.stop()
        spark = null
      }
    }
  }

  test("purge-on-write should be disabled by default") {
    val tablePath = s"$testBasePath/default_disabled"

    // Write some data
    val df = spark.range(100).toDF("id")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Transaction counter should be 0 (feature disabled)
    assert(PurgeOnWriteTransactionCounter.get(tablePath) === 0)
  }

  test("purge-on-write should trigger after N writes") {
    val tablePath = s"$testBasePath/trigger_after_writes"

    // Enable purge-on-write with triggerAfterWrites=3
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "3")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "24")
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "true")

    val df = spark.range(50).toDF("id")

    // Write 1: counter = 1
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(tablePath)
    assert(PurgeOnWriteTransactionCounter.get(tablePath) === 1)

    // Write 2: counter = 2
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(tablePath)
    assert(PurgeOnWriteTransactionCounter.get(tablePath) === 2)

    // Write 3: counter should reset to 0 after purge triggers
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(tablePath)
    assert(PurgeOnWriteTransactionCounter.get(tablePath) === 0,
      "Counter should reset to 0 after purge triggers on 3rd write")
  }

  test("purge-on-write should clean up old orphaned split files") {
    val tablePath = s"$testBasePath/cleanup_splits"

    // Enable purge-on-write with very short retention and disabled check
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "2")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "24")
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "true")

    val df = spark.range(50).toDF("id")

    // Write 1: creates split files
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Create orphaned split files and age them
    val orphan1 = new Path(tablePath, s"orphan1_${UUID.randomUUID()}.split")
    val orphan2 = new Path(tablePath, s"orphan2_${UUID.randomUUID()}.split")
    fs.create(orphan1).close()
    fs.create(orphan2).close()

    val oldTimestamp = System.currentTimeMillis() - (25L * 60 * 60 * 1000) // 25 hours ago
    fs.setTimes(orphan1, oldTimestamp, -1)
    fs.setTimes(orphan2, oldTimestamp, -1)

    assert(fs.exists(orphan1), "Orphan1 should exist before purge")
    assert(fs.exists(orphan2), "Orphan2 should exist before purge")

    // Write 2: should trigger purge and clean up old orphaned files
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Verify orphaned files are deleted
    assert(!fs.exists(orphan1), "Orphan1 should be deleted after purge")
    assert(!fs.exists(orphan2), "Orphan2 should be deleted after purge")
  }

  test("purge-on-write should clean up old transaction log files") {
    val tablePath = s"$testBasePath/cleanup_txlog"

    // Enable purge-on-write with short retention
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "15")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "24")
    spark.conf.set("spark.indextables.purgeOnWrite.txLogRetentionHours", "24")

    val df = spark.range(10).toDF("id")

    // Write 10 times to create transaction log files 0-9
    (1 to 10).foreach { i =>
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) SaveMode.Overwrite else SaveMode.Append)
        .save(tablePath)
    }

    val txLogPath = new Path(tablePath, "_transaction_log")

    // Age transaction log files 0-8 to 25 hours ago (keep 9-10 recent)
    val oldTimestamp = System.currentTimeMillis() - (25L * 60 * 60 * 1000)
    (0 to 8).foreach { version =>
      val versionFile = new Path(txLogPath, f"$version%020d.json")
      if (fs.exists(versionFile)) {
        fs.setTimes(versionFile, oldTimestamp, -1)
      }
    }

    // Verify old files exist before purge
    assert(fs.exists(new Path(txLogPath, "00000000000000000000.json")))
    assert(fs.exists(new Path(txLogPath, "00000000000000000005.json")))

    // Write 5 more times to reach triggerAfterWrites=15 and trigger purge
    (1 to 5).foreach { _ =>
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(SaveMode.Append)
        .save(tablePath)
    }

    // Verify old transaction log files are deleted
    assert(!fs.exists(new Path(txLogPath, "00000000000000000000.json")),
      "Old version 0 should be deleted")
    assert(!fs.exists(new Path(txLogPath, "00000000000000000005.json")),
      "Old version 5 should be deleted")

    // Verify recent files are kept
    assert(fs.exists(new Path(txLogPath, "00000000000000000009.json")),
      "Recent version 9 should be kept")
  }

  test("purge-on-write should propagate credentials from write options") {
    val tablePath = s"$testBasePath/credential_propagation"

    // Enable purge-on-write
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "1")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "24")

    val df = spark.range(50).toDF("id")

    // Write with custom credentials (for local FS this just validates no errors)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .option("spark.indextables.aws.accessKey", "test-access-key")
      .option("spark.indextables.aws.secretKey", "test-secret-key")
      .save(tablePath)

    // Should complete without errors (credential propagation successful)
    assert(fs.exists(new Path(tablePath, "_transaction_log")))
  }

  test("purge-on-write should not fail writes even if purge fails") {
    val tablePath = s"$testBasePath/graceful_failure"

    // Enable purge-on-write with invalid config (will cause purge to fail)
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "1")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "-999") // Invalid

    val df = spark.range(50).toDF("id")

    // Write should succeed even though purge will fail
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Verify data was written successfully
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    assert(result.count() === 50)
  }

  test("purge-on-write counter should be per-table") {
    val table1Path = s"$testBasePath/table1"
    val table2Path = s"$testBasePath/table2"

    // Enable purge-on-write
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "5")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")

    val df = spark.range(50).toDF("id")

    // Write to table1 twice
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(table1Path)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(table1Path)

    // Write to table2 three times
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(table2Path)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(table2Path)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(table2Path)

    // Verify separate counters
    assert(PurgeOnWriteTransactionCounter.get(table1Path) === 2)
    assert(PurgeOnWriteTransactionCounter.get(table2Path) === 3)
  }

  test("purge-on-write should respect different retention periods for splits vs transaction logs") {
    val tablePath = s"$testBasePath/different_retention"

    // Enable purge-on-write with different retention periods
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "3")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "24")
    spark.conf.set("spark.indextables.purgeOnWrite.txLogRetentionHours", "48")

    val df = spark.range(10).toDF("id")

    // Write twice to create some transaction logs
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(tablePath)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(tablePath)

    val txLogPath = new Path(tablePath, "_transaction_log")

    // Create aged orphaned split and transaction log
    val orphanSplit = new Path(tablePath, s"orphan_${UUID.randomUUID()}.split")
    fs.create(orphanSplit).close()

    val splitAgeHours = 25L // Old enough for 24h retention
    val splitTimestamp = System.currentTimeMillis() - (splitAgeHours * 60 * 60 * 1000)
    fs.setTimes(orphanSplit, splitTimestamp, -1)

    // Age version 0 to 30 hours (old for split retention, but not for txlog retention)
    val txLog0 = new Path(txLogPath, "00000000000000000000.json")
    if (fs.exists(txLog0)) {
      val txlogAgeHours = 30L // Too old for 24h split, but not for 48h txlog
      val txlogTimestamp = System.currentTimeMillis() - (txlogAgeHours * 60 * 60 * 1000)
      fs.setTimes(txLog0, txlogTimestamp, -1)
    }

    assert(fs.exists(orphanSplit), "Orphan split should exist before purge")

    // Write one more time to trigger purge (3rd write)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Orphan split should be deleted (older than 24h)
    assert(!fs.exists(orphanSplit), "Orphan split should be deleted (older than 24h)")

    // Transaction log 0 should still exist (not older than 48h)
    assert(fs.exists(txLog0), "Transaction log 0 should still exist (not older than 48h)")
  }
}
