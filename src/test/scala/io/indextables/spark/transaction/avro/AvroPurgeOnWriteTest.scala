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

import org.apache.spark.sql.{SaveMode, SparkSession}

import org.apache.hadoop.fs.{FileSystem, Path}

import io.indextables.spark.purge.PurgeOnWriteTransactionCounter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

/**
 * Integration tests for purge-on-write feature with Avro state format.
 *
 * Tests cover:
 *   - Trigger after N writes with Avro format
 *   - State directory preservation during purge
 *   - Split file cleanup with Avro format
 *   - Counter behavior with Avro state
 */
class AvroPurgeOnWriteTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession  = _
  var testBasePath: String = _
  var fs: FileSystem       = _

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("AvroPurgeOnWriteTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      // Use Avro format (default)
      .config("spark.indextables.state.format", "avro")
      .config("spark.indextables.checkpoint.enabled", "true")
      .config("spark.indextables.checkpoint.interval", "5")
      .config("spark.indextables.purge.retentionCheckEnabled", "true")
      .getOrCreate()

    testBasePath = Files.createTempDirectory("avro_purge_on_write_test").toString
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

  override def afterEach(): Unit =
    try
      if (fs != null && testBasePath != null) {
        fs.delete(new Path(testBasePath), true)
      }
    finally {
      PurgeOnWriteTransactionCounter.clearAll()
      if (spark != null) {
        spark.stop()
        spark = null
      }
    }

  test("Avro: purge-on-write should be disabled by default") {
    val tablePath = s"$testBasePath/default_disabled"

    // Write some data
    val df = spark.range(100).toDF("id")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "id")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Transaction counter should be 0 (feature disabled)
    assert(PurgeOnWriteTransactionCounter.get(tablePath) === 0)
  }

  test("Avro: purge-on-write should trigger after N writes") {
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
      .option("spark.indextables.indexing.fastfields", "id")
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
    assert(
      PurgeOnWriteTransactionCounter.get(tablePath) === 0,
      "Counter should reset to 0 after purge triggers on 3rd write"
    )
  }

  test("Avro: state directory should be preserved after purge-on-write") {
    val tablePath = s"$testBasePath/preserve_state_dir"

    // Enable purge-on-write
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "2")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "24")

    val df = spark.range(100).toDF("id")

    // Write 1
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "id")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Write 2 - should trigger purge
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Verify state directory exists after purge
    val txLogDir = new File(tablePath, "_transaction_log")
    val stateDirectories = txLogDir.listFiles().filter { f =>
      f.getName.startsWith("state-v") && f.isDirectory
    }
    assert(stateDirectories.length >= 1, "State directory should be preserved after purge")

    // Verify data is intact
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    assert(readDf.count() === 200, "Should have 200 records after 2 writes")
  }

  test("Avro: table should be readable after purge-on-write") {
    val tablePath = s"$testBasePath/readable_after_purge"

    // Enable purge-on-write
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "2")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")

    val df = spark.range(50).toDF("id")

    // Writes that trigger purge
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "id")
      .mode(SaveMode.Overwrite)
      .save(tablePath)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Verify table is readable
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    assert(readDf.count() === 100)

    // Verify queries work
    assert(readDf.filter("id >= 25").count() === 50)
  }

  test("Avro: per-operation purge config should override session config") {
    val tablePath = s"$testBasePath/per_operation_config"

    // Session config: purge disabled
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "false")

    val df = spark.range(50).toDF("id")

    // Write 1: enable via per-operation config
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "id")
      .option("spark.indextables.purgeOnWrite.enabled", "true")
      .option("spark.indextables.purgeOnWrite.triggerAfterWrites", "2")
      .option("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
      .mode(SaveMode.Overwrite)
      .save(tablePath)
    assert(PurgeOnWriteTransactionCounter.get(tablePath) === 1)

    // Write 2: should trigger purge
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.purgeOnWrite.enabled", "true")
      .option("spark.indextables.purgeOnWrite.triggerAfterWrites", "2")
      .option("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
      .mode(SaveMode.Append)
      .save(tablePath)
    assert(PurgeOnWriteTransactionCounter.get(tablePath) === 0, "Counter should reset after purge")
  }

  test("Avro: _last_checkpoint should reference avro-state after purge-on-write") {
    val tablePath = s"$testBasePath/checkpoint_format_after_purge"

    // Enable purge-on-write
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "2")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")

    val df = spark.range(50).toDF("id")

    // Writes that trigger purge
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "id")
      .mode(SaveMode.Overwrite)
      .save(tablePath)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Verify _last_checkpoint references Avro format
    val lastCheckpointFile = new File(tablePath, "_transaction_log/_last_checkpoint")
    if (lastCheckpointFile.exists()) {
      val content = scala.io.Source.fromFile(lastCheckpointFile).mkString
      assert(
        content.contains("avro-state") || content.contains("stateDir"),
        s"_last_checkpoint should reference Avro state format after purge-on-write"
      )
    }

    // Verify table works
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    assert(readDf.count() === 100)
  }
}
