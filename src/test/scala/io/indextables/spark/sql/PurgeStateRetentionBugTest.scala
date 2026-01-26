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

import java.nio.file.Files

import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.{FileSystem, Path}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

/**
 * Test to reproduce bug where PURGE INDEXTABLE ignores LOG RETENTION
 * and aggressively deletes state directories before they expire.
 *
 * Bug report: State directories are being deleted even when they are
 * newer than the specified LOG RETENTION period.
 */
class PurgeStateRetentionBugTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: String     = _
  var fs: FileSystem      = _

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .appName("PurgeStateRetentionBugTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      // Use Avro state format - this is where the bug is reported
      .config("spark.indextables.state.format", "avro")
      .config("spark.indextables.checkpoint.enabled", "true")
      .config("spark.indextables.checkpoint.interval", "3")
      // Disable retention check to allow short retention periods for testing
      .config("spark.indextables.purge.retentionCheckEnabled", "false")
      .getOrCreate()

    tempDir = Files.createTempDirectory("purge_state_retention_bug_test").toString
    fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    if (tempDir != null) {
      fs.delete(new Path(tempDir), true)
    }
  }

  test("PURGE should NOT delete state directories newer than LOG RETENTION period") {
    val tablePath    = s"$tempDir/state_retention_test"
    val sparkSession = spark
    import sparkSession.implicits._

    // Write data to create multiple versions (checkpoints at intervals of 3)
    // This should create state directories
    (1 to 12).foreach { i =>
      val data = Seq((i, s"record_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    val txLogPath = new Path(s"$tablePath/_transaction_log")

    // List state directories before purge
    val statesBefore = fs.listStatus(txLogPath)
      .filter(_.getPath.getName.startsWith("state-v"))
      .map(_.getPath.getName)
      .sorted

    println(s"State directories BEFORE purge: ${statesBefore.mkString(", ")}")
    assert(statesBefore.nonEmpty, "Should have at least one state directory before purge")

    // Run PURGE with 24 HOURS LOG RETENTION
    // All state directories are freshly created (< 1 minute old)
    // So with 24 HOURS retention, NONE should be deleted (except possibly the latest is always kept)
    val result = spark.sql(
      s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS TRANSACTION LOG RETENTION 24 HOURS DRY RUN"
    ).collect()

    val metrics = result(0).getStruct(1)
    val expiredStatesFound = metrics.getLong(6)   // expired_states_found
    val expiredStatesDeleted = metrics.getLong(7) // expired_states_deleted (should be 0 for DRY RUN)

    println(s"PURGE result: status=${metrics.getString(0)}, expiredStatesFound=$expiredStatesFound")

    // BUG: The test expects 0 expired states found because all states are fresh (< 24 hours old)
    // But the bug causes states to be found as expired even though they're very new
    assert(
      expiredStatesFound == 0,
      s"BUG REPRODUCED: PURGE found $expiredStatesFound 'expired' state directories, " +
      s"but all state directories are < 1 minute old and should NOT be considered expired " +
      s"with 24 HOURS LOG RETENTION. State directories: ${statesBefore.mkString(", ")}"
    )

    // List state directories after purge (DRY RUN, so should be same)
    val statesAfter = fs.listStatus(txLogPath)
      .filter(_.getPath.getName.startsWith("state-v"))
      .map(_.getPath.getName)
      .sorted

    println(s"State directories AFTER DRY RUN purge: ${statesAfter.mkString(", ")}")
    assert(statesAfter.toSet == statesBefore.toSet, "DRY RUN should not delete any states")
  }

  test("PURGE should respect LOG RETENTION for manifests (not use default 1 hour)") {
    val tablePath    = s"$tempDir/manifest_retention_test"
    val sparkSession = spark
    import sparkSession.implicits._

    // Write data to create state directories and manifests
    (1 to 12).foreach { i =>
      val data = Seq((i, s"record_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    val txLogPath = new Path(s"$tablePath/_transaction_log")
    val manifestsPath = new Path(txLogPath, "manifests")

    // Check if manifests directory exists
    val hasManifests = fs.exists(manifestsPath)
    println(s"Manifests directory exists: $hasManifests")

    if (hasManifests) {
      val manifests = fs.listStatus(manifestsPath).map(_.getPath.getName)
      println(s"Manifests BEFORE purge: ${manifests.mkString(", ")}")
    }

    // Run PURGE with 24 HOURS LOG RETENTION
    // All manifests are freshly created (< 1 minute old)
    // Even if manifest GC uses 1 hour default, they should NOT be deleted
    // because they're still within any reasonable retention
    val result = spark.sql(
      s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS TRANSACTION LOG RETENTION 24 HOURS DRY RUN"
    ).collect()

    val metrics = result(0).getStruct(1)
    val expiredStatesFound = metrics.getLong(6)

    println(s"PURGE result: expiredStatesFound=$expiredStatesFound (includes manifests)")

    // Bug check: If manifests are being found as "expired" with 24 hour retention,
    // then the LOG RETENTION parameter is not being respected for manifests
    assert(
      expiredStatesFound == 0,
      s"BUG: PURGE found $expiredStatesFound 'expired' items, " +
      s"but all states and manifests are < 1 minute old. " +
      s"This suggests LOG RETENTION 24 HOURS is not being honored."
    )
  }

  test("PURGE should delete state directories ONLY when older than LOG RETENTION period") {
    val tablePath    = s"$tempDir/state_aged_retention_test"
    val sparkSession = spark
    import sparkSession.implicits._

    // Write data to create multiple versions
    (1 to 12).foreach { i =>
      val data = Seq((i, s"record_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    val txLogPath = new Path(s"$tablePath/_transaction_log")

    // List state directories before aging
    val statesBefore = fs.listStatus(txLogPath)
      .filter(_.getPath.getName.startsWith("state-v"))
      .sortBy(_.getPath.getName)

    println(s"State directories BEFORE aging: ${statesBefore.map(_.getPath.getName).mkString(", ")}")

    // Find the OLDEST state directory (not the latest) and age it to 25 hours ago
    // The latest should NEVER be deleted regardless of age
    val latestVersion = statesBefore.map { s =>
      s.getPath.getName.replace("state-v", "").toLong
    }.max

    val oldTimestamp = System.currentTimeMillis() - (25L * 60 * 60 * 1000) // 25 hours ago

    var agedCount = 0
    statesBefore.foreach { stateStatus =>
      val version = stateStatus.getPath.getName.replace("state-v", "").toLong
      if (version != latestVersion) {
        // Age this state directory
        fs.setTimes(stateStatus.getPath, oldTimestamp, -1)
        agedCount += 1
        println(s"Aged ${stateStatus.getPath.getName} to 25 hours ago")
      }
    }

    // Run PURGE with 24 HOURS LOG RETENTION
    // Only aged states should be found as expired
    val result = spark.sql(
      s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS TRANSACTION LOG RETENTION 24 HOURS DRY RUN"
    ).collect()

    val metrics = result(0).getStruct(1)
    val expiredStatesFound = metrics.getLong(6)

    println(s"PURGE result: expiredStatesFound=$expiredStatesFound, agedCount=$agedCount")

    // Should find exactly the aged states (all except latest)
    assert(
      expiredStatesFound == agedCount,
      s"Expected to find $agedCount expired states (the aged ones), but found $expiredStatesFound"
    )
  }
}
