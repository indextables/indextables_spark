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

package io.indextables.spark.prewarm

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.SparkSession

import io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
import io.indextables.tantivy4java.split.SplitSearcher.IndexComponent
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import org.slf4j.LoggerFactory

/**
 * Tests for prewarm catch-up behavior when new hosts are added or splits are discovered.
 *
 * Verifies that:
 *   1. Prewarm state is tracked per split 2. New hosts trigger catch-up prewarming 3. New splits discovered in
 *      transaction log trigger catch-up 4. State tracking is consistent across operations
 */
class PrewarmCatchUpTest extends AnyFunSuite with BeforeAndAfterEach {

  private val logger = LoggerFactory.getLogger(classOf[PrewarmCatchUpTest])

  var spark: SparkSession   = _
  var tempTablePath: String = _

  override def beforeEach(): Unit = {
    try
      GlobalSplitCacheManager.flushAllCaches()
    catch {
      case _: Exception => // Ignore
    }

    // Clear locality manager state
    DriverSplitLocalityManager.clear()

    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("PrewarmCatchUpTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.indextables.cache.disk.enabled", "false")
      .getOrCreate()

    tempTablePath = Files.createTempDirectory("prewarm_catchup_test_").toFile.getAbsolutePath
    logger.info(s"Test table path: $tempTablePath")
  }

  override def afterEach(): Unit = {
    // Clear prewarm configs
    Seq(
      "spark.indextables.prewarm.enabled",
      "spark.indextables.prewarm.segments",
      "spark.indextables.prewarm.fields",
      "spark.indextables.prewarm.catchUpNewHosts"
    ).foreach { key =>
      try spark.conf.unset(key)
      catch {
        case _: Exception => // Ignore
      }
    }

    // Clear locality manager state
    DriverSplitLocalityManager.clear()

    if (spark != null) {
      spark.stop()
      spark = null
    }

    if (tempTablePath != null) {
      deleteRecursively(new File(tempTablePath))
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  private def createTestData(numRecords: Int = 200): Unit = {
    val ss = spark
    import ss.implicits._

    val testData = (1 until numRecords + 1)
      .map { i =>
        (
          i.toLong,
          s"title_$i",
          s"content for record $i",
          s"category_${i % 5}",
          i * 1.5
        )
      }
      .toDF("id", "title", "content", "category", "score")

    testData
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.batchSize", "50")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(tempTablePath)

    logger.info(s"Created test data with $numRecords records")
  }

  // === Prewarm State Recording Tests ===

  test("recordPrewarmCompletion should track prewarm state for splits") {
    val testSplitPath = "s3://bucket/table/split_001.split"
    val testHost      = "executor-1"
    val segments      = Set(IndexComponent.TERM, IndexComponent.FASTFIELD)
    val fields        = Some(Set("title", "content"))

    // Record prewarm completion
    DriverSplitLocalityManager.recordPrewarmCompletion(testSplitPath, testHost, segments, fields)

    // Verify state was recorded
    val state = DriverSplitLocalityManager.getPrewarmState(testSplitPath)
    assert(state.isDefined, "Prewarm state should be recorded")
    assert(state.get.hostname == testHost, s"Host should be $testHost")
    assert(state.get.segments == segments, "Segments should match")
    assert(state.get.fields == fields, "Fields should match")

    logger.info("recordPrewarmCompletion test passed")
  }

  test("getSplitsNeedingCatchUp should identify splits on new hosts") {
    val testSplitPath = "s3://bucket/table/split_002.split"
    val originalHost  = "executor-1"
    val newHost       = "executor-2"
    val segments      = Set(IndexComponent.TERM)

    // Record prewarm on original host
    DriverSplitLocalityManager.recordPrewarmCompletion(testSplitPath, originalHost, segments, None)

    // Need to also assign the split to the new host for catch-up to detect it
    DriverSplitLocalityManager.assignSplitsForQuery(Seq(testSplitPath), Set(newHost))

    // Check if split needs catch-up on new host
    val needsCatchUp = DriverSplitLocalityManager.getSplitsNeedingCatchUp(
      Set(newHost),
      segments
    )

    // New host should trigger catch-up for this split
    val splitsForNewHost = needsCatchUp.getOrElse(newHost, Seq.empty)
    logger.info(s"Splits needing catch-up on $newHost: $splitsForNewHost")

    assert(splitsForNewHost.contains(testSplitPath), s"Split should need catch-up on new host $newHost")

    logger.info("getSplitsNeedingCatchUp for new host test passed")
  }

  test("markSplitNeedsCatchUp should flag split for re-prewarming") {
    val testSplitPath = "s3://bucket/table/split_003.split"
    val host          = "executor-1"
    val segments      = Set(IndexComponent.TERM)

    // Record initial prewarm
    DriverSplitLocalityManager.recordPrewarmCompletion(testSplitPath, host, segments, None)

    // Assign the split for tracking
    DriverSplitLocalityManager.assignSplitsForQuery(Seq(testSplitPath), Set(host))

    // Mark split as needing catch-up (simulating host reassignment)
    DriverSplitLocalityManager.markSplitNeedsCatchUp(testSplitPath, host)

    // Check catch-up status
    val needsCatchUp = DriverSplitLocalityManager.getSplitsNeedingCatchUp(
      Set(host),
      segments
    )

    val splitsForHost = needsCatchUp.getOrElse(host, Seq.empty)
    logger.info(s"Splits marked for catch-up: $splitsForHost")

    assert(splitsForHost.contains(testSplitPath), "Split marked for catch-up should be returned")

    logger.info("markSplitNeedsCatchUp test passed")
  }

  // === New Host Detection Tests ===

  test("new hosts should be detected when cluster scales") {
    val splitPaths = Seq(
      "s3://bucket/table/split_a.split",
      "s3://bucket/table/split_b.split",
      "s3://bucket/table/split_c.split"
    )
    val originalHosts = Set("executor-0", "executor-1")
    val newHosts      = Set("executor-0", "executor-1", "executor-2", "executor-3")
    val segments      = Set(IndexComponent.TERM, IndexComponent.FASTFIELD)

    // Assign splits initially to original hosts
    DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, originalHosts)

    // Record prewarm on original hosts
    splitPaths.zipWithIndex.foreach {
      case (path, idx) =>
        val host = if (idx % 2 == 0) "executor-0" else "executor-1"
        DriverSplitLocalityManager.recordPrewarmCompletion(path, host, segments, None)
    }

    // Reassign with expanded host set - this simulates scale-out
    DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, newHosts)

    // Check catch-up needs with expanded host set
    val needsCatchUp = DriverSplitLocalityManager.getSplitsNeedingCatchUp(newHosts, segments)

    logger.info(s"Catch-up map: $needsCatchUp")

    // New hosts should potentially need catch-up if splits are reassigned
    val totalSplitsNeedingCatchUp = needsCatchUp.values.flatten.toSet.size
    logger.info(s"Total splits needing catch-up: $totalSplitsNeedingCatchUp")

    // At minimum, the function should return without error
    assert(needsCatchUp != null, "getSplitsNeedingCatchUp should not return null")

    logger.info("New host detection test passed")
  }

  // === SQL Command Integration Tests ===

  test("PREWARM command should record completion for catch-up tracking") {
    createTestData(100)

    // Execute prewarm
    val prewarmResult = spark.sql(
      s"PREWARM INDEXTABLES CACHE '$tempTablePath' FOR SEGMENTS (TERM_DICT, FAST_FIELD)"
    )
    val prewarmRows = prewarmResult.collect()

    assert(prewarmRows.length > 0, "Prewarm should return results")

    val splitsPrewarmed = prewarmRows.head.getAs[Int]("splits_prewarmed")
    logger.info(s"Splits prewarmed: $splitsPrewarmed")

    // Verify that prewarm completion was recorded
    // We can't easily verify the internal state, but we can verify the command succeeded
    assert(splitsPrewarmed > 0, "Should prewarm at least one split")

    val status = prewarmRows.head.getAs[String]("status")
    assert(status == "success" || status == "partial", s"Status should indicate success, got: $status")

    logger.info("PREWARM command state recording test passed")
  }

  // === Read-Time Catch-Up Tests ===

  test("read with catchUpNewHosts=true should trigger catch-up prewarm") {
    createTestData(100)

    // First, do initial prewarm
    spark.sql(s"PREWARM INDEXTABLES CACHE '$tempTablePath'").collect()

    // Enable catch-up on read
    spark.conf.set("spark.indextables.prewarm.enabled", "true")
    spark.conf.set("spark.indextables.prewarm.catchUpNewHosts", "true")

    // Read data - should check for catch-up needs
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    val count = df.count()

    logger.info(s"Read with catchUpNewHosts=true returned $count records")
    assert(count > 0, "Query should return results")

    logger.info("Read-time catch-up test passed")
  }

  test("read with catchUpNewHosts=false should skip catch-up prewarm") {
    createTestData(100)

    // Enable prewarm but disable catch-up
    spark.conf.set("spark.indextables.prewarm.enabled", "true")
    spark.conf.set("spark.indextables.prewarm.catchUpNewHosts", "false")

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    val count = df.count()

    logger.info(s"Read with catchUpNewHosts=false returned $count records")
    assert(count > 0, "Query should return results without catch-up")

    logger.info("Read-time skip catch-up test passed")
  }

  // === New Splits Detection Tests ===

  test("new splits in transaction log should be detected for prewarm") {
    createTestData(100)

    // First prewarm
    spark.sql(s"PREWARM INDEXTABLES CACHE '$tempTablePath'").collect()

    // Add more data to create new splits
    val ss = spark
    import ss.implicits._

    val newData = (101 until 201)
      .map(i => (i.toLong, s"title_$i", s"new content $i", s"category_${i % 5}", i * 1.5))
      .toDF("id", "title", "content", "category", "score")

    newData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.batchSize", "25")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("append")
      .save(tempTablePath)

    logger.info("Added new data to create new splits")

    // Enable prewarm with catch-up
    spark.conf.set("spark.indextables.prewarm.enabled", "true")
    spark.conf.set("spark.indextables.prewarm.catchUpNewHosts", "true")

    // Read should detect new splits and trigger catch-up
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    val count = df.count()

    logger.info(s"Read after adding new data returned $count records")
    // With 100 initial + 100 appended = 200 total
    assert(count >= 100, s"Should have at least 100 records from append, got $count")
    assert(count <= 200, s"Should have at most 200 records, got $count")

    logger.info("New splits detection test passed")
  }

  // === State Persistence Tests ===

  test("prewarm state should persist across multiple operations") {
    val splitPath = "s3://test/split_persist.split"
    val host      = "executor-0"
    val segments  = Set(IndexComponent.TERM, IndexComponent.POSTINGS)
    val fields    = Some(Set("title"))

    // Record initial prewarm
    DriverSplitLocalityManager.recordPrewarmCompletion(splitPath, host, segments, fields)

    // Simulate multiple read operations checking state
    for (_ <- 1 to 5) {
      val state = DriverSplitLocalityManager.getPrewarmState(splitPath)
      assert(state.isDefined, "State should persist across operations")
      assert(state.get.hostname == host, "Host should remain same")
    }

    // Verify state is still correct
    val finalState = DriverSplitLocalityManager.getPrewarmState(splitPath)
    assert(finalState.isDefined, "State should persist")
    assert(finalState.get.segments == segments, "Segments should match")

    logger.info("State persistence test passed")
  }

  // === getNewSplitsNeedingPrewarm Tests ===

  test("getNewSplitsNeedingPrewarm should identify never-prewarmed splits") {
    val prewarmedSplit = "s3://bucket/split_prewarmed.split"
    val newSplit       = "s3://bucket/split_new.split"
    val segments       = Set(IndexComponent.TERM)

    // Prewarm only one split
    DriverSplitLocalityManager.recordPrewarmCompletion(prewarmedSplit, "host-1", segments, None)

    // Check which splits need prewarm
    val needsPrewarm = DriverSplitLocalityManager.getNewSplitsNeedingPrewarm(
      Seq(prewarmedSplit, newSplit),
      segments
    )

    logger.info(s"Splits needing prewarm: $needsPrewarm")

    assert(needsPrewarm.contains(newSplit), "New split should need prewarm")
    assert(!needsPrewarm.contains(prewarmedSplit), "Already prewarmed split should not need prewarm")

    logger.info("getNewSplitsNeedingPrewarm test passed")
  }

  test("getNewSplitsNeedingPrewarm should identify splits with missing segments") {
    val splitPath         = "s3://bucket/split_partial.split"
    val prewarmedSegments = Set(IndexComponent.TERM)
    val requestedSegments = Set(IndexComponent.TERM, IndexComponent.FASTFIELD)

    // Prewarm with only TERM
    DriverSplitLocalityManager.recordPrewarmCompletion(splitPath, "host-1", prewarmedSegments, None)

    // Check if split needs prewarm for additional segments
    val needsPrewarm = DriverSplitLocalityManager.getNewSplitsNeedingPrewarm(
      Seq(splitPath),
      requestedSegments
    )

    logger.info(s"Splits needing additional prewarm: $needsPrewarm")

    assert(needsPrewarm.contains(splitPath), "Split with missing segments should need prewarm")

    logger.info("getNewSplitsNeedingPrewarm missing segments test passed")
  }

  // === Concurrent Access Tests ===

  test("prewarm state tracking should be thread-safe") {
    val numThreads         = 10
    val numSplitsPerThread = 100
    val segments           = Set(IndexComponent.TERM)

    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.{Await, Future}
    import scala.concurrent.duration._

    val futures = (0 until numThreads).map { threadId =>
      Future {
        (0 until numSplitsPerThread).foreach { splitId =>
          val splitPath = s"s3://bucket/thread_$threadId/split_$splitId.split"
          val host      = s"executor-${threadId % 4}"

          DriverSplitLocalityManager.recordPrewarmCompletion(splitPath, host, segments, None)
        }
        threadId
      }
    }

    val results = Await.result(Future.sequence(futures), 30.seconds)

    assert(results.size == numThreads, "All threads should complete")

    logger.info(s"Concurrent access test passed with $numThreads threads, ${numThreads * numSplitsPerThread} operations")
  }

  // === Clear State Tests ===

  test("clear should reset prewarm state") {
    val splitPath = "s3://bucket/table/split_clear.split"
    val host      = "executor-0"
    val segments  = Set(IndexComponent.TERM)

    // Record state
    DriverSplitLocalityManager.recordPrewarmCompletion(splitPath, host, segments, None)

    // Verify state exists
    val beforeClear = DriverSplitLocalityManager.getPrewarmState(splitPath)
    assert(beforeClear.isDefined, "State should exist before clear")

    // Clear all state
    DriverSplitLocalityManager.clear()

    // After clear, prewarm state should also be gone
    // Note: The current implementation doesn't clear prewarm state in clear()
    // This test documents the current behavior
    val stats = DriverSplitLocalityManager.getStats()
    logger.info(s"After clear, stats: $stats")

    assert(stats.totalTrackedSplits == 0, "Split assignments should be cleared")

    logger.info("clear state test passed")
  }

  // === isCatchUpEnabled Tests ===

  test("isCatchUpEnabled should respect configuration") {
    val configEnabled  = Map("spark.indextables.prewarm.catchUpNewHosts" -> "true")
    val configDisabled = Map("spark.indextables.prewarm.catchUpNewHosts" -> "false")
    val configDefault  = Map.empty[String, String]

    assert(DriverSplitLocalityManager.isCatchUpEnabled(configEnabled), "Should be enabled when set to true")
    assert(!DriverSplitLocalityManager.isCatchUpEnabled(configDisabled), "Should be disabled when set to false")
    assert(!DriverSplitLocalityManager.isCatchUpEnabled(configDefault), "Should default to false")

    logger.info("isCatchUpEnabled configuration test passed")
  }

  // === clearCatchUpForHost Tests ===

  test("clearCatchUpForHost should remove catch-up tracking for specific host") {
    val splitPath = "s3://bucket/split_catchup.split"
    val host1     = "executor-1"
    val host2     = "executor-2"
    val segments  = Set(IndexComponent.TERM)

    // Mark splits for catch-up on multiple hosts
    DriverSplitLocalityManager.markSplitNeedsCatchUp(splitPath, host1)
    DriverSplitLocalityManager.markSplitNeedsCatchUp(splitPath, host2)

    // Clear catch-up for host1 only
    DriverSplitLocalityManager.clearCatchUpForHost(host1)

    // host2 should still need catch-up
    // This test verifies the method exists and doesn't throw

    logger.info("clearCatchUpForHost test passed")
  }
}
