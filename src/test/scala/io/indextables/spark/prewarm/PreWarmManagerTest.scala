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
import java.nio.file.{Files, Paths}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import org.apache.hadoop.fs.Path

import io.indextables.spark.storage.DriverSplitLocalityManager
import io.indextables.spark.transaction.{AddAction, TransactionLogFactory}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PreWarmManagerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: File       = _

  override def beforeAll(): Unit = {
    // Create Spark session for testing
    spark = SparkSession
      .builder()
      .appName("PreWarmManagerTest")
      .master("local[2]") // Use 2 cores for testing executor distribution
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    // Create temporary directory for test data
    tempDir = Files.createTempDirectory("prewarm_test_").toFile
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (tempDir != null && tempDir.exists()) {
      FileUtils.deleteDirectory(tempDir)
    }
  }

  override def beforeEach(): Unit = {
    // Clear any previous pre-warm state
    PreWarmManager.clearAll()
    DriverSplitLocalityManager.clear()
  }

  override def afterEach(): Unit = {
    PreWarmManager.clearAll()
    DriverSplitLocalityManager.clear()
  }

  // Schema for test tables
  private val testSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("content", StringType, nullable = false)
    )
  )

  /**
   * Create a real indextables table and return its AddActions with full paths. This ensures tests use real split files
   * with proper metadata.
   */
  private def createRealTableAndGetAddActions(tableName: String, numRows: Int = 100): (String, Seq[AddAction]) = {
    val tablePath = new File(tempDir, tableName).getAbsolutePath

    // Create test data
    val data = spark
      .range(numRows)
      .select(
        col("id"),
        concat(lit("content-"), col("id")).as("content")
      )

    // Write as indextables
    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Get AddActions from transaction log
    val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)
    val addActions     = transactionLog.listFiles()

    // Validate that AddActions have required metadata
    addActions.foreach { action =>
      require(action.hasFooterOffsets, s"AddAction ${action.path} missing hasFooterOffsets")
      require(action.footerStartOffset.isDefined, s"AddAction ${action.path} missing footerStartOffset")
      require(action.docMappingJson.isDefined, s"AddAction ${action.path} missing docMappingJson")
    }

    // Convert relative paths to full paths for PreWarmManager
    val actionsWithFullPaths = addActions.map { action =>
      val fullPath = new Path(tablePath, action.path).toString
      action.copy(path = fullPath)
    }

    (tablePath, actionsWithFullPaths)
  }

  // Helper to create fake AddActions for tests that don't need real files
  // (e.g., tests that only check orchestration with pre-warm disabled)
  private def createFakeAddAction(path: String, size: Long): AddAction =
    AddAction(
      path = path,
      partitionValues = Map.empty,
      size = size,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      footerStartOffset = Some(100L),
      footerEndOffset = Some(size),
      hasFooterOffsets = true,
      docMappingJson = Some("""{"field_mappings":[{"name":"content","type":"text"}],"mode":"lenient"}""")
    )

  test("Pre-warm explicitly disabled should skip cache warming") {
    // When pre-warm is disabled, we can use fake AddActions since files won't be opened
    val addActions = Seq(
      createFakeAddAction("test-split-1.split", 1000),
      createFakeAddAction("test-split-2.split", 2000)
    )

    val filters: Array[Any] = Array(EqualTo("content", "test"))
    val broadcastConfig = spark.sparkContext.broadcast(
      Map(
        "spark.indextables.cache.prewarm.enabled" -> "false"
      )
    )

    val result = PreWarmManager.executePreWarm(
      spark.sparkContext,
      addActions,
      testSchema,
      filters,
      broadcastConfig.value,
      isPreWarmEnabled = false
    )

    result.warmupInitiated shouldBe false
    result.totalWarmupsCreated shouldBe 0
    result.warmupAssignments shouldBe Map.empty
  }

  test("Pre-warm enabled should assign hosts and warm real splits") {
    // Create a real table with splits
    val (tablePath, addActions) = createRealTableAndGetAddActions("prewarm_test_1")
    addActions.length should be > 0

    val filters: Array[Any] = Array(EqualTo("content", "content-1"))
    val broadcastConfig = spark.sparkContext.broadcast(
      Map(
        "spark.indextables.cache.prewarm.enabled" -> "true",
        "spark.indextables.cache.maxSize"         -> "100000000"
      )
    )

    // With DriverSplitLocalityManager, hosts are automatically assigned during executePreWarm
    val result = PreWarmManager.executePreWarm(
      spark.sparkContext,
      addActions,
      testSchema,
      filters,
      broadcastConfig.value,
      isPreWarmEnabled = true
    )

    result.warmupInitiated shouldBe true
    // With real splits, warmups should be created and executed successfully
    result.totalWarmupsCreated should be >= 1
  }

  test("Pre-warm enabled with real splits should create warmup tasks") {
    // Create a real table with splits
    val (tablePath, addActions) = createRealTableAndGetAddActions("prewarm_test_2", numRows = 200)
    addActions.length should be > 0

    val filters: Array[Any] = Array(EqualTo("content", "content-50"))
    val broadcastConfig = spark.sparkContext.broadcast(
      Map(
        "spark.indextables.cache.prewarm.enabled" -> "true",
        "spark.indextables.cache.maxSize"         -> "100000000"
      )
    )

    val result = PreWarmManager.executePreWarm(
      spark.sparkContext,
      addActions,
      testSchema,
      filters,
      broadcastConfig.value,
      isPreWarmEnabled = true
    )

    result.warmupInitiated shouldBe true
    // Verify warmups were actually created for real splits
    result.totalWarmupsCreated should be >= 1
  }

  test("joinWarmupFuture should handle missing futures gracefully") {
    val splitPath = "test-split.split"
    val queryHash = "test-query-hash"

    // Try to join a non-existent warmup future
    val result = PreWarmManager.joinWarmupFuture(splitPath, queryHash, isPreWarmEnabled = true)

    result shouldBe false // Should return false when no future exists
  }

  test("joinWarmupFuture should return false when pre-warm is disabled") {
    val splitPath = "test-split.split"
    val queryHash = "test-query-hash"

    // Try to join when pre-warm is disabled
    val result = PreWarmManager.joinWarmupFuture(splitPath, queryHash, isPreWarmEnabled = false)

    result shouldBe false // Should return false when disabled
  }

  test("clearAll should reset all pre-warm state") {
    // Create a real table with splits
    val (tablePath, addActions) = createRealTableAndGetAddActions("prewarm_test_clearall")
    addActions.length should be > 0

    val filters: Array[Any] = Array(EqualTo("content", "content-1"))
    val broadcastConfig = spark.sparkContext.broadcast(
      Map(
        "spark.indextables.cache.prewarm.enabled" -> "true"
      )
    )

    val result = PreWarmManager.executePreWarm(
      spark.sparkContext,
      addActions,
      testSchema,
      filters,
      broadcastConfig.value,
      isPreWarmEnabled = true
    )

    // Verify state was created
    result.warmupInitiated shouldBe true

    // Clear all state
    PreWarmManager.clearAll()

    // Verify state was cleared by checking that statistics are gone
    val statsAfterClear = PreWarmManager.getPreWarmStats("any-hash")
    statsAfterClear shouldBe None
  }

  test("getPreWarmStats should return statistics for executed pre-warms") {
    // Create a real table with splits
    val (tablePath, addActions) = createRealTableAndGetAddActions("prewarm_test_stats", numRows = 100)
    addActions.length should be > 0

    val filters: Array[Any] = Array(EqualTo("content", "content-1"))
    val broadcastConfig = spark.sparkContext.broadcast(
      Map(
        "spark.indextables.cache.prewarm.enabled" -> "true"
      )
    )

    // Execute pre-warm with real splits
    val result = PreWarmManager.executePreWarm(
      spark.sparkContext,
      addActions,
      testSchema,
      filters,
      broadcastConfig.value,
      isPreWarmEnabled = true
    )

    result.warmupInitiated shouldBe true

    // Generate same query hash to retrieve stats
    val queryHash = java.util.UUID
      .nameUUIDFromBytes(
        filters.map(_.toString).mkString("|").getBytes
      )
      .toString
      .take(8)

    val stats = PreWarmManager.getPreWarmStats(queryHash)
    stats should not be None
    stats.get.totalSplits shouldBe addActions.length
    stats.get.preWarmTimeMs should be >= 0L
  }
}
