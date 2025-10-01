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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.sources.EqualTo
import io.indextables.spark.transaction.AddAction
import io.indextables.spark.storage.BroadcastSplitLocalityManager
import java.io.File
import java.nio.file.{Files, Paths}
import org.apache.commons.io.FileUtils
import scala.jdk.CollectionConverters._

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
    BroadcastSplitLocalityManager.clearAll()
  }

  override def afterEach(): Unit = {
    PreWarmManager.clearAll()
    BroadcastSplitLocalityManager.clearAll()
  }

  test("Pre-warm explicitly disabled should skip cache warming") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("content", StringType, nullable = false)
      )
    )

    val addActions = Seq(
      AddAction(
        path = "test-split-1.split",
        partitionValues = Map.empty,
        size = 1000,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "test-split-2.split",
        partitionValues = Map.empty,
        size = 2000,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
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
      schema,
      filters,
      broadcastConfig.value,
      isPreWarmEnabled = false
    )

    result.warmupInitiated shouldBe false
    result.totalWarmupsCreated shouldBe 0
    result.warmupAssignments shouldBe Map.empty
  }

  test("Pre-warm enabled with no preferred hosts should skip task distribution") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("content", StringType, nullable = false)
      )
    )

    val addActions = Seq(
      AddAction(
        path = "test-split-1.split",
        partitionValues = Map.empty,
        size = 1000,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "test-split-2.split",
        partitionValues = Map.empty,
        size = 2000,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val filters: Array[Any] = Array(EqualTo("content", "test"))
    val broadcastConfig = spark.sparkContext.broadcast(
      Map(
        "spark.indextables.cache.prewarm.enabled" -> "true",
        "spark.indextables.cache.maxSize"         -> "100000000"
      )
    )

    // No splits have preferred hosts (no locality information)
    val result = PreWarmManager.executePreWarm(
      spark.sparkContext,
      addActions,
      schema,
      filters,
      broadcastConfig.value,
      isPreWarmEnabled = true
    )

    result.warmupInitiated shouldBe true
    result.totalWarmupsCreated shouldBe 0 // No preferred hosts = no tasks created
    result.warmupAssignments shouldBe Map.empty
  }

  test("Pre-warm enabled with preferred hosts should create warmup tasks") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("content", StringType, nullable = false)
      )
    )

    val addActions = Seq(
      AddAction(
        path = "test-split-1.split",
        partitionValues = Map.empty,
        size = 1000,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "test-split-2.split",
        partitionValues = Map.empty,
        size = 2000,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    // Record some split locality to create preferred hosts
    BroadcastSplitLocalityManager.recordSplitAccess("test-split-1.split", "host1")
    BroadcastSplitLocalityManager.recordSplitAccess("test-split-2.split", "host2")
    BroadcastSplitLocalityManager.updateBroadcastLocality(spark.sparkContext)

    val filters: Array[Any] = Array(EqualTo("content", "test"))
    val broadcastConfig = spark.sparkContext.broadcast(
      Map(
        "spark.indextables.cache.prewarm.enabled" -> "true",
        "spark.indextables.cache.maxSize"         -> "100000000"
      )
    )

    val result = PreWarmManager.executePreWarm(
      spark.sparkContext,
      addActions,
      schema,
      filters,
      broadcastConfig.value,
      isPreWarmEnabled = true
    )

    result.warmupInitiated shouldBe true
    result.totalWarmupsCreated shouldBe >=(1) // Should create at least one task
    result.warmupAssignments.size should be >= 1
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
    val schema = StructType(
      Seq(
        StructField("content", StringType, nullable = false)
      )
    )

    val addActions = Seq(
      AddAction(
        path = "test-split.split",
        partitionValues = Map.empty,
        size = 1000,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    // Setup some locality and execute pre-warm
    BroadcastSplitLocalityManager.recordSplitAccess("test-split.split", "host1")
    BroadcastSplitLocalityManager.updateBroadcastLocality(spark.sparkContext)

    val filters: Array[Any] = Array(EqualTo("content", "test"))
    val broadcastConfig = spark.sparkContext.broadcast(
      Map(
        "spark.indextables.cache.prewarm.enabled" -> "true"
      )
    )

    val result = PreWarmManager.executePreWarm(
      spark.sparkContext,
      addActions,
      schema,
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
    val schema = StructType(
      Seq(
        StructField("content", StringType, nullable = false)
      )
    )

    val addActions = Seq(
      AddAction(
        path = "test-split-1.split",
        partitionValues = Map.empty,
        size = 1000,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "test-split-2.split",
        partitionValues = Map.empty,
        size = 2000,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val filters: Array[Any] = Array(EqualTo("content", "test"))
    val broadcastConfig = spark.sparkContext.broadcast(
      Map(
        "spark.indextables.cache.prewarm.enabled" -> "true"
      )
    )

    // Execute pre-warm (even with no preferred hosts, stats should be recorded)
    val result = PreWarmManager.executePreWarm(
      spark.sparkContext,
      addActions,
      schema,
      filters,
      broadcastConfig.value,
      isPreWarmEnabled = true
    )

    result.warmupInitiated shouldBe true

    // Generate same query hash to retrieve stats
    val queryHash = java.util.UUID
      .nameUUIDFromBytes(
        Array(EqualTo("content", "test")).map(_.toString).mkString("|").getBytes
      )
      .toString
      .take(8)

    val stats = PreWarmManager.getPreWarmStats(queryHash)
    stats should not be None
    stats.get.totalSplits shouldBe 2
    stats.get.preWarmTimeMs should be >= 0L
  }
}
