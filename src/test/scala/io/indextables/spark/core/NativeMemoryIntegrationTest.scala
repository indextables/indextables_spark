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

package io.indextables.spark.core

import java.nio.file.Files

import io.indextables.tantivy4java.memory.NativeMemoryManager

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/**
 * Integration tests verifying that tantivy4java's unified memory pool is wired into
 * Spark's unified memory manager via SparkUnifiedMemoryAccountant.
 *
 * Uses a dedicated SparkSession with off-heap memory enabled, since our MemoryConsumer
 * uses MemoryMode.OFF_HEAP and Spark requires `spark.memory.offHeap.enabled=true` with
 * a non-zero `spark.memory.offHeap.size` for off-heap allocations to succeed.
 */
class NativeMemoryIntegrationTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  protected var spark: SparkSession = _
  protected var tempDir: String     = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("NativeMemoryIntegrationTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      // Enable off-heap memory for native memory tracking via MemoryMode.OFF_HEAP
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "2g")
      // Test AWS defaults
      .config("spark.indextables.aws.accessKey", "test-default-access-key")
      .config("spark.indextables.aws.secretKey", "test-default-secret-key")
      .config("spark.indextables.aws.sessionToken", "test-default-session-token")
      .config("spark.indextables.s3.pathStyleAccess", "true")
      .config("spark.indextables.aws.region", "us-east-1")
      .config("spark.indextables.s3.endpoint", "http://localhost:10101")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    _root_.io.indextables.spark.storage.SplitConversionThrottle.initialize(
      maxParallelism = Runtime.getRuntime.availableProcessors() max 1
    )
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  override def beforeEach(): Unit =
    tempDir = Files.createTempDirectory("tantivy4spark-test").toString

  override def afterEach(): Unit =
    if (tempDir != null) {
      deleteRecursively(new java.io.File(tempDir))
    }

  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) file.listFiles().foreach(deleteRecursively)
    file.delete()
  }

  test("native memory pool should be configured after write operation") {
    val tablePath = s"file://$tempDir/native_memory_test"

    // Write test data — triggers NativeMemoryInitializer.ensureInitialized() on executor
    spark
      .range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text")
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Verify the memory pool was configured via setAccountant()
    assert(NativeMemoryManager.isConfigured(), "Native memory pool should be configured after a write")

    // Verify stats API is functional
    val stats = NativeMemoryManager.getStats()
    info(s"Native memory stats after write:")
    info(s"  used: ${stats.getUsedBytes} bytes")
    info(s"  peak: ${stats.getPeakBytes} bytes")
    info(s"  granted: ${stats.getGrantedBytes} bytes")

    // After a write, peak bytes must be > 0 — the Arrow FFI write path tracks
    // index writer heap via MemoryReservation under the "index_writer" category.
    stats.getUsedBytes should be >= 0L
    stats.getPeakBytes should be > 0L
  }

  test("native memory stats API should be functional") {
    val tablePath = s"file://$tempDir/native_memory_stats_test"

    spark
      .range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text")
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // resetPeak should work without error
    val oldPeak = NativeMemoryManager.resetPeak()
    oldPeak should be >= 0L

    // Category peak breakdown should include "index_writer" from the Arrow FFI write path
    val stats         = NativeMemoryManager.getStats()
    val peakBreakdown = stats.getCategoryPeakBreakdown
    peakBreakdown should not be null
    peakBreakdown.asScala should contain key "index_writer"

    info(s"Category peak breakdown:")
    peakBreakdown.asScala.foreach { case (category, bytes) =>
      info(s"  $category: $bytes bytes (peak)")
    }
  }

  test("DESCRIBE INDEXTABLES ENVIRONMENT should include native memory stats") {
    val tablePath = s"file://$tempDir/native_memory_describe_test"

    spark
      .range(0, 10)
      .selectExpr("id", "CAST(id AS STRING) as text")
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.sql("DESCRIBE INDEXTABLES ENVIRONMENT").collect()

    val nativeMemoryRows = result.filter(_.getString(2) == "native_memory")
    info(s"Found ${nativeMemoryRows.length} native_memory rows in DESCRIBE ENVIRONMENT output:")
    nativeMemoryRows.foreach { row =>
      info(s"  ${row.getString(3)} = ${row.getString(4)}")
    }

    assert(nativeMemoryRows.nonEmpty, "DESCRIBE ENVIRONMENT should include native_memory rows")

    val propertyNames = nativeMemoryRows.map(_.getString(3)).toSet
    assert(propertyNames.contains("native_memory.configured"), "Should have native_memory.configured")
    assert(propertyNames.contains("native_memory.used_bytes"), "Should have native_memory.used_bytes")
    assert(propertyNames.contains("native_memory.peak_bytes"), "Should have native_memory.peak_bytes")
    assert(propertyNames.contains("native_memory.granted_bytes"), "Should have native_memory.granted_bytes")

    // configured should be "true" since we wrote data (triggers init)
    val configuredRow = nativeMemoryRows.find(_.getString(3) == "native_memory.configured")
    configuredRow.get.getString(4) shouldBe "true"

    // Category peak breakdown should include index_writer
    val categoryPeakRows = nativeMemoryRows.filter(_.getString(3).startsWith("native_memory.category_peak."))
    assert(categoryPeakRows.nonEmpty, "DESCRIBE ENVIRONMENT should include category peak rows")
    val categoryNames = categoryPeakRows.map(_.getString(3).stripPrefix("native_memory.category_peak.")).toSet
    assert(categoryNames.contains("index_writer"), "Should have index_writer category peak")
  }

  test("native memory pool should track read path operations") {
    val tablePath = s"file://$tempDir/native_memory_read_test"

    // Write test data
    spark
      .range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text")
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    NativeMemoryManager.resetPeak()

    // Read path — triggers NativeMemoryInitializer via GlobalSplitCacheManager
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val rows = df.filter("id > 50").collect()
    assert(rows.nonEmpty, "Read should return rows")

    // Pool should still be configured after read
    assert(NativeMemoryManager.isConfigured(), "Native memory pool should be configured after read")

    val stats = NativeMemoryManager.getStats()
    info(s"Native memory stats after read: peak=${stats.getPeakBytes}, used=${stats.getUsedBytes}")
  }

  test("native memory pool should track simple aggregate path") {
    val tablePath = s"file://$tempDir/native_memory_agg_test"

    spark
      .range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text")
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    NativeMemoryManager.resetPeak()

    // Simple aggregate pushdown — triggers SimpleAggregateColumnarReader
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val count = df.count()
    assert(count == 100, s"Count should be 100, got $count")

    assert(NativeMemoryManager.isConfigured(), "Native memory pool should be configured after aggregate")

    val stats = NativeMemoryManager.getStats()
    info(s"Native memory stats after aggregate: peak=${stats.getPeakBytes}, used=${stats.getUsedBytes}")
  }

  test("native memory pool should track group-by aggregate path") {
    val tablePath = s"file://$tempDir/native_memory_groupby_test"

    spark
      .range(0, 100)
      .selectExpr("id", "CAST(id % 10 AS STRING) as category", "CAST(id AS DOUBLE) as score")
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(tablePath)

    NativeMemoryManager.resetPeak()

    // Group-by aggregate — triggers GroupByAggregateColumnarReader
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val result = df.groupBy("category").count().collect()
    assert(result.nonEmpty, "Group-by should return results")

    assert(NativeMemoryManager.isConfigured(), "Native memory pool should be configured after group-by aggregate")

    val stats = NativeMemoryManager.getStats()
    info(s"Native memory stats after group-by: peak=${stats.getPeakBytes}, used=${stats.getUsedBytes}")
  }

  test("native memory pool should track merge path") {
    val tablePath = s"file://$tempDir/native_memory_merge_test"

    // Write multiple batches to create multiple splits
    for (i <- 0 until 3) {
      spark
        .range(i * 100, (i + 1) * 100)
        .selectExpr("id", "CAST(id AS STRING) as text")
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 0) "overwrite" else "append")
        .save(tablePath)
    }

    NativeMemoryManager.resetPeak()

    // Merge splits — triggers NativeMemoryInitializer in MergeSplitsCommand.
    // Off-heap pool is 2GB, merge requests 3x default heap (3GB) which exceeds pool.
    // Reduce merge heap so it fits: 256MB * 3 = 768MB < 2GB pool.
    spark.conf.set("spark.indextables.merge.heapSize", "256m")
    try {
      spark.sql(s"MERGE SPLITS '$tablePath'")
    } finally {
      spark.conf.unset("spark.indextables.merge.heapSize")
    }

    assert(NativeMemoryManager.isConfigured(), "Native memory pool should be configured after merge")

    val stats = NativeMemoryManager.getStats()
    info(s"Native memory stats after merge: peak=${stats.getPeakBytes}, used=${stats.getUsedBytes}")
    // Merge should have triggered native memory allocation
    stats.getPeakBytes should be > 0L
  }

  test("NATIVE_MEMORY_ENABLED_KEY should have correct config key string") {
    // Verifies the constant value. Behavioral testing of the disabled path requires a
    // separate JVM (OnceLock prevents re-initialization within the same process).
    val key = io.indextables.spark.memory.NativeMemoryInitializer.NATIVE_MEMORY_ENABLED_KEY
    key shouldBe "spark.indextables.native.memory.enabled"
  }
}
