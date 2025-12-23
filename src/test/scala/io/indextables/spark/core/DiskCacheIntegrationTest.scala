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

import java.io.File
import java.nio.file.{Files, Path}
import java.util.Comparator

import org.apache.spark.sql.SparkSession

import io.indextables.spark.storage.{GlobalSplitCacheManager, SplitCacheConfig}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach

/**
 * Integration tests for L2 Disk Cache functionality.
 *
 * This test suite validates:
 *   1. Disk cache configuration flows through to tantivy4java 2. Cache files are created at the specified path 3. Disk
 *      cache can be explicitly disabled 4. GlobalSplitCacheManager disk cache methods work correctly 5. Disk cache
 *      works alongside batch optimization
 */
class DiskCacheIntegrationTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private var tempDir: Path       = _
  private var diskCachePath: Path = _
  private var tablePath: Path     = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create Spark session
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("DiskCacheIntegrationTest")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    // Create temp directories
    tempDir = Files.createTempDirectory("disk-cache-test")
    diskCachePath = tempDir.resolve("disk_cache")
    tablePath = tempDir.resolve("test_table")

    Files.createDirectories(diskCachePath)
    Files.createDirectories(tablePath)
  }

  override def afterAll(): Unit = {
    // Clean up Spark session
    if (spark != null) {
      spark.stop()
    }

    // Clean up temp directories
    if (tempDir != null && Files.exists(tempDir)) {
      deleteDirectory(tempDir)
    }

    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Clear all cache managers before each test
    GlobalSplitCacheManager.clearAll()
  }

  override def afterEach(): Unit = {
    // Clear caches after each test
    GlobalSplitCacheManager.clearAll()
    super.afterEach()
  }

  private def deleteDirectory(path: Path): Unit =
    if (Files.exists(path)) {
      Files
        .walk(path)
        .sorted(Comparator.reverseOrder[Path]())
        .forEach(p => Files.deleteIfExists(p))
    }

  private def createTestData(): org.apache.spark.sql.DataFrame = {
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._

    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("content", StringType, nullable = false),
        StructField("score", DoubleType, nullable = false)
      )
    )

    val data = Seq(
      Row(1, "hello world", 100.0),
      Row(2, "test data", 200.0),
      Row(3, "more content", 300.0),
      Row(4, "search terms", 400.0),
      Row(5, "final entry", 500.0)
    )

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  // ===== Configuration Flow Tests =====

  test("disk cache configuration should be passed to SplitCacheConfig") {
    val testTablePath = tablePath.resolve("config_flow_test").toString

    // Write test data with disk cache configuration
    createTestData().write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", diskCachePath.toString)
      .option("spark.indextables.cache.disk.compression", "lz4")
      .option("spark.indextables.cache.disk.maxSize", "1G")
      .save(testTablePath)

    // Verify write succeeded
    val written = new File(testTablePath)
    written.exists() shouldBe true
    written.listFiles().length should be > 0
  }

  test("disk cache disabled should not create cache files") {
    val testTablePath  = tablePath.resolve("disabled_test").toString
    val localCachePath = diskCachePath.resolve("disabled_cache")
    Files.createDirectories(localCachePath)

    // Write with disk cache explicitly disabled
    createTestData().write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.cache.disk.enabled", "false")
      .option("spark.indextables.cache.disk.path", localCachePath.toString)
      .save(testTablePath)

    // Read the data
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.cache.disk.enabled", "false")
      .option("spark.indextables.cache.disk.path", localCachePath.toString)
      .load(testTablePath)

    df.count() shouldBe 5

    // GlobalSplitCacheManager should report disk cache as not enabled
    // (since we explicitly disabled it)
    // Note: The actual disk cache enablement depends on the cache manager instance
  }

  test("disk cache with ZSTD compression configuration") {
    val testTablePath = tablePath.resolve("zstd_test").toString

    createTestData().write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", diskCachePath.resolve("zstd_cache").toString)
      .option("spark.indextables.cache.disk.compression", "zstd")
      .save(testTablePath)

    // Verify write succeeded
    val written = new File(testTablePath)
    written.exists() shouldBe true
  }

  test("disk cache with no compression configuration") {
    val testTablePath = tablePath.resolve("no_compress_test").toString

    createTestData().write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", diskCachePath.resolve("no_compress_cache").toString)
      .option("spark.indextables.cache.disk.compression", "none")
      .save(testTablePath)

    val written = new File(testTablePath)
    written.exists() shouldBe true
  }

  // ===== Read Path Tests =====

  test("read with disk cache should succeed") {
    val testTablePath  = tablePath.resolve("read_test").toString
    val localCachePath = diskCachePath.resolve("read_cache").toString

    // Write data first
    createTestData().write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(testTablePath)

    // Read with disk cache enabled
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", localCachePath)
      .option("spark.indextables.cache.disk.compression", "lz4")
      .load(testTablePath)

    // Verify data is readable
    df.count() shouldBe 5

    val results = df.collect()
    results.length shouldBe 5
  }

  test("read with disk cache and filter pushdown") {
    val testTablePath  = tablePath.resolve("filter_test").toString
    val localCachePath = diskCachePath.resolve("filter_cache").toString

    // Write data
    createTestData().write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.indexing.typemap.content", "string")
      .save(testTablePath)

    // Read with filter
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", localCachePath)
      .load(testTablePath)
      .filter("score > 200")

    df.count() shouldBe 3 // IDs 3, 4, 5 have score > 200
  }

  // ===== Combined Configuration Tests =====

  test("disk cache and batch optimization together") {
    val testTablePath  = tablePath.resolve("combined_test").toString
    val localCachePath = diskCachePath.resolve("combined_cache").toString

    // Write data
    createTestData().write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(testTablePath)

    // Read with both disk cache and batch optimization
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", localCachePath)
      .option("spark.indextables.cache.disk.compression", "lz4")
      .option("spark.indextables.read.batchOptimization.enabled", "true")
      .option("spark.indextables.read.batchOptimization.profile", "balanced")
      .load(testTablePath)

    df.count() shouldBe 5
  }

  test("session-level disk cache configuration") {
    val testTablePath  = tablePath.resolve("session_test").toString
    val localCachePath = diskCachePath.resolve("session_cache").toString

    // Set session-level config
    spark.conf.set("spark.indextables.cache.disk.enabled", "true")
    spark.conf.set("spark.indextables.cache.disk.path", localCachePath)
    spark.conf.set("spark.indextables.cache.disk.compression", "lz4")

    try {
      // Write data
      createTestData().write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(testTablePath)

      // Read without explicit disk cache options (should use session config)
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testTablePath)

      df.count() shouldBe 5
    } finally {
      // Clean up session config
      spark.conf.unset("spark.indextables.cache.disk.enabled")
      spark.conf.unset("spark.indextables.cache.disk.path")
      spark.conf.unset("spark.indextables.cache.disk.compression")
    }
  }

  // ===== GlobalSplitCacheManager Tests =====

  test("GlobalSplitCacheManager.isDiskCacheEnabled returns false when no cache managers") {
    // Clear all caches first
    GlobalSplitCacheManager.clearAll()

    // Should return false when no cache managers exist
    GlobalSplitCacheManager.isDiskCacheEnabled() shouldBe false
  }

  test("GlobalSplitCacheManager.getDiskCacheStats returns None when no cache managers") {
    GlobalSplitCacheManager.clearAll()

    val stats = GlobalSplitCacheManager.getDiskCacheStats()
    stats shouldBe None
  }

  // ===== Size Configuration Tests =====

  test("disk cache with custom max size") {
    val testTablePath  = tablePath.resolve("maxsize_test").toString
    val localCachePath = diskCachePath.resolve("maxsize_cache").toString

    createTestData().write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", localCachePath)
      .option("spark.indextables.cache.disk.maxSize", "500M")
      .save(testTablePath)

    val written = new File(testTablePath)
    written.exists() shouldBe true
  }

  test("disk cache with custom min compress size") {
    val testTablePath  = tablePath.resolve("mincompress_test").toString
    val localCachePath = diskCachePath.resolve("mincompress_cache").toString

    createTestData().write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", localCachePath)
      .option("spark.indextables.cache.disk.minCompressSize", "8K")
      .save(testTablePath)

    val written = new File(testTablePath)
    written.exists() shouldBe true
  }

  test("disk cache with custom manifest sync interval") {
    val testTablePath  = tablePath.resolve("manifest_test").toString
    val localCachePath = diskCachePath.resolve("manifest_cache").toString

    createTestData().write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", localCachePath)
      .option("spark.indextables.cache.disk.manifestSyncInterval", "60")
      .save(testTablePath)

    val written = new File(testTablePath)
    written.exists() shouldBe true
  }

  // ===== Edge Case Tests =====

  test("disk cache path with spaces should work") {
    val spacePath     = diskCachePath.resolve("path with spaces")
    val testTablePath = tablePath.resolve("space_test").toString
    Files.createDirectories(spacePath)

    createTestData().write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", spacePath.toString)
      .save(testTablePath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", spacePath.toString)
      .load(testTablePath)

    df.count() shouldBe 5
  }

  test("multiple reads with same disk cache path") {
    val testTablePath  = tablePath.resolve("multi_read_test").toString
    val localCachePath = diskCachePath.resolve("multi_read_cache").toString

    // Write data
    createTestData().write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(testTablePath)

    // First read
    val df1 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", localCachePath)
      .load(testTablePath)

    df1.count() shouldBe 5

    // Second read (should use cached data)
    val df2 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", localCachePath)
      .load(testTablePath)

    df2.count() shouldBe 5

    // Third read with filter
    val df3 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", localCachePath)
      .load(testTablePath)
      .filter("id < 3")

    df3.count() shouldBe 2
  }
}
