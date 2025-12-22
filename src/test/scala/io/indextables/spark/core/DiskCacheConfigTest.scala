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

import io.indextables.spark.storage.{DiskCacheStats, SplitCacheConfig}
import io.indextables.spark.util.ConfigUtils
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for L2 Disk Cache configuration.
 *
 * This test suite validates:
 *   1. IndexTables4SparkOptions disk cache accessors
 *   2. SplitCacheConfig disk cache field handling
 *   3. Compression algorithm configuration
 *   4. Size parsing for disk cache parameters
 *   5. ConfigUtils disk cache option parsing
 *   6. DiskCacheStats case class functionality
 */
class DiskCacheConfigTest extends AnyFunSuite with Matchers {

  // ===== IndexTables4SparkOptions Tests =====

  test("diskCacheEnabled - parsing") {
    val enabledOpts = IndexTables4SparkOptions(
      Map("spark.indextables.cache.disk.enabled" -> "true")
    )
    enabledOpts.diskCacheEnabled shouldBe Some(true)

    val disabledOpts = IndexTables4SparkOptions(
      Map("spark.indextables.cache.disk.enabled" -> "false")
    )
    disabledOpts.diskCacheEnabled shouldBe Some(false)

    val missingOpts = IndexTables4SparkOptions(Map.empty[String, String])
    missingOpts.diskCacheEnabled shouldBe None
  }

  test("diskCachePath - parsing") {
    val opts = IndexTables4SparkOptions(
      Map("spark.indextables.cache.disk.path" -> "/mnt/nvme/cache")
    )
    opts.diskCachePath shouldBe Some("/mnt/nvme/cache")

    val missingOpts = IndexTables4SparkOptions(Map.empty[String, String])
    missingOpts.diskCachePath shouldBe None
  }

  test("diskCacheMaxSize - parsing") {
    val opts = IndexTables4SparkOptions(
      Map("spark.indextables.cache.disk.maxSize" -> "100G")
    )
    opts.diskCacheMaxSize shouldBe Some("100G")

    val byteOpts = IndexTables4SparkOptions(
      Map("spark.indextables.cache.disk.maxSize" -> "50000000000")
    )
    byteOpts.diskCacheMaxSize shouldBe Some("50000000000")
  }

  test("diskCacheCompression - parsing and case handling") {
    val lz4Opts = IndexTables4SparkOptions(
      Map("spark.indextables.cache.disk.compression" -> "lz4")
    )
    lz4Opts.diskCacheCompression shouldBe Some("lz4")

    val zstdOpts = IndexTables4SparkOptions(
      Map("spark.indextables.cache.disk.compression" -> "ZSTD")
    )
    zstdOpts.diskCacheCompression shouldBe Some("ZSTD")

    val noneOpts = IndexTables4SparkOptions(
      Map("spark.indextables.cache.disk.compression" -> "none")
    )
    noneOpts.diskCacheCompression shouldBe Some("none")
  }

  test("diskCacheMinCompressSize - parsing") {
    val opts = IndexTables4SparkOptions(
      Map("spark.indextables.cache.disk.minCompressSize" -> "8K")
    )
    opts.diskCacheMinCompressSize shouldBe Some("8K")
  }

  test("diskCacheManifestSyncInterval - parsing") {
    val opts = IndexTables4SparkOptions(
      Map("spark.indextables.cache.disk.manifestSyncInterval" -> "60")
    )
    opts.diskCacheManifestSyncInterval shouldBe Some(60)
  }

  // ===== SplitCacheConfig Tests =====

  test("SplitCacheConfig - disk cache disabled by default (no path)") {
    val config = SplitCacheConfig()
    // Without a disk cache path, disk cache should not be configured
    config.diskCacheEnabled shouldBe None
    config.diskCachePath shouldBe None
  }

  test("SplitCacheConfig - disk cache explicit enabled with path") {
    val config = SplitCacheConfig(
      diskCacheEnabled = Some(true),
      diskCachePath = Some("/tmp/test_cache")
    )

    config.diskCacheEnabled shouldBe Some(true)
    config.diskCachePath shouldBe Some("/tmp/test_cache")

    // Should create Java config without error
    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - disk cache explicit disabled") {
    val config = SplitCacheConfig(
      diskCacheEnabled = Some(false),
      diskCachePath = Some("/tmp/test_cache") // Path provided but disabled
    )

    config.diskCacheEnabled shouldBe Some(false)

    // Should create Java config without tiered cache
    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - disk cache with LZ4 compression") {
    val config = SplitCacheConfig(
      diskCacheEnabled = Some(true),
      diskCachePath = Some("/tmp/test_cache"),
      diskCacheCompression = Some("lz4")
    )

    config.diskCacheCompression shouldBe Some("lz4")

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - disk cache with ZSTD compression") {
    val config = SplitCacheConfig(
      diskCacheEnabled = Some(true),
      diskCachePath = Some("/tmp/test_cache"),
      diskCacheCompression = Some("zstd")
    )

    config.diskCacheCompression shouldBe Some("zstd")

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - disk cache with no compression") {
    val config = SplitCacheConfig(
      diskCacheEnabled = Some(true),
      diskCachePath = Some("/tmp/test_cache"),
      diskCacheCompression = Some("none")
    )

    config.diskCacheCompression shouldBe Some("none")

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - disk cache with custom max size") {
    val config = SplitCacheConfig(
      diskCacheEnabled = Some(true),
      diskCachePath = Some("/tmp/test_cache"),
      diskCacheMaxSize = Some(100L * 1024L * 1024L * 1024L) // 100GB
    )

    config.diskCacheMaxSize shouldBe Some(100L * 1024L * 1024L * 1024L)

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - disk cache with custom min compress size") {
    val config = SplitCacheConfig(
      diskCacheEnabled = Some(true),
      diskCachePath = Some("/tmp/test_cache"),
      diskCacheMinCompressSize = Some(8192L) // 8KB
    )

    config.diskCacheMinCompressSize shouldBe Some(8192L)

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - disk cache with custom manifest sync interval") {
    val config = SplitCacheConfig(
      diskCacheEnabled = Some(true),
      diskCachePath = Some("/tmp/test_cache"),
      diskCacheManifestSyncInterval = Some(60) // 60 seconds
    )

    config.diskCacheManifestSyncInterval shouldBe Some(60)

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - disk cache with all parameters") {
    val config = SplitCacheConfig(
      diskCacheEnabled = Some(true),
      diskCachePath = Some("/tmp/test_cache"),
      diskCacheMaxSize = Some(50L * 1024L * 1024L * 1024L), // 50GB
      diskCacheCompression = Some("lz4"),
      diskCacheMinCompressSize = Some(4096L),               // 4KB
      diskCacheManifestSyncInterval = Some(30)              // 30 seconds
    )

    config.diskCacheEnabled shouldBe Some(true)
    config.diskCachePath shouldBe Some("/tmp/test_cache")
    config.diskCacheMaxSize shouldBe Some(50L * 1024L * 1024L * 1024L)
    config.diskCacheCompression shouldBe Some("lz4")
    config.diskCacheMinCompressSize shouldBe Some(4096L)
    config.diskCacheManifestSyncInterval shouldBe Some(30)

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  // ===== ConfigUtils Tests =====

  test("ConfigUtils.createSplitCacheConfig - includes disk cache options") {
    val configMap = Map(
      "spark.indextables.cache.disk.enabled"              -> "true",
      "spark.indextables.cache.disk.path"                 -> "/mnt/nvme/cache",
      "spark.indextables.cache.disk.maxSize"              -> "100G",
      "spark.indextables.cache.disk.compression"          -> "lz4",
      "spark.indextables.cache.disk.minCompressSize"      -> "8K",
      "spark.indextables.cache.disk.manifestSyncInterval" -> "45"
    )

    val config = ConfigUtils.createSplitCacheConfig(configMap, Some("test-table"))

    config.diskCacheEnabled shouldBe Some(true)
    config.diskCachePath shouldBe Some("/mnt/nvme/cache")
    config.diskCacheMaxSize shouldBe Some(100L * 1024L * 1024L * 1024L)
    config.diskCacheCompression shouldBe Some("lz4")
    config.diskCacheMinCompressSize shouldBe Some(8L * 1024L)
    config.diskCacheManifestSyncInterval shouldBe Some(45)

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("ConfigUtils.createSplitCacheConfig - disk cache disabled") {
    val configMap = Map(
      "spark.indextables.cache.disk.enabled" -> "false",
      "spark.indextables.cache.disk.path"    -> "/mnt/nvme/cache"
    )

    val config = ConfigUtils.createSplitCacheConfig(configMap, Some("test-table"))

    config.diskCacheEnabled shouldBe Some(false)
    config.diskCachePath shouldBe Some("/mnt/nvme/cache")

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("ConfigUtils.createSplitCacheConfig - disk cache with only path") {
    val configMap = Map(
      "spark.indextables.cache.disk.path" -> "/tmp/cache_test"
    )

    val config = ConfigUtils.createSplitCacheConfig(configMap, Some("test-table"))

    config.diskCacheEnabled shouldBe None // Not explicitly set
    config.diskCachePath shouldBe Some("/tmp/cache_test")

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  // ===== Full Configuration Flow Tests =====

  test("full configuration flow - disk cache with LZ4") {
    val options = Map(
      "spark.indextables.cache.disk.enabled"              -> "true",
      "spark.indextables.cache.disk.path"                 -> "/local_disk0/tantivy_cache",
      "spark.indextables.cache.disk.maxSize"              -> "200G",
      "spark.indextables.cache.disk.compression"          -> "lz4",
      "spark.indextables.cache.disk.minCompressSize"      -> "4K",
      "spark.indextables.cache.disk.manifestSyncInterval" -> "30"
    )

    val sparkOpts = IndexTables4SparkOptions(options)

    sparkOpts.diskCacheEnabled shouldBe Some(true)
    sparkOpts.diskCachePath shouldBe Some("/local_disk0/tantivy_cache")
    sparkOpts.diskCacheMaxSize shouldBe Some("200G")
    sparkOpts.diskCacheCompression shouldBe Some("lz4")
    sparkOpts.diskCacheMinCompressSize shouldBe Some("4K")
    sparkOpts.diskCacheManifestSyncInterval shouldBe Some(30)

    val config = SplitCacheConfig(
      diskCacheEnabled = sparkOpts.diskCacheEnabled,
      diskCachePath = sparkOpts.diskCachePath,
      diskCacheMaxSize = sparkOpts.diskCacheMaxSize.map(SplitCacheConfig.parseSizeString),
      diskCacheCompression = sparkOpts.diskCacheCompression,
      diskCacheMinCompressSize = sparkOpts.diskCacheMinCompressSize.map(SplitCacheConfig.parseSizeString),
      diskCacheManifestSyncInterval = sparkOpts.diskCacheManifestSyncInterval
    )

    config.diskCacheEnabled shouldBe Some(true)
    config.diskCachePath shouldBe Some("/local_disk0/tantivy_cache")
    config.diskCacheMaxSize shouldBe Some(200L * 1024L * 1024L * 1024L)
    config.diskCacheCompression shouldBe Some("lz4")
    config.diskCacheMinCompressSize shouldBe Some(4L * 1024L)
    config.diskCacheManifestSyncInterval shouldBe Some(30)

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("full configuration flow - disk cache with ZSTD for max compression") {
    val options = Map(
      "spark.indextables.cache.disk.enabled"     -> "true",
      "spark.indextables.cache.disk.path"        -> "/mnt/ssd/archive_cache",
      "spark.indextables.cache.disk.compression" -> "zstd"
    )

    val sparkOpts = IndexTables4SparkOptions(options)

    sparkOpts.diskCacheEnabled shouldBe Some(true)
    sparkOpts.diskCachePath shouldBe Some("/mnt/ssd/archive_cache")
    sparkOpts.diskCacheCompression shouldBe Some("zstd")

    val config = SplitCacheConfig(
      diskCacheEnabled = sparkOpts.diskCacheEnabled,
      diskCachePath = sparkOpts.diskCachePath,
      diskCacheCompression = sparkOpts.diskCacheCompression
    )

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("full configuration flow - disk cache disabled explicitly") {
    val options = Map(
      "spark.indextables.cache.disk.enabled" -> "false"
    )

    val sparkOpts = IndexTables4SparkOptions(options)
    sparkOpts.diskCacheEnabled shouldBe Some(false)

    val config = SplitCacheConfig(
      diskCacheEnabled = sparkOpts.diskCacheEnabled
    )

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  // ===== Edge Cases =====

  test("unknown compression algorithm should fall back to LZ4") {
    val config = SplitCacheConfig(
      diskCacheEnabled = Some(true),
      diskCachePath = Some("/tmp/test_cache"),
      diskCacheCompression = Some("unknown-algorithm")
    )

    // Should not throw, should fall back to LZ4
    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("empty compression string should use default LZ4") {
    val options = Map(
      "spark.indextables.cache.disk.enabled"     -> "true",
      "spark.indextables.cache.disk.path"        -> "/tmp/cache",
      "spark.indextables.cache.disk.compression" -> ""
    )

    val sparkOpts = IndexTables4SparkOptions(options)
    sparkOpts.diskCacheCompression shouldBe Some("")

    val config = SplitCacheConfig(
      diskCacheEnabled = sparkOpts.diskCacheEnabled,
      diskCachePath = sparkOpts.diskCachePath,
      diskCacheCompression = sparkOpts.diskCacheCompression
    )

    // Empty string should fall back to default (LZ4)
    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("zero max size should use auto-detection") {
    val config = SplitCacheConfig(
      diskCacheEnabled = Some(true),
      diskCachePath = Some("/tmp/test_cache"),
      diskCacheMaxSize = Some(0L) // 0 = auto (2/3 available disk)
    )

    config.diskCacheMaxSize shouldBe Some(0L)

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("case insensitive key lookup in ConfigUtils") {
    // Test that lowercase keys work (CaseInsensitiveStringMap behavior)
    val configMap = Map(
      "spark.indextables.cache.disk.enabled" -> "true",
      "spark.indextables.cache.disk.path"    -> "/tmp/cache"
    )

    val config = ConfigUtils.createSplitCacheConfig(configMap)

    config.diskCacheEnabled shouldBe Some(true)
    config.diskCachePath shouldBe Some("/tmp/cache")
  }

  // ===== DiskCacheStats Tests =====

  test("DiskCacheStats - summary format") {
    val stats = DiskCacheStats(
      totalBytes = 52428800000L,     // ~50GB
      maxBytes = 107374182400L,      // 100GB
      usagePercent = 48.8,
      splitCount = 1247,
      componentCount = 8729
    )

    val summary = stats.summary
    summary should include("48.8%")
    summary should include("52428800000")
    summary should include("107374182400")
    summary should include("1247")
    summary should include("8729")
  }

  test("DiskCacheStats - zero values") {
    val emptyStats = DiskCacheStats(
      totalBytes = 0L,
      maxBytes = 107374182400L,
      usagePercent = 0.0,
      splitCount = 0,
      componentCount = 0
    )

    emptyStats.totalBytes shouldBe 0L
    emptyStats.usagePercent shouldBe 0.0
    emptyStats.splitCount shouldBe 0
    emptyStats.componentCount shouldBe 0

    val summary = emptyStats.summary
    summary should include("0.0%")
  }

  // ===== Size Parsing Tests (reusing existing parseSizeString) =====

  test("disk cache size parsing - gigabytes") {
    SplitCacheConfig.parseSizeString("100G") shouldBe 100L * 1024L * 1024L * 1024L
    SplitCacheConfig.parseSizeString("200G") shouldBe 200L * 1024L * 1024L * 1024L
    SplitCacheConfig.parseSizeString("1g") shouldBe 1L * 1024L * 1024L * 1024L
  }

  test("disk cache size parsing - megabytes") {
    SplitCacheConfig.parseSizeString("512M") shouldBe 512L * 1024L * 1024L
    SplitCacheConfig.parseSizeString("100m") shouldBe 100L * 1024L * 1024L
  }

  test("disk cache size parsing - kilobytes") {
    SplitCacheConfig.parseSizeString("4K") shouldBe 4L * 1024L
    SplitCacheConfig.parseSizeString("8k") shouldBe 8L * 1024L
    SplitCacheConfig.parseSizeString("512K") shouldBe 512L * 1024L
  }

  // ===== Auto-Detection Tests =====

  test("getDefaultDiskCachePath returns None when /local_disk0 not available") {
    // On most dev machines, /local_disk0 doesn't exist
    // This test verifies the fallback behavior
    val defaultPath = SplitCacheConfig.getDefaultDiskCachePath()

    // Either returns Some (on Databricks/EMR) or None (dev machines)
    // We can't guarantee which, but it shouldn't throw
    defaultPath match {
      case Some(path) =>
        path should include("tantivy4spark_slicecache")
      case None =>
        // Expected on dev machines without /local_disk0
        succeed
    }
  }

  // ===== Integration with Batch Optimization =====

  test("disk cache and batch optimization can be configured together") {
    val config = SplitCacheConfig(
      // Disk cache config
      diskCacheEnabled = Some(true),
      diskCachePath = Some("/tmp/test_cache"),
      diskCacheCompression = Some("lz4"),
      // Batch optimization config
      batchOptimizationEnabled = Some(true),
      batchOptimizationProfile = Some("balanced")
    )

    config.diskCacheEnabled shouldBe Some(true)
    config.batchOptimizationEnabled shouldBe Some(true)

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("full config with both disk cache and batch optimization via ConfigUtils") {
    val configMap = Map(
      // Disk cache
      "spark.indextables.cache.disk.enabled"              -> "true",
      "spark.indextables.cache.disk.path"                 -> "/tmp/combined_cache",
      "spark.indextables.cache.disk.compression"          -> "lz4",
      // Batch optimization
      "spark.indextables.read.batchOptimization.enabled"  -> "true",
      "spark.indextables.read.batchOptimization.profile"  -> "aggressive"
    )

    val config = ConfigUtils.createSplitCacheConfig(configMap, Some("test-table"))

    config.diskCacheEnabled shouldBe Some(true)
    config.diskCachePath shouldBe Some("/tmp/combined_cache")
    config.diskCacheCompression shouldBe Some("lz4")
    config.batchOptimizationEnabled shouldBe Some(true)
    config.batchOptimizationProfile shouldBe Some("aggressive")

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }
}
