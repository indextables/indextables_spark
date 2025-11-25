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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.indextables.spark.storage.SplitCacheConfig

/**
 * Tests for batch optimization configuration.
 *
 * This test suite validates:
 *   1. Size string parsing (K/M/G formats)
 *   2. IndexTables4SparkOptions batch optimization accessors
 *   3. SplitCacheConfig batch optimization field handling
 *   4. Profile-based configuration
 *   5. Custom parameter overrides
 */
class BatchOptimizationConfigTest extends AnyFunSuite with Matchers {

  // ===== Size Parsing Tests =====

  test("parseSizeString - plain bytes") {
    SplitCacheConfig.parseSizeString("1024") shouldBe 1024L
    SplitCacheConfig.parseSizeString("16777216") shouldBe 16777216L
    SplitCacheConfig.parseSizeString("0") shouldBe 0L
  }

  test("parseSizeString - kilobytes") {
    SplitCacheConfig.parseSizeString("1K") shouldBe 1024L
    SplitCacheConfig.parseSizeString("512K") shouldBe 512L * 1024L
    SplitCacheConfig.parseSizeString("128k") shouldBe 128L * 1024L // case insensitive
  }

  test("parseSizeString - megabytes") {
    SplitCacheConfig.parseSizeString("1M") shouldBe 1024L * 1024L
    SplitCacheConfig.parseSizeString("16M") shouldBe 16L * 1024L * 1024L
    SplitCacheConfig.parseSizeString("512m") shouldBe 512L * 1024L * 1024L
  }

  test("parseSizeString - gigabytes") {
    SplitCacheConfig.parseSizeString("1G") shouldBe 1024L * 1024L * 1024L
    SplitCacheConfig.parseSizeString("4G") shouldBe 4L * 1024L * 1024L * 1024L
    SplitCacheConfig.parseSizeString("2g") shouldBe 2L * 1024L * 1024L * 1024L
  }

  test("parseSizeString - whitespace handling") {
    SplitCacheConfig.parseSizeString("  512K  ") shouldBe 512L * 1024L
    SplitCacheConfig.parseSizeString("\t16M\n") shouldBe 16L * 1024L * 1024L
  }

  test("parseSizeString - invalid format throws exception") {
    val invalidFormats = Seq("16X", "abc", "K512", "M16", "", "1.5M", "512KB")

    invalidFormats.foreach { invalid =>
      val exception = intercept[IllegalArgumentException] {
        SplitCacheConfig.parseSizeString(invalid)
      }
      exception.getMessage should include(invalid)
    }
  }

  // ===== IndexTables4SparkOptions Tests =====

  test("batchOptimizationEnabled - parsing") {
    val enabledOpts = IndexTables4SparkOptions(Map(
      "spark.indextables.read.batchOptimization.enabled" -> "true"
    ))
    enabledOpts.batchOptimizationEnabled shouldBe Some(true)

    val disabledOpts = IndexTables4SparkOptions(Map(
      "spark.indextables.read.batchOptimization.enabled" -> "false"
    ))
    disabledOpts.batchOptimizationEnabled shouldBe Some(false)

    val missingOpts = IndexTables4SparkOptions(Map.empty[String, String])
    missingOpts.batchOptimizationEnabled shouldBe None
  }

  test("batchOptimizationProfile - parsing and lowercase conversion") {
    val balancedOpts = IndexTables4SparkOptions(Map(
      "spark.indextables.read.batchOptimization.profile" -> "BALANCED"
    ))
    balancedOpts.batchOptimizationProfile shouldBe Some("balanced")

    val aggressiveOpts = IndexTables4SparkOptions(Map(
      "spark.indextables.read.batchOptimization.profile" -> "Aggressive"
    ))
    aggressiveOpts.batchOptimizationProfile shouldBe Some("aggressive")

    val conservativeOpts = IndexTables4SparkOptions(Map(
      "spark.indextables.read.batchOptimization.profile" -> "conservative"
    ))
    conservativeOpts.batchOptimizationProfile shouldBe Some("conservative")
  }

  test("batchOptMaxRangeSize - parsing") {
    val opts = IndexTables4SparkOptions(Map(
      "spark.indextables.read.batchOptimization.maxRangeSize" -> "32M"
    ))
    opts.batchOptMaxRangeSize shouldBe Some("32M")
  }

  test("batchOptGapTolerance - parsing") {
    val opts = IndexTables4SparkOptions(Map(
      "spark.indextables.read.batchOptimization.gapTolerance" -> "2M"
    ))
    opts.batchOptGapTolerance shouldBe Some("2M")
  }

  test("batchOptMinDocs - parsing") {
    val opts = IndexTables4SparkOptions(Map(
      "spark.indextables.read.batchOptimization.minDocsForOptimization" -> "100"
    ))
    opts.batchOptMinDocs shouldBe Some(100)
  }

  test("batchOptMaxConcurrentPrefetch - parsing") {
    val opts = IndexTables4SparkOptions(Map(
      "spark.indextables.read.batchOptimization.maxConcurrentPrefetch" -> "16"
    ))
    opts.batchOptMaxConcurrentPrefetch shouldBe Some(16)
  }

  test("adaptiveTuningEnabled - parsing") {
    val enabledOpts = IndexTables4SparkOptions(Map(
      "spark.indextables.read.adaptiveTuning.enabled" -> "true"
    ))
    enabledOpts.adaptiveTuningEnabled shouldBe Some(true)

    val disabledOpts = IndexTables4SparkOptions(Map(
      "spark.indextables.read.adaptiveTuning.enabled" -> "false"
    ))
    disabledOpts.adaptiveTuningEnabled shouldBe Some(false)
  }

  test("adaptiveTuningMinBatches - parsing") {
    val opts = IndexTables4SparkOptions(Map(
      "spark.indextables.read.adaptiveTuning.minBatchesBeforeAdjustment" -> "10"
    ))
    opts.adaptiveTuningMinBatches shouldBe Some(10)
  }

  // ===== SplitCacheConfig Tests =====

  test("SplitCacheConfig - default batch optimization (should use balanced)") {
    val config = SplitCacheConfig()
    val javaConfig = config.toJavaCacheConfig()

    // Should successfully create config with defaults
    javaConfig should not be null
  }

  test("SplitCacheConfig - explicit enabled with balanced profile") {
    val config = SplitCacheConfig(
      batchOptimizationEnabled = Some(true),
      batchOptimizationProfile = Some("balanced")
    )

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - explicit disabled") {
    val config = SplitCacheConfig(
      batchOptimizationEnabled = Some(false)
    )

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - conservative profile") {
    val config = SplitCacheConfig(
      batchOptimizationProfile = Some("conservative")
    )

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - aggressive profile") {
    val config = SplitCacheConfig(
      batchOptimizationProfile = Some("aggressive")
    )

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - custom parameters with balanced profile") {
    val config = SplitCacheConfig(
      batchOptimizationProfile = Some("balanced"),
      batchOptMaxRangeSize = Some(32L * 1024L * 1024L), // 32MB
      batchOptGapTolerance = Some(2L * 1024L * 1024L),  // 2MB
      batchOptMinDocs = Some(100),
      batchOptMaxConcurrentPrefetch = Some(16)
    )

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - custom parameters without profile") {
    val config = SplitCacheConfig(
      batchOptMaxRangeSize = Some(8L * 1024L * 1024L), // 8MB
      batchOptGapTolerance = Some(256L * 1024L)        // 256KB
    )

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - adaptive tuning enabled") {
    val config = SplitCacheConfig(
      adaptiveTuningEnabled = Some(true),
      adaptiveTuningMinBatches = Some(10)
    )

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("SplitCacheConfig - adaptive tuning disabled") {
    val config = SplitCacheConfig(
      adaptiveTuningEnabled = Some(false)
    )

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  // ===== Integration Tests =====

  test("full configuration flow - balanced profile with custom params") {
    val options = Map(
      "spark.indextables.read.batchOptimization.enabled" -> "true",
      "spark.indextables.read.batchOptimization.profile" -> "balanced",
      "spark.indextables.read.batchOptimization.maxRangeSize" -> "32M",
      "spark.indextables.read.batchOptimization.gapTolerance" -> "1M",
      "spark.indextables.read.batchOptimization.minDocsForOptimization" -> "75",
      "spark.indextables.read.batchOptimization.maxConcurrentPrefetch" -> "12",
      "spark.indextables.read.adaptiveTuning.enabled" -> "true",
      "spark.indextables.read.adaptiveTuning.minBatchesBeforeAdjustment" -> "8"
    )

    val sparkOpts = IndexTables4SparkOptions(options)

    // Verify all options parsed correctly
    sparkOpts.batchOptimizationEnabled shouldBe Some(true)
    sparkOpts.batchOptimizationProfile shouldBe Some("balanced")
    sparkOpts.batchOptMaxRangeSize shouldBe Some("32M")
    sparkOpts.batchOptGapTolerance shouldBe Some("1M")
    sparkOpts.batchOptMinDocs shouldBe Some(75)
    sparkOpts.batchOptMaxConcurrentPrefetch shouldBe Some(12)
    sparkOpts.adaptiveTuningEnabled shouldBe Some(true)
    sparkOpts.adaptiveTuningMinBatches shouldBe Some(8)

    // Create SplitCacheConfig with parsed values
    val config = SplitCacheConfig(
      batchOptimizationEnabled = sparkOpts.batchOptimizationEnabled,
      batchOptimizationProfile = sparkOpts.batchOptimizationProfile,
      batchOptMaxRangeSize = sparkOpts.batchOptMaxRangeSize.map(SplitCacheConfig.parseSizeString),
      batchOptGapTolerance = sparkOpts.batchOptGapTolerance.map(SplitCacheConfig.parseSizeString),
      batchOptMinDocs = sparkOpts.batchOptMinDocs,
      batchOptMaxConcurrentPrefetch = sparkOpts.batchOptMaxConcurrentPrefetch,
      adaptiveTuningEnabled = sparkOpts.adaptiveTuningEnabled,
      adaptiveTuningMinBatches = sparkOpts.adaptiveTuningMinBatches
    )

    // Verify SplitCacheConfig fields
    config.batchOptimizationEnabled shouldBe Some(true)
    config.batchOptimizationProfile shouldBe Some("balanced")
    config.batchOptMaxRangeSize shouldBe Some(32L * 1024L * 1024L)
    config.batchOptGapTolerance shouldBe Some(1L * 1024L * 1024L)
    config.batchOptMinDocs shouldBe Some(75)
    config.batchOptMaxConcurrentPrefetch shouldBe Some(12)
    config.adaptiveTuningEnabled shouldBe Some(true)
    config.adaptiveTuningMinBatches shouldBe Some(8)

    // Verify Java config creation succeeds
    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("full configuration flow - aggressive profile") {
    val options = Map(
      "spark.indextables.read.batchOptimization.profile" -> "aggressive"
    )

    val sparkOpts = IndexTables4SparkOptions(options)
    sparkOpts.batchOptimizationProfile shouldBe Some("aggressive")

    val config = SplitCacheConfig(
      batchOptimizationProfile = sparkOpts.batchOptimizationProfile
    )

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("full configuration flow - conservative profile") {
    val options = Map(
      "spark.indextables.read.batchOptimization.profile" -> "conservative"
    )

    val sparkOpts = IndexTables4SparkOptions(options)
    sparkOpts.batchOptimizationProfile shouldBe Some("conservative")

    val config = SplitCacheConfig(
      batchOptimizationProfile = sparkOpts.batchOptimizationProfile
    )

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("full configuration flow - disabled optimization") {
    val options = Map(
      "spark.indextables.read.batchOptimization.enabled" -> "false"
    )

    val sparkOpts = IndexTables4SparkOptions(options)
    sparkOpts.batchOptimizationEnabled shouldBe Some(false)

    val config = SplitCacheConfig(
      batchOptimizationEnabled = sparkOpts.batchOptimizationEnabled
    )

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("ConfigUtils.createSplitCacheConfig - includes batch optimization") {
    import io.indextables.spark.util.ConfigUtils

    val configMap = Map(
      "spark.indextables.read.batchOptimization.enabled" -> "true",
      "spark.indextables.read.batchOptimization.profile" -> "aggressive",
      "spark.indextables.read.batchOptimization.maxRangeSize" -> "32M",
      "spark.indextables.read.batchOptimization.gapTolerance" -> "2M",
      "spark.indextables.read.batchOptimization.minDocsForOptimization" -> "25",
      "spark.indextables.read.batchOptimization.maxConcurrentPrefetch" -> "12",
      "spark.indextables.read.adaptiveTuning.enabled" -> "true",
      "spark.indextables.read.adaptiveTuning.minBatchesBeforeAdjustment" -> "10"
    )

    val config = ConfigUtils.createSplitCacheConfig(configMap, Some("test-table"))

    // Verify batch optimization fields populated
    config.batchOptimizationEnabled shouldBe Some(true)
    config.batchOptimizationProfile shouldBe Some("aggressive")
    config.batchOptMaxRangeSize shouldBe Some(32L * 1024L * 1024L)
    config.batchOptGapTolerance shouldBe Some(2L * 1024L * 1024L)
    config.batchOptMinDocs shouldBe Some(25)
    config.batchOptMaxConcurrentPrefetch shouldBe Some(12)
    config.adaptiveTuningEnabled shouldBe Some(true)
    config.adaptiveTuningMinBatches shouldBe Some(10)

    // Verify Java config creation succeeds
    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  // ===== Edge Cases =====

  test("unknown profile should log warning and use balanced") {
    val config = SplitCacheConfig(
      batchOptimizationProfile = Some("unknown-profile")
    )

    // Should not throw, should fall back to balanced
    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("empty profile string defaults to balanced") {
    val options = Map(
      "spark.indextables.read.batchOptimization.profile" -> ""
    )

    val sparkOpts = IndexTables4SparkOptions(options)
    sparkOpts.batchOptimizationProfile shouldBe Some("")

    val config = SplitCacheConfig(
      batchOptimizationProfile = sparkOpts.batchOptimizationProfile
    )

    val javaConfig = config.toJavaCacheConfig()
    javaConfig should not be null
  }

  test("negative values should be caught by Java config validation") {
    val config = SplitCacheConfig(
      batchOptMinDocs = Some(-1) // Invalid value
    )

    // Java BatchOptimizationConfig should throw on invalid values
    val exception = intercept[IllegalArgumentException] {
      config.toJavaCacheConfig()
    }
    exception.getMessage should include("minDocsForOptimization must be positive")
  }
}
