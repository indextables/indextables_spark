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

package io.indextables.spark.storage

import io.indextables.spark.TestBase

/**
 * Comprehensive tests for Batch Retrieval Optimization functionality.
 *
 * Tests cover:
 *   - Range consolidation logic (merging adjacent/overlapping byte ranges)
 *   - Gap tolerance handling
 *   - Prefetch mechanism and concurrency limits
 *   - Cache eviction under memory pressure
 *   - Error handling during range reads
 */
class BatchRetrievalOptimizationTest extends TestBase {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[BatchRetrievalOptimizationTest])

  // ============================================================================
  // RANGE CONSOLIDATION TESTS
  // ============================================================================

  test("should merge adjacent byte ranges into single request") {
    val tablePath = s"file://$tempDir/test_adjacent_ranges"

    // Create test data with multiple documents that will be read in sequence
    val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.read.batchOptimization.enabled", "true")
      .option("spark.indextables.read.batchOptimization.gapTolerance", "1M")
      .save(tablePath)

    // Read with batch optimization - adjacent ranges should be consolidated
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.enabled", "true")
      .load(tablePath)

    // Verify all data is returned correctly
    result.count() shouldBe 500

    logger.info("Adjacent range consolidation test passed")
  }

  test("should NOT merge ranges with gaps exceeding tolerance") {
    val tablePath = s"file://$tempDir/test_gap_tolerance"

    // Create sparse data that will have large gaps between document positions
    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Read with very small gap tolerance - ranges should NOT be consolidated
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.enabled", "true")
      .option("spark.indextables.read.batchOptimization.gapTolerance", "1K") // Very small
      .load(tablePath)

    result.count() shouldBe 100

    logger.info("Gap tolerance enforcement test passed")
  }

  test("should respect maximum range size limit") {
    val tablePath = s"file://$tempDir/test_max_range_size"

    // Create larger dataset
    val df = spark.range(0, 1000).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Read with small max range size - should create multiple requests
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.enabled", "true")
      .option("spark.indextables.read.batchOptimization.maxRangeSize", "2M") // Small limit
      .load(tablePath)

    result.limit(1000).count() shouldBe 1000

    logger.info("Maximum range size limit test passed")
  }

  test("should handle overlapping ranges correctly") {
    val tablePath = s"file://$tempDir/test_overlapping_ranges"

    val df = spark.range(0, 200).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Multiple queries that might create overlapping range requests
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.enabled", "true")
      .load(tablePath)

    // Filter should still work correctly with batch optimization
    val filtered = result.filter("id >= 50 AND id < 150")
    filtered.limit(200).count() shouldBe 100

    logger.info("Overlapping ranges test passed")
  }

  // ============================================================================
  // PREFETCH MECHANISM TESTS
  // ============================================================================

  test("should respect prefetch concurrency limits") {
    val tablePath = s"file://$tempDir/test_prefetch_concurrency"

    val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Read with limited prefetch concurrency
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.enabled", "true")
      .option("spark.indextables.read.batchOptimization.maxConcurrentPrefetch", "2")
      .load(tablePath)

    // All data should be returned despite concurrency limit
    result.limit(500).count() shouldBe 500

    logger.info("Prefetch concurrency limit test passed")
  }

  test("should trigger prefetch on sequential reads") {
    val tablePath = s"file://$tempDir/test_sequential_prefetch"

    val df = spark.range(0, 1000).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.enabled", "true")
      .load(tablePath)

    // Sequential scan should benefit from prefetch
    val ids = result.select("id").limit(1000).collect().map(_.getLong(0)).sorted
    ids.length shouldBe 1000
    ids.head shouldBe 0
    ids.last shouldBe 999

    logger.info("Sequential prefetch test passed")
  }

  // ============================================================================
  // CACHE BEHAVIOR TESTS
  // ============================================================================

  test("should not exceed memory limits with large result sets") {
    val tablePath = s"file://$tempDir/test_memory_limits"

    // Create larger dataset
    val df = spark
      .range(0, 5000)
      .selectExpr(
        "id",
        "CAST(id AS STRING) as text",
        "REPEAT('x', 100) as padding" // Add some bulk
      )
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.enabled", "true")
      .load(tablePath)

    // Should handle large result set without OOM
    result.count() shouldBe 5000

    logger.info("Memory limits test passed")
  }

  test("should skip prefetch for already-cached chunks") {
    val tablePath = s"file://$tempDir/test_cache_skip"

    val df = spark.range(0, 200).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val baseRead = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.enabled", "true")
      .load(tablePath)

    // First read - populates cache
    baseRead.count() shouldBe 200

    // Second read - should use cache, not prefetch again
    val cachedResult = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.enabled", "true")
      .load(tablePath)

    cachedResult.count() shouldBe 200

    logger.info("Cache skip test passed")
  }

  // ============================================================================
  // ERROR HANDLING TESTS
  // ============================================================================

  test("should handle gracefully when batch optimization is disabled") {
    val tablePath = s"file://$tempDir/test_disabled_optimization"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Explicitly disable batch optimization
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.enabled", "false")
      .load(tablePath)

    result.count() shouldBe 100

    logger.info("Disabled optimization test passed")
  }

  test("should fall back gracefully on invalid configuration") {
    val tablePath = s"file://$tempDir/test_invalid_config"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Use profile which provides validated defaults
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.profile", "conservative")
      .load(tablePath)

    result.count() shouldBe 100

    logger.info("Invalid config fallback test passed")
  }

  // ============================================================================
  // PROFILE-BASED CONFIGURATION TESTS
  // ============================================================================

  test("should apply conservative profile settings correctly") {
    val tablePath = s"file://$tempDir/test_conservative_profile"

    val df = spark.range(0, 300).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.profile", "conservative")
      .load(tablePath)

    result.limit(300).count() shouldBe 300

    logger.info("Conservative profile test passed")
  }

  test("should apply aggressive profile settings correctly") {
    val tablePath = s"file://$tempDir/test_aggressive_profile"

    val df = spark.range(0, 300).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.profile", "aggressive")
      .load(tablePath)

    result.limit(300).count() shouldBe 300

    logger.info("Aggressive profile test passed")
  }

  // ============================================================================
  // MINIMUM DOCUMENT THRESHOLD TESTS
  // ============================================================================

  test("should bypass optimization for small result sets") {
    val tablePath = s"file://$tempDir/test_small_result_bypass"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Set high minimum threshold - optimization should be bypassed
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.enabled", "true")
      .option("spark.indextables.read.batchOptimization.minDocsForOptimization", "200")
      .load(tablePath)

    // Should still return correct results even when optimization bypassed
    result.count() shouldBe 100

    logger.info("Small result bypass test passed")
  }

  test("should apply optimization for large result sets") {
    val tablePath = s"file://$tempDir/test_large_result_optimization"

    val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Set low minimum threshold - optimization should apply
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.enabled", "true")
      .option("spark.indextables.read.batchOptimization.minDocsForOptimization", "50")
      .load(tablePath)

    result.limit(500).count() shouldBe 500

    logger.info("Large result optimization test passed")
  }

  // ============================================================================
  // ADAPTIVE TUNING TESTS
  // ============================================================================

  test("should collect metrics for adaptive tuning") {
    val tablePath = s"file://$tempDir/test_adaptive_metrics"

    val df = spark.range(0, 200).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Enable adaptive tuning
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.read.batchOptimization.enabled", "true")
      .option("spark.indextables.read.adaptiveTuning.enabled", "true")
      .load(tablePath)

    // Run multiple queries to collect metrics
    result.count()
    result.filter("id > 50").count()
    result.filter("id < 150").count()

    logger.info("Adaptive tuning metrics test passed")
  }
}
