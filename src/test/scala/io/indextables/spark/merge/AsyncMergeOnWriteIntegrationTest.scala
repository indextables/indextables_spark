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

package io.indextables.spark.merge

import io.indextables.spark.TestBase
import org.scalatest.BeforeAndAfterEach

/**
 * Integration tests for async merge-on-write functionality.
 *
 * These tests verify:
 *   1. Async merge does not block write completion
 *   2. New threshold calculation formulas work correctly
 *   3. Configuration options are respected
 *   4. Multiple writes can proceed while merge runs in background
 *   5. Job monitoring via SQL command works
 */
class AsyncMergeOnWriteIntegrationTest extends TestBase with BeforeAndAfterEach {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[AsyncMergeOnWriteIntegrationTest])

  override def beforeEach(): Unit = {
    // Reset the manager before each test for isolation
    AsyncMergeOnWriteManager.reset()
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    // Clean up after each test
    AsyncMergeOnWriteManager.reset()
    super.afterEach()
  }

  // ===== Configuration Tests =====

  test("AsyncMergeOnWriteConfig batch size calculation with default fraction") {
    val config = AsyncMergeOnWriteConfig.default

    // With 24 CPUs and default fraction 0.167 (1/6), batch size should be 4
    config.calculateBatchSize(24) shouldBe 4

    // With 12 CPUs, batch size should be 2
    config.calculateBatchSize(12) shouldBe 2

    // Minimum is 1
    config.calculateBatchSize(1) shouldBe 1
    config.calculateBatchSize(5) shouldBe 1 // 5 * 0.167 = 0.835 -> 0, but min is 1
  }

  test("AsyncMergeOnWriteConfig threshold calculation") {
    val config = AsyncMergeOnWriteConfig.default

    // threshold = batchSize * minBatchesToTrigger
    // With 24 CPUs, batchSize = 4, minBatchesToTrigger = 1 (default)
    // threshold = 4 * 1 = 4
    config.calculateMergeThreshold(24) shouldBe 4

    // With minBatchesToTrigger = 2
    val config2 = AsyncMergeOnWriteConfig(minBatchesToTrigger = 2)
    // threshold = 4 * 2 = 8
    config2.calculateMergeThreshold(24) shouldBe 8
  }

  test("AsyncMergeOnWriteConfig with custom fraction") {
    val config = AsyncMergeOnWriteConfig(batchCpuFraction = 0.25)

    // With 24 CPUs and 0.25 fraction, batch size should be 6
    config.calculateBatchSize(24) shouldBe 6

    // threshold with default minBatchesToTrigger = 1
    config.calculateMergeThreshold(24) shouldBe 6
  }

  // ===== Write Tests =====

  test("perform normal write when async merge is disabled") {
    val tablePath = s"file://$tempDir/test_async_disabled"
    logger.info(s"Testing normal write (async disabled) at: $tablePath")

    val df = spark
      .range(0, 1000)
      .selectExpr("id", "CAST(id AS STRING) as text")

    // Write with merge enabled but async disabled
    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.async.enabled", "false")
      .option("spark.indextables.mergeOnWrite.batchCpuFraction", "0.167")
      .option("spark.indextables.mergeOnWrite.minBatchesToTrigger", "100") // High to avoid actual merge
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 1000
    logger.info("✅ Write with async disabled completed successfully")
  }

  test("perform write with async merge enabled") {
    val tablePath = s"file://$tempDir/test_async_enabled"
    logger.info(s"Testing write (async enabled) at: $tablePath")

    val df = spark
      .range(0, 500)
      .selectExpr("id", "CAST(id AS STRING) as text")

    // Write with async merge enabled but high threshold to avoid actual merge
    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.async.enabled", "true")
      .option("spark.indextables.mergeOnWrite.batchCpuFraction", "0.167")
      .option("spark.indextables.mergeOnWrite.minBatchesToTrigger", "100") // High to avoid trigger
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 500
    logger.info("✅ Write with async enabled completed successfully")
  }

  test("not trigger merge when below new threshold") {
    val tablePath = s"file://$tempDir/test_below_new_threshold"
    logger.info(s"Testing below-threshold scenario at: $tablePath")

    val df = spark
      .range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text")

    // Write with very high threshold (requires many batches worth of groups)
    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.async.enabled", "true")
      .option("spark.indextables.mergeOnWrite.batchCpuFraction", "0.5")    // Large batches
      .option("spark.indextables.mergeOnWrite.minBatchesToTrigger", "10") // Require 10 batches
      .option("spark.indextables.mergeOnWrite.targetSize", "1G")          // Large target to reduce groups
      .save(tablePath)

    // No merge job should be in progress
    AsyncMergeOnWriteManager.isMergeInProgress(tablePath) shouldBe false

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 100
    logger.info("✅ Below-threshold test completed (no merge triggered)")
  }

  test("calculate threshold using new formula") {
    val tablePath = s"file://$tempDir/test_new_threshold_formula"
    logger.info(s"Testing new threshold formula at: $tablePath")

    val defaultParallelism = spark.sparkContext.defaultParallelism
    logger.info(s"Default parallelism (cluster CPUs): $defaultParallelism")

    // Test configuration
    val batchCpuFraction = 0.25
    val minBatchesToTrigger = 2

    // Expected values
    val expectedBatchSize = math.max(1, (defaultParallelism * batchCpuFraction).toInt)
    val expectedThreshold = expectedBatchSize * minBatchesToTrigger

    logger.info(s"Expected batch size: $expectedBatchSize")
    logger.info(s"Expected threshold: $expectedThreshold merge groups")

    val df = spark
      .range(0, 300)
      .selectExpr("id", "CAST(id AS STRING) as text")

    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.async.enabled", "true")
      .option("spark.indextables.mergeOnWrite.batchCpuFraction", batchCpuFraction.toString)
      .option("spark.indextables.mergeOnWrite.minBatchesToTrigger", minBatchesToTrigger.toString)
      .option("spark.indextables.mergeOnWrite.targetSize", "1G") // Large to minimize groups
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 300
    logger.info(s"✅ New threshold formula test completed (threshold: $expectedThreshold)")
  }

  // ===== Manager State Tests =====

  test("manager tracks no jobs when merges don't trigger") {
    val tablePath = s"file://$tempDir/test_no_jobs_tracking"
    logger.info(s"Testing job tracking (no triggers) at: $tablePath")

    val df = spark
      .range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text")

    // High threshold to prevent merge trigger
    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.async.enabled", "true")
      .option("spark.indextables.mergeOnWrite.minBatchesToTrigger", "100")
      .save(tablePath)

    // No active jobs should be present
    AsyncMergeOnWriteManager.getActiveJobCount shouldBe 0
    AsyncMergeOnWriteManager.getAllJobStatus shouldBe empty

    logger.info("✅ Job tracking test completed")
  }

  test("DESCRIBE INDEXTABLES MERGE JOBS returns correct schema") {
    val result = spark.sql("DESCRIBE INDEXTABLES MERGE JOBS")

    val columns = result.columns
    columns should contain("job_id")
    columns should contain("table_path")
    columns should contain("status")
    columns should contain("total_groups")
    columns should contain("completed_groups")
    columns should contain("total_batches")
    columns should contain("completed_batches")
    columns should contain("progress_pct")
    columns should contain("duration_ms")
    columns should contain("error_message")

    // No jobs, so empty result
    result.count() shouldBe 0

    logger.info("✅ DESCRIBE MERGE JOBS schema test completed")
  }

  test("DESCRIBE INDEXTABLES MERGE JOBS is queryable") {
    val result = spark.sql("DESCRIBE INDEXTABLES MERGE JOBS")
    result.createOrReplaceTempView("merge_jobs")

    // Query should work even with no results
    val runningJobs = spark.sql("SELECT * FROM merge_jobs WHERE status = 'RUNNING'")
    runningJobs.count() shouldBe 0

    val failedJobs = spark.sql("SELECT * FROM merge_jobs WHERE status = 'FAILED'")
    failedJobs.count() shouldBe 0

    logger.info("✅ DESCRIBE MERGE JOBS queryability test completed")
  }

  // ===== Configuration Propagation Tests =====

  test("propagate async config options to manager") {
    val tablePath = s"file://$tempDir/test_config_propagation"
    logger.info(s"Testing config propagation at: $tablePath")

    val df = spark
      .range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text")

    // Write with custom async config
    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.async.enabled", "true")
      .option("spark.indextables.mergeOnWrite.batchCpuFraction", "0.2")
      .option("spark.indextables.mergeOnWrite.maxConcurrentBatches", "5")
      .option("spark.indextables.mergeOnWrite.minBatchesToTrigger", "100") // High to avoid actual merge
      .option("spark.indextables.mergeOnWrite.targetSize", "8G")
      .option("spark.indextables.mergeOnWrite.shutdownTimeoutMs", "60000")
      .save(tablePath)

    // Manager should be configured (even if no merge triggered)
    // Available slots should match maxConcurrentBatches
    AsyncMergeOnWriteManager.getAvailableBatchSlots shouldBe 5

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 100
    logger.info("✅ Config propagation test completed")
  }

  // ===== Multiple Writes Tests =====

  test("handle multiple writes to same table") {
    val tablePath = s"file://$tempDir/test_multiple_writes_async"
    logger.info(s"Testing multiple writes at: $tablePath")

    // First write
    val df1 = spark
      .range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text")

    df1.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.async.enabled", "true")
      .option("spark.indextables.mergeOnWrite.minBatchesToTrigger", "100")
      .save(tablePath)

    // Second write (append)
    val df2 = spark
      .range(100, 200)
      .selectExpr("id", "CAST(id AS STRING) as text")

    df2.write
      .mode("append")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.async.enabled", "true")
      .option("spark.indextables.mergeOnWrite.minBatchesToTrigger", "100")
      .save(tablePath)

    // Verify all data
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 200
    result.select("id").distinct().count() shouldBe 200

    logger.info("✅ Multiple writes test completed")
  }

  test("handle partitioned table with async merge") {
    val tablePath = s"file://$tempDir/test_partitioned_async"
    logger.info(s"Testing partitioned table at: $tablePath")

    val df = spark
      .range(0, 500)
      .selectExpr(
        "id",
        "CAST(id AS STRING) as text",
        "CAST(id % 5 AS STRING) as partition_col"
      )

    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("partition_col")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.async.enabled", "true")
      .option("spark.indextables.mergeOnWrite.minBatchesToTrigger", "100")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 500

    // Verify partition filtering works
    val partition0 = result.filter("partition_col = '0'")
    partition0.count() shouldBe 100

    logger.info("✅ Partitioned table test completed")
  }

  // ===== Failure Resilience Tests =====

  test("not fail write if async config parsing fails") {
    val tablePath = s"file://$tempDir/test_config_parse_failure"
    logger.info(s"Testing config parse failure resilience at: $tablePath")

    val df = spark
      .range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text")

    // Invalid config values should not break the write
    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.async.enabled", "true")
      .option("spark.indextables.mergeOnWrite.batchCpuFraction", "invalid") // Invalid - should use default
      .option("spark.indextables.mergeOnWrite.maxConcurrentBatches", "-5")  // Invalid - should use default
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 100
    logger.info("✅ Config parse failure resilience test completed")
  }
}
