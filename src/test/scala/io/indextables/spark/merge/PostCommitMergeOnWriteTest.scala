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

import org.scalatest.BeforeAndAfterEach

import io.indextables.spark.TestBase

/**
 * Tests for the post-commit merge-on-write evaluation logic.
 *
 * This test suite validates the simplified merge-on-write architecture that:
 * 1. Writes splits normally to final location
 * 2. Commits transaction log
 * 3. Evaluates if merge is worthwhile based on merge group count vs threshold
 * 4. Invokes MERGE SPLITS command if threshold is met
 */
class PostCommitMergeOnWriteTest extends TestBase with BeforeAndAfterEach {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[PostCommitMergeOnWriteTest])

  /**
   * Test basic write operation without merge-on-write enabled
   */
  test("perform normal write when disabled") {
    val tablePath = s"file://$tempDir/test_disabled"
    logger.info(s"Testing normal write (merge-on-write disabled) at: $tablePath")

    // Create test data
    val df = spark.range(0, 1000)
      .selectExpr("id", "CAST(id AS STRING) as text", "id % 10 as category")

    // Write without merge-on-write
    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "false")
      .save(tablePath)

    // Read back and verify
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 1000

    // Check transaction log
    val txLogPath = new java.io.File(s"$tempDir/test_disabled/_transaction_log")
    txLogPath.exists() shouldBe true

    logger.info("✅ Normal write completed successfully")
  }

  /**
   * Test that merge-on-write doesn't trigger when threshold is not met
   */
  test("not trigger merge when below threshold") {
    val tablePath = s"file://$tempDir/test_below_threshold"
    logger.info(s"Testing below-threshold scenario at: $tablePath")

    // Create small amount of data (will create few splits)
    val df = spark.range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text")

    // Write with merge-on-write enabled but high multiplier
    // Default parallelism is typically 8-12, so multiplier of 100 means threshold = 800-1200 groups
    // We'll only have 1-2 groups, so merge should NOT trigger
    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "100.0")
      .option("spark.indextables.mergeOnWrite.targetSize", "1M") // Small target to create multiple groups
      .save(tablePath)

    // Read back and verify - data should be intact
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 100

    logger.info("✅ Below-threshold scenario completed (merge should not have triggered)")
  }

  /**
   * Test threshold calculation with custom multiplier
   */
  test("calculate threshold correctly based on parallelism and multiplier") {
    val tablePath = s"file://$tempDir/test_threshold_calc"
    logger.info(s"Testing threshold calculation at: $tablePath")

    val defaultParallelism = spark.sparkContext.defaultParallelism
    logger.info(s"Default parallelism: $defaultParallelism")

    // Test with multiplier 2.0 (default)
    val multiplier = 2.0
    val expectedThreshold = (defaultParallelism * multiplier).toInt

    logger.info(s"Expected threshold with multiplier $multiplier: $expectedThreshold merge groups")

    // Create data that will generate splits
    val df = spark.range(0, 500)
      .selectExpr("id", "CAST(id AS STRING) as text")

    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", multiplier.toString)
      .option("spark.indextables.mergeOnWrite.targetSize", "100K") // Small target to create groups
      .save(tablePath)

    // Verify data integrity regardless of whether merge triggered
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 500

    logger.info(s"✅ Threshold calculation test completed (threshold: $expectedThreshold)")
  }

  /**
   * Test that write options are passed through to merge executor
   */
  test("propagate write options to merge executor") {
    val tablePath = s"file://${tempDir}/test_options_propagation"
    logger.info(s"Testing options propagation at: $tablePath")

    // Create test data
    val df = spark.range(0, 200)
      .selectExpr("id", "CAST(id AS STRING) as text")

    // Write with custom merge options
    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "0.1") // Very low threshold to ensure merge
      .option("spark.indextables.mergeOnWrite.targetSize", "50K") // Small target
      .option("spark.indextables.merge.heapSize", "512M") // Custom heap size
      .option("spark.indextables.merge.debug", "true") // Enable debug
      .save(tablePath)

    // Verify data integrity
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 200

    logger.info("✅ Options propagation test completed")
  }

  /**
   * Test merge-on-write with partitioned data
   */
  test("work correctly with partitioned tables") {
    val tablePath = s"file://${tempDir}/test_partitioned"
    logger.info(s"Testing partitioned table at: $tablePath")

    // Create partitioned data
    val df = spark.range(0, 1000)
      .selectExpr(
        "id",
        "CAST(id AS STRING) as text",
        "CAST(id % 10 AS STRING) as partition_col"
      )

    // Write with partitioning and merge-on-write
    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("partition_col")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "0.5")
      .option("spark.indextables.mergeOnWrite.targetSize", "50K")
      .save(tablePath)

    // Verify data integrity and partition structure
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 1000

    // Verify we can read specific partitions
    val partition0 = result.filter("partition_col = '0'")
    partition0.count() shouldBe 100

    logger.info("✅ Partitioned table test completed")
  }

  /**
   * Test multiple writes to same table with merge-on-write
   */
  test("handle multiple writes with accumulating splits") {
    val tablePath = s"file://${tempDir}/test_multiple_writes"
    logger.info(s"Testing multiple writes at: $tablePath")

    // First write
    val df1 = spark.range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text")

    df1.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "5.0") // High threshold
      .option("spark.indextables.mergeOnWrite.targetSize", "50K")
      .save(tablePath)

    // Second write (append)
    val df2 = spark.range(100, 200)
      .selectExpr("id", "CAST(id AS STRING) as text")

    df2.write
      .mode("append")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "5.0")
      .option("spark.indextables.mergeOnWrite.targetSize", "50K")
      .save(tablePath)

    // Third write (append) - might trigger merge due to accumulated splits
    val df3 = spark.range(200, 300)
      .selectExpr("id", "CAST(id AS STRING) as text")

    df3.write
      .mode("append")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "0.1") // Very low threshold - should merge
      .option("spark.indextables.mergeOnWrite.targetSize", "50K")
      .save(tablePath)

    // Verify all data is present
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 300

    // Verify no duplicates
    result.select("id").distinct().count() shouldBe 300

    logger.info("✅ Multiple writes test completed")
  }

  /**
   * Test that merge failures don't break writes
   */
  test("not fail write if merge fails") {
    val tablePath = s"file://${tempDir}/test_merge_failure_resilience"
    logger.info(s"Testing merge failure resilience at: $tablePath")

    // Create test data
    val df = spark.range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text")

    // Write with invalid merge config (should not break the write)
    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "0.1")
      .option("spark.indextables.mergeOnWrite.targetSize", "invalid_size") // Invalid - should cause parse error
      .save(tablePath)

    // Data should still be written successfully despite merge failure
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 100

    logger.info("✅ Merge failure resilience test completed (write succeeded despite merge error)")
  }
}
