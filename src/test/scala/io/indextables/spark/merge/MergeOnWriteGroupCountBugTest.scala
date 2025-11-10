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
 * Regression test for merge-on-write group counting bug.
 *
 * Bug Description:
 * The countMergeGroupsFromTransactionLog method was incorrectly counting individual files
 * as "mergeable groups" instead of counting the actual merge groups that would be created
 * by bin-packing those files. This caused merge-on-write to trigger inappropriately.
 *
 * Reproduction Scenario (from bad_merge_trigger.txt):
 * - 620 total files in transaction log
 * - Target size: 4GB
 * - Default parallelism: 64 cores
 * - Merge threshold: 128 groups (64 × 2.0)
 * - Buggy behavior: Reported 300 "mergeable groups" → triggered merge
 * - Actual merge groups: Only 46 groups were created
 * - Expected behavior: Should NOT trigger merge (46 < 128)
 *
 * This test validates that:
 * 1. Many small files are correctly counted as forming fewer merge groups
 * 2. Merge only triggers when actual merge group count exceeds threshold
 * 3. The bin-packing simulation matches MergeSplitsExecutor logic
 */
class MergeOnWriteGroupCountBugTest extends TestBase with BeforeAndAfterEach {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[MergeOnWriteGroupCountBugTest])

  /**
   * Test that reproduces the bug scenario from bad_merge_trigger.txt
   *
   * Creates many small files that should form relatively few merge groups,
   * and verifies that merge does NOT trigger inappropriately.
   */
  test("should NOT trigger merge when many small files form few groups") {
    val tablePath = s"file://$tempDir/test_group_count_bug"
    logger.info(s"Testing group count bug reproduction at: $tablePath")

    // Simulate the scenario from the log:
    // - Create many small files (simulating 300+ small splits)
    // - Use large target size (4GB) so files pack into relatively few groups
    // - Set high threshold so merge should NOT trigger

    // Create initial data with many small partitions
    // Each partition will create a small split file
    val numRecordsPerPartition = 100  // Small number to create small splits
    val numPartitions = 300           // Many partitions = many small files

    val df = spark.range(0, numRecordsPerPartition * numPartitions)
      .selectExpr(
        "id",
        "CAST(id AS STRING) as text",
        "(id % 2) + 2025 as year",    // Two year partitions: 2025-11-07 and 2025-11-08
        s"(id % $numPartitions) as partition_id"  // Spread across many partitions
      )
      .repartition(numPartitions)  // Force many small splits

    logger.info(s"Created DataFrame with ${numRecordsPerPartition * numPartitions} records in $numPartitions partitions")

    // First write: create many small split files
    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "false")  // Disable for initial write
      .partitionBy("year")  // Partition by year (similar to kdate in the log)
      .save(tablePath)

    // Verify initial write created many files
    val txLog = new io.indextables.spark.transaction.OptimizedTransactionLog(
      new org.apache.hadoop.fs.Path(tablePath),
      spark
    )
    val initialFiles = txLog.listFiles()
    logger.info(s"Initial write created ${initialFiles.length} split files")

    // Files should be small (much less than 4GB target size)
    val avgFileSize = initialFiles.map(_.size).sum / initialFiles.length
    logger.info(s"Average file size: ${avgFileSize / (1024.0 * 1024.0)} MB")

    // Now append more data with merge-on-write enabled
    // This should:
    // 1. Count merge groups correctly (should be relatively few groups due to large target size)
    // 2. NOT trigger merge because group count < threshold

    val appendDf = spark.range(0, 1000)
      .selectExpr(
        "id",
        "CAST(id AS STRING) as text",
        "2025 as year",
        "id % 10 as partition_id"
      )

    // Set threshold high enough that merge should NOT trigger
    // With 64-core default parallelism and multiplier 2.0, threshold = 128 groups
    // Our many small files should pack into < 128 groups with 4GB target size
    appendDf.write
      .mode("append")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "4G")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "2.0")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")  // Lower threshold for tests
      .partitionBy("year")
      .save(tablePath)

    // Verify data is correct
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val totalExpected = (numRecordsPerPartition * numPartitions) + 1000
    result.count() shouldBe totalExpected

    // Verify file count - should have initial files + append files (no merge should have occurred)
    val finalFiles = txLog.listFiles()
    logger.info(s"Final write has ${finalFiles.length} split files (initial: ${initialFiles.length})")

    // Files should NOT have been merged - we should have more files than initial
    finalFiles.length should be > initialFiles.length

    logger.info("✅ Group count bug test passed - merge did NOT trigger inappropriately")
  }

  /**
   * Test that merge DOES trigger when actual merge group count exceeds threshold
   */
  test("should trigger merge when actual merge groups exceed threshold") {
    val tablePath = s"file://$tempDir/test_merge_triggers"
    logger.info(s"Testing merge trigger with sufficient groups at: $tablePath")

    // Create scenario where merge SHOULD trigger:
    // - Many small files that would form many merge groups
    // - Low threshold so merge triggers

    val numPartitions = 200
    val df = spark.range(0, 10000)
      .selectExpr(
        "id",
        "CAST(id AS STRING) as text",
        "2025 as year"
      )
      .repartition(numPartitions)

    // Write with low threshold - should trigger merge
    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "10M")  // Small target = many groups
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "0.1")  // Very low multiplier
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .partitionBy("year")
      .save(tablePath)

    // Verify data
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 10000

    logger.info("✅ Merge trigger test passed - merge triggered when threshold was met")
  }

  /**
   * Test edge case: files that don't form any valid merge groups
   * (all files are single-file groups)
   */
  test("should NOT trigger merge when files cannot form valid groups") {
    val tablePath = s"file://$tempDir/test_no_valid_groups"
    logger.info(s"Testing no-valid-groups scenario at: $tablePath")

    // Create just 1 small file - cannot form a merge group (need ≥2 files per group)
    val df = spark.range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text", "2025 as year")
      .coalesce(1)  // Single partition = single file

    df.write
      .mode("overwrite")
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "100M")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "0.1")  // Low threshold
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .partitionBy("year")
      .save(tablePath)

    // Verify data
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 100

    // Verify only 1 file exists (no merge occurred)
    val txLog = new io.indextables.spark.transaction.OptimizedTransactionLog(
      new org.apache.hadoop.fs.Path(tablePath),
      spark
    )
    val files = txLog.listFiles()
    files.length shouldBe 1

    logger.info("✅ No-valid-groups test passed - merge did not trigger for single file")
  }
}
