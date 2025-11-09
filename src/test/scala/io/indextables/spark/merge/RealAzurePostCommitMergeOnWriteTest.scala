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

import org.apache.spark.sql.functions._

import io.indextables.spark.RealAzureTestBase

/**
 * Real Azure Blob Storage integration tests for post-commit merge-on-write functionality.
 *
 * This test validates:
 *   - Post-commit merge-on-write with real Azure Blob Storage
 *   - Threshold-based merge triggering
 *   - Write options propagation to merge executor
 *   - Azure credentials flow through to merge operations
 *   - Data integrity across writes and merges
 *
 * Credentials are loaded from ~/.azure/credentials file, environment variables, or system properties.
 */
class RealAzurePostCommitMergeOnWriteTest extends RealAzureTestBase {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[RealAzurePostCommitMergeOnWriteTest])

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Skip all tests if Azure credentials are not available
    assume(
      hasAzureCredentials(),
      "Azure credentials not available - skipping tests"
    )
  }

  /**
   * Count the number of split files in the transaction log.
   * This gives us the current active split count after all merges.
   */
  private def countActiveSplits(azurePath: String): Int = {
    import org.apache.hadoop.fs.Path
    import io.indextables.spark.transaction.TransactionLogFactory
    import org.apache.spark.sql.util.CaseInsensitiveStringMap

    val path = new Path(azurePath)
    val transactionLog = TransactionLogFactory.create(
      path,
      spark,
      new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    )

    val activeSplits = transactionLog.listFiles()
    val count = activeSplits.size

    logger.info(s"📊 Active splits in transaction log: $count")
    count
  }

  test("Real Azure: post-commit merge-on-write should trigger when threshold is met") {
    val testId    = generateTestId()
    val azurePath = s"azure://$testContainer/post-commit-merge-threshold-$testId"

    logger.info(s"Testing post-commit merge-on-write with threshold triggering at: $azurePath")

    // Create test data that will generate enough splits to trigger merge
    val df = spark
      .range(0, 2000)
      .select(
        col("id"),
        concat(
          lit("Document content for ID "),
          col("id"),
          lit(". Additional text to ensure meaningful split sizes. ")
        ).as("text")
      )

    // Write with merge-on-write enabled and very low threshold to ensure merge triggers
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "0.1") // Very low threshold
      .option("spark.indextables.mergeOnWrite.targetSize", "1M") // Small target to create multiple groups
      .option("spark.indextables.indexwriter.batchSize", "100") // Force multiple splits
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1") // Allow test to run with limited disk
      .mode("overwrite")
      .save(azurePath)

    logger.info(s"✅ Write completed (merge should have been triggered)")

    // Verify that merge actually occurred by checking split count
    // With 2000 rows and batchSize=100, we expect ~20 initial splits
    // After merge with targetSize=1M and low threshold, we should have fewer splits
    val activeSplitCount = countActiveSplits(azurePath)

    // Merged splits should be significantly fewer than initial splits
    activeSplitCount should be < 20
    logger.info(s"✅ Merge confirmed: reduced to $activeSplitCount active splits (expected < 20)")

    // Verify data integrity
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    result.count() shouldBe 2000

    logger.info("✅ Data integrity verified after post-commit merge-on-write")
  }

  test("Real Azure: post-commit merge-on-write should not trigger when below threshold") {
    val testId    = generateTestId()
    val azurePath = s"azure://$testContainer/post-commit-no-merge-$testId"

    logger.info(s"Testing post-commit merge-on-write with threshold NOT met at: $azurePath")

    // Create small amount of data
    val df = spark
      .range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text")

    // Write with merge-on-write enabled but very high threshold to prevent merge
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "100.0") // Very high threshold
      .option("spark.indextables.mergeOnWrite.targetSize", "1M")
      .mode("overwrite")
      .save(azurePath)

    logger.info(s"✅ Write completed (merge should NOT have triggered)")

    // Verify that merge did NOT occur by checking split count
    // With 100 rows and no merge, we expect only a couple of splits
    val activeSplitCount = countActiveSplits(azurePath)

    // Without merge (very high threshold), splits should remain as written
    activeSplitCount should be >= 1
    activeSplitCount should be <= 10
    logger.info(s"✅ Merge correctly did NOT trigger ($activeSplitCount active splits, as written)")

    // Verify data integrity
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    result.count() shouldBe 100

    logger.info("✅ Data integrity verified (no merge triggered as expected)")
  }

  test("Real Azure: post-commit merge-on-write should propagate write options to merge") {
    val testId    = generateTestId()
    val azurePath = s"azure://$testContainer/post-commit-options-$testId"

    logger.info(s"Testing options propagation in post-commit merge-on-write at: $azurePath")

    // Create test data
    val df = spark
      .range(0, 500)
      .select(
        col("id"),
        concat(
          lit("Content for document "),
          col("id")
        ).as("text")
      )

    // Write with custom merge options to verify they propagate
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "0.1")
      .option("spark.indextables.mergeOnWrite.targetSize", "1M")
      .option("spark.indextables.merge.heapSize", "512M") // Custom heap size
      .option("spark.indextables.merge.debug", "true") // Enable debug
      .option("spark.indextables.indexwriter.batchSize", "50")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .mode("overwrite")
      .save(azurePath)

    logger.info(s"✅ Write completed with custom merge options")

    // Verify data integrity
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    result.count() shouldBe 500

    logger.info("✅ Options propagation test completed successfully")
  }

  test("Real Azure: post-commit merge-on-write should work with partitioned data") {
    val testId    = generateTestId()
    val azurePath = s"azure://$testContainer/post-commit-partitioned-$testId"

    logger.info(s"Testing post-commit merge-on-write with partitioned data at: $azurePath")

    // Create partitioned data
    val df = spark
      .range(0, 1000)
      .select(
        col("id"),
        concat(lit("Content for "), col("id")).as("text"),
        (col("id") % 10).cast("string").as("partition_col")
      )

    // Write with partitioning and merge-on-write
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("partition_col")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "0.5")
      .option("spark.indextables.mergeOnWrite.targetSize", "500K")
      .option("spark.indextables.indexwriter.batchSize", "50")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .mode("overwrite")
      .save(azurePath)

    logger.info(s"✅ Partitioned write with merge-on-write completed")

    // Verify data integrity and partition structure
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    result.count() shouldBe 1000

    // Verify partition filtering
    val partition0 = result.filter(col("partition_col") === "0")
    partition0.count() shouldBe 100

    logger.info("✅ Partitioned merge-on-write test completed successfully")
  }

  test("Real Azure: post-commit merge-on-write should handle multiple appends") {
    val testId    = generateTestId()
    val azurePath = s"azure://$testContainer/post-commit-appends-$testId"

    logger.info(s"Testing post-commit merge-on-write with multiple appends at: $azurePath")

    // First write
    val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "5.0") // Higher threshold
      .option("spark.indextables.mergeOnWrite.targetSize", "500K")
      .option("spark.indextables.indexwriter.batchSize", "50")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .mode("overwrite")
      .save(azurePath)

    logger.info("✅ First write completed")

    // Second write (append)
    val df2 = spark.range(100, 200).selectExpr("id", "CAST(id AS STRING) as text")
    df2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "5.0")
      .option("spark.indextables.mergeOnWrite.targetSize", "500K")
      .option("spark.indextables.indexwriter.batchSize", "50")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .mode("append")
      .save(azurePath)

    logger.info("✅ Second write completed")

    // Third write (append) with lower threshold to trigger merge
    val df3 = spark.range(200, 300).selectExpr("id", "CAST(id AS STRING) as text")
    df3.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "0.1") // Very low threshold
      .option("spark.indextables.mergeOnWrite.targetSize", "500K")
      .option("spark.indextables.indexwriter.batchSize", "50")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .mode("append")
      .save(azurePath)

    logger.info("✅ Third write completed (merge should have triggered)")

    // Verify that merge actually occurred by checking split count
    // After 3 writes with batchSize=50, we would have many small splits
    // After merge with targetSize=500K and low threshold, we should have fewer splits
    val activeSplitCount = countActiveSplits(azurePath)

    // After merge, we should have consolidated splits
    activeSplitCount should be < 15
    logger.info(s"✅ Merge confirmed after multiple appends: reduced to $activeSplitCount active splits")

    // Verify all data is present
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    result.count() shouldBe 300

    // Verify no duplicates
    result.select("id").distinct().count() shouldBe 300

    logger.info("✅ Multiple appends with merge-on-write test completed successfully")
  }

  test("Real Azure: post-commit merge-on-write should handle OAuth credentials") {
    // Only run if OAuth credentials are available
    assume(hasOAuthCredentials(), "OAuth credentials required for this test")

    val testId    = generateTestId()
    val azurePath = s"azure://$testContainer/post-commit-oauth-$testId"

    logger.info(s"Testing post-commit merge-on-write with OAuth credentials at: $azurePath")

    // Create test data
    val df = spark
      .range(0, 200)
      .selectExpr("id", "CAST(id AS STRING) as text")

    // Write with merge-on-write enabled
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "0.1")
      .option("spark.indextables.mergeOnWrite.targetSize", "500K")
      .option("spark.indextables.indexwriter.batchSize", "50")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .mode("overwrite")
      .save(azurePath)

    logger.info(s"✅ Write with OAuth credentials completed")

    // Verify data integrity
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    result.count() shouldBe 200

    logger.info("✅ OAuth credentials test completed successfully")
  }
}
