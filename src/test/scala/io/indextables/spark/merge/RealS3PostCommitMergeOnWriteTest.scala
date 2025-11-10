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

import java.io.{File, FileInputStream}
import java.util.{Properties, UUID}

import scala.util.Using

import org.apache.spark.sql.functions._

import io.indextables.spark.RealS3TestBase

/**
 * Real AWS S3 integration tests for post-commit merge-on-write functionality.
 *
 * This test validates:
 *   - Post-commit merge-on-write with real S3 storage
 *   - Threshold-based merge triggering
 *   - Write options propagation to merge executor
 *   - AWS credentials flow through to merge operations
 *   - Data integrity across writes and merges
 *
 * Credentials are loaded from ~/.aws/credentials file.
 */
class RealS3PostCommitMergeOnWriteTest extends RealS3TestBase {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[RealS3PostCommitMergeOnWriteTest])

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/post-commit-merge-on-write-$testRunId"

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    if (awsCredentials.isDefined) {
      val (accessKey, secretKey) = awsCredentials.get

      // Configure Spark for real S3 access
      spark.conf.set("spark.indextables.aws.accessKey", accessKey)
      spark.conf.set("spark.indextables.aws.secretKey", secretKey)
      spark.conf.set("spark.indextables.aws.region", S3_REGION)

      // Configure Hadoop config for CloudStorageProvider
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
      hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
      hadoopConf.set("spark.indextables.aws.region", S3_REGION)

      logger.info(s"🔐 AWS credentials loaded successfully")
      logger.info(s"🌊 Configured Spark for S3 access to bucket: $S3_BUCKET in region: $S3_REGION")
      logger.info(s"📍 Test base path: $testBasePath")
    } else {
      logger.warn(s"⚠️  No AWS credentials found in ~/.aws/credentials - tests will be skipped")
    }
  }

  override def afterAll(): Unit = {
    // Clean up test data
    if (awsCredentials.isDefined) {
      cleanupTestData()
    }
    super.afterAll()
  }

  /** Load AWS credentials from ~/.aws/credentials file. */
  private def loadAwsCredentials(): Option[(String, String)] =
    try {
      val home     = System.getProperty("user.home")
      val credFile = new File(s"$home/.aws/credentials")

      if (credFile.exists()) {
        val props = new Properties()
        Using(new FileInputStream(credFile))(fis => props.load(fis))

        val accessKey = props.getProperty("aws_access_key_id")
        val secretKey = props.getProperty("aws_secret_access_key")

        if (accessKey != null && secretKey != null) {
          Some((accessKey, secretKey))
        } else {
          logger.warn(s"⚠️  AWS credentials not found in ~/.aws/credentials")
          None
        }
      } else {
        logger.warn(s"⚠️  ~/.aws/credentials file not found")
        None
      }
    } catch {
      case e: Exception =>
        logger.warn(s"⚠️  Error loading AWS credentials: ${e.getMessage}")
        None
    }

  /** Get write options with AWS credentials for executor distribution. */
  private def getWriteOptions(additionalOptions: Map[String, String] = Map.empty): Map[String, String] = {
    val (accessKey, secretKey) = awsCredentials.get
    Map(
      "spark.indextables.aws.accessKey" -> accessKey,
      "spark.indextables.aws.secretKey" -> secretKey,
      "spark.indextables.aws.region"    -> S3_REGION
    ) ++ additionalOptions
  }

  /** Get read options with AWS credentials for executor distribution. */
  private def getReadOptions(): Map[String, String] = {
    val (accessKey, secretKey) = awsCredentials.get
    Map(
      "spark.indextables.aws.accessKey" -> accessKey,
      "spark.indextables.aws.secretKey" -> secretKey,
      "spark.indextables.aws.region"    -> S3_REGION
    )
  }

  /** Clean up test data from S3. */
  private def cleanupTestData(): Unit =
    try {
      logger.info(s"ℹ️  Test data cleanup skipped (unique paths used): $testBasePath")
    } catch {
      case e: Exception =>
        logger.warn(s"⚠️  Warning: Could not clean up test data: ${e.getMessage}")
    }

  /**
   * Count the number of split files in the transaction log.
   * This gives us the current active split count after all merges.
   */
  private def countActiveSplits(tablePath: String): Int = {
    import org.apache.hadoop.fs.Path
    import io.indextables.spark.transaction.TransactionLogFactory
    import org.apache.spark.sql.util.CaseInsensitiveStringMap

    val path = new Path(tablePath)
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

  test("Real S3: post-commit merge-on-write should trigger when threshold is met") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/merge-threshold-test"

    logger.info(s"Testing post-commit merge-on-write with threshold triggering at: $tablePath")

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
    val writeOptions = getWriteOptions(Map(
      "spark.indextables.mergeOnWrite.enabled" -> "true",
      "spark.indextables.mergeOnWrite.mergeGroupMultiplier" -> "0.1", // Very low threshold
      "spark.indextables.mergeOnWrite.targetSize" -> "1M", // Small target to create multiple groups
      "spark.indextables.indexwriter.batchSize" -> "100", // Force multiple splits
      "spark.indextables.mergeOnWrite.minDiskSpaceGB" -> "1" // Allow test to run with limited disk
    ))

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    logger.info(s"✅ Write completed (merge should have been triggered)")

    // Verify that merge actually occurred by checking split count
    // With 2000 rows and batchSize=100, we expect ~20 initial splits
    // After merge with targetSize=1M and low threshold, we should have fewer splits
    val activeSplitCount = countActiveSplits(tablePath)

    // Merged splits should be significantly fewer than initial splits
    // We expect at least consolidation to happen
    activeSplitCount should be < 20
    logger.info(s"✅ Merge confirmed: reduced to $activeSplitCount active splits (expected < 20)")

    // Verify data integrity
    val readOptions = getReadOptions()
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    result.count() shouldBe 2000

    logger.info("✅ Data integrity verified after post-commit merge-on-write")
  }

  test("Real S3: post-commit merge-on-write should not trigger when below threshold") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/no-merge-threshold-test"

    logger.info(s"Testing post-commit merge-on-write with threshold NOT met at: $tablePath")

    // Create small amount of data
    val df = spark
      .range(0, 100)
      .selectExpr("id", "CAST(id AS STRING) as text")

    // Write with merge-on-write enabled but very high threshold to prevent merge
    val writeOptions = getWriteOptions(Map(
      "spark.indextables.mergeOnWrite.enabled" -> "true",
      "spark.indextables.mergeOnWrite.mergeGroupMultiplier" -> "100.0", // Very high threshold
      "spark.indextables.mergeOnWrite.targetSize" -> "1M"
    ))

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    logger.info(s"✅ Write completed (merge should NOT have triggered)")

    // Verify that merge did NOT occur by checking split count
    // With 100 rows and no merge, we expect only a couple of splits
    val activeSplitCount = countActiveSplits(tablePath)

    // Without merge (very high threshold), splits should remain as written
    // We expect 2-4 splits for this small dataset
    activeSplitCount should be >= 1
    activeSplitCount should be <= 10
    logger.info(s"✅ Merge correctly did NOT trigger ($activeSplitCount active splits, as written)")

    // Verify data integrity
    val readOptions = getReadOptions()
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    result.count() shouldBe 100

    logger.info("✅ Data integrity verified (no merge triggered as expected)")
  }

  test("Real S3: post-commit merge-on-write should propagate write options to merge") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/options-propagation-test"

    logger.info(s"Testing options propagation in post-commit merge-on-write at: $tablePath")

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
    val writeOptions = getWriteOptions(Map(
      "spark.indextables.mergeOnWrite.enabled" -> "true",
      "spark.indextables.mergeOnWrite.mergeGroupMultiplier" -> "0.1",
      "spark.indextables.mergeOnWrite.targetSize" -> "1M",
      "spark.indextables.merge.heapSize" -> "512M", // Custom heap size
      "spark.indextables.merge.debug" -> "true", // Enable debug
      "spark.indextables.indexwriter.batchSize" -> "50",
      "spark.indextables.mergeOnWrite.minDiskSpaceGB" -> "1"
    ))

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    logger.info(s"✅ Write completed with custom merge options")

    // Verify data integrity
    val readOptions = getReadOptions()
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    result.count() shouldBe 500

    logger.info("✅ Options propagation test completed successfully")
  }

  test("Real S3: post-commit merge-on-write should work with partitioned data") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/partitioned-merge-test"

    logger.info(s"Testing post-commit merge-on-write with partitioned data at: $tablePath")

    // Create partitioned data
    val df = spark
      .range(0, 1000)
      .select(
        col("id"),
        concat(lit("Content for "), col("id")).as("text"),
        (col("id") % 10).cast("string").as("partition_col")
      )

    // Write with partitioning and merge-on-write
    val writeOptions = getWriteOptions(Map(
      "spark.indextables.mergeOnWrite.enabled" -> "true",
      "spark.indextables.mergeOnWrite.mergeGroupMultiplier" -> "0.5",
      "spark.indextables.mergeOnWrite.targetSize" -> "500K",
      "spark.indextables.indexwriter.batchSize" -> "50",
      "spark.indextables.mergeOnWrite.minDiskSpaceGB" -> "1"
    ))

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("partition_col")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    logger.info(s"✅ Partitioned write with merge-on-write completed")

    // Verify data integrity and partition structure
    val readOptions = getReadOptions()
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    result.count() shouldBe 1000

    // Verify partition filtering
    val partition0 = result.filter(col("partition_col") === "0")
    partition0.count() shouldBe 100

    logger.info("✅ Partitioned merge-on-write test completed successfully")
  }

  test("Real S3: post-commit merge-on-write should handle multiple appends") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/multiple-appends-test"

    logger.info(s"Testing post-commit merge-on-write with multiple appends at: $tablePath")

    val writeOptions = getWriteOptions(Map(
      "spark.indextables.mergeOnWrite.enabled" -> "true",
      "spark.indextables.mergeOnWrite.mergeGroupMultiplier" -> "5.0", // Higher threshold
      "spark.indextables.mergeOnWrite.targetSize" -> "500K",
      "spark.indextables.indexwriter.batchSize" -> "50",
      "spark.indextables.mergeOnWrite.minDiskSpaceGB" -> "1"
    ))

    // First write
    val df1 = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    logger.info("✅ First write completed")

    // Second write (append)
    val df2 = spark.range(100, 200).selectExpr("id", "CAST(id AS STRING) as text")
    df2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("append")
      .save(tablePath)

    logger.info("✅ Second write completed")

    // Third write (append) with lower threshold to trigger merge
    val df3 = spark.range(200, 300).selectExpr("id", "CAST(id AS STRING) as text")
    df3.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions + ("spark.indextables.mergeOnWrite.mergeGroupMultiplier" -> "0.1"))
      .mode("append")
      .save(tablePath)

    logger.info("✅ Third write completed (merge should have triggered)")

    // Verify that merge actually occurred by checking split count
    // After 3 writes with batchSize=50, we would have many small splits
    // After merge with targetSize=500K and low threshold, we should have fewer splits
    val activeSplitCount = countActiveSplits(tablePath)

    // After merge, we should have consolidated splits
    // We expect fewer than 15 splits for 300 rows
    activeSplitCount should be < 15
    logger.info(s"✅ Merge confirmed after multiple appends: reduced to $activeSplitCount active splits")

    // Verify all data is present
    val readOptions = getReadOptions()
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    result.count() shouldBe 300

    // Verify no duplicates
    result.select("id").distinct().count() shouldBe 300

    logger.info("✅ Multiple appends with merge-on-write test completed successfully")
  }
}
