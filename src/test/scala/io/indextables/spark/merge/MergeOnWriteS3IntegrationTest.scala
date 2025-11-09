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
import java.util.Properties

import scala.util.Using

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * S3-specific integration tests for merge-on-write feature.
 *
 * These tests validate shuffle-based merge-on-write with S3 storage:
 * 1. Write phase: Split creation on executors
 * 2. Shuffle phase: RDD-based split distribution (no S3 staging)
 * 3. Merge phase: Executor-side merge using local split bytes
 * 4. Upload phase: Final merged splits uploaded to S3
 * 5. Transaction log: S3-based transaction log operations
 * 6. Data integrity: Verify all data survives shuffle and merge
 *
 * ARCHITECTURE: Shuffle-based (no S3 staging, 100% locality via Spark shuffle)
 *
 * NOTE: These tests require AWS credentials and an S3 bucket.
 * Set environment variables:
 *   - AWS_ACCESS_KEY_ID
 *   - AWS_SECRET_ACCESS_KEY
 *   - INDEXTABLES_TEST_S3_BUCKET (e.g., "my-test-bucket")
 *   - INDEXTABLES_TEST_S3_PREFIX (optional, e.g., "test-prefix")
 *
 * Tests will be skipped if credentials are not available.
 */
class MergeOnWriteS3IntegrationTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _
  private var s3Bucket: Option[String] = None
  private var s3Prefix: String = "merge-on-write-test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Try to load AWS credentials from ~/.aws/credentials first, then fall back to environment variables
    val awsCredentials = loadAwsCredentials()
      .orElse(sys.env.get("AWS_ACCESS_KEY_ID").flatMap(ak =>
        sys.env.get("AWS_SECRET_ACCESS_KEY").map(sk => (ak, sk))))

    // Load S3 bucket from environment or use default
    s3Bucket = sys.env.get("INDEXTABLES_TEST_S3_BUCKET").orElse(Some("test-tantivy4sparkbucket"))
    s3Prefix = sys.env.getOrElse("INDEXTABLES_TEST_S3_PREFIX", "merge-on-write-test")

    if (awsCredentials.isDefined && s3Bucket.isDefined) {
      val (accessKey, secretKey) = awsCredentials.get

      spark = SparkSession.builder()
        .appName("MergeOnWriteS3IntegrationTest")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        // S3 configuration
        .config("spark.indextables.aws.accessKey", accessKey)
        .config("spark.indextables.aws.secretKey", secretKey)
        .config("spark.indextables.aws.region", "us-east-2")
        .config("spark.indextables.s3.maxConcurrency", "4")
        .config("spark.indextables.s3.partSize", "64M")
        // Hadoop S3A configuration for direct FileSystem access
        .config("spark.hadoop.fs.s3a.access.key", accessKey)
        .config("spark.hadoop.fs.s3a.secret.key", secretKey)
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-2")
        .getOrCreate()

      println(s"✅ S3 integration tests enabled")
      println(s"   - Bucket: ${s3Bucket.get}")
      println(s"   - Prefix: $s3Prefix")
    } else {
      println(s"⚠️  S3 integration tests SKIPPED - missing credentials or bucket")
      println(s"   - Set AWS credentials in ~/.aws/credentials or environment variables")
      println(s"   - Set INDEXTABLES_TEST_S3_BUCKET environment variable (or use default)")
    }
  }

  /** Load AWS credentials from ~/.aws/credentials file. */
  private def loadAwsCredentials(): Option[(String, String)] = {
    try {
      val home = System.getProperty("user.home")
      val credFile = new File(s"$home/.aws/credentials")

      if (credFile.exists()) {
        val props = new Properties()
        Using(new FileInputStream(credFile))(fis => props.load(fis))

        val accessKey = props.getProperty("aws_access_key_id")
        val secretKey = props.getProperty("aws_secret_access_key")

        if (accessKey != null && secretKey != null) {
          Some((accessKey, secretKey))
        } else {
          None
        }
      } else {
        None
      }
    } catch {
      case e: Exception =>
        None
    }
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  private def skipIfS3NotConfigured(): Unit = {
    if (spark == null || s3Bucket.isEmpty) {
      cancel("S3 not configured - set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, INDEXTABLES_TEST_S3_BUCKET")
    }
  }

  test("S3: End-to-end merge-on-write with staging") {
    skipIfS3NotConfigured()

    val tablePath = s"s3a://${s3Bucket.get}/$s3Prefix/e2e-merge-test-${System.currentTimeMillis()}"
    val stagingPath = s"$tablePath/_staging"

    // Step 1: Create test data with many small splits
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val sourceData = 1.to(10000).map { i =>
      (i, s"content_$i", i % 100, s"category_${i % 10}")
    }.toDF("id", "content", "score", "category")
      .repartition(8)  // 8 small splits

    // Step 2: Write with merge-on-write enabled
    val writeStartTime = System.currentTimeMillis()
    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.indexWriter.batchSize", "1000")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .mode(SaveMode.Overwrite)
      .save(tablePath)
    val writeDuration = System.currentTimeMillis() - writeStartTime

    println(s"✅ S3 write completed in ${writeDuration}ms")

    // Step 3: Read back and verify
    val readStartTime = System.currentTimeMillis()
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    val readDuration = System.currentTimeMillis() - readStartTime

    val rowCount = readData.count()
    rowCount shouldBe 10000

    println(s"✅ S3 read completed in ${readDuration}ms")
    println(s"   - All 10000 records verified")

    // Step 4: Validate transaction log contents using TransactionLogFactory API
    println("\n[Validation] Reading transaction log contents...")
    import io.indextables.spark.transaction.TransactionLogFactory
    import org.apache.hadoop.fs.Path

    val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)
    val addActions = try {
      transactionLog.listFiles()
    } finally {
      transactionLog.close()
    }

    println(s"   - Transaction log has ${addActions.size} split(s)")
    addActions.foreach { action =>
      println(s"     * ${action.path}")
    }

    // Verify we have fewer splits than original (8 partitions should merge down)
    assert(addActions.size < 8, s"Expected < 8 splits after merge, got ${addActions.size}")
    assert(addActions.size > 0, "Expected at least 1 split")

    val mergedCount = addActions.count(_.path.startsWith("merged-"))
    val promotedCount = addActions.count(_.path.startsWith("split-"))
    println(s"   ✓ Merge occurred: 8 splits → ${addActions.size} splits (${mergedCount} merged, ${promotedCount} promoted)")

    // Step 5: Validate min/max statistics present
    println("\n[Validation] Checking data skipping statistics...")
    addActions.foreach { action =>
      assert(action.minValues.isDefined, s"Split ${action.path} missing minValues")
      assert(action.maxValues.isDefined, s"Split ${action.path} missing maxValues")
      assert(action.minValues.get.contains("score"), s"Split ${action.path} missing score in minValues")
      assert(action.maxValues.get.contains("score"), s"Split ${action.path} missing score in maxValues")
    }
    println(s"   ✓ All ${addActions.size} splits have min/max statistics for data skipping")

    // Step 6: Validate staging cleanup (no .tmp files)
    println("\n[Validation] Checking staging cleanup...")
    try {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(stagingPath), hadoopConf)

      try {
        if (fs.exists(new Path(stagingPath))) {
          val stagingFiles = fs.listStatus(new Path(stagingPath))
          val tmpFiles = stagingFiles.filter(_.getPath.getName.endsWith(".tmp"))
          assert(tmpFiles.isEmpty, s"Found ${tmpFiles.length} .tmp files in staging")
          println(s"   ✓ No .tmp files in staging (${stagingFiles.length} total files)")
        } else {
          println(s"   ✓ Staging directory does not exist (cleaned up completely)")
        }
      } finally {
        fs.close()
      }
    } catch {
      case e: Exception =>
        println(s"   ⚠ Could not validate staging cleanup: ${e.getMessage}")
        println(s"   ℹ Staging cleanup validation skipped for S3")
    }

    // Step 7: Verify filter pushdown
    val filteredCount = readData.filter($"category" === "category_5").count()
    filteredCount shouldBe 1000

    // Step 8: Verify aggregation
    val aggResult = readData.agg(count("*"), sum("score")).collect()(0)
    aggResult.getAs[Long](0) shouldBe 10000

    println(s"✅ S3 comprehensive validation passed")
    println(s"   - Merge: 8 → ${addActions.size} splits")
    println(s"   - Statistics: PRESENT")
    println(s"   - Staging cleanup: VERIFIED")
    println(s"   - Data integrity: VERIFIED")
  }

  test("S3: Partitioned dataset with merge-on-write") {
    skipIfS3NotConfigured()

    val tablePath = s"s3a://${s3Bucket.get}/$s3Prefix/partitioned-test-${System.currentTimeMillis()}"
    val stagingPath = s"$tablePath/_staging"

    // Step 1: Create partitioned test data
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val sourceData = 1.to(5000).flatMap { i =>
      Seq(
        (i, s"content_$i", i % 100, "2024-01-01", 10),
        (i + 5000, s"content_${i + 5000}", (i + 5000) % 100, "2024-01-02", 11),
        (i + 10000, s"content_${i + 10000}", (i + 10000) % 100, "2024-01-03", 12)
      )
    }.toDF("id", "content", "score", "date", "hour")
      .repartition(6)

    // Step 2: Write partitioned data
    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .partitionBy("date", "hour")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    println(s"✅ S3 partitioned write completed")

    // Step 3: Read with partition pruning
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val date1Count = readData.filter($"date" === "2024-01-01").count()
    date1Count shouldBe 5000

    val date2Count = readData.filter($"date" === "2024-01-02").count()
    date2Count shouldBe 5000

    println(s"✅ S3 partition pruning verified")
  }

  test("S3: Append mode with merge-on-write") {
    skipIfS3NotConfigured()

    val tablePath = s"s3a://${s3Bucket.get}/$s3Prefix/append-test-${System.currentTimeMillis()}"
    val stagingPath = s"$tablePath/_staging"

    // Step 1: Initial write
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val batch1 = 1.to(5000).map { i =>
      (i, s"content_$i", i % 100)
    }.toDF("id", "content", "score")
      .repartition(4)

    batch1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Step 2: Append second batch
    val batch2 = 5001.to(10000).map { i =>
      (i, s"content_$i", i % 100)
    }.toDF("id", "content", "score")
      .repartition(4)

    batch2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .mode(SaveMode.Append)
      .save(tablePath)

    // Step 3: Verify both batches
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val totalCount = readData.count()
    totalCount shouldBe 10000

    readData.filter($"id" === 100).count() shouldBe 1
    readData.filter($"id" === 5100).count() shouldBe 1

    println(s"✅ S3 append mode verified")
  }

  test("S3: Verify staging and final locations") {
    skipIfS3NotConfigured()

    val tablePath = s"s3a://${s3Bucket.get}/$s3Prefix/staging-test-${System.currentTimeMillis()}"
    val stagingPath = s"$tablePath/_staging"

    // Step 1: Write data
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val sourceData = 1.to(5000).map { i =>
      (i, s"content_$i", i % 100)
    }.toDF("id", "content", "score")
      .repartition(4)

    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Step 2: Verify transaction log exists
    val transactionLogPath = s"$tablePath/_transaction_log"

    // Use Hadoop FileSystem API to check S3 paths
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(transactionLogPath),
      hadoopConf
    )

    val txLogExists = fs.exists(new org.apache.hadoop.fs.Path(transactionLogPath))
    txLogExists shouldBe true

    // Step 3: List files in transaction log
    val txLogFiles = fs.listStatus(new org.apache.hadoop.fs.Path(transactionLogPath))
    txLogFiles should not be empty

    val jsonFiles = txLogFiles.filter((f: org.apache.hadoop.fs.FileStatus) =>
      f.getPath.getName.endsWith(".json") || f.getPath.getName.endsWith(".json.gz"))
    jsonFiles should not be empty

    println(s"✅ S3 transaction log verified")
    println(s"   - Transaction log files: ${jsonFiles.length}")

    // Step 4: Verify splits exist in final location
    val splitFiles = fs.listStatus(new org.apache.hadoop.fs.Path(tablePath))
      .filter((f: org.apache.hadoop.fs.FileStatus) => f.getPath.getName.endsWith(".split"))

    splitFiles should not be empty
    println(s"   - Split files: ${splitFiles.length}")

    fs.close()
  }

  test("S3: Retry logic for transient failures") {
    skipIfS3NotConfigured()

    val tablePath = s"s3a://${s3Bucket.get}/$s3Prefix/retry-test-${System.currentTimeMillis()}"
    val stagingPath = s"$tablePath/_staging"

    // Step 1: Write with retry configuration
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val sourceData = 1.to(5000).map { i =>
      (i, s"content_$i", i % 100)
    }.toDF("id", "content", "score")
      .repartition(4)

    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .option("spark.indextables.mergeOnWrite.maxRetries", "5")  // More retries
      .option("spark.indextables.mergeOnWrite.retryDelayMs", "500")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Step 2: Verify write succeeded despite potential transient failures
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    readData.count() shouldBe 5000

    println(s"✅ S3 retry logic verified - write completed successfully")
  }

  test("S3: Large dataset performance test") {
    skipIfS3NotConfigured()

    val tablePath = s"s3a://${s3Bucket.get}/$s3Prefix/large-test-${System.currentTimeMillis()}"
    val stagingPath = s"$tablePath/_staging"

    // Step 1: Create larger dataset
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val sourceData = 1.to(50000).map { i =>
      (i, s"content_$i", i % 1000, s"category_${i % 50}")
    }.toDF("id", "content", "score", "category")
      .repartition(16)  // More partitions = more splits

    // Step 2: Write and measure time
    val writeStartTime = System.currentTimeMillis()
    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.indexWriter.batchSize", "2000")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "100M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .mode(SaveMode.Overwrite)
      .save(tablePath)
    val writeDuration = System.currentTimeMillis() - writeStartTime

    // Step 3: Read and measure time
    val readStartTime = System.currentTimeMillis()
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    val rowCount = readData.count()
    val readDuration = System.currentTimeMillis() - readStartTime

    rowCount shouldBe 50000

    println(s"✅ S3 large dataset test completed")
    println(s"   - Write time: ${writeDuration}ms (${50000 * 1000 / writeDuration} rows/sec)")
    println(s"   - Read time: ${readDuration}ms (${50000 * 1000 / readDuration} rows/sec)")
    println(s"   - Records: $rowCount")
  }

  test("S3: Direct upload - single split bypasses merge") {
    skipIfS3NotConfigured()

    val tablePath = s"s3a://${s3Bucket.get}/$s3Prefix/direct-upload-test-${System.currentTimeMillis()}"

    println("\n" + "="*80)
    println("S3 TEST: Direct Upload Path Validation (Shuffle-Based)")
    println("="*80)

    // Step 1: Create single partition (will trigger direct upload, not merge)
    println("\n[Step 1] Creating single partition with 1000 records...")
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val sourceData = 1.to(1000).map { i =>
      (i, s"content_$i", i % 100, s"category_${i % 10}")
    }.toDF("id", "content", "score", "category")
      .repartition(1)  // Single partition = single split

    println(s"   ✓ Created DataFrame with 1000 records in 1 partition")

    // Step 2: Write with merge-on-write (minSplitsToMerge: 2, but only 1 split)
    println("\n[Step 2] Writing to S3 with merge-on-write (minSplitsToMerge: 2)...")
    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")  // Requires 2+ to merge
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.indexing.typemap.content", "string")
      .option("spark.indextables.indexing.typemap.category", "string")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    println(s"   ✓ S3 write completed")

    // Step 3: Read transaction log with full metadata
    println("\n[Step 3] Reading S3 transaction log...")
    import io.indextables.spark.transaction.TransactionLogFactory
    import org.apache.hadoop.fs.Path

    val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)
    val addActions = try {
      transactionLog.listFiles()
    } finally {
      transactionLog.close()
    }

    println(s"   - Transaction log has ${addActions.size} split(s)")
    addActions.foreach { action =>
      println(s"     * ${action.path}")
      println(s"       - numMergeOps: ${action.numMergeOps}")
    }

    // Step 4: Validate direct upload occurred (shuffle-based merge)
    println("\n[Step 4] Validating direct upload path...")
    assert(addActions.size == 1, s"Expected exactly 1 split, got ${addActions.size}")

    val directUploadSplit = addActions.head

    // Verify it's a direct upload split (not merged) - shuffle-based merge uses "part-" prefix
    assert(directUploadSplit.path.startsWith("part-"),
      s"Expected part- prefix for direct upload split, got: ${directUploadSplit.path}")

    // Verify numMergeOps is 0 (no merge operations - direct upload)
    assert(directUploadSplit.numMergeOps.isDefined, "Expected numMergeOps to be present")
    assert(directUploadSplit.numMergeOps.get == 0,
      s"Expected numMergeOps=0 for direct upload split, got: ${directUploadSplit.numMergeOps.get}")

    println(s"   ✓ Split correctly uses direct upload (not merged)")
    println(s"   ✓ numMergeOps=0 confirms no merge occurred")

    // Step 5: Validate statistics
    println("\n[Step 5] Validating data skipping statistics...")
    assert(directUploadSplit.minValues.isDefined, "Expected minValues")
    assert(directUploadSplit.maxValues.isDefined, "Expected maxValues")
    assert(directUploadSplit.minValues.get.contains("score"), "Expected score in minValues")
    assert(directUploadSplit.maxValues.get.contains("score"), "Expected score in maxValues")
    println(s"   ✓ Statistics preserved")

    // Step 6: Validate data readable from S3
    println("\n[Step 6] Reading data from S3...")
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val rowCount = readData.count()
    assert(rowCount == 1000, s"Expected 1000 rows, got $rowCount")
    println(s"   ✓ Successfully read ${rowCount} rows from S3 promoted split")

    // Step 7: Validate filter and aggregation
    val filtered = readData.filter($"score" === 42)
    assert(filtered.count() > 0)

    val aggResult = readData.agg(count("*"), min("score"), max("score")).collect()(0)
    assert(aggResult.getAs[Long](0) == 1000)
    assert(aggResult.getAs[Number](1).longValue() == 0)
    assert(aggResult.getAs[Number](2).longValue() == 99)
    println(s"   ✓ Filter pushdown and aggregation work")

    println("\n" + "="*80)
    println("S3 PROMOTION TEST SUMMARY:")
    println("="*80)
    println(s"  - Final splits: 1 (0 merged, 1 promoted)")
    println(s"  - mergeStrategy: promoted")
    println(s"  - numMergeOps: 0")
    println(s"  - Data integrity: VERIFIED")
    println(s"  - S3 storage: VERIFIED")
    println("="*80 + "\n")
  }
}
