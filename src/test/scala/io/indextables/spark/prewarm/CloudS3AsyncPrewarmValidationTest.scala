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

package io.indextables.spark.prewarm

import java.io.File
import java.nio.file.Files
import java.util.{Properties, UUID}

import scala.util.Using

import org.apache.spark.sql.functions._

import org.apache.hadoop.fs.{FileSystem, Path}

import io.indextables.spark.storage.GlobalSplitCacheManager
import io.indextables.spark.CloudS3TestBase
import io.indextables.tantivy4java.split.SplitCacheManager

/**
 * Validates async prewarm functionality with real AWS S3 storage.
 *
 * This test suite validates:
 *   1. PREWARM ... ASYNC MODE returns immediately with job_id 2. DESCRIBE INDEXTABLES PREWARM JOBS shows
 *      running/completed jobs 3. WAIT FOR INDEXTABLES PREWARM JOBS blocks until completion 4. After async prewarm,
 *      queries hit cache (no S3 access)
 *
 * Prerequisites:
 *   - AWS credentials in ~/.aws/credentials file
 *   - Bucket configured via system property or default: test-tantivy4sparkbucket
 */
class CloudS3AsyncPrewarmValidationTest extends CloudS3TestBase {

  private val S3_BUCKET    = System.getProperty("test.s3.bucket", "test-tantivy4sparkbucket")
  private val S3_REGION    = System.getProperty("test.s3.region", "us-east-2")
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/async-prewarm-validation-$testRunId"

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None
  private var fs: FileSystem                           = _

  // Disk cache path for local validation
  private var diskCachePath: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    if (awsCredentials.isDefined) {
      val (accessKey, secretKey) = awsCredentials.get

      // Configure Hadoop config FIRST before creating FileSystem
      val hadoopConf = spark.sparkContext.hadoopConfiguration

      // Set standard S3A properties for Hadoop FileSystem
      hadoopConf.set("fs.s3a.access.key", accessKey)
      hadoopConf.set("fs.s3a.secret.key", secretKey)
      hadoopConf.set("fs.s3a.endpoint.region", S3_REGION)

      // Also set indextables properties for CloudStorageProvider
      hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
      hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
      hadoopConf.set("spark.indextables.aws.region", S3_REGION)

      // Configure Spark for real S3 access
      spark.conf.set("spark.indextables.aws.accessKey", accessKey)
      spark.conf.set("spark.indextables.aws.secretKey", secretKey)
      spark.conf.set("spark.indextables.aws.region", S3_REGION)

      // Create FileSystem instance
      fs = new Path(testBasePath).getFileSystem(hadoopConf)

      println(s"✅ AWS credentials loaded successfully")
      println(s"✅ Test base path: $testBasePath")
    } else {
      println("⚠️  No AWS credentials found - tests will be skipped")
    }

    // Create a local disk cache directory for this test
    diskCachePath = Files.createTempDirectory("async_prewarm_s3_disk_cache_").toFile.getAbsolutePath
    println(s"✅ Disk cache path: $diskCachePath")
  }

  override def afterAll(): Unit =
    try {
      // Clean up S3 test directory if it exists
      if (awsCredentials.isDefined && fs != null) {
        val basePath = new Path(testBasePath)
        if (fs.exists(basePath)) {
          println(s"🧹 Cleaning up S3 test directory: $testBasePath")
          fs.delete(basePath, true)
        }
      }
      // Clean up local disk cache directory
      if (diskCachePath != null) {
        deleteRecursively(new File(diskCachePath))
      }
    } finally
      super.afterAll()

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Reset async prewarm job manager before each test
    AsyncPrewarmJobManager.reset()
  }

  override def afterEach(): Unit = {
    AsyncPrewarmJobManager.reset()
    super.afterEach()
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  /** Helper to get disk cache stats from DESCRIBE command. */
  private def getDiskCacheStats(): Option[(Long, Long)] =
    try {
      val result = spark.sql("DESCRIBE INDEXTABLES DISK CACHE").collect()
      result.find(_.getAs[String]("executor_id") == "driver").flatMap { row =>
        if (row.getAs[Boolean]("enabled") && !row.isNullAt(row.fieldIndex("total_bytes"))) {
          val totalBytes       = row.getAs[Long]("total_bytes")
          val componentsCached = row.getAs[Long]("components_cached")
          Some((totalBytes, componentsCached))
        } else None
      }
    } catch {
      case e: Exception =>
        println(s"⚠️  Failed to get disk cache stats: ${e.getMessage}")
        None
    }

  test("async prewarm should start job and return immediately with job_id") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val (accessKey, secretKey) = awsCredentials.get
    val testPath               = s"$testBasePath/async-basic-test"

    // Enable disk cache for tracking
    spark.conf.set("spark.indextables.cache.disk.enabled", "true")
    spark.conf.set("spark.indextables.cache.disk.path", diskCachePath)

    val ss = spark
    import ss.implicits._

    // Step 1: Create test data
    println("Step 1: Creating test data on S3...")
    val testData = (1 until 201)
      .map(i => (i.toLong, s"title_$i", s"category_${i % 10}", i * 1.5))
      .toDF("id", "title", "category", "score")

    testData
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .option("spark.indextables.indexWriter.batchSize", "50")
      .option("spark.indextables.indexing.fastfields", "score,id")
      .mode("overwrite")
      .save(testPath)

    println(s"✅ Created test data at $testPath")

    // Step 2: Flush cache to ensure clean state
    println("Step 2: Flushing cache to ensure clean state...")
    spark.sql("FLUSH INDEXTABLES DISK CACHE").collect()
    GlobalSplitCacheManager.flushAllCaches()
    Thread.sleep(500)

    // Step 3: Execute async prewarm
    println("Step 3: Starting async prewarm...")
    val startTime = System.currentTimeMillis()
    val prewarmResult = spark.sql(
      s"PREWARM INDEXTABLES CACHE '$testPath' FOR SEGMENTS (TERM_DICT, FAST_FIELD, POSTINGS) ASYNC MODE"
    )
    val prewarmRows = prewarmResult.collect()
    val returnTime  = System.currentTimeMillis() - startTime

    println(s"✅ Async prewarm returned in ${returnTime}ms")
    println(s"   Result: ${prewarmRows.map(_.toString()).mkString(", ")}")

    // Verify async prewarm returned
    assert(prewarmRows.nonEmpty, "Async prewarm should return results")

    // Verify job_id is present
    val jobId = prewarmRows.head.getAs[String]("job_id")
    assert(jobId != null && jobId.nonEmpty, s"Should return job_id, got: $jobId")
    println(s"   Job ID: $jobId")

    // Verify async_mode column is true
    val asyncMode = prewarmRows.head.getAs[Boolean]("async_mode")
    assert(asyncMode, "async_mode should be true")

    println("✅ Async prewarm started successfully with job_id")
  }

  test("DESCRIBE PREWARM JOBS should show async job status") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val (accessKey, secretKey) = awsCredentials.get
    val testPath               = s"$testBasePath/describe-jobs-test"

    // Enable disk cache
    spark.conf.set("spark.indextables.cache.disk.enabled", "true")
    spark.conf.set("spark.indextables.cache.disk.path", diskCachePath)

    val ss = spark
    import ss.implicits._

    // Create test data
    val testData = (1 until 301)
      .map(i => (i.toLong, s"title_$i", s"category_${i % 5}", i * 2.0))
      .toDF("id", "title", "category", "score")

    testData
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .option("spark.indextables.indexWriter.batchSize", "75")
      .option("spark.indextables.indexing.fastfields", "score,id")
      .mode("overwrite")
      .save(testPath)

    // Flush cache
    spark.sql("FLUSH INDEXTABLES DISK CACHE").collect()
    GlobalSplitCacheManager.flushAllCaches()
    Thread.sleep(500)

    // Start async prewarm
    println("Starting async prewarm...")
    val prewarmResult = spark.sql(
      s"PREWARM INDEXTABLES CACHE '$testPath' FOR SEGMENTS (TERM_DICT, FAST_FIELD, POSTINGS, DOC_STORE) ASYNC MODE"
    )
    val prewarmRows = prewarmResult.collect()
    val jobId       = prewarmRows.head.getAs[String]("job_id")
    println(s"✅ Async prewarm started with job_id: $jobId")

    // Check job status with DESCRIBE
    println("Checking job status with DESCRIBE PREWARM JOBS...")
    val describeResult = spark.sql("DESCRIBE INDEXTABLES PREWARM JOBS")
    val describeRows   = describeResult.collect()

    println(s"   DESCRIBE returned ${describeRows.length} rows")
    describeRows.foreach(row => println(s"   - ${row.toString()}"))

    // Verify schema has expected columns
    val outputNames = describeResult.columns
    assert(outputNames.contains("job_id"), "Should have job_id column")
    assert(outputNames.contains("status"), "Should have status column")
    assert(outputNames.contains("total_splits"), "Should have total_splits column")
    assert(outputNames.contains("completed_splits"), "Should have completed_splits column")

    println("✅ DESCRIBE PREWARM JOBS executed successfully")
  }

  test("WAIT FOR PREWARM JOBS should block until job completes") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val (accessKey, secretKey) = awsCredentials.get
    val testPath               = s"$testBasePath/wait-for-jobs-test"

    // Enable disk cache
    spark.conf.set("spark.indextables.cache.disk.enabled", "true")
    spark.conf.set("spark.indextables.cache.disk.path", diskCachePath)

    val ss = spark
    import ss.implicits._

    // Create test data
    val testData = (1 until 201)
      .map(i => (i.toLong, s"title_$i", s"category_${i % 10}", i * 1.5))
      .toDF("id", "title", "category", "score")

    testData
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .option("spark.indextables.indexWriter.batchSize", "50")
      .option("spark.indextables.indexing.fastfields", "score,id")
      .mode("overwrite")
      .save(testPath)

    // Flush cache
    spark.sql("FLUSH INDEXTABLES DISK CACHE").collect()
    GlobalSplitCacheManager.flushAllCaches()
    Thread.sleep(500)

    // Start async prewarm
    println("Starting async prewarm...")
    val prewarmResult = spark.sql(
      s"PREWARM INDEXTABLES CACHE '$testPath' FOR SEGMENTS (TERM_DICT, FAST_FIELD, POSTINGS) ASYNC MODE"
    )
    val prewarmRows = prewarmResult.collect()
    val jobId       = prewarmRows.head.getAs[String]("job_id")
    println(s"✅ Async prewarm started with job_id: $jobId")

    // Wait for job to complete
    println("Waiting for prewarm jobs to complete...")
    val startTime  = System.currentTimeMillis()
    val waitResult = spark.sql(s"WAIT FOR INDEXTABLES PREWARM JOBS '$testPath' TIMEOUT 120")
    val waitRows   = waitResult.collect()
    val waitTime   = System.currentTimeMillis() - startTime

    println(s"✅ WAIT completed in ${waitTime}ms")
    waitRows.foreach(row => println(s"   - ${row.toString()}"))

    // Verify wait returned completion status
    assert(waitRows.nonEmpty, "WAIT should return results")

    val status = waitRows.head.getAs[String]("status")
    println(s"   Final status: $status")

    // Status should be COMPLETED, not_found (if job finished and cleaned up), or timeout
    assert(
      status == "COMPLETED" || status == "not_found" || status.contains("TIMEOUT"),
      s"Status should indicate completion or timeout, got: $status"
    )

    println("✅ WAIT FOR PREWARM JOBS executed successfully")
  }

  test("async prewarm followed by wait should enable cache-only queries") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val (accessKey, secretKey) = awsCredentials.get
    val testPath               = s"$testBasePath/async-cache-validation-test"

    // Enable disk cache
    spark.conf.set("spark.indextables.cache.disk.enabled", "true")
    spark.conf.set("spark.indextables.cache.disk.path", diskCachePath)
    spark.conf.set("spark.indextables.cache.disk.maxSize", "1G")

    val ss = spark
    import ss.implicits._

    // Create test data
    println("Step 1: Creating test data on S3...")
    val testData = (1 until 301)
      .map(i => (i.toLong, s"title_$i", s"category_${i % 10}", i * 1.5))
      .toDF("id", "title", "category", "score")

    testData
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .option("spark.indextables.indexWriter.batchSize", "75")
      .option("spark.indextables.indexing.fastfields", "score,id")
      .mode("overwrite")
      .save(testPath)

    println(s"✅ Created test data at $testPath")

    // Flush cache
    println("Step 2: Flushing cache...")
    spark.sql("FLUSH INDEXTABLES DISK CACHE").collect()
    GlobalSplitCacheManager.flushAllCaches()
    Thread.sleep(500)

    // Start async prewarm with all segments
    println("Step 3: Starting async prewarm...")
    val prewarmResult = spark.sql(
      s"PREWARM INDEXTABLES CACHE '$testPath' FOR SEGMENTS (TERM_DICT, FAST_FIELD, POSTINGS, DOC_STORE) ASYNC MODE"
    )
    val prewarmRows = prewarmResult.collect()
    val jobId       = prewarmRows.head.getAs[String]("job_id")
    println(s"✅ Async prewarm started with job_id: $jobId")

    // Wait for completion
    println("Step 4: Waiting for async prewarm to complete...")
    val waitResult = spark.sql(s"WAIT FOR INDEXTABLES PREWARM JOBS '$testPath' TIMEOUT 120")
    val waitRows   = waitResult.collect()
    println(s"✅ Async prewarm completed")
    waitRows.foreach(row => println(s"   - ${row.toString()}"))

    // Record S3 bytes after prewarm
    val s3BytesAfterPrewarm = SplitCacheManager.getObjectStorageBytesFetched()
    println(s"📊 S3 bytes fetched after prewarm: $s3BytesAfterPrewarm")

    // Execute queries that should hit cache
    println("Step 5: Executing queries (should hit cache)...")
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(testPath)

    // Query 1: Equality filter
    val count1 = df.filter(col("category") === "category_5").count()
    println(s"  Query 1 (equality filter): $count1 records")

    // Query 2: Range filter
    val count2 = df.filter(col("score") > 100.0 && col("score") < 300.0).count()
    println(s"  Query 2 (range filter): $count2 records")

    // Query 3: Aggregation
    val aggResult = df.agg(count("*").as("cnt"), sum("score").as("total")).collect()
    println(s"  Query 3 (aggregation): count=${aggResult.head.getAs[Long]("cnt")}")

    // Verify no S3 access during queries
    val s3BytesAfterQueries = SplitCacheManager.getObjectStorageBytesFetched()
    val s3BytesDelta        = s3BytesAfterQueries - s3BytesAfterPrewarm
    println(s"📊 S3 bytes fetched after queries: $s3BytesAfterQueries (delta: $s3BytesDelta)")

    // Key assertion: after full async prewarm, queries should NOT access S3
    assert(
      s3BytesDelta == 0,
      s"After full async prewarm, queries should NOT access S3. " +
        s"S3 bytes fetched increased by $s3BytesDelta bytes during queries."
    )
    println("✅ VERIFIED: No S3 access during queries after async prewarm")
  }

  /** Load AWS credentials from ~/.aws/credentials file. */
  private def loadAwsCredentials(): Option[(String, String)] =
    try {
      val home     = System.getProperty("user.home")
      val credFile = new File(s"$home/.aws/credentials")

      if (!credFile.exists()) {
        println(s"⚠️  AWS credentials file not found at: ${credFile.getAbsolutePath}")
        return None
      }

      Using(new java.io.FileInputStream(credFile)) { fis =>
        val props = new Properties()
        props.load(fis)

        val accessKey = props.getProperty("aws_access_key_id")
        val secretKey = props.getProperty("aws_secret_access_key")

        if (accessKey != null && secretKey != null) {
          Some((accessKey, secretKey))
        } else {
          println("⚠️  AWS credentials not found in ~/.aws/credentials")
          None
        }
      }.toOption.flatten
    } catch {
      case ex: Exception =>
        println(s"⚠️  Failed to read AWS credentials: ${ex.getMessage}")
        None
    }
}
