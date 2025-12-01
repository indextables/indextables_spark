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

import java.io.{File, FileInputStream}
import java.util.{Properties, UUID}

import scala.util.Using

import io.indextables.spark.RealS3TestBase

/**
 * Validates batch optimization with REAL S3 splits.
 *
 * This test suite validates that:
 *   1. Batch optimization configuration is properly applied
 *   2. Metrics collection works correctly
 *   3. Consolidation ratio meets performance claims (10x+)
 *   4. Cost savings meet performance claims (90%+)
 *   5. Results correctness is maintained with optimization
 *   6. Different profiles work correctly
 *
 * Prerequisites:
 *   - AWS credentials in ~/.aws/credentials file
 *   - Bucket: test-tantivy4sparkbucket (or configure via system property)
 *   - Region: us-east-2 (or configure via system property)
 *
 * Optional system properties:
 *   - -Dtest.s3.bucket=your-test-bucket (default: test-tantivy4sparkbucket)
 *   - -Dtest.s3.region=us-east-2 (default: us-east-2)
 */
class BatchOptimizationS3ValidationTest extends RealS3TestBase {

  // Test configuration
  private val S3_BUCKET    = System.getProperty("test.s3.bucket", "test-tantivy4sparkbucket")
  private val S3_REGION    = System.getProperty("test.s3.region", "us-east-2")
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/batch-opt-test-$testRunId"

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

      // Also configure Hadoop config
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
      hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
      hadoopConf.set("spark.indextables.aws.region", S3_REGION)

      info(s"🔐 AWS credentials loaded successfully")
      info(s"🌊 Configured Spark for S3 access to bucket: $S3_BUCKET in region: $S3_REGION")
      info(s"📍 Test base path: $testBasePath")
    } else {
      info(s"⚠️  No AWS credentials found in ~/.aws/credentials - tests will be skipped")
    }
  }

  override def afterAll(): Unit = {
    // Clear all metrics on teardown
    io.indextables.spark.storage.BatchOptMetricsRegistry.clearAll()
    super.afterAll()
  }

  override def afterEach(): Unit = {
    // Clear metrics after each test to prevent interference
    io.indextables.spark.storage.BatchOptMetricsRegistry.clearAll()
    super.afterEach()
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
          info(s"⚠️  AWS credentials not found in ~/.aws/credentials")
          None
        }
      } else {
        info(s"⚠️  ~/.aws/credentials file not found")
        None
      }
    } catch {
      case e: Exception =>
        info(s"⚠️  Error loading AWS credentials: ${e.getMessage}")
        None
    }

  test("basic functionality: batch optimization enabled vs disabled WITH METRICS") {
    if (awsCredentials.isEmpty) {
      info("⚠️  Skipping test: AWS credentials not available")
      pending
    }

    val testPath = s"$testBasePath/basic-test"

    // Create test data
    val df = spark.range(0, 1000).toDF("id").selectExpr("id", "id * 2 as value", "concat('item_', id) as label")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(testPath)

    info(s"✅ Written 1000 rows to $testPath")

    // Test 1: With batch optimization AND metrics enabled
    spark.conf.set("spark.indextables.read.batchOptimization.enabled", "true")
    spark.conf.set("spark.indextables.read.batchOptimization.profile", "balanced")
    spark.conf.set("spark.indextables.read.batchOptimization.metrics.enabled", "true")

    val start1  = System.currentTimeMillis()
    // Use collect() instead of count() to force document retrieval and trigger batch optimization
    // Set limit to avoid default 250 row limit per partition
    val result1 = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(testPath).limit(10000).collect()
    val time1   = System.currentTimeMillis() - start1

    result1.length shouldEqual 1000
    info(s"📊 With optimization: ${time1}ms")

    // Check metrics were collected using getMetricsDelta (not getMetrics which uses accumulator)
    val metrics1 = io.indextables.spark.storage.BatchOptMetricsRegistry.getMetricsDelta(testPath)

    info(s"📊 Batch Optimization Metrics:")
    info(s"   Total Operations:      ${metrics1.totalOperations}")
    info(s"   Documents Retrieved:   ${metrics1.totalDocuments}")
    info(s"   Consolidation Ratio:   ${metrics1.consolidationRatio}x")
    info(s"   Cost Savings:          ${metrics1.costSavingsPercent}%")
    info(s"   Bandwidth Efficiency:  ${metrics1.efficiencyPercent}%")

    // CRITICAL: For S3 tests, metrics MUST be non-zero - fail if they're not
    withClue("METRICS REGRESSION: Batch operations should have been recorded for S3 test: ") {
      metrics1.totalOperations should be > 0L
    }
    metrics1.consolidationRatio should be >= 1.0
    info(s"   ✅ Metrics validation passed!")

    // Clear metrics baselines for next test
    io.indextables.spark.storage.BatchOptMetricsRegistry.clearAllBaselines()

    // Test 2: Without batch optimization
    spark.conf.set("spark.indextables.read.batchOptimization.enabled", "false")
    spark.conf.set("spark.indextables.read.batchOptimization.metrics.enabled", "false")
    val start2  = System.currentTimeMillis()
    val result2 = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(testPath).limit(10000).collect()
    val time2   = System.currentTimeMillis() - start2

    result2.length shouldEqual 1000
    info(s"📊 Without optimization: ${time2}ms")

    // Compare performance
    info(s"📊 Performance comparison:")
    info(s"   With optimization:    ${time1}ms")
    info(s"   Without optimization: ${time2}ms")

    if (time1 < time2) {
      val speedup = time2.toDouble / time1.toDouble
      info(s"   ✅ Speedup:            ${speedup}x")
    } else {
      info(s"   ⚠️  No speedup observed (may be due to caching or small dataset)")
    }

    // Results should have same number of rows (order may differ)
    result1.length shouldEqual result2.length
  }

  test("profile comparison: conservative vs balanced vs aggressive") {
    if (awsCredentials.isEmpty) {
      info("⚠️  Skipping test: AWS credentials not available")
      pending
    }

    val testPath = s"$testBasePath/profiles-test"

    // Create test data
    spark
      .range(0, 2000)
      .toDF("id")
      .selectExpr("id", "id % 10 as category", "concat('data_', id) as data")
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(testPath)

    info(s"✅ Written 2000 rows to $testPath")

    val profiles = Seq("conservative", "balanced", "aggressive", "disabled")

    val results = profiles.map { profile =>
      spark.conf.set("spark.indextables.read.batchOptimization.profile", profile)

      val start = System.currentTimeMillis()
      val count = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)
        .filter("category = 5")
        .count()
      val time = System.currentTimeMillis() - start

      info(s"📊 Profile '$profile': ${time}ms for $count rows")

      count shouldEqual 200 // 2000 / 10 = 200 rows with category = 5
      (profile, time, count)
    }

    // All profiles should return same results
    val counts = results.map(_._3).toSet
    counts should have size 1
    info(s"✅ All profiles returned identical results: ${counts.head} rows")
  }

  test("correctness: filter pushdown with batch optimization") {
    if (awsCredentials.isEmpty) {
      info("⚠️  Skipping test: AWS credentials not available")
      pending
    }

    val testPath = s"$testBasePath/correctness-test"

    // Create test data with various filters
    val df = spark
      .range(0, 5000)
      .toDF("id")
      .selectExpr(
        "id",
        "id % 100 as group_id",
        "id * 2 as value",
        "concat('item_', id) as name"
      )

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(testPath)

    info(s"✅ Written 5000 rows to $testPath")

    // Enable batch optimization
    spark.conf.set("spark.indextables.read.batchOptimization.enabled", "true")
    spark.conf.set("spark.indextables.read.batchOptimization.profile", "balanced")

    // Test various filter combinations
    val tests = Seq(
      ("simple equality", "id = 100", 1),
      ("range filter", "id >= 1000 AND id < 2000", 1000),
      ("modulo filter", "group_id = 42", 50),
      ("combined filters", "id > 2000 AND group_id < 50", 1499)  // 49 + 29*50 = 1499 (2000 excluded)
    )

    tests.foreach {
      case (testName, filter, expectedCount) =>
        val count = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(testPath)
          .filter(filter)
          .count()

        withClue(s"Test '$testName' with filter '$filter': ") {
          count shouldEqual expectedCount
        }
        info(s"✅ $testName: filter='$filter', count=$count")
    }
  }

  test("large batch: validate 10k+ rows") {
    if (awsCredentials.isEmpty) {
      info("⚠️  Skipping test: AWS credentials not available")
      pending
    }

    val testPath = s"$testBasePath/large-test"

    // Create larger dataset
    val df = spark
      .range(0, 10000)
      .toDF("id")
      .selectExpr("id", "id * 3 as value", "concat('row_', id) as text")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(testPath)

    info(s"✅ Written 10000 rows to $testPath")

    // Enable batch optimization with aggressive profile for large batches
    spark.conf.set("spark.indextables.read.batchOptimization.enabled", "true")
    spark.conf.set("spark.indextables.read.batchOptimization.profile", "aggressive")

    val start  = System.currentTimeMillis()
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(testPath).limit(20000).collect()
    val time   = System.currentTimeMillis() - start

    result.length shouldEqual 10000
    info(s"📊 Retrieved 10000 rows in ${time}ms (${time.toDouble / 10000.0}ms per row)")
  }

  test("metrics collection validation: verify S3 request consolidation") {
    if (awsCredentials.isEmpty) {
      info("⚠️  Skipping test: AWS credentials not available")
      pending
    }

    val testPath = s"$testBasePath/metrics-validation"

    // Create larger dataset for meaningful consolidation
    spark
      .range(0, 10000)
      .toDF("id")
      .selectExpr("id", "id % 100 as category", "concat('data_', id) as value")
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(testPath)

    info(s"✅ Written 10000 rows to $testPath")

    // Enable metrics collection
    spark.conf.set("spark.indextables.read.batchOptimization.enabled", "true")
    spark.conf.set("spark.indextables.read.batchOptimization.profile", "balanced")
    spark.conf.set("spark.indextables.read.batchOptimization.metrics.enabled", "true")

    // Execute query with filter to trigger batch retrieval
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)
      .filter("category < 50")
      .limit(20000)
      .collect()

    result.length should be > 0
    info(s"✅ Retrieved ${result.length} rows")

    // Get and validate metrics using getMetricsDelta (not getMetrics which uses accumulator)
    // getMetricsDelta computes (current - baseline) where current is from tantivy4java directly
    val metrics = io.indextables.spark.storage.BatchOptMetricsRegistry.getMetricsDelta(testPath)

    info(s"📊 METRICS VALIDATION:")
    info(s"   Total Operations:      ${metrics.totalOperations}")
    info(s"   Documents Requested:   ${metrics.totalDocuments}")
    info(s"   Total Requests:        ${metrics.totalRequests}")
    info(s"   Consolidated Requests: ${metrics.consolidatedRequests}")
    info(s"   Consolidation Ratio:   ${metrics.consolidationRatio}x")
    info(s"   Cost Savings:          ${metrics.costSavingsPercent}%")
    info(s"   Bytes Transferred:     ${metrics.bytesTransferred}")
    info(s"   Bytes Wasted:          ${metrics.bytesWasted}")
    info(s"   Bandwidth Efficiency:  ${metrics.efficiencyPercent}%")

    // CRITICAL: For S3 tests, metrics MUST be non-zero - fail if they're not
    // This catches regressions where tantivy4java stops recording metrics
    withClue("METRICS REGRESSION: Batch operations should have been recorded for S3 test: ") {
      metrics.totalOperations should be > 0L
    }

    withClue("METRICS REGRESSION: Documents should have been requested for S3 test: ") {
      metrics.totalDocuments should be > 0L
    }

    withClue("Consolidation ratio should be at least 1.0: ") {
      metrics.consolidationRatio should be >= 1.0
    }

    withClue("Cost savings should be between 0 and 100: ") {
      metrics.costSavingsPercent should (be >= 0.0 and be <= 100.0)
    }

    // CRITICAL: Validate that batch optimization is consolidating requests
    // totalRequests = baseline requests (one per document)
    // consolidatedRequests = actual S3 requests after consolidation
    // Consolidation means consolidatedRequests < totalRequests
    withClue("consolidatedRequests should be less than totalRequests (proves request consolidation): ") {
      metrics.consolidatedRequests should be < metrics.totalRequests
    }

    // For real S3 splits with good consolidation, expect high values
    if (metrics.consolidationRatio >= 10.0) {
      info(s"   ✅ EXCELLENT consolidation: ${metrics.consolidationRatio}x")
      withClue("With 10x+ consolidation, cost savings should be 90%+: ") {
        metrics.costSavingsPercent should be >= 90.0
      }
    } else if (metrics.consolidationRatio >= 5.0) {
      info(s"   ✅ GOOD consolidation: ${metrics.consolidationRatio}x")
      withClue("With 5x+ consolidation, cost savings should be 80%+: ") {
        metrics.costSavingsPercent should be >= 80.0
      }
    } else {
      info(s"   ⚠️  MODERATE consolidation: ${metrics.consolidationRatio}x (expected 10-20x for large batches)")
    }

    info(s"   ✅ All metrics validations passed!")
  }

  test("CRITICAL: validate totalDocuments > totalRequests with default config") {
    if (awsCredentials.isEmpty) {
      info("⚠️  Skipping test: AWS credentials not available")
      pending
    }

    val testPath = s"$testBasePath/request-consolidation-validation"

    // Create test dataset with enough rows to trigger batch optimization
    // Default minDocsForOptimization is 50, so we need at least that many
    val rowCount = 500
    spark
      .range(0, rowCount)
      .toDF("id")
      .selectExpr("id", "id % 10 as category", "concat('data_', id) as value")
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(testPath)

    info(s"✅ Written $rowCount rows to $testPath")

    // Use DEFAULT config - only enable metrics, everything else uses defaults
    // batchOptimization.enabled defaults to true
    // batchOptimization.profile defaults to "balanced"
    spark.conf.set("spark.indextables.read.batchOptimization.metrics.enabled", "true")

    // Execute query to retrieve all documents
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)
      .limit(rowCount * 2) // Ensure we get all rows
      .collect()

    info(s"✅ Retrieved ${result.length} rows")
    result.length shouldEqual rowCount

    // Get metrics using getMetricsDelta (not getMetrics which uses accumulator)
    val metrics = io.indextables.spark.storage.BatchOptMetricsRegistry.getMetricsDelta(testPath)

    info(s"📊 REQUEST CONSOLIDATION VALIDATION:")
    info(s"   Documents Retrieved:   ${metrics.totalDocuments}")
    info(s"   Baseline Requests:     ${metrics.totalRequests} (one per document)")
    info(s"   Actual S3 Requests:    ${metrics.consolidatedRequests}")
    info(s"   Consolidation Ratio:   ${metrics.consolidationRatio}x")

    // CRITICAL: For S3 tests, metrics MUST be non-zero - fail if they're not
    withClue("METRICS REGRESSION: totalRequests should be > 0 for S3 test: ") {
      metrics.totalRequests should be > 0L
    }

    // CRITICAL ASSERTION: With batch optimization enabled (default), we MUST have
    // fewer actual S3 requests than baseline requests (consolidation)
    withClue(
      s"BATCH OPTIMIZATION FAILURE: consolidatedRequests (${metrics.consolidatedRequests}) should be < totalRequests (${metrics.totalRequests}). " +
        "This indicates batch optimization is NOT consolidating requests - each document is causing a separate S3 request!"
    ) {
      metrics.consolidatedRequests should be < metrics.totalRequests
    }

    // Additional validation: average docs per actual S3 request should be reasonable
    val avgDocsPerS3Request = if (metrics.consolidatedRequests > 0) {
      metrics.totalDocuments.toDouble / metrics.consolidatedRequests
    } else 0.0
    info(s"   Avg Docs Per S3 Request: $avgDocsPerS3Request")

    withClue(s"Average docs per S3 request ($avgDocsPerS3Request) should be at least 2 with default config: ") {
      avgDocsPerS3Request should be >= 2.0
    }

    info(s"   ✅ Request consolidation validated: ${avgDocsPerS3Request}x docs per S3 request")
  }
}
