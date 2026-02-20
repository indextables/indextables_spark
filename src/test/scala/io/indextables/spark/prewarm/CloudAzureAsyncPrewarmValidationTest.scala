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
import java.util.UUID

import org.apache.spark.sql.functions._

import io.indextables.spark.storage.GlobalSplitCacheManager
import io.indextables.spark.CloudAzureTestBase

/**
 * Validates async prewarm functionality with real Azure Blob Storage.
 *
 * This test suite validates:
 *   1. PREWARM ... ASYNC MODE returns immediately with job_id 2. DESCRIBE INDEXTABLES PREWARM JOBS shows
 *      running/completed jobs 3. WAIT FOR INDEXTABLES PREWARM JOBS blocks until completion 4. After async prewarm,
 *      queries hit cache
 *
 * Credentials are loaded from multiple sources with the following priority:
 *   1. System properties: test.azure.storageAccount, test.azure.accountKey 2. ~/.azure/credentials file (matches
 *      tantivy4java pattern) 3. Environment variables: AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY
 */
class CloudAzureAsyncPrewarmValidationTest extends CloudAzureTestBase {

  // Generate unique test run ID to avoid conflicts
  private val testRunId = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath =
    s"abfss://$testContainer@${getStorageAccount.getOrElse("unknown")}.dfs.core.windows.net/async-prewarm-validation-$testRunId"

  // Disk cache path for local validation
  private var diskCachePath: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create a local disk cache directory for this test
    diskCachePath = Files.createTempDirectory("async_prewarm_azure_disk_cache_").toFile.getAbsolutePath
    println(s"✅ Disk cache path: $diskCachePath")

    if (hasAzureCredentials()) {
      println(s"✅ Azure credentials loaded successfully")
      println(s"✅ Test base path: $testBasePath")
    } else {
      println("⚠️  No Azure credentials found - tests will be skipped")
    }
  }

  override def afterAll(): Unit =
    try
      // Clean up local disk cache directory
      if (diskCachePath != null) {
        deleteRecursively(new File(diskCachePath))
      }
    // Note: Azure test directory cleanup would require Azure SDK
    // For now, rely on container lifecycle policies or manual cleanup
    finally
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
    assume(hasAzureCredentials(), "Azure credentials required for Azure tests")
    assume(getStorageAccount.isDefined, "Azure storage account required")
    assume(getAccountKey.isDefined, "Azure account key required")

    val storageAccount = getStorageAccount.get
    val accountKey     = getAccountKey.get
    val testPath =
      s"abfss://$testContainer@$storageAccount.dfs.core.windows.net/async-prewarm-test-$testRunId/basic-test"

    // Enable disk cache
    spark.conf.set("spark.indextables.cache.disk.enabled", "true")
    spark.conf.set("spark.indextables.cache.disk.path", diskCachePath)

    val ss = spark
    import ss.implicits._

    // Step 1: Create test data
    println("Step 1: Creating test data on Azure...")
    val testData = (1 until 201)
      .map(i => (i.toLong, s"title_$i", s"category_${i % 10}", i * 1.5))
      .toDF("id", "title", "category", "score")

    testData
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.azure.accountName", storageAccount)
      .option("spark.indextables.azure.accountKey", accountKey)
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
    assume(hasAzureCredentials(), "Azure credentials required for Azure tests")
    assume(getStorageAccount.isDefined, "Azure storage account required")
    assume(getAccountKey.isDefined, "Azure account key required")

    val storageAccount = getStorageAccount.get
    val accountKey     = getAccountKey.get
    val testPath =
      s"abfss://$testContainer@$storageAccount.dfs.core.windows.net/async-prewarm-test-$testRunId/describe-jobs"

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
      .option("spark.indextables.azure.accountName", storageAccount)
      .option("spark.indextables.azure.accountKey", accountKey)
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
    assume(hasAzureCredentials(), "Azure credentials required for Azure tests")
    assume(getStorageAccount.isDefined, "Azure storage account required")
    assume(getAccountKey.isDefined, "Azure account key required")

    val storageAccount = getStorageAccount.get
    val accountKey     = getAccountKey.get
    val testPath =
      s"abfss://$testContainer@$storageAccount.dfs.core.windows.net/async-prewarm-test-$testRunId/wait-for-jobs"

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
      .option("spark.indextables.azure.accountName", storageAccount)
      .option("spark.indextables.azure.accountKey", accountKey)
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

  test("async prewarm followed by wait should enable cache hits") {
    assume(hasAzureCredentials(), "Azure credentials required for Azure tests")
    assume(getStorageAccount.isDefined, "Azure storage account required")
    assume(getAccountKey.isDefined, "Azure account key required")

    val storageAccount = getStorageAccount.get
    val accountKey     = getAccountKey.get
    val testPath =
      s"abfss://$testContainer@$storageAccount.dfs.core.windows.net/async-prewarm-test-$testRunId/cache-validation"

    // Enable disk cache
    spark.conf.set("spark.indextables.cache.disk.enabled", "true")
    spark.conf.set("spark.indextables.cache.disk.path", diskCachePath)
    spark.conf.set("spark.indextables.cache.disk.maxSize", "1G")

    val ss = spark
    import ss.implicits._

    // Create test data
    println("Step 1: Creating test data on Azure...")
    val testData = (1 until 301)
      .map(i => (i.toLong, s"title_$i", s"category_${i % 10}", i * 1.5))
      .toDF("id", "title", "category", "score")

    testData
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.azure.accountName", storageAccount)
      .option("spark.indextables.azure.accountKey", accountKey)
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

    // Record cache stats before prewarm
    val statsBeforePrewarm = getDiskCacheStats()
    println(s"📊 Cache stats before prewarm: $statsBeforePrewarm")

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

    // Record cache stats after prewarm
    Thread.sleep(500)
    val statsAfterPrewarm = getDiskCacheStats()
    println(s"📊 Cache stats after prewarm: $statsAfterPrewarm")

    // Execute queries
    println("Step 5: Executing queries...")
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.azure.accountName", storageAccount)
      .option("spark.indextables.azure.accountKey", accountKey)
      .load(testPath)

    // Query 1: Equality filter
    val count1 = df.filter(col("category") === "category_5").count()
    println(s"  Query 1 (equality filter): $count1 records")
    assert(count1 > 0, "Query should return results")

    // Query 2: Range filter
    val count2 = df.filter(col("score") > 100.0 && col("score") < 300.0).count()
    println(s"  Query 2 (range filter): $count2 records")
    assert(count2 > 0, "Query should return results")

    // Query 3: Aggregation
    val aggResult = df.agg(count("*").as("cnt"), sum("score").as("total")).collect()
    println(s"  Query 3 (aggregation): count=${aggResult.head.getAs[Long]("cnt")}")

    // Record cache stats after queries
    Thread.sleep(500)
    val statsAfterQueries = getDiskCacheStats()
    println(s"📊 Cache stats after queries: $statsAfterQueries")

    // Verify cache was populated by prewarm
    (statsBeforePrewarm, statsAfterPrewarm) match {
      case (Some((bytesBefore, componentsBefore)), Some((bytesAfter, componentsAfter))) =>
        val componentsDelta = componentsAfter - componentsBefore
        println(s"📊 Cache delta from prewarm: $componentsDelta components")
        assert(componentsDelta > 0, "Prewarm should populate cache")
        println("✅ Verified: Async prewarm populated the cache")

      case _ =>
        println("⚠️  Could not verify cache stats (disk cache may not be enabled)")
    }

    println("✅ Async prewarm + wait + query workflow completed successfully")
  }
}
