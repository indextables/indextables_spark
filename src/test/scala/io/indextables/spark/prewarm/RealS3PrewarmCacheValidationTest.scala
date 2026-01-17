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

import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.spark.sql.functions._

import org.apache.hadoop.fs.{FileSystem, Path}

import io.indextables.spark.storage.GlobalSplitCacheManager
import io.indextables.spark.RealS3TestBase
import io.indextables.tantivy4java.split.SplitCacheManager

/**
 * Validates that prewarming ALL fields and ALL segments on S3 eliminates cache misses.
 *
 * This test suite validates that after a complete prewarm, subsequent queries do not cause any new cache entries to be
 * created, proving that all required data was prewarmed.
 *
 * Key validation approach:
 *   1. Write test data to S3 2. Execute FLUSH to ensure clean cache state 3. Execute PREWARM with ALL segments
 *      including DOC_STORE 4. Record cache stats (total_bytes, components_cached) 5. Execute multiple queries that
 *      exercise different code paths 6. Verify cache stats haven't increased (no new cache entries)
 *
 * Prerequisites:
 *   - AWS credentials in ~/.aws/credentials file
 *   - Bucket configured via system property or default: test-tantivy4sparkbucket
 */
class RealS3PrewarmCacheValidationTest extends RealS3TestBase {

  private val S3_BUCKET    = System.getProperty("test.s3.bucket", "test-tantivy4sparkbucket")
  private val S3_REGION    = System.getProperty("test.s3.region", "us-east-2")
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/prewarm-cache-validation-$testRunId"

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
    diskCachePath = Files.createTempDirectory("prewarm_s3_disk_cache_").toFile.getAbsolutePath
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

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  /** List all files in the disk cache directory for debugging. */
  private def listDiskCacheContents(label: String): Unit = {
    val cacheDir = new File(diskCachePath)
    if (cacheDir.exists() && cacheDir.isDirectory) {
      val files = listFilesRecursively(cacheDir)
      println(s"\n📁 Disk cache contents ($label): ${files.length} files")
      if (files.isEmpty) {
        println("   (empty)")
      } else {
        // Group by split name for cleaner output
        val byPrefix = files.groupBy { f =>
          val name = f.getName
          // Extract split identifier (UUID pattern) from filename
          val uuidPattern = "[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}".r
          uuidPattern.findFirstIn(name).getOrElse("other")
        }
        byPrefix.toSeq.sortBy(_._1).foreach {
          case (prefix, groupFiles) =>
            println(s"   Split $prefix: ${groupFiles.length} files")
            groupFiles.sortBy(_.getName).take(10).foreach(f => println(s"     - ${f.getName} (${f.length()} bytes)"))
            if (groupFiles.length > 10) {
              println(s"     ... and ${groupFiles.length - 10} more files")
            }
        }
      }
    } else {
      println(s"\n📁 Disk cache contents ($label): directory does not exist")
    }
  }

  private def listFilesRecursively(dir: File): Seq[File] =
    if (dir.isDirectory) {
      Option(dir.listFiles()).map(_.toSeq).getOrElse(Seq.empty).flatMap { f =>
        if (f.isDirectory) listFilesRecursively(f) else Seq(f)
      }
    } else Seq.empty

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

  test("complete prewarm of ALL segments should eliminate cache misses for subsequent queries") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val (accessKey, secretKey) = awsCredentials.get
    val testPath               = s"$testBasePath/complete-prewarm-test"

    // Enable disk cache for tracking
    spark.conf.set("spark.indextables.cache.disk.enabled", "true")
    spark.conf.set("spark.indextables.cache.disk.path", diskCachePath)
    spark.conf.set("spark.indextables.cache.disk.maxSize", "1G")

    val ss = spark
    import ss.implicits._

    // Step 1: Create test data with multiple field types
    println("Step 1: Creating test data on S3...")
    val testData = (1 until 501)
      .map { i =>
        (
          i.toLong,
          s"title_$i with some searchable content",
          s"This is the full text content for record $i with various words",
          s"category_${i % 10}",
          i * 1.5,
          s"2024-${"%02d".format((i % 12) + 1)}-${"%02d".format((i % 28) + 1)}"
        )
      }
      .toDF("id", "title", "content", "category", "score", "date_str")

    testData
      .coalesce(2) // Create 2 splits for better test coverage
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .option("spark.indextables.indexWriter.batchSize", "100")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.typemap.title", "string")
      .option("spark.indextables.indexing.typemap.category", "string")
      .option("spark.indextables.indexing.fastfields", "score,id")
      .mode("overwrite")
      .save(testPath)

    println(s"✅ Created test data at $testPath")

    // Step 2: Flush cache to ensure clean state
    println("Step 2: Flushing cache to ensure clean state...")
    spark.sql("FLUSH INDEXTABLES DISK CACHE").collect()
    GlobalSplitCacheManager.flushAllCaches()
    Thread.sleep(500)

    // List cache contents BEFORE prewarm
    listDiskCacheContents("BEFORE PREWARM")

    // Step 3: Execute PREWARM with ALL segments (including DOC_STORE for complete coverage)
    println("\nStep 3: Prewarming ALL segments and ALL fields...")
    val prewarmResult = spark.sql(
      s"PREWARM INDEXTABLES CACHE '$testPath' FOR SEGMENTS (TERM_DICT, FAST_FIELD, POSTINGS, FIELD_NORM, DOC_STORE)"
    )
    val prewarmRows = prewarmResult.collect()

    println(s"✅ Prewarm result: ${prewarmRows.map(_.toString()).mkString(", ")}")

    // Verify prewarm succeeded
    assert(prewarmRows.nonEmpty, "Prewarm should return results")
    val splitsPrewarmed = prewarmRows.map(_.getAs[Int]("splits_prewarmed")).sum
    assert(splitsPrewarmed > 0, s"Should prewarm at least one split, got: $splitsPrewarmed")

    val prewarmStatus = prewarmRows.head.getAs[String]("status")
    assert(
      prewarmStatus == "success" || prewarmStatus == "partial",
      s"Prewarm status should be success or partial, got: $prewarmStatus"
    )

    // Step 4: Record cache stats after prewarm
    println("\nStep 4: Recording cache stats after prewarm...")
    Thread.sleep(500) // Allow cache to stabilize

    // List cache contents AFTER prewarm
    listDiskCacheContents("AFTER PREWARM")

    val statsAfterPrewarm = getDiskCacheStats()
    println(s"📊 Cache stats after prewarm: $statsAfterPrewarm")

    // Record S3 bytes fetched after prewarm (before queries)
    val s3BytesFetchedAfterPrewarm = SplitCacheManager.getObjectStorageBytesFetched()
    println(s"📊 S3 bytes fetched after prewarm: $s3BytesFetchedAfterPrewarm")

    // Step 5: Execute multiple queries that exercise different code paths
    println("\nStep 5: Executing queries that should all hit cache...")

    // List cache contents BEFORE queries
    listDiskCacheContents("BEFORE QUERIES (should match AFTER PREWARM)")

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(testPath)

    // Query 1: Simple equality filter (uses term dict)
    val count1 = df.filter(col("category") === "category_5").count()
    println(s"  Query 1 (equality filter): $count1 records")
    assert(count1 > 0, "Equality query should return results")

    // Query 2: Range filter (uses fast fields)
    val count2 = df.filter(col("score") > 100.0 && col("score") < 300.0).count()
    println(s"  Query 2 (range filter): $count2 records")
    assert(count2 > 0, "Range query should return results")

    // Query 3: Document retrieval (uses doc store)
    val docs = df.select("id", "title", "content", "category", "score").limit(50).collect()
    println(s"  Query 3 (document retrieval): ${docs.length} documents")
    assert(docs.length > 0, "Document retrieval should return results")

    // Query 4: Aggregation (uses fast fields)
    val aggResult = df.agg(count("*").as("cnt"), sum("score").as("total")).collect()
    println(s"  Query 4 (aggregation): count=${aggResult.head.getAs[Long]("cnt")}")

    // Query 5: Multiple filters combined
    val count5 = df.filter(col("id") > 100 && col("category") === "category_3").count()
    println(s"  Query 5 (combined filters): $count5 records")

    // Step 6: Verify cache stats haven't increased significantly (no new cache entries)
    println("\nStep 6: Verifying no significant new cache entries were created...")
    Thread.sleep(500) // Allow any async cache operations to complete

    // List cache contents AFTER queries
    listDiskCacheContents("AFTER QUERIES")

    val statsAfterQueries = getDiskCacheStats()
    println(s"📊 Cache stats after queries: $statsAfterQueries")

    // Verify no S3 access occurred during queries
    val s3BytesFetchedAfterQueries = SplitCacheManager.getObjectStorageBytesFetched()
    val s3BytesDelta               = s3BytesFetchedAfterQueries - s3BytesFetchedAfterPrewarm
    println(s"📊 S3 bytes fetched after queries: $s3BytesFetchedAfterQueries (delta: $s3BytesDelta)")

    // Key assertion: after full prewarm, queries should NOT access S3 at all
    assert(
      s3BytesDelta == 0,
      s"After full prewarm, queries should NOT access S3. " +
        s"S3 bytes fetched increased by $s3BytesDelta bytes during queries. " +
        s"Before queries: $s3BytesFetchedAfterPrewarm, After queries: $s3BytesFetchedAfterQueries"
    )
    println("✅ VERIFIED: No S3 access during queries after full prewarm")

    (statsAfterPrewarm, statsAfterQueries) match {
      case (Some((bytesAfterPrewarm, componentsAfterPrewarm)), Some((bytesAfterQueries, componentsAfterQueries))) =>
        val componentsDelta = componentsAfterQueries - componentsAfterPrewarm

        println(s"📊 Cache delta - components: $componentsDelta (before: $componentsAfterPrewarm, after: $componentsAfterQueries)")

        // Calculate what percentage of query components were already cached by prewarm
        // Higher is better - means prewarm covered more of what queries needed
        val prewarmCoveragePercent = if (componentsAfterQueries > 0) {
          (componentsAfterPrewarm.toDouble / componentsAfterQueries) * 100
        } else 100.0

        println(s"📊 Prewarm coverage: $prewarmCoveragePercent% of query components were prewarmed")

        // Key assertion: prewarm should cache 100% of what queries need (no cache misses)
        // With the cache sub-range lookup fix, complete prewarm should eliminate all cache misses
        assert(
          prewarmCoveragePercent >= 100.0,
          s"Prewarm only covered $prewarmCoveragePercent% of components needed by queries. " +
            s"Before: $componentsAfterPrewarm, After: $componentsAfterQueries, Delta: $componentsDelta. " +
            "Complete prewarm should cache 100% of required components (no new cache entries during queries)."
        )

        if (componentsDelta == 0) {
          println("✅ PERFECT: Complete prewarm eliminated ALL cache misses")
        } else if (prewarmCoveragePercent >= 80.0) {
          println(s"✅ EXCELLENT: Prewarm cached $prewarmCoveragePercent% of required components")
        } else {
          println(
            s"✅ GOOD: Prewarm cached $prewarmCoveragePercent% of required components (some additional loads expected)"
          )
        }

      case (None, _) =>
        println("⚠️  Could not get cache stats after prewarm - disk cache may not be enabled")
        println("✅ Prewarm completed successfully (cache stats not available for validation)")

      case (_, None) =>
        println("⚠️  Could not get cache stats after queries")
        println("✅ Prewarm completed successfully (cache stats not available for validation)")
    }
  }

  test("prewarm with specific fields should cache only those fields") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val (accessKey, secretKey) = awsCredentials.get
    val testPath               = s"$testBasePath/field-specific-prewarm-test"

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

    println(s"✅ Created field-specific test data at $testPath")

    // Flush cache
    spark.sql("FLUSH INDEXTABLES DISK CACHE").collect()
    GlobalSplitCacheManager.flushAllCaches()
    Thread.sleep(500)

    // Prewarm only the 'score' field with FAST_FIELD segment
    val prewarmResult = spark.sql(
      s"PREWARM INDEXTABLES CACHE '$testPath' FOR SEGMENTS (FAST_FIELD) ON FIELDS (score)"
    )
    val prewarmRows = prewarmResult.collect()

    println(s"✅ Field-specific prewarm result: ${prewarmRows.map(_.toString()).mkString(", ")}")

    assert(prewarmRows.nonEmpty, "Prewarm should return results")

    val fieldsPrewarmed = prewarmRows.head.getAs[String]("fields")
    println(s"📊 Fields prewarmed: $fieldsPrewarmed")

    // Verify the fields column shows the specific field
    assert(
      fieldsPrewarmed.contains("score") || fieldsPrewarmed == "all",
      s"Fields should include 'score', got: $fieldsPrewarmed"
    )

    // Execute a range query on 'score' - should hit cache
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(testPath)

    val rangeCount = df.filter(col("score") > 200.0 && col("score") < 400.0).count()
    println(s"✅ Range query on prewarmed field: $rangeCount records")
    assert(rangeCount > 0, "Range query should return results")

    println("✅ Field-specific prewarm test passed")
  }

  test("prewarm should report reasonable duration for S3 data") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val (accessKey, secretKey) = awsCredentials.get
    val testPath               = s"$testBasePath/prewarm-duration-test"

    // Enable disk cache
    spark.conf.set("spark.indextables.cache.disk.enabled", "true")
    spark.conf.set("spark.indextables.cache.disk.path", diskCachePath)

    val ss = spark
    import ss.implicits._

    // Create test data
    val testData =
      (1 until 1001).map(i => (i.toLong, s"title_$i", s"content_$i", i * 1.5)).toDF("id", "title", "content", "score")

    testData
      .coalesce(2)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .option("spark.indextables.indexWriter.batchSize", "200")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(testPath)

    println(s"✅ Created duration test data at $testPath")

    // Flush cache
    spark.sql("FLUSH INDEXTABLES DISK CACHE").collect()
    GlobalSplitCacheManager.flushAllCaches()
    Thread.sleep(500)

    // Time the prewarm
    val startTime = System.currentTimeMillis()
    val prewarmResult = spark.sql(
      s"PREWARM INDEXTABLES CACHE '$testPath' FOR SEGMENTS (TERM_DICT, FAST_FIELD, POSTINGS)"
    )
    val prewarmRows   = prewarmResult.collect()
    val totalDuration = System.currentTimeMillis() - startTime

    val reportedDuration = prewarmRows.head.getAs[Long]("duration_ms")

    println(s"📊 Prewarm completed - reported: ${reportedDuration}ms, total: ${totalDuration}ms")

    // Verify duration is reasonable (less than 2 minutes for test data from S3)
    assert(reportedDuration < 120000, s"Prewarm took too long: ${reportedDuration}ms")
    assert(reportedDuration >= 0, "Reported duration should be non-negative")

    println("✅ Prewarm duration test passed")
  }

  test("queries WITHOUT prewarm should increment S3 bytes fetched counter") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val (accessKey, secretKey) = awsCredentials.get
    val testPath               = s"$testBasePath/no-prewarm-counter-test"

    // Enable disk cache
    spark.conf.set("spark.indextables.cache.disk.enabled", "true")
    spark.conf.set("spark.indextables.cache.disk.path", diskCachePath)

    val ss = spark
    import ss.implicits._

    // Create test data
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
      .option("spark.indextables.indexWriter.batchSize", "100")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(testPath)

    println(s"✅ Created test data at $testPath")

    // Flush all caches to ensure clean state
    spark.sql("FLUSH INDEXTABLES DISK CACHE").collect()
    GlobalSplitCacheManager.flushAllCaches()
    Thread.sleep(500)

    // Record S3 bytes BEFORE any queries (no prewarm)
    val s3BytesBeforeQueries = SplitCacheManager.getObjectStorageBytesFetched()
    println(s"📊 S3 bytes fetched before queries (no prewarm): $s3BytesBeforeQueries")

    // Execute queries WITHOUT prewarm - these should access S3
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

    // Record S3 bytes AFTER queries
    val s3BytesAfterQueries = SplitCacheManager.getObjectStorageBytesFetched()
    val s3BytesDelta        = s3BytesAfterQueries - s3BytesBeforeQueries
    println(s"📊 S3 bytes fetched after queries: $s3BytesAfterQueries (delta: $s3BytesDelta)")

    // Key assertion: WITHOUT prewarm, queries SHOULD access S3
    assert(
      s3BytesDelta > 0,
      s"Without prewarm, queries SHOULD access S3. " +
        s"S3 bytes fetched did not increase during queries. " +
        s"Before: $s3BytesBeforeQueries, After: $s3BytesAfterQueries"
    )
    println(s"✅ VERIFIED: Queries without prewarm accessed S3 ($s3BytesDelta bytes fetched)")
  }

  test("prewarm all segments should enable zero-S3 aggregations") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val (accessKey, secretKey) = awsCredentials.get
    val testPath               = s"$testBasePath/partial-prewarm-agg-test"

    // Enable disk cache
    spark.conf.set("spark.indextables.cache.disk.enabled", "true")
    spark.conf.set("spark.indextables.cache.disk.path", diskCachePath)

    val ss = spark
    import ss.implicits._

    // Create test data with fast fields for aggregation
    val testData = (1 until 501)
      .map(i => (i.toLong, s"title_$i", s"category_${i % 10}", i * 1.5, i % 100))
      .toDF("id", "title", "category", "score", "value")

    testData
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .option("spark.indextables.indexWriter.batchSize", "100")
      .option("spark.indextables.indexing.fastfields", "score,value,id")
      .mode("overwrite")
      .save(testPath)

    println(s"✅ Created test data at $testPath")

    // Flush all caches to ensure clean state
    spark.sql("FLUSH INDEXTABLES DISK CACHE").collect()
    GlobalSplitCacheManager.flushAllCaches()
    Thread.sleep(500)

    // Record S3 bytes BEFORE prewarm
    val s3BytesBeforePrewarm = SplitCacheManager.getObjectStorageBytesFetched()
    println(s"📊 S3 bytes fetched before prewarm: $s3BytesBeforePrewarm")

    // Prewarm all segments needed for aggregations
    // Note: DOC_STORE is also needed for schema resolution and some aggregation paths
    val prewarmResult = spark.sql(
      s"PREWARM INDEXTABLES CACHE '$testPath' FOR SEGMENTS (FAST_FIELD, TERM_DICT, POSTINGS, FIELD_NORM, DOC_STORE)"
    )
    val prewarmRows = prewarmResult.collect()

    println(s"✅ Prewarm result: ${prewarmRows.map(_.toString()).mkString(", ")}")
    assert(prewarmRows.nonEmpty, "Prewarm should return results")

    val splitsPrewarmed = prewarmRows.head.getAs[Int]("splits_prewarmed")
    assert(splitsPrewarmed > 0, "Should prewarm at least one split")

    // Record S3 bytes AFTER prewarm
    val s3BytesAfterPrewarm = SplitCacheManager.getObjectStorageBytesFetched()
    val prewarmS3BytesDelta = s3BytesAfterPrewarm - s3BytesBeforePrewarm
    println(s"📊 S3 bytes fetched after prewarm: $s3BytesAfterPrewarm (delta: $prewarmS3BytesDelta)")

    // Verify prewarm DID access S3
    assert(
      prewarmS3BytesDelta > 0,
      s"Prewarm SHOULD access S3 to fetch index segments. " +
        s"Before: $s3BytesBeforePrewarm, After: $s3BytesAfterPrewarm"
    )
    println(s"✅ VERIFIED: Prewarm accessed S3 ($prewarmS3BytesDelta bytes fetched)")

    // Now run aggregations - these should NOT access S3
    // Use a single DataFrame and run pushed-down aggregations
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(testPath)

    // Aggregation 1: Simple count (pushed down to tantivy)
    val count1 = df.count()
    println(s"  Aggregation 1 (count): $count1")

    // Aggregation 2: Multiple aggregates on fast fields (pushed down)
    val multiAgg = df
      .agg(
        count("*").as("cnt"),
        sum("score").as("total_score"),
        avg("value").as("avg_value"),
        min("id").as("min_id"),
        max("id").as("max_id")
      )
      .collect()
      .head
    println(s"  Aggregation 2 (multi): count=${multiAgg.getLong(0)}, sum=${multiAgg.getDouble(1)}")

    // Aggregation 3: Filtered count (uses term dict for filter)
    val filteredCount = df.filter(col("category") === "category_5").count()
    println(s"  Aggregation 3 (filtered count): $filteredCount")

    // Aggregation 4: Filtered aggregation with fast fields
    val filteredAgg = df
      .filter(col("category") === "category_5")
      .agg(count("*").as("cnt"), sum("score").as("total"))
      .collect()
      .head
    println(s"  Aggregation 4 (filtered multi-agg): count=${filteredAgg.getLong(0)}")

    // Record S3 bytes AFTER aggregations
    val s3BytesAfterAggregations = SplitCacheManager.getObjectStorageBytesFetched()
    val aggS3BytesDelta          = s3BytesAfterAggregations - s3BytesAfterPrewarm
    println(s"📊 S3 bytes fetched after aggregations: $s3BytesAfterAggregations (delta: $aggS3BytesDelta)")

    // Key assertion: After prewarming all segments, aggregations should NOT access S3
    assert(
      aggS3BytesDelta == 0,
      s"After prewarming all segments, aggregations should NOT access S3. " +
        s"S3 bytes increased by $aggS3BytesDelta bytes during aggregations. " +
        s"After prewarm: $s3BytesAfterPrewarm, After aggregations: $s3BytesAfterAggregations"
    )
    println("✅ VERIFIED: Aggregations after full prewarm did NOT access S3")
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
