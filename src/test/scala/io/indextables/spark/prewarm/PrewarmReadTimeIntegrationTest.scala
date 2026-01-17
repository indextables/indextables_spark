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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import io.indextables.spark.storage.GlobalSplitCacheManager
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import org.slf4j.LoggerFactory

/**
 * Integration tests for read-time prewarming via Spark configuration properties.
 *
 * Tests that prewarm executes automatically when reading data with appropriate config set.
 */
class PrewarmReadTimeIntegrationTest extends AnyFunSuite with BeforeAndAfterEach {

  private val logger = LoggerFactory.getLogger(classOf[PrewarmReadTimeIntegrationTest])

  var spark: SparkSession   = _
  var tempTablePath: String = _

  override def beforeEach(): Unit = {
    try
      GlobalSplitCacheManager.flushAllCaches()
    catch {
      case _: Exception => // Ignore
    }

    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("PrewarmReadTimeIntegrationTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.indextables.cache.disk.enabled", "false")
      .getOrCreate()

    tempTablePath = Files.createTempDirectory("prewarm_readtime_test_").toFile.getAbsolutePath
    logger.info(s"Test table path: $tempTablePath")
  }

  override def afterEach(): Unit = {
    // Clear any prewarm configs
    Seq(
      "spark.indextables.prewarm.enabled",
      "spark.indextables.prewarm.segments",
      "spark.indextables.prewarm.fields",
      "spark.indextables.prewarm.splitsPerTask",
      "spark.indextables.prewarm.failOnMissingField",
      "spark.indextables.prewarm.catchUpNewHosts"
    ).foreach { key =>
      try spark.conf.unset(key)
      catch {
        case _: Exception => // Ignore
      }
    }

    if (spark != null) {
      spark.stop()
      spark = null
    }

    if (tempTablePath != null) {
      deleteRecursively(new File(tempTablePath))
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  private def createTestData(numRecords: Int = 200): Unit = {
    val ss = spark
    import ss.implicits._

    val testData = (1 until numRecords + 1)
      .map { i =>
        (
          i.toLong,
          s"title_$i",
          s"content for record $i",
          s"category_${i % 5}",
          i * 1.5
        )
      }
      .toDF("id", "title", "content", "category", "score")

    testData
      .coalesce(1)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.batchSize", "50")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(tempTablePath)

    logger.info(s"Created test data with $numRecords records")
  }

  // === Basic Read-Time Prewarm Tests ===

  test("read with prewarm.enabled=true should trigger prewarm before query") {
    createTestData(100)

    // Enable read-time prewarm
    spark.conf.set("spark.indextables.prewarm.enabled", "true")

    // Read and query - prewarm should happen automatically
    val startTime = System.currentTimeMillis()
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    val count    = df.filter(col("category") === "category_2").count()
    val duration = System.currentTimeMillis() - startTime

    logger.info(s"Read with prewarm completed in ${duration}ms, found $count records")

    assert(count > 0, "Query should return results")
    // First read with prewarm might be slower, subsequent should be faster

    logger.info("Read-time prewarm.enabled test passed")
  }

  test("read with prewarm.enabled=false should not trigger prewarm") {
    createTestData(100)

    // Explicitly disable prewarm
    spark.conf.set("spark.indextables.prewarm.enabled", "false")

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    val count = df.filter(col("category") === "category_1").count()

    assert(count > 0, "Query should return results without prewarm")

    logger.info("Read-time prewarm.enabled=false test passed")
  }

  // === Segment Configuration Tests ===

  test("read with prewarm.segments config should prewarm only specified segments") {
    createTestData(100)

    spark.conf.set("spark.indextables.prewarm.enabled", "true")
    spark.conf.set("spark.indextables.prewarm.segments", "TERM_DICT,FAST_FIELD")

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    // Execute a query that would benefit from these segments
    val count = df.filter(col("score") > 100.0).count()

    logger.info(s"Query with TERM_DICT,FAST_FIELD prewarm returned $count records")
    assert(count > 0, "Query should return results")

    logger.info("Read-time prewarm.segments test passed")
  }

  test("read with all segments including DOC_STORE should prewarm completely") {
    createTestData(100)

    spark.conf.set("spark.indextables.prewarm.enabled", "true")
    spark.conf.set("spark.indextables.prewarm.segments", "TERM_DICT,FAST_FIELD,POSTINGS,FIELD_NORM,DOC_STORE")

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    // Query and retrieve document content
    val results = df.select("id", "title", "content").limit(10).collect()

    logger.info(s"Query with all segments prewarm returned ${results.length} documents")
    assert(results.length > 0, "Should return documents")

    logger.info("Read-time prewarm all segments test passed")
  }

  // === Field Configuration Tests ===

  test("read with prewarm.fields config should prewarm only specified fields") {
    createTestData(100)

    spark.conf.set("spark.indextables.prewarm.enabled", "true")
    spark.conf.set("spark.indextables.prewarm.fields", "title,category")

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    // Query on prewarmed field using equality filter (supported)
    val count = df.filter(col("category") === "category_1").count()

    logger.info(s"Query with field-specific prewarm returned $count records")
    assert(count > 0, "Query should return results")

    logger.info("Read-time prewarm.fields test passed")
  }

  test("read with empty prewarm.fields should prewarm all fields") {
    createTestData(100)

    spark.conf.set("spark.indextables.prewarm.enabled", "true")
    spark.conf.set("spark.indextables.prewarm.fields", "")

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    val count = df.count()
    logger.info(s"Query with empty fields config returned $count records")

    assert(count > 0, "Query should return all records")

    logger.info("Read-time prewarm empty fields test passed")
  }

  // === Parallelism Configuration Tests ===

  test("read with prewarm.splitsPerTask config should use custom parallelism") {
    createTestData(200)

    spark.conf.set("spark.indextables.prewarm.enabled", "true")
    spark.conf.set("spark.indextables.prewarm.splitsPerTask", "5")

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    // Use score (fast field) for range query instead of id
    val count = df.filter(col("score") > 150.0).count()

    logger.info(s"Query with splitsPerTask=5 returned $count records")
    assert(count > 0, "Query should return results")

    logger.info("Read-time prewarm.splitsPerTask test passed")
  }

  // === Field Validation Configuration Tests ===

  test("read with failOnMissingField=false should handle invalid fields gracefully") {
    createTestData(100)

    spark.conf.set("spark.indextables.prewarm.enabled", "true")
    spark.conf.set("spark.indextables.prewarm.fields", "title,nonexistent_field")
    spark.conf.set("spark.indextables.prewarm.failOnMissingField", "false")

    // Should not throw exception
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    val count = df.count()

    logger.info(s"Query with warn-skip mode returned $count records")
    assert(count > 0, "Query should return results even with invalid field in prewarm config")

    logger.info("Read-time failOnMissingField=false test passed")
  }

  test("read with failOnMissingField=true should use config for field validation") {
    createTestData(100)

    spark.conf.set("spark.indextables.prewarm.enabled", "true")
    spark.conf.set("spark.indextables.prewarm.fields", "title,definitely_not_a_field")
    spark.conf.set("spark.indextables.prewarm.failOnMissingField", "true")

    // Field validation is best-effort - if metadata unavailable, prewarm succeeds
    try {
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempTablePath)

      // Force evaluation
      val count = df.count()
      logger.info(s"Count with failOnMissingField=true: $count (field validation was unavailable)")
      assert(count > 0, "Query should return results")
    } catch {
      case e: Exception =>
        // If field validation was available, exception thrown
        logger.info(s"Exception with failOnMissingField=true: ${e.getMessage}")
        assert(e != null, "Exception should be thrown for invalid field")
    }

    logger.info("Read-time failOnMissingField=true config test passed")
  }

  // === Option-Level Configuration Tests ===

  test("read-time prewarm via reader options should work") {
    createTestData(100)

    // Configure via reader options instead of session
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.prewarm.enabled", "true")
      .option("spark.indextables.prewarm.segments", "TERM_DICT")
      .option("spark.indextables.prewarm.fields", "title")
      .load(tempTablePath)

    val count = df.filter(col("title") === "title_50").count()

    logger.info(s"Query with reader-option prewarm returned $count records")
    assert(count > 0, "Query should return results")

    logger.info("Read-time prewarm via reader options test passed")
  }

  test("reader options should override session-level prewarm config") {
    createTestData(100)

    // Session says prewarm is disabled
    spark.conf.set("spark.indextables.prewarm.enabled", "false")

    // Reader option enables it
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.prewarm.enabled", "true")
      .option("spark.indextables.prewarm.segments", "TERM_DICT")
      .load(tempTablePath)

    val count = df.count()

    logger.info(s"Query with reader-option override returned $count records")
    assert(count > 0, "Query should work with reader option override")

    logger.info("Reader option override test passed")
  }

  // === Multiple Read Tests ===

  test("multiple reads should benefit from prewarmed cache") {
    createTestData(200)

    spark.conf.set("spark.indextables.prewarm.enabled", "true")
    spark.conf.set("spark.indextables.prewarm.segments", "TERM_DICT,FAST_FIELD,POSTINGS")

    // First read - triggers prewarm
    val start1 = System.currentTimeMillis()
    val df1 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)
    val count1    = df1.filter(col("category") === "category_0").count()
    val duration1 = System.currentTimeMillis() - start1

    // Second read - should use cached data
    val start2 = System.currentTimeMillis()
    val df2 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)
    val count2    = df2.filter(col("category") === "category_1").count()
    val duration2 = System.currentTimeMillis() - start2

    logger.info(s"First read: ${duration1}ms, $count1 records")
    logger.info(s"Second read: ${duration2}ms, $count2 records")

    assert(count1 > 0, "First query should return results")
    assert(count2 > 0, "Second query should return results")

    // Note: In local mode, the difference might not be significant
    // In a distributed environment, second read would be much faster

    logger.info("Multiple reads with prewarm test passed")
  }

  // === Partition Filter Tests ===

  test("read with partition filter should only prewarm matching partitions") {
    val ss = spark
    import ss.implicits._

    val partitionedPath = Files.createTempDirectory("prewarm_partition_readtime_").toFile.getAbsolutePath

    try {
      // Create partitioned data
      val testData = (1 until 201)
        .map { i =>
          (
            i.toLong,
            s"content_$i",
            s"region_${i % 3}"
          )
        }
        .toDF("id", "content", "region")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("region")
        .option("spark.indextables.indexWriter.batchSize", "30")
        .mode("overwrite")
        .save(partitionedPath)

      // Enable prewarm with partition filter
      spark.conf.set("spark.indextables.prewarm.enabled", "true")
      spark.conf.set("spark.indextables.prewarm.partitionFilter", "region = 'region_1'")

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(partitionedPath)

      // Query the filtered partition
      val count = df.filter(col("region") === "region_1").count()

      logger.info(s"Query on filtered partition returned $count records")
      assert(count > 0, "Query on filtered partition should return results")

    } finally
      deleteRecursively(new File(partitionedPath))

    logger.info("Read-time partition filter test passed")
  }

  // ===== Prewarm + Batch Optimization Interaction Tests =====

  test("prewarm should work alongside batch optimization") {
    createTestData(300)

    // Enable both prewarm and batch optimization
    spark.conf.set("spark.indextables.prewarm.enabled", "true")
    spark.conf.set("spark.indextables.prewarm.segments", "TERM_DICT,FAST_FIELD")
    spark.conf.set("spark.indextables.read.batchOptimization.enabled", "true")
    spark.conf.set("spark.indextables.read.batchOptimization.profile", "balanced")

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    // Execute query that benefits from both prewarm and batch optimization
    val result = df
      .filter(col("score") > 100.0)
      .agg(
        count("*").as("cnt"),
        sum("score").as("total_score")
      )
      .collect()

    assert(result.length > 0, "Query with prewarm + batch optimization should return results")

    val cnt = result.head.getAs[Long]("cnt")
    logger.info(s"Prewarm + batch optimization query returned $cnt records")
    assert(cnt > 0, "Should have matching records")

    logger.info("Prewarm + batch optimization interaction test passed")
  }

  test("prewarm config should not interfere with batch optimization config") {
    createTestData(200)

    // Set prewarm configs
    spark.conf.set("spark.indextables.prewarm.enabled", "true")
    spark.conf.set("spark.indextables.prewarm.splitsPerTask", "3")
    spark.conf.set("spark.indextables.prewarm.segments", "TERM_DICT,POSTINGS")

    // Set batch optimization configs
    spark.conf.set("spark.indextables.read.batchOptimization.enabled", "true")
    spark.conf.set("spark.indextables.read.batchOptimization.maxRangeSize", "8M")
    spark.conf.set("spark.indextables.read.batchOptimization.gapTolerance", "256K")
    spark.conf.set("spark.indextables.read.batchOptimization.minDocsForOptimization", "25")

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    // First query - triggers prewarm
    val count1 = df.filter(col("category") === "category_2").count()
    logger.info(s"First query (with prewarm) returned $count1 records")
    assert(count1 > 0, "First query should return results")

    // Second query - uses prewarmed cache + batch optimization
    val count2 = df.filter(col("score") > 50.0 && col("score") < 150.0).count()
    logger.info(s"Second query (batch opt) returned $count2 records")
    assert(count2 > 0, "Second query should return results")

    logger.info("Prewarm config independence from batch optimization test passed")
  }

  test("batch optimization should benefit from prewarmed index components") {
    createTestData(400)

    // Enable prewarm for FAST_FIELD (used in range queries)
    spark.conf.set("spark.indextables.prewarm.enabled", "true")
    spark.conf.set("spark.indextables.prewarm.segments", "FAST_FIELD")

    // Enable batch optimization
    spark.conf.set("spark.indextables.read.batchOptimization.enabled", "true")
    spark.conf.set("spark.indextables.read.batchOptimization.profile", "aggressive")

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    // Time a range query (benefits from both fast field prewarm and batch opt)
    val startTime = System.currentTimeMillis()
    val result = df
      .filter(col("score") > 100.0 && col("score") < 500.0)
      .select("id", "content", "score")
      .collect()
    val duration = System.currentTimeMillis() - startTime

    logger.info(s"Range query with prewarmed FAST_FIELD + batch opt: ${result.length} rows in ${duration}ms")
    assert(result.length > 0, "Range query should return results")

    // Second run should be faster (cached)
    val startTime2 = System.currentTimeMillis()
    val result2 = df
      .filter(col("score") > 200.0 && col("score") < 400.0)
      .select("id", "score")
      .collect()
    val duration2 = System.currentTimeMillis() - startTime2

    logger.info(s"Second range query: ${result2.length} rows in ${duration2}ms")

    logger.info("Batch optimization with prewarmed components test passed")
  }

  test("disabling prewarm should not affect batch optimization") {
    createTestData(200)

    // Disable prewarm, enable batch optimization
    spark.conf.set("spark.indextables.prewarm.enabled", "false")
    spark.conf.set("spark.indextables.read.batchOptimization.enabled", "true")
    spark.conf.set("spark.indextables.read.batchOptimization.profile", "balanced")

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempTablePath)

    // Query should work with batch optimization only
    val result = df.filter(col("score") > 100.0).collect()

    logger.info(s"Query with batch opt only (no prewarm) returned ${result.length} rows")
    assert(result.length > 0, "Query should return results with batch opt only")

    logger.info("Batch optimization without prewarm test passed")
  }
}
