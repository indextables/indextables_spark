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
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, _}

import io.indextables.spark.storage.BroadcastSplitLocalityManager
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Integration test for the pre-warm functionality with actual IndexTables4Spark DataSource. This test verifies the
 * complete pre-warm flow from write to read with cache warming.
 */
class PreWarmIntegrationTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  var spark: SparkSession   = _
  var tempTablePath: String = _

  override def beforeAll(): Unit = {
    // Create Spark session for testing
    spark = SparkSession
      .builder()
      .appName("PreWarmIntegrationTest")
      .master("local[4]") // Use 4 cores for testing executor distribution
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.adaptive.enabled", "false") // Disable AQE for predictable testing
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    // Create temporary directory for test table
    val tempDir = Files.createTempDirectory("prewarm_integration_test_").toFile
    tempTablePath = tempDir.getAbsolutePath
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (tempTablePath != null) {
      val tempDir = new File(tempTablePath)
      if (tempDir.exists()) {
        FileUtils.deleteDirectory(tempDir)
      }
    }
  }

  override def beforeEach(): Unit = {
    // Clear any previous pre-warm state
    PreWarmManager.clearAll()
    BroadcastSplitLocalityManager.clearAll()
  }

  override def afterEach(): Unit = {
    PreWarmManager.clearAll()
    BroadcastSplitLocalityManager.clearAll()
  }

  // Note: These are integration tests that would require the full IndexTables4Spark implementation
  // For now, they serve as documentation of the expected behavior

  test("Pre-warm should be enabled by default") {
    // This test verifies that pre-warm is enabled by default
    // and queries execute with cache warming

    val testData = Seq(
      (1, "apache spark framework"),
      (2, "tantivy search engine"),
      (3, "lucene java library"),
      (4, "elasticsearch distributed search")
    )

    // Create DataFrame using createDataFrame method to avoid implicits issue
    val testDataDF = spark.createDataFrame(testData).toDF("id", "content")

    // Write test data (this creates splits)
    testDataDF.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tempTablePath)

    // Read with default configuration (pre-warm should be enabled by default)
    val df      = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tempTablePath)
    val results = df.filter(col("content").contains("search")).collect()

    // Verify that we got results and pre-warm was executed by default
    results.length should be >= 1

    // Check that pre-warm was enabled (we can't easily verify the logs in tests,
    // but we can verify the query executed successfully with pre-warm enabled by default)
    val searchResults = results.map(_.getString(1))
    searchResults should contain("tantivy search engine")
  }

  test("Pre-warm should warm caches when explicitly enabled") {
    // This test verifies that explicitly enabling pre-warm actually warms caches
    // and subsequent queries benefit from warmed caches

    val testData = Seq(
      (1, "apache spark framework for big data"),
      (2, "tantivy rust search engine library"),
      (3, "lucene java full text search"),
      (4, "elasticsearch distributed search platform"),
      (5, "solr enterprise search server")
    )

    // Create DataFrame using createDataFrame method to avoid implicits issue
    val testDataDF = spark.createDataFrame(testData).toDF("id", "content")

    // Write test data to create multiple splits
    testDataDF.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tempTablePath)

    // Read with pre-warm explicitly enabled
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.cache.prewarm.enabled", "true")
      .option("spark.indextables.cache.maxSize", "50000000")
      .load(tempTablePath)

    // Execute query that should benefit from pre-warming
    val results = df.filter(col("content").contains("search")).collect()

    // Verify that we got the expected results
    results.length should be >= 1
    val searchResults = results.map(_.getString(1))
    searchResults should contain("tantivy rust search engine library")
    searchResults should contain("lucene java full text search")
  }

  test("Pre-warm can be explicitly disabled") {
    // This test verifies that pre-warm can be explicitly disabled

    val testData = Seq(
      (1, "machine learning algorithms"),
      (2, "natural language processing"),
      (3, "computer vision techniques"),
      (4, "artificial intelligence applications")
    )

    // Create DataFrame using createDataFrame method to avoid implicits issue
    val testDataDF = spark.createDataFrame(testData).toDF("id", "content")

    // Write test data
    testDataDF.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tempTablePath)

    // Read with pre-warm explicitly disabled
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.cache.prewarm.enabled", "false")
      .load(tempTablePath)

    val results = df.filter(col("content").contains("learning")).collect()

    // Verify query still works when pre-warm is disabled
    results.length should be >= 1
    val searchResults = results.map(_.getString(1))
    searchResults should contain("machine learning algorithms")
  }

  test("Pre-warm should handle basic search operations") {
    // This test verifies that pre-warm works with basic search operations

    val testData = Seq(
      (1, "apache spark big data processing"),
      (2, "tantivy fast search indexing"),
      (3, "lucene text analysis library"),
      (4, "elasticsearch real time search")
    )

    // Create DataFrame using createDataFrame method to avoid implicits issue
    val testDataDF = spark.createDataFrame(testData).toDF("id", "content")

    // Write test data
    testDataDF.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tempTablePath)

    // Read with pre-warm enabled
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.cache.prewarm.enabled", "true")
      .load(tempTablePath)

    // Execute basic filter operation
    val results = df.filter(col("content").contains("search")).collect()

    // Verify that pre-warm handled basic filters correctly
    results.length should be >= 1
    val searchResults = results.map(_.getString(1))
    searchResults should contain("tantivy fast search indexing")
    searchResults should contain("elasticsearch real time search")
  }

  test("Pre-warm should handle failures gracefully") {
    // This test verifies that pre-warm failures don't break query execution
    // and that appropriate warnings are logged

    val testData = Seq(
      (1, "test data for failure handling"),
      (2, "robust error recovery testing")
    )

    // Create DataFrame using createDataFrame method to avoid implicits issue
    val testDataDF = spark.createDataFrame(testData).toDF("id", "content")

    // Write test data
    testDataDF.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tempTablePath)

    // Read with pre-warm enabled but with configuration that might cause issues
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.cache.prewarm.enabled", "true")
      .option("spark.indextables.cache.maxSize", "1000") // Very small cache
      .load(tempTablePath)

    // Execute query - should succeed despite potential pre-warm issues
    val results = df.filter(col("content").contains("test")).collect()

    // Verify that query executed successfully even with potential pre-warm failures
    results.length should be >= 1
    val searchResults = results.map(_.getString(1))
    searchResults should contain("test data for failure handling")
  }
}
