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

import java.io.File
import java.nio.file.Files

import scala.util.Random

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession

import io.indextables.tantivy4java.aggregation.{
  AverageAggregation,
  AverageResult,
  CountAggregation,
  CountResult,
  MaxAggregation,
  MaxResult,
  MinAggregation,
  MinResult,
  StatsAggregation,
  StatsResult,
  SumAggregation,
  SumResult
}
import io.indextables.tantivy4java.batch.{BatchDocument, BatchDocumentBuilder}
import io.indextables.tantivy4java.core.{Document, Index, IndexWriter, Schema, SchemaBuilder}
import io.indextables.tantivy4java.split.{SplitAggregation, SplitCacheManager, SplitMatchAllQuery, SplitSearcher}
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import org.scalatest.funsuite.AnyFunSuite

/**
 * Integration test that actually creates tantivy4java indexes and executes aggregations. This validates the complete
 * end-to-end aggregation pushdown functionality.
 */
class AggregatePushdownIntegrationTest extends AnyFunSuite {

  test("tantivy4java COUNT aggregation end-to-end") {
    val tempDir = Files.createTempDirectory("tantivy-agg-test").toFile

    try {
      // Create a test index with some data
      val (indexPath, documentCount, splitMetadata) = createTestIndex(tempDir, numDocs = 100)

      // Test COUNT aggregation using tantivy4java directly
      val cacheConfig = new SplitCacheManager.CacheConfig(s"test-cache-count-${System.currentTimeMillis()}")
        .withMaxCacheSize(50000000) // 50MB
      val cacheManager = SplitCacheManager.getInstance(cacheConfig)

      println(s"🔧 Creating searcher for: file://$indexPath")
      val searcher = cacheManager.createSplitSearcher(s"file://$indexPath", splitMetadata)
      println(s"🔧 Searcher created successfully")

      // Create COUNT aggregation using value field (configured as fast field, as per new API)
      val countAgg     = new CountAggregation("value")
      val aggregations = new java.util.HashMap[String, SplitAggregation]()
      aggregations.put("doc_count", countAgg)

      // Execute aggregation-only search
      val query  = new SplitMatchAllQuery()
      val result = searcher.aggregate(query, aggregations)

      // Verify results
      assert(result.hasAggregations(), "Should have aggregation results")
      val countResult = result.getAggregation("doc_count").asInstanceOf[CountResult]
      assert(countResult.getCount() == documentCount, s"Count should be $documentCount, got ${countResult.getCount()}")

      println(s"✅ COUNT aggregation: Expected $documentCount, Got ${countResult.getCount()}")

    } finally
      // Clean up
      deleteRecursively(tempDir)
  }

  test("tantivy4java SUM aggregation end-to-end") {
    val tempDir = Files.createTempDirectory("tantivy-agg-test").toFile

    try {
      // Create a test index with some data
      val (indexPath, documentCount, splitMetadata) = createTestIndex(tempDir, numDocs = 50)

      val cacheConfig =
        new SplitCacheManager.CacheConfig(s"test-cache-${System.currentTimeMillis()}-${util.Random.nextInt()}")
          .withMaxCacheSize(50000000)
      val cacheManager = SplitCacheManager.getInstance(cacheConfig)

      println(s"🔧 Creating searcher for: file://$indexPath")
      val searcher = cacheManager.createSplitSearcher(s"file://$indexPath", splitMetadata)
      println(s"🔧 Searcher created successfully")

      // Create SUM aggregation on score field
      val sumAgg       = new SumAggregation("score_sum", "score")
      val aggregations = new java.util.HashMap[String, SplitAggregation]()
      aggregations.put("score_sum", sumAgg)

      // Execute aggregation
      val query  = new SplitMatchAllQuery()
      val result = searcher.aggregate(query, aggregations)

      // Verify results
      assert(result.hasAggregations(), "Should have aggregation results")
      val sumResult = result.getAggregation("score_sum").asInstanceOf[SumResult]
      val actualSum = sumResult.getSum()

      // Expected sum: documents have scores 1, 2, 3, ..., 50
      // Sum = 50 * 51 / 2 = 1275
      val expectedSum = (1 to documentCount).sum.toDouble
      assert(actualSum == expectedSum, s"Sum should be $expectedSum, got $actualSum")

      println(s"✅ SUM aggregation: Expected $expectedSum, Got $actualSum")

    } finally
      deleteRecursively(tempDir)
  }

  test("tantivy4java AVG aggregation end-to-end") {
    val tempDir = Files.createTempDirectory("tantivy-agg-test").toFile

    try {
      val (indexPath, documentCount, splitMetadata) = createTestIndex(tempDir, numDocs = 10)

      val cacheConfig =
        new SplitCacheManager.CacheConfig(s"test-cache-${System.currentTimeMillis()}-${util.Random.nextInt()}")
          .withMaxCacheSize(50000000)
      val cacheManager = SplitCacheManager.getInstance(cacheConfig)

      println(s"🔧 Creating searcher for: file://$indexPath")
      val searcher = cacheManager.createSplitSearcher(s"file://$indexPath", splitMetadata)
      println(s"🔧 Searcher created successfully")

      // Create AVG aggregation on score field
      val avgAgg       = new AverageAggregation("score_avg", "score")
      val aggregations = new java.util.HashMap[String, SplitAggregation]()
      aggregations.put("score_avg", avgAgg)

      // Execute aggregation
      val query  = new SplitMatchAllQuery()
      val result = searcher.aggregate(query, aggregations)

      // Verify results
      assert(result.hasAggregations(), "Should have aggregation results")
      val avgResult = result.getAggregation("score_avg").asInstanceOf[AverageResult]
      val actualAvg = avgResult.getAverage()

      // Expected average: (1 + 2 + ... + 10) / 10 = 55 / 10 = 5.5
      val expectedAvg = (1 to documentCount).sum.toDouble / documentCount
      assert(math.abs(actualAvg - expectedAvg) < 0.001, s"Average should be $expectedAvg, got $actualAvg")

      println(s"✅ AVG aggregation: Expected $expectedAvg, Got $actualAvg")

    } finally
      deleteRecursively(tempDir)
  }

  test("tantivy4java MIN/MAX aggregation end-to-end") {
    val tempDir = Files.createTempDirectory("tantivy-agg-test").toFile

    try {
      val (indexPath, documentCount, splitMetadata) = createTestIndex(tempDir, numDocs = 20)

      val cacheConfig =
        new SplitCacheManager.CacheConfig(s"test-cache-${System.currentTimeMillis()}-${util.Random.nextInt()}")
          .withMaxCacheSize(50000000)
      val cacheManager = SplitCacheManager.getInstance(cacheConfig)

      println(s"🔧 Creating searcher for: file://$indexPath")
      val searcher = cacheManager.createSplitSearcher(s"file://$indexPath", splitMetadata)
      println(s"🔧 Searcher created successfully")

      // Create MIN and MAX aggregations
      val minAgg       = new MinAggregation("score_min", "score")
      val maxAgg       = new MaxAggregation("score_max", "score")
      val aggregations = new java.util.HashMap[String, SplitAggregation]()
      aggregations.put("score_min", minAgg)
      aggregations.put("score_max", maxAgg)

      // Execute aggregation
      val query  = new SplitMatchAllQuery()
      val result = searcher.aggregate(query, aggregations)

      // Verify results
      assert(result.hasAggregations(), "Should have aggregation results")
      val minResult = result.getAggregation("score_min").asInstanceOf[MinResult]
      val maxResult = result.getAggregation("score_max").asInstanceOf[MaxResult]

      val actualMin = minResult.getMin()
      val actualMax = maxResult.getMax()

      // Expected: min = 1, max = 20
      assert(actualMin == 1.0, s"Min should be 1.0, got $actualMin")
      assert(actualMax == documentCount.toDouble, s"Max should be $documentCount, got $actualMax")

      println(s"✅ MIN aggregation: Expected 1.0, Got $actualMin")
      println(s"✅ MAX aggregation: Expected $documentCount, Got $actualMax")

    } finally
      deleteRecursively(tempDir)
  }

  test("tantivy4java STATS aggregation (all-in-one) end-to-end") {
    val tempDir = Files.createTempDirectory("tantivy-agg-test").toFile

    try {
      val (indexPath, documentCount, splitMetadata) = createTestIndex(tempDir, numDocs = 5)

      val cacheConfig =
        new SplitCacheManager.CacheConfig(s"test-cache-${System.currentTimeMillis()}-${util.Random.nextInt()}")
          .withMaxCacheSize(50000000)
      val cacheManager = SplitCacheManager.getInstance(cacheConfig)

      println(s"🔧 Creating searcher for: file://$indexPath")
      val searcher = cacheManager.createSplitSearcher(s"file://$indexPath", splitMetadata)
      println(s"🔧 Searcher created successfully")

      // Create STATS aggregation (computes all metrics at once)
      val statsAgg     = new StatsAggregation("score_stats", "score")
      val aggregations = new java.util.HashMap[String, SplitAggregation]()
      aggregations.put("score_stats", statsAgg)

      // Execute aggregation
      val query  = new SplitMatchAllQuery()
      val result = searcher.aggregate(query, aggregations)

      // Verify results
      assert(result.hasAggregations(), "Should have aggregation results")
      val statsResult = result.getAggregation("score_stats").asInstanceOf[StatsResult]

      val count = statsResult.getCount()
      val sum   = statsResult.getSum()
      val avg   = statsResult.getAverage()
      val min   = statsResult.getMin()
      val max   = statsResult.getMax()

      // Expected values for scores 1, 2, 3, 4, 5
      assert(count == documentCount, s"Count should be $documentCount, got $count")
      assert(sum == 15.0, s"Sum should be 15.0, got $sum")
      assert(avg == 3.0, s"Average should be 3.0, got $avg")
      assert(min == 1.0, s"Min should be 1.0, got $min")
      assert(max == 5.0, s"Max should be 5.0, got $max")

      println(s"✅ STATS aggregation:")
      println(s"   Count: Expected $documentCount, Got $count")
      println(s"   Sum: Expected 15.0, Got $sum")
      println(s"   Avg: Expected 3.0, Got $avg")
      println(s"   Min: Expected 1.0, Got $min")
      println(s"   Max: Expected 5.0, Got $max")

    } finally
      deleteRecursively(tempDir)
  }

  test("tantivy4java multiple aggregations in single query") {
    val tempDir = Files.createTempDirectory("tantivy-agg-test").toFile

    try {
      val (indexPath, documentCount, splitMetadata) = createTestIndex(tempDir, numDocs = 10)

      val cacheConfig =
        new SplitCacheManager.CacheConfig(s"test-cache-${System.currentTimeMillis()}-${util.Random.nextInt()}")
          .withMaxCacheSize(50000000)
      val cacheManager = SplitCacheManager.getInstance(cacheConfig)

      println(s"🔧 Creating searcher for: file://$indexPath")
      val searcher = cacheManager.createSplitSearcher(s"file://$indexPath", splitMetadata)
      println(s"🔧 Searcher created successfully")

      // Create multiple aggregations in one query (matching reference test pattern)
      val aggregations = new java.util.HashMap[String, SplitAggregation]()
      aggregations.put("doc_count", new CountAggregation("value")) // Count using value field
      aggregations.put("score_sum", new SumAggregation("score"))   // Sum using field name only

      // Execute all aggregations in single query using search method with Map
      val query  = new SplitMatchAllQuery()
      val result = searcher.search(query, 10, aggregations)

      // Verify results (simplified for 2 aggregations)
      assert(result.hasAggregations(), "Should have aggregation results")

      val countResult = result.getAggregation("doc_count").asInstanceOf[CountResult]
      val sumResult   = result.getAggregation("score_sum").asInstanceOf[SumResult]

      assert(countResult.getCount() == documentCount)
      assert(sumResult.getSum() == (1 to documentCount).sum.toDouble)

      println(s"✅ Multiple aggregations in single query (COUNT + SUM) - all passed")

    } finally
      deleteRecursively(tempDir)
  }

  test("auto-fast-field configuration with Spark DataSource") {
    val spark = SparkSession
      .builder()
      .appName("AutoFastFieldTest")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._

      // Create test data with numeric fields
      val testData = Seq(
        ("doc1", "content1", 10, 100L),
        ("doc2", "content2", 20, 200L),
        ("doc3", "content3", 30, 300L)
      ).toDF("id", "content", "score", "value")

      val tempDir   = Files.createTempDirectory("auto-fast-field-test").toFile
      val tablePath = tempDir.getAbsolutePath

      // Write data WITHOUT explicit fast field configuration
      // This should trigger auto-fast-field configuration for the first numeric field (score)
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(tablePath)

      println(s"✅ Auto-fast-field test: Data written to $tablePath without explicit fast field config")

      // Read back and verify we can perform aggregations (which proves fast fields were auto-configured)
      val readData = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // If auto-fast-field worked, this should succeed without errors
      val count = readData.count()
      assert(count == 3, s"Should have 3 rows, got $count")

      println(s"✅ Auto-fast-field test: Successfully read data and performed count aggregation")

      // Clean up
      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }

  /** Create a test tantivy index with sample data. Returns (indexPath, documentCount, splitMetadata). */
  private def createTestIndex(baseDir: File, numDocs: Int): (String, Int, QuickwitSplit.SplitMetadata) = {
    // Create unique identifiers to avoid cache conflicts between tests
    val uniqueId = s"agg_test_${System.nanoTime()}_${util.Random.nextInt(10000)}"
    val indexDir = new File(baseDir, s"${uniqueId}_index")
    indexDir.mkdirs()

    // Create schema using correct API matching tantivy4java reference test
    // Configure numeric fields as fast fields for aggregation support
    val schema = new SchemaBuilder()
      .addTextField("content", true, false, "default", "position") // stored + indexed, not fast
      .addIntegerField("score", true, true, true)                  // stored + indexed + fast (like reference test)
      .addIntegerField("value", true, true, true)                  // stored + indexed + fast
      .build()

    // Create index and writer
    val index  = new Index(schema, indexDir.getAbsolutePath)
    val writer = index.writer(100000000, 1) // 100MB heap, 1 thread

    // Add documents (id field is implicit) - matching reference test pattern
    println(s"🔧 Creating index with $numDocs documents")
    for (i <- 1 to numDocs) {
      val doc = new Document()
      doc.addText("content", s"content for document $i")
      doc.addInteger("score", i.toLong)       // score: 1, 2, 3, ..., numDocs (like reference test)
      doc.addInteger("value", (i + 9).toLong) // value: 10, 11, 12, ..., numDocs + 9

      writer.addDocument(doc)
    }
    println(s"🔧 Added $numDocs documents to index")

    // Commit and close
    writer.commit()
    writer.close()
    index.close()

    // Create split from index using unique naming to avoid cache conflicts
    val splitDir = new File(baseDir, "splits")
    splitDir.mkdirs()

    val splitPath = new File(splitDir, s"$uniqueId.split").getAbsolutePath
    val splitConfig = new QuickwitSplit.SplitConfig(
      uniqueId, // Use the same unique ID for consistency
      "test-source",
      "test-node"
    )

    val splitMetadata = QuickwitSplit.convertIndexFromPath(indexDir.getAbsolutePath, splitPath, splitConfig)

    println(s"🔧 Created split: $splitPath with $numDocs documents")
    println(s"🔧 Split metadata: hasFooterOffsets=${splitMetadata.hasFooterOffsets()}")

    (splitPath, numDocs, splitMetadata)
  }

  /** Recursively delete a directory and all its contents. */
  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}
