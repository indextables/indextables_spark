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

import io.indextables.tantivy4java.aggregation.StatsAggregation
import io.indextables.tantivy4java.aggregation.StatsResult
import io.indextables.tantivy4java.core.{Document, Index, SchemaBuilder}
import io.indextables.tantivy4java.split.{SplitAggregation, SplitCacheManager, SplitMatchAllQuery, SplitTermQuery}
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import org.scalatest.funsuite.AnyFunSuite

/**
 * Reproduction test for tantivy4java 0.25.3 bug: searcher.aggregate(query, aggregations) does NOT filter aggregations
 * by query.
 *
 * Expected: StatsAggregation should only aggregate over documents matching the query Actual: StatsAggregation
 * aggregates over ALL documents, ignoring the query filter
 */
class FilteredAggregationBugTest extends AnyFunSuite {

  test("BUG: Filtered StatsAggregation ignores query filter") {
    val tempDir = Files.createTempDirectory("tantivy-filtered-agg-bug").toFile

    try {
      println("\n=== tantivy4java 0.25.3 Filtered Aggregation Bug Reproduction ===\n")

      // Step 1: Create index with 4 documents (2 with batch=batch1, 2 with batch=batch2)
      val (splitPath, splitMetadata) = createTestIndex(tempDir)

      // Step 2: Test unfiltered aggregation (baseline - should work)
      println("Step 1: Testing UNFILTERED aggregation (baseline)...")
      testUnfilteredAggregation(splitPath, splitMetadata)

      // Step 3: Test filtered aggregation (demonstrates the bug)
      println("\nStep 2: Testing FILTERED aggregation (demonstrates bug)...")
      testFilteredAggregation(splitPath, splitMetadata)

    } finally {
      deleteRecursively(tempDir)
      println("\n✓ Cleanup completed")
    }
  }

  private def createTestIndex(baseDir: File): (String, QuickwitSplit.SplitMetadata) = {
    val uniqueId = s"filtered_agg_bug_${System.nanoTime()}"
    val indexDir = new File(baseDir, s"${uniqueId}_index")
    indexDir.mkdirs()

    println("Creating index with 4 documents:")
    println("  - 2 docs with batch=batch1 (value=10, value=20)")
    println("  - 2 docs with batch=batch2 (value=30, value=40)")
    println("  - Total: sum of all values = 100")

    // Create schema with string field "batch" and numeric fast field "value"
    val schema = new SchemaBuilder()
      .addTextField("batch", true, false, "default", "position") // String field for filtering
      .addIntegerField("value", true, true, true)                // Numeric fast field for aggregation
      .build()

    val index  = new Index(schema, indexDir.getAbsolutePath)
    val writer = index.writer(100000000, 1)

    // Add 2 documents with batch="batch1"
    val doc1 = new Document()
    doc1.addText("batch", "batch1")
    doc1.addInteger("value", 10L)
    writer.addDocument(doc1)

    val doc2 = new Document()
    doc2.addText("batch", "batch1")
    doc2.addInteger("value", 20L)
    writer.addDocument(doc2)

    // Add 2 documents with batch="batch2"
    val doc3 = new Document()
    doc3.addText("batch", "batch2")
    doc3.addInteger("value", 30L)
    writer.addDocument(doc3)

    val doc4 = new Document()
    doc4.addText("batch", "batch2")
    doc4.addInteger("value", 40L)
    writer.addDocument(doc4)

    writer.commit()
    writer.close()
    index.close()

    // Create split
    val splitDir = new File(baseDir, "splits")
    splitDir.mkdirs()
    val splitPath     = new File(splitDir, s"$uniqueId.split").getAbsolutePath
    val splitConfig   = new QuickwitSplit.SplitConfig(uniqueId, "test-source", "test-node")
    val splitMetadata = QuickwitSplit.convertIndexFromPath(indexDir.getAbsolutePath, splitPath, splitConfig)

    println(s"✓ Created split: $splitPath\n")

    (splitPath, splitMetadata)
  }

  private def testUnfilteredAggregation(splitPath: String, splitMetadata: QuickwitSplit.SplitMetadata): Unit = {
    val cacheConfig = new SplitCacheManager.CacheConfig(s"test-unfiltered-${System.currentTimeMillis()}")
      .withMaxCacheSize(50000000)
    val cacheManager = SplitCacheManager.getInstance(cacheConfig)
    val searcher     = cacheManager.createSplitSearcher(s"file://$splitPath", splitMetadata)

    try {
      // Create match-all query (no filtering)
      val query = new SplitMatchAllQuery()

      // Create StatsAggregation on "value" field
      val aggregations = new java.util.HashMap[String, SplitAggregation]()
      aggregations.put("value_stats", new StatsAggregation("value_stats", "value"))

      // Execute aggregation
      val result = searcher.aggregate(query, aggregations)
      val stats  = result.getAggregation("value_stats").asInstanceOf[StatsResult]

      println(s"  Query: match_all (no filter)")
      println(s"  Count: ${stats.getCount()} (expected: 4)")
      println(s"  Sum:   ${stats.getSum()} (expected: 100.0)")
      println(s"  Min:   ${stats.getMin()} (expected: 10.0)")
      println(s"  Max:   ${stats.getMax()} (expected: 40.0)")

      val correct = stats.getCount() == 4 &&
        stats.getSum() == 100.0 &&
        stats.getMin() == 10.0 &&
        stats.getMax() == 40.0

      if (correct) {
        println("  ✅ UNFILTERED aggregation works correctly!")
      } else {
        println("  ❌ UNFILTERED aggregation is broken!")
        fail("Unfiltered aggregation should work correctly")
      }

    } finally
      searcher.close()
  }

  private def testFilteredAggregation(splitPath: String, splitMetadata: QuickwitSplit.SplitMetadata): Unit = {
    val cacheConfig = new SplitCacheManager.CacheConfig(s"test-filtered-${System.currentTimeMillis()}")
      .withMaxCacheSize(50000000)
    val cacheManager = SplitCacheManager.getInstance(cacheConfig)
    val searcher     = cacheManager.createSplitSearcher(s"file://$splitPath", splitMetadata)

    try {
      // Create query that filters to batch="batch1"
      val query = new SplitTermQuery("batch", "batch1")

      // Create StatsAggregation on "value" field
      val aggregations = new java.util.HashMap[String, SplitAggregation]()
      aggregations.put("value_stats", new StatsAggregation("value_stats", "value"))

      // Execute aggregation with filter
      val result = searcher.aggregate(query, aggregations)
      val stats  = result.getAggregation("value_stats").asInstanceOf[StatsResult]

      println(s"  Query: batch='batch1' (filter to 2 documents)")
      println(s"  Count: ${stats.getCount()} (expected: 2)")
      println(s"  Sum:   ${stats.getSum()} (expected: 30.0 = 10 + 20)")
      println(s"  Min:   ${stats.getMin()} (expected: 10.0)")
      println(s"  Max:   ${stats.getMax()} (expected: 20.0)")

      // Check for bug
      val hasBug = stats.getCount() == 4 || // Count all docs instead of filtered
        stats.getSum() == 100.0 || // Sum all values instead of filtered
        stats.getMax() == 40.0     // Max from all docs instead of filtered

      if (hasBug) {
        println("\n  ❌ BUG CONFIRMED: Aggregation is NOT filtered by query!")
        println("     The aggregation executed on all 4 documents instead of the 2 matching 'batch1'")
        println("     This proves that searcher.aggregate(query, aggregations) ignores the query parameter.")
        fail("Filtered aggregation should respect the query filter")
      } else {
        println("\n  ✅ No bug detected - filtered aggregation works correctly!")
      }

    } finally
      searcher.close()
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    file.delete()
  }
}
