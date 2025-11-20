/*
 * Test filtered aggregations with SplitBooleanQuery (the actual query type used in production)
 */

package io.indextables.spark.core

import java.io.File
import java.nio.file.Files

import io.indextables.tantivy4java.aggregation.StatsAggregation
import io.indextables.tantivy4java.aggregation.StatsResult
import io.indextables.tantivy4java.core.{Document, Index, SchemaBuilder}
import io.indextables.tantivy4java.split.{SplitAggregation, SplitBooleanQuery, SplitCacheManager, SplitTermQuery}
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test filtered aggregations using SplitBooleanQuery (the query type actually used in production). This tests the REAL
 * code path that's failing.
 */
class FilteredAggregationBooleanQueryTest extends AnyFunSuite {

  test("Filtered StatsAggregation with SplitBooleanQuery") {
    val tempDir = Files.createTempDirectory("tantivy-boolean-query-agg").toFile

    try {
      println("\n=== Testing Filtered Aggregation with SplitBooleanQuery ===\n")

      val (splitPath, splitMetadata) = createTestIndex(tempDir)

      println("Testing filtered aggregation with SplitBooleanQuery...")
      testFilteredAggregationWithBooleanQuery(splitPath, splitMetadata)

    } finally {
      deleteRecursively(tempDir)
      println("\n✓ Cleanup completed")
    }
  }

  private def createTestIndex(baseDir: File): (String, QuickwitSplit.SplitMetadata) = {
    val uniqueId = s"boolean_query_test_${System.nanoTime()}"
    val indexDir = new File(baseDir, s"${uniqueId}_index")
    indexDir.mkdirs()

    println("Creating index with 4 documents:")
    println("  - 2 docs with batch=batch1 (value=10, value=20)")
    println("  - 2 docs with batch=batch2 (value=30, value=40)")

    val schema = new SchemaBuilder()
      .addTextField("batch", true, false, "default", "position")
      .addIntegerField("value", true, true, true)
      .build()

    val index  = new Index(schema, indexDir.getAbsolutePath)
    val writer = index.writer(100000000, 1)

    // batch1 docs
    val doc1 = new Document()
    doc1.addText("batch", "batch1")
    doc1.addInteger("value", 10L)
    writer.addDocument(doc1)

    val doc2 = new Document()
    doc2.addText("batch", "batch1")
    doc2.addInteger("value", 20L)
    writer.addDocument(doc2)

    // batch2 docs
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

    println(s"✓ Created split\n")

    (splitPath, splitMetadata)
  }

  private def testFilteredAggregationWithBooleanQuery(splitPath: String, splitMetadata: QuickwitSplit.SplitMetadata)
    : Unit = {
    val cacheConfig = new SplitCacheManager.CacheConfig(s"test-boolean-${System.currentTimeMillis()}")
      .withMaxCacheSize(50000000)
    val cacheManager = SplitCacheManager.getInstance(cacheConfig)
    val searcher     = cacheManager.createSplitSearcher(s"file://$splitPath", splitMetadata)

    try {
      // Create a SplitBooleanQuery with two MUST clauses (matching production usage)
      // This simulates: WHERE batch IS NOT NULL AND batch = 'batch1'
      val booleanQuery = new SplitBooleanQuery()

      // Add the filter as a MUST clause
      val termQuery = new SplitTermQuery("batch", "batch1")
      booleanQuery.addMust(termQuery)

      println(s"  Query: SplitBooleanQuery with MUST clause for batch='batch1'")

      // Create StatsAggregation
      val aggregations = new java.util.HashMap[String, SplitAggregation]()
      aggregations.put("value_stats", new StatsAggregation("value_stats", "value"))

      // Execute aggregation with BooleanQuery filter
      val result = searcher.aggregate(booleanQuery, aggregations)
      val stats  = result.getAggregation("value_stats").asInstanceOf[StatsResult]

      println(s"  Count: ${stats.getCount()} (expected: 2)")
      println(s"  Sum:   ${stats.getSum()} (expected: 30.0)")
      println(s"  Min:   ${stats.getMin()} (expected: 10.0)")
      println(s"  Max:   ${stats.getMax()} (expected: 20.0)")

      // Check results
      val correct = stats.getCount() == 2 &&
        stats.getSum() == 30.0 &&
        stats.getMin() == 10.0 &&
        stats.getMax() == 20.0

      if (correct) {
        println("\n  ✅ Filtered aggregation with SplitBooleanQuery works correctly!")
      } else {
        println("\n  ❌ BUG FOUND: Filtered aggregation with SplitBooleanQuery is broken!")
        println(s"     Expected: count=2, sum=30.0, max=20.0")
        println(s"     Got: count=${stats.getCount()}, sum=${stats.getSum()}, max=${stats.getMax()}")
        fail("Filtered aggregation with SplitBooleanQuery should work")
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
