package io.indextables.spark.debug

import java.time.LocalDateTime

import io.indextables.tantivy4java.core._
import io.indextables.tantivy4java.split._
import io.indextables.tantivy4java.split.merge._
import org.scalatest.funsuite.AnyFunSuite

/**
 * Direct test using tantivy4java API to isolate the issue. This bypasses all Spark/IndexTables4Spark code and tests the
 * split query directly.
 */
class DirectTantivy4JavaTimestampTest extends AnyFunSuite {

  test("Direct tantivy4java split timestamp query test") {
    val indexPath = java.nio.file.Files.createTempDirectory("direct_test_index").toString
    val splitPath = java.nio.file.Files.createTempDirectory("direct_test").toString + "/test.split"

    try {
      println("=== Direct tantivy4java Timestamp Test ===")

      // Create schema
      val builder = new SchemaBuilder()
      builder.addIntegerField("id", true, true, true)
      builder.addDateField("ts", true, true, true) // stored, indexed, FAST
      val schema = builder.build()

      // Create index
      val index  = new Index(schema, indexPath, false)
      val writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)

      // Write document with timestamp
      val doc = new Document()
      doc.addInteger("id", 1)
      val dt = LocalDateTime.of(2025, 11, 7, 5, 0, 0)
      doc.addDate("ts", dt)
      writer.addDocument(doc)
      doc.close()

      writer.commit()
      writer.close()

      println(s"✓ Written 1 document with timestamp: $dt")

      // Create split
      val config   = new QuickwitSplit.SplitConfig("test-split", "test-source", "test-node")
      val metadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config)
      println(s"✓ Created split with ${metadata.getNumDocs} documents")

      // Create searcher
      val cacheConfig  = new SplitCacheManager.CacheConfig("direct-test-cache")
      val cacheManager = SplitCacheManager.getInstance(cacheConfig)
      val searcher     = cacheManager.createSplitSearcher("file://" + splitPath, metadata)

      println("\n=== Testing Query ===")

      // Test query using parseQuery with RFC3339
      val queryString = "ts:[2025-11-07T05:00:00Z TO 2025-11-07T05:00:01Z}"
      println(s"Query: $queryString")

      val query  = searcher.parseQuery(queryString)
      val result = searcher.search(query, 10)

      val hitCount = result.getHits.size()
      println(s"Hits: $hitCount")

      result.close()
      searcher.close()
      schema.close()
      index.close()

      assert(hitCount == 1, s"Expected 1 hit, got $hitCount")
      println("✅ Direct tantivy4java test PASSED!")

    } finally {
      // Cleanup
      import java.nio.file.{Files, Paths}
      import scala.util.Try
      Try {
        Files
          .walk(Paths.get(indexPath))
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(p => Files.deleteIfExists(p))
      }
      Try {
        val splitDir = Paths.get(splitPath).getParent
        Files
          .walk(splitDir)
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(p => Files.deleteIfExists(p))
      }
    }
  }
}
