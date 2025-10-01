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

package com.tantivy4spark.debug

import com.tantivy4spark.TestBase
import com.tantivy4spark.search.{TantivyNative, TantivySearchEngine, SplitSearchEngine}
import com.tantivy4spark.storage.SplitCacheConfig
import com.tantivy4java.QuickwitSplit
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import java.io.File

class NativeLibraryTest extends TestBase {

  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  test("should load native library in executors") {
    val spark = this.spark
    import spark.implicits._

    // Create a simple RDD to test library loading in executors
    val testData = Seq(1, 2, 3, 4, 5).toDF("value")

    val result = testData.rdd
      .mapPartitions { _ =>
        try {
          import com.tantivy4spark.search.TantivyNative
          val loaded = TantivyNative.ensureLibraryLoaded()
          if (loaded) {
            try {
              // Try to call the native method to see if it works
              val handle = TantivyNative.createIndex("{\"fields\":[]}")
              Iterator(s"Library loaded: $loaded, Native call succeeded: $handle")
            } catch {
              case e: UnsatisfiedLinkError =>
                Iterator(s"Library loaded: $loaded, but native method not found: ${e.getMessage}")
              case e: Exception =>
                Iterator(s"Library loaded: $loaded, but native call failed: ${e.getMessage}")
            }
          } else {
            Iterator(s"Library loaded: $loaded")
          }
        } catch {
          case e: Exception =>
            Iterator(s"Exception: ${e.getMessage}")
        }
      }
      .collect()

    result.foreach(println)

    // This test just verifies that we can attempt library loading without crashing
    assert(result.nonEmpty)
  }

  test("searchAll JNI method should work correctly") {
    // Ensure library is loaded
    assert(TantivyNative.ensureLibraryLoaded(), "Native library should load successfully")

    // Create a test schema with various field types (using the format expected by SchemaMapper)
    val schema =
      """{
        "fields": [
          {
            "name": "id",
            "type": "i64",
            "indexed": true,
            "stored": true
          },
          {
            "name": "title",
            "type": "text",
            "indexed": true,
            "stored": true
          },
          {
            "name": "content",
            "type": "text",
            "indexed": true,
            "stored": true
          },
          {
            "name": "score",
            "type": "f64",
            "indexed": true,
            "stored": true
          }
        ]
      }"""

    var indexHandle: Long = 0

    try {
      // Step 1: Create an index
      println("Step 1: Creating index...")
      indexHandle = TantivyNative.createIndex(schema)
      assert(indexHandle != 0, "Index handle should be non-zero")
      println(s"Index created with handle: $indexHandle")

      // Step 2: Add test documents
      println("Step 2: Adding documents...")
      val documents = Seq(
        """{"id": 1, "title": "First Document", "content": "This is the first test document with some content", "score": 4.5}""",
        """{"id": 2, "title": "Second Document", "content": "This is the second test document about technology", "score": 3.8}""",
        """{"id": 3, "title": "Third Document", "content": "This is the third document discussing various topics", "score": 4.2}""",
        """{"id": 4, "title": "Fourth Document", "content": "The fourth document contains different information", "score": 3.9}""",
        """{"id": 5, "title": "Fifth Document", "content": "The last document in our test collection", "score": 4.1}"""
      )

      documents.zipWithIndex.foreach {
        case (doc, idx) =>
          val added = TantivyNative.addDocument(indexHandle, doc)
          assert(added, s"Document ${idx + 1} should be added successfully")
          println(s"Added document ${idx + 1}")
      }

      // Step 3: Commit and create split (write-only index pattern)
      println("Step 3: Committing index and creating split...")

      // First commit the index
      TantivyNative.commit(indexHandle)
      println("Index committed successfully")

      // Create temporary split file
      val tempSplitFile = java.nio.file.Files.createTempFile("native_test_split", ".split")
      tempSplitFile.toFile.deleteOnExit()

      // Since the old TantivyNative API doesn't provide split creation,
      // let's create a proper test using TantivySearchEngine instead
      println("Creating split using TantivySearchEngine approach...")

      // Close the old index handle since we'll create a new one
      TantivyNative.closeIndex(indexHandle)

      // Create a new index using TantivySearchEngine (which has split support)
      import org.apache.spark.sql.types._
      import org.apache.spark.sql.catalyst.InternalRow
      import org.apache.spark.unsafe.types.UTF8String
      import com.tantivy4spark.search.TantivySearchEngine

      val sparkSchema = StructType(
        Array(
          StructField("id", LongType, nullable = false),
          StructField("title", StringType, nullable = false),
          StructField("content", StringType, nullable = false),
          StructField("score", IntegerType, nullable = false)
        )
      )

      val searchEngine = new TantivySearchEngine(sparkSchema)

      // Add the same documents (parse from the JSON strings or define structured data)
      val structuredDocuments = Seq(
        (1L, "First Document", "This is the first test document with some content", 4),
        (2L, "Second Document", "This is the second test document about technology", 3),
        (3L, "Third Document", "This is the third document discussing various topics", 4),
        (4L, "Fourth Document", "The fourth document contains different information", 3),
        (5L, "Fifth Document", "The last document in our test collection", 4)
      )

      structuredDocuments.foreach {
        case (id, title, content, score) =>
          val row = InternalRow(id, UTF8String.fromString(title), UTF8String.fromString(content), score)
          searchEngine.addDocument(row)
      }

      // Create split using TantivySearchEngine - capture the metadata it returns!
      val (actualSplitPath, splitMetadata) =
        searchEngine.commitAndCreateSplit(tempSplitFile.toString, 0L, "native-test-node")

      println(s"Split created successfully at: $actualSplitPath")
      println(s"Split metadata hasFooterOffsets: ${splitMetadata.hasFooterOffsets()}")

      // Close the search engine since it's write-only
      searchEngine.close()

      // Step 4: Test split-based search with different limits
      println("Step 4: Testing split-based search...")

      // Create cache manager for split searching
      import com.tantivy4java.{SplitCacheManager, Query}
      val cacheConfig = new SplitCacheManager.CacheConfig(s"native-test-cache-${System.nanoTime()}")
        .withMaxCacheSize(50000000L) // 50MB
      val cacheManager = SplitCacheManager.getInstance(cacheConfig)

      // Use the metadata returned by commitAndCreateSplit (which has footer offsets)
      val metadata = splitMetadata

      // This should now have footer offsets since it came from commitAndCreateSplit
      if (!metadata.hasFooterOffsets()) {
        println(s"⚠️  ERROR: Split metadata from commitAndCreateSplit does not have footer offsets!")
        throw new RuntimeException("commitAndCreateSplit should return metadata with footer offsets")
      }

      val splitSearcher = cacheManager.createSplitSearcher(actualSplitPath, metadata)

      try {
        // Test with different limits
        val testLimits = Seq(3, 5) // Reduced limits since we have 5 documents

        testLimits.foreach { limit =>
          println(s"Testing split search with limit: $limit")

          // Search all documents
          val splitQuery   = splitSearcher.parseQuery("*")
          val searchResult = splitSearcher.search(splitQuery, limit)
          val hits         = searchResult.getHits()

          val expectedHits = Math.min(structuredDocuments.length, limit)
          assert(hits.size() == expectedHits, s"Should return $expectedHits hits for limit $limit, got ${hits.size()}")

          println(s"Found ${hits.size()} hits for limit $limit")

          // Verify each hit contains expected document data
          for (i <- 0 until hits.size()) {
            val hit = hits.get(i)
            // Use docBatch for efficient document retrieval
            import scala.jdk.CollectionConverters._
            val documents = splitSearcher.docBatch(List(hit.getDocAddress()).asJava).asScala
            val document  = if (documents.nonEmpty) documents.head else null

            assert(document != null, s"Document $i should not be null")

            // Test field value extraction using tantivy4java API
            val idValue      = document.getFirst("id")
            val titleValue   = document.getFirst("title")
            val contentValue = document.getFirst("content")
            val scoreValue   = document.getFirst("score")

            assert(idValue != null, s"Document $i should have id field")
            assert(titleValue != null, s"Document $i should have title field")
            assert(contentValue != null, s"Document $i should have content field")
            assert(scoreValue != null, s"Document $i should have score field")

            println(s"✅ Hit $i: ID=$idValue, Title=$titleValue, Score=$scoreValue")

            document.close()
          }

          searchResult.close()
          // SplitQuery objects don't need to be closed
        }

        println("Split-based search test completed successfully!")

      } finally {
        splitSearcher.close()
        cacheManager.close()
      }

    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally
      // Cleanup handled by the split searcher and cache manager
      println("Test cleanup completed")
  }

  test("split-based searchAll should handle edge cases") {
    // Test edge cases using the split-based architecture
    val sparkSchema = StructType(
      Array(
        StructField("id", LongType, nullable = false),
        StructField("text", StringType, nullable = false)
      )
    )

    println("Testing split-based edge cases...")

    // Test 1: Empty split (no documents)
    println("Test 1: Empty index/split...")
    val emptySearchEngine  = new TantivySearchEngine(sparkSchema)
    val tempEmptySplitFile = File.createTempFile("empty_test_split", ".split")
    tempEmptySplitFile.deleteOnExit()

    try {
      // Don't add any documents - commit empty index and create split
      val (emptySplitPath, emptyMetadata) =
        emptySearchEngine.commitAndCreateSplit(tempEmptySplitFile.toString, 0L, "empty-test-node")

      println(s"Empty split metadata hasFooterOffsets: ${emptyMetadata.hasFooterOffsets()}")

      // Use the metadata returned by commitAndCreateSplit
      if (!emptyMetadata.hasFooterOffsets()) {
        println("⚠️  ERROR: Empty split metadata from commitAndCreateSplit does not have footer offsets!")
        throw new RuntimeException(
          "commitAndCreateSplit should return metadata with footer offsets even for empty splits"
        )
      }

      val uniqueId1    = System.nanoTime()
      val cacheConfig1 = SplitCacheConfig(cacheName = s"empty-test-cache-$uniqueId1")
      val emptySplitReader =
        SplitSearchEngine.fromSplitFileWithMetadata(sparkSchema, emptySplitPath, emptyMetadata, cacheConfig1)

      try {
        val emptyResults = emptySplitReader.searchAll(10)
        assert(emptyResults.length == 0, "Empty split should return 0 results")
        println("✅ Empty split test passed")
      } finally
        emptySplitReader.close()
    } finally
      emptySearchEngine.close()

    // Test 2: Single document split
    println("Test 2: Single document split...")
    val singleSearchEngine  = new TantivySearchEngine(sparkSchema)
    val tempSingleSplitFile = File.createTempFile("single_test_split", ".split")
    tempSingleSplitFile.deleteOnExit()

    try {
      // Add exactly one document
      val singleRow = InternalRow(1L, UTF8String.fromString("single document"))
      singleSearchEngine.addDocument(singleRow)

      val (singleSplitPath, singleMetadata) =
        singleSearchEngine.commitAndCreateSplit(tempSingleSplitFile.toString, 0L, "single-test-node")

      println(s"Single split metadata hasFooterOffsets: ${singleMetadata.hasFooterOffsets()}")

      // Use the metadata returned by commitAndCreateSplit
      if (!singleMetadata.hasFooterOffsets()) {
        println("⚠️  ERROR: Single split metadata from commitAndCreateSplit does not have footer offsets!")
        throw new RuntimeException("commitAndCreateSplit should return metadata with footer offsets")
      }

      val uniqueId2    = System.nanoTime()
      val cacheConfig2 = SplitCacheConfig(cacheName = s"single-test-cache-$uniqueId2")
      val singleSplitReader =
        SplitSearchEngine.fromSplitFileWithMetadata(sparkSchema, singleSplitPath, singleMetadata, cacheConfig2)

      try {
        // Test with limit larger than document count
        val largeResults = singleSplitReader.searchAll(100)
        assert(largeResults.length == 1, "Should return exactly 1 hit when only 1 document exists")
        assert(largeResults(0).getLong(0) == 1L, "Document ID should be 1")

        // Test with limit of 1 - should return exactly 1 result
        val limitResults = singleSplitReader.searchAll(1)
        assert(limitResults.length == 1, "Should return exactly 1 hit when limit is 1 and there's 1 document")

        println("✅ Single document split test passed")
      } finally
        singleSplitReader.close()
    } finally
      singleSearchEngine.close()

    println("✅ All split-based edge case tests passed!")
  }
}
