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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import io.indextables.tantivy4java.core.{Index, Schema, SchemaBuilder, Document, IndexWriter}
import io.indextables.tantivy4java.split.{SplitSearcher, SplitCacheManager}
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import java.nio.file.{Files, Paths}
import scala.util.Using

class MinimalSplitTest extends AnyFunSuite with BeforeAndAfterEach {

  test("should create index, convert to split, and read documents using pure tantivy4java API") {
    println("=== Testing Pure tantivy4java API (Single Threaded) ===")

    // Step 1: Create a basic index with tantivy4java API directly
    val tempDir   = Files.createTempDirectory("tantivy_test")
    val indexPath = tempDir.resolve("test_index")
    val splitPath = tempDir.resolve("test.split")

    try {
      // Create schema
      val schema = new SchemaBuilder()
        .addTextField("title", true, false, "default", "position")
        .addTextField("content", true, false, "default", "position")
        .addIntegerField("score", true, true, true)
        .build()

      println("✅ Schema created successfully")

      // Create index and add documents
      val index  = new Index(schema, indexPath.toString)
      val writer = index.writer(50000000, 1)

      // Add test documents
      for (i <- 1 to 3) {
        val doc = new Document()
        doc.addText("title", s"Document Title $i")
        doc.addText("content", s"This is test content for document $i")
        doc.addInteger("score", i * 10)
        writer.addDocument(doc)
        println(s"Added document $i")
      }

      writer.commit()
      writer.close()
      index.close()
      println("✅ Index created and documents committed")

      // Step 2: Convert index to split using QuickwitSplit
      val uniqueId = java.util.UUID.randomUUID().toString
      val splitConfig = new QuickwitSplit.SplitConfig(
        s"test-index-$uniqueId",
        "test-source",
        "test-node"
      )

      // Capture the metadata returned by convertIndexFromPath (which has footer offsets)
      val splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath.toString, splitPath.toString, splitConfig)
      println("✅ Split created successfully")
      println(s"Split metadata hasFooterOffsets: ${splitMetadata.hasFooterOffsets()}")

      // Step 3: Read from split using SplitCacheManager and SplitSearcher
      val cacheConfig = new SplitCacheManager.CacheConfig("minimal-test-cache")
        .withMaxCacheSize(50000000L) // 50MB
      val cacheManager = SplitCacheManager.getInstance(cacheConfig)

      val splitUrl = splitPath.toAbsolutePath.toString

      // Use the metadata returned by convertIndexFromPath (which has footer offsets)
      val metadata = splitMetadata

      // This should have footer offsets since it came directly from convertIndexFromPath
      if (!metadata.hasFooterOffsets()) {
        println("⚠️  ERROR: Metadata from convertIndexFromPath does not have footer offsets!")
        throw new RuntimeException("convertIndexFromPath should return metadata with footer offsets")
      }

      val splitSearcher = cacheManager.createSplitSearcher(splitUrl, metadata)

      try {
        // Test schema introspection
        val splitSchema = splitSearcher.getSchema()
        println(s"Split schema field count: ${splitSchema.getFieldCount()}")
        println(s"Split schema field names: ${splitSchema.getFieldNames()}")

        // Search for all documents using the new SplitQuery API
        val splitQuery   = splitSearcher.parseQuery("*")
        val searchResult = splitSearcher.search(splitQuery, 10)

        val hits = searchResult.getHits()
        println(s"Found ${hits.size()} documents in split")

        // Extract and validate document field values
        for (i <- 0 until hits.size()) {
          val hit = hits.get(i)
          println(s"\n--- Processing hit $i ---")
          println(s"Hit score: ${hit.getScore()}")
          println(s"Doc address: ${hit.getDocAddress()}")

          // Get the document using the docBatch() method for efficiency
          // For single document, we still use docBatch for consistency
          import scala.jdk.CollectionConverters._
          val documents = splitSearcher.docBatch(List(hit.getDocAddress()).asJava).asScala
          val document  = if (documents.nonEmpty) documents.head else null

          if (document != null) {
            // Test field value extraction
            val titleValue   = document.getFirst("title")
            val contentValue = document.getFirst("content")
            val scoreValue   = document.getFirst("score")

            println(s"Title: $titleValue (${if (titleValue != null) titleValue.getClass.getSimpleName else "null"})")
            println(
              s"Content: $contentValue (${if (contentValue != null) contentValue.getClass.getSimpleName else "null"})"
            )
            println(s"Score: $scoreValue (${if (scoreValue != null) scoreValue.getClass.getSimpleName else "null"})")

            // Verify field values are not null
            assert(titleValue != null, s"Title should not be null for document $i")
            assert(contentValue != null, s"Content should not be null for document $i")
            assert(scoreValue != null, s"Score should not be null for document $i")

            // Verify field types
            assert(titleValue.isInstanceOf[String], s"Title should be String, got ${titleValue.getClass}")
            assert(contentValue.isInstanceOf[String], s"Content should be String, got ${contentValue.getClass}")
            assert(scoreValue.isInstanceOf[Number], s"Score should be Number, got ${scoreValue.getClass}")

            // Verify field content
            assert(
              titleValue.toString.startsWith("Document Title"),
              s"Title should start with 'Document Title', got: $titleValue"
            )
            assert(
              contentValue.toString.contains("test content"),
              s"Content should contain 'test content', got: $contentValue"
            )

            document.close()
            println(s"✅ Document $i validated successfully")
          } else {
            println(s"❌ Document $i is null!")
            assert(false, s"Document $i should not be null")
          }
        }

        searchResult.close()
        // SplitQuery objects don't need to be closed

        println(s"\n✅ Successfully validated ${hits.size()} documents with correct field values")
        assert(hits.size() == 3, s"Should find exactly 3 documents, found ${hits.size()}")

      } finally {
        splitSearcher.close()
        cacheManager.close()
      }

    } finally
      // Cleanup
      try {
        Files.deleteIfExists(splitPath)
        Files.walk(indexPath).sorted(java.util.Comparator.reverseOrder()).forEach(Files.deleteIfExists(_))
        Files.deleteIfExists(tempDir)
      } catch {
        case e: Exception => println(s"Cleanup error: ${e.getMessage}")
      }
  }
}
