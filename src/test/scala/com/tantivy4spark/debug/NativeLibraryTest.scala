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
import com.tantivy4spark.search.TantivyNative
import org.apache.spark.sql.functions._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class NativeLibraryTest extends TestBase {

  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  test("should load native library in executors") {
    val spark = this.spark
    import spark.implicits._
    
    // Create a simple RDD to test library loading in executors
    val testData = Seq(1, 2, 3, 4, 5).toDF("value")
    
    val result = testData.rdd.mapPartitions { _ =>
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
    }.collect()
    
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
      
      documents.zipWithIndex.foreach { case (doc, idx) =>
        val added = TantivyNative.addDocument(indexHandle, doc)
        assert(added, s"Document ${idx + 1} should be added successfully")
        println(s"Added document ${idx + 1}")
      }
      
      // Step 3: Commit the index
      println("Step 3: Committing index...")
      val committed = TantivyNative.commit(indexHandle)
      assert(committed, "Index should commit successfully")
      println("Index committed successfully")
      
      // Step 4: Call searchAll directly
      println("Step 4: Testing searchAll method...")
      
      // Test with different limits
      val testLimits = Seq(3, 10, 100)
      
      testLimits.foreach { limit =>
        println(s"Testing searchAll with limit: $limit")
        val results = TantivyNative.searchAll(indexHandle, limit)
        
        // Step 5: Verify results
        assert(results != null, s"searchAll should return non-null results for limit $limit")
        assert(results.nonEmpty, s"searchAll should return non-empty results for limit $limit")
        println(s"Raw results for limit $limit: $results")
        
        // Parse JSON results
        val jsonResults = objectMapper.readTree(results)
        assert(jsonResults.has("hits"), "Results should contain 'hits' field")
        
        val hits = jsonResults.get("hits")
        val expectedHits = Math.min(documents.length, limit)
        assert(hits.size() == expectedHits, 
          s"Should return $expectedHits hits for limit $limit, got ${hits.size()}")
        
        // Verify each hit contains expected fields
        for (i <- 0 until hits.size()) {
          val hit = hits.get(i)
          assert(hit.has("score"), s"Hit $i should have score field")
          assert(hit.has("doc"), s"Hit $i should have doc field")
          
          val doc = hit.get("doc")
          assert(doc.has("id"), s"Document $i should have id field")
          assert(doc.has("title"), s"Document $i should have title field")
          assert(doc.has("content"), s"Document $i should have content field")
          assert(doc.has("score"), s"Document $i should have score field")
          
          println(s"Hit $i: ID=${doc.get("id").asText()}, Title=${doc.get("title").asText()}, Score=${hit.get("score").asDouble()}")
        }
      }
      
      println("searchAll JNI method test completed successfully!")
      
    } catch {
      case e: Exception =>
        println(s"Test failed with exception: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      // Clean up the index
      if (indexHandle != 0) {
        println("Cleaning up index...")
        val closed = TantivyNative.closeIndex(indexHandle)
        if (closed) {
          println("Index closed successfully")
        } else {
          println("Warning: Failed to close index")
        }
      }
    }
  }
  
  test("searchAll JNI method should handle edge cases") {
    // Ensure library is loaded
    assert(TantivyNative.ensureLibraryLoaded(), "Native library should load successfully")
    
    // Create a simple schema (using the format expected by SchemaMapper)
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
            "name": "text",
            "type": "text",
            "indexed": true,
            "stored": true
          }
        ]
      }"""
    
    var indexHandle: Long = 0
    
    try {
      // Create empty index
      indexHandle = TantivyNative.createIndex(schema)
      assert(indexHandle != 0, "Index handle should be non-zero")
      
      // Commit empty index
      val committed = TantivyNative.commit(indexHandle)
      assert(committed, "Empty index should commit successfully")
      
      // Test searchAll on empty index
      println("Testing searchAll on empty index...")
      val emptyResults = TantivyNative.searchAll(indexHandle, 10)
      assert(emptyResults != null, "searchAll should return non-null results for empty index")
      
      val jsonEmptyResults = objectMapper.readTree(emptyResults)
      assert(jsonEmptyResults.has("hits"), "Empty results should contain 'hits' field")
      
      val emptyHits = jsonEmptyResults.get("hits")
      assert(emptyHits.size() == 0, "Empty index should return 0 hits")
      println("Empty index test passed")
      
      // Add one document and test
      val singleDoc = """{"id": 1, "text": "single document"}"""
      val added = TantivyNative.addDocument(indexHandle, singleDoc)
      assert(added, "Single document should be added successfully")
      
      val recommitted = TantivyNative.commit(indexHandle)
      assert(recommitted, "Index with single document should commit successfully")
      
      // Test searchAll with limit larger than document count
      println("Testing searchAll with limit larger than document count...")
      val singleResults = TantivyNative.searchAll(indexHandle, 100)
      val jsonSingleResults = objectMapper.readTree(singleResults)
      val singleHits = jsonSingleResults.get("hits")
      assert(singleHits.size() == 1, "Should return exactly 1 hit when only 1 document exists")
      println("Single document test passed")
      
      // Test searchAll with limit of 0 - should return empty results
      println("Testing searchAll with limit of 0...")
      val zeroResults = TantivyNative.searchAll(indexHandle, 0)
      val jsonZeroResults = objectMapper.readTree(zeroResults)
      val zeroHits = jsonZeroResults.get("hits")
      assert(zeroHits.size() == 0, "Should return 0 hits when limit is 0")
      println("Zero limit test passed")
      
    } catch {
      case e: Exception =>
        println(s"Edge case test failed with exception: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      // Clean up
      if (indexHandle != 0) {
        TantivyNative.closeIndex(indexHandle)
      }
    }
  }
}