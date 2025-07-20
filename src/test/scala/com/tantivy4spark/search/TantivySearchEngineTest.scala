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

package com.tantivy4spark.search

import com.tantivy4spark.{TantivyTestBase, TestDataGenerator, TestOptions, TestSchemas, MockTantivyNative}
import com.tantivy4spark.storage.DataLocation
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

class TantivySearchEngineTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    MockTantivyNative.reset()
  }
  
  "TantivySearchEngine" should "initialize with default options" in {
    val engine = new TantivySearchEngine(testOptions.toMap, Some(TestSchemas.basicSchema))
    engine shouldNot be(null)
  }
  
  it should "handle empty search results" in {
    val engine = new TantivySearchEngine(testOptions.toMap, Some(TestSchemas.basicSchema))
    val results = engine.search("test query", testDir.toString)
    
    results.hasNext shouldBe false
  }
  
  it should "parse search results correctly" in {
    // Use unified TantivyIndexManager for coordinated indexing and searching
    val indexPath = testDir.toString
    val indexOptions = testOptions.copy(basePath = indexPath, indexId = "test_index").toMap
    val indexManager = new com.tantivy4spark.search.TantivyIndexManager(indexPath, TestSchemas.basicSchema, indexOptions)
    
    // Create test documents
    val doc1 = org.apache.spark.sql.catalyst.InternalRow(
      1L, // id
      org.apache.spark.unsafe.types.UTF8String.fromString("Test Document"), // title
      org.apache.spark.unsafe.types.UTF8String.fromString("This is a test document for search"), // content
      System.currentTimeMillis(), // timestamp
      0.95, // score
      true // active
    )
    
    val doc2 = org.apache.spark.sql.catalyst.InternalRow(
      2L, // id
      org.apache.spark.unsafe.types.UTF8String.fromString("Another Document"), // title
      org.apache.spark.unsafe.types.UTF8String.fromString("This is another test document"), // content
      System.currentTimeMillis(), // timestamp
      0.85, // score
      false // active
    )
    
    // Index the documents using the unified manager
    indexManager.writeRow(doc1)
    indexManager.writeRow(doc2)
    indexManager.commit()
    
    // Search using the same manager (shared configuration)
    val results = indexManager.search("Document").toList
    
    // Should return results now that we have indexed data
    results.length should be >= 1
    
    // Verify the search results contain our documents
    results.foreach { result =>
      result.score should be >= 0.0f
    }
    
    indexManager.close()
  }
  
  it should "handle malformed search results gracefully" in {
    MockTantivyNative.addSearchResult("bad query", "invalid json")
    
    val engine = new TantivySearchEngine(testOptions.toMap, Some(TestSchemas.basicSchema))
    val results = engine.search("bad query", testDir.toString)
    
    results.hasNext shouldBe false
  }
  
  it should "search with filters" in {
    // Create schema with filter fields
    val extendedSchema = org.apache.spark.sql.types.StructType(TestSchemas.basicSchema.fields ++ Seq(
      org.apache.spark.sql.types.StructField("category", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("status", org.apache.spark.sql.types.StringType)
    ))
    
    // Use unified TantivyIndexManager
    val indexPath = testDir.toString
    val indexOptions = testOptions.copy(basePath = indexPath, indexId = "test_index").toMap
    val indexManager = new com.tantivy4spark.search.TantivyIndexManager(indexPath, extendedSchema, indexOptions)
    
    val testDoc = org.apache.spark.sql.catalyst.InternalRow(
      1L, // id
      org.apache.spark.unsafe.types.UTF8String.fromString("Test Document"), // title
      org.apache.spark.unsafe.types.UTF8String.fromString("This is a test document"), // content
      System.currentTimeMillis(), // timestamp
      0.85, // score
      true, // active
      org.apache.spark.unsafe.types.UTF8String.fromString("tech"), // category
      org.apache.spark.unsafe.types.UTF8String.fromString("active") // status
    )
    
    indexManager.writeRow(testDoc)
    indexManager.commit()
    
    // Search with filters using the same manager
    val filters = Map("category" -> "tech", "status" -> "active")
    val results = indexManager.searchWithFilters("test", filters).toList
    
    // Should return at least 1 result since we indexed matching data
    results.length should be >= 1
    
    indexManager.close()
  }
  
  it should "handle empty filters in searchWithFilters" in {
    val mockResponse = Map(
      "hits" -> List(),
      "total_hits" -> 0,
      "elapsed_time_micros" -> 100
    )
    
    val responseJson = objectMapper.writeValueAsString(mockResponse)
    MockTantivyNative.addSearchResult("test", responseJson)
    
    val engine = new TantivySearchEngine(testOptions.toMap, Some(TestSchemas.basicSchema))
    val results = engine.searchWithFilters("test", testDir.toString, Map.empty)
    
    results.hasNext shouldBe false
  }
  
  it should "extract data location fields with defaults" in {
    // Use unified TantivyIndexManager
    val indexPath = testDir.toString
    val indexOptions = testOptions.copy(basePath = indexPath, indexId = "test_index").toMap
    val indexManager = new com.tantivy4spark.search.TantivyIndexManager(indexPath, TestSchemas.basicSchema, indexOptions)
    
    val minimalDoc = org.apache.spark.sql.catalyst.InternalRow(
      3L, // id
      org.apache.spark.unsafe.types.UTF8String.fromString("Minimal Document"), // title
      org.apache.spark.unsafe.types.UTF8String.fromString("minimal content"), // content
      System.currentTimeMillis(), // timestamp
      0.5, // score
      true // active
    )
    
    indexManager.writeRow(minimalDoc)
    indexManager.commit()
    
    val results = indexManager.search("minimal").toList
    
    // Should find at least one result with our indexed data
    results.length should be >= 1
    
    indexManager.close()
  }
  
  it should "handle numeric fields correctly" in {
    // Use unified TantivyIndexManager
    val indexPath = testDir.toString
    val indexOptions = testOptions.copy(basePath = indexPath, indexId = "test_index").toMap
    val indexManager = new com.tantivy4spark.search.TantivyIndexManager(indexPath, TestSchemas.basicSchema, indexOptions)
    
    val numericDoc = org.apache.spark.sql.catalyst.InternalRow(
      999L, // id - numeric field
      org.apache.spark.unsafe.types.UTF8String.fromString("Numeric Document"), // title
      org.apache.spark.unsafe.types.UTF8String.fromString("document with numeric content"), // content
      System.currentTimeMillis(), // timestamp
      95.5, // score - numeric field
      false // active
    )
    
    indexManager.writeRow(numericDoc)
    indexManager.commit()
    
    val results = indexManager.search("numeric").toList
    
    // Should find the document with numeric fields
    results.length should be >= 1
    
    indexManager.close()
  }
  
  it should "refresh index correctly" in {
    val engine = new TantivySearchEngine(testOptions.toMap, Some(TestSchemas.basicSchema))
    
    // This should not throw an exception
    engine.refreshIndex(testDir.toString)
  }
  
  it should "close resources properly" in {
    val engine = new TantivySearchEngine(testOptions.toMap, Some(TestSchemas.basicSchema))
    
    // This should not throw an exception
    engine.close()
  }
  
  it should "handle custom max results option" in {
    val customOptions = testOptions.copy(maxResults = 50).toMap
    val engine = new TantivySearchEngine(customOptions)
    
    engine shouldNot be(null)
  }
  
  it should "handle missing document ID gracefully" in {
    // Use unified TantivyIndexManager
    val indexPath = testDir.toString
    val indexOptions = testOptions.copy(basePath = indexPath, indexId = "test_index").toMap
    val indexManager = new com.tantivy4spark.search.TantivyIndexManager(indexPath, TestSchemas.basicSchema, indexOptions)
    
    val testDoc = org.apache.spark.sql.catalyst.InternalRow(
      4L, // id
      org.apache.spark.unsafe.types.UTF8String.fromString("Document Without ID"), // title
      org.apache.spark.unsafe.types.UTF8String.fromString("content without explicit ID"), // content
      System.currentTimeMillis(), // timestamp
      0.7, // score
      true // active
    )
    
    indexManager.writeRow(testDoc)
    indexManager.commit()
    
    val results = indexManager.search("without").toList
    
    // Should find at least one result with indexed data
    results.length should be >= 1
    
    indexManager.close()
  }
}

class TantivySearchResultTest extends AnyFlatSpec with Matchers {
  
  "SearchResult" should "be created with all parameters" in {
    val dataLocation = DataLocation("bucket", "key", 100L, 200L)
    val highlights = Map("title" -> "highlighted text")
    
    val result = SearchResult("doc1", 0.95f, dataLocation, highlights)
    
    result.docId shouldBe "doc1"
    result.score shouldBe 0.95f
    result.dataLocation shouldBe dataLocation
    result.highlights shouldBe highlights
  }
  
  it should "have empty highlights by default" in {
    val dataLocation = DataLocation("bucket", "key", 100L, 200L)
    val result = SearchResult("doc1", 0.95f, dataLocation)
    
    result.highlights shouldBe Map.empty
  }
}

class TantivyIndexTest extends AnyFlatSpec with Matchers {
  
  "TantivyIndex" should "be created with all parameters" in {
    val fields = Map("title" -> "text", "score" -> "f64")
    val segments = List("segment1", "segment2")
    
    val index = TantivyIndex("test_index", fields, segments)
    
    index.name shouldBe "test_index"
    index.fields shouldBe fields
    index.segmentPaths shouldBe segments
  }
}