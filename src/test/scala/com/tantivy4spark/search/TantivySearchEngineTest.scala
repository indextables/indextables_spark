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

import com.tantivy4spark.{TantivyTestBase, TestDataGenerator, TestOptions, MockTantivyNative}
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
    val engine = new TantivySearchEngine(testOptions.toMap)
    engine shouldNot be(null)
  }
  
  it should "handle empty search results" in {
    val engine = new TantivySearchEngine(testOptions.toMap)
    val results = engine.search("test query", testDir.toString)
    
    results.hasNext shouldBe false
  }
  
  it should "parse search results correctly" in {
    val mockResponse = Map(
      "hits" -> List(
        Map(
          "score" -> 0.95,
          "document" -> Map(
            "_id" -> "doc1",
            "_bucket" -> "test-bucket",
            "_key" -> "test/path",
            "_offset" -> 0,
            "_length" -> 1024,
            "title" -> "Test Document"
          ),
          "snippet" -> Map(
            "title" -> "Test <em>Document</em>"
          )
        ),
        Map(
          "score" -> 0.78,
          "document" -> Map(
            "_id" -> "doc2",
            "_bucket" -> "test-bucket",
            "_key" -> "test/path2",
            "_offset" -> 1024,
            "_length" -> 512,
            "title" -> "Another Document"
          ),
          "snippet" -> Map()
        )
      ),
      "total_hits" -> 2,
      "elapsed_time_micros" -> 1500
    )
    
    val responseJson = objectMapper.writeValueAsString(mockResponse)
    MockTantivyNative.addSearchResult("test query", responseJson)
    
    // Mock the native calls
    val configId = MockTantivyNative.createMockConfig()
    val engineId = MockTantivyNative.createMockEngine(configId)
    
    val engine = new TantivySearchEngine(testOptions.toMap)
    val results = engine.search("test query", testDir.toString).toList
    
    results should have length 2
    
    val firstResult = results.head
    firstResult.docId shouldBe "doc1"
    firstResult.score shouldBe 0.95f
    firstResult.dataLocation.bucket shouldBe "test-bucket"
    firstResult.dataLocation.key shouldBe "test/path"
    firstResult.dataLocation.offset shouldBe 0
    firstResult.dataLocation.length shouldBe 1024
    firstResult.highlights should contain("title" -> "Test <em>Document</em>")
  }
  
  it should "handle malformed search results gracefully" in {
    MockTantivyNative.addSearchResult("bad query", "invalid json")
    
    val engine = new TantivySearchEngine(testOptions.toMap)
    val results = engine.search("bad query", testDir.toString)
    
    results.hasNext shouldBe false
  }
  
  it should "search with filters" in {
    val mockResponse = Map(
      "hits" -> List(
        Map(
          "score" -> 0.85,
          "document" -> Map(
            "_id" -> "filtered_doc",
            "_bucket" -> "test-bucket",
            "_key" -> "filtered/path",
            "_offset" -> 0,
            "_length" -> 256,
            "category" -> "tech"
          ),
          "snippet" -> Map()
        )
      ),
      "total_hits" -> 1,
      "elapsed_time_micros" -> 800
    )
    
    val responseJson = objectMapper.writeValueAsString(mockResponse)
    val expectedQuery = "(test) AND (category:tech AND status:active)"
    MockTantivyNative.addSearchResult(expectedQuery, responseJson)
    
    val engine = new TantivySearchEngine(testOptions.toMap)
    val filters = Map("category" -> "tech", "status" -> "active")
    val results = engine.searchWithFilters("test", testDir.toString, filters).toList
    
    results should have length 1
    results.head.docId shouldBe "filtered_doc"
  }
  
  it should "handle empty filters in searchWithFilters" in {
    val mockResponse = Map(
      "hits" -> List(),
      "total_hits" -> 0,
      "elapsed_time_micros" -> 100
    )
    
    val responseJson = objectMapper.writeValueAsString(mockResponse)
    MockTantivyNative.addSearchResult("test", responseJson)
    
    val engine = new TantivySearchEngine(testOptions.toMap)
    val results = engine.searchWithFilters("test", testDir.toString, Map.empty)
    
    results.hasNext shouldBe false
  }
  
  it should "extract data location fields with defaults" in {
    val mockResponse = Map(
      "hits" -> List(
        Map(
          "score" -> 0.5,
          "document" -> Map(
            "_id" -> "minimal_doc"
            // Missing location fields
          ),
          "snippet" -> Map()
        )
      ),
      "total_hits" -> 1,
      "elapsed_time_micros" -> 200
    )
    
    val responseJson = objectMapper.writeValueAsString(mockResponse)
    MockTantivyNative.addSearchResult("minimal", responseJson)
    
    val engine = new TantivySearchEngine(testOptions.toMap)
    val results = engine.search("minimal", testDir.toString).toList
    
    results should have length 1
    val result = results.head
    result.dataLocation.bucket shouldBe "default-bucket"
    result.dataLocation.key shouldBe ""
    result.dataLocation.offset shouldBe 0
    result.dataLocation.length shouldBe 0
  }
  
  it should "handle numeric fields correctly" in {
    val mockResponse = Map(
      "hits" -> List(
        Map(
          "score" -> 0.9,
          "document" -> Map(
            "_id" -> "numeric_doc",
            "_offset" -> "1024", // String representation
            "_length" -> 2048L,  // Long value
            "_bucket" -> "numeric-bucket",
            "_key" -> "numeric/key"
          ),
          "snippet" -> Map()
        )
      ),
      "total_hits" -> 1,
      "elapsed_time_micros" -> 300
    )
    
    val responseJson = objectMapper.writeValueAsString(mockResponse)
    MockTantivyNative.addSearchResult("numeric", responseJson)
    
    val engine = new TantivySearchEngine(testOptions.toMap)
    val results = engine.search("numeric", testDir.toString).toList
    
    results should have length 1
    val result = results.head
    result.dataLocation.offset shouldBe 1024L
    result.dataLocation.length shouldBe 2048L
  }
  
  it should "refresh index correctly" in {
    val engine = new TantivySearchEngine(testOptions.toMap)
    
    // This should not throw an exception
    engine.refreshIndex(testDir.toString)
  }
  
  it should "close resources properly" in {
    val engine = new TantivySearchEngine(testOptions.toMap)
    
    // This should not throw an exception
    engine.close()
  }
  
  it should "handle custom max results option" in {
    val customOptions = testOptions.copy(maxResults = 50).toMap
    val engine = new TantivySearchEngine(customOptions)
    
    engine shouldNot be(null)
  }
  
  it should "handle missing document ID gracefully" in {
    val mockResponse = Map(
      "hits" -> List(
        Map(
          "score" -> 0.7,
          "document" -> Map(
            // No _id field
            "_bucket" -> "test-bucket",
            "_key" -> "test/path"
          ),
          "snippet" -> Map()
        )
      ),
      "total_hits" -> 1,
      "elapsed_time_micros" -> 400
    )
    
    val responseJson = objectMapper.writeValueAsString(mockResponse)
    MockTantivyNative.addSearchResult("no_id", responseJson)
    
    val engine = new TantivySearchEngine(testOptions.toMap)
    val results = engine.search("no_id", testDir.toString).toList
    
    results should have length 1
    results.head.docId shouldBe ""
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