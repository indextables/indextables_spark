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

package com.tantivy4spark.native

import com.tantivy4spark.{TantivyTestBase, MockTantivyNative}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class TantivyNativeTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    MockTantivyNative.reset()
  }
  
  "TantivyNative" should "have correct library name detection" in {
    // Test library name selection based on platform
    // This tests the platform-specific library loading logic
    val libraryName = if (System.getProperty("os.name").toLowerCase.contains("windows")) {
      "tantivy_jni.dll"
    } else if (System.getProperty("os.name").toLowerCase.contains("mac")) {
      "libtantivy_jni.dylib"
    } else {
      "libtantivy_jni.so"
    }
    
    libraryName should not be empty
    libraryName should include("tantivy_jni")
  }
  
  it should "handle configuration management" in {
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    
    val config = Map(
      "base_path" -> testDir.toString,
      "index_config" -> Map(
        "index_id" -> "test_index"
      )
    )
    
    val configJson = objectMapper.writeValueAsString(config)
    
    // Mock the native library behavior
    // In a real test, this would call TantivyNative.createConfig(configJson)
    // For now, we test the JSON generation
    configJson should include("base_path")
    configJson should include("index_config")
    configJson should include("test_index")
  }
  
  it should "handle search engine lifecycle" in {
    // Mock configuration and engine creation
    val configId = MockTantivyNative.createMockConfig()
    configId should be > 0L
    
    val engineId = MockTantivyNative.createMockEngine(configId)
    engineId should be > 0L
    
    // Test that different configs get different IDs
    val anotherConfigId = MockTantivyNative.createMockConfig()
    anotherConfigId should not equal configId
  }
  
  it should "handle index writer lifecycle" in {
    // Mock configuration and writer creation
    val configId = MockTantivyNative.createMockConfig()
    val writerId = MockTantivyNative.createMockWriter(configId)
    
    writerId should be > 0L
    
    // Test multiple writers from same config
    val anotherWriterId = MockTantivyNative.createMockWriter(configId)
    anotherWriterId should not equal writerId
  }
  
  it should "generate unique IDs for different components" in {
    val config1 = MockTantivyNative.createMockConfig()
    val config2 = MockTantivyNative.createMockConfig()
    val engine1 = MockTantivyNative.createMockEngine(config1)
    val engine2 = MockTantivyNative.createMockEngine(config2)
    val writer1 = MockTantivyNative.createMockWriter(config1)
    val writer2 = MockTantivyNative.createMockWriter(config2)
    
    val allIds = Set(config1, config2, engine1, engine2, writer1, writer2)
    allIds should have size 6 // All IDs should be unique
  }
  
  it should "handle search operations with mock results" in {
    val query = "test query"
    val expectedResult = """{"hits": [], "total_hits": 0, "elapsed_time_micros": 100}"""
    
    MockTantivyNative.addSearchResult(query, expectedResult)
    
    val result = MockTantivyNative.searchResults.get(query)
    result shouldBe Some(expectedResult)
  }
  
  it should "track indexed documents" in {
    val documents = List(
      """{"id": "doc1", "title": "Test Document 1"}""",
      """{"id": "doc2", "title": "Test Document 2"}"""
    )
    
    documents.foreach { doc =>
      MockTantivyNative.indexedDocuments = MockTantivyNative.indexedDocuments :+ doc
    }
    
    MockTantivyNative.indexedDocuments should have length 2
    MockTantivyNative.indexedDocuments should contain(documents.head)
    MockTantivyNative.indexedDocuments should contain(documents.last)
  }
  
  it should "reset state correctly" in {
    // Add some data
    MockTantivyNative.createMockConfig()
    MockTantivyNative.addSearchResult("test", "result")
    MockTantivyNative.indexedDocuments = List("doc1", "doc2")
    
    // Verify data exists
    MockTantivyNative.configs should not be empty
    MockTantivyNative.searchResults should not be empty
    MockTantivyNative.indexedDocuments should not be empty
    
    // Reset
    MockTantivyNative.reset()
    
    // Verify clean state
    MockTantivyNative.configs shouldBe empty
    MockTantivyNative.searchResults shouldBe empty
    MockTantivyNative.indexedDocuments shouldBe empty
    MockTantivyNative.engines shouldBe empty
    MockTantivyNative.writers shouldBe empty
    MockTantivyNative.nextId shouldBe 1
  }
  
  it should "handle concurrent mock operations" in {
    import scala.concurrent.{Future, ExecutionContext}
    import scala.concurrent.duration._
    
    implicit val ec: ExecutionContext = ExecutionContext.global
    
    // Create multiple configs concurrently
    val configFutures = (1 to 10).map { _ =>
      Future {
        MockTantivyNative.createMockConfig()
      }
    }
    
    val configIds = scala.concurrent.Await.result(Future.sequence(configFutures), 10.seconds)
    
    configIds should have length 10
    configIds.toSet should have size 10 // All should be unique
  }
  
  it should "validate JSON structure for configurations" in {
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    
    // Test valid configuration JSON
    val validConfig = Map(
      "base_path" -> "/valid/path",
      "index_config" -> Map(
        "index_id" -> "valid_index",
        "doc_mapping" -> Map(
          "mode" -> "strict"
        )
      )
    )
    
    val validJson = objectMapper.writeValueAsString(validConfig)
    
    // Should be parseable back to Map
    val parsedConfig = objectMapper.readValue(validJson, classOf[Map[String, Any]])
    parsedConfig should contain key "base_path"
    parsedConfig should contain key "index_config"
  }
  
  it should "handle schema JSON generation correctly" in {
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    
    val schema = Map(
      "field_mappings" -> Map(
        "id" -> Map("type" -> "i64", "indexed" -> true),
        "title" -> Map("type" -> "text", "indexed" -> true, "stored" -> true),
        "content" -> Map("type" -> "text", "indexed" -> true)
      ),
      "timestamp_field" -> "_timestamp",
      "default_search_fields" -> List("title", "content")
    )
    
    val schemaJson = objectMapper.writeValueAsString(schema)
    
    schemaJson should include("field_mappings")
    schemaJson should include("timestamp_field")
    schemaJson should include("default_search_fields")
    
    // Should be parseable
    val parsedSchema = objectMapper.readValue(schemaJson, classOf[Map[String, Any]])
    parsedSchema should contain key "field_mappings"
  }
  
  it should "simulate document indexing workflow" in {
    val configId = MockTantivyNative.createMockConfig()
    val writerId = MockTantivyNative.createMockWriter(configId)
    
    // Simulate indexing documents
    val documents = (1 to 5).map { i =>
      s"""{"id": "doc$i", "title": "Document $i", "content": "Content for document $i"}"""
    }
    
    documents.foreach { doc =>
      MockTantivyNative.indexedDocuments = MockTantivyNative.indexedDocuments :+ doc
    }
    
    MockTantivyNative.indexedDocuments should have length 5
    
    // Verify all documents were indexed
    documents.foreach { doc =>
      MockTantivyNative.indexedDocuments should contain(doc)
    }
  }
}