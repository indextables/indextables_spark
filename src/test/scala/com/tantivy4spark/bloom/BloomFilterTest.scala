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

package com.tantivy4spark.bloom

import com.tantivy4spark.TestBase
import org.scalatest.matchers.should.Matchers

class BloomFilterTest extends TestBase with Matchers {

  test("BloomFilter should handle basic add and contains operations") {
    val filter = new BloomFilter(expectedItems = 1000, falsePositiveRate = 0.01)
    
    // Add some tokens
    val tokens = Set("spark", "apache", "tantivy", "search", "bloom", "filter")
    tokens.foreach(filter.add)
    
    // Test positive cases
    tokens.foreach { token =>
      filter.mightContain(token) shouldBe true
    }
    
    // Test negative cases (should mostly be false, but might have false positives)
    val nonExistentTokens = Set("nonexistent", "missing", "absent")
    val falsePositives = nonExistentTokens.count(filter.mightContain)
    
    // Should have very few false positives with good parameters
    (falsePositives.toDouble / nonExistentTokens.size) should be <= 0.1
  }

  test("BloomFilter should maintain reasonable false positive rate") {
    val expectedItems = 10000
    val targetFalsePositiveRate = 0.01
    val filter = new BloomFilter(expectedItems, targetFalsePositiveRate)
    
    // Add expected number of items
    (1 to expectedItems).foreach { i =>
      filter.add(s"item_$i")
    }
    
    val stats = filter.getStats
    val actualFPR = stats("currentFalsePositiveRate").asInstanceOf[Double]
    
    // Actual FPR should be close to target (within reasonable bounds)
    actualFPR should be <= (targetFalsePositiveRate * 2.0)
    
    println(s"✅ Bloom filter stats: $stats")
  }

  test("BloomFilter should serialize and deserialize correctly") {
    val original = new BloomFilter(expectedItems = 100, falsePositiveRate = 0.01)
    
    val tokens = Set("serialization", "test", "bloom", "filter", "tantivy4spark")
    tokens.foreach(original.add)
    
    // Serialize
    val serialized = original.serialize()
    serialized.length should be > 0
    
    // Deserialize
    val deserialized = BloomFilter.deserialize(serialized)
    
    // Test that deserialized filter behaves similarly
    tokens.foreach { token =>
      deserialized.mightContain(token) shouldBe true
    }
    
    println(s"✅ Serialization: ${serialized.length} bytes")
  }

  test("TextTokenizer should tokenize text correctly") {
    val tokenizer = new TextTokenizer()
    
    // Test basic tokenization
    val text1 = "Apache Spark is a unified analytics engine"
    val tokens1 = tokenizer.tokenize(text1)
    
    tokens1 should contain("apache")
    tokens1 should contain("spark")
    tokens1 should contain("unified")
    tokens1 should contain("analytics")
    tokens1 should contain("engine")
    
    // Test complex text with punctuation
    val text2 = "user@domain.com sent log-data_2024 via HTTP/2.0"
    val tokens2 = tokenizer.tokenize(text2)
    
    tokens2 should contain("user")
    tokens2 should contain("domain")
    tokens2 should contain("com")
    tokens2 should contain("log")
    tokens2 should contain("data")
    tokens2 should contain("2024")
    tokens2 should contain("http")
    
    println(s"✅ Tokenized '$text1' -> ${tokens1.size} tokens")
    println(s"✅ Tokenized '$text2' -> ${tokens2.size} tokens")
  }

  test("TextTokenizer should handle search query extraction") {
    val tokenizer = new TextTokenizer()
    
    // Test quoted phrases
    val query1 = "\"machine learning\" AND apache spark"
    val terms1 = tokenizer.extractSearchTerms(query1)
    
    terms1 should contain("machine")
    terms1 should contain("learning")
    terms1 should contain("apache")
    terms1 should contain("spark")
    
    // Test mixed query
    val query2 = "error AND \"connection timeout\" OR database"
    val terms2 = tokenizer.extractSearchTerms(query2)
    
    terms2 should contain("error")
    terms2 should contain("connection")
    terms2 should contain("timeout")
    terms2 should contain("database")
    
    println(s"✅ Extracted from '$query1': ${terms1.mkString(", ")}")
    println(s"✅ Extracted from '$query2': ${terms2.mkString(", ")}")
  }

  test("BloomFilterManager should create filters for multiple columns") {
    val manager = new BloomFilterManager()
    
    val textData = Map(
      "title" -> Seq(
        "Introduction to Apache Spark",
        "Machine Learning with Python", 
        "Data Processing at Scale"
      ),
      "content" -> Seq(
        "Apache Spark is a unified analytics engine for large-scale data processing",
        "Python provides excellent libraries for machine learning and data science",
        "Distributed computing enables processing of massive datasets"
      ),
      "category" -> Seq("Technology", "Programming", "Data Science")
    )
    
    val bloomFilters = manager.createBloomFilters(textData)
    
    bloomFilters should have size 3
    bloomFilters.keys should contain("title")
    bloomFilters.keys should contain("content") 
    bloomFilters.keys should contain("category")
    
    // Test search capabilities
    val searchTerms = Set("apache", "spark", "machine", "learning")
    val mightContain = manager.mightContainAnyTerm(bloomFilters, searchTerms)
    mightContain shouldBe true
    
    // Test with non-existent terms
    val nonExistentTerms = Set("nonexistent", "missing", "absent")
    val shouldNotContain = manager.mightContainAnyTerm(bloomFilters, nonExistentTerms)
    // Might be true due to false positives, but generally should be false
    
    println(s"✅ Created bloom filters for ${bloomFilters.size} columns")
    bloomFilters.foreach { case (column, filter) =>
      println(s"   $column: ${filter.getStats}")
    }
  }

  test("BloomFilterStorage should handle compression and encoding") {
    val storage = BloomFilterStorage.getInstance
    
    // Create test bloom filters
    val filter1 = BloomFilter.fromTokens(Set("apache", "spark", "tantivy", "search"))
    val filter2 = BloomFilter.fromTokens(Set("machine", "learning", "data", "science"))
    
    val bloomFilters = Map(
      "title" -> filter1,
      "content" -> filter2
    )
    
    // Test encoding
    val encoded = storage.encodeBloomFilters(bloomFilters)
    encoded should have size 2
    encoded.keys should contain("title")
    encoded.keys should contain("content")
    
    // Each encoded filter should be a non-empty base64 string
    encoded.values.foreach { encodedFilter =>
      encodedFilter should not be empty
      encodedFilter.length should be > 10 // Reasonable minimum size
    }
    
    // Test decoding
    val decoded = storage.decodeBloomFilters(encoded, "test-file-path")
    decoded should have size 2
    
    // Test that decoded filters work correctly
    decoded("title").mightContain("apache") shouldBe true
    decoded("content").mightContain("machine") shouldBe true
    
    println(s"✅ Encoded filters: ${encoded.map { case (k, v) => s"$k -> ${v.length} chars" }}")
  }

  test("BloomFilterStorage should handle caching efficiently") {
    val storage = BloomFilterStorage.getInstance
    storage.clearCache() // Start with clean cache
    
    val filter = BloomFilter.fromTokens(Set("cache", "test", "efficiency"))
    val encoded = storage.encodeBloomFilter(filter)
    val encodedMap = Map("test_column" -> encoded)
    
    // First decode - should be cache miss
    val decoded1 = storage.decodeBloomFilters(encodedMap, "test-file-1")
    
    // Second decode of same file - should be cache hit
    val decoded2 = storage.decodeBloomFilters(encodedMap, "test-file-1")
    
    val cacheStats = storage.getCacheStats
    val hitRate = cacheStats("hitRate").asInstanceOf[Double]
    
    // Should have some cache hits
    hitRate should be > 0.0
    
    println(s"✅ Cache stats: $cacheStats")
  }

  test("Bloom filter integration should work end-to-end") {
    val manager = new BloomFilterManager()
    val storage = BloomFilterStorage.getInstance
    
    // Simulate real-world data
    val documentData = Map(
      "title" -> Seq(
        "Apache Spark Performance Tuning Guide",
        "Machine Learning Pipeline with MLlib",
        "Real-time Stream Processing",
        "Data Lake Architecture Patterns",
        "SQL Query Optimization Techniques"
      ),
      "content" -> Seq(
        "This guide covers performance optimization techniques for Apache Spark applications including memory tuning and cluster configuration",
        "MLlib provides scalable machine learning algorithms for classification, regression, clustering and collaborative filtering",
        "Stream processing enables real-time analytics on continuous data streams using structured streaming APIs",
        "Data lake architectures provide flexible storage for structured and unstructured data at massive scale",
        "Query optimization improves SQL performance through predicate pushdown, column pruning and cost-based optimization"
      )
    )
    
    // Create bloom filters
    val bloomFilters = manager.createBloomFilters(documentData)
    
    // Encode for storage
    val encoded = storage.encodeBloomFilters(bloomFilters)
    
    // Simulate storage and retrieval
    val decoded = storage.decodeBloomFilters(encoded, "integration-test-file")
    
    // Test various search scenarios
    val sparkSearchTerms = Set("spark", "apache")
    val mlSearchTerms = Set("machine", "learning") 
    val streamSearchTerms = Set("stream", "processing")
    val nonExistentTerms = Set("nonexistent", "missing")
    
    manager.mightContainAnyTerm(decoded, sparkSearchTerms) shouldBe true
    manager.mightContainAnyTerm(decoded, mlSearchTerms) shouldBe true  
    manager.mightContainAnyTerm(decoded, streamSearchTerms) shouldBe true
    
    // Non-existent terms might return false positives, but generally should be false
    val falsePositiveResult = manager.mightContainAnyTerm(decoded, nonExistentTerms)
    
    println(s"✅ End-to-end test completed")
    println(s"   Spark terms: ${manager.mightContainAnyTerm(decoded, sparkSearchTerms)}")
    println(s"   ML terms: ${manager.mightContainAnyTerm(decoded, mlSearchTerms)}")
    println(s"   Stream terms: ${manager.mightContainAnyTerm(decoded, streamSearchTerms)}")
    println(s"   Non-existent terms: $falsePositiveResult")
    
    // Verify compression efficiency
    val originalSize = documentData.values.flatten.map(_.length).sum
    val compressedSize = encoded.values.map(_.length).sum
    val compressionRatio = compressedSize.toDouble / originalSize
    
    println(s"   Compression: ${originalSize} chars -> ${compressedSize} chars (${(compressionRatio * 100).formatted("%.1f")}%)")
  }
}