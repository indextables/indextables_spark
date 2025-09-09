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
import java.io.File

class MinimalDirectTest extends TestBase {

  test("minimal isolated split-based test") {
    println("=== MINIMAL ISOLATED SPLIT-BASED TEST ===")
    
    withTempPath { tempPath =>
      val indexPath = new File(tempPath, "index")
      val splitPath = new File(tempPath, "test.split")
      
      println("1. Creating tantivy4java index...")
      import com.tantivy4java._
      val schema = new SchemaBuilder()
        .addIntegerField("test_id", true, true, true)  // stored, indexed, fast
        .addTextField("test_name", true, false, "default", "position")     // stored, not fast, tokenizer, record
        .build()
      
      indexPath.mkdirs() // Create the index directory
      val index = new Index(schema, indexPath.toString)
      val writer = index.writer(50000000, 1)
      
      println("2. Adding document...")
      val doc = new Document()
      doc.addInteger("test_id", 42)
      doc.addText("test_name", "TestDocument")
      writer.addDocument(doc)
      println("   Document added successfully")
      
      writer.commit()
      writer.close()
      index.close()
      println("✅ Index created with test data")
      
      println("3. Converting index to split using QuickwitSplit...")
      val splitConfig = new QuickwitSplit.SplitConfig("minimal-test", "minimal-source", "minimal-node")
      val metadata = QuickwitSplit.convertIndexFromPath(indexPath.toString, splitPath.toString, splitConfig)
      println("✅ Split created successfully")
      println(s"Split metadata hasFooterOffsets: ${metadata.hasFooterOffsets()}")
      
      println("4. Testing SplitSearcher...")
      import com.tantivy4java._
      val cacheConfig = new SplitCacheManager.CacheConfig("minimal-test-cache")
        .withMaxCacheSize(50000000L) // 50MB
      val cacheManager = SplitCacheManager.getInstance(cacheConfig)
      val splitSearcher = cacheManager.createSplitSearcher(splitPath.toString, metadata)
      
      try {
        val allQuery = new SplitMatchAllQuery()
        val searchResult = splitSearcher.search(allQuery, 10)
        val hits = searchResult.getHits()
        println(s"   Search returned ${hits.size()} documents")
        
        if (hits.size() > 0) {
          hits.forEach { hit =>
            val doc = splitSearcher.doc(hit.getDocAddress())
            val testId = doc.get("test_id").get(0)
            val testName = doc.get("test_name").get(0)
            println(s"   Found: test_id=$testId, test_name=$testName")
            doc.close()
          }
        }
        searchResult.close()
        
        // This should succeed with the split-based approach
        assert(hits.size() == 1, s"Expected 1 document, got ${hits.size()}")
        
        println("✅ MINIMAL SPLIT-BASED TEST PASSED!")
        
      } finally {
        splitSearcher.close()
        cacheManager.close()
      }
    }
  }
}