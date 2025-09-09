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
import com.tantivy4java._
import com.tantivy4java.QuickwitSplit
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

/**
 * Simple test to validate that tantivy4java SplitTermQuery works properly
 * with splits created directly using tantivy4java.
 */
class SimpleTermQueryTest2 extends TestBase {

  private def isNativeLibraryAvailable(): Boolean = {
    try {
      import com.tantivy4spark.search.TantivyNative
      TantivyNative.ensureLibraryLoaded()
      true
    } catch {
      case _: Exception => false
    }
  }

  test("tantivy4java direct term query should work with manual split creation") {
    assume(isNativeLibraryAvailable(), "Native Tantivy library not available - skipping integration test")
    
    // Create tantivy index directly using tantivy4java API (like MinimalSplitTest)
    import java.nio.file.Files
    val tempDir = Files.createTempDirectory("tantivy_test")
    val indexPath = tempDir.resolve("test_index")
    val splitPath = tempDir.resolve("test.split")
    
    try {
      // Create schema
      val schema = new SchemaBuilder()
        .addIntegerField("id", true, true, true)
        .addTextField("department", true, false, "default", "position")
        .addTextField("status", true, false, "default", "position")
        .build()
      
      println("✅ Schema created successfully")
      
      // Create index and add documents
      val index = new Index(schema, indexPath.toString)
      val writer = index.writer(50000000, 1)
      
      // Add test documents
      val testData = Seq(
        (1, "Engineering", "active"),
        (2, "Marketing", "pending"), 
        (3, "Sales", "active"),
        (4, "Engineering", "pending"),
        (5, "HR", "active")
      )
      
      testData.foreach { case (id, dept, status) =>
        val doc = new Document()
        doc.addInteger("id", id)
        doc.addText("department", dept)
        doc.addText("status", status)
        writer.addDocument(doc)
        println(s"Added document: $id, $dept, $status")
      }
      
      writer.commit()
      writer.close()
      index.close()
      println("✅ Index created with test data")
      
      // Convert index to split using QuickwitSplit
      val splitConfig = new QuickwitSplit.SplitConfig("test-split", "test-source", "test-node")
      val metadata = QuickwitSplit.convertIndexFromPath(indexPath.toString, splitPath.toString, splitConfig)
      println("✅ Split created successfully")
      println(s"Split metadata hasFooterOffsets: ${metadata.hasFooterOffsets()}")
      
      // Use tantivy4java directly to test queries
      val cacheConfig = new SplitCacheManager.CacheConfig("simple-test-cache")
        .withMaxCacheSize(50000000L) // 50MB
      val cacheManager = SplitCacheManager.getInstance(cacheConfig)
      val splitSearcher = cacheManager.createSplitSearcher(splitPath.toString, metadata)
      
      try {
        val searchSchema = splitSearcher.getSchema()
        println(s"🔍 Schema fields: ${searchSchema.getFieldNames()}")
        
        // Test 1: Simple term query for "Engineering"
        println("\n🔎 Test 1: Term query for department = 'Engineering'")
        val engineeringQuery = new SplitTermQuery("department", "Engineering")
        println(s"Created SplitQuery: ${engineeringQuery.getClass.getSimpleName}")
        
        val engineeringResults = splitSearcher.search(engineeringQuery, 10)
        val engineeringResultsSize = engineeringResults.getHits().size()
        println(s"Found $engineeringResultsSize results for 'Engineering'")
        engineeringResults.close()
        
        // Should find 2 results (id=1 and id=4)
        assert(engineeringResultsSize == 2, s"Expected 2 Engineering results, got $engineeringResultsSize")
        
        // Test 2: Term query with no results
        println("\n🔎 Test 2: Term query for department = 'engineering' (lowercase)")
        val engineeringLowerQuery = new SplitTermQuery("department", "engineering")
        val engineeringLowerResults = splitSearcher.search(engineeringLowerQuery, 10)
        println(s"Found ${engineeringLowerResults.getHits().size()} results")
        engineeringLowerResults.close()
        
        // Test 3: Match all query
        println("\n🔎 Test 3: Match all query")
        val allQuery = new SplitMatchAllQuery()
        val allResults = splitSearcher.search(allQuery, 10)
        val totalDocuments = allResults.getHits().size()
        println(s"Found $totalDocuments total documents")
        allResults.close()
        
        // Should find all 5 documents
        assert(totalDocuments == 5, s"Expected 5 total documents, got $totalDocuments")
        
        println("✅ All SplitQuery tests passed!")
        
      } finally {
        splitSearcher.close()
        cacheManager.close()
      }
      
    } finally {
      // Clean up
      import java.io.File
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          file.listFiles().foreach(deleteRecursively)
        }
        file.delete()
      }
      deleteRecursively(tempDir.toFile)
    }
  }
}