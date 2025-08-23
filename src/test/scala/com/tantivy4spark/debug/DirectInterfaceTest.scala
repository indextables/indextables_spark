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
import com.tantivy4spark.search.{TantivyDirectInterface, TantivySearchEngine, SplitSearchEngine}
import com.tantivy4spark.storage.SplitCacheConfig
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import java.io.File
import java.nio.file.Files

class DirectInterfaceTest extends TestBase {

  test("should work with direct interface - minimal test") {
    // Use unique field names to avoid global state collision
    val schema = StructType(Array(
      StructField("user_id", LongType, nullable = false),
      StructField("user_name", StringType, nullable = false)
    ))
    
    // Create unique split file to avoid cache collisions across test runs
    val uniqueId = System.nanoTime()
    val tempSplitFile = Files.createTempFile(s"test_split_minimal_${uniqueId}", ".split").toFile
    tempSplitFile.deleteOnExit()
    
    val searchEngine = new TantivySearchEngine(schema)
    
    try {
      // Add a single document
      val row = InternalRow(1L, UTF8String.fromString("test"))
      searchEngine.addDocument(row)
      
      // Commit and create split
      val splitPath = searchEngine.commitAndCreateSplit(tempSplitFile.getAbsolutePath, 0L, "test-node")
      
      // Read from split instead of index with unique cache configuration
      val uniqueCacheConfig = SplitCacheConfig(cacheName = s"test-cache-minimal-${uniqueId}")
      val splitReader = SplitSearchEngine.fromSplitFile(schema, splitPath, uniqueCacheConfig)
      
      try {
        val results = splitReader.searchAll(10)
        
        println(s"Found ${results.length} documents")
        results.foreach { row =>
          println(s"Row: user_id=${row.getLong(0)}, user_name=${row.getUTF8String(1)}")
        }
        
        assert(results.length == 1, s"Should find exactly 1 document, got ${results.length}")
      } finally {
        splitReader.close()
      }
      
    } finally {
      searchEngine.close()
      tempSplitFile.delete()
    }
  }
  
  test("should work with multiple documents") {
    // Use completely different field names to avoid collision
    val schema = StructType(Array(
      StructField("person_id", LongType, nullable = false),
      StructField("person_name", StringType, nullable = false)
    ))
    
    // Create unique split file to avoid cache collisions across test runs
    val uniqueId = System.nanoTime()
    val tempSplitFile = Files.createTempFile(s"test_split_multi_${uniqueId}", ".split").toFile
    tempSplitFile.deleteOnExit()
    
    val searchEngine = new TantivySearchEngine(schema)
    
    try {
      // Add multiple documents
      val rows = Array(
        InternalRow(1L, UTF8String.fromString("Alice")),
        InternalRow(2L, UTF8String.fromString("Bob")),
        InternalRow(3L, UTF8String.fromString("Charlie"))
      )
      
      rows.foreach(searchEngine.addDocument)
      
      // Commit and create split
      val splitPath = searchEngine.commitAndCreateSplit(tempSplitFile.getAbsolutePath, 0L, "test-node")
      
      // Read from split instead of index with unique cache configuration
      val uniqueCacheConfig = SplitCacheConfig(cacheName = s"test-cache-multi-${uniqueId}")
      val splitReader = SplitSearchEngine.fromSplitFile(schema, splitPath, uniqueCacheConfig)
      
      try {
        val results = splitReader.searchAll(10)
        
        println(s"Found ${results.length} documents")
        results.foreach { row =>
          println(s"Row: person_id=${row.getLong(0)}, person_name=${row.getUTF8String(1)}")
        }
        
        assert(results.length == 3, s"Should find exactly 3 documents, got ${results.length}")
      } finally {
        splitReader.close()
      }
      
    } finally {
      searchEngine.close()
      tempSplitFile.delete()
    }
  }
}