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
import com.tantivy4spark.search.TantivySearchEngine
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

class DocumentExtractionTest extends TestBase {

  test("should extract documents from index components") {
    val schema = StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))
    
    val searchEngine = new TantivySearchEngine(schema)
    
    // Add a few documents
    val row1 = InternalRow(1L, UTF8String.fromString("Alice"))
    val row2 = InternalRow(2L, UTF8String.fromString("Bob"))
    val row3 = InternalRow(3L, UTF8String.fromString("Charlie"))
    
    println("Adding documents to Tantivy index...")
    searchEngine.addDocument(row1)
    searchEngine.addDocument(row2)
    searchEngine.addDocument(row3)
    
    println("Calling commitAndGetComponents...")
    val components = searchEngine.commitAndGetComponents()
    
    println(s"Got ${components.size} components: ${components.keys.mkString(", ")}")
    
    components.keys.foreach { key =>
      val content = new String(components(key), "UTF-8")
      println(s"Component '$key': ${components(key).length} bytes")
      println(s"Content of $key: $content")
    }
    
    // Verify we have data.json component
    assert(components.contains("data.json"), "Should have data.json component with actual document data")
    
    searchEngine.close()
  }
}