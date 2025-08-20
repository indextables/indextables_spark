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
import com.tantivy4spark.search.TantivyDirectInterface
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

class MinimalDirectTest extends TestBase {

  test("minimal isolated direct interface test") {
    println("=== MINIMAL ISOLATED TEST ===")
    
    val schema = StructType(Array(
      StructField("test_id", LongType, nullable = false),
      StructField("test_name", StringType, nullable = false)
    ))
    
    println(s"Schema: $schema")
    
    val interface = new TantivyDirectInterface(schema)
    
    try {
      println("1. Adding document...")
      val row = InternalRow(42L, UTF8String.fromString("TestDocument"))
      interface.addDocument(row)
      println("   Document added successfully")
      
      println("2. Committing...")
      interface.commit()
      println("   Commit completed")
      
      println("3. Searching...")
      val results = interface.searchAll(10)
      println(s"   Search returned ${results.length} documents")
      
      if (results.length > 0) {
        results.foreach { row =>
          println(s"   Found: test_id=${row.getLong(0)}, test_name=${row.getUTF8String(1)}")
        }
      }
      
      // This should succeed if our fix works
      assert(results.length == 1, s"Expected 1 document, got ${results.length}")
      assert(results(0).getLong(0) == 42L, "Expected test_id=42")
      
      println("✅ MINIMAL TEST PASSED!")
      
    } finally {
      interface.close()
      println("   Interface closed")
    }
  }
}