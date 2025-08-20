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
import java.nio.file.Files
import java.util.zip.{ZipInputStream, ZipOutputStream, ZipEntry}
import java.io.{ByteArrayInputStream, FileOutputStream}

class ZipRestorationDebugTest extends TestBase {

  test("debug ZIP restoration following tantivy4java patterns exactly") {
    println("=== ZIP RESTORATION DEBUG TEST (Following tantivy4java patterns) ===")
    
    val schema = StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("title", StringType, nullable = false)
    ))
    
    // Step 1: Create and populate index (following tantivy4java IndexPersistenceTest pattern)
    println("Step 1: Creating index and adding documents...")
    val originalInterface = new TantivyDirectInterface(schema)
    
    val originalComponents = try {
      // Add test documents
      val doc1 = InternalRow(1L, UTF8String.fromString("Persistent Document One"))
      val doc2 = InternalRow(2L, UTF8String.fromString("Persistent Document Two"))
      
      originalInterface.addDocument(doc1)
      originalInterface.addDocument(doc2)
      
      // Commit (essential step!)
      originalInterface.commit()
      println("✓ Added 2 documents and committed")
      
      // Verify documents exist in original index
      val originalResults = originalInterface.searchAll(10)
      println(s"✓ Original index search returned ${originalResults.length} documents")
      originalResults.foreach { row =>
        println(s"  📄 Original: id=${row.getLong(0)}, title=${row.getUTF8String(1)}")
      }
      
      // Get components for restoration (this creates the ZIP)
      val components = originalInterface.getIndexComponents()
      println(s"✓ Retrieved ${components.size} index components")
      components.foreach { case (name, data) =>
        println(s"   - $name: ${data.length} bytes")
      }
      
      components
      
    } finally {
      originalInterface.close()
      println("✓ Original interface closed")
    }
    
    // Step 2: Restore from ZIP components (following tantivy4java pattern)
    println("\nStep 2: Restoring from ZIP components...")
    
    // Debug: Let's examine the ZIP contents
    originalComponents.get("tantivy_index.zip") match {
      case Some(zipData) =>
        println(s"✓ Found ZIP archive with ${zipData.length} bytes")
        
        // List ZIP contents
        val zipContents = listZipContents(zipData)
        println("✓ ZIP contents:")
        zipContents.foreach { filename =>
          println(s"   - $filename")
        }
        
        // Now restore using fromIndexComponents
        val restoredInterface = TantivyDirectInterface.fromIndexComponents(schema, originalComponents)
        
        try {
          println("✓ Restored interface created successfully")
          
          // Step 3: Search restored index (following tantivy4java pattern)
          println("\nStep 3: Searching restored index...")
          val restoredResults = restoredInterface.searchAll(10)
          println(s"✓ Restored index search returned ${restoredResults.length} documents")
          
          if (restoredResults.length == 0) {
            println("❌ ISSUE FOUND: Restored index has 0 documents!")
            
            // Let's debug this step by step, following tantivy4java pattern
            println("Debug: Let's check the restored index systematically...")
            
            // This would require direct access to the internal index, but let's at least verify our process
            
          } else {
            restoredResults.foreach { row =>
              println(s"  📄 Restored: id=${row.getLong(0)}, title=${row.getUTF8String(1)}")
            }
            println("✅ ZIP restoration working correctly!")
          }
          
          // Assertion
          assert(restoredResults.length == 2, s"Expected 2 documents, got ${restoredResults.length}")
          
        } finally {
          restoredInterface.close()
          println("✓ Restored interface closed")
        }
        
      case None =>
        fail("No tantivy_index.zip found in components")
    }
    
    println("=== DEBUG TEST COMPLETE ===")
  }
  
  private def listZipContents(zipData: Array[Byte]): List[String] = {
    val bais = new ByteArrayInputStream(zipData)
    val zis = new ZipInputStream(bais)
    val contents = scala.collection.mutable.ListBuffer[String]()
    
    try {
      var entry: ZipEntry = zis.getNextEntry
      while (entry != null) {
        if (!entry.isDirectory) {
          contents += entry.getName
        }
        zis.closeEntry()
        entry = zis.getNextEntry
      }
    } finally {
      zis.close()
      bais.close()
    }
    
    contents.toList
  }
}