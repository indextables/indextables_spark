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

import org.scalatest.funsuite.AnyFunSuite
import com.tantivy4java._
import java.nio.file.Files
import scala.collection.JavaConverters._

/**
 * Pure tantivy4java test to demonstrate type conversion issues in split searches.
 * 
 * This test isolates the exact issue where Document.getFirst() from split searches
 * returns unexpected types compared to what was originally written.
 */
class TypeConversionBugDemo extends AnyFunSuite {

  test("pure tantivy4java split search type conversion bug") {
    println("=== Pure tantivy4java Split Search Type Bug Demo ===")
    
    val tempDir = Files.createTempDirectory("split_type_bug")
    val indexPath = tempDir.resolve("index") 
    val splitPath = tempDir.resolve("test.split")
    
    try {
      // Create schema with various field types
      val schema = new SchemaBuilder()
        .addTextField("name", true, false, "default", "position")       // TEXT
        .addFloatField("price", true, true, true)                       // FLOAT  
        .addIntegerField("count", true, true, true)                     // INTEGER
        .addBooleanField("available", true, true, true)                 // BOOLEAN
        .build()
      
      println("Schema created:")
      schema.getFieldNames().forEach { name =>
        val fieldType = schema.getFieldInfo(name).getType()
        println(s"  $name: $fieldType")
      }
      
      // Create index and add documents
      val index = new Index(schema, indexPath.toString)
      val writer = index.writer(50000000, 1)
      
      // Add test documents with known values
      val testData = List(
        ("Apple", 1.99, 100, true),
        ("Orange", 2.50, 50, false), 
        ("Banana", 0.75, 200, true)
      )
      
      testData.foreach { case (name, price, count, available) =>
        val doc = new Document()
        doc.addText("name", name)
        doc.addFloat("price", price)  
        doc.addInteger("count", count)
        doc.addBoolean("available", available)
        writer.addDocument(doc)
        println(s"Added: $name, $price, $count, $available")
      }
      
      writer.commit()
      writer.close()
      index.close()
      
      // Convert to split file
      val splitConfig = new QuickwitSplit.SplitConfig("test-split", "test-source", "test-node")
      QuickwitSplit.convertIndexFromPath(indexPath.toString, splitPath.toString, splitConfig)
      println("Split file created")
      
      // NOW THE BUG DEMO: Read from split and check types
      val cacheConfig = new SplitCacheManager.CacheConfig("test-cache").withMaxCacheSize(100000000L)
      val cacheManager = SplitCacheManager.getInstance(cacheConfig)
      val splitSearcher = cacheManager.createSplitSearcher("file://" + splitPath.toAbsolutePath)
      
      try {
        println("\nSplit schema:")
        val splitSchema = splitSearcher.getSchema()
        splitSchema.getFieldNames().forEach { name =>
          val fieldType = splitSchema.getFieldInfo(name).getType()
          println(s"  $name: $fieldType")
        }
        
        // Search all documents
        val query = Query.allQuery()
        val searchResult = splitSearcher.search(query, 10)
        val hits = searchResult.getHits().asScala
        
        println(s"\nFound ${hits.size} documents from split search:")
        
        hits.zipWithIndex.foreach { case (hit, i) =>
          println(s"\n--- Document $i ---")
          val document = splitSearcher.doc(hit.getDocAddress())
          
          val name = document.getFirst("name")
          val price = document.getFirst("price") 
          val count = document.getFirst("count")
          val available = document.getFirst("available")
          
          println(s"Values retrieved:")
          println(s"  name: '$name' (${if (name != null) name.getClass.getSimpleName else "null"})")
          println(s"  price: $price (${if (price != null) price.getClass.getSimpleName else "null"})")
          println(s"  count: $count (${if (count != null) count.getClass.getSimpleName else "null"})")
          println(s"  available: $available (${if (available != null) available.getClass.getSimpleName else "null"})")
          
          // Type validation
          val problems = scala.collection.mutable.ListBuffer[String]()
          
          if (name != null && !name.isInstanceOf[String]) {
            problems += s"name should be String, got ${name.getClass.getSimpleName}"
          }
          
          if (price != null && !price.isInstanceOf[java.lang.Float] && !price.isInstanceOf[java.lang.Double]) {
            problems += s"price should be Float/Double, got ${price.getClass.getSimpleName}" 
          }
          
          if (count != null && !count.isInstanceOf[java.lang.Integer] && !count.isInstanceOf[java.lang.Long]) {
            problems += s"count should be Integer/Long, got ${count.getClass.getSimpleName}"
          }
          
          if (available != null && !available.isInstanceOf[java.lang.Boolean]) {
            problems += s"available should be Boolean, got ${available.getClass.getSimpleName}"
          }
          
          if (problems.nonEmpty) {
            println("🚨 TYPE CONVERSION BUGS:")
            problems.foreach(p => println(s"  - $p"))
          } else {
            println("✅ All types correct")
          }
          
          document.close()
        }
        
        searchResult.close()
        query.close()
        
      } finally {
        splitSearcher.close()
        cacheManager.close()
      }
      
    } finally {
      // Cleanup
      try {
        Files.deleteIfExists(splitPath)
        Files.walk(indexPath).sorted(java.util.Comparator.reverseOrder()).forEach(Files.deleteIfExists(_))
        Files.deleteIfExists(tempDir)
      } catch {
        case e: Exception => println(s"Cleanup failed: ${e.getMessage}")
      }
    }
  }

}