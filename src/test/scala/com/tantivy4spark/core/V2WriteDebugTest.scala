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

package com.tantivy4spark.core

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import com.tantivy4spark.TestBase
import java.nio.file.Files

/**
 * Debug test to isolate V2 write issues.
 */
class V2WriteDebugTest extends AnyFunSuite with TestBase {

  test("Minimal V2 write test - no optimizations") {
    val tempPath = Files.createTempDirectory("v2_debug_").toFile.getAbsolutePath
    
    // Create minimal test data
    val df = spark.range(0, 100)
      .selectExpr("id", "CONCAT('content_', CAST(id AS STRING)) as content")
    
    // Write using V2 DataSource with optimizeWrite explicitly disabled
    println(s"DEBUG: Writing to path: $tempPath")
    
    df.write
      .format("tantivy4spark")
      .mode("overwrite")
      .option("optimizeWrite", "false")  // Explicitly disable optimization
      .save(tempPath)
    
    println("DEBUG: Write completed successfully")
    
    // Read back
    val readDf = spark.read
      .format("tantivy4spark")
      .load(tempPath)
    
    println(s"DEBUG: Read count: ${readDf.count()}")
    assert(readDf.count() == 100, "Should read back all 100 rows")
  }

  test("Minimal V2 write test - with optimizations") {
    val tempPath = Files.createTempDirectory("v2_debug_opt_").toFile.getAbsolutePath
    
    // Create minimal test data  
    val df = spark.range(0, 100)
      .selectExpr("id", "CONCAT('content_', CAST(id AS STRING)) as content")
    
    // Write using V2 DataSource with optimizeWrite enabled
    println(s"DEBUG: Writing to path with optimization: $tempPath")
    
    try {
      df.write
        .format("tantivy4spark")
        .mode("overwrite")
        .option("optimizeWrite", "true")   // Explicitly enable optimization
        .option("targetRecordsPerSplit", "50")  // Small target to trigger multiple partitions
        .option("estimatedRowCount", "100")     // Pass row count estimate
        .save(tempPath)
      
      println("DEBUG: Optimized write completed successfully")
      
      // Read back
      val readDf = spark.read
        .format("tantivy4spark")
        .load(tempPath)
      
      println(s"DEBUG: Read count: ${readDf.count()}")
      assert(readDf.count() == 100, "Should read back all 100 rows")
      
    } catch {
      case e: Exception =>
        println(s"DEBUG: Optimized write failed with: ${e.getMessage}")
        throw e
    }
  }
}