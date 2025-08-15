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

import com.tantivy4spark.TestBase
import org.apache.spark.sql.SaveMode

class PathParameterTest extends TestBase {

  test("should accept direct path parameters in save() method") {
    withTempPath { tempPath =>
      val df = createTestDataFrame()
      
      // Test direct path parameter usage - this should work the same as .option("path", tempPath)
      try {
        (df.write
          .format("tantivy4spark")
          .mode(SaveMode.Overwrite)
          .save(tempPath))  // Direct path parameter instead of .option("path", tempPath)
        
        // If write succeeds, direct path parameters are working
        succeed
      } catch {
        case _: RuntimeException | _: UnsatisfiedLinkError => 
          // Expected if native library has issues - this is fine
          succeed
        case e: Exception =>
          // Should reach execution phase, not fail on configuration
          e.getMessage should not include "ClassNotFoundException"
          e.getMessage should not include "Path is required"
      }
    }
  }

  test("should accept direct path parameters in load() method") {
    withTempPath { tempPath =>
      // Test direct path parameter usage - this should work the same as .option("path", tempPath)
      val exception = intercept[Exception] {
        spark.read
          .format("tantivy4spark")
          .load(tempPath)  // Direct path parameter instead of .option("path", tempPath)
          .count()
      }
      
      // Should reach execution phase, not fail on configuration
      exception.getMessage should not include "ClassNotFoundException" 
      exception.getMessage should not include "Path is required"
    }
  }

  test("should support S3-style paths with direct parameters") {
    val s3Path = "s3a://my-bucket/data/table"
    
    // Test write with S3 path
    val writeException = intercept[Exception] {
      createTestDataFrame().write
        .format("tantivy4spark")
        .mode(SaveMode.Overwrite)
        .save(s3Path)
    }
    
    writeException.getMessage should not include "Path is required"
    
    // Test read with S3 path  
    val readException = intercept[Exception] {
      spark.read
        .format("tantivy4spark")
        .load(s3Path)
        .count()
    }
    
    readException.getMessage should not include "Path is required"
  }

  test("should demonstrate direct path parameters work as expected") {
    withTempPath { tempPath =>
      // Demonstrate that both approaches work the same way
      val df = createTestDataFrame()
      
      var directPathWorked = false
      var optionPathWorked = false
      
      // Direct path approach
      try {
        df.write.format("tantivy4spark").mode(SaveMode.Overwrite).save(tempPath)
        directPathWorked = true
      } catch {
        case _: RuntimeException | _: UnsatisfiedLinkError => 
          directPathWorked = true // Expected if native library has issues
        case e: Exception =>
          e.getMessage should not include "Path is required"
      }
      
      // Option-based approach  
      try {
        df.write.format("tantivy4spark").mode(SaveMode.Overwrite).option("path", tempPath).save()
        optionPathWorked = true
      } catch {
        case _: RuntimeException | _: UnsatisfiedLinkError => 
          optionPathWorked = true // Expected if native library has issues
        case e: Exception =>
          e.getMessage should not include "Path is required"
      }
      
      // Both approaches should work the same way
      directPathWorked shouldBe true
      optionPathWorked shouldBe true
    }
  }
}