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

package com.tantivy4spark.config

import com.tantivy4spark.TestBase
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

class IndexWriterConfigTest extends TestBase {

  test("should use default index writer configuration") {
    withTempPath { tempPath =>
      // Generate simple test data
      val testData = spark.range(100).select(
        col("id"),
        concat(lit("Record"), col("id")).as("content")
      )
      
      // Write without any custom configuration - should use defaults (100MB heap, 2 threads)
      testData.write
        .format("tantivy4spark")
        .mode(SaveMode.Overwrite)
        .save(tempPath)
      
      // Read back to verify it worked
      val readData = spark.read
        .format("tantivy4spark")
        .load(tempPath)
      
      readData.count() shouldBe 100
    }
  }

  test("should use custom index writer configuration from Spark config") {
    withTempPath { tempPath =>
      // Set custom index writer configuration via Spark session
      spark.conf.set("spark.tantivy4spark.indexWriter.heapSize", "200000000") // 200MB
      spark.conf.set("spark.tantivy4spark.indexWriter.threads", "4") // 4 threads
      
      try {
        // Generate simple test data
        val testData = spark.range(100).select(
          col("id"),
          concat(lit("Record"), col("id")).as("content")
        )
        
        // Write with custom configuration
        testData.write
          .format("tantivy4spark")
          .mode(SaveMode.Overwrite)
          .save(tempPath)
        
        // Read back to verify it worked
        val readData = spark.read
          .format("tantivy4spark")
          .load(tempPath)
        
        readData.count() shouldBe 100
      } finally {
        // Clean up configuration
        spark.conf.unset("spark.tantivy4spark.indexWriter.heapSize")
        spark.conf.unset("spark.tantivy4spark.indexWriter.threads")
      }
    }
  }

  test("should use custom index writer configuration from DataFrame options") {
    withTempPath { tempPath =>
      // Generate simple test data
      val testData = spark.range(100).select(
        col("id"),
        concat(lit("Record"), col("id")).as("content")
      )
      
      // Write with custom configuration via DataFrame options (highest precedence)
      testData.write
        .format("tantivy4spark")
        .option("spark.tantivy4spark.indexWriter.heapSize", "150000000") // 150MB
        .option("spark.tantivy4spark.indexWriter.threads", "3") // 3 threads
        .mode(SaveMode.Overwrite)
        .save(tempPath)
      
      // Read back to verify it worked
      val readData = spark.read
        .format("tantivy4spark")
        .load(tempPath)
      
      readData.count() shouldBe 100
    }
  }

  test("should override Spark config with DataFrame options") {
    withTempPath { tempPath =>
      // Set Spark session configuration
      spark.conf.set("spark.tantivy4spark.indexWriter.heapSize", "200000000") // 200MB
      spark.conf.set("spark.tantivy4spark.indexWriter.threads", "4") // 4 threads
      
      try {
        // Generate simple test data
        val testData = spark.range(100).select(
          col("id"),
          concat(lit("Record"), col("id")).as("content")
        )
        
        // DataFrame options should override Spark config
        testData.write
          .format("tantivy4spark")
          .option("spark.tantivy4spark.indexWriter.heapSize", "50000000") // 50MB (overrides 200MB)
          .option("spark.tantivy4spark.indexWriter.threads", "1") // 1 thread (overrides 4)
          .mode(SaveMode.Overwrite)
          .save(tempPath)
        
        // Read back to verify it worked
        val readData = spark.read
          .format("tantivy4spark")
          .load(tempPath)
        
        readData.count() shouldBe 100
      } finally {
        // Clean up configuration
        spark.conf.unset("spark.tantivy4spark.indexWriter.heapSize")
        spark.conf.unset("spark.tantivy4spark.indexWriter.threads")
      }
    }
  }
}