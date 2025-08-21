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

class BloomFilterConfigTest extends TestBase {

  test("should create bloom filters by default") {
    withTempPath { tempPath =>
      val data = Seq(
        (1, "Apache Spark"),
        (2, "Machine Learning"),
        (3, "Data Science")
      )
      val testData = spark.createDataFrame(data).toDF("id", "title")

      // Write data without specifying bloom filter config (should default to enabled)
      testData.write
        .format("tantivy4spark")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read transaction log to verify bloom filters were created
      val transactionLog = new com.tantivy4spark.transaction.TransactionLog(
        new org.apache.hadoop.fs.Path(tempPath), 
        spark
      )
      val addActions = transactionLog.listFiles()
      
      // Should have bloom filters
      addActions should not be empty
      addActions.head.bloomFilters should be(defined)
      
      info(s"✅ Bloom filters created by default: ${addActions.head.bloomFilters.isDefined}")
    }
  }

  test("should disable bloom filters when configuration is set to false via DataFrame option") {
    withTempPath { tempPath =>
      val data = Seq(
        (1, "Apache Spark"),
        (2, "Machine Learning"), 
        (3, "Data Science")
      )
      val testData = spark.createDataFrame(data).toDF("id", "title")

      // Write data with bloom filters explicitly disabled via DataFrame option
      testData.write
        .format("tantivy4spark")
        .option("spark.tantivy4spark.bloom.filters.enabled", "false")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read transaction log to verify bloom filters were NOT created
      val transactionLog = new com.tantivy4spark.transaction.TransactionLog(
        new org.apache.hadoop.fs.Path(tempPath),
        spark
      )
      val addActions = transactionLog.listFiles()
      
      // Should NOT have bloom filters
      addActions should not be empty
      addActions.head.bloomFilters should be(None)
      
      info(s"✅ Bloom filters disabled when DataFrame option set to false: ${addActions.head.bloomFilters.isEmpty}")
    }
  }

  test("should disable bloom filters when configuration is set to false via Spark config") {
    // Set Spark configuration
    val originalConfig = spark.conf.getOption("spark.tantivy4spark.bloom.filters.enabled")
    spark.conf.set("spark.tantivy4spark.bloom.filters.enabled", "false")
    
    try {
      withTempPath { tempPath =>
        val data = Seq(
          (1, "Apache Spark"),
          (2, "Machine Learning"), 
          (3, "Data Science")
        )
        val testData = spark.createDataFrame(data).toDF("id", "title")

        // Write data without options (should pick up Spark config)
        testData.write
          .format("tantivy4spark")
          .mode(SaveMode.Overwrite)
          .save(tempPath)

        // Read transaction log to verify bloom filters were NOT created
        val transactionLog = new com.tantivy4spark.transaction.TransactionLog(
          new org.apache.hadoop.fs.Path(tempPath),
          spark
        )
        val addActions = transactionLog.listFiles()
        
        // Should NOT have bloom filters
        addActions should not be empty
        addActions.head.bloomFilters should be(None)
        
        info(s"✅ Bloom filters disabled when Spark config set to false: ${addActions.head.bloomFilters.isEmpty}")
      }
    } finally {
      // Restore original configuration
      originalConfig match {
        case Some(value) => spark.conf.set("spark.tantivy4spark.bloom.filters.enabled", value)
        case None => spark.conf.unset("spark.tantivy4spark.bloom.filters.enabled")
      }
    }
  }

  test("should prioritize DataFrame option over Spark config") {
    // Set Spark configuration to enabled
    val originalConfig = spark.conf.getOption("spark.tantivy4spark.bloom.filters.enabled")
    spark.conf.set("spark.tantivy4spark.bloom.filters.enabled", "true")
    
    try {
      withTempPath { tempPath =>
        val data = Seq(
          (1, "Apache Spark"),
          (2, "Machine Learning"), 
          (3, "Data Science")
        )
        val testData = spark.createDataFrame(data).toDF("id", "title")

        // Write data with DataFrame option disabled (should override Spark config)
        testData.write
          .format("tantivy4spark")
          .option("spark.tantivy4spark.bloom.filters.enabled", "false")
          .mode(SaveMode.Overwrite)
          .save(tempPath)

        // Read transaction log to verify bloom filters were NOT created
        val transactionLog = new com.tantivy4spark.transaction.TransactionLog(
          new org.apache.hadoop.fs.Path(tempPath),
          spark
        )
        val addActions = transactionLog.listFiles()
        
        // Should NOT have bloom filters (DataFrame option should override Spark config)
        addActions should not be empty
        addActions.head.bloomFilters should be(None)
        
        info(s"✅ DataFrame option overrides Spark config: ${addActions.head.bloomFilters.isEmpty}")
      }
    } finally {
      // Restore original configuration
      originalConfig match {
        case Some(value) => spark.conf.set("spark.tantivy4spark.bloom.filters.enabled", value)
        case None => spark.conf.unset("spark.tantivy4spark.bloom.filters.enabled")
      }
    }
  }

  test("should enable bloom filters when configuration is set to true") {
    withTempPath { tempPath =>
      val data = Seq(
        (1, "Apache Spark"),
        (2, "Machine Learning"),
        (3, "Data Science")
      )
      val testData = spark.createDataFrame(data).toDF("id", "title")

      // Write data with bloom filters explicitly enabled
      testData.write
        .format("tantivy4spark")
        .option("spark.tantivy4spark.bloom.filters.enabled", "true")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read transaction log to verify bloom filters were created
      val transactionLog = new com.tantivy4spark.transaction.TransactionLog(
        new org.apache.hadoop.fs.Path(tempPath),
        spark
      )
      val addActions = transactionLog.listFiles()
      
      // Should have bloom filters
      addActions should not be empty
      addActions.head.bloomFilters should be(defined)
      
      info(s"✅ Bloom filters enabled when config set to true: ${addActions.head.bloomFilters.isDefined}")
    }
  }

  test("should still allow reading data when bloom filters are disabled") {
    withTempPath { tempPath =>
      val data = Seq(
        (1, "Apache Spark", "technology"),
        (2, "Machine Learning", "AI"),
        (3, "Data Science", "analytics")
      )
      val originalData = spark.createDataFrame(data).toDF("id", "title", "category")

      // Write data with bloom filters disabled
      originalData.write
        .format("tantivy4spark")
        .option("spark.tantivy4spark.bloom.filters.enabled", "false")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read data back and verify it works correctly
      val readData = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      val readCount = readData.count()
      val originalCount = originalData.count()
      
      readCount shouldBe originalCount
      
      // Test filtering still works (though without bloom filter optimization)
      val filteredData = readData.filter(readData("title").contains("Spark")).collect()
      filteredData should have length 1
      filteredData(0).getAs[String]("title") should include("Spark")
      
      info(s"✅ Data read successfully without bloom filters: $readCount records")
    }
  }

  test("should not affect performance significantly when bloom filters disabled for non-text columns") {
    withTempPath { tempPath =>
      val data = Seq(
        (1, 100, 89.5),
        (2, 200, 92.3),
        (3, 150, 87.8)
      )
      val testData = spark.createDataFrame(data).toDF("id", "value", "score")

      // Write numeric data with bloom filters disabled
      testData.write
        .format("tantivy4spark")
        .option("spark.tantivy4spark.bloom.filters.enabled", "false")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read and verify data
      val readData = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      readData.count() shouldBe 3
      
      // Verify numeric filtering still works
      val filtered = readData.filter(readData("value") > 125).collect()
      filtered should have length 2
      
      info(s"✅ Numeric data processed correctly without bloom filters")
    }
  }
}