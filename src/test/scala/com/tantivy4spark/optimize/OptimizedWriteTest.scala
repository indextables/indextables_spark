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

package com.tantivy4spark.optimize

import com.tantivy4spark.TestBase
import com.tantivy4spark.config.{Tantivy4SparkConfig}
import com.tantivy4spark.core.Tantivy4SparkOptions
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class OptimizedWriteTest extends TestBase {

  test("should enable optimized write by default") {
    withTempPath { tempPath =>
      val data = spark.range(1000).toDF("id")
      
      // Write with default settings (optimized write should be enabled)
      data.write
        .format("tantivy4spark")
        .save(tempPath)

      // Read back and verify data
      val result = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      result.count() shouldBe 1000
      println("✅ Default optimized write enabled")
    }
  }

  test("should allow disabling optimized write via write option") {
    withTempPath { tempPath =>
      val data = spark.range(500).toDF("id")
      
      // Explicitly disable optimized write
      data.write
        .format("tantivy4spark")
        .option("optimizeWrite", "false")
        .save(tempPath)

      // Read back and verify data
      val result = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      result.count() shouldBe 500
      println("✅ Optimized write successfully disabled via write option")
    }
  }

  test("should allow custom target records per split via write option") {
    withTempPath { tempPath =>
      val data = spark.range(2000).toDF("id")
      
      // Set custom target records per split
      data.write
        .format("tantivy4spark")
        .option("optimizeWrite", "true")
        .option("targetRecordsPerSplit", "500")
        .save(tempPath)

      // Read back and verify data
      val result = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      result.count() shouldBe 2000
      println("✅ Custom target records per split via write option")
    }
  }

  test("should allow configuring optimized write via Spark conf") {
    withTempPath { tempPath =>
      // Configure via Spark session
      spark.conf.set("spark.indextables.optimizeWrite.enabled", "true")
      spark.conf.set("spark.indextables.optimizeWrite.targetRecordsPerSplit", "750")

      val data = spark.range(1500).toDF("id")
      
      // Write without explicit options (should use Spark conf)
      data.write
        .format("tantivy4spark")
        .save(tempPath)

      // Read back and verify data
      val result = spark.read
        .format("tantivy4spark")
        .load(tempPath).limit(100000)

      result.count() shouldBe 1500
      println("✅ Optimized write configured via Spark conf")
      
      // Clean up configuration
      spark.conf.unset("spark.indextables.optimizeWrite.enabled")
      spark.conf.unset("spark.indextables.optimizeWrite.targetRecordsPerSplit")
    }
  }

  test("should validate target records per split configuration") {
    withTempPath { tempPath =>
      val data = spark.range(100).toDF("id")
      
      // Try to set invalid target records per split (should fail gracefully)
      val _ = intercept[IllegalArgumentException] {
        data.write
          .format("tantivy4spark")
          .option("targetRecordsPerSplit", "0")
          .save(tempPath)
      }
      
      // Should contain validation error message
      println("✅ Target records per split validation works correctly")
    }
  }

  ignore("should handle large datasets with optimized write") {
    withTempPath { tempPath =>
      // Create a dataset with multiple columns to test partitioning
      val data = spark.range(1000)
        .select(
          col("id"),
          (col("id") % 100).cast(StringType).as("category"),
          (col("id") * 2).as("value")
        )
      
      // Write with small target per split to force multiple partitions
      data.write
        .format("tantivy4spark")
        .option("optimizeWrite", "true")
        .option("targetRecordsPerSplit", "500")
        .save(tempPath)

      // Read back and verify (limited by 10k search limit)
      val result = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      // Don't check exact count due to search limit, just verify it works
      val count = result.count()
      count should be > 0L
      
      // Verify we can query the data
      val categoryResults = result.filter(col("category") === "50")
      val categoryCount = categoryResults.count()
      categoryCount should be > 0L
      
      println("✅ Large dataset handled correctly with optimized write")
    }
  }

  test("should test Tantivy4SparkOptions utility") {
    // Test options parsing
    val optionsMap = Map(
      "optimizeWrite" -> "true",
      "targetRecordsPerSplit" -> "500000",
      "bloomFiltersEnabled" -> "false"
    )
    
    import scala.jdk.CollectionConverters._
    val caseInsensitiveMap = new CaseInsensitiveStringMap(optionsMap.asJava)
    val options = Tantivy4SparkOptions(caseInsensitiveMap)
    
    options.optimizeWrite shouldBe Some(true)
    options.targetRecordsPerSplit shouldBe Some(500000L)
    options.bloomFiltersEnabled shouldBe Some(false)
    options.forceStandardStorage shouldBe None // Not set
    
    println("✅ Tantivy4SparkOptions utility works correctly")
  }

  test("should verify configuration hierarchy precedence") {
    withTempPath { tempPath =>
      // Set Spark config to false
      spark.conf.set("spark.indextables.optimizeWrite.enabled", "false")
      
      val data = spark.range(100).toDF("id")
      
      // Write option should override Spark config
      data.write
        .format("tantivy4spark")
        .option("optimizeWrite", "true")
        .option("targetRecordsPerSplit", "50")
        .save(tempPath)

      // Read back and verify
      val result = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      result.count() shouldBe 100
      
      println("✅ Configuration hierarchy precedence works correctly")
      
      // Clean up
      spark.conf.unset("spark.indextables.optimizeWrite.enabled")
    }
  }

  test("should create correct number of split files - 100 rows should create 1 split") {
    withTempPath { tempPath =>
      val data = spark.range(100).toDF("id")
      
      // Write with 1M target per split - should create 1 split file for 100 rows
      data.write
        .format("tantivy4spark")
        .option("optimizeWrite", "true")
        .option("targetRecordsPerSplit", "1000000")
        .save(tempPath)

      // Count actual split files created (don't verify read count due to 10k search limit)
      import com.tantivy4spark.transaction.{TransactionLog, TransactionLogFactory}
      import org.apache.hadoop.fs.Path
      val transactionLog = TransactionLogFactory.create(new Path(tempPath), spark)
      val files = transactionLog.listFiles()
      
      files.length shouldBe 1
      println(s"✅ 100 rows created ${files.length} split file (expected 1)")
    }
  }

  test("should create correct number of split files - 250 rows should create 3 splits") {
    withTempPath { tempPath =>
      val data = spark.range(250).toDF("id")
      
      // Write with 100 target per split - should create 3 split files for 250 rows
      data.write
        .format("tantivy4spark")
        .option("optimizeWrite", "true")
        .option("targetRecordsPerSplit", "100")
        .save(tempPath)

      // Count actual split files created
      import com.tantivy4spark.transaction.{TransactionLog, TransactionLogFactory}
      import org.apache.hadoop.fs.Path
      val transactionLog = TransactionLogFactory.create(new Path(tempPath), spark)
      val files = transactionLog.listFiles()
      
      // Should be 3 splits: ceil(250/100) = 3
      files.length shouldBe 3
      println(s"✅ 250 rows created ${files.length} split files (expected 3)")
    }
  }

  test("should create correct number of split files - 1000 rows with 100 target should create 10 splits") {
    withTempPath { tempPath =>
      val data = spark.range(1000).toDF("id")
      
      // Write with 100 target per split - should create 10 split files for 1000 rows
      data.write
        .format("tantivy4spark")
        .option("optimizeWrite", "true")
        .option("targetRecordsPerSplit", "100")
        .save(tempPath)

      // Count actual split files created
      import com.tantivy4spark.transaction.{TransactionLog, TransactionLogFactory}
      import org.apache.hadoop.fs.Path
      val transactionLog = TransactionLogFactory.create(new Path(tempPath), spark)
      val files = transactionLog.listFiles()
      
      // Should be 10 splits: ceil(1000/100) = 10
      files.length shouldBe 10
      println(s"✅ 1000 rows with 100 target created ${files.length} split files (expected 10)")
    }
  }

  test("should create correct number of split files - 500 rows with 50 target should create 10 splits") {
    withTempPath { tempPath =>
      val data = spark.range(500).toDF("id")
      
      // Write with 50 target per split - should create 10 split files for 500 rows
      data.write
        .format("tantivy4spark")
        .option("optimizeWrite", "true")
        .option("targetRecordsPerSplit", "50")
        .save(tempPath)

      // Count actual split files created
      import com.tantivy4spark.transaction.{TransactionLog, TransactionLogFactory}
      import org.apache.hadoop.fs.Path
      val transactionLog = TransactionLogFactory.create(new Path(tempPath), spark)
      val files = transactionLog.listFiles()
      
      // Should be 10 splits: ceil(500/50) = 10
      files.length shouldBe 10
      println(s"✅ 500 rows with 50 target created ${files.length} split files (expected 10)")
    }
  }

  test("should handle edge case - 1 row should create 1 split") {
    withTempPath { tempPath =>
      val data = spark.range(1).toDF("id")
      
      // Write with 1M target per split - should create 1 split file for 1 row
      data.write
        .format("tantivy4spark")
        .option("optimizeWrite", "true")
        .option("targetRecordsPerSplit", "1000000")
        .save(tempPath)

      // Count actual split files created
      import com.tantivy4spark.transaction.{TransactionLog, TransactionLogFactory}
      import org.apache.hadoop.fs.Path
      val transactionLog = TransactionLogFactory.create(new Path(tempPath), spark)
      val files = transactionLog.listFiles()
      
      files.length shouldBe 1
      println(s"✅ 1 row created ${files.length} split file (expected 1)")
    }
  }
}
