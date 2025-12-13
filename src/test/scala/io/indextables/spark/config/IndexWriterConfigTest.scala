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

package io.indextables.spark.config

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import io.indextables.spark.TestBase

class IndexWriterConfigTest extends TestBase {

  test("should use default index writer configuration") {
    withTempPath { tempPath =>
      // Generate simple test data
      val testData = spark
        .range(100)
        .select(
          col("id"),
          concat(lit("Record"), col("id")).as("content")
        )

      // Write without any custom configuration - should use defaults (100MB heap, 2 threads)
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read back to verify it worked
      val readData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      readData.count() shouldBe 100
    }
  }

  test("should use custom index writer configuration from Spark config") {
    withTempPath { tempPath =>
      // Set custom index writer configuration via Spark session
      spark.conf.set("spark.indextables.indexWriter.heapSize", "200000000") // 200MB
      spark.conf.set("spark.indextables.indexWriter.threads", "4")          // 4 threads

      try {
        // Generate simple test data
        val testData = spark
          .range(100)
          .select(
            col("id"),
            concat(lit("Record"), col("id")).as("content")
          )

        // Write with custom configuration
        testData.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode(SaveMode.Overwrite)
          .save(tempPath)

        // Read back to verify it worked
        val readData = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(tempPath)

        readData.count() shouldBe 100
      } finally {
        // Clean up configuration
        spark.conf.unset("spark.indextables.indexWriter.heapSize")
        spark.conf.unset("spark.indextables.indexWriter.threads")
      }
    }
  }

  test("should use custom index writer configuration from DataFrame options") {
    withTempPath { tempPath =>
      // Generate simple test data
      val testData = spark
        .range(100)
        .select(
          col("id"),
          concat(lit("Record"), col("id")).as("content")
        )

      // Write with custom configuration via DataFrame options (highest precedence)
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexWriter.heapSize", "150000000") // 150MB
        .option("spark.indextables.indexWriter.threads", "3")          // 3 threads
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read back to verify it worked
      val readData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      readData.count() shouldBe 100
    }
  }

  test("should override Spark config with DataFrame options") {
    withTempPath { tempPath =>
      // Set Spark session configuration
      spark.conf.set("spark.indextables.indexWriter.heapSize", "200000000") // 200MB
      spark.conf.set("spark.indextables.indexWriter.threads", "4")          // 4 threads

      try {
        // Generate simple test data
        val testData = spark
          .range(100)
          .select(
            col("id"),
            concat(lit("Record"), col("id")).as("content")
          )

        // DataFrame options should override Spark config
        testData.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.indexWriter.heapSize", "50000000") // 50MB (overrides 200MB)
          .option("spark.indextables.indexWriter.threads", "1")         // 1 thread (overrides 4)
          .mode(SaveMode.Overwrite)
          .save(tempPath)

        // Read back to verify it worked
        val readData = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(tempPath)

        readData.count() shouldBe 100
      } finally {
        // Clean up configuration
        spark.conf.unset("spark.indextables.indexWriter.heapSize")
        spark.conf.unset("spark.indextables.indexWriter.threads")
      }
    }
  }

  test("should use batch writing by default") {
    withTempPath { tempPath =>
      // Generate test data that will require multiple batches (default batch size is 10,000)
      val testData = spark
        .range(25000)
        .select(
          col("id"),
          concat(lit("Record"), col("id")).as("content")
        )

      // Write with default batch settings
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read back to verify all documents were written correctly
      val readData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      readData.count() shouldBe 25000
    }
  }

  test("should support custom batch size configuration") {
    withTempPath { tempPath =>
      // Generate test data
      val testData = spark
        .range(1000)
        .select(
          col("id"),
          concat(lit("Record"), col("id")).as("content")
        )

      // Write with custom batch size (smaller for testing)
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexWriter.batchSize", "100") // Small batch size
        .option("spark.indextables.indexWriter.useBatch", "true")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read back to verify it worked
      val readData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      readData.count() shouldBe 1000
    }
  }

  test("should support disabling batch writing") {
    withTempPath { tempPath =>
      // Generate test data
      val testData = spark
        .range(100)
        .select(
          col("id"),
          concat(lit("Record"), col("id")).as("content")
        )

      // Write with batch writing disabled
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexWriter.useBatch", "false") // Disable batch writing
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read back to verify it worked
      val readData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      readData.count() shouldBe 100
    }
  }

  test("should use default maxBatchBufferSize configuration (90MB)") {
    withTempPath { tempPath =>
      // Generate test data with moderate-sized documents
      val testData = spark
        .range(100)
        .select(
          col("id"),
          concat(lit("Content_"), col("id")).as("content")
        )

      // Write without explicit maxBatchBufferSize - should use 90MB default
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read back to verify it worked
      val readData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      readData.count() shouldBe 100
    }
  }

  test("should support custom maxBatchBufferSize configuration") {
    withTempPath { tempPath =>
      // Generate test data
      val testData = spark
        .range(500)
        .select(
          col("id"),
          concat(lit("Record_"), col("id")).as("content")
        )

      // Write with a smaller maxBatchBufferSize to force more frequent flushes
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexWriter.maxBatchBufferSize", "1M") // 1MB - very small
        .option("spark.indextables.indexWriter.batchSize", "10000")       // Large doc count threshold
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read back to verify all documents were written correctly
      val readData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      readData.count() shouldBe 500
    }
  }

  test("should flush early when buffer size exceeds maxBatchBufferSize with large documents") {
    withTempPath { tempPath =>
      // Generate test data with large documents (10KB each)
      // At 10KB per doc, 100 docs = 1MB, so 500 docs would be ~5MB
      val largeContent = "X" * 10000 // 10KB string

      val testData = spark
        .range(500)
        .select(
          col("id"),
          concat(lit(largeContent), col("id")).as("content")
        )

      // Write with a small maxBatchBufferSize (2MB) but large batchSize (10000)
      // This should trigger early flushes based on buffer size, not document count
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexWriter.maxBatchBufferSize", "2M")   // 2MB buffer limit
        .option("spark.indextables.indexWriter.batchSize", "10000")         // Would never trigger by count
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read back to verify all documents were written correctly
      val readData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      readData.count() shouldBe 500
    }
  }

  test("should handle very large documents that individually approach buffer limit") {
    withTempPath { tempPath =>
      // Generate test data with very large documents (500KB each)
      val veryLargeContent = "Y" * 500000 // 500KB string

      val testData = spark
        .range(50)
        .select(
          col("id"),
          concat(lit(veryLargeContent), col("id")).as("content")
        )

      // Write with default maxBatchBufferSize (90MB)
      // 50 docs * 500KB = 25MB, should work fine
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexWriter.batchSize", "100") // Small batch count
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read back to verify all documents were written correctly
      val readData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      readData.count() shouldBe 50
    }
  }

  test("should respect maxBatchBufferSize with size string formats") {
    withTempPath { tempPath =>
      val testData = spark
        .range(100)
        .select(
          col("id"),
          concat(lit("Content_"), col("id")).as("content")
        )

      // Test various size formats: "50M", "50m", "51200K", "51200k"
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexWriter.maxBatchBufferSize", "50M") // 50MB
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      val readData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      readData.count() shouldBe 100
    }
  }
}
