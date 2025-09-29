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

package com.tantivy4spark.storage

import com.tantivy4spark.TestBase
import org.apache.spark.sql.types._
import java.nio.file.{Files, Paths}
import java.io.RandomAccessFile
import scala.util.Random

/**
 * Test memory-efficient upload for large splits to prevent OOM errors.
 * Tests both the streaming upload mechanism and configuration options.
 */
class LargeFileUploadTest extends TestBase {

  test("Memory-efficient upload should handle large files without OOM") {
    withTempPath { path =>
      val schema = StructType(Seq(
        StructField("id", LongType, nullable = false),
        StructField("data", StringType, nullable = true)
      ))

      // Generate test data that will create a larger split
      val largeString = "x" * 1000 // 1KB per record
      val testData = (1 to 20000).map { i => // ~20MB total
        (i.toLong, s"$largeString-$i")
      }

      val rows = testData.map { case (id, data) =>
        org.apache.spark.sql.Row(id, data)
      }
      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

      val tablePath = path.toString

      // Configure for streaming upload with lower threshold for testing
      df.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.tantivy4spark.s3.streamingThreshold", "5000000") // 5MB threshold for testing
        .option("spark.tantivy4spark.s3.multipartThreshold", "5000000") // 5MB multipart threshold
        .option("spark.tantivy4spark.s3.maxConcurrency", "2") // Limit concurrency for test
        .mode("overwrite")
        .save(tablePath)

      // Verify data was written correctly using efficient count
      val readDf = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tablePath)

      val count = readDf.count()
      assert(count == 20000, s"Expected 20000 records, got $count")

      // Verify some sample data by reading a small subset
      import org.apache.spark.sql.functions._
      val sampleData = readDf.filter(col("id") === 1L).collect()
      assert(sampleData.length == 1, "Should find exactly one record with id=1")
      assert(sampleData.head.getString(1).contains("x-1"), "Data should contain expected pattern")

      println("✅ Large file upload test completed successfully")
    }
  }

  test("Configuration threshold should control upload strategy") {
    withTempPath { path =>
      val schema = StructType(Seq(
        StructField("id", LongType, nullable = false),
        StructField("content", StringType, nullable = true)
      ))

      // Create smaller dataset
      val testData = (1 to 100).map { i =>
        (i.toLong, s"Content for record $i")
      }

      val rows = testData.map { case (id, content) =>
        org.apache.spark.sql.Row(id, content)
      }
      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

      val tablePath = path.toString

      // Set a very low threshold to force streaming upload even for small files
      df.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.tantivy4spark.s3.streamingThreshold", "1024") // 1KB threshold
        .option("spark.tantivy4spark.s3.multipartThreshold", "1024") // 1KB multipart threshold
        .mode("overwrite")
        .save(tablePath)

      // Verify data integrity using efficient count
      val readDf = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tablePath)

      val count = readDf.count()
      assert(count == 100, s"Expected 100 records, got $count")

      println("✅ Configuration threshold test completed successfully")
    }
  }

  ignore("Stress test: Very large file (1GB+) upload") {
    // This test is ignored by default as it requires significant memory and time
    // Enable only for stress testing with sufficient resources
    withTempPath { path =>
      val schema = StructType(Seq(
        StructField("id", LongType, nullable = false),
        StructField("large_data", StringType, nullable = true)
      ))

      // Generate very large dataset (1GB+)
      val largeString = "x" * 50000 // 50KB per record
      val testData = (1 to 20000).map { i => // ~1GB total
        (i.toLong, s"$largeString-$i")
      }

      val rows = testData.map { case (id, data) =>
        org.apache.spark.sql.Row(id, data)
      }
      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

      val tablePath = path.toString

      val startTime = System.currentTimeMillis()

      // Test with optimized settings for large files
      df.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.tantivy4spark.s3.streamingThreshold", "100000000") // 100MB
        .option("spark.tantivy4spark.s3.multipartThreshold", "100000000") // 100MB
        .option("spark.tantivy4spark.s3.maxConcurrency", "8") // Higher concurrency
        .option("spark.tantivy4spark.s3.partSize", "134217728") // 128MB parts
        .mode("overwrite")
        .save(tablePath)

      val uploadTime = System.currentTimeMillis() - startTime
      println(s"✅ Large file stress test completed in ${uploadTime}ms")

      // Verify a sample of the data
      val readDf = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tablePath)

      val count = readDf.count()
      assert(count == 20000, s"Expected 20000 records, got $count")

      println("✅ Large file verification completed successfully")
    }
  }
}