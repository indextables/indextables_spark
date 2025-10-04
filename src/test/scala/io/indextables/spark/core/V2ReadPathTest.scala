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

package io.indextables.spark.core

import org.apache.spark.sql.functions._

import io.indextables.spark.TestBase
import org.scalatest.matchers.should.Matchers._

/**
 * Focused test for V2 DataSource API read path to ensure no serialization issues. This test specifically checks the V2
 * path that was missing from other test coverage.
 */
class V2ReadPathTest extends TestBase {

  ignore("should read data via V2 DataSource API without serialization errors") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // First, write some test data using the standard API (V1 path works)
      val writeData = Seq(
        (1, "First Document", "tech", true),
        (2, "Second Document", "business", false),
        (3, "Third Document", "tech", true)
      ).toDF("id", "title", "category", "is_active")

      // Write data using tantivy4spark format
      writeData.write
        .format("tantivy4spark")
        .save(tempPath)

      println(s"✅ Successfully wrote ${writeData.count()} records to $tempPath")

      // Now test the V2 DataSource API read path specifically
      // This should trigger the V2 TableProvider path including inferSchema and read operations
      try {
        val readDataV2 = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider") // V2 Provider explicitly
          .load(tempPath)

        // This should trigger schema inference via V2 path
        val schema = readDataV2.schema
        schema.fields.length should be > 0
        println(s"✅ V2 Schema inference successful with ${schema.fields.length} fields")

        // This should trigger actual data reading via V2 path - the critical test!
        val rows = readDataV2.collect() // This is where serialization issues would occur
        rows.length should be(3)
        println(s"✅ V2 Data reading successful - read ${rows.length} rows")

        // Test that we can actually access the data
        val firstRow = rows(0)
        firstRow.getAs[Int]("id") should be(1)
        firstRow.getAs[String]("title") should be("First Document")
        println(s"✅ V2 Data access successful - first row ID: ${firstRow.getAs[Int]("id")}")

        // Test DataFrame operations that would trigger more serialization
        val filteredCount = readDataV2.filter(col("category") === "tech").count()
        filteredCount should be(2)
        println(s"✅ V2 Filtering operations successful - tech count: $filteredCount")

        // Test show() which was mentioned in the original error
        println("Testing show() operation...")
        readDataV2.show()
        println("✅ show() operation completed without serialization errors")

      } catch {
        case ex: java.io.NotSerializableException =>
          fail(s"V2 read path has serialization issue: ${ex.getMessage}")
        case ex: Exception =>
          fail(s"V2 read path failed with unexpected error: ${ex.getMessage}")
      }
    }
  }

  ignore("should handle V2 read path with configuration options") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Write test data
      val writeData = Seq(
        (1, "Config Test Document", "testing"),
        (2, "Another Test", "validation")
      ).toDF("id", "title", "type")

      writeData.write
        .format("tantivy4spark")
        .save(tempPath)

      // Test V2 read with various configuration options that should be serialized properly
      try {
        val readDataV2 = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.cache.maxSize", "100000000")
          .option("spark.indextables.cache.queryCache", "true")
          .option("spark.indextables.cache.name", "test-cache")
          .load(tempPath)

        // These operations should all work without serialization issues
        val count = readDataV2.count()
        count should be(2)

        val schema = readDataV2.schema
        schema.fieldNames should contain("id")
        schema.fieldNames should contain("title")
        schema.fieldNames should contain("type")

        println(s"✅ V2 read with configuration options successful - $count rows, ${schema.fields.length} fields")

      } catch {
        case ex: java.io.NotSerializableException =>
          fail(s"V2 read path with options has serialization issue: ${ex.getMessage}")
        case ex: Exception =>
          fail(s"V2 read path with options failed: ${ex.getMessage}")
      }
    }
  }

  ignore("should perform end-to-end V2 write and read cycle") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Test complete V2 write and read cycle
      val writeData = Seq(
        (1, "V2 Write Test", "e2e", 100.0, true),
        (2, "Another V2 Test", "integration", 200.0, false),
        (3, "Final V2 Test", "e2e", 150.0, true)
      ).toDF("id", "title", "test_type", "score", "passed")

      try {
        // Test V2 WRITE path - this is critical to test
        println("Testing V2 write path...")
        writeData.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider") // V2 Provider for writing
          .option("spark.indextables.cache.maxSize", "50000000")
          .option("spark.indextables.cache.name", "v2-write-test-cache")
          .mode("overwrite")
          .save(tempPath)

        println(s"✅ V2 Write successful - wrote ${writeData.count()} records")

        // Test V2 READ path
        println("Testing V2 read path...")
        val readDataV2 = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider") // V2 Provider for reading
          .option("spark.indextables.cache.maxSize", "50000000")
          .option("spark.indextables.cache.name", "v2-read-test-cache")
          .load(tempPath)

        // Verify the data
        val readCount = readDataV2.count()
        readCount should be(3)
        println(s"✅ V2 Read successful - read $readCount records")

        // Test complex operations that would stress serialization
        val e2eTests = readDataV2.filter(col("test_type") === "e2e")
        val e2eCount = e2eTests.count()
        e2eCount should be(2)

        val avgScore = readDataV2.agg(avg("score")).collect().head.getDouble(0)
        avgScore should be(150.0 +- 1.0)

        val passedTests = readDataV2.filter(col("passed") === true).count()
        passedTests should be(2)

        println(s"✅ V2 Complex operations successful - e2e: $e2eCount, avg score: $avgScore, passed: $passedTests")

        // Test show() on V2 read data
        println("Testing show() on V2 data...")
        readDataV2.show()
        println("✅ V2 show() operation successful")

      } catch {
        case ex: java.io.NotSerializableException =>
          fail(s"End-to-end V2 cycle has serialization issue: ${ex.getMessage}")
        case ex: Exception =>
          fail(s"End-to-end V2 cycle failed: ${ex.getMessage}")
      }
    }
  }

  ignore("should handle V2 write with configuration hierarchy") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Test that V2 write respects configuration hierarchy
      val writeData = Seq(
        (1, "Config Hierarchy Test", "validation")
      ).toDF("id", "title", "test_category")

      // Set different configs at different levels to test hierarchy
      spark.conf.set("spark.indextables.cache.maxSize", "30000000") // Spark level
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("spark.indextables.cache.maxSize", "20000000") // Hadoop level

      try {
        // V2 write with options should override the other levels
        writeData.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.cache.maxSize", "40000000") // Highest precedence
          .option("spark.indextables.cache.name", "hierarchy-test-cache")
          .mode("overwrite")
          .save(tempPath)

        println("✅ V2 Write with configuration hierarchy successful")

        // Read back to verify
        val readData = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.cache.maxSize", "40000000")
          .load(tempPath)

        val count = readData.count()
        count should be(1)

        println("✅ V2 Read with configuration hierarchy successful")

        // Clean up
        spark.conf.unset("spark.indextables.cache.maxSize")
        hadoopConf.unset("spark.indextables.cache.maxSize")

      } catch {
        case ex: java.io.NotSerializableException =>
          fail(s"V2 configuration hierarchy test has serialization issue: ${ex.getMessage}")
        case ex: Exception =>
          fail(s"V2 configuration hierarchy test failed: ${ex.getMessage}")
      }
    }
  }
}
