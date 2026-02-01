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
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * Tests for IS NULL / IS NOT NULL filter support.
 *
 * These filters require FAST field configuration in Tantivy to work correctly. The validation runs on the driver to
 * provide clear error messages.
 */
class NullFilterValidationTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  test("IS NOT NULL with fast field should return only non-null records") {
    withTempPath { path =>
      // Create test data with some null values
      val data = spark
        .createDataFrame(
          Seq(
            (1, Some("alice@example.com"), 100),
            (2, None, 200),
            (3, Some("bob@example.com"), 300),
            (4, None, 400),
            (5, Some("carol@example.com"), 500)
          )
        )
        .toDF("id", "email", "score")

      // Write with email configured as fast field
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "id,email,score")
        .mode("overwrite")
        .save(path)

      println(s"=== DEBUG: Data written to $path ===")

      // First, read all data without filter to verify data exists
      val allData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      println(s"=== DEBUG: All data (no filter) ===")
      allData.show()
      println(s"Total count: ${allData.count()}")

      // Read and filter for non-null emails
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("email").isNotNull)

      println(s"=== DEBUG: After IS NOT NULL filter ===")
      result.show()

      val rows = result.collect()
      println(s"IS NOT NULL returned ${rows.length} records (expected 3)")
      rows.foreach(r => println(s"  Row: id=${r.getInt(0)}, email=${r.getString(1)}, score=${r.getInt(2)}"))

      rows.length shouldBe 3
      rows.map(_.getInt(0)).toSet shouldBe Set(1, 3, 5)
    }
  }

  test("IS NULL with fast field should return only null records") {
    withTempPath { path =>
      // Create test data with some null values
      val data = spark
        .createDataFrame(
          Seq(
            (1, Some("alice@example.com"), 100),
            (2, None, 200),
            (3, Some("bob@example.com"), 300),
            (4, None, 400),
            (5, Some("carol@example.com"), 500)
          )
        )
        .toDF("id", "email", "score")

      // Write with email configured as fast field
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "id,email,score")
        .mode("overwrite")
        .save(path)

      // Read and filter for null emails
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("email").isNull)

      val rows = result.collect()
      rows.length shouldBe 2
      rows.map(_.getInt(0)).toSet shouldBe Set(2, 4)
      println(s"IS NULL returned ${rows.length} records (expected 2)")
    }
  }

  test("IS NOT NULL on non-fast field should be handled by Spark") {
    withTempPath { path =>
      // Create test data (no nulls in name column)
      val data = spark
        .createDataFrame(
          Seq(
            (1, "alice", 100),
            (2, "bob", 200)
          )
        )
        .toDF("id", "name", "score")

      // Write with only score as fast field (name is NOT fast)
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(path)

      // IS NOT NULL on non-fast field is NOT pushed down to Tantivy
      // Instead, Spark handles the filter after reading all data
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("name").isNotNull)
        .collect()

      // Since there are no nulls in the data, all 2 records should be returned
      result.length shouldBe 2
      result.map(_.getInt(0)).toSet shouldBe Set(1, 2)
      println(s"IS NOT NULL on non-fast field returned ${result.length} records (handled by Spark)")
    }
  }

  test("IS NULL on non-fast field should be handled by Spark") {
    withTempPath { path =>
      // Create test data (no nulls in name column)
      val data = spark
        .createDataFrame(
          Seq(
            (1, "alice", 100),
            (2, "bob", 200)
          )
        )
        .toDF("id", "name", "score")

      // Write with only score as fast field (name is NOT fast)
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(path)

      // IS NULL on non-fast field is NOT pushed down to Tantivy
      // Instead, Spark handles the filter after reading all data
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("name").isNull)
        .collect()

      // Since there are no nulls in the data, 0 records should be returned
      result.length shouldBe 0
      println(s"IS NULL on non-fast field returned ${result.length} records (handled by Spark)")
    }
  }

  test("IS NULL/IS NOT NULL on non-existent field should throw error") {
    withTempPath { path =>
      // Create test data
      val data = spark
        .createDataFrame(
          Seq(
            (1, "alice", 100),
            (2, "bob", 200)
          )
        )
        .toDF("id", "name", "score")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "id,name,score")
        .mode("overwrite")
        .save(path)

      // Attempt to use IS NOT NULL on non-existent field
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Spark's schema validation throws ExtendedAnalysisException before our validation kicks in
      // Either exception type is acceptable - the key is that an error occurs
      val exception = intercept[Exception] {
        result.filter(col("nonexistent_field").isNotNull).collect()
      }

      // The error message should reference the non-existent field
      exception.getMessage should include("nonexistent_field")
      println(s"Got expected error: ${exception.getClass.getSimpleName}: ${exception.getMessage}")
    }
  }

  test("Combined filter with IS NOT NULL and equality should work") {
    withTempPath { path =>
      // Create test data
      val data = spark
        .createDataFrame(
          Seq(
            (1, Some("active"), 100),
            (2, None, 200),
            (3, Some("inactive"), 300),
            (4, Some("active"), 400),
            (5, None, 500)
          )
        )
        .toDF("id", "status", "score")

      // Write with all fields as fast
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "id,status,score")
        .mode("overwrite")
        .save(path)

      // Filter: status IS NOT NULL AND score > 200
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("status").isNotNull && col("score") > 200)

      val rows = result.collect()
      rows.length shouldBe 2
      rows.map(_.getInt(0)).toSet shouldBe Set(3, 4)
      println(s"Combined filter returned ${rows.length} records (expected 2)")
    }
  }

  test("Aggregate with IS NOT NULL filter should work when field is fast") {
    withTempPath { path =>
      // Create test data
      val data = spark
        .createDataFrame(
          Seq(
            (1, Some("active"), 100),
            (2, None, 200),
            (3, Some("inactive"), 300),
            (4, Some("active"), 400),
            (5, None, 500)
          )
        )
        .toDF("id", "status", "score")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "id,status,score")
        .mode("overwrite")
        .save(path)

      // Count where status IS NOT NULL
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("status").isNotNull)
        .count()

      result shouldBe 3
      println(s"COUNT with IS NOT NULL filter: $result (expected 3)")
    }
  }

  test("OR filter with IS NULL should work") {
    withTempPath { path =>
      // Create test data
      val data = spark
        .createDataFrame(
          Seq(
            (1, Some("alice"), 100),
            (2, None, 200),
            (3, Some("bob"), 300),
            (4, None, 50),
            (5, Some("carol"), 500)
          )
        )
        .toDF("id", "name", "score")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "id,name,score")
        .mode("overwrite")
        .save(path)

      // Filter: name IS NULL OR score < 150
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("name").isNull || col("score") < 150)

      val rows = result.collect()
      // id=1 (score=100), id=2 (null name), id=4 (null name, score=50)
      rows.length shouldBe 3
      rows.map(_.getInt(0)).toSet shouldBe Set(1, 2, 4)
      println(s"OR filter with IS NULL returned ${rows.length} records (expected 3)")
    }
  }

  test("SELECT * with IS NULL on text field (non-FAST) should work via Spark post-filtering") {
    withTempPath { path =>
      // Create test data with some null text values
      // This tests the exact scenario: SELECT * FROM table WHERE text_field IS NULL
      // where text_field is a TEXT type (not FAST)
      val data = spark
        .createDataFrame(
          Seq(
            (1L, Some("Hello world, this is a test message")),
            (2L, None), // null text
            (3L, Some("Another message with some content")),
            (4L, None), // null text
            (5L, Some("Final message here"))
          )
        )
        .toDF("id", "message_content")

      // Write with message_content as TEXT (not FAST) - typical for log/message fields
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.message_content", "text")
        // Note: NOT adding message_content to fastfields
        .mode("overwrite")
        .save(path)

      // This is the exact query pattern that was failing:
      // SELECT * FROM table WHERE text_field IS NULL
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("message_content").isNull)

      val rows = result.collect()

      // Should return 2 rows (id=2 and id=4 have null message_content)
      rows.length shouldBe 2
      rows.map(_.getLong(0)).toSet shouldBe Set(2L, 4L)
      println(s"SELECT * with IS NULL on TEXT field returned ${rows.length} records (expected 2)")
    }
  }

  test("COUNT with IS NULL on text field (non-FAST) should work via Spark post-filtering") {
    withTempPath { path =>
      // Test aggregate with IS NULL on non-FAST text field
      val data = spark
        .createDataFrame(
          Seq(
            (1L, Some("Message one")),
            (2L, None),
            (3L, Some("Message three")),
            (4L, None),
            (5L, Some("Message five"))
          )
        )
        .toDF("id", "log_message")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.log_message", "text")
        .mode("overwrite")
        .save(path)

      // COUNT with IS NULL on non-FAST text field should work
      // Spark handles the filter via post-filtering
      val count = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("log_message").isNull)
        .count()

      count shouldBe 2L
      println(s"COUNT with IS NULL on TEXT field: $count (expected 2)")
    }
  }

  test("SELECT * with IS NOT NULL on text field (non-FAST) should work via Spark post-filtering") {
    withTempPath { path =>
      // Test IS NOT NULL on non-FAST TEXT field
      val data = spark
        .createDataFrame(
          Seq(
            (1L, Some("Hello world, this is a test message")),
            (2L, None), // null text
            (3L, Some("Another message with some content")),
            (4L, None), // null text
            (5L, Some("Final message here"))
          )
        )
        .toDF("id", "message_content")

      // Write with message_content as TEXT (not FAST)
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.message_content", "text")
        .mode("overwrite")
        .save(path)

      // SELECT * WHERE text_field IS NOT NULL
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("message_content").isNotNull)

      val rows = result.collect()

      // Should return 3 rows (id=1, 3, 5 have non-null message_content)
      rows.length shouldBe 3
      rows.map(_.getLong(0)).toSet shouldBe Set(1L, 3L, 5L)
      println(s"SELECT * with IS NOT NULL on TEXT field returned ${rows.length} records (expected 3)")
    }
  }

  test("COUNT with IS NOT NULL on text field (non-FAST) should work via Spark post-filtering") {
    withTempPath { path =>
      // Test aggregate with IS NOT NULL on non-FAST text field
      val data = spark
        .createDataFrame(
          Seq(
            (1L, Some("Message one")),
            (2L, None),
            (3L, Some("Message three")),
            (4L, None),
            (5L, Some("Message five"))
          )
        )
        .toDF("id", "log_message")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.log_message", "text")
        .mode("overwrite")
        .save(path)

      // COUNT with IS NOT NULL on non-FAST text field should work
      val count = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("log_message").isNotNull)
        .count()

      count shouldBe 3L
      println(s"COUNT with IS NOT NULL on TEXT field: $count (expected 3)")
    }
  }
}
