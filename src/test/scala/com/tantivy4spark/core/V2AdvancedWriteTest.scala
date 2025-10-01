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
import org.scalatest.matchers.should.Matchers._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

/** Tests for advanced V2 write operations including overwrite with filters, truncate, and error handling. */
class V2AdvancedWriteTest extends TestBase {

  ignore("should support truncate operation via V2 API") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create initial data
      val initialData = Seq(
        (1, "Initial Data 1", "original"),
        (2, "Initial Data 2", "original"),
        (3, "Initial Data 3", "original")
      ).toDF("id", "content", "category")

      initialData.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(tempPath)

      // Verify initial data
      val initialRead = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tempPath)
      initialRead.count() should be(3)

      // Use truncate via SaveMode.Overwrite (which triggers truncate in V2 API)
      val newData = Seq(
        (10, "New Data 1", "replacement"),
        (20, "New Data 2", "replacement")
      ).toDF("id", "content", "category")

      newData.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode(SaveMode.Overwrite) // This should trigger truncate
        .save(tempPath)

      // Verify truncate worked
      val finalRead = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tempPath)

      val finalRows = finalRead.collect()
      finalRows.length should be(2)

      // Verify only new data exists
      val categories = finalRows.map(_.getAs[String]("category")).distinct
      categories should contain only "replacement"
    }
  }

  ignore("should handle write errors and abort scenarios") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create valid initial data
      val validData = Seq(
        (1, "Valid Data", "test")
      ).toDF("id", "content", "category")

      validData.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(tempPath)

      // Verify initial write worked
      val initialRead = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tempPath)
      initialRead.count() should be(1)

      // Test that invalid configuration doesn't corrupt existing data
      val moreData = Seq(
        (2, "More Data", "test")
      ).toDF("id", "content", "category")

      // Write with potentially problematic configuration
      try
        moreData.write
          .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
          .option("spark.indextables.cache.maxSize", "-1") // Invalid config
          .mode("append")
          .save(tempPath)
      catch {
        case _: Exception => // Expected to potentially fail
      }

      // Verify original data is still intact
      val finalRead = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tempPath)
      finalRead.count() should be >= 1L // At least original data should exist
    }
  }

  ignore("should support append mode via V2 API") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Initial write
      val batch1 = Seq(
        (1, "Batch 1 Data", "first")
      ).toDF("id", "content", "batch")

      batch1.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(tempPath)

      // Append additional data
      val batch2 = Seq(
        (2, "Batch 2 Data", "second"),
        (3, "Batch 3 Data", "third")
      ).toDF("id", "content", "batch")

      batch2.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("append")
        .save(tempPath)

      // Verify all data exists
      val finalRead = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tempPath)

      val finalCount = finalRead.count()
      finalCount should be(3)

      // Verify all batches are present
      val batches = finalRead.select("batch").distinct().collect().map(_.getAs[String]("batch"))
      batches should contain("first")
      batches should contain("second")
      batches should contain("third")
    }
  }

  ignore("should handle large batch writes") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create larger dataset to test batch processing
      val largeData = (1 to 1000)
        .map(i => (i, s"Large Dataset Record $i", "bulk", i * 1.5, i % 2 == 0))
        .toDF("id", "description", "category", "value", "flag")

      largeData.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.indextables.optimizeWrite.targetRecordsPerSplit", "250") // Force multiple splits
        .mode("overwrite")
        .save(tempPath)

      // Verify all data was written correctly
      val readBack = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tempPath)

      readBack.count() should be(1000)

      // Test filtering on large dataset
      val evenRecords = readBack.filter(col("flag") === true).count()
      evenRecords should be(500)

      // Test aggregation on large dataset
      val avgValue = readBack.agg(avg("value")).collect().head.getDouble(0)
      avgValue should be(750.75 +- 0.1) // (1+2+...+1000) * 1.5 / 1000 / 2
    }
  }

  ignore("should handle concurrent write scenarios") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Initial data
      val initialData = Seq((1, "Initial", "base")).toDF("id", "content", "type")
      initialData.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(tempPath)

      // Simulate concurrent writes by writing different data sets
      val writer1Data = Seq((2, "Writer 1", "concurrent")).toDF("id", "content", "type")
      val writer2Data = Seq((3, "Writer 2", "concurrent")).toDF("id", "content", "type")

      // Both should succeed (append mode)
      writer1Data.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("append")
        .save(tempPath)

      writer2Data.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("append")
        .save(tempPath)

      // Verify final state
      val finalRead = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tempPath)

      val finalCount = finalRead.count()
      finalCount should be(3)

      // Verify all writes are present
      val types = finalRead.select("type").distinct().collect().map(_.getAs[String]("type"))
      types should contain("base")
      types should contain("concurrent")
    }
  }
}
