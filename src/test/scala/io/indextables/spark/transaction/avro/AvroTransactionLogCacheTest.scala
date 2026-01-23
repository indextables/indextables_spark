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

package io.indextables.spark.transaction.avro

import java.io.File

import io.indextables.spark.io.CloudStorageProvider
import io.indextables.spark.transaction.EnhancedTransactionLogCache
import io.indextables.spark.TestBase

/**
 * Tests for Avro state format transaction log caching behavior.
 *
 * These tests verify that:
 *   - Avro state directory lookups are cached correctly
 *   - Cache hits avoid repeated storage calls
 *   - Cache invalidation works for Avro format
 *   - Multiple queries reuse cached state
 */
class AvroTransactionLogCacheTest extends TestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Ensure Avro format is used (default)
    spark.conf.set("spark.indextables.state.format", "avro")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Clear caches before each test
    EnhancedTransactionLogCache.clearGlobalCaches()
    CloudStorageProvider.resetCounters()
  }

  test("Avro state: cache should be populated on first read") {
    withTempPath { tempPath =>
      // Write test data with Avro format
      val df = spark.range(100).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches and reset counters
      EnhancedTransactionLogCache.clearGlobalCaches()
      CloudStorageProvider.resetCounters()

      // First read - should populate cache
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.schema // Force schema resolution

      val afterFirstRead = CloudStorageProvider.getCountersSnapshot
      assert(afterFirstRead.exists > 0, "First read should make storage calls")

      // Second read - should hit cache
      val readDf2 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf2.schema

      val afterSecondRead = CloudStorageProvider.getCountersSnapshot
      // Second read should make minimal additional calls (cache hit)
      // Avro format may make slightly more calls due to state directory checks
      val additionalCalls = afterSecondRead.exists - afterFirstRead.exists
      assert(additionalCalls <= 4, s"Second read should make minimal additional calls, got $additionalCalls")
    }
  }

  test("Avro state: query execution should use cached state") {
    withTempPath { tempPath =>
      // Write test data
      val df = spark.range(100).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches and reset counters
      EnhancedTransactionLogCache.clearGlobalCaches()
      CloudStorageProvider.resetCounters()

      // Read and warm cache
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.schema
      val afterRead = CloudStorageProvider.getCountersSnapshot

      // Execute multiple queries
      val count1 = readDf.count()
      val afterCount1 = CloudStorageProvider.getCountersSnapshot

      val count2 = readDf.filter("id > 50").count()
      val afterCount2 = CloudStorageProvider.getCountersSnapshot

      val sum = readDf.agg(Map("id" -> "sum")).collect()
      val afterSum = CloudStorageProvider.getCountersSnapshot

      // Verify counts
      assert(count1 == 100)
      assert(count2 == 49)

      // Query execution should not cause significant additional exists() calls
      val queryExistsCalls = afterSum.exists - afterRead.exists
      assert(
        queryExistsCalls <= 3,
        s"Query execution should cause minimal exists() calls, got $queryExistsCalls"
      )
    }
  }

  test("Avro state: cache invalidation should force re-read") {
    withTempPath { tempPath =>
      // Write initial data
      val df = spark.range(50).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // First read to populate cache
      val readDf1 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      assert(readDf1.count() == 50)

      // Append more data
      val df2 = spark.range(50, 100).selectExpr("id", "concat('text_', id) as content")
      df2.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("append")
        .save(tempPath)

      // Clear caches to force re-read
      EnhancedTransactionLogCache.clearGlobalCaches()

      // Read again - should see new data
      val readDf2 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      assert(readDf2.count() == 100, "After cache invalidation, should see appended data")
    }
  }

  test("Avro state: cache invalidation should cause storage calls") {
    withTempPath { tempPath =>
      // Write test data
      val df = spark.range(100).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Read to populate cache
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.count()

      CloudStorageProvider.resetCounters()
      val beforeInvalidate = CloudStorageProvider.getCountersSnapshot

      // Clear global caches programmatically
      EnhancedTransactionLogCache.clearGlobalCaches()

      // Read again - should cause storage calls (cache miss)
      val readDf2 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf2.schema

      val afterRead = CloudStorageProvider.getCountersSnapshot
      val newCalls = afterRead.exists - beforeInvalidate.exists
      assert(newCalls > 0, s"After cache invalidation, read should cause storage calls, got $newCalls")
    }
  }

  test("Avro state: state directory should be detected correctly") {
    withTempPath { tempPath =>
      // Write test data with checkpoint to create Avro state
      val df = spark.range(100).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Verify Avro state directory was created
      val txLogPath = new File(tempPath, "_transaction_log")
      val stateDirectories = txLogPath.listFiles().filter(_.getName.startsWith("state-v"))

      assert(stateDirectories.nonEmpty, "Should have created Avro state directory")

      // Verify _last_checkpoint points to Avro state
      val lastCheckpointFile = new File(txLogPath, "_last_checkpoint")
      assert(lastCheckpointFile.exists(), "_last_checkpoint file should exist")

      val lastCheckpointContent = scala.io.Source.fromFile(lastCheckpointFile).mkString
      assert(
        lastCheckpointContent.contains("avro-state") || lastCheckpointContent.contains("stateDir"),
        s"_last_checkpoint should reference Avro state format"
      )
    }
  }

  test("Avro state: multiple tables should have independent caches") {
    withTempPath { tempPath1 =>
      withTempPath { tempPath2 =>
        // Write to first table
        val df1 = spark.range(50).selectExpr("id", "concat('table1_', id) as content")
        df1.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.checkpoint.enabled", "true")
          .option("spark.indextables.checkpoint.interval", "1")
          .mode("overwrite")
          .save(tempPath1)

        // Write to second table
        val df2 = spark.range(100).selectExpr("id", "concat('table2_', id) as content")
        df2.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.checkpoint.enabled", "true")
          .option("spark.indextables.checkpoint.interval", "1")
          .mode("overwrite")
          .save(tempPath2)

        // Read both tables
        val read1 = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(tempPath1)
        val read2 = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(tempPath2)

        // Verify counts are independent
        assert(read1.count() == 50)
        assert(read2.count() == 100)

        // Clear global caches to allow re-reading first table
        EnhancedTransactionLogCache.clearGlobalCaches()

        // Append to first table
        val df1Append = spark.range(50, 75).selectExpr("id", "concat('table1_', id) as content")
        df1Append.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("append")
          .save(tempPath1)

        // Re-read first table - should see new data
        val read1Again = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(tempPath1)
        assert(read1Again.count() == 75, "First table should reflect appended data")

        // Second table should still work
        val read2Again = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(tempPath2)
        assert(read2Again.count() == 100, "Second table count should be unchanged")
      }
    }
  }

  test("Avro state: collect operation should use cached state") {
    withTempPath { tempPath =>
      // Write test data
      val df = spark.range(100).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches
      EnhancedTransactionLogCache.clearGlobalCaches()
      CloudStorageProvider.resetCounters()

      // Read to warm cache
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.schema
      val afterRead = CloudStorageProvider.getCountersSnapshot

      // Collect all data
      val collected = readDf.collect()
      val afterCollect = CloudStorageProvider.getCountersSnapshot

      assert(collected.length == 100)

      // Collect should not cause significant additional exists() calls
      val collectExistsCalls = afterCollect.exists - afterRead.exists
      assert(
        collectExistsCalls <= 2,
        s"Collect should cause minimal additional exists() calls, got $collectExistsCalls"
      )
    }
  }
}
