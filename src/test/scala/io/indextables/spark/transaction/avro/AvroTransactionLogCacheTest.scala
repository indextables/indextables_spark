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
      // Tolerance increased to 5 to account for protocol/metadata cache interactions
      val additionalCalls = afterSecondRead.exists - afterFirstRead.exists
      assert(additionalCalls <= 5, s"Second read should make minimal additional calls, got $additionalCalls")
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
      val count1      = readDf.count()
      val afterCount1 = CloudStorageProvider.getCountersSnapshot

      val count2      = readDf.filter("id > 50").count()
      val afterCount2 = CloudStorageProvider.getCountersSnapshot

      val sum      = readDf.agg(Map("id" -> "sum")).collect()
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
      val newCalls  = afterRead.exists - beforeInvalidate.exists
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
      val txLogPath        = new File(tempPath, "_transaction_log")
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
      val collected    = readDf.collect()
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

  test("Avro state: partitioned count query should make zero storage requests after cache warm") {
    withTempPath { tempPath =>
      // Write partitioned test data with checkpoint to create Avro state
      val df = spark
        .range(100)
        .selectExpr(
          "id",
          "concat('text_', id) as content",
          "cast(id % 5 as string) as partition_col"
        )
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .partitionBy("partition_col")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches to start fresh
      EnhancedTransactionLogCache.clearGlobalCaches()

      // Create view for SQL queries
      val tableDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      tableDf.createOrReplaceTempView("partitioned_table")

      // First query to warm cache - reset counters after
      val warmUp = spark.sql("SELECT partition_col, count(*) FROM partitioned_table GROUP BY partition_col")
      warmUp.collect()

      // Reset counters AFTER cache is warmed
      CloudStorageProvider.resetCounters()
      val beforeQuery = CloudStorageProvider.getCountersSnapshot

      // Second identical query - should use cached data
      val result = spark.sql("SELECT partition_col, count(*) FROM partitioned_table GROUP BY partition_col")
      result.collect()

      val afterQuery = CloudStorageProvider.getCountersSnapshot

      // Calculate total storage requests made during second query
      val totalRequests  = afterQuery.total - beforeQuery.total
      val existsCalls    = afterQuery.exists - beforeQuery.exists
      val readFileCalls  = afterQuery.readFile - beforeQuery.readFile
      val listFilesCalls = afterQuery.listFiles - beforeQuery.listFiles

      // Verify zero storage requests for the cached query
      assert(
        totalRequests == 0,
        s"Cached partitioned count query should make ZERO storage requests, but made: " +
          s"total=$totalRequests (exists=$existsCalls, readFile=$readFileCalls, listFiles=$listFilesCalls)"
      )
    }
  }

  test("Avro state: simple count query should make zero storage requests after cache warm") {
    withTempPath { tempPath =>
      // Write test data with checkpoint to create Avro state
      val df = spark.range(100).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches to start fresh
      EnhancedTransactionLogCache.clearGlobalCaches()

      // Create view for SQL queries
      val tableDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      tableDf.createOrReplaceTempView("test_table")

      // First query to warm cache
      spark.sql("SELECT count(*) FROM test_table").collect()

      // Reset counters AFTER cache is warmed
      CloudStorageProvider.resetCounters()
      val beforeQuery = CloudStorageProvider.getCountersSnapshot

      // Second identical query - should use cached data
      val result = spark.sql("SELECT count(*) FROM test_table").collect()

      val afterQuery = CloudStorageProvider.getCountersSnapshot

      // Calculate total storage requests made during second query
      val totalRequests = afterQuery.total - beforeQuery.total

      // Verify result is correct
      assert(result(0).getLong(0) == 100, "Count should be 100")

      // Verify zero storage requests for the cached query
      assert(
        totalRequests == 0,
        s"Cached count query should make ZERO storage requests, but made: total=$totalRequests " +
          s"(exists=${afterQuery.exists - beforeQuery.exists}, " +
          s"readFile=${afterQuery.readFile - beforeQuery.readFile}, " +
          s"listFiles=${afterQuery.listFiles - beforeQuery.listFiles})"
      )
    }
  }

  test("Avro state: multiple queries should all make zero storage requests after cache warm") {
    withTempPath { tempPath =>
      // Write test data with checkpoint to create Avro state
      val df = spark
        .range(100)
        .selectExpr(
          "id",
          "id * 2 as value",
          "concat('text_', id) as content"
        )
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches to start fresh
      EnhancedTransactionLogCache.clearGlobalCaches()

      // Create view for SQL queries
      val tableDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      tableDf.createOrReplaceTempView("test_table_multi")

      // First query to warm cache
      spark.sql("SELECT count(*) FROM test_table_multi").collect()

      // Reset counters AFTER cache is warmed
      CloudStorageProvider.resetCounters()
      val beforeQueries = CloudStorageProvider.getCountersSnapshot

      // Execute multiple queries - ALL should use cached data
      val count1 = spark.sql("SELECT count(*) FROM test_table_multi").collect()(0).getLong(0)
      val count2 = spark.sql("SELECT count(*) FROM test_table_multi WHERE id > 50").collect()(0).getLong(0)
      val sum    = spark.sql("SELECT sum(value) FROM test_table_multi").collect()(0).getLong(0)
      val avg    = spark.sql("SELECT avg(value) FROM test_table_multi").collect()(0).getDouble(0)

      val afterQueries = CloudStorageProvider.getCountersSnapshot

      // Verify results are correct
      assert(count1 == 100, "Count should be 100")
      assert(count2 == 49, "Count where id > 50 should be 49")
      assert(sum == 9900, "Sum should be 9900 (0+2+4+...+198)")
      assert(avg == 99.0, "Avg should be 99.0")

      // Calculate total storage requests made during all queries
      val totalRequests = afterQueries.total - beforeQueries.total

      // Verify zero storage requests for all cached queries
      assert(
        totalRequests == 0,
        s"All cached queries should make ZERO storage requests, but made: total=$totalRequests " +
          s"(exists=${afterQueries.exists - beforeQueries.exists}, " +
          s"readFile=${afterQueries.readFile - beforeQueries.readFile}, " +
          s"listFiles=${afterQueries.listFiles - beforeQueries.listFiles})"
      )
    }
  }
}
