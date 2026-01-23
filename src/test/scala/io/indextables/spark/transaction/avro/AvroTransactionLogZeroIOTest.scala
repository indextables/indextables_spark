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

import io.indextables.spark.io.CloudStorageProvider
import io.indextables.spark.transaction.{EnhancedTransactionLogCache, SchemaDeduplication}
import io.indextables.spark.transaction.avro.{AvroManifestReader, StateManifestIO}
import io.indextables.spark.TestBase

/**
 * Tests to verify that after initial read, subsequent queries cause ZERO cloud IO.
 *
 * These tests validate the caching fix for Avro transaction log performance:
 *   - StateManifest should be cached globally after first read
 *   - filterEmptyObjectMappings should be cached per schema hash
 *   - Partition-filtered queries should use cached StateManifest
 *   - Repeated queries should not trigger any cloud storage reads
 */
class AvroTransactionLogZeroIOTest extends TestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Ensure Avro format is used (default)
    spark.conf.set("spark.indextables.state.format", "avro")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Clear all caches and reset counters before each test
    EnhancedTransactionLogCache.clearGlobalCaches()
    CloudStorageProvider.resetCounters()
    StateManifestIO.resetReadCounter()
    SchemaDeduplication.resetParseCounter()
    AvroManifestReader.resetReadCounter()
  }

  test("query after read should cause zero StateManifest reads") {
    withTempPath { tempPath =>
      // Write test data with Avro format and checkpoint
      val df = spark.range(100).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches and reset counters
      EnhancedTransactionLogCache.clearGlobalCaches()
      StateManifestIO.resetReadCounter()

      // First read - populates caches
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.count()

      val readsAfterFirst = StateManifestIO.getReadCount()
      assert(readsAfterFirst >= 1, "First read should read StateManifest from storage")

      // Reset counter to track subsequent queries
      StateManifestIO.resetReadCounter()

      // Subsequent queries should NOT cause StateManifest reads (cache hits)
      readDf.count()
      readDf.filter("id > 50").count()
      readDf.agg(Map("id" -> "sum")).collect()

      val readsAfterSubsequent = StateManifestIO.getReadCount()
      assert(readsAfterSubsequent == 0,
        s"Subsequent queries should cause ZERO StateManifest reads, but got $readsAfterSubsequent")
    }
  }

  test("filterEmptyObjectMappings should be cached per schema hash") {
    // Create a test schema
    val schema = """[{"name":"field1","type":"text"},{"name":"field2","type":"i64"}]"""
    val hash = SchemaDeduplication.computeSchemaHash(schema)

    // Reset counter
    SchemaDeduplication.resetParseCounter()

    // First call - should parse JSON
    val parseCountBefore = SchemaDeduplication.getParseCallCount()
    val result1 = SchemaDeduplication.filterEmptyObjectMappingsCached(hash, schema)
    val parseCountAfterFirst = SchemaDeduplication.getParseCallCount()
    assert(parseCountAfterFirst == parseCountBefore + 1,
      s"First call should parse JSON (expected ${parseCountBefore + 1}, got $parseCountAfterFirst)")

    // Second call with same schema hash - should NOT parse (cache hit)
    val result2 = SchemaDeduplication.filterEmptyObjectMappingsCached(hash, schema)
    val parseCountAfterSecond = SchemaDeduplication.getParseCallCount()
    assert(parseCountAfterSecond == parseCountAfterFirst,
      s"Second call should NOT re-parse cached schema (expected $parseCountAfterFirst, got $parseCountAfterSecond)")

    // Results should be identical
    assert(result1 == result2, "Cached and non-cached results should match")
  }

  test("partition-filtered queries should use cached StateManifest") {
    withTempPath { tempPath =>
      // Write partitioned test data
      val df = spark.range(100)
        .selectExpr(
          "id",
          "concat('text_', id) as content",
          "CAST(id % 10 AS STRING) as partition_col"
        )
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .partitionBy("partition_col")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches and reset counter
      EnhancedTransactionLogCache.clearGlobalCaches()
      StateManifestIO.resetReadCounter()

      // First filtered query - should read StateManifest
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.filter("partition_col = '1'").count()

      val readsAfterFirst = StateManifestIO.getReadCount()
      assert(readsAfterFirst >= 1, "First filtered query should read StateManifest")

      // Reset counter
      StateManifestIO.resetReadCounter()

      // Second filtered query with DIFFERENT filter - should NOT re-read StateManifest
      readDf.filter("partition_col = '2'").count()
      val readsAfterSecond = StateManifestIO.getReadCount()
      assert(readsAfterSecond == 0,
        s"Second filtered query should use cached StateManifest, but got $readsAfterSecond reads")

      // Third query with unfiltered - should still use cached StateManifest
      readDf.count()
      val readsAfterThird = StateManifestIO.getReadCount()
      assert(readsAfterThird == 0,
        s"Unfiltered query should use cached StateManifest, but got $readsAfterThird reads")
    }
  }

  test("hotspot functions should not execute during cached query") {
    withTempPath { tempPath =>
      // Write test data with schema that has object fields
      val df = spark.range(100).selectExpr(
        "id",
        "concat('text_', id) as content",
        "struct(id as nested_id, concat('nested_', id) as nested_text) as nested_field"
      )
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches
      EnhancedTransactionLogCache.clearGlobalCaches()

      // Initial read - populates all caches
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.count()

      // Clear all hotspot counters
      StateManifestIO.resetReadCounter()
      SchemaDeduplication.resetParseCounter()

      // Cached query - should NOT execute any hotspot functions
      readDf.count()

      // Assert no hotspot execution
      assert(StateManifestIO.getReadCount() == 0,
        s"StateManifestIO.readStateManifest should not execute on cache hit, but got ${StateManifestIO.getReadCount()}")
      assert(SchemaDeduplication.getParseCallCount() == 0,
        s"filterEmptyObjectMappings should not execute on cache hit, but got ${SchemaDeduplication.getParseCallCount()}")
    }
  }

  test("cache hit rate should be 100% after initial read for repeated listFiles") {
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

      // Initial read to populate caches
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.count()

      // Get baseline StateManifest reads
      StateManifestIO.resetReadCounter()

      // Perform 10 repeated queries
      (1 to 10).foreach { _ =>
        readDf.count()
      }

      // Should be zero additional reads (all cache hits)
      val additionalReads = StateManifestIO.getReadCount()
      assert(additionalReads == 0,
        s"Expected 0 additional StateManifest reads after cache warmup, but got $additionalReads")
    }
  }

  test("new DataFrame instance should reuse global cache") {
    withTempPath { tempPath =>
      // Write test data
      val df = spark.range(100).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches and reset counter
      EnhancedTransactionLogCache.clearGlobalCaches()
      StateManifestIO.resetReadCounter()

      // First DataFrame instance - populates global cache
      val readDf1 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf1.count()

      val readsAfterFirst = StateManifestIO.getReadCount()
      assert(readsAfterFirst >= 1, "First read should populate cache")

      // Reset counter
      StateManifestIO.resetReadCounter()

      // NEW DataFrame instance - should reuse GLOBAL cache
      val readDf2 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf2.count()

      val readsAfterSecond = StateManifestIO.getReadCount()
      assert(readsAfterSecond == 0,
        s"New DataFrame instance should reuse global cache, but got $readsAfterSecond reads")
    }
  }

  test("filtered schema cache should prevent repeated JSON parsing") {
    withTempPath { tempPath =>
      // Write test data with multiple files to trigger schema processing
      spark.range(0, 50).selectExpr("id", "concat('text_', id) as content")
        .repartition(5)
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Append more files to create multiple splits with same schema
      spark.range(50, 100).selectExpr("id", "concat('text_', id) as content")
        .repartition(5)
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("append")
        .save(tempPath)

      // Clear caches
      EnhancedTransactionLogCache.clearGlobalCaches()
      SchemaDeduplication.resetParseCounter()

      // First read - will parse schemas but should cache them
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.count()

      val parsesAfterFirst = SchemaDeduplication.getParseCallCount()
      // Should parse only unique schemas (likely 1-2), not once per file
      assert(parsesAfterFirst <= 3,
        s"Should parse only unique schemas, not per file. Got $parsesAfterFirst parses")

      // Reset counter
      SchemaDeduplication.resetParseCounter()

      // Second read should NOT parse at all (all schemas cached)
      readDf.count()

      val parsesAfterSecond = SchemaDeduplication.getParseCallCount()
      assert(parsesAfterSecond == 0,
        s"Second read should not parse any schemas (all cached), but got $parsesAfterSecond")
    }
  }

  test("Avro manifest files should be cached across partition-filtered queries") {
    withTempPath { tempPath =>
      // Write partitioned test data to create multiple manifests
      val df = spark.range(100)
        .selectExpr(
          "id",
          "concat('text_', id) as content",
          "CAST(id % 5 AS STRING) as partition_col"
        )
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .partitionBy("partition_col")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches and reset counters
      EnhancedTransactionLogCache.clearGlobalCaches()
      AvroManifestReader.resetReadCounter()
      StateManifestIO.resetReadCounter()

      // First filtered query - should read Avro manifest files from storage
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.filter("partition_col = '1'").count()

      val avroReadsAfterFirst = AvroManifestReader.getReadCount()
      assert(avroReadsAfterFirst >= 1, s"First query should read Avro manifest files, got $avroReadsAfterFirst reads")

      // Reset counter
      AvroManifestReader.resetReadCounter()

      // Second filtered query with DIFFERENT filter - should use cached Avro manifest files
      readDf.filter("partition_col = '2'").count()
      val avroReadsAfterSecond = AvroManifestReader.getReadCount()
      assert(avroReadsAfterSecond == 0,
        s"Second filtered query should use cached Avro manifest files, but got $avroReadsAfterSecond reads")

      // Third query - unfiltered - should also use cached Avro manifests
      readDf.count()
      val avroReadsAfterThird = AvroManifestReader.getReadCount()
      assert(avroReadsAfterThird == 0,
        s"Unfiltered query should use cached Avro manifest files, but got $avroReadsAfterThird reads")
    }
  }

  test("repeated partition-filtered queries should cause zero cloud IO") {
    withTempPath { tempPath =>
      // Write partitioned test data
      val df = spark.range(100)
        .selectExpr(
          "id",
          "concat('text_', id) as content",
          "CAST(id % 3 AS STRING) as part"
        )
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .partitionBy("part")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches
      EnhancedTransactionLogCache.clearGlobalCaches()

      // First filtered query - populates all caches
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.filter("part = '0'").count()

      // Reset ALL counters
      StateManifestIO.resetReadCounter()
      AvroManifestReader.resetReadCounter()
      SchemaDeduplication.resetParseCounter()

      // 10 repeated partition-filtered queries - should ALL be cache hits
      (1 to 10).foreach { i =>
        val part = i % 3  // Cycle through partitions
        readDf.filter(s"part = '$part'").count()
      }

      // Assert ZERO cloud I/O for transaction log metadata
      assert(StateManifestIO.getReadCount() == 0,
        s"Repeated partition-filtered queries should NOT read StateManifest, but got ${StateManifestIO.getReadCount()}")
      assert(AvroManifestReader.getReadCount() == 0,
        s"Repeated partition-filtered queries should NOT read Avro manifest files, but got ${AvroManifestReader.getReadCount()}")
      assert(SchemaDeduplication.getParseCallCount() == 0,
        s"Repeated partition-filtered queries should NOT parse schemas, but got ${SchemaDeduplication.getParseCallCount()}")
    }
  }
}
