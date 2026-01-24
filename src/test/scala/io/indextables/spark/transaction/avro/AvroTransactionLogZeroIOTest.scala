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
    EnhancedTransactionLogCache.resetGlobalJsonParseCounter()
    CloudStorageProvider.resetCounters()
    StateManifestIO.resetReadCounter()
    StateManifestIO.resetParseCounter()
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

  test("REGRESSION: initial partition-filtered query should parse schema only once per unique schema") {
    // This test catches the bug where getActionsFromCheckpointWithFilters() was calling
    // restoreSchemas() AFTER readAvroStateCheckpoint() had already restored schemas,
    // causing double-filtering (and N schema parses instead of 1 for N files with 1 schema)
    withTempPath { tempPath =>
      // Write partitioned test data with 5 partitions, creating multiple splits
      val df = spark.range(500)
        .selectExpr(
          "id",
          "concat('text_', id) as content",
          "CAST(id % 5 AS STRING) as part"
        )
        .repartition(10)  // Create 10 splits to make N > 1
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .partitionBy("part")
        .mode("overwrite")
        .save(tempPath)

      // Clear ALL caches and counters
      EnhancedTransactionLogCache.clearGlobalCaches()
      SchemaDeduplication.resetParseCounter()

      // FIRST partition-filtered query - this goes through getActionsFromCheckpointWithFilters
      // Should parse schema only once (or a small number for unique schemas), NOT once per file
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      val count = readDf.filter("part = '0'").count()
      assert(count > 0, "Query should return some results")

      val parseCalls = SchemaDeduplication.getParseCallCount()
      // With 1 unique schema, should be EXACTLY 1 parse call:
      // - First file: cache miss → parse → cache
      // - Subsequent files: cache hit → no parse
      assert(parseCalls == 1,
        s"Initial partition-filtered query should parse schema exactly once per unique schema, " +
          s"but got $parseCalls parses. This indicates the bug where restoreSchemas() was called " +
          s"after Avro path already restored schemas, or caching is not working.")
    }
  }

  test("REGRESSION: initial unfiltered query should also parse schema only once per unique schema") {
    // Similar test for the listFilesOptimized() path
    withTempPath { tempPath =>
      // Write test data with multiple splits
      val df = spark.range(500)
        .selectExpr("id", "concat('text_', id) as content")
        .repartition(10)  // Create 10 splits
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear ALL caches and counters
      EnhancedTransactionLogCache.clearGlobalCaches()
      SchemaDeduplication.resetParseCounter()

      // First read - should parse schema only once
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      val count = readDf.count()
      assert(count == 500, s"Expected 500 rows, got $count")

      val parseCalls = SchemaDeduplication.getParseCallCount()
      // With 1 unique schema, should be EXACTLY 1 parse call (cached filtering)
      assert(parseCalls == 1,
        s"Initial unfiltered query should parse schema exactly once per unique schema, " +
          s"but got $parseCalls parses. This indicates filterEmptyObjectMappings is not being cached.")
    }
  }

  test("REGRESSION: 100 splits with 1 schema should parse schema exactly once") {
    // Stress test with 100 splits to catch any caching gaps
    withTempPath { tempPath =>
      // Write 100 splits with 1 unique schema
      val df = spark.range(10000)
        .selectExpr("id", "concat('text_', id) as content")
        .repartition(100)  // Create 100 splits
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear ALL caches and counters
      EnhancedTransactionLogCache.clearGlobalCaches()
      SchemaDeduplication.resetParseCounter()
      StateManifestIO.resetReadCounter()
      StateManifestIO.resetParseCounter()

      // First read - should parse schema exactly once
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      val count = readDf.count()
      assert(count == 10000, s"Expected 10000 rows, got $count")

      // Verify schema filtering happens exactly once per unique schema
      val schemaParseCalls = SchemaDeduplication.getParseCallCount()
      assert(schemaParseCalls == 1,
        s"100 splits with 1 unique schema should parse schema exactly once, " +
          s"but got $schemaParseCalls parses. This indicates a caching gap.")

      // Verify StateManifest is parsed exactly once (cached)
      val manifestParseCalls = StateManifestIO.getParseCount()
      assert(manifestParseCalls == 1,
        s"StateManifest should be parsed exactly once (cached), " +
          s"but got $manifestParseCalls parses.")
    }
  }

  test("GLOBAL JSON PARSE: initial read parses are O(1), subsequent queries minimal") {
    // This test validates the GLOBAL JSON parse counter across all components.
    // For Avro state format, we expect:
    // - Initial read: a small constant number of parses (NOT proportional to file count)
    //   These include: _last_checkpoint, StateManifest metadata, metadata action,
    //   schema filtering, and DocMappingMetadata extraction
    // - Subsequent queries: minimal parses (ideally 0, but some metadata access may occur)
    withTempPath { tempPath =>
      // Write test data with 10 splits and 1 unique schema
      val df = spark.range(1000)
        .selectExpr("id", "concat('text_', id) as content")
        .repartition(10)  // Create 10 splits
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear ALL caches and reset GLOBAL counter
      EnhancedTransactionLogCache.clearGlobalCaches()
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()

      // Initial read - should have a small constant number of parses (NOT 10)
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      val count = readDf.count()
      assert(count == 1000, s"Expected 1000 rows, got $count")

      val globalParsesAfterRead = EnhancedTransactionLogCache.getGlobalJsonParseCount()
      // The parse count should be O(1) - a small constant, not proportional to file count
      // Expected parses: _last_checkpoint (1), StateManifest metadata (1), metadata action (1),
      // schema filtering (1 per unique schema), DocMappingMetadata (1 per unique schema)
      assert(globalParsesAfterRead <= 10,
        s"Initial read should trigger a small constant number of JSON parses (O(1)), " +
          s"but got $globalParsesAfterRead. This may indicate per-file parsing.")

      // Reset counters for subsequent query tracking
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()
      SchemaDeduplication.resetParseCounter()

      // Subsequent queries on the SAME DataFrame should trigger ZERO JSON parses.
      // All metadata, protocol, version actions, and schemas are cached globally.
      readDf.count()
      val parsesAfterCount = EnhancedTransactionLogCache.getGlobalJsonParseCount()

      readDf.filter("id > 500").count()
      val parsesAfterFilter = EnhancedTransactionLogCache.getGlobalJsonParseCount()

      readDf.agg(Map("id" -> "sum")).collect()
      val parsesAfterAgg = EnhancedTransactionLogCache.getGlobalJsonParseCount()

      // Debug: identify which query causes parsing
      if (parsesAfterCount > 0) println(s"DEBUG: count() caused $parsesAfterCount parses")
      if (parsesAfterFilter > parsesAfterCount) println(s"DEBUG: filter() caused ${parsesAfterFilter - parsesAfterCount} parses")
      if (parsesAfterAgg > parsesAfterFilter) println(s"DEBUG: agg() caused ${parsesAfterAgg - parsesAfterFilter} parses")

      val globalParsesAfterQueries = parsesAfterAgg
      val schemaParsesAfterQueries = SchemaDeduplication.getParseCallCount()

      // STRICT: Subsequent queries must trigger ZERO JSON parses
      assert(globalParsesAfterQueries == 0,
        s"Subsequent queries on same DataFrame should trigger ZERO JSON parses, " +
          s"but got $globalParsesAfterQueries. This indicates a cache bypass.")

      // Schema parsing must also be 0
      assert(schemaParsesAfterQueries == 0,
        s"Schema filtering should be fully cached - got $schemaParsesAfterQueries parses")
    }
  }

  test("GLOBAL JSON PARSE: parse count is constant regardless of file count") {
    // This test verifies that JSON parse count is O(1) not O(n) with respect to file count
    withTempPath { tempPath =>
      // Write 100 splits with 1 unique schema
      val df = spark.range(10000)
        .selectExpr("id", "concat('text_', id) as content")
        .repartition(100)  // Create 100 splits
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches and reset counter
      EnhancedTransactionLogCache.clearGlobalCaches()
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()

      // Read 100-split table
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.count()

      val globalParsesFor100Splits = EnhancedTransactionLogCache.getGlobalJsonParseCount()
      // With 100 splits but 1 unique schema, parse count should still be small constant
      // If it were O(n), we'd see ~100 parses
      assert(globalParsesFor100Splits <= 10,
        s"100 splits with 1 schema should trigger <=10 JSON parses, but got $globalParsesFor100Splits. " +
          s"This indicates per-file parsing instead of per-schema parsing.")
    }
  }

  test("GLOBAL JSON PARSE: new DataFrame on same table uses cached schemas") {
    // Validates that global caches work across DataFrame instances
    // The key validation is that SCHEMA parsing (the expensive O(n) operation) is cached.
    withTempPath { tempPath =>
      // Write test data
      val df = spark.range(500)
        .selectExpr("id", "concat('text_', id) as content")
        .repartition(5)
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches and reset counters
      EnhancedTransactionLogCache.clearGlobalCaches()
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()
      SchemaDeduplication.resetParseCounter()

      // First DataFrame - populates caches
      val readDf1 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf1.count()

      val parsesAfterFirst = EnhancedTransactionLogCache.getGlobalJsonParseCount()
      val schemaParsesAfterFirst = SchemaDeduplication.getParseCallCount()
      // First read requires parsing metadata structures - should be small constant
      assert(parsesAfterFirst <= 10,
        s"First read should parse a small constant number of JSON structures, got $parsesAfterFirst")
      assert(schemaParsesAfterFirst == 1,
        s"First read should parse schema exactly once, got $schemaParsesAfterFirst")

      // Reset counters
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()
      SchemaDeduplication.resetParseCounter()

      // NEW DataFrame instance on same table - should use global caches for EVERYTHING
      // Metadata, protocol, version actions, and schemas are all cached globally
      val readDf2 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf2.count()

      val parsesAfterSecond = EnhancedTransactionLogCache.getGlobalJsonParseCount()
      val schemaParsesAfterSecond = SchemaDeduplication.getParseCallCount()

      // STRICT: New DataFrame on same table should trigger ZERO JSON parses
      // All metadata, protocol, version actions, and schemas are cached globally
      assert(parsesAfterSecond == 0,
        s"New DataFrame on same table should trigger ZERO JSON parses (all cached globally), " +
          s"but got $parsesAfterSecond. This indicates a cache bypass.")

      // Schema parsing must also be 0
      assert(schemaParsesAfterSecond == 0,
        s"New DataFrame should NOT re-parse schemas (cached), but got $schemaParsesAfterSecond schema parses")
    }
  }

  // ============================================================================
  // DEBUG TEST: Capture stack traces to identify where JSON parsing happens
  // ============================================================================

  test("DEBUG: capture stack traces for JSON parsing on subsequent queries") {
    withTempPath { tempPath =>
      // Write test data with multiple splits (matches failing test setup)
      val df = spark.range(1000)
        .selectExpr("id", "concat('text_', id) as content")
        .repartition(10)  // Create 10 splits
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches
      EnhancedTransactionLogCache.clearGlobalCaches()
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()
      EnhancedTransactionLogCache.disableThrowOnJsonParse()  // Ensure disabled for initial read

      // Initial read - populate caches
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.count()

      println(s"=== Initial read completed, global JSON parses: ${EnhancedTransactionLogCache.getGlobalJsonParseCount()} ===")

      // Test 1: count() on SAME DataFrame
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()
      EnhancedTransactionLogCache.enableThrowOnJsonParse()

      try {
        println("=== Test 1: count() on SAME DataFrame ===")
        readDf.count()
        println("=== count() completed without JSON parsing (GOOD!) ===")
      } catch {
        case e: Exception if findDebugException(e) =>
          println("=== JSON PARSING DETECTED ON count() ===")
          e.printStackTrace()
          fail(s"Unexpected JSON parsing on count(). See stack trace above.")
      } finally {
        EnhancedTransactionLogCache.disableThrowOnJsonParse()
      }

      // Test 2: filter() on SAME DataFrame - THIS IS THE ONE THAT FAILS
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()

      // First, run WITHOUT throw-on-parse to see if any parsing happens
      readDf.filter("id > 500").count()
      val filterParseCount = EnhancedTransactionLogCache.getGlobalJsonParseCount()
      println(s"=== Test 2: filter() on SAME DataFrame - parse count: $filterParseCount ===")

      if (filterParseCount > 0) {
        // Re-run WITH throw-on-parse to capture stack trace
        println("=== Re-running filter() WITH throw-on-parse to capture stack trace ===")
        EnhancedTransactionLogCache.resetGlobalJsonParseCounter()
        EnhancedTransactionLogCache.enableThrowOnJsonParse()
        try {
          readDf.filter("id > 500").count()
          println("=== filter() re-run completed without throwing (exception was swallowed somewhere) ===")
          println("=== Checking if parse counter increased... ===")
          val rerunParseCount = EnhancedTransactionLogCache.getGlobalJsonParseCount()
          println(s"=== Re-run parse count: $rerunParseCount ===")
        } catch {
          case e: Exception =>
            if (findDebugException(e)) {
              println("=== JSON PARSING DETECTED ON filter() ===")
              println("Stack trace shows where caching is being bypassed:")
              e.printStackTrace()
            } else {
              println(s"=== Non-debug exception: ${e.getClass.getName}: ${e.getMessage} ===")
              e.printStackTrace()
            }
        } finally {
          EnhancedTransactionLogCache.disableThrowOnJsonParse()
        }
        // Don't fail - we want to capture the info
        println(s"=== filter() caused $filterParseCount parse(s) - this needs investigation ===")
      } else {
        println("=== filter() completed without JSON parsing (GOOD!) ===")
      }

      // Test 3: NEW DataFrame on same table
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()
      EnhancedTransactionLogCache.enableThrowOnJsonParse()

      try {
        println("=== Test 3: NEW DataFrame on same table ===")
        val readDf2 = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(tempPath)
        readDf2.count()
        println("=== NEW DataFrame query completed without JSON parsing (GOOD!) ===")
      } catch {
        case e: Exception if findDebugException(e) =>
          println("=== JSON PARSING DETECTED ON NEW DATAFRAME ===")
          e.printStackTrace()
          fail(s"Unexpected JSON parsing on new DataFrame. See stack trace above.")
      } finally {
        EnhancedTransactionLogCache.disableThrowOnJsonParse()
      }
    }
  }

  /** Helper to check if exception chain contains DEBUG message */
  private def findDebugException(e: Throwable): Boolean = {
    var cause: Throwable = e
    while (cause != null) {
      if (cause.getMessage != null && cause.getMessage.contains("DEBUG: JSON parse detected")) {
        return true
      }
      cause = cause.getCause
    }
    false
  }
}
