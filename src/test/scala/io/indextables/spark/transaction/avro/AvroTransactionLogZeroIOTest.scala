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

  // ============================================================================
  // PRODUCTION REGRESSION TESTS: Reproduce O(n) parsing issue
  // ============================================================================

  /**
   * PRODUCTION BUG REPRODUCTION: inferSchema causes O(n) DocMappingMetadata parses
   *
   * In production with 10k splits, this takes 20+ seconds because DocMappingMetadata.parse
   * is called once per AddAction instead of once per unique schema.
   *
   * Stack trace 1 from production:
   *   inferSchema -> getSchema -> getMetadata -> getCheckpointActionsCached
   *   -> getActionsFromCheckpoint -> readAvroStateCheckpoint
   *   -> StateManifestIO.readStateManifest -> parseStateManifest
   *
   * Stack trace 2 from production:
   *   prewarmCache -> listFilesOptimized -> getOrComputeAvroFileList
   *   -> (foreach AddAction) -> getDocMappingMetadata -> DocMappingMetadata.parse
   */
  test("PRODUCTION BUG: inferSchema with 100 splits should parse schema O(1) times, not O(n)") {
    withTempPath { tempPath =>
      // Clear all caches before the test
      EnhancedTransactionLogCache.clearGlobalCaches()
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()
      StateManifestIO.resetParseCounter()

      // Write 100 splits with the same schema
      val numSplits = 100
      val df = spark.range(10000)
        .selectExpr("id", "concat('text_', id) as content")
        .repartition(numSplits) // Create 100 partitions -> 100 splits
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // CRITICAL: Clear all caches to simulate fresh JVM / cold start
      EnhancedTransactionLogCache.clearGlobalCaches()
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()
      StateManifestIO.resetParseCounter()

      println(s"=== PRODUCTION BUG REPRO: $numSplits splits, 1 unique schema ===")
      println(s"=== Before read: JSON parses=${EnhancedTransactionLogCache.getGlobalJsonParseCount()}, StateManifest parses=${StateManifestIO.getParseCount()} ===")

      // This is what happens in production: inferSchema is called when table is accessed
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      // Force schema inference (happens automatically, but be explicit)
      val schema = readDf.schema
      println(s"=== After inferSchema: JSON parses=${EnhancedTransactionLogCache.getGlobalJsonParseCount()}, StateManifest parses=${StateManifestIO.getParseCount()} ===")

      val parsesAfterInferSchema = EnhancedTransactionLogCache.getGlobalJsonParseCount()

      // Execute a query to trigger listFilesOptimized/prewarmCache
      val count = readDf.count()
      println(s"=== After count(): JSON parses=${EnhancedTransactionLogCache.getGlobalJsonParseCount()}, StateManifest parses=${StateManifestIO.getParseCount()} ===")

      val totalParses = EnhancedTransactionLogCache.getGlobalJsonParseCount()

      // ASSERTION: Parse count should be O(1), not O(n)
      // With proper caching:
      //   - StateManifest parse: 1 (per state directory)
      //   - DocMappingMetadata parse: 1 (per unique schema)
      //   - Other metadata parses: small constant number
      // Total should be < 20 for a table with 100 splits and 1 unique schema
      val maxExpectedParses = 20 // generous upper bound for O(1) behavior

      println(s"=== RESULT: Total JSON parses = $totalParses (expected < $maxExpectedParses for O(1) behavior) ===")

      assert(
        totalParses < maxExpectedParses,
        s"PRODUCTION BUG DETECTED: JSON parse count is O(n) instead of O(1)! " +
          s"Got $totalParses parses for $numSplits splits. " +
          s"Expected < $maxExpectedParses parses. " +
          s"This causes 20+ second delays in production with 10k files."
      )
    }
  }

  test("PRODUCTION BUG: prewarmCache with 100 splits should NOT parse DocMappingMetadata per file") {
    withTempPath { tempPath =>
      // Clear all caches before the test
      EnhancedTransactionLogCache.clearGlobalCaches()
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()

      // Write 100 splits with the same schema
      val numSplits = 100
      val df = spark.range(10000)
        .selectExpr("id", "concat('text_', id) as content")
        .repartition(numSplits)
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // CRITICAL: Clear all caches to simulate fresh JVM / cold start
      EnhancedTransactionLogCache.clearGlobalCaches()
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()

      println(s"=== PREWARM BUG REPRO: $numSplits splits, 1 unique schema ===")

      // First, do the initial read which populates caches
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.count() // Force evaluation

      val parsesAfterFirstRead = EnhancedTransactionLogCache.getGlobalJsonParseCount()
      println(s"=== After first read: JSON parses = $parsesAfterFirstRead ===")

      // Now clear ONLY the file list cache to simulate what happens on subsequent queries
      // (StateManifest and DocMappingMetadata should remain cached)
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()

      // Second read - should use cached schemas
      val readDf2 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf2.count()

      val parsesAfterSecondRead = EnhancedTransactionLogCache.getGlobalJsonParseCount()
      println(s"=== After second read (should use cached schemas): JSON parses = $parsesAfterSecondRead ===")

      // Second read should trigger ZERO parses since DocMappingMetadata is cached per schema hash
      assert(
        parsesAfterSecondRead == 0,
        s"CACHE BUG: Second read should use cached DocMappingMetadata! " +
          s"Got $parsesAfterSecondRead parses, expected 0. " +
          s"DocMappingMetadata is being re-parsed instead of using cache."
      )
    }
  }

  test("PRODUCTION BUG: DocMappingMetadata cache key should use docMappingRef not full JSON") {
    withTempPath { tempPath =>
      // Clear all caches
      EnhancedTransactionLogCache.clearGlobalCaches()
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()

      // Write a table with multiple splits but same schema
      val numSplits = 50
      val df = spark.range(5000)
        .selectExpr("id", "concat('text_', id) as content")
        .repartition(numSplits)
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches
      EnhancedTransactionLogCache.clearGlobalCaches()
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()

      // Read and force file list computation
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      // Get the file list to see how many unique schemas there are
      readDf.count()

      val totalParses = EnhancedTransactionLogCache.getGlobalJsonParseCount()
      println(s"=== $numSplits splits with 1 unique schema: $totalParses JSON parses ===")

      // If caching by docMappingRef works, we should have at most a small constant number of parses
      // (1 for DocMappingMetadata + 1 for StateManifest + a few for metadata/protocol)
      // NOT 50 parses (one per split)
      assert(
        totalParses < numSplits / 2,
        s"DocMappingMetadata is likely using full JSON as cache key instead of docMappingRef! " +
          s"Got $totalParses parses for $numSplits splits with 1 unique schema. " +
          s"Expected far fewer if caching by schema hash."
      )
    }
  }

  test("UNIT TEST: computeSchemaHash should normalize nested field_mappings arrays") {
    // Test that field_mappings arrays are sorted by "name" consistently
    val schema1 = """[{"name":"struct_0","type":"object","field_mappings":[{"name":"field_a","type":"text"},{"name":"field_c","type":"bool"},{"name":"field_b","type":"u64"}]}]"""
    val schema2 = """[{"name":"struct_0","type":"object","field_mappings":[{"name":"field_a","type":"text"},{"name":"field_b","type":"u64"},{"name":"field_c","type":"bool"}]}]"""

    // These two schemas are semantically identical (just different field ordering in field_mappings)
    // After normalization, they should produce the same hash
    val hash1 = SchemaDeduplication.computeSchemaHash(schema1)
    val hash2 = SchemaDeduplication.computeSchemaHash(schema2)

    println(s"=== NORMALIZATION TEST ===")
    println(s"Schema 1 (field_c before field_b): $schema1")
    println(s"Hash 1: $hash1")
    println(s"Schema 2 (field_b before field_c): $schema2")
    println(s"Hash 2: $hash2")

    assert(hash1 == hash2,
      s"computeSchemaHash should produce the same hash for semantically identical schemas! " +
        s"Hash1=$hash1, Hash2=$hash2. " +
        s"This indicates nested field_mappings arrays are not being sorted correctly.")
  }

  test("PRODUCTION BUG: 100 columns with struct/array should produce consistent hash across splits") {
    // This test reproduces the production issue where tables with many columns
    // have different docMappingRef hashes per split due to field ordering differences
    // in the JSON returned from tantivy4java
    //
    // CRITICAL: Include struct and array types as these have nested field_mappings
    // that can come back from tantivy4java in different orders
    withTempPath { tempPath =>
      // Clear all caches before the test
      EnhancedTransactionLogCache.clearGlobalCaches()
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()
      StateManifestIO.resetParseCounter()
      SchemaDeduplication.resetParseCounter()

      // Create a DataFrame with:
      // - 90 simple string columns
      // - 5 struct columns (each with 3 nested fields)
      // - 5 array columns
      val numSimpleColumns = 90
      val numStructColumns = 5
      val numArrayColumns = 5
      val numSplits = 10

      val simpleColumnExprs = (0 until numSimpleColumns).map(i => s"CAST(id + $i AS STRING) as col_$i")
      val structColumnExprs = (0 until numStructColumns).map(i =>
        s"named_struct('field_a', CAST(id AS STRING), 'field_b', id * $i, 'field_c', id > 50) as struct_$i"
      )
      val arrayColumnExprs = (0 until numArrayColumns).map(i =>
        s"array(CAST(id AS STRING), CAST(id + $i AS STRING), CAST(id * 2 AS STRING)) as array_$i"
      )

      val allColumnExprs = simpleColumnExprs ++ structColumnExprs ++ arrayColumnExprs
      val df = spark.range(1000)
        .selectExpr(allColumnExprs: _*)
        .repartition(numSplits)

      val totalColumns = numSimpleColumns + numStructColumns + numArrayColumns
      println(s"=== PRODUCTION BUG REPRO: $totalColumns columns ($numSimpleColumns simple, $numStructColumns struct, $numArrayColumns array), $numSplits splits ===")
      println(s"=== Schema has ${df.schema.fields.length} fields ===")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // CRITICAL: Clear all caches to simulate fresh JVM / cold start
      EnhancedTransactionLogCache.clearGlobalCaches()
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()
      StateManifestIO.resetParseCounter()
      SchemaDeduplication.resetParseCounter()

      println(s"=== Before read: JSON parses=${EnhancedTransactionLogCache.getGlobalJsonParseCount()}, " +
        s"StateManifest parses=${StateManifestIO.getParseCount()}, " +
        s"SchemaDedup parses=${SchemaDeduplication.getParseCallCount()} ===")

      // Read and trigger table initialization
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      // Force evaluation
      val count = readDf.count()
      println(s"=== Row count: $count ===")

      val jsonParses = EnhancedTransactionLogCache.getGlobalJsonParseCount()
      val manifestParses = StateManifestIO.getParseCount()
      val schemaDedupParses = SchemaDeduplication.getParseCallCount()

      println(s"=== After read: JSON parses=$jsonParses, " +
        s"StateManifest parses=$manifestParses, " +
        s"SchemaDedup parses=$schemaDedupParses ===")

      // Check how many unique docMappingRefs exist in the file list
      val txLog = io.indextables.spark.transaction.TransactionLogFactory.create(
        new org.apache.hadoop.fs.Path(tempPath),
        spark,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
      )
      val files = txLog.listFiles()
      val uniqueRefs = files.flatMap(_.docMappingRef).distinct
      val uniqueJsons = files.flatMap(_.docMappingJson).distinct

      println(s"=== File analysis: ${files.size} files, ${uniqueRefs.size} unique docMappingRefs, ${uniqueJsons.size} unique docMappingJsons ===")

      // If normalization is working, all files should share the same docMappingRef
      // If it's broken, each file might have a different hash
      if (uniqueRefs.size > 1) {
        println(s"=== WARNING: Found ${uniqueRefs.size} unique docMappingRefs - schema normalization may be broken! ===")
        uniqueRefs.take(3).foreach(ref => println(s"   ref: $ref"))

        // Print the struct/array portions of the JSON for debugging
        println(s"=== Comparing struct/array fields from first 2 files: ===")
        uniqueJsons.take(2).zipWithIndex.foreach { case (json, idx) =>
          // Find the struct fields (object types with field_mappings)
          val structPattern = """"name":"struct_\d+"""".r
          val structMatches = structPattern.findAllIn(json).toList
          println(s"--- File $idx has ${structMatches.size} struct fields ---")

          // Extract a struct field definition for comparison
          val structStartIdx = json.indexOf(""""name":"struct_0""")
          if (structStartIdx >= 0) {
            val structSnippet = json.substring(structStartIdx, Math.min(structStartIdx + 800, json.length))
            println(s"--- File $idx struct_0 snippet: ---")
            println(structSnippet)
          }
        }

        // Compare raw JSON lengths
        println(s"=== JSON lengths: ===")
        uniqueJsons.take(3).zipWithIndex.foreach { case (json, idx) =>
          println(s"   File $idx: ${json.length} chars")
        }

        // Now test: if we re-hash the unique JSONs, do they produce the same hash?
        println(s"=== Re-hashing unique JSONs to test normalization: ===")
        val recomputedHashes = uniqueJsons.map(json => SchemaDeduplication.computeSchemaHash(json))
        val uniqueRecomputedHashes = recomputedHashes.distinct
        println(s"   ${uniqueJsons.size} unique JSONs -> ${uniqueRecomputedHashes.size} unique hashes after re-normalization")
        if (uniqueRecomputedHashes.size == 1) {
          println(s"   NORMALIZATION WORKS: All JSONs produce same hash ${uniqueRecomputedHashes.head}")
          println(s"   BUG IS IN REGISTRY: docMappingRef is not being reused during write!")
        } else {
          println(s"   NORMALIZATION FAILS: Different JSONs produce different hashes")
          uniqueRecomputedHashes.take(3).foreach(h => println(s"      hash: $h"))
        }
      }

      // ASSERTION: With 1 unique schema (100 columns), we should have exactly 1 unique docMappingRef
      assert(
        uniqueRefs.size <= 1,
        s"PRODUCTION BUG DETECTED: Schema normalization is broken! " +
          s"Found ${uniqueRefs.size} unique docMappingRefs for $numSplits splits with identical schema. " +
          s"Expected 1 unique ref. This causes O(n) parsing instead of O(1)."
      )

      // ASSERTION: Parse count should be O(1), not O(n)
      val maxExpectedParses = 20
      assert(
        jsonParses < maxExpectedParses,
        s"PRODUCTION BUG DETECTED: JSON parse count is O(n) instead of O(1)! " +
          s"Got $jsonParses JSON parses for $numSplits splits with $totalColumns columns. " +
          s"Expected < $maxExpectedParses parses."
      )
    }
  }

  test("PRODUCTION BUG: createOrReplaceTempView with 100 splits should parse schema O(1) times") {
    // This test reproduces the exact production scenario:
    // spark.read.format(...).load(...).createOrReplaceTempView("view")
    withTempPath { tempPath =>
      // Clear all caches before the test
      EnhancedTransactionLogCache.clearGlobalCaches()
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()
      StateManifestIO.resetParseCounter()
      SchemaDeduplication.resetParseCounter()

      // Write 100 splits with the same schema
      val numSplits = 100
      val df = spark.range(10000)
        .selectExpr("id", "concat('text_', id) as content")
        .repartition(numSplits)
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // CRITICAL: Clear all caches to simulate fresh JVM / cold start
      EnhancedTransactionLogCache.clearGlobalCaches()
      EnhancedTransactionLogCache.resetGlobalJsonParseCounter()
      StateManifestIO.resetParseCounter()
      SchemaDeduplication.resetParseCounter()

      println(s"=== PRODUCTION BUG REPRO: createOrReplaceTempView with $numSplits splits ===")
      println(s"=== Before view creation: JSON parses=${EnhancedTransactionLogCache.getGlobalJsonParseCount()}, " +
        s"StateManifest parses=${StateManifestIO.getParseCount()}, " +
        s"SchemaDedup parses=${SchemaDeduplication.getParseCallCount()} ===")

      // This is the EXACT production path that's slow:
      // spark.read.format(...).load(...).createOrReplaceTempView("view")
      spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
        .createOrReplaceTempView("testView")

      val jsonParses = EnhancedTransactionLogCache.getGlobalJsonParseCount()
      val manifestParses = StateManifestIO.getParseCount()
      val schemaDedupParses = SchemaDeduplication.getParseCallCount()

      println(s"=== After view creation: JSON parses=$jsonParses, " +
        s"StateManifest parses=$manifestParses, " +
        s"SchemaDedup parses=$schemaDedupParses ===")

      // ASSERTION: Parse count should be O(1), not O(n)
      // With 100 splits and 1 unique schema:
      //   - StateManifest parse: 1
      //   - SchemaDedup (filterEmptyObjectMappings): 1 per unique schema = 1
      //   - Other metadata parses: small constant
      val maxExpectedParses = 20 // generous upper bound for O(1) behavior

      assert(
        jsonParses < maxExpectedParses,
        s"PRODUCTION BUG DETECTED in createOrReplaceTempView: JSON parse count is O(n) instead of O(1)! " +
          s"Got $jsonParses JSON parses for $numSplits splits with 1 unique schema. " +
          s"Expected < $maxExpectedParses parses for O(1) behavior."
      )

      // Also verify SchemaDeduplication parses - should be exactly 1 for 1 unique schema
      assert(
        schemaDedupParses <= 2,
        s"PRODUCTION BUG: filterEmptyObjectMappings called $schemaDedupParses times for 1 unique schema. " +
          s"Expected <= 2 (1 for initial + possibly 1 for cache miss)."
      )

      // Clean up
      spark.catalog.dropTempView("testView")
    }
  }

  /**
   * REGRESSION FIX VALIDATION: Re-checkpointing old tables consolidates redundant mappings
   *
   * Before the fix in writeIncrementalAvroState, each executor could produce different hashes
   * for the same schema because field ordering could vary from tantivy4java. This resulted in
   * N unique schema entries in the registry for N splits, all pointing to semantically identical schemas.
   *
   * This test verifies that:
   * 1. SchemaDeduplication.deduplicateSchemas correctly normalizes schemas before hashing
   * 2. When re-checkpointing with the fix, redundant mappings are consolidated into one
   */
  test("REGRESSION FIX: re-checkpointing with redundant schema mappings consolidates them") {
    import io.indextables.spark.transaction.{AddAction, SchemaDeduplication}

    // Simulate the OLD bug: same schema with different field orderings -> different old hashes
    // The schemas are semantically identical but have different JSON representations
    val schema1 = """[{"name":"struct_0","type":"object","field_mappings":[{"name":"field_a","type":"text"},{"name":"field_b","type":"u64"},{"name":"field_c","type":"bool"}]}]"""
    val schema2 = """[{"name":"struct_0","type":"object","field_mappings":[{"name":"field_c","type":"bool"},{"name":"field_a","type":"text"},{"name":"field_b","type":"u64"}]}]"""
    val schema3 = """[{"name":"struct_0","type":"object","field_mappings":[{"name":"field_b","type":"u64"},{"name":"field_c","type":"bool"},{"name":"field_a","type":"text"}]}]"""

    // Create AddActions with the different schema representations (simulating old bug)
    val addActions = Seq(
      AddAction(
        path = "split1.split",
        partitionValues = Map.empty,
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true,
        docMappingJson = Some(schema1)  // First ordering
      ),
      AddAction(
        path = "split2.split",
        partitionValues = Map.empty,
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true,
        docMappingJson = Some(schema2)  // Second ordering
      ),
      AddAction(
        path = "split3.split",
        partitionValues = Map.empty,
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true,
        docMappingJson = Some(schema3)  // Third ordering
      )
    )

    // Run deduplication (this is what CHECKPOINT does internally)
    val (deduplicatedActions, schemaRegistry) = SchemaDeduplication.deduplicateSchemas(addActions, Map.empty)

    println(s"=== REGRESSION FIX VALIDATION ===")
    println(s"Input: 3 AddActions with semantically identical schemas (different JSON orderings)")
    println(s"Schema registry entries: ${schemaRegistry.size}")

    // CRITICAL: All 3 schemas should consolidate into 1 registry entry
    val schemaEntries = schemaRegistry.filterKeys(_.startsWith(SchemaDeduplication.SCHEMA_KEY_PREFIX))
    assert(
      schemaEntries.size == 1,
      s"REGRESSION: Expected 1 unique schema after normalization, got ${schemaEntries.size}. " +
        s"Re-checkpointing will NOT consolidate redundant mappings! " +
        s"Registry keys: ${schemaEntries.keys.mkString(", ")}"
    )

    // All deduplicated actions should reference the same hash
    val deduplicatedRefs = deduplicatedActions.collect { case a: AddAction => a }.flatMap(_.docMappingRef).toSet
    assert(
      deduplicatedRefs.size == 1,
      s"REGRESSION: Expected all actions to reference same hash, got ${deduplicatedRefs.size} unique hashes. " +
        s"Hashes: ${deduplicatedRefs.mkString(", ")}"
    )

    println(s"PASS: All 3 schemas consolidated into 1 registry entry with hash: ${deduplicatedRefs.head}")
    println(s"This confirms that re-checkpointing old tables will consolidate redundant mappings.")
  }

  /**
   * Additional verification: The same hash is produced regardless of field ordering.
   * This tests the computeSchemaHash normalization directly.
   */
  test("REGRESSION FIX: computeSchemaHash produces identical hash for reordered schemas") {
    import io.indextables.spark.transaction.SchemaDeduplication

    // Multiple representations of the same schema with nested field orderings
    val schemas = Seq(
      """[{"name":"struct_0","type":"object","field_mappings":[{"name":"field_a","type":"text"},{"name":"field_b","type":"u64"},{"name":"field_c","type":"bool"}]}]""",
      """[{"name":"struct_0","type":"object","field_mappings":[{"name":"field_c","type":"bool"},{"name":"field_a","type":"text"},{"name":"field_b","type":"u64"}]}]""",
      """[{"name":"struct_0","type":"object","field_mappings":[{"name":"field_b","type":"u64"},{"name":"field_c","type":"bool"},{"name":"field_a","type":"text"}]}]""",
      """[{"name":"struct_0","type":"object","field_mappings":[{"name":"field_c","type":"bool"},{"name":"field_b","type":"u64"},{"name":"field_a","type":"text"}]}]"""
    )

    val hashes = schemas.map(SchemaDeduplication.computeSchemaHash)

    println(s"=== HASH NORMALIZATION VERIFICATION ===")
    schemas.zip(hashes).zipWithIndex.foreach { case ((schema, hash), idx) =>
      println(s"Schema $idx: ${schema.take(80)}... -> Hash: $hash")
    }

    val uniqueHashes = hashes.toSet
    assert(
      uniqueHashes.size == 1,
      s"REGRESSION: computeSchemaHash should produce identical hash for reordered schemas! " +
        s"Got ${uniqueHashes.size} unique hashes: ${uniqueHashes.mkString(", ")}"
    )

    println(s"PASS: All ${schemas.size} schema orderings produce the same hash: ${uniqueHashes.head}")
  }
}
