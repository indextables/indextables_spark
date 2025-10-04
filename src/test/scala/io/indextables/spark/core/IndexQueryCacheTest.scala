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

import org.scalatest.funsuite.AnyFunSuite
import io.indextables.spark.filters.{IndexQueryFilter, IndexQueryAllFilter}

/**
 * Unit tests for IndexQuery cache implementation with Guava LRU cache.
 * Tests key generation, protocol stripping, and cache functionality.
 */
class IndexQueryCacheTest extends AnyFunSuite {

  test("generateInstanceKey should strip protocols correctly") {
    val testCases = Seq(
      ("s3://bucket/path", None, "bucket_path"),
      ("s3a://bucket/path", None, "bucket_path"),
      ("file:///local/path", None, "local_path"),  // Protocol stripped, slashes replaced
      ("hdfs://namenode/path", None, "namenode_path"),
      ("/local/path", None, "_local_path"),
      ("gs://bucket/path", None, "bucket_path")
    )

    testCases.foreach {
      case (tablePath, executionId, expected) =>
        val key = IndexTables4SparkScanBuilder.generateInstanceKey(tablePath, executionId)
        assert(
          key == expected,
          s"For path '$tablePath', expected key '$expected' but got '$key'"
        )
    }
  }

  test("generateInstanceKey should ignore execution ID (path-only keys)") {
    val tablePath = "s3://bucket/path"
    val executionId = Some("123")
    val key = IndexTables4SparkScanBuilder.generateInstanceKey(tablePath, executionId)

    // Execution ID is ignored to avoid timing issues between planning and execution
    assert(key == "bucket_path", s"Expected 'bucket_path' but got '$key'")
  }

  test("generateInstanceKey should handle multiple slashes correctly") {
    val tablePath = "s3://bucket//double//slashes"
    val key = IndexTables4SparkScanBuilder.generateInstanceKey(tablePath, None)

    assert(key == "bucket__double__slashes", s"Expected 'bucket__double__slashes' but got '$key'")
  }

  test("cache should store and retrieve IndexQuery filters") {
    val key = "test_key_1"
    val filters = Seq(
      IndexQueryFilter("content", "spark"),
      IndexQueryAllFilter("machine learning")
    )

    IndexTables4SparkScanBuilder.storeIndexQueries(key, filters)
    val retrieved = IndexTables4SparkScanBuilder.getIndexQueries(key)

    assert(retrieved.length == 2, s"Expected 2 filters but got ${retrieved.length}")
    assert(retrieved(0).isInstanceOf[IndexQueryFilter])
    assert(retrieved(1).isInstanceOf[IndexQueryAllFilter])
  }

  test("cache should return empty sequence for non-existent keys") {
    val retrieved = IndexTables4SparkScanBuilder.getIndexQueries("non_existent_key_12345")

    assert(retrieved.isEmpty, s"Expected empty sequence but got ${retrieved.length} items")
  }

  test("cache should allow clearing of entries") {
    val key = "test_key_clear"
    val filters = Seq(IndexQueryFilter("field", "query"))

    IndexTables4SparkScanBuilder.storeIndexQueries(key, filters)
    assert(IndexTables4SparkScanBuilder.getIndexQueries(key).nonEmpty)

    IndexTables4SparkScanBuilder.clearIndexQueries(key)
    assert(IndexTables4SparkScanBuilder.getIndexQueries(key).isEmpty)
  }

  test("cache should overwrite existing entries") {
    val key = "test_key_overwrite"
    val filters1 = Seq(IndexQueryFilter("field1", "query1"))
    val filters2 = Seq(IndexQueryFilter("field2", "query2"), IndexQueryFilter("field3", "query3"))

    IndexTables4SparkScanBuilder.storeIndexQueries(key, filters1)
    assert(IndexTables4SparkScanBuilder.getIndexQueries(key).length == 1)

    IndexTables4SparkScanBuilder.storeIndexQueries(key, filters2)
    val retrieved = IndexTables4SparkScanBuilder.getIndexQueries(key)

    assert(retrieved.length == 2, s"Expected 2 filters after overwrite but got ${retrieved.length}")
  }

  test("cache stats should be available") {
    val stats = IndexTables4SparkScanBuilder.getCacheStats()

    assert(stats.contains("Size:"), s"Stats should contain 'Size:' but got: $stats")
    assert(stats.contains("Hits:"), s"Stats should contain 'Hits:' but got: $stats")
    assert(stats.contains("Misses:"), s"Stats should contain 'Misses:' but got: $stats")
    assert(stats.contains("Evictions:"), s"Stats should contain 'Evictions:' but got: $stats")
  }

  test("concurrent queries use same key (path-based isolation)") {
    val tablePath = "s3://bucket/data"
    val key1 = IndexTables4SparkScanBuilder.generateInstanceKey(tablePath, Some("exec1"))
    val key2 = IndexTables4SparkScanBuilder.generateInstanceKey(tablePath, Some("exec2"))

    // Path-only keys mean concurrent queries share the same key
    // This is acceptable because IndexQuery filters are immutable per query plan
    assert(key1 == key2, s"Execution IDs are ignored, keys should be same: '$key1' vs '$key2'")
    assert(key1 == "bucket_data")
  }

  test("different paths in same query should have different keys") {
    val executionId = Some("123")
    val key1 = IndexTables4SparkScanBuilder.generateInstanceKey("s3://bucket/table1", executionId)
    val key2 = IndexTables4SparkScanBuilder.generateInstanceKey("s3://bucket/table2", executionId)

    assert(key1 != key2, s"Different table paths should produce different keys: '$key1' vs '$key2'")
  }
}
