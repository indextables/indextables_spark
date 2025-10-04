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
 * Unit tests for IndexQuery cache implementation using stable relation keys.
 * Tests storage and retrieval based on output attribute expression IDs.
 */
class IndexQueryCacheTest extends AnyFunSuite {

  test("cache should store and retrieve IndexQuery filters") {
    val key = "relation_123_4"
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
    val retrieved = IndexTables4SparkScanBuilder.getIndexQueries("relation_nonexistent_999")

    assert(retrieved.isEmpty, s"Expected empty sequence but got ${retrieved.length} items")
  }

  test("cache should allow clearing of entries") {
    val key = "relation_clear_test_5"
    val filters = Seq(IndexQueryFilter("field", "query"))

    IndexTables4SparkScanBuilder.storeIndexQueries(key, filters)
    assert(IndexTables4SparkScanBuilder.getIndexQueries(key).nonEmpty)

    IndexTables4SparkScanBuilder.clearIndexQueries(key)
    assert(IndexTables4SparkScanBuilder.getIndexQueries(key).isEmpty)
  }

  test("cache should overwrite existing entries") {
    val key = "relation_overwrite_test_3"
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

  test("different relation keys should not interfere") {
    val key1 = "relation_100_4"
    val key2 = "relation_200_4"

    val filters1 = Seq(IndexQueryFilter("field1", "query1"))
    val filters2 = Seq(IndexQueryFilter("field2", "query2"))

    IndexTables4SparkScanBuilder.storeIndexQueries(key1, filters1)
    IndexTables4SparkScanBuilder.storeIndexQueries(key2, filters2)

    val retrieved1 = IndexTables4SparkScanBuilder.getIndexQueries(key1)
    val retrieved2 = IndexTables4SparkScanBuilder.getIndexQueries(key2)

    assert(retrieved1.length == 1)
    assert(retrieved2.length == 1)
    assert(retrieved1(0).asInstanceOf[IndexQueryFilter].queryString == "query1")
    assert(retrieved2(0).asInstanceOf[IndexQueryFilter].queryString == "query2")
  }

  test("generateRelationKey should create consistent keys from schema") {
    import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

    val schema = StructType(Seq(
      StructField("id", StringType),
      StructField("content", StringType),
      StructField("score", IntegerType)
    ))

    val key1 = IndexTables4SparkScanBuilder.generateRelationKey(schema)
    val key2 = IndexTables4SparkScanBuilder.generateRelationKey(schema)

    // Keys should be consistent for the same schema
    assert(key1 == key2, s"Expected consistent keys but got '$key1' and '$key2'")
    assert(key1.startsWith("relation_"), s"Key should start with 'relation_' but got '$key1'")
    assert(key1.endsWith("_3"), s"Key should end with schema length '_3' but got '$key1'")
  }
}
