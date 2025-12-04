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

package io.indextables.spark.transaction

import java.util.Collections

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase
import org.scalatest.BeforeAndAfterEach

/**
 * Test to verify that getSchema() uses caching correctly.
 *
 * REGRESSION TEST: This test was created to catch a bug where getSchema() was not using the cached metadata, causing
 * repeated reads from the transaction log on every query. The TransactionLogAdapter was missing an override for
 * getSchema(), so it fell back to the non-caching parent TransactionLog.getSchema() implementation.
 */
class TransactionLogSchemaCacheTest extends TestBase with BeforeAndAfterEach {

  var transactionLog: TransactionLog = _
  var tablePath: String              = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tablePath = tempDir + "/test_schema_cache_table"
  }

  override def afterEach(): Unit = {
    if (transactionLog != null) {
      transactionLog.close()
    }
    super.afterEach()
  }

  test("getSchema should use caching - repeated calls should result in cache hits") {
    // Use optimized transaction log (default behavior)
    val options = new CaseInsensitiveStringMap(Collections.emptyMap())
    transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)

    // Initialize the transaction log with a schema
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("value", IntegerType, nullable = true)
      )
    )
    transactionLog.initialize(schema)

    // Add a file to ensure the table has data
    transactionLog.addFile(AddAction("file1.split", Map.empty, 1000L, System.currentTimeMillis(), true))

    // Get initial cache stats
    val stats1        = transactionLog.getCacheStats().get
    val initialHits   = stats1.hits
    val initialMisses = stats1.misses
    println(s"Initial cache stats: hits=$initialHits, misses=$initialMisses")

    // First getSchema call - may populate cache (miss expected)
    val schema1 = transactionLog.getSchema()
    assert(schema1.isDefined, "Should have a schema")
    assert(schema1.get.fields.length == 3, "Schema should have 3 fields")

    val stats2 = transactionLog.getCacheStats().get
    println(s"After first getSchema: hits=${stats2.hits}, misses=${stats2.misses}")

    // Second getSchema call - SHOULD use cache (hit expected)
    val schema2 = transactionLog.getSchema()
    assert(schema2.isDefined, "Should still have a schema")
    assert(schema2.get == schema1.get, "Schema should be identical")

    val stats3 = transactionLog.getCacheStats().get
    println(s"After second getSchema: hits=${stats3.hits}, misses=${stats3.misses}")

    // Third getSchema call - SHOULD use cache (hit expected)
    val schema3 = transactionLog.getSchema()
    assert(schema3.isDefined, "Should still have a schema")

    val stats4 = transactionLog.getCacheStats().get
    println(s"After third getSchema: hits=${stats4.hits}, misses=${stats4.misses}")

    // CRITICAL ASSERTION: After 3 getSchema calls, we should have at least 2 cache hits
    // (the first call may miss to populate the cache, but subsequent calls should hit)
    val totalHitsAfterSchemaCalls = stats4.hits - initialHits

    // With the bug, hits will be 0 because getSchema() bypasses caching
    // After the fix, hits should be >= 2 (second and third calls should hit)
    assert(
      totalHitsAfterSchemaCalls >= 2,
      s"getSchema should use caching! Expected at least 2 cache hits for repeated calls, " +
        s"but got $totalHitsAfterSchemaCalls hits. " +
        s"This indicates getSchema() is bypassing the cache and re-reading the transaction log each time."
    )
  }

  test("getSchema should behave like getMetadata for caching") {
    // This test compares getSchema caching behavior with getMetadata caching behavior
    // Both should use caching, so their behavior should be similar

    val options = new CaseInsensitiveStringMap(Collections.emptyMap())
    transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)

    // Initialize the transaction log
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("name", StringType, nullable = true)
      )
    )
    transactionLog.initialize(schema)
    transactionLog.addFile(AddAction("file1.split", Map.empty, 1000L, System.currentTimeMillis(), true))

    // Test getMetadata caching (known to work correctly)
    val metadataStats1      = transactionLog.getCacheStats().get
    val metadataInitialHits = metadataStats1.hits

    transactionLog.getMetadata()
    transactionLog.getMetadata()
    transactionLog.getMetadata()

    val metadataStats2 = transactionLog.getCacheStats().get
    val metadataHits   = metadataStats2.hits - metadataInitialHits
    println(s"getMetadata cache hits after 3 calls: $metadataHits")

    // Invalidate cache to reset for schema test
    transactionLog.invalidateCache()

    // Test getSchema caching
    val schemaStats1      = transactionLog.getCacheStats().get
    val schemaInitialHits = schemaStats1.hits

    transactionLog.getSchema()
    transactionLog.getSchema()
    transactionLog.getSchema()

    val schemaStats2 = transactionLog.getCacheStats().get
    val schemaHits   = schemaStats2.hits - schemaInitialHits
    println(s"getSchema cache hits after 3 calls: $schemaHits")

    // Both should have at least 2 cache hits (first call populates, subsequent calls hit)
    assert(metadataHits >= 2, s"getMetadata should have cache hits, got $metadataHits")
    assert(
      schemaHits >= 2,
      s"getSchema should have similar caching behavior to getMetadata. " +
        s"getMetadata had $metadataHits hits, but getSchema had only $schemaHits hits. " +
        s"This indicates getSchema() is not properly delegating to the cached implementation."
    )
  }

  test("TransactionLogAdapter should delegate getSchema to OptimizedTransactionLog") {
    // This test specifically verifies that TransactionLogAdapter properly delegates
    // getSchema() to the underlying OptimizedTransactionLog, which has caching.

    val options = new CaseInsensitiveStringMap(Collections.emptyMap())
    transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)

    // Verify we're using the adapter (which should wrap OptimizedTransactionLog)
    assert(
      transactionLog.isInstanceOf[TransactionLogAdapter],
      "Should be using TransactionLogAdapter for default configuration"
    )

    // Initialize
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("name", StringType, nullable = true)
      )
    )
    transactionLog.initialize(schema)

    // Call getSchema multiple times
    val result1 = transactionLog.getSchema()
    val result2 = transactionLog.getSchema()
    val result3 = transactionLog.getSchema()

    // All results should be identical
    assert(result1 == result2, "Schema results should be identical")
    assert(result2 == result3, "Schema results should be identical")

    // Verify caching is working
    val stats = transactionLog.getCacheStats().get
    println(s"Final cache stats: $stats")

    // With proper delegation, we should see cache hits
    // The exact number depends on implementation details, but should be > 0
    assert(
      stats.hits > 0,
      s"Cache should have hits when getSchema() is called multiple times, " +
        s"but got ${stats.hits} hits. This suggests getSchema() is not using the cache."
    )
  }
}
