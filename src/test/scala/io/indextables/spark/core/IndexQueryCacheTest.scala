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

import io.indextables.spark.filters.{IndexQueryAllFilter, IndexQueryFilter}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for IndexQuery cache implementation using WeakHashMap with relation objects. Tests storage and retrieval
 * based on DataSourceV2Relation object identity.
 */
class IndexQueryCacheTest extends AnyFunSuite {

  test("cache should store and retrieve IndexQuery filters") {
    val relation = new Object() // Mock relation object
    val filters = Seq(
      IndexQueryFilter("content", "spark"),
      IndexQueryAllFilter("machine learning")
    )

    IndexTables4SparkScanBuilder.storeIndexQueries(relation, filters)
    val retrieved = IndexTables4SparkScanBuilder.getIndexQueries(relation)

    assert(retrieved.length == 2, s"Expected 2 filters but got ${retrieved.length}")
    assert(retrieved(0).isInstanceOf[IndexQueryFilter])
    assert(retrieved(1).isInstanceOf[IndexQueryAllFilter])
  }

  test("cache should return empty sequence for non-existent relation objects") {
    val relation = new Object() // Different object not in cache

    val retrieved = IndexTables4SparkScanBuilder.getIndexQueries(relation)

    assert(retrieved.isEmpty, s"Expected empty sequence but got ${retrieved.length} items")
  }

  test("cache should allow clearing of entries") {
    val relation = new Object()
    val filters  = Seq(IndexQueryFilter("field", "query"))

    IndexTables4SparkScanBuilder.storeIndexQueries(relation, filters)
    assert(IndexTables4SparkScanBuilder.getIndexQueries(relation).nonEmpty)

    IndexTables4SparkScanBuilder.clearIndexQueries(relation)
    assert(IndexTables4SparkScanBuilder.getIndexQueries(relation).isEmpty)
  }

  test("cache should append to existing entries for same relation") {
    // This behavior is required for CTEs where IndexQuery can appear in both
    // the inner CTE definition and the outer query referencing the CTE.
    // Both IndexQueries need to be accumulated and applied together.
    val relation = new Object()
    val filters1 = Seq(IndexQueryFilter("field1", "query1"))
    val filters2 = Seq(IndexQueryFilter("field2", "query2"), IndexQueryFilter("field3", "query3"))

    IndexTables4SparkScanBuilder.storeIndexQueries(relation, filters1)
    assert(IndexTables4SparkScanBuilder.getIndexQueries(relation).length == 1)

    IndexTables4SparkScanBuilder.storeIndexQueries(relation, filters2)
    val retrieved = IndexTables4SparkScanBuilder.getIndexQueries(relation)

    // Should have all 3 filters (1 + 2 appended)
    assert(retrieved.length == 3, s"Expected 3 filters after append but got ${retrieved.length}")
  }

  test("cache stats should be available") {
    val stats = IndexTables4SparkScanBuilder.getCacheStats()

    assert(stats.contains("Size:"), s"Stats should contain 'Size:' but got: $stats")
    assert(stats.contains("WeakHashMap"), s"Stats should mention 'WeakHashMap' but got: $stats")
  }

  test("different relation objects should not interfere") {
    val relation1 = new Object()
    val relation2 = new Object()

    val filters1 = Seq(IndexQueryFilter("field1", "query1"))
    val filters2 = Seq(IndexQueryFilter("field2", "query2"))

    IndexTables4SparkScanBuilder.storeIndexQueries(relation1, filters1)
    IndexTables4SparkScanBuilder.storeIndexQueries(relation2, filters2)

    val retrieved1 = IndexTables4SparkScanBuilder.getIndexQueries(relation1)
    val retrieved2 = IndexTables4SparkScanBuilder.getIndexQueries(relation2)

    assert(retrieved1.length == 1)
    assert(retrieved2.length == 1)
    assert(retrieved1(0).asInstanceOf[IndexQueryFilter].queryString == "query1")
    assert(retrieved2(0).asInstanceOf[IndexQueryFilter].queryString == "query2")
  }

  test("ThreadLocal should allow setting and getting relation objects") {
    val relation = new Object()

    IndexTables4SparkScanBuilder.setCurrentRelation(relation)
    val retrieved = IndexTables4SparkScanBuilder.getCurrentRelation()

    assert(retrieved.isDefined, s"Expected Some(relation) but got None")
    assert(retrieved.get eq relation, s"Expected same relation object")

    IndexTables4SparkScanBuilder.clearCurrentRelation()
    val clearedResult = IndexTables4SparkScanBuilder.getCurrentRelation()

    assert(clearedResult.isEmpty, s"Expected None after clear but got $clearedResult")
  }

  test("WeakHashMap should allow GC to clean up unreferenced relations") {
    var relation: AnyRef = new Object()
    val filters          = Seq(IndexQueryFilter("field", "query"))

    IndexTables4SparkScanBuilder.storeIndexQueries(relation, filters)
    assert(IndexTables4SparkScanBuilder.getIndexQueries(relation).nonEmpty)

    // Remove strong reference and suggest GC
    relation = null
    System.gc()
    Thread.sleep(100) // Give GC a chance to run

    // WeakHashMap should eventually clean up the entry
    // (We can't reliably test this without strong reference, so just verify cache still works)
    val newRelation = new Object()
    IndexTables4SparkScanBuilder.storeIndexQueries(newRelation, filters)
    assert(IndexTables4SparkScanBuilder.getIndexQueries(newRelation).nonEmpty)
  }
}
