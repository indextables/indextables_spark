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

package io.indextables.spark.stats

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataSkippingOptimizationTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    // Reset all caches and metrics before each test
    DataSkippingMetrics.resetAll()
    FilterExpressionCache.invalidateAll()
  }

  // ==========================================================================
  // DataSkippingMetrics Tests
  // ==========================================================================

  test("DataSkippingMetrics: records scan with partition pruning") {
    DataSkippingMetrics.recordScan(
      tablePath = "/test/table",
      totalFiles = 100,
      afterPartitionPruning = 40,
      afterDataSkipping = 20
    )

    val stats = DataSkippingMetrics.getTableStats("/test/table")
    stats shouldBe defined
    stats.get.totalFiles shouldBe 100
    stats.get.partitionPrunedFiles shouldBe 60 // 100 - 40
    stats.get.dataSkippedFiles shouldBe 20     // 40 - 20
    stats.get.finalFiles shouldBe 20
    stats.get.partitionSkipRate shouldBe 0.6 +- 0.01
    stats.get.dataSkipRate shouldBe 0.5 +- 0.01
    stats.get.totalSkipRate shouldBe 0.8 +- 0.01
  }

  test("DataSkippingMetrics: tracks filter type contributions") {
    DataSkippingMetrics.recordScan(
      tablePath = "/test/table",
      totalFiles = 100,
      afterPartitionPruning = 100,
      afterDataSkipping = 50,
      filterTypesUsed = Map("EqualTo" -> 30, "GreaterThan" -> 20)
    )

    val stats = DataSkippingMetrics.getTableStats("/test/table")
    stats shouldBe defined
    stats.get.filterTypes should contain("EqualTo" -> 30L)
    stats.get.filterTypes should contain("GreaterThan" -> 20L)
  }

  test("DataSkippingMetrics: aggregates multiple scans") {
    DataSkippingMetrics.recordScan("/test/table", 100, 80, 60)
    DataSkippingMetrics.recordScan("/test/table", 50, 30, 20)

    val stats = DataSkippingMetrics.getTableStats("/test/table")
    stats shouldBe defined
    stats.get.totalFiles shouldBe 150
    stats.get.partitionPrunedFiles shouldBe 40 // (100-80) + (50-30)
    stats.get.dataSkippedFiles shouldBe 30     // (80-60) + (30-20)
    stats.get.finalFiles shouldBe 80           // 60 + 20
  }

  test("DataSkippingMetrics: global stats aggregate across tables") {
    DataSkippingMetrics.recordScan("/table1", 100, 80, 60)
    DataSkippingMetrics.recordScan("/table2", 200, 150, 100)

    val globalStats = DataSkippingMetrics.getGlobalStats()
    globalStats.totalFiles shouldBe 300
    globalStats.partitionPrunedFiles shouldBe 70 // (100-80) + (200-150)
    globalStats.dataSkippedFiles shouldBe 70     // (80-60) + (150-100)
    globalStats.finalFiles shouldBe 160          // 60 + 100
  }

  test("DataSkippingMetrics: formatStats produces readable output") {
    DataSkippingMetrics.recordScan("/test/table", 100, 60, 30, Map("EqualTo" -> 20, "In" -> 10))

    val stats = DataSkippingMetrics.getTableStats("/test/table").get
    val formatted = DataSkippingMetrics.formatStats(stats)

    formatted should include("Total files considered: 100")
    formatted should include("Partition pruned: 40")
    formatted should include("Data skipped (min/max): 30")
    formatted should include("Final files to scan: 30")
    formatted should include("EqualTo: 20 files skipped")
    formatted should include("In: 10 files skipped")
  }

  // ==========================================================================
  // ExpressionSimplifier Tests
  // ==========================================================================

  test("ExpressionSimplifier: IsNull on non-nullable column returns AlwaysFalse") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))

    val result = ExpressionSimplifier.simplifyFilter(IsNull("id"), schema)
    ExpressionSimplifier.isAlwaysFalse(result) shouldBe true
  }

  test("ExpressionSimplifier: IsNotNull on non-nullable column returns AlwaysTrue") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))

    val result = ExpressionSimplifier.simplifyFilter(IsNotNull("id"), schema)
    ExpressionSimplifier.isAlwaysTrue(result) shouldBe true
  }

  test("ExpressionSimplifier: IsNull on nullable column is unchanged") {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true)
    ))

    val filter = IsNull("name")
    val result = ExpressionSimplifier.simplifyFilter(filter, schema)
    result shouldBe filter
  }

  test("ExpressionSimplifier: IN with empty list returns AlwaysFalse") {
    val schema = StructType(Seq(StructField("status", StringType, nullable = true)))

    val result = ExpressionSimplifier.simplifyFilter(In("status", Array.empty), schema)
    ExpressionSimplifier.isAlwaysFalse(result) shouldBe true
  }

  test("ExpressionSimplifier: IN with single value converts to EqualTo") {
    val schema = StructType(Seq(StructField("status", StringType, nullable = true)))

    val result = ExpressionSimplifier.simplifyFilter(In("status", Array("active")), schema)
    result shouldBe EqualTo("status", "active")
  }

  test("ExpressionSimplifier: AND with AlwaysFalse returns AlwaysFalse") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))

    // IsNull on non-nullable -> AlwaysFalse, so AND should return AlwaysFalse
    val filter = And(IsNull("id"), EqualTo("id", 1))
    val result = ExpressionSimplifier.simplifyFilter(filter, schema)
    ExpressionSimplifier.isAlwaysFalse(result) shouldBe true
  }

  test("ExpressionSimplifier: AND with AlwaysTrue returns other operand") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))

    // IsNotNull on non-nullable -> AlwaysTrue, so AND should return EqualTo
    val filter = And(IsNotNull("id"), EqualTo("id", 1))
    val result = ExpressionSimplifier.simplifyFilter(filter, schema)
    result shouldBe EqualTo("id", 1)
  }

  test("ExpressionSimplifier: OR with AlwaysTrue returns AlwaysTrue") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))

    // IsNotNull on non-nullable -> AlwaysTrue, so OR should return AlwaysTrue
    val filter = Or(IsNotNull("id"), EqualTo("id", 1))
    val result = ExpressionSimplifier.simplifyFilter(filter, schema)
    ExpressionSimplifier.isAlwaysTrue(result) shouldBe true
  }

  test("ExpressionSimplifier: OR with AlwaysFalse returns other operand") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))

    // IsNull on non-nullable -> AlwaysFalse, so OR should return EqualTo
    val filter = Or(IsNull("id"), EqualTo("id", 1))
    val result = ExpressionSimplifier.simplifyFilter(filter, schema)
    result shouldBe EqualTo("id", 1)
  }

  test("ExpressionSimplifier: NOT(NOT(x)) returns x") {
    val schema = StructType(Seq(StructField("status", StringType, nullable = true)))

    val filter = Not(Not(EqualTo("status", "active")))
    val result = ExpressionSimplifier.simplifyFilter(filter, schema)
    result shouldBe EqualTo("status", "active")
  }

  test("ExpressionSimplifier: NOT(AlwaysTrue) returns AlwaysFalse") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))

    // NOT(IsNotNull(non-nullable)) -> NOT(AlwaysTrue) -> AlwaysFalse
    val filter = Not(IsNotNull("id"))
    val result = ExpressionSimplifier.simplifyFilter(filter, schema)
    ExpressionSimplifier.isAlwaysFalse(result) shouldBe true
  }

  test("ExpressionSimplifier: simplify array removes AlwaysTrue filters") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("status", StringType, nullable = true)
    ))

    val filters = Array[Filter](
      IsNotNull("id"),              // -> AlwaysTrue (non-nullable)
      EqualTo("status", "active")
    )

    val simplified = ExpressionSimplifier.simplify(filters, schema)
    simplified should have length 1
    simplified(0) shouldBe EqualTo("status", "active")
  }

  test("ExpressionSimplifier: simplify array with AlwaysFalse returns AlwaysFalse only") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))

    val filters = Array[Filter](
      IsNull("id"),                 // -> AlwaysFalse (non-nullable)
      EqualTo("id", 1)
    )

    val simplified = ExpressionSimplifier.simplify(filters, schema)
    simplified should have length 1
    ExpressionSimplifier.isAlwaysFalse(simplified(0)) shouldBe true
  }

  test("ExpressionSimplifier: getReferencedColumns extracts all column names") {
    val filter = And(
      Or(EqualTo("a", 1), GreaterThan("b", 2)),
      In("c", Array(1, 2, 3))
    )

    val columns = ExpressionSimplifier.getReferencedColumns(filter)
    columns shouldBe Set("a", "b", "c")
  }

  // ==========================================================================
  // FilterExpressionCache Tests
  // ==========================================================================

  test("FilterExpressionCache: caches simplified filters") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))
    val filters = Array[Filter](IsNotNull("id"))

    // First call - should be a miss
    val result1 = FilterExpressionCache.getOrSimplify(filters, schema)
    val (_, misses1, _, _) = FilterExpressionCache.getStats()
    misses1 shouldBe 1

    // Second call with same inputs - should be a hit
    val result2 = FilterExpressionCache.getOrSimplify(filters, schema)
    val (hits2, misses2, _, _) = FilterExpressionCache.getStats()
    hits2 shouldBe 1
    misses2 shouldBe 1

    result1 shouldBe result2
  }

  test("FilterExpressionCache: different schemas have different cache entries") {
    val schema1 = StructType(Seq(StructField("id", IntegerType, nullable = false)))
    val schema2 = StructType(Seq(StructField("id", IntegerType, nullable = true)))
    val filters = Array[Filter](IsNull("id"))

    val result1 = FilterExpressionCache.getOrSimplify(filters, schema1)
    val result2 = FilterExpressionCache.getOrSimplify(filters, schema2)

    // Non-nullable -> AlwaysFalse, nullable -> unchanged
    result1(0) shouldBe ExpressionSimplifier.AlwaysFalse
    result2(0) shouldBe IsNull("id")
  }

  test("FilterExpressionCache: caches IN filter ranges") {
    val values = Array[Any]("apple", "banana", "cherry")

    // First call - should be a miss
    val range1 = FilterExpressionCache.getOrComputeInRange("fruit", values)
    val (_, _, _, inMisses1) = FilterExpressionCache.getStats()
    inMisses1 shouldBe 1

    // Second call - should be a hit
    val range2 = FilterExpressionCache.getOrComputeInRange("fruit", values)
    val (_, _, inHits2, inMisses2) = FilterExpressionCache.getStats()
    inHits2 shouldBe 1
    inMisses2 shouldBe 1

    range1 shouldBe range2
    range1.get.minValue.toString shouldBe "apple"
    range1.get.maxValue.toString shouldBe "cherry"
    range1.get.valueCount shouldBe 3
  }

  test("FilterExpressionCache: IN range with numeric values") {
    val values = Array[Any](10, 5, 20, 15)

    val range = FilterExpressionCache.getOrComputeInRange("score", values)
    range shouldBe defined
    // String comparison, so "10" < "15" < "20" < "5"
    range.get.minValue.toString shouldBe "10"
    range.get.maxValue.toString shouldBe "5"
    range.get.valueCount shouldBe 4
  }

  test("FilterExpressionCache: IN range with nulls filters them out") {
    val values = Array[Any]("apple", null, "cherry")

    val range = FilterExpressionCache.getOrComputeInRange("fruit", values)
    range shouldBe defined
    range.get.valueCount shouldBe 2 // null is filtered out
  }

  test("FilterExpressionCache: IN range with all nulls returns None") {
    val values = Array[Any](null, null)

    val range = FilterExpressionCache.getOrComputeInRange("fruit", values)
    range shouldBe None
  }

  test("FilterExpressionCache: invalidateAll clears both caches") {
    val schema = StructType(Seq(StructField("id", IntegerType, nullable = true)))

    FilterExpressionCache.getOrSimplify(Array(EqualTo("id", 1)), schema)
    FilterExpressionCache.getOrComputeInRange("id", Array[Any](1, 2, 3))

    val (size1, size2) = FilterExpressionCache.getCacheSizes()
    size1 should be > 0L
    size2 should be > 0L

    FilterExpressionCache.invalidateAll()

    val (sizeAfter1, sizeAfter2) = FilterExpressionCache.getCacheSizes()
    sizeAfter1 shouldBe 0
    sizeAfter2 shouldBe 0
  }

  test("FilterExpressionCache: hit rates calculation") {
    val schema = StructType(Seq(StructField("id", IntegerType, nullable = true)))
    val filters = Array[Filter](EqualTo("id", 1))

    // 1 miss
    FilterExpressionCache.getOrSimplify(filters, schema)
    // 2 hits
    FilterExpressionCache.getOrSimplify(filters, schema)
    FilterExpressionCache.getOrSimplify(filters, schema)

    val (simplifiedRate, _) = FilterExpressionCache.getHitRates()
    simplifiedRate shouldBe (2.0 / 3.0) +- 0.01
  }

  // ==========================================================================
  // Integration Tests
  // ==========================================================================

  test("Integration: expression simplification combined with caching") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("status", StringType, nullable = true),
      StructField("score", IntegerType, nullable = true)
    ))

    val filters = Array[Filter](
      IsNotNull("id"),              // -> AlwaysTrue
      EqualTo("status", "active"),
      In("score", Array(1))         // -> EqualTo
    )

    // First call
    val simplified1 = FilterExpressionCache.getOrSimplify(filters, schema)

    // Should have removed AlwaysTrue and converted IN to EqualTo
    simplified1 should have length 2
    simplified1 should contain(EqualTo("status", "active"))
    simplified1 should contain(EqualTo("score", 1))

    // Second call should hit cache
    val simplified2 = FilterExpressionCache.getOrSimplify(filters, schema)
    simplified2 shouldBe simplified1

    val (hits, misses, _, _) = FilterExpressionCache.getStats()
    hits shouldBe 1
    misses shouldBe 1
  }

  test("Integration: canBeSatisfied with AlwaysFalse") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))

    // Filter that simplifies to AlwaysFalse
    val unsatisfiableFilters = Array[Filter](IsNull("id"))
    ExpressionSimplifier.canBeSatisfied(unsatisfiableFilters, schema) shouldBe false

    // Filter that can be satisfied
    val satisfiableFilters = Array[Filter](EqualTo("id", 1))
    ExpressionSimplifier.canBeSatisfied(satisfiableFilters, schema) shouldBe true
  }

  test("Integration: isNoOp with all AlwaysTrue") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))

    // All non-nullable IsNotNull filters -> all AlwaysTrue -> no-op
    val noOpFilters = Array[Filter](IsNotNull("id"), IsNotNull("name"))
    ExpressionSimplifier.isNoOp(noOpFilters, schema) shouldBe true

    // Regular filter -> not a no-op
    val realFilters = Array[Filter](EqualTo("id", 1))
    ExpressionSimplifier.isNoOp(realFilters, schema) shouldBe false
  }
}
