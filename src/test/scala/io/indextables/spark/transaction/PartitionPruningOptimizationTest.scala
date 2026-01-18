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

import org.apache.spark.sql.sources._

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for partition pruning optimizations.
 */
class PartitionPruningOptimizationTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    // Clear caches before each test
    PartitionFilterCache.invalidate()
  }

  // Helper to create AddActions for testing
  private def createAddAction(path: String, partitionValues: Map[String, String]): AddAction =
    AddAction(
      path = path,
      partitionValues = partitionValues,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true
    )

  // ============= PartitionFilterCache Tests =============

  test("PartitionFilterCache: cache hit on repeated evaluations") {
    // Test the cache directly (bypassing prunePartitions which uses index for EqualTo)
    val filters = Array[Filter](GreaterThan("year", "2023"))  // Use range filter to test cache
    val partitionValues = Map("year" -> "2024")

    // First call - cache miss
    val result1 = PartitionFilterCache.getOrCompute(filters, partitionValues, true)
    val (hits1, misses1, _) = PartitionFilterCache.getStats()

    result1 shouldBe true
    misses1 shouldBe 1
    hits1 shouldBe 0

    // Second call - cache hit
    val result2 = PartitionFilterCache.getOrCompute(filters, partitionValues, true)
    val (hits2, misses2, _) = PartitionFilterCache.getStats()

    result2 shouldBe true
    hits2 shouldBe 1
    misses2 shouldBe 1
  }

  test("PartitionFilterCache: different filters have different cache entries") {
    val partitionValues = Map("year" -> "2024")

    // First filter set (use range filters to test cache)
    val filters1 = Array[Filter](GreaterThan("year", "2023"))
    PartitionFilterCache.getOrCompute(filters1, partitionValues, true)

    // Second filter set (different)
    val filters2 = Array[Filter](GreaterThan("year", "2025"))
    PartitionFilterCache.getOrCompute(filters2, partitionValues, false)

    val (hits, misses, _) = PartitionFilterCache.getStats()
    misses shouldBe 2 // Both should be misses
  }

  test("PartitionFilterCache: different partition values have different cache entries") {
    val filters = Array[Filter](GreaterThan("year", "2023"))  // Use range filter to test cache

    // First partition values
    val partitionValues1 = Map("year" -> "2024")
    PartitionFilterCache.getOrCompute(filters, partitionValues1, true)

    // Second partition values (different)
    val partitionValues2 = Map("year" -> "2025")
    PartitionFilterCache.getOrCompute(filters, partitionValues2, true)

    val (hits, misses, _) = PartitionFilterCache.getStats()
    misses shouldBe 2 // Both should be misses
  }

  test("PartitionFilterCache: invalidation clears cache") {
    val filters = Array[Filter](GreaterThan("year", "2023"))  // Use range filter to test cache
    val partitionValues = Map("year" -> "2024")

    // Add entry to cache
    PartitionFilterCache.getOrCompute(filters, partitionValues, true)
    PartitionFilterCache.size() should be > 0

    // Invalidate
    PartitionFilterCache.invalidate()

    // Cache should be empty
    PartitionFilterCache.size() shouldBe 0

    // Stats should be reset
    val (hits, misses, _) = PartitionFilterCache.getStats()
    hits shouldBe 0
    misses shouldBe 0
  }

  // ============= PartitionIndex Tests =============

  test("PartitionIndex: builds correctly from AddActions") {
    val addActions = Seq(
      createAddAction("file1.split", Map("region" -> "us-east", "date" -> "2024-01-01")),
      createAddAction("file2.split", Map("region" -> "us-east", "date" -> "2024-01-01")),
      createAddAction("file3.split", Map("region" -> "us-west", "date" -> "2024-01-01")),
      createAddAction("file4.split", Map("region" -> "us-east", "date" -> "2024-01-02"))
    )

    val index = PartitionIndex.build(addActions, Seq("region", "date"))

    index.totalFiles shouldBe 4
    index.partitionCount shouldBe 3 // 3 unique partition combinations
    index.partitionColumns shouldBe Seq("region", "date")
  }

  test("PartitionIndex: equality lookup returns correct partitions") {
    val addActions = Seq(
      createAddAction("file1.split", Map("region" -> "us-east")),
      createAddAction("file2.split", Map("region" -> "us-east")),
      createAddAction("file3.split", Map("region" -> "us-west")),
      createAddAction("file4.split", Map("region" -> "eu-central"))
    )

    val index = PartitionIndex.build(addActions, Seq("region"))

    val usEastPartitions = index.getPartitionsForEquality("region", "us-east")
    usEastPartitions.size shouldBe 1
    usEastPartitions.head shouldBe Map("region" -> "us-east")
  }

  test("PartitionIndex: IN lookup returns correct partitions") {
    val addActions = Seq(
      createAddAction("file1.split", Map("region" -> "us-east")),
      createAddAction("file2.split", Map("region" -> "us-west")),
      createAddAction("file3.split", Map("region" -> "eu-central")),
      createAddAction("file4.split", Map("region" -> "ap-south"))
    )

    val index = PartitionIndex.build(addActions, Seq("region"))

    val matchingPartitions = index.getPartitionsForIn("region", Seq("us-east", "us-west"))
    matchingPartitions.size shouldBe 2
    matchingPartitions should contain(Map("region" -> "us-east"))
    matchingPartitions should contain(Map("region" -> "us-west"))
  }

  test("PartitionIndex: getFilesForPartitions returns correct files") {
    val addActions = Seq(
      createAddAction("file1.split", Map("region" -> "us-east")),
      createAddAction("file2.split", Map("region" -> "us-east")),
      createAddAction("file3.split", Map("region" -> "us-west"))
    )

    val index = PartitionIndex.build(addActions, Seq("region"))

    val files = index.getFilesForPartitions(Set(Map("region" -> "us-east")))
    files.length shouldBe 2
    files.map(_.path) should contain allOf("file1.split", "file2.split")
  }

  test("PartitionIndex: empty index for no partition columns") {
    val addActions = Seq(createAddAction("file1.split", Map.empty))
    val index = PartitionIndex.build(addActions, Seq.empty)

    index.isEmpty shouldBe true
    index.partitionCount shouldBe 0
    index.totalFiles shouldBe 1
  }

  // ============= FilterSelectivityEstimator Tests =============

  test("FilterSelectivityEstimator: equality filters are most selective") {
    val equalityScore = FilterSelectivityEstimator.estimateSelectivity(EqualTo("col", "value"))
    val rangeScore = FilterSelectivityEstimator.estimateSelectivity(GreaterThan("col", 10))

    equalityScore should be < rangeScore
  }

  test("FilterSelectivityEstimator: small IN is more selective than large IN") {
    val smallInScore = FilterSelectivityEstimator.estimateSelectivity(In("col", Array("a", "b")))
    val largeInScore = FilterSelectivityEstimator.estimateSelectivity(In("col", Array("a", "b", "c", "d", "e", "f", "g")))

    smallInScore should be < largeInScore
  }

  test("FilterSelectivityEstimator: orders filters by selectivity") {
    val filters = Array[Filter](
      GreaterThan("age", 20),        // Range - less selective
      In("region", Array("us-east", "us-west")),  // Small IN - selective
      EqualTo("status", "active")    // Equality - most selective
    )

    val ordered = FilterSelectivityEstimator.orderBySelectivity(filters)

    // Most selective (EqualTo) should be first
    ordered(0) shouldBe a[EqualTo]
    // Least selective (GreaterThan) should be last
    ordered(2) shouldBe a[GreaterThan]
  }

  test("FilterSelectivityEstimator: canUseIndexLookup identifies indexable filters") {
    FilterSelectivityEstimator.canUseIndexLookup(EqualTo("col", "value")) shouldBe true
    FilterSelectivityEstimator.canUseIndexLookup(EqualNullSafe("col", "value")) shouldBe true
    FilterSelectivityEstimator.canUseIndexLookup(In("col", Array("a", "b"))) shouldBe true

    FilterSelectivityEstimator.canUseIndexLookup(GreaterThan("col", 10)) shouldBe false
    FilterSelectivityEstimator.canUseIndexLookup(StringStartsWith("col", "prefix")) shouldBe false
  }

  test("FilterSelectivityEstimator: partitionFilters separates indexable from non-indexable") {
    val filters = Array[Filter](
      EqualTo("region", "us-east"),
      GreaterThan("age", 20),
      In("status", Array("active", "pending"))
    )

    val (indexable, remaining) = FilterSelectivityEstimator.partitionFilters(filters)

    indexable.length shouldBe 2  // EqualTo and In
    remaining.length shouldBe 1  // GreaterThan
    remaining(0) shouldBe a[GreaterThan]
  }

  // ============= PartitionPruning Integration Tests =============

  test("PartitionPruning: basic equality pruning with index") {
    val addActions = Seq(
      createAddAction("file1.split", Map("region" -> "us-east")),
      createAddAction("file2.split", Map("region" -> "us-east")),
      createAddAction("file3.split", Map("region" -> "us-west")),
      createAddAction("file4.split", Map("region" -> "eu-central"))
    )

    val filters = Array[Filter](EqualTo("region", "us-east"))
    val result = PartitionPruning.prunePartitions(addActions, Seq("region"), filters)

    result.length shouldBe 2
    result.map(_.path) should contain allOf("file1.split", "file2.split")
  }

  test("PartitionPruning: IN filter pruning with index") {
    val addActions = Seq(
      createAddAction("file1.split", Map("region" -> "us-east")),
      createAddAction("file2.split", Map("region" -> "us-west")),
      createAddAction("file3.split", Map("region" -> "eu-central")),
      createAddAction("file4.split", Map("region" -> "ap-south"))
    )

    val filters = Array[Filter](In("region", Array("us-east", "us-west")))
    val result = PartitionPruning.prunePartitions(addActions, Seq("region"), filters)

    result.length shouldBe 2
    result.map(_.path) should contain allOf("file1.split", "file2.split")
  }

  test("PartitionPruning: compound filter with equality and range") {
    val addActions = Seq(
      createAddAction("file1.split", Map("region" -> "us-east", "year" -> "2023")),
      createAddAction("file2.split", Map("region" -> "us-east", "year" -> "2024")),
      createAddAction("file3.split", Map("region" -> "us-west", "year" -> "2024")),
      createAddAction("file4.split", Map("region" -> "eu-central", "year" -> "2024"))
    )

    // Use string comparison for year (consistent with how partition values are stored)
    val filters = Array[Filter](
      EqualTo("region", "us-east"),
      GreaterThanOrEqual("year", "2024")
    )
    val result = PartitionPruning.prunePartitions(addActions, Seq("region", "year"), filters)

    // file2.split has region=us-east and year=2024 (>= 2024)
    result.length shouldBe 1
    result.head.path shouldBe "file2.split"
  }

  test("PartitionPruning: compound filter with AND logic") {
    // Test with numeric year values to ensure proper comparison
    val addActions = Seq(
      createAddAction("file1.split", Map("region" -> "us-east", "month" -> "01")),
      createAddAction("file2.split", Map("region" -> "us-east", "month" -> "06")),
      createAddAction("file3.split", Map("region" -> "us-west", "month" -> "06")),
      createAddAction("file4.split", Map("region" -> "us-east", "month" -> "12"))
    )

    val filters = Array[Filter](
      EqualTo("region", "us-east"),
      GreaterThan("month", "05")
    )
    val result = PartitionPruning.prunePartitions(addActions, Seq("region", "month"), filters)

    // Should match file2 (us-east, 06) and file4 (us-east, 12)
    result.length shouldBe 2
    result.map(_.path).sorted shouldBe Seq("file2.split", "file4.split")
  }

  test("PartitionPruning: no matching partitions returns empty") {
    val addActions = Seq(
      createAddAction("file1.split", Map("region" -> "us-east")),
      createAddAction("file2.split", Map("region" -> "us-west"))
    )

    val filters = Array[Filter](EqualTo("region", "eu-central"))
    val result = PartitionPruning.prunePartitions(addActions, Seq("region"), filters)

    result shouldBe empty
  }

  test("PartitionPruning: no partition filters returns all files") {
    val addActions = Seq(
      createAddAction("file1.split", Map("region" -> "us-east")),
      createAddAction("file2.split", Map("region" -> "us-west"))
    )

    val filters = Array[Filter](EqualTo("non_partition_column", "value"))
    val result = PartitionPruning.prunePartitions(addActions, Seq("region"), filters)

    result.length shouldBe 2
  }

  test("PartitionPruning: empty filters returns all files") {
    val addActions = Seq(
      createAddAction("file1.split", Map("region" -> "us-east")),
      createAddAction("file2.split", Map("region" -> "us-west"))
    )

    val result = PartitionPruning.prunePartitions(addActions, Seq("region"), Array.empty)

    result.length shouldBe 2
  }

  test("PartitionPruning: optimizations can be disabled") {
    val addActions = Seq(
      createAddAction("file1.split", Map("region" -> "us-east")),
      createAddAction("file2.split", Map("region" -> "us-west"))
    )

    val filters = Array[Filter](EqualTo("region", "us-east"))

    // Test with all optimizations disabled
    val result = PartitionPruning.prunePartitionsOptimized(
      addActions,
      Seq("region"),
      filters,
      filterCacheEnabled = false,
      indexEnabled = false,
      parallelThreshold = 1000,
      selectivityOrdering = false
    )

    result.length shouldBe 1
    result.head.path shouldBe "file1.split"
  }

  test("PartitionPruning: filter cache improves hit rate on repeated queries") {
    // Use range filters to test cache (equality filters use index lookup and bypass cache)
    val addActions = (1 to 100).map { i =>
      val year = s"${2020 + (i % 5)}"
      createAddAction(s"file$i.split", Map("year" -> year))
    }

    // Use range filter to ensure cache is used
    val filters = Array[Filter](GreaterThan("year", "2022"))

    // First query - uses cache for partition evaluation
    PartitionPruning.prunePartitionsOptimized(
      addActions,
      Seq("year"),
      filters,
      filterCacheEnabled = true,
      indexEnabled = false,  // Disable index to test cache
      parallelThreshold = 1000,
      selectivityOrdering = true
    )
    val stats1 = PartitionPruning.getOptimizationStats()

    // Second query (should benefit from cache)
    PartitionPruning.prunePartitionsOptimized(
      addActions,
      Seq("year"),
      filters,
      filterCacheEnabled = true,
      indexEnabled = false,  // Disable index to test cache
      parallelThreshold = 1000,
      selectivityOrdering = true
    )
    val stats2 = PartitionPruning.getOptimizationStats()

    // Cache hits should increase
    stats2("filterCacheHits").asInstanceOf[Long] should be > stats1("filterCacheHits").asInstanceOf[Long]
  }

  // ============= Lazy Evaluation Tests =============

  test("PartitionPruning: lazy AND evaluation short-circuits") {
    // Create a filter where the first condition is always false
    val filters = Array[Filter](
      EqualTo("region", "nonexistent"),  // This is false
      EqualTo("year", "2024")            // This should not be evaluated
    )

    val addActions = Seq(
      createAddAction("file1.split", Map("region" -> "us-east", "year" -> "2024"))
    )

    // Should return empty (first filter fails)
    val result = PartitionPruning.prunePartitions(addActions, Seq("region", "year"), filters)
    result shouldBe empty
  }

  // ============= Large Scale Tests =============

  test("PartitionPruning: handles large number of files efficiently") {
    // Create 1000 files across 10 partitions
    val addActions = (1 to 1000).map { i =>
      val region = s"region-${i % 10}"
      createAddAction(s"file$i.split", Map("region" -> region))
    }

    val filters = Array[Filter](EqualTo("region", "region-0"))

    val startTime = System.currentTimeMillis()
    val result = PartitionPruning.prunePartitions(addActions, Seq("region"), filters)
    val duration = System.currentTimeMillis() - startTime

    result.length shouldBe 100  // 1000 files / 10 partitions = 100 per partition
    duration should be < 1000L  // Should complete in under 1 second
  }

  test("PartitionPruning: handles many unique partitions") {
    // Create 100 files across 100 unique partitions
    val addActions = (1 to 100).map { i =>
      createAddAction(s"file$i.split", Map("region" -> s"region-$i"))
    }

    val filters = Array[Filter](In("region", (1 to 10).map(i => s"region-$i").toArray))

    val result = PartitionPruning.prunePartitions(addActions, Seq("region"), filters)

    result.length shouldBe 10
  }
}
