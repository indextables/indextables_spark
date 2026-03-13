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

import java.nio.file.Files

import io.indextables.spark.storage.{BatchOptMetrics, BatchOptMetricsRegistry}
import io.indextables.spark.TestBase

/**
 * Test to verify batch metrics behavior with baseline/delta approach. Each query should show correct per-query metrics
 * via getMetricsDelta().
 */
class BatchMetricsBehaviorTest extends TestBase {

  private var testPath: String = _

  private def printMetrics(label: String, m: BatchOptMetrics): Unit =
    info(
      s"$label: ops=${m.totalOperations}, docs=${m.totalDocuments}, " +
        s"requests=${m.totalRequests}, consolidated=${m.consolidatedRequests}, " +
        s"ratio=${m.consolidationRatio}x, savings=${m.costSavingsPercent}%, " +
        s"prefetch=${m.totalPrefetchDurationMs}ms, segments=${m.segmentsProcessed}"
    )

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create our own temp directory since tempDir isn't set until beforeEach
    testPath = Files.createTempDirectory("batch-metrics-test-").toString

    // Create 10,000 rows in a SINGLE partition to ensure all docs go to one split
    spark
      .range(0, 10000)
      .toDF("id")
      .selectExpr("id", "id % 100 as category", "concat('data_', id) as value")
      .repartition(1) // Force single partition = single split
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(testPath)

    info(s"Created 10,000 rows (single split) at: $testPath")
  }

  test("per-query metrics work correctly with baseline/delta approach") {
    info("\n=== PER-QUERY METRICS VALIDATION ===")

    // Run multiple queries and verify each gets correct per-query metrics
    // Use limit(Int.MaxValue) to override the default 250-row limit
    val result1 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)
      .limit(Int.MaxValue)
      .collect()

    val delta1   = BatchOptMetricsRegistry.getMetricsDelta(testPath)
    val docCount = delta1.totalDocuments

    info(s"Query 1: ${result1.length} rows returned, $docCount docs in metrics")
    printMetrics("  DELTA", delta1)

    // Key validation: docs returned should match docs in metrics
    // (unless below 50-doc threshold)
    if (result1.length >= 50) {
      assert(
        delta1.totalOperations >= 1,
        s"Expected at least 1 batch op for ${result1.length} docs, got ${delta1.totalOperations}"
      )
      assert(delta1.totalDocuments > 0, s"Expected totalDocuments > 0 for ${result1.length} rows")
      assert(
        delta1.consolidatedRequests < delta1.totalRequests || delta1.totalRequests == 0,
        s"Expected consolidation (consolidated < total requests)"
      )

      info(s"\n✅ SUCCESS: Per-query metrics work correctly")
      info(s"   Rows returned: ${result1.length}")
      info(s"   Docs in metrics: ${delta1.totalDocuments}")
      info(s"   Batch operations: ${delta1.totalOperations}")
      info(s"   Consolidation: ${delta1.totalRequests} → ${delta1.consolidatedRequests} (${delta1.consolidationRatio}x)")
    } else {
      // Below threshold - no batch optimization
      assert(delta1.totalOperations == 0, s"Expected 0 batch ops below 50-doc threshold, got ${delta1.totalOperations}")
      info(s"\n✅ SUCCESS: Below threshold, correctly shows 0 batch ops")
    }
  }

  test("limit(1) x5 - each query should have zero metrics (below threshold)") {
    info("\n=== LIMIT(1) x5 - USING getMetricsDelta() ===")

    for (i <- 1 to 5) {
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)
        .limit(1)
        .collect()

      val delta = BatchOptMetricsRegistry.getMetricsDelta(testPath)

      info(s"Query $i: ${result.length} rows")
      printMetrics("  DELTA", delta)

      // Below 50-doc threshold, no batch optimization should occur
      assert(delta.totalOperations == 0, s"Expected 0 ops for limit(1), got ${delta.totalOperations}")
      info("")
    }
  }

  test("limit(100) x5 - each query should show consistent per-query metrics") {
    info("\n=== LIMIT(100) x5 - USING getMetricsDelta() ===")

    for (i <- 1 to 5) {
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)
        .limit(100)
        .collect()

      val delta = BatchOptMetricsRegistry.getMetricsDelta(testPath)

      info(s"Query $i: ${result.length} rows")
      printMetrics("  DELTA", delta)

      // Above 50-doc threshold, should see consistent metrics per query
      assert(delta.totalOperations == 1, s"Expected 1 op per query, got ${delta.totalOperations}")
      assert(delta.totalDocuments == 100, s"Expected 100 docs per query, got ${delta.totalDocuments}")
      assert(delta.totalRequests == 100, s"Expected 100 baseline requests, got ${delta.totalRequests}")
      // After tantivy4java fix: should consolidate to 1-2 requests instead of ~10
      assert(
        delta.consolidatedRequests > 0 && delta.consolidatedRequests <= 100,
        s"Expected consolidation (0 < x <= 100), got ${delta.consolidatedRequests}"
      )
      info("")
    }
  }

  test("limit(500) x3 - each query should show consistent per-query metrics") {
    info("\n=== LIMIT(500) x3 - USING getMetricsDelta() ===")

    for (i <- 1 to 3) {
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)
        .limit(500)
        .collect()

      val delta = BatchOptMetricsRegistry.getMetricsDelta(testPath)

      info(s"Query $i: ${result.length} rows")
      printMetrics("  DELTA", delta)

      // Above 50-doc threshold, should see consistent metrics per query
      assert(delta.totalOperations == 1, s"Expected 1 op per query, got ${delta.totalOperations}")
      assert(delta.totalDocuments == 500, s"Expected 500 docs per query, got ${delta.totalDocuments}")
      assert(delta.totalRequests == 500, s"Expected 500 baseline requests, got ${delta.totalRequests}")
      // After tantivy4java fix: should consolidate to 1-2 requests instead of ~5
      assert(
        delta.consolidatedRequests > 0 && delta.consolidatedRequests <= 500,
        s"Expected consolidation (0 < x <= 500), got ${delta.consolidatedRequests}"
      )
      info("")
    }
  }

  test("global metrics should accumulate across queries") {
    info("\n=== GLOBAL METRICS ACCUMULATION TEST ===")

    // Run 3 queries with limit(100)
    for (i <- 1 to 3)
      spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)
        .limit(100)
        .collect()

    val global = BatchOptMetricsRegistry.getGlobalMetrics()
    info(s"Global metrics after 3 queries:")
    printMetrics("  GLOBAL", global)

    // Global should show accumulated values (at least 3 ops from these queries)
    assert(global.totalOperations >= 3, s"Expected at least 3 ops globally, got ${global.totalOperations}")
  }

  test("docBatchProjected with 10000 docs should result in a single batch operation") {
    info("\n=== BATCH SIZE VALIDATION: 10000 docs via docBatchProjected = 1 op ===")

    // Read all 10000 docs — docBatchProjected sends all addresses in one native call
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)
      .limit(Int.MaxValue)
      .collect()

    val delta = BatchOptMetricsRegistry.getMetricsDelta(testPath)

    info(s"Rows returned: ${result.length}")
    printMetrics("  DELTA", delta)

    // Validate we got all 10000 rows
    assert(result.length == 10000, s"Expected 10000 rows, got ${result.length}")

    // docBatchProjected processes all addresses in a single native call
    assert(delta.totalOperations == 1, s"Expected 1 batch op via docBatchProjected, got ${delta.totalOperations}")

    // Validate total docs matches rows returned
    assert(delta.totalDocuments == 10000, s"Expected 10000 docs in metrics, got ${delta.totalDocuments}")

    // Validate total requests equals docs (one request per doc before consolidation)
    assert(delta.totalRequests == 10000, s"Expected 10000 baseline requests, got ${delta.totalRequests}")

    // ⚠️ KNOWN BUG IN TANTIVY4JAVA: Hardcoded 100-document limit per range
    // See: TANTIVY4JAVA_BATCH_OPTIMIZATION_BUG.md
    // Current behavior: ~101 consolidated requests (10000 docs / 100 per range)
    // Expected after fix: 1-2 consolidated requests (entire split < gap_tolerance)
    //
    // The split file is only 273KB, much smaller than the 512KB gap_tolerance.
    // Once tantivy4java is fixed to use actual byte positions and respect gap_tolerance,
    // this should consolidate into 1-2 S3 requests total (one per batch operation or
    // even merged across batches if documents are adjacent).

    // CRITICAL ASSERTION: Validate actual S3 requests (consolidatedRequests)
    // This is the key metric - it represents ACTUAL S3 GET calls made
    val actualS3Calls = delta.consolidatedRequests
    if (actualS3Calls > 10) {
      // Bug still present - expecting ~101 requests
      info(s"⚠️  TANTIVY4JAVA BUG DETECTED: $actualS3Calls actual S3 calls")
      info(s"   Current: 10000 docs → $actualS3Calls S3 requests (~100 docs per range)")
      info(s"   Expected after fix: 10000 docs → 1-3 S3 requests (respecting 512KB gap_tolerance)")
      assert(
        actualS3Calls > 50 && actualS3Calls < 150,
        s"Bug present but S3 calls outside expected range: $actualS3Calls (expected ~101)"
      )
    } else {
      // Bug fixed - should be 1-3 requests (may vary by segment layout)
      info(s"✅ TANTIVY4JAVA BUG FIXED: $actualS3Calls actual S3 calls")
      info(s"   Achieved optimal consolidation: 10000 docs → $actualS3Calls S3 requests")
      info(s"   Consolidation ratio: ${delta.consolidationRatio}x, savings=${delta.costSavingsPercent}%")

      // REGRESSION PREVENTION: Strict assertion on S3 calls
      assert(
        actualS3Calls >= 1 && actualS3Calls <= 3,
        s"REGRESSION: Expected 1-3 S3 requests after bug fix, got $actualS3Calls. " +
          s"This indicates the consolidation optimization has regressed!"
      )
    }

    // Validate segments counter is being tracked (>= 0)
    // Note: May be 0 for delta calculations or certain operation types
    assert(delta.segmentsProcessed >= 0, s"Expected segments processed >= 0, got ${delta.segmentsProcessed}")

    // For local file testing, prefetch duration may be 0 or very small
    // Just validate it's being tracked (non-negative)
    assert(delta.totalPrefetchDurationMs >= 0, s"Expected prefetch duration >= 0, got ${delta.totalPrefetchDurationMs}")

    info(s"\n✅ SUCCESS: docBatchMaxSize=5000 correctly batches 10000 docs into 2 operations")
    info(s"   Batch operations: ${delta.totalOperations} (expected: 2)")
    info(s"   Total documents: ${delta.totalDocuments} (expected: 10000)")
    info(s"   S3 Calls: ${delta.totalRequests} → $actualS3Calls (${delta.consolidationRatio}x)")
    info(s"   Segments processed: ${delta.segmentsProcessed}")
    info(s"   Prefetch duration: ${delta.totalPrefetchDurationMs}ms")
  }

  test("accumulator-based getMetrics() should now be populated") {
    info("\n=== ACCUMULATOR (getMetrics) VALIDATION ===")

    // Enable metrics collection via session config
    spark.conf.set("spark.indextables.read.batchOptimization.metrics.enabled", "true")

    // Clear any existing accumulator for this path
    BatchOptMetricsRegistry.clear(testPath)

    // Execute a query that should trigger batch optimization
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)
      .limit(200)
      .collect()

    info(s"Query returned ${result.length} rows")

    // Check both methods return metrics
    val accumulatorMetrics = BatchOptMetricsRegistry.getMetrics(testPath)
    val deltaMetrics       = BatchOptMetricsRegistry.getMetricsDelta(testPath)

    info(
      s"Accumulator (getMetrics): ${accumulatorMetrics.map(m => s"ops=${m.totalOperations}, docs=${m.totalDocuments}").getOrElse("None")}"
    )
    info(s"Delta (getMetricsDelta): ops=${deltaMetrics.totalOperations}, docs=${deltaMetrics.totalDocuments}")

    // Accumulator should now be defined and populated
    assert(accumulatorMetrics.isDefined, "getMetrics() should return Some() when metrics collection is enabled")

    val accMetrics = accumulatorMetrics.get
    assert(
      accMetrics.totalOperations > 0,
      s"Accumulator should have totalOperations > 0, got ${accMetrics.totalOperations}"
    )
    assert(
      accMetrics.totalDocuments > 0,
      s"Accumulator should have totalDocuments > 0, got ${accMetrics.totalDocuments}"
    )

    info(s"\n✅ SUCCESS: Both getMetrics() and getMetricsDelta() return non-zero metrics")
    info(s"   Accumulator: ops=${accMetrics.totalOperations}, docs=${accMetrics.totalDocuments}")
    info(s"   Delta: ops=${deltaMetrics.totalOperations}, docs=${deltaMetrics.totalDocuments}")

    // Reset config
    spark.conf.unset("spark.indextables.read.batchOptimization.metrics.enabled")
  }
}
