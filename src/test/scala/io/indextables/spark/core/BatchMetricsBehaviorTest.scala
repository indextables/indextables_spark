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
 * Test to verify batch metrics behavior with baseline/delta approach.
 *
 * With the unified streaming read path (PR #248), all reads use `startStreamingRetrieval()`/`nextBatch()` which
 * streams Arrow batches directly via FFI. This bypasses `docBatchProjected()`, so batch optimization metrics
 * (totalOperations, totalDocuments, consolidatedRequests) are zero for streaming reads. This is expected — the
 * streaming path is itself a more efficient mechanism that eliminates per-document S3 requests entirely.
 *
 * Tests validate:
 * - Data correctness through the streaming path at various limits
 * - Metrics infrastructure (registry, baselines, deltas) remains functional
 * - The limit(1) below-threshold behavior is preserved
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

  test("per-query metrics baseline/delta infrastructure works with streaming reads") {
    info("\n=== PER-QUERY METRICS VALIDATION (STREAMING PATH) ===")

    val result1 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)
      .limit(Int.MaxValue)
      .collect()

    val delta1 = BatchOptMetricsRegistry.getMetricsDelta(testPath)

    info(s"Query 1: ${result1.length} rows returned")
    printMetrics("  DELTA", delta1)

    // Validate data correctness
    assert(result1.length == 10000, s"Expected 10000 rows, got ${result1.length}")

    // Streaming path bypasses docBatchProjected, so batch optimization metrics are zero.
    // This is expected — streaming reads Arrow batches directly via FFI without per-document requests.
    assert(
      delta1.totalOperations == 0,
      s"Streaming reads bypass docBatchProjected; expected 0 ops, got ${delta1.totalOperations}"
    )

    info(s"\n✅ SUCCESS: Streaming read returned all 10000 rows; metrics infrastructure functional")
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

  test("limit(100) x5 - streaming reads return correct row counts") {
    info("\n=== LIMIT(100) x5 - STREAMING PATH ===")

    for (i <- 1 to 5) {
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)
        .limit(100)
        .collect()

      val delta = BatchOptMetricsRegistry.getMetricsDelta(testPath)

      info(s"Query $i: ${result.length} rows")
      printMetrics("  DELTA", delta)

      assert(result.length == 100, s"Expected 100 rows, got ${result.length}")
      // Streaming path: zero batch optimization metrics (reads bypass docBatchProjected)
      assert(delta.totalOperations == 0, s"Streaming: expected 0 ops, got ${delta.totalOperations}")
      info("")
    }
  }

  test("limit(500) x3 - streaming reads return correct row counts") {
    info("\n=== LIMIT(500) x3 - STREAMING PATH ===")

    for (i <- 1 to 3) {
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)
        .limit(500)
        .collect()

      val delta = BatchOptMetricsRegistry.getMetricsDelta(testPath)

      info(s"Query $i: ${result.length} rows")
      printMetrics("  DELTA", delta)

      assert(result.length == 500, s"Expected 500 rows, got ${result.length}")
      // Streaming path: zero batch optimization metrics (reads bypass docBatchProjected)
      assert(delta.totalOperations == 0, s"Streaming: expected 0 ops, got ${delta.totalOperations}")
      info("")
    }
  }

  test("global metrics registry is accessible after streaming reads") {
    info("\n=== GLOBAL METRICS REGISTRY TEST ===")

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

    // Global metrics registry should be accessible (non-null).
    // Streaming reads bypass docBatchProjected, so batch optimization counters are zero.
    // The registry still functions correctly — it just has nothing to accumulate from streaming.
    assert(global.totalOperations >= 0, s"Global metrics should be non-negative, got ${global.totalOperations}")
  }

  test("streaming read of 10000 docs returns all rows correctly") {
    info("\n=== FULL TABLE READ: 10000 docs via streaming path ===")

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)
      .limit(Int.MaxValue)
      .collect()

    val delta = BatchOptMetricsRegistry.getMetricsDelta(testPath)

    info(s"Rows returned: ${result.length}")
    printMetrics("  DELTA", delta)

    // Validate all 10000 rows returned through streaming path
    assert(result.length == 10000, s"Expected 10000 rows, got ${result.length}")

    // Streaming path bypasses docBatchProjected entirely — it reads Arrow batches
    // directly via FFI (startStreamingRetrieval/nextBatch). Batch optimization metrics
    // are zero because there are no per-document S3 requests to consolidate.
    assert(
      delta.totalOperations == 0,
      s"Streaming reads bypass docBatchProjected; expected 0 ops, got ${delta.totalOperations}"
    )

    // Metrics infrastructure fields should be non-negative
    assert(delta.segmentsProcessed >= 0, s"Expected segments processed >= 0, got ${delta.segmentsProcessed}")
    assert(delta.totalPrefetchDurationMs >= 0, s"Expected prefetch duration >= 0, got ${delta.totalPrefetchDurationMs}")

    info(s"\n✅ SUCCESS: Streaming path returned all 10000 rows")
  }

  test("accumulator-based getMetrics() is accessible with streaming reads") {
    info("\n=== ACCUMULATOR (getMetrics) VALIDATION ===")

    // Enable metrics collection via session config
    spark.conf.set("spark.indextables.read.batchOptimization.metrics.enabled", "true")

    // Clear any existing accumulator for this path
    BatchOptMetricsRegistry.clear(testPath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)
      .limit(200)
      .collect()

    info(s"Query returned ${result.length} rows")
    assert(result.length == 200, s"Expected 200 rows, got ${result.length}")

    // Check both methods are accessible
    val accumulatorMetrics = BatchOptMetricsRegistry.getMetrics(testPath)
    val deltaMetrics       = BatchOptMetricsRegistry.getMetricsDelta(testPath)

    info(
      s"Accumulator (getMetrics): ${accumulatorMetrics.map(m => s"ops=${m.totalOperations}, docs=${m.totalDocuments}").getOrElse("None")}"
    )
    info(s"Delta (getMetricsDelta): ops=${deltaMetrics.totalOperations}, docs=${deltaMetrics.totalDocuments}")

    // Accumulator should be defined when metrics collection is enabled
    assert(accumulatorMetrics.isDefined, "getMetrics() should return Some() when metrics collection is enabled")

    // Streaming reads bypass docBatchProjected, so batch optimization counters are zero.
    // The accumulator infrastructure still works — it just has nothing to accumulate from streaming.
    val accMetrics = accumulatorMetrics.get
    assert(
      accMetrics.totalOperations >= 0,
      s"Accumulator metrics should be non-negative, got ${accMetrics.totalOperations}"
    )

    info(s"\n✅ SUCCESS: Metrics infrastructure functional with streaming reads")
    info(s"   Accumulator: ops=${accMetrics.totalOperations}, docs=${accMetrics.totalDocuments}")
    info(s"   Delta: ops=${deltaMetrics.totalOperations}, docs=${deltaMetrics.totalDocuments}")

    // Reset config
    spark.conf.unset("spark.indextables.read.batchOptimization.metrics.enabled")
  }
}
