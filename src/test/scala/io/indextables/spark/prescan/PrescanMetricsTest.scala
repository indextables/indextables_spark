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

package io.indextables.spark.prescan

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

/** Unit tests for PrescanMetricsData, PrescanMetricsAccumulator, and PrescanMetricsRegistry. */
class PrescanMetricsTest extends AnyFunSuite with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    PrescanMetricsRegistry.clear()
  }

  // ======================
  // PrescanMetricsData tests
  // ======================

  test("PrescanMetricsData.empty should have all zero values") {
    val empty = PrescanMetricsData.empty

    assert(empty.splitsBeforePrescan == 0)
    assert(empty.splitsAfterPrescan == 0)
    assert(empty.prescanTimeMs == 0)
    assert(empty.cacheHits == 0)
    assert(empty.cacheMisses == 0)
    assert(empty.errors == 0)
  }

  test("merge should combine two metrics correctly") {
    val m1 = PrescanMetricsData(
      splitsBeforePrescan = 100,
      splitsAfterPrescan = 50,
      prescanTimeMs = 500,
      cacheHits = 80,
      cacheMisses = 20,
      errors = 2
    )
    val m2 = PrescanMetricsData(
      splitsBeforePrescan = 200,
      splitsAfterPrescan = 100,
      prescanTimeMs = 300,
      cacheHits = 150,
      cacheMisses = 50,
      errors = 1
    )

    val merged = m1.merge(m2)

    assert(merged.splitsBeforePrescan == 300)
    assert(merged.splitsAfterPrescan == 150)
    assert(merged.prescanTimeMs == 800)
    assert(merged.cacheHits == 230)
    assert(merged.cacheMisses == 70)
    assert(merged.errors == 3)
  }

  test("eliminationRate should calculate correctly") {
    val metrics = PrescanMetricsData(
      splitsBeforePrescan = 100,
      splitsAfterPrescan = 25,
      prescanTimeMs = 100,
      cacheHits = 0,
      cacheMisses = 0,
      errors = 0
    )

    assert(metrics.eliminationRate == 0.75)
  }

  test("eliminationRate should be 0 when no splits") {
    val metrics = PrescanMetricsData.empty

    assert(metrics.eliminationRate == 0.0)
  }

  test("eliminationRate should be 1.0 when all splits eliminated") {
    val metrics = PrescanMetricsData(
      splitsBeforePrescan = 100,
      splitsAfterPrescan = 0,
      prescanTimeMs = 100,
      cacheHits = 0,
      cacheMisses = 0,
      errors = 0
    )

    assert(metrics.eliminationRate == 1.0)
  }

  test("cacheHitRate should calculate correctly") {
    val metrics = PrescanMetricsData(
      splitsBeforePrescan = 100,
      splitsAfterPrescan = 50,
      prescanTimeMs = 100,
      cacheHits = 80,
      cacheMisses = 20,
      errors = 0
    )

    assert(metrics.cacheHitRate == 0.8)
  }

  test("cacheHitRate should be 0 when no cache operations") {
    val metrics = PrescanMetricsData.empty

    assert(metrics.cacheHitRate == 0.0)
  }

  test("splitsEliminated should calculate correctly") {
    val metrics = PrescanMetricsData(
      splitsBeforePrescan = 100,
      splitsAfterPrescan = 25,
      prescanTimeMs = 100,
      cacheHits = 0,
      cacheMisses = 0,
      errors = 0
    )

    assert(metrics.splitsEliminated == 75)
  }

  test("summary should include key information") {
    val metrics = PrescanMetricsData(
      splitsBeforePrescan = 100,
      splitsAfterPrescan = 25,
      prescanTimeMs = 150,
      cacheHits = 80,
      cacheMisses = 20,
      errors = 2
    )

    val summary = metrics.summary

    assert(summary.contains("75"))   // splits eliminated
    assert(summary.contains("100"))  // total splits
    assert(summary.contains("150"))  // time ms
    assert(summary.contains("80%"))  // cache hit rate
    assert(summary.contains("2"))    // errors
  }

  // ======================
  // PrescanMetricsAccumulator tests
  // ======================

  test("accumulator should start as zero") {
    val acc = new PrescanMetricsAccumulator

    assert(acc.isZero)
    assert(acc.value == PrescanMetricsData.empty)
  }

  test("accumulator should add values correctly") {
    val acc = new PrescanMetricsAccumulator

    acc.add(PrescanMetricsData(
      splitsBeforePrescan = 100,
      splitsAfterPrescan = 50,
      prescanTimeMs = 100,
      cacheHits = 80,
      cacheMisses = 20,
      errors = 1
    ))

    assert(!acc.isZero)
    assert(acc.value.splitsBeforePrescan == 100)
    assert(acc.value.splitsAfterPrescan == 50)
  }

  test("accumulator should merge correctly") {
    val acc1 = new PrescanMetricsAccumulator
    acc1.add(PrescanMetricsData(
      splitsBeforePrescan = 100,
      splitsAfterPrescan = 50,
      prescanTimeMs = 100,
      cacheHits = 0,
      cacheMisses = 0,
      errors = 0
    ))

    val acc2 = new PrescanMetricsAccumulator
    acc2.add(PrescanMetricsData(
      splitsBeforePrescan = 200,
      splitsAfterPrescan = 100,
      prescanTimeMs = 200,
      cacheHits = 0,
      cacheMisses = 0,
      errors = 0
    ))

    acc1.merge(acc2)

    assert(acc1.value.splitsBeforePrescan == 300)
    assert(acc1.value.splitsAfterPrescan == 150)
    assert(acc1.value.prescanTimeMs == 300)
  }

  test("accumulator copy should be independent") {
    val acc = new PrescanMetricsAccumulator
    acc.add(PrescanMetricsData(
      splitsBeforePrescan = 100,
      splitsAfterPrescan = 50,
      prescanTimeMs = 100,
      cacheHits = 0,
      cacheMisses = 0,
      errors = 0
    ))

    val copy = acc.copy()

    acc.add(PrescanMetricsData(
      splitsBeforePrescan = 100,
      splitsAfterPrescan = 50,
      prescanTimeMs = 100,
      cacheHits = 0,
      cacheMisses = 0,
      errors = 0
    ))

    assert(acc.value.splitsBeforePrescan == 200)
    assert(copy.value.splitsBeforePrescan == 100)
  }

  test("accumulator reset should clear values") {
    val acc = new PrescanMetricsAccumulator
    acc.add(PrescanMetricsData(
      splitsBeforePrescan = 100,
      splitsAfterPrescan = 50,
      prescanTimeMs = 100,
      cacheHits = 0,
      cacheMisses = 0,
      errors = 0
    ))

    acc.reset()

    assert(acc.isZero)
    assert(acc.value == PrescanMetricsData.empty)
  }

  test("accumulator snapshot should return copy of current value") {
    val acc = new PrescanMetricsAccumulator
    acc.add(PrescanMetricsData(
      splitsBeforePrescan = 100,
      splitsAfterPrescan = 50,
      prescanTimeMs = 100,
      cacheHits = 0,
      cacheMisses = 0,
      errors = 0
    ))

    val snapshot = acc.snapshot

    assert(snapshot.splitsBeforePrescan == 100)
    assert(snapshot == acc.value)
  }

  // ======================
  // PrescanMetricsRegistry tests
  // ======================

  test("registry should return None for unregistered table") {
    assert(PrescanMetricsRegistry.get("s3://bucket/table").isEmpty)
    assert(PrescanMetricsRegistry.getMetrics("s3://bucket/table").isEmpty)
  }

  test("registry should register and retrieve accumulator") {
    val acc = new PrescanMetricsAccumulator
    PrescanMetricsRegistry.register("s3://bucket/table", acc)

    assert(PrescanMetricsRegistry.get("s3://bucket/table").isDefined)
    assert(PrescanMetricsRegistry.get("s3://bucket/table").get eq acc)
  }

  test("getOrCreate should create accumulator if not exists") {
    val acc1 = PrescanMetricsRegistry.getOrCreate("s3://bucket/table")
    val acc2 = PrescanMetricsRegistry.getOrCreate("s3://bucket/table")

    assert(acc1 eq acc2)
  }

  test("getMetrics should return metrics from accumulator") {
    val acc = PrescanMetricsRegistry.getOrCreate("s3://bucket/table")
    acc.add(PrescanMetricsData(
      splitsBeforePrescan = 100,
      splitsAfterPrescan = 50,
      prescanTimeMs = 100,
      cacheHits = 0,
      cacheMisses = 0,
      errors = 0
    ))

    val metrics = PrescanMetricsRegistry.getMetrics("s3://bucket/table")

    assert(metrics.isDefined)
    assert(metrics.get.splitsBeforePrescan == 100)
  }

  test("remove should remove accumulator") {
    PrescanMetricsRegistry.getOrCreate("s3://bucket/table")
    assert(PrescanMetricsRegistry.get("s3://bucket/table").isDefined)

    PrescanMetricsRegistry.remove("s3://bucket/table")
    assert(PrescanMetricsRegistry.get("s3://bucket/table").isEmpty)
  }

  test("clear should remove all accumulators") {
    PrescanMetricsRegistry.getOrCreate("s3://bucket/table1")
    PrescanMetricsRegistry.getOrCreate("s3://bucket/table2")

    assert(PrescanMetricsRegistry.registeredTables.size == 2)

    PrescanMetricsRegistry.clear()

    assert(PrescanMetricsRegistry.registeredTables.isEmpty)
  }

  test("registeredTables should return all registered paths") {
    PrescanMetricsRegistry.getOrCreate("s3://bucket/table1")
    PrescanMetricsRegistry.getOrCreate("s3://bucket/table2")
    PrescanMetricsRegistry.getOrCreate("/local/table3")

    val tables = PrescanMetricsRegistry.registeredTables

    assert(tables.size == 3)
    assert(tables.contains("s3://bucket/table1"))
    assert(tables.contains("s3://bucket/table2"))
    assert(tables.contains("/local/table3"))
  }

  test("allMetricsSummary should return summaries for all tables") {
    val acc1 = PrescanMetricsRegistry.getOrCreate("s3://bucket/table1")
    acc1.add(PrescanMetricsData(
      splitsBeforePrescan = 100,
      splitsAfterPrescan = 50,
      prescanTimeMs = 100,
      cacheHits = 80,
      cacheMisses = 20,
      errors = 0
    ))

    val acc2 = PrescanMetricsRegistry.getOrCreate("s3://bucket/table2")
    acc2.add(PrescanMetricsData(
      splitsBeforePrescan = 200,
      splitsAfterPrescan = 50,
      prescanTimeMs = 200,
      cacheHits = 100,
      cacheMisses = 100,
      errors = 1
    ))

    val summaries = PrescanMetricsRegistry.allMetricsSummary

    assert(summaries.size == 2)
    assert(summaries("s3://bucket/table1").contains("50"))
    assert(summaries("s3://bucket/table2").contains("150"))
  }
}
