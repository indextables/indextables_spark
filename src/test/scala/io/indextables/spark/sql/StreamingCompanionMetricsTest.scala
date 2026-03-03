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

package io.indextables.spark.sql

import io.indextables.spark.TestBase

/**
 * Unit tests for StreamingCompanionMetrics accumulator registration and update behavior.
 */
class StreamingCompanionMetricsTest extends TestBase {

  test("all accumulators start at zero") {
    val metrics = new StreamingCompanionMetrics(spark.sparkContext)
    metrics.syncCycles.value shouldBe 0L
    metrics.totalFilesIndexed.value shouldBe 0L
    metrics.totalDurationMs.value shouldBe 0L
    metrics.errorCount.value shouldBe 0L
  }

  test("recordCycleSuccess increments syncCycles by 1") {
    val metrics = new StreamingCompanionMetrics(spark.sparkContext)
    metrics.recordCycleSuccess(filesIndexed = 5, durationMs = 100)
    metrics.syncCycles.value shouldBe 1L
  }

  test("recordCycleSuccess adds filesIndexed to totalFilesIndexed") {
    val metrics = new StreamingCompanionMetrics(spark.sparkContext)
    metrics.recordCycleSuccess(filesIndexed = 12, durationMs = 200)
    metrics.totalFilesIndexed.value shouldBe 12L
  }

  test("recordCycleSuccess adds durationMs to totalDurationMs") {
    val metrics = new StreamingCompanionMetrics(spark.sparkContext)
    metrics.recordCycleSuccess(filesIndexed = 0, durationMs = 350)
    metrics.totalDurationMs.value shouldBe 350L
  }

  test("recordCycleError increments errorCount by 1") {
    val metrics = new StreamingCompanionMetrics(spark.sparkContext)
    metrics.recordCycleError()
    metrics.errorCount.value shouldBe 1L
  }

  test("multiple cycle recordings accumulate correctly") {
    val metrics = new StreamingCompanionMetrics(spark.sparkContext)
    metrics.recordCycleSuccess(filesIndexed = 10, durationMs = 100)
    metrics.recordCycleSuccess(filesIndexed = 5, durationMs = 200)
    metrics.recordCycleSuccess(filesIndexed = 0, durationMs = 50)
    metrics.syncCycles.value shouldBe 3L
    metrics.totalFilesIndexed.value shouldBe 15L
    metrics.totalDurationMs.value shouldBe 350L
    metrics.errorCount.value shouldBe 0L
  }

  test("errors do not affect success accumulators") {
    val metrics = new StreamingCompanionMetrics(spark.sparkContext)
    metrics.recordCycleSuccess(filesIndexed = 3, durationMs = 100)
    metrics.recordCycleError()
    metrics.recordCycleError()
    metrics.syncCycles.value shouldBe 1L
    metrics.totalFilesIndexed.value shouldBe 3L
    metrics.errorCount.value shouldBe 2L
  }

  test("accumulators are registered with expected name prefixes") {
    val metrics = new StreamingCompanionMetrics(spark.sparkContext)
    // Verify accumulator objects have the correct names
    metrics.syncCycles.name.get should include("syncCycles")
    metrics.totalFilesIndexed.name.get should include("totalFilesIndexed")
    metrics.totalDurationMs.name.get should include("totalDurationMs")
    metrics.errorCount.name.get should include("errorCount")
  }
}
