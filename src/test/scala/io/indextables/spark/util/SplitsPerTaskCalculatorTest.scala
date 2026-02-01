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

package io.indextables.spark.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for SplitsPerTaskCalculator auto-selection algorithm.
 *
 * Algorithm summary:
 *   - If totalSplits <= defaultParallelism × 2: use 1 split per task (maximize parallelism)
 *   - Otherwise: ensure at least 4 × defaultParallelism groups, capped at maxSplitsPerTask
 */
class SplitsPerTaskCalculatorTest extends AnyFunSuite with Matchers {

  // ============================================================================
  // AUTO-SELECTION: SMALL TABLE TESTS
  // ============================================================================

  test("small table: 1 split with 4 cores returns 1 split per task") {
    // 1 <= 4*2=8, so maximize parallelism
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 1,
      defaultParallelism = 4,
      configuredValue = None
    )
    result shouldBe 1
  }

  test("small table: splits at threshold returns 1 split per task") {
    // 8 <= 4*2=8, so maximize parallelism (boundary case)
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 8,
      defaultParallelism = 4,
      configuredValue = None
    )
    result shouldBe 1
  }

  test("small table: splits just below threshold returns 1 split per task") {
    // 7 <= 4*2=8, so maximize parallelism
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 7,
      defaultParallelism = 4,
      configuredValue = None
    )
    result shouldBe 1
  }

  test("small table: large cluster with few splits returns 1 split per task") {
    // 100 <= 64*2=128, so maximize parallelism
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 100,
      defaultParallelism = 64,
      configuredValue = None
    )
    result shouldBe 1
  }

  test("small table: splits exactly at threshold with large cluster returns 1") {
    // 128 <= 64*2=128, boundary case
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 128,
      defaultParallelism = 64,
      configuredValue = None
    )
    result shouldBe 1
  }

  // ============================================================================
  // AUTO-SELECTION: LARGE TABLE TESTS
  // ============================================================================

  test("large table: splits just above threshold triggers batching") {
    // 9 > 4*2=8, target groups = 4*4=16
    // splitsPerTask = ceil(9/16) = 1
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 9,
      defaultParallelism = 4,
      configuredValue = None
    )
    result shouldBe 1
  }

  test("large table: 32 splits with 4 cores returns 2 splits per task") {
    // 32 > 4*2=8, target groups = 4*4=16
    // splitsPerTask = ceil(32/16) = 2
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 32,
      defaultParallelism = 4,
      configuredValue = None
    )
    result shouldBe 2
  }

  test("large table: 64 splits with 4 cores returns 4 splits per task") {
    // 64 > 4*2=8, target groups = 4*4=16
    // splitsPerTask = ceil(64/16) = 4
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 64,
      defaultParallelism = 4,
      configuredValue = None
    )
    result shouldBe 4
  }

  test("large table: 128 splits with 4 cores returns 8 (capped at default max)") {
    // 128 > 4*2=8, target groups = 4*4=16
    // splitsPerTask = ceil(128/16) = 8, capped at default max 8
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 128,
      defaultParallelism = 4,
      configuredValue = None
    )
    result shouldBe 8
  }

  test("large table: 256 splits with 4 cores returns 8 (capped)") {
    // 256 > 4*2=8, target groups = 4*4=16
    // splitsPerTask = ceil(256/16) = 16, capped at default max 8
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 256,
      defaultParallelism = 4,
      configuredValue = None
    )
    result shouldBe 8
  }

  test("large table: 512 splits with 64 cores returns 2 splits per task") {
    // 512 > 64*2=128, target groups = 64*4=256
    // splitsPerTask = ceil(512/256) = 2
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 512,
      defaultParallelism = 64,
      configuredValue = None
    )
    result shouldBe 2
  }

  test("large table: 1024 splits with 64 cores returns 4 splits per task") {
    // 1024 > 64*2=128, target groups = 64*4=256
    // splitsPerTask = ceil(1024/256) = 4
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 1024,
      defaultParallelism = 64,
      configuredValue = None
    )
    result shouldBe 4
  }

  test("large table: 2048 splits with 64 cores returns 8 (capped)") {
    // 2048 > 64*2=128, target groups = 64*4=256
    // splitsPerTask = ceil(2048/256) = 8, at default max
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 2048,
      defaultParallelism = 64,
      configuredValue = None
    )
    result shouldBe 8
  }

  test("large table: 10000 splits with 64 cores returns 8 (capped)") {
    // 10000 > 64*2=128, target groups = 64*4=256
    // splitsPerTask = ceil(10000/256) = 40, capped at default max 8
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 10000,
      defaultParallelism = 64,
      configuredValue = None
    )
    result shouldBe 8
  }

  // ============================================================================
  // CUSTOM MAX SPLITS PER TASK
  // ============================================================================

  test("custom max: higher cap allows more splits per task") {
    // 256 > 4*2=8, target groups = 4*4=16
    // splitsPerTask = ceil(256/16) = 16, with custom cap 16
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 256,
      defaultParallelism = 4,
      configuredValue = None,
      maxSplitsPerTask = 16
    )
    result shouldBe 16
  }

  test("custom max: lower cap reduces splits per task") {
    // 64 > 4*2=8, target groups = 4*4=16
    // splitsPerTask = ceil(64/16) = 4, capped at custom max 2
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 64,
      defaultParallelism = 4,
      configuredValue = None,
      maxSplitsPerTask = 2
    )
    result shouldBe 2
  }

  test("custom max: cap of 1 forces single-split tasks") {
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 1000,
      defaultParallelism = 64,
      configuredValue = None,
      maxSplitsPerTask = 1
    )
    result shouldBe 1
  }

  // ============================================================================
  // EXPLICIT NUMERIC CONFIGURATION
  // ============================================================================

  test("explicit config: numeric value overrides auto-selection") {
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 100,
      defaultParallelism = 64,
      configuredValue = Some("4")
    )
    result shouldBe 4
  }

  test("explicit config: value 1 returns 1") {
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 1000,
      defaultParallelism = 64,
      configuredValue = Some("1")
    )
    result shouldBe 1
  }

  test("explicit config: large value is used as-is") {
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 100,
      defaultParallelism = 4,
      configuredValue = Some("100")
    )
    result shouldBe 100
  }

  test("explicit config: zero is treated as 1 (minimum)") {
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 100,
      defaultParallelism = 4,
      configuredValue = Some("0")
    )
    result shouldBe 1
  }

  test("explicit config: negative is treated as 1 (minimum)") {
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 100,
      defaultParallelism = 4,
      configuredValue = Some("-5")
    )
    result shouldBe 1
  }

  // ============================================================================
  // AUTO KEYWORD AND INVALID VALUES
  // ============================================================================

  test("auto keyword: 'auto' triggers auto-selection") {
    // "auto" is not a valid integer, so auto-selection is used
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 100,
      defaultParallelism = 64,
      configuredValue = Some("auto")
    )
    // 100 <= 64*2=128, so maximize parallelism
    result shouldBe 1
  }

  test("auto keyword: empty string triggers auto-selection") {
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 100,
      defaultParallelism = 64,
      configuredValue = Some("")
    )
    result shouldBe 1
  }

  test("auto keyword: invalid string triggers auto-selection") {
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 100,
      defaultParallelism = 64,
      configuredValue = Some("invalid")
    )
    result shouldBe 1
  }

  test("auto keyword: None triggers auto-selection") {
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 100,
      defaultParallelism = 64,
      configuredValue = None
    )
    result shouldBe 1
  }

  // ============================================================================
  // EDGE CASES
  // ============================================================================

  test("edge case: zero splits returns 1") {
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 0,
      defaultParallelism = 4,
      configuredValue = None
    )
    result shouldBe 1
  }

  test("edge case: single split returns 1") {
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 1,
      defaultParallelism = 64,
      configuredValue = None
    )
    result shouldBe 1
  }

  test("edge case: zero parallelism is guarded to 1") {
    // With parallelism=1, threshold=2, target groups=4
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 100,
      defaultParallelism = 0,
      configuredValue = None
    )
    // 100 > 1*2=2, target groups = 1*4=4
    // splitsPerTask = ceil(100/4) = 25, capped at 8
    result shouldBe 8
  }

  test("edge case: negative parallelism is guarded to 1") {
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 100,
      defaultParallelism = -5,
      configuredValue = None
    )
    result shouldBe 8
  }

  test("edge case: very large split count") {
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 1000000,
      defaultParallelism = 64,
      configuredValue = None
    )
    // Always capped at maxSplitsPerTask
    result shouldBe 8
  }

  test("edge case: single core cluster with many splits") {
    // 100 > 1*2=2, target groups = 1*4=4
    // splitsPerTask = ceil(100/4) = 25, capped at 8
    val result = SplitsPerTaskCalculator.calculate(
      totalSplits = 100,
      defaultParallelism = 1,
      configuredValue = None
    )
    result shouldBe 8
  }

  // ============================================================================
  // RESULTING GROUP COUNT VERIFICATION
  // ============================================================================

  test("group count: small table achieves maximum parallelism") {
    val totalSplits = 8
    val parallelism = 4
    val splitsPerTask = SplitsPerTaskCalculator.calculate(
      totalSplits = totalSplits,
      defaultParallelism = parallelism,
      configuredValue = None
    )
    val groups = math.ceil(totalSplits.toDouble / splitsPerTask).toInt
    groups shouldBe 8 // Full parallelism
  }

  test("group count: large table achieves approximately 4x parallelism groups") {
    val totalSplits = 1024 // Use power of 2 for exact division
    val parallelism = 64
    val splitsPerTask = SplitsPerTaskCalculator.calculate(
      totalSplits = totalSplits,
      defaultParallelism = parallelism,
      configuredValue = None
    )
    val groups       = math.ceil(totalSplits.toDouble / splitsPerTask).toInt
    val targetGroups = parallelism * 4 // 256
    // Algorithm: splitsPerTask = ceil(1024/256) = 4, groups = 1024/4 = 256
    groups shouldBe targetGroups
  }

  test("group count: large table with non-exact division achieves close to target") {
    val totalSplits = 1000
    val parallelism = 64
    val splitsPerTask = SplitsPerTaskCalculator.calculate(
      totalSplits = totalSplits,
      defaultParallelism = parallelism,
      configuredValue = None
    )
    val groups       = math.ceil(totalSplits.toDouble / splitsPerTask).toInt
    val targetGroups = parallelism * 4 // 256
    // Algorithm: splitsPerTask = ceil(1000/256) = 4, groups = ceil(1000/4) = 250
    // We get close to target (within 10%), but not exactly due to ceiling
    splitsPerTask shouldBe 4
    groups shouldBe 250
    groups.toDouble / targetGroups should be >= 0.9 // Within 10% of target
  }

  test("group count: very large table still respects max splits cap") {
    val totalSplits = 100000
    val parallelism = 64
    val splitsPerTask = SplitsPerTaskCalculator.calculate(
      totalSplits = totalSplits,
      defaultParallelism = parallelism,
      configuredValue = None
    )
    splitsPerTask shouldBe 8 // Default max
    val groups = math.ceil(totalSplits.toDouble / splitsPerTask).toInt
    groups shouldBe 12500 // 100000/8
  }

  // ============================================================================
  // DEFAULT CONSTANT
  // ============================================================================

  test("DefaultMaxSplitsPerTask constant should be 8") {
    SplitsPerTaskCalculator.DefaultMaxSplitsPerTask shouldBe 8
  }

  // ============================================================================
  // COMPREHENSIVE EXAMPLES FROM DESIGN DOC
  // ============================================================================

  test("design doc example: small cluster (4 cores) table progression") {
    val parallelism = 4
    // threshold = 4*2 = 8
    // target groups = 4*4 = 16

    // 8 splits: at threshold, returns 1
    SplitsPerTaskCalculator.calculate(8, parallelism, None) shouldBe 1

    // 16 splits: above threshold, ceil(16/16)=1
    SplitsPerTaskCalculator.calculate(16, parallelism, None) shouldBe 1

    // 32 splits: ceil(32/16)=2
    SplitsPerTaskCalculator.calculate(32, parallelism, None) shouldBe 2

    // 64 splits: ceil(64/16)=4
    SplitsPerTaskCalculator.calculate(64, parallelism, None) shouldBe 4

    // 256 splits: ceil(256/16)=16, capped at 8
    SplitsPerTaskCalculator.calculate(256, parallelism, None) shouldBe 8
  }

  test("design doc example: large cluster (64 cores) table progression") {
    val parallelism = 64
    // threshold = 64*2 = 128
    // target groups = 64*4 = 256

    // 128 splits: at threshold, returns 1
    SplitsPerTaskCalculator.calculate(128, parallelism, None) shouldBe 1

    // 256 splits: above threshold, ceil(256/256)=1
    SplitsPerTaskCalculator.calculate(256, parallelism, None) shouldBe 1

    // 512 splits: ceil(512/256)=2
    SplitsPerTaskCalculator.calculate(512, parallelism, None) shouldBe 2

    // 1024 splits: ceil(1024/256)=4
    SplitsPerTaskCalculator.calculate(1024, parallelism, None) shouldBe 4

    // 4096 splits: ceil(4096/256)=16, capped at 8
    SplitsPerTaskCalculator.calculate(4096, parallelism, None) shouldBe 8
  }
}
