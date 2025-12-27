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

package io.indextables.spark.prewarm

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

/**
 * Simple unit test for PreWarmManager that doesn't require full Spark setup. This tests the basic functionality without
 * network dependencies.
 */
class PreWarmManagerSimpleTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit =
    // Clear any previous pre-warm state
    PreWarmManager.clearAll()

  override def afterEach(): Unit =
    PreWarmManager.clearAll()

  test("joinWarmupFuture should return false when pre-warm is disabled") {
    val splitPath = "test-split.split"
    val queryHash = "test-query-hash"

    // Try to join when pre-warm is disabled
    val result = PreWarmManager.joinWarmupFuture(splitPath, queryHash, isPreWarmEnabled = false)

    result shouldBe false
  }

  test("joinWarmupFuture should return false when no future exists") {
    val splitPath = "test-split.split"
    val queryHash = "test-query-hash"

    // Try to join a non-existent warmup future
    val result = PreWarmManager.joinWarmupFuture(splitPath, queryHash, isPreWarmEnabled = true)

    result shouldBe false
  }

  test("clearAll should reset all pre-warm state") {
    // Initially, no stats should exist
    val statsBeforeClear = PreWarmManager.getPreWarmStats("any-hash")
    statsBeforeClear shouldBe None

    // Simulate having some state by calling clearAll (this sets internal state)
    PreWarmManager.clearAll()

    // Verify state was cleared
    val statsAfterClear = PreWarmManager.getPreWarmStats("any-hash")
    statsAfterClear shouldBe None
  }

  test("getPreWarmStats should return None for non-existent query hashes") {
    val stats = PreWarmManager.getPreWarmStats("non-existent-hash")
    stats shouldBe None
  }

  test("PreWarmManager should have expected public methods") {
    // This test verifies that the PreWarmManager has the expected API
    // without actually executing complex operations

    val methods = PreWarmManager.getClass.getMethods.map(_.getName).toSet

    // Verify key methods exist
    methods should contain("executePreWarm")
    methods should contain("joinWarmupFuture")
    methods should contain("getPreWarmStats")
    methods should contain("clearAll")
  }
}
