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

package io.indextables.spark.storage

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for DriverSplitLocalityManager.
 */
class DriverSplitLocalityManagerTest extends AnyFunSuite with Matchers {

  // ============================================================================
  // INTERLEAVE BY HOST TESTS
  // ============================================================================

  test("interleaveByHost should return empty sequence for empty input") {
    val result = DriverSplitLocalityManager.interleaveByHost(
      Seq.empty[String],
      (_: String) => Some("host1")
    )
    result shouldBe empty
  }

  test("interleaveByHost should handle single host") {
    val items = Seq("a", "b", "c")
    val result = DriverSplitLocalityManager.interleaveByHost(
      items,
      (_: String) => Some("host1")
    )
    // All items on same host - order should be preserved
    result should contain theSameElementsInOrderAs Seq("a", "b", "c")
  }

  test("interleaveByHost should round-robin across multiple hosts") {
    // Items: 3 for host1, 2 for host2, 1 for host3
    case class Item(id: String, host: String)
    val items = Seq(
      Item("a1", "host1"), Item("a2", "host1"), Item("a3", "host1"),
      Item("b1", "host2"), Item("b2", "host2"),
      Item("c1", "host3")
    )

    val result = DriverSplitLocalityManager.interleaveByHost(
      items,
      (i: Item) => Some(i.host)
    )

    // Hosts are sorted alphabetically: host1, host2, host3
    // Round-robin: host1, host2, host3, host1, host2, host1
    result.map(_.id) should contain theSameElementsInOrderAs Seq("a1", "b1", "c1", "a2", "b2", "a3")
  }

  test("interleaveByHost should handle items without host preference last") {
    case class Item(id: String, host: Option[String])
    val items = Seq(
      Item("a1", Some("host1")),
      Item("a2", Some("host1")),
      Item("none1", None),
      Item("b1", Some("host2")),
      Item("none2", None)
    )

    val result = DriverSplitLocalityManager.interleaveByHost(
      items,
      (i: Item) => i.host
    )

    // Items with hosts come first (interleaved), then items without preference
    // Hosts sorted: host1, host2, then None
    // Round-robin: host1, host2, None, host1, None
    result.map(_.id) should contain theSameElementsInOrderAs Seq("a1", "b1", "none1", "a2", "none2")
  }

  test("interleaveByHost should handle equal distribution across hosts") {
    case class Item(id: String, host: String)
    val items = Seq(
      Item("a1", "host1"), Item("a2", "host1"),
      Item("b1", "host2"), Item("b2", "host2"),
      Item("c1", "host3"), Item("c2", "host3")
    )

    val result = DriverSplitLocalityManager.interleaveByHost(
      items,
      (i: Item) => Some(i.host)
    )

    // Each host has 2 items
    // Round 1: host1, host2, host3 -> a1, b1, c1
    // Round 2: host1, host2, host3 -> a2, b2, c2
    result.map(_.id) should contain theSameElementsInOrderAs Seq("a1", "b1", "c1", "a2", "b2", "c2")
  }

  test("interleaveByHost should produce deterministic ordering") {
    case class Item(id: String, host: String)
    val items = Seq(
      Item("x", "zebra"),
      Item("y", "apple"),
      Item("z", "mango")
    )

    // Run multiple times - should always get same result
    val results = (1 to 5).map { _ =>
      DriverSplitLocalityManager.interleaveByHost(items, (i: Item) => Some(i.host)).map(_.id)
    }

    // All results should be identical (hosts sorted alphabetically: apple, mango, zebra)
    results.distinct should have size 1
    results.head should contain theSameElementsInOrderAs Seq("y", "z", "x")
  }

  test("interleaveByHost should maximize executor utilization with realistic workload") {
    // Simulate 12 splits across 3 hosts (4 each)
    case class Split(path: String, host: String)
    val splits = Seq(
      Split("s1", "10.0.0.1"), Split("s2", "10.0.0.1"), Split("s3", "10.0.0.1"), Split("s4", "10.0.0.1"),
      Split("s5", "10.0.0.2"), Split("s6", "10.0.0.2"), Split("s7", "10.0.0.2"), Split("s8", "10.0.0.2"),
      Split("s9", "10.0.0.3"), Split("s10", "10.0.0.3"), Split("s11", "10.0.0.3"), Split("s12", "10.0.0.3")
    )

    val result = DriverSplitLocalityManager.interleaveByHost(
      splits,
      (s: Split) => Some(s.host)
    )

    // First 3 tasks should go to different hosts (round-robin starts all executors)
    val firstThreeHosts = result.take(3).map(_.host).toSet
    firstThreeHosts should have size 3

    // Consecutive tasks should alternate between hosts (not all same host)
    // Check that we don't have more than 1 consecutive task for the same host
    val consecutiveSameHost = result.sliding(2).count {
      case Seq(a, b) => a.host == b.host
      case _         => false
    }
    // With 3 hosts and 4 items each, there should be 0 consecutive same-host pairs
    consecutiveSameHost shouldBe 0

    // Verify all items are present
    result should have size 12
    result.map(_.path).toSet shouldBe splits.map(_.path).toSet
  }
}
