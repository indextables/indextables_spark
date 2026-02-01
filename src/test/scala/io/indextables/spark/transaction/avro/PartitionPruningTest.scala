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

package io.indextables.spark.transaction.avro

import org.apache.spark.sql.sources._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PartitionPruningTest extends AnyFunSuite with Matchers {

  test("pruneManifests should prune by equality filter") {
    val manifests = Seq(
      createManifestInfo("m1.avro", Map("date" -> PartitionBounds(Some("2024-01-01"), Some("2024-01-10")))),
      createManifestInfo("m2.avro", Map("date" -> PartitionBounds(Some("2024-01-11"), Some("2024-01-20")))),
      createManifestInfo("m3.avro", Map("date" -> PartitionBounds(Some("2024-01-21"), Some("2024-01-31"))))
    )

    // Filter: date = "2024-01-15" -> should match only m2
    val filter = EqualTo("date", "2024-01-15")
    val result = PartitionPruner.pruneManifests(manifests, filter)

    result should have size 1
    result.head.path shouldBe "m2.avro"
  }

  test("pruneManifests should prune by greater than filter") {
    val manifests = Seq(
      createManifestInfo("m1.avro", Map("value" -> PartitionBounds(Some("100"), Some("200")))),
      createManifestInfo("m2.avro", Map("value" -> PartitionBounds(Some("201"), Some("300")))),
      createManifestInfo("m3.avro", Map("value" -> PartitionBounds(Some("301"), Some("400"))))
    )

    // Filter: value > "250" -> should match m2 (max=300 > 250) and m3
    val filter = GreaterThan("value", "250")
    val result = PartitionPruner.pruneManifests(manifests, filter)

    result should have size 2
    result.map(_.path) should contain allOf ("m2.avro", "m3.avro")
  }

  test("pruneManifests should prune by less than filter") {
    val manifests = Seq(
      createManifestInfo("m1.avro", Map("id" -> PartitionBounds(Some("001"), Some("100")))),
      createManifestInfo("m2.avro", Map("id" -> PartitionBounds(Some("101"), Some("200")))),
      createManifestInfo("m3.avro", Map("id" -> PartitionBounds(Some("201"), Some("300"))))
    )

    // Filter: id < "150" -> should match m1 (min=001 < 150) and m2 (min=101 < 150)
    val filter = LessThan("id", "150")
    val result = PartitionPruner.pruneManifests(manifests, filter)

    result should have size 2
    result.map(_.path) should contain allOf ("m1.avro", "m2.avro")
  }

  test("pruneManifests should prune by IN filter") {
    val manifests = Seq(
      createManifestInfo("m1.avro", Map("region" -> PartitionBounds(Some("eu-west"), Some("eu-west")))),
      createManifestInfo("m2.avro", Map("region" -> PartitionBounds(Some("us-east"), Some("us-east")))),
      createManifestInfo("m3.avro", Map("region" -> PartitionBounds(Some("us-west"), Some("us-west")))),
      createManifestInfo("m4.avro", Map("region" -> PartitionBounds(Some("ap-east"), Some("ap-east"))))
    )

    // Filter: region IN ("us-east", "us-west")
    val filter = In("region", Array("us-east", "us-west"))
    val result = PartitionPruner.pruneManifests(manifests, filter)

    result should have size 2
    result.map(_.path) should contain allOf ("m2.avro", "m3.avro")
  }

  test("pruneManifests should handle AND filters") {
    val manifests = Seq(
      createManifestInfo(
        "m1.avro",
        Map(
          "date"   -> PartitionBounds(Some("2024-01-01"), Some("2024-01-15")),
          "region" -> PartitionBounds(Some("us-east"), Some("us-east"))
        )
      ),
      createManifestInfo(
        "m2.avro",
        Map(
          "date"   -> PartitionBounds(Some("2024-01-01"), Some("2024-01-15")),
          "region" -> PartitionBounds(Some("us-west"), Some("us-west"))
        )
      ),
      createManifestInfo(
        "m3.avro",
        Map(
          "date"   -> PartitionBounds(Some("2024-01-16"), Some("2024-01-31")),
          "region" -> PartitionBounds(Some("us-east"), Some("us-east"))
        )
      )
    )

    // Filter: date = "2024-01-10" AND region = "us-east"
    val filter = And(EqualTo("date", "2024-01-10"), EqualTo("region", "us-east"))
    val result = PartitionPruner.pruneManifests(manifests, filter)

    result should have size 1
    result.head.path shouldBe "m1.avro"
  }

  test("pruneManifests should handle OR filters") {
    val manifests = Seq(
      createManifestInfo("m1.avro", Map("region" -> PartitionBounds(Some("us-east"), Some("us-east")))),
      createManifestInfo("m2.avro", Map("region" -> PartitionBounds(Some("us-west"), Some("us-west")))),
      createManifestInfo("m3.avro", Map("region" -> PartitionBounds(Some("eu-west"), Some("eu-west"))))
    )

    // Filter: region = "us-east" OR region = "eu-west"
    val filter = Or(EqualTo("region", "us-east"), EqualTo("region", "eu-west"))
    val result = PartitionPruner.pruneManifests(manifests, filter)

    result should have size 2
    result.map(_.path) should contain allOf ("m1.avro", "m3.avro")
  }

  test("pruneManifests should include manifests without partition bounds") {
    val manifests = Seq(
      createManifestInfo("m1.avro", Map("date" -> PartitionBounds(Some("2024-01-01"), Some("2024-01-15")))),
      ManifestInfo("m2.avro", 1000, 1, 100, None), // No partition bounds
      createManifestInfo("m3.avro", Map("date" -> PartitionBounds(Some("2024-01-16"), Some("2024-01-31"))))
    )

    // Filter should not prune m2 since it has no bounds
    val filter = EqualTo("date", "2024-01-10")
    val result = PartitionPruner.pruneManifests(manifests, filter)

    result should have size 2
    result.map(_.path) should contain allOf ("m1.avro", "m2.avro")
  }

  test("pruneManifests should return all manifests when no filter provided") {
    val manifests = Seq(
      createManifestInfo("m1.avro", Map("date" -> PartitionBounds(Some("2024-01-01"), Some("2024-01-15")))),
      createManifestInfo("m2.avro", Map("date" -> PartitionBounds(Some("2024-01-16"), Some("2024-01-31"))))
    )

    val result = PartitionPruner.pruneManifests(manifests, Seq.empty)
    result should have size 2
  }

  test("pruneManifests should handle StringStartsWith filter") {
    val manifests = Seq(
      createManifestInfo("m1.avro", Map("path" -> PartitionBounds(Some("abc"), Some("abz")))),
      createManifestInfo("m2.avro", Map("path" -> PartitionBounds(Some("bcd"), Some("bcz")))),
      createManifestInfo("m3.avro", Map("path" -> PartitionBounds(Some("cde"), Some("cdz"))))
    )

    // Filter: path LIKE "ab%" -> should match only m1
    val filter = StringStartsWith("path", "ab")
    val result = PartitionPruner.pruneManifests(manifests, filter)

    result should have size 1
    result.head.path shouldBe "m1.avro"
  }

  test("pruneManifests should prune by range filter (GreaterThanOrEqual + LessThan)") {
    val manifests = Seq(
      createManifestInfo("m1.avro", Map("ts" -> PartitionBounds(Some("1000"), Some("1999")))),
      createManifestInfo("m2.avro", Map("ts" -> PartitionBounds(Some("2000"), Some("2999")))),
      createManifestInfo("m3.avro", Map("ts" -> PartitionBounds(Some("3000"), Some("3999")))),
      createManifestInfo("m4.avro", Map("ts" -> PartitionBounds(Some("4000"), Some("4999"))))
    )

    // Filter: ts >= 2500 AND ts < 3500
    val filter = And(
      GreaterThanOrEqual("ts", "2500"),
      LessThan("ts", "3500")
    )
    val result = PartitionPruner.pruneManifests(manifests, filter)

    result should have size 2
    result.map(_.path) should contain allOf ("m2.avro", "m3.avro")
  }

  test("boundsMatchFilter should handle IsNotNull filter") {
    val bounds = Map("name" -> PartitionBounds(Some("Alice"), Some("Zara")))

    // IsNotNull should return true if bounds have values
    PartitionPruner.boundsMatchFilter(bounds, IsNotNull("name")) shouldBe true

    // IsNotNull on column with no bounds
    PartitionPruner.boundsMatchFilter(Map.empty, IsNotNull("name")) shouldBe true
  }

  test("boundsMatchFilter should handle NOT filter conservatively") {
    val bounds = Map("status" -> PartitionBounds(Some("active"), Some("pending")))

    // NOT(status = "active") should include this manifest
    // because the range includes more than just "active"
    val filter = Not(EqualTo("status", "active"))
    PartitionPruner.boundsMatchFilter(bounds, filter) shouldBe true

    // If bounds are exactly one value that matches, it can be pruned
    val exactBounds = Map("status" -> PartitionBounds(Some("active"), Some("active")))
    PartitionPruner.boundsMatchFilter(exactBounds, filter) shouldBe false
  }

  test("extractReferencedColumns should extract column names from filters") {
    PartitionPruner.extractReferencedColumns(EqualTo("a", 1)) shouldBe Set("a")
    PartitionPruner.extractReferencedColumns(GreaterThan("b", 2)) shouldBe Set("b")
    PartitionPruner.extractReferencedColumns(In("c", Array(1, 2, 3))) shouldBe Set("c")
    PartitionPruner.extractReferencedColumns(And(EqualTo("a", 1), EqualTo("b", 2))) shouldBe Set("a", "b")
    PartitionPruner.extractReferencedColumns(Or(EqualTo("x", 1), EqualTo("y", 2))) shouldBe Set("x", "y")
    PartitionPruner.extractReferencedColumns(Not(IsNull("z"))) shouldBe Set("z")
  }

  test("isPartitionFilter should correctly identify partition-only filters") {
    val partitionColumns = Set("date", "region")

    // Filter only on partition columns
    PartitionPruner.isPartitionFilter(EqualTo("date", "2024-01-01"), partitionColumns) shouldBe true
    PartitionPruner.isPartitionFilter(
      And(EqualTo("date", "2024-01-01"), EqualTo("region", "us")),
      partitionColumns
    ) shouldBe true

    // Filter includes non-partition column
    PartitionPruner.isPartitionFilter(EqualTo("id", 123), partitionColumns) shouldBe false
    PartitionPruner.isPartitionFilter(
      And(EqualTo("date", "2024-01-01"), EqualTo("id", 123)),
      partitionColumns
    ) shouldBe false
  }

  test("separateFilters should correctly split partition and data filters") {
    val partitionColumns = Set("date", "region")
    val filters = Seq(
      EqualTo("date", "2024-01-01"),
      EqualTo("region", "us-east"),
      EqualTo("id", 123),
      GreaterThan("score", 0.5)
    )

    val (partitionFilters, dataFilters) = PartitionPruner.separateFilters(filters, partitionColumns)

    partitionFilters should have size 2
    dataFilters should have size 2

    partitionFilters.map {
      case EqualTo(attr, _) => attr
      case _                => ""
    } should contain allOf ("date", "region")

    dataFilters.map {
      case EqualTo(attr, _)     => attr
      case GreaterThan(attr, _) => attr
      case _                    => ""
    } should contain allOf ("id", "score")
  }

  test("partition pruning should provide significant speedup on large manifest lists") {
    // Create 100 manifests with different date ranges
    val manifests = (1 to 100).map { i =>
      val startDate = f"2024-$i%02d-01"
      val endDate   = f"2024-$i%02d-28"
      createManifestInfo(s"m$i.avro", Map("month" -> PartitionBounds(Some(startDate), Some(endDate))))
    }

    // Filter for a specific month (should match only 1 manifest)
    val filter = EqualTo("month", "2024-06-15")

    val startTime = System.currentTimeMillis()
    val result    = PartitionPruner.pruneManifests(manifests, filter)
    val duration  = System.currentTimeMillis() - startTime

    // Should prune to 1 manifest
    result should have size 1
    result.head.path shouldBe "m6.avro"

    // Should be very fast (< 10ms for 100 manifests)
    duration should be < 10L
  }

  // Helper methods

  private def createManifestInfo(
    path: String,
    partitionBounds: Map[String, PartitionBounds]
  ): ManifestInfo =
    ManifestInfo(
      path = path,
      numEntries = 1000,
      minAddedAtVersion = 1,
      maxAddedAtVersion = 100,
      partitionBounds = Some(partitionBounds)
    )
}
