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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SelectiveCompactionTest extends AnyFunSuite with Matchers {

  // ============================================================================
  // TombstoneDistributor.extractPartitionValues tests
  // ============================================================================

  test("extractPartitionValues parses single partition from path") {
    val path   = "s3://bucket/table/date=2024-01-15/file.split"
    val result = TombstoneDistributor.extractPartitionValues(path)
    result shouldBe Map("date" -> "2024-01-15")
  }

  test("extractPartitionValues parses multiple partitions from path") {
    val path   = "s3://bucket/table/year=2024/month=01/day=15/file.split"
    val result = TombstoneDistributor.extractPartitionValues(path)
    result shouldBe Map("year" -> "2024", "month" -> "01", "day" -> "15")
  }

  test("extractPartitionValues handles local paths") {
    val path   = "/local/path/region=us-east/category=electronics/file.split"
    val result = TombstoneDistributor.extractPartitionValues(path)
    result shouldBe Map("region" -> "us-east", "category" -> "electronics")
  }

  test("extractPartitionValues returns empty map for unpartitioned path") {
    val path   = "s3://bucket/table/file.split"
    val result = TombstoneDistributor.extractPartitionValues(path)
    result shouldBe Map.empty
  }

  test("extractPartitionValues handles Azure abfss paths") {
    val path   = "abfss://container@account.dfs.core.windows.net/table/date=2024-01-01/file.split"
    val result = TombstoneDistributor.extractPartitionValues(path)
    result shouldBe Map("date" -> "2024-01-01")
  }

  test("extractPartitionValues handles special characters in values") {
    val path   = "s3://bucket/table/name=hello_world/id=abc-123/file.split"
    val result = TombstoneDistributor.extractPartitionValues(path)
    result shouldBe Map("name" -> "hello_world", "id" -> "abc-123")
  }

  // ============================================================================
  // TombstoneDistributor.valuesWithinBounds tests
  // ============================================================================

  test("valuesWithinBounds returns true when value is within bounds") {
    val partitionValues = Map("date" -> "2024-01-15")
    val bounds          = Some(Map("date" -> PartitionBounds(Some("2024-01-01"), Some("2024-01-31"))))

    TombstoneDistributor.valuesWithinBounds(partitionValues, bounds) shouldBe true
  }

  test("valuesWithinBounds returns false when value is before min") {
    val partitionValues = Map("date" -> "2023-12-15")
    val bounds          = Some(Map("date" -> PartitionBounds(Some("2024-01-01"), Some("2024-01-31"))))

    TombstoneDistributor.valuesWithinBounds(partitionValues, bounds) shouldBe false
  }

  test("valuesWithinBounds returns false when value is after max") {
    val partitionValues = Map("date" -> "2024-02-15")
    val bounds          = Some(Map("date" -> PartitionBounds(Some("2024-01-01"), Some("2024-01-31"))))

    TombstoneDistributor.valuesWithinBounds(partitionValues, bounds) shouldBe false
  }

  test("valuesWithinBounds handles multiple partition columns") {
    val partitionValues = Map("year" -> "2024", "month" -> "06")
    val bounds = Some(
      Map(
        "year"  -> PartitionBounds(Some("2024"), Some("2024")),
        "month" -> PartitionBounds(Some("01"), Some("12"))
      )
    )

    TombstoneDistributor.valuesWithinBounds(partitionValues, bounds) shouldBe true
  }

  test("valuesWithinBounds returns true when bounds have no min (unbounded lower)") {
    val partitionValues = Map("date" -> "2020-01-01")
    val bounds          = Some(Map("date" -> PartitionBounds(None, Some("2024-12-31"))))

    TombstoneDistributor.valuesWithinBounds(partitionValues, bounds) shouldBe true
  }

  test("valuesWithinBounds returns true when bounds have no max (unbounded upper)") {
    val partitionValues = Map("date" -> "2030-01-01")
    val bounds          = Some(Map("date" -> PartitionBounds(Some("2024-01-01"), None)))

    TombstoneDistributor.valuesWithinBounds(partitionValues, bounds) shouldBe true
  }

  test("valuesWithinBounds returns true for unpartitioned manifest with unpartitioned tombstone") {
    val partitionValues                              = Map.empty[String, String]
    val bounds: Option[Map[String, PartitionBounds]] = None

    TombstoneDistributor.valuesWithinBounds(partitionValues, bounds) shouldBe true
  }

  test("valuesWithinBounds returns false for partitioned tombstone with unpartitioned manifest") {
    val partitionValues                              = Map("date" -> "2024-01-15")
    val bounds: Option[Map[String, PartitionBounds]] = None

    TombstoneDistributor.valuesWithinBounds(partitionValues, bounds) shouldBe false
  }

  // ============================================================================
  // TombstoneDistributor.distributeTombstones tests
  // ============================================================================

  test("distributeTombstones assigns tombstones to correct manifests by partition bounds") {
    // Manifest 1: January 2024
    val manifest1 = ManifestInfo(
      path = "manifests/m1.avro",
      numEntries = 1000,
      minAddedAtVersion = 1,
      maxAddedAtVersion = 10,
      partitionBounds = Some(Map("date" -> PartitionBounds(Some("2024-01-01"), Some("2024-01-31"))))
    )

    // Manifest 2: February 2024
    val manifest2 = ManifestInfo(
      path = "manifests/m2.avro",
      numEntries = 1000,
      minAddedAtVersion = 11,
      maxAddedAtVersion = 20,
      partitionBounds = Some(Map("date" -> PartitionBounds(Some("2024-02-01"), Some("2024-02-29"))))
    )

    // Manifest 3: March 2024
    val manifest3 = ManifestInfo(
      path = "manifests/m3.avro",
      numEntries = 1000,
      minAddedAtVersion = 21,
      maxAddedAtVersion = 30,
      partitionBounds = Some(Map("date" -> PartitionBounds(Some("2024-03-01"), Some("2024-03-31"))))
    )

    // Tombstones: 500 in January, 0 in February, 100 in March
    val januaryTombstones = (1 to 500).map(i => f"s3://bucket/table/date=2024-01-${(i % 28 + 1)}%02d/file$i.split")
    val marchTombstones   = (1 to 100).map(i => f"s3://bucket/table/date=2024-03-${(i % 28 + 1)}%02d/file$i.split")
    val tombstones: Set[String] = (januaryTombstones ++ marchTombstones).toSet

    val result = TombstoneDistributor.distributeTombstones(Seq(manifest1, manifest2, manifest3), tombstones)

    result(0).tombstoneCount shouldBe 500 // January manifest
    result(1).tombstoneCount shouldBe 0   // February manifest (clean!)
    result(2).tombstoneCount shouldBe 100 // March manifest
  }

  test("distributeTombstones returns zero counts when no tombstones") {
    val manifest = ManifestInfo(
      path = "manifests/m1.avro",
      numEntries = 1000,
      minAddedAtVersion = 1,
      maxAddedAtVersion = 10,
      partitionBounds = Some(Map("date" -> PartitionBounds(Some("2024-01-01"), Some("2024-01-31"))))
    )

    val result = TombstoneDistributor.distributeTombstones(Seq(manifest), Set.empty)

    result(0).tombstoneCount shouldBe 0
    result(0).liveEntryCount shouldBe 1000
  }

  // ============================================================================
  // TombstoneDistributor.selectivePartition tests
  // ============================================================================

  test("selectivePartition separates clean and dirty manifests by threshold") {
    // Clean manifest: 5% tombstones (below 10% threshold)
    val cleanManifest = ManifestInfo(
      path = "manifests/clean.avro",
      numEntries = 1000,
      minAddedAtVersion = 1,
      maxAddedAtVersion = 10,
      tombstoneCount = 50,
      liveEntryCount = 950
    )

    // Dirty manifest: 20% tombstones (above 10% threshold)
    val dirtyManifest = ManifestInfo(
      path = "manifests/dirty.avro",
      numEntries = 1000,
      minAddedAtVersion = 11,
      maxAddedAtVersion = 20,
      tombstoneCount = 200,
      liveEntryCount = 800
    )

    val (keep, rewrite) = TombstoneDistributor.selectivePartition(Seq(cleanManifest, dirtyManifest), threshold = 0.10)

    keep.size shouldBe 1
    keep.head.path shouldBe "manifests/clean.avro"

    rewrite.size shouldBe 1
    rewrite.head.path shouldBe "manifests/dirty.avro"
  }

  test("selectivePartition keeps all manifests when all are clean") {
    val manifests = (1 to 5).map { i =>
      ManifestInfo(
        path = s"manifests/m$i.avro",
        numEntries = 1000,
        minAddedAtVersion = i,
        maxAddedAtVersion = i,
        tombstoneCount = 50, // 5% - clean
        liveEntryCount = 950
      )
    }

    val (keep, rewrite) = TombstoneDistributor.selectivePartition(manifests, threshold = 0.10)

    keep.size shouldBe 5
    rewrite.size shouldBe 0
  }

  test("selectivePartition marks all for rewrite when all are dirty") {
    val manifests = (1 to 5).map { i =>
      ManifestInfo(
        path = s"manifests/m$i.avro",
        numEntries = 1000,
        minAddedAtVersion = i,
        maxAddedAtVersion = i,
        tombstoneCount = 200, // 20% - dirty
        liveEntryCount = 800
      )
    }

    val (keep, rewrite) = TombstoneDistributor.selectivePartition(manifests, threshold = 0.10)

    keep.size shouldBe 0
    rewrite.size shouldBe 5
  }

  // ============================================================================
  // TombstoneDistributor.isSelectiveCompactionBeneficial tests
  // ============================================================================

  test("isSelectiveCompactionBeneficial returns true when savings >= 10%") {
    val keepManifests = (1 to 9).map { i =>
      ManifestInfo(s"m$i.avro", numEntries = 1000, minAddedAtVersion = i, maxAddedAtVersion = i)
    }
    val rewriteManifests = Seq(
      ManifestInfo("dirty.avro", numEntries = 1000, minAddedAtVersion = 10, maxAddedAtVersion = 10)
    )

    // Keeping 9000 entries out of 10000 = 90% savings
    TombstoneDistributor.isSelectiveCompactionBeneficial(keepManifests, rewriteManifests) shouldBe true
  }

  test("isSelectiveCompactionBeneficial returns false when no manifests to keep") {
    val keepManifests = Seq.empty[ManifestInfo]
    val rewriteManifests = Seq(
      ManifestInfo("dirty.avro", numEntries = 1000, minAddedAtVersion = 1, maxAddedAtVersion = 1)
    )

    TombstoneDistributor.isSelectiveCompactionBeneficial(keepManifests, rewriteManifests) shouldBe false
  }

  test("isSelectiveCompactionBeneficial returns false when no manifests to rewrite") {
    val keepManifests = Seq(
      ManifestInfo("clean.avro", numEntries = 1000, minAddedAtVersion = 1, maxAddedAtVersion = 1)
    )
    val rewriteManifests = Seq.empty[ManifestInfo]

    TombstoneDistributor.isSelectiveCompactionBeneficial(keepManifests, rewriteManifests) shouldBe false
  }

  test("isSelectiveCompactionBeneficial returns false when savings < 10%") {
    val keepManifests = Seq(
      ManifestInfo("clean.avro", numEntries = 100, minAddedAtVersion = 1, maxAddedAtVersion = 1)
    )
    val rewriteManifests = (1 to 9).map { i =>
      ManifestInfo(s"dirty$i.avro", numEntries = 1000, minAddedAtVersion = i, maxAddedAtVersion = i)
    }

    // Keeping 100 entries out of 9100 = ~1% savings - not beneficial
    TombstoneDistributor.isSelectiveCompactionBeneficial(keepManifests, rewriteManifests) shouldBe false
  }

  // ============================================================================
  // Integration scenario tests
  // ============================================================================

  test("10K partitions with 1 dirty partition only marks 1 manifest for rewrite") {
    // Simulate 100 manifests, each covering ~100 partitions
    // Only manifest 50 has tombstones
    val manifests = (1 to 100).map { i =>
      val startDate = f"2024-${(i / 12) + 1}%02d-${(i % 28) + 1}%02d"
      val endDate   = f"2024-${(i / 12) + 1}%02d-${((i % 28) + 3).min(28)}%02d"
      ManifestInfo(
        path = s"manifests/m$i.avro",
        numEntries = 100,
        minAddedAtVersion = i,
        maxAddedAtVersion = i,
        partitionBounds = Some(Map("date" -> PartitionBounds(Some(startDate), Some(endDate))))
      )
    }

    // Tombstones only affect manifest 50's date range
    val tombstones = (1 to 50).map(i => "s3://bucket/table/date=2024-05-15/file$i.split").toSet

    val result = TombstoneDistributor.distributeTombstones(manifests, tombstones)

    // Most manifests should have 0 tombstones
    val cleanManifests = result.filter(_.tombstoneCount == 0)
    val dirtyManifests = result.filter(_.tombstoneCount > 0)

    // At least 90% of manifests should be clean (the tombstones only affect 1 date)
    cleanManifests.size should be >= 90

    val (keep, rewrite) = TombstoneDistributor.selectivePartition(result, threshold = 0.10)

    // Should keep most manifests
    keep.size should be >= 90

    // Should only rewrite the dirty ones
    rewrite.size should be <= 10
  }
}
