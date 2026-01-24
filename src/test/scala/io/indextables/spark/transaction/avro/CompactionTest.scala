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

import io.indextables.spark.TestBase
import io.indextables.spark.io.CloudStorageProviderFactory

import org.apache.spark.sql.util.CaseInsensitiveStringMap

class CompactionTest extends TestBase {

  test("compaction should trigger when tombstone ratio exceeds 10%") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Write initial state with 100 files
        val initialFiles = (1 to 100).map(i => createTestFileEntry(s"file$i.split", version = 1))
        stateWriter.writeState(
          currentVersion = 1,
          newFiles = initialFiles,
          removedPaths = Set.empty
        )

        // Get initial manifest
        val manifestIO = StateManifestIO(cloudProvider)
        val initialManifest = manifestIO.readStateManifest(s"$tempPath/state-v00000000000000000001")
        val initialManifestCount = initialManifest.manifests.size

        // Remove 15 files (15% tombstone ratio > 10% threshold, should compact)
        val removedPaths = (1 to 15).map(i => s"file$i.split").toSet
        val stateDir2 = stateWriter.writeState(
          currentVersion = 2,
          newFiles = Seq.empty,
          removedPaths = removedPaths
        )

        // After compaction, tombstones should be empty
        val compactedManifest = manifestIO.readStateManifest(stateDir2)
        compactedManifest.tombstones shouldBe empty
        compactedManifest.numFiles shouldBe 85

        // Verify data integrity after compaction
        val manifestReader = AvroManifestReader(cloudProvider)
        val manifestPaths = compactedManifest.manifests.map(m => s"$stateDir2/${m.path}")
        val readFiles = manifestReader.readManifestsParallel(manifestPaths)

        readFiles should have size 85
        readFiles.map(_.path) should not contain "file1.split"
        readFiles.map(_.path) should contain("file16.split")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("compaction should not trigger when tombstone ratio is below 10%") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Write initial state with 200 files
        val initialFiles = (1 to 200).map(i => createTestFileEntry(s"file$i.split", version = 1))
        stateWriter.writeState(
          currentVersion = 1,
          newFiles = initialFiles,
          removedPaths = Set.empty
        )

        // Remove 5 files (2.5% tombstone ratio < 10% threshold, should NOT compact)
        val removedPaths = (1 to 5).map(i => s"file$i.split").toSet
        val stateDir2 = stateWriter.writeState(
          currentVersion = 2,
          newFiles = Seq.empty,
          removedPaths = removedPaths
        )

        // After incremental write, tombstones should be present
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir2)

        manifest.tombstones should have size 5
        manifest.numFiles shouldBe 195
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("compaction should trigger when large remove operation occurs (with forceCompaction)") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Write initial state with 50,000 files
        val initialFiles = (1 to 50000).map(i => createTestFileEntry(s"file$i.split", version = 1))
        stateWriter.writeState(
          currentVersion = 1,
          newFiles = initialFiles,
          removedPaths = Set.empty
        )

        // Remove 1001 files with forceCompaction to test compaction behavior
        // Note: largeRemoveThreshold is Int.MaxValue by default (disabled)
        // so we use forceCompaction to verify compaction clears tombstones
        val removedPaths = (1 to 1001).map(i => s"file$i.split").toSet
        val stateDir2 = stateWriter.writeState(
          currentVersion = 2,
          newFiles = Seq.empty,
          removedPaths = removedPaths,
          forceCompaction = true
        )

        // After compaction, tombstones should be empty
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir2)

        manifest.tombstones shouldBe empty
        manifest.numFiles shouldBe 48999
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("compaction should preserve partition locality (files sorted by partition)") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Create files with partition values in random order
        val files = Seq(
          createTestFileEntry("f1.split", partitionValues = Map("date" -> "2024-01-15")),
          createTestFileEntry("f2.split", partitionValues = Map("date" -> "2024-01-01")),
          createTestFileEntry("f3.split", partitionValues = Map("date" -> "2024-01-20")),
          createTestFileEntry("f4.split", partitionValues = Map("date" -> "2024-01-10")),
          createTestFileEntry("f5.split", partitionValues = Map("date" -> "2024-01-05"))
        )

        // Write with force compaction to ensure sorting
        stateWriter.writeState(
          currentVersion = 1,
          newFiles = files,
          removedPaths = Set.empty,
          forceCompaction = true
        )

        // Read back and verify sorted order
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(s"$tempPath/state-v00000000000000000001")

        val manifestReader = AvroManifestReader(cloudProvider)
        val manifestPaths = manifest.manifests.map(m => s"$tempPath/state-v00000000000000000001/${m.path}")
        val readFiles = manifestReader.readManifestsParallel(manifestPaths)

        readFiles should have size 5
        // Files should be sorted by partition value
        val dates = readFiles.map(_.partitionValues("date"))
        dates shouldBe Seq("2024-01-01", "2024-01-05", "2024-01-10", "2024-01-15", "2024-01-20")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("compaction should handle multiple partition columns correctly") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Create files with multiple partition columns
        val files = Seq(
          createTestFileEntry("f1.split", partitionValues = Map("date" -> "2024-01-01", "region" -> "us-west")),
          createTestFileEntry("f2.split", partitionValues = Map("date" -> "2024-01-01", "region" -> "us-east")),
          createTestFileEntry("f3.split", partitionValues = Map("date" -> "2024-01-02", "region" -> "eu-west")),
          createTestFileEntry("f4.split", partitionValues = Map("date" -> "2024-01-02", "region" -> "us-east"))
        )

        stateWriter.writeState(
          currentVersion = 1,
          newFiles = files,
          removedPaths = Set.empty,
          forceCompaction = true
        )

        // Verify partition bounds are computed correctly
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(s"$tempPath/state-v00000000000000000001")

        manifest.manifests.head.partitionBounds shouldBe defined
        val bounds = manifest.manifests.head.partitionBounds.get

        bounds should contain key "date"
        bounds("date").min shouldBe Some("2024-01-01")
        bounds("date").max shouldBe Some("2024-01-02")

        bounds should contain key "region"
        bounds("region").min shouldBe Some("eu-west")
        bounds("region").max shouldBe Some("us-west")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("compaction should preserve schema registry entries") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        val schemaRegistry = Map(
          "_schema_abc123" -> """{"type":"record","name":"doc1"}""",
          "_schema_def456" -> """{"type":"record","name":"doc2"}"""
        )

        val files = (1 to 100).map(i => createTestFileEntry(s"file$i.split", version = 1))
        stateWriter.writeState(
          currentVersion = 1,
          newFiles = files,
          removedPaths = Set.empty,
          schemaRegistry = schemaRegistry
        )

        // Force compaction
        val removedPaths = (1 to 50).map(i => s"file$i.split").toSet
        val stateDir2 = stateWriter.writeState(
          currentVersion = 2,
          newFiles = Seq.empty,
          removedPaths = removedPaths,
          schemaRegistry = schemaRegistry,
          forceCompaction = true
        )

        // Verify schema registry is preserved after compaction
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir2)

        manifest.schemaRegistry should have size 2
        manifest.schemaRegistry should contain key "_schema_abc123"
        manifest.schemaRegistry should contain key "_schema_def456"
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("needsCompaction should correctly evaluate compaction conditions") {
    val cloudProvider = CloudStorageProviderFactory.createProvider(
      "/tmp/unused",
      new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
      spark.sparkContext.hadoopConfiguration
    )

    try {
      val stateWriter = StateWriter(cloudProvider, "/tmp/unused")

      // Test tombstone ratio threshold (10%)
      val manifest100Files = StateManifest(
        formatVersion = 1,
        stateVersion = 1,
        createdAt = System.currentTimeMillis(),
        numFiles = 100,
        totalBytes = 1000000L,
        manifests = Seq.empty,
        tombstones = Seq.empty,
        schemaRegistry = Map.empty,
        protocolVersion = 4
      )

      // 11 removes on 100 files = 11% > 10% threshold (triggers compaction)
      stateWriter.needsCompaction(manifest100Files, 11) shouldBe true

      // 10 removes on 100 files = 10% == 10% threshold (does NOT trigger, need > not >=)
      stateWriter.needsCompaction(manifest100Files, 10) shouldBe false

      // 8 removes on 100 files = 8% < 10% threshold
      stateWriter.needsCompaction(manifest100Files, 8) shouldBe false

      // Test large remove threshold (> 1000 removes)
      // Note: largeRemoveThreshold defaults to Int.MaxValue (disabled)
      // Must pass explicit config to test this behavior
      val manifest50kFiles = StateManifest(
        formatVersion = 1,
        stateVersion = 1,
        createdAt = System.currentTimeMillis(),
        numFiles = 50000,
        totalBytes = 50000000000L,
        manifests = Seq.empty,
        tombstones = Seq.empty,
        schemaRegistry = Map.empty,
        protocolVersion = 4
      )

      // With explicit largeRemoveThreshold=1000, 1001 removes triggers compaction
      val configWithLargeRemoveThreshold = CompactionConfig(largeRemoveThreshold = 1000)
      stateWriter.needsCompaction(manifest50kFiles, 1001, configWithLargeRemoveThreshold) shouldBe true

      // 999 removes doesn't trigger (ratio still low, below 1000 threshold)
      stateWriter.needsCompaction(manifest50kFiles, 999, configWithLargeRemoveThreshold) shouldBe false

      // With default config (largeRemoveThreshold=Int.MaxValue), large removes don't trigger
      stateWriter.needsCompaction(manifest50kFiles, 1001) shouldBe false

      // Test manifest count threshold (> 20 manifests)
      val manifestManyParts = StateManifest(
        formatVersion = 1,
        stateVersion = 1,
        createdAt = System.currentTimeMillis(),
        numFiles = 100000,
        totalBytes = 100000000000L,
        manifests = (1 to 25).map(i => ManifestInfo(s"manifest-$i.avro", 4000, 1, 100, None)),
        tombstones = Seq.empty,
        schemaRegistry = Map.empty,
        protocolVersion = 4
      )

      // > 20 manifests triggers compaction
      stateWriter.needsCompaction(manifestManyParts, 0) shouldBe true
    } finally {
      cloudProvider.close()
    }
  }

  // Helper methods

  private def createTestFileEntry(
      path: String,
      version: Long = 1,
      partitionValues: Map[String, String] = Map.empty): FileEntry = {
    FileEntry(
      path = path,
      partitionValues = partitionValues,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = None,
      minValues = None,
      maxValues = None,
      numRecords = Some(100L),
      footerStartOffset = None,
      footerEndOffset = None,
      hasFooterOffsets = false,
      splitTags = None,
      numMergeOps = None,
      docMappingRef = None,
      uncompressedSizeBytes = None,
      addedAtVersion = version,
      addedAtTimestamp = System.currentTimeMillis()
    )
  }
}
