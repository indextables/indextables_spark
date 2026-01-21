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

class IncrementalStateWriteTest extends TestBase {

  test("writeState should create initial state with empty files") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        val stateDir = stateWriter.writeState(
          currentVersion = 1,
          newFiles = Seq.empty,
          removedPaths = Set.empty
        )

        stateDir should include("state-v00000000000000000001")

        // Verify manifest was created
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir)

        manifest.stateVersion shouldBe 1
        manifest.numFiles shouldBe 0
        manifest.manifests should have size 1 // Empty manifest chunk
        manifest.tombstones shouldBe empty
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("writeState should create initial state with files") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)
        val files = (1 to 100).map(i => createTestFileEntry(s"splits/file$i.split", version = 1))

        val stateDir = stateWriter.writeState(
          currentVersion = 1,
          newFiles = files,
          removedPaths = Set.empty
        )

        // Verify manifest
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir)

        manifest.stateVersion shouldBe 1
        manifest.numFiles shouldBe 100
        manifest.tombstones shouldBe empty

        // Verify files can be read back
        val manifestReader = AvroManifestReader(cloudProvider)
        val manifestPaths = manifest.manifests.map(m => s"$stateDir/${m.path}")
        val readFiles = manifestReader.readManifestsParallel(manifestPaths)

        readFiles should have size 100
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("writeState should create incremental state with new files") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Write initial state
        val initialFiles = (1 to 50).map(i => createTestFileEntry(s"splits/file$i.split", version = 1))
        val stateDir1 = stateWriter.writeState(
          currentVersion = 1,
          newFiles = initialFiles,
          removedPaths = Set.empty
        )

        // Write incremental state with new files
        val newFiles = (51 to 75).map(i => createTestFileEntry(s"splits/file$i.split", version = 2))
        val stateDir2 = stateWriter.writeState(
          currentVersion = 2,
          newFiles = newFiles,
          removedPaths = Set.empty
        )

        stateDir2 should include("state-v00000000000000000002")

        // Verify manifest
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir2)

        manifest.stateVersion shouldBe 2
        manifest.numFiles shouldBe 75 // 50 + 25
        manifest.manifests.size should be >= 2 // At least initial + new
        manifest.tombstones shouldBe empty
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("writeState should handle file removals with tombstones") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Write initial state with 200 files
        val initialFiles = (1 to 200).map(i => createTestFileEntry(s"splits/file$i.split", version = 1))
        stateWriter.writeState(
          currentVersion = 1,
          newFiles = initialFiles,
          removedPaths = Set.empty
        )

        // Write state with removals (5 files = 2.5% tombstone ratio, below 10% threshold)
        val removedPaths = (1 to 5).map(i => s"splits/file$i.split").toSet
        val stateDir2 = stateWriter.writeState(
          currentVersion = 2,
          newFiles = Seq.empty,
          removedPaths = removedPaths
        )

        // Verify manifest has tombstones (incremental write, not compacted)
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir2)

        manifest.stateVersion shouldBe 2
        manifest.numFiles shouldBe 195 // 200 - 5
        manifest.tombstones should have size 5
        manifest.tombstones should contain allElementsOf removedPaths
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("writeState should trigger compaction when tombstone ratio exceeds threshold") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Write initial state with 100 files
        val initialFiles = (1 to 100).map(i => createTestFileEntry(s"splits/file$i.split", version = 1))
        stateWriter.writeState(
          currentVersion = 1,
          newFiles = initialFiles,
          removedPaths = Set.empty
        )

        // Remove 20 files (20% tombstone ratio exceeds 10% threshold)
        val removedPaths = (1 to 20).map(i => s"splits/file$i.split").toSet
        val stateDir2 = stateWriter.writeState(
          currentVersion = 2,
          newFiles = Seq.empty,
          removedPaths = removedPaths
        )

        // After compaction, tombstones should be empty
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir2)

        manifest.stateVersion shouldBe 2
        manifest.numFiles shouldBe 80 // 100 - 20
        manifest.tombstones shouldBe empty // Compacted - no tombstones

        // Verify files can be read back correctly
        val manifestReader = AvroManifestReader(cloudProvider)
        val manifestPaths = manifest.manifests.map(m => s"$stateDir2/${m.path}")
        val readFiles = manifestReader.readManifestsParallel(manifestPaths)

        readFiles should have size 80
        readFiles.map(_.path) should not contain "splits/file1.split"
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("writeState should force compaction when requested") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Write initial state
        val initialFiles = (1 to 50).map(i => createTestFileEntry(s"splits/file$i.split", version = 1))
        stateWriter.writeState(
          currentVersion = 1,
          newFiles = initialFiles,
          removedPaths = Set.empty
        )

        // Add a few files and remove one (below threshold)
        val newFiles = (51 to 55).map(i => createTestFileEntry(s"splits/file$i.split", version = 2))
        val stateDir2 = stateWriter.writeState(
          currentVersion = 2,
          newFiles = newFiles,
          removedPaths = Set("splits/file1.split"),
          forceCompaction = true // Force compaction even below threshold
        )

        // Should be compacted (no tombstones)
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir2)

        manifest.tombstones shouldBe empty
        manifest.numFiles shouldBe 54 // 50 + 5 - 1
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("writeCompactedStateFromFiles should sort by partition values") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Create files with various partition values (out of order)
        val files = Seq(
          createTestFileEntry("f1.split", partitionValues = Map("date" -> "2024-01-15")),
          createTestFileEntry("f2.split", partitionValues = Map("date" -> "2024-01-01")),
          createTestFileEntry("f3.split", partitionValues = Map("date" -> "2024-01-10")),
          createTestFileEntry("f4.split", partitionValues = Map("date" -> "2024-01-05"))
        )

        val stateDir = s"$tempPath/state-v00000000000000000001"
        stateWriter.writeCompactedStateFromFiles(stateDir, files, 1, Map.empty)

        // Read back and verify sorted order
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir)

        val manifestReader = AvroManifestReader(cloudProvider)
        val manifestPaths = manifest.manifests.map(m => s"$stateDir/${m.path}")
        val readFiles = manifestReader.readManifestsParallel(manifestPaths)

        readFiles should have size 4
        // Files should be sorted by partition value
        val dates = readFiles.map(_.partitionValues("date"))
        dates shouldBe Seq("2024-01-01", "2024-01-05", "2024-01-10", "2024-01-15")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("writeCompactedStateFromFiles should compute partition bounds") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        val files = Seq(
          createTestFileEntry("f1.split", partitionValues = Map("date" -> "2024-01-01", "region" -> "us-east")),
          createTestFileEntry("f2.split", partitionValues = Map("date" -> "2024-01-15", "region" -> "us-west")),
          createTestFileEntry("f3.split", partitionValues = Map("date" -> "2024-01-10", "region" -> "eu-west"))
        )

        val stateDir = s"$tempPath/state-v00000000000000000001"
        stateWriter.writeCompactedStateFromFiles(stateDir, files, 1, Map.empty)

        // Verify partition bounds in manifest
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir)

        manifest.manifests should not be empty
        manifest.manifests.head.partitionBounds shouldBe defined

        val bounds = manifest.manifests.head.partitionBounds.get
        bounds should contain key "date"
        bounds("date").min shouldBe Some("2024-01-01")
        bounds("date").max shouldBe Some("2024-01-15")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("writeState should preserve schema registry") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        val files = (1 to 10).map(i => createTestFileEntry(s"splits/file$i.split", version = 1))
        val schemaRegistry = Map(
          "_schema_abc123" -> """{"fields":[{"name":"id","type":"int"}]}""",
          "_schema_def456" -> """{"fields":[{"name":"name","type":"string"}]}"""
        )

        val stateDir = stateWriter.writeState(
          currentVersion = 1,
          newFiles = files,
          removedPaths = Set.empty,
          schemaRegistry = schemaRegistry
        )

        // Verify schema registry preserved
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir)

        manifest.schemaRegistry should have size 2
        manifest.schemaRegistry should contain key "_schema_abc123"
        manifest.schemaRegistry should contain key "_schema_def456"
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("needsCompaction should return true when tombstone ratio exceeds threshold") {
    val cloudProvider = CloudStorageProviderFactory.createProvider(
      "/tmp/unused",
      new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
      spark.sparkContext.hadoopConfiguration
    )

    try {
      val stateWriter = StateWriter(cloudProvider, "/tmp/unused")

      val manifest = StateManifest(
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

      // 15 removes on 100 files = 15% tombstone ratio (exceeds 10% threshold)
      stateWriter.needsCompaction(manifest, 15) shouldBe true

      // 5 removes on 100 files = 5% tombstone ratio (below threshold)
      stateWriter.needsCompaction(manifest, 5) shouldBe false
    } finally {
      cloudProvider.close()
    }
  }

  test("needsCompaction should return true for large remove operations") {
    val cloudProvider = CloudStorageProviderFactory.createProvider(
      "/tmp/unused",
      new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
      spark.sparkContext.hadoopConfiguration
    )

    try {
      val stateWriter = StateWriter(cloudProvider, "/tmp/unused")

      val manifest = StateManifest(
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

      // 1001 removes triggers compaction regardless of ratio
      stateWriter.needsCompaction(manifest, 1001) shouldBe true

      // 999 removes (below 1000 threshold, and ratio is low)
      stateWriter.needsCompaction(manifest, 999) shouldBe false
    } finally {
      cloudProvider.close()
    }
  }

  test("convertAddActionsToFileEntries should convert correctly") {
    val cloudProvider = CloudStorageProviderFactory.createProvider(
      "/tmp/unused",
      new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
      spark.sparkContext.hadoopConfiguration
    )

    try {
      val stateWriter = StateWriter(cloudProvider, "/tmp/unused")

      val adds = Seq(
        io.indextables.spark.transaction.AddAction(
          path = "splits/test.split",
          partitionValues = Map("date" -> "2024-01-01"),
          size = 1000L,
          modificationTime = 1705123456789L,
          dataChange = true,
          numRecords = Some(100L)
        )
      )

      val entries = stateWriter.convertAddActionsToFileEntries(adds, version = 42, timestamp = 1705123456789L)

      entries should have size 1
      entries.head.path shouldBe "splits/test.split"
      entries.head.partitionValues shouldBe Map("date" -> "2024-01-01")
      entries.head.size shouldBe 1000L
      entries.head.addedAtVersion shouldBe 42
      entries.head.addedAtTimestamp shouldBe 1705123456789L
    } finally {
      cloudProvider.close()
    }
  }

  test("writeState should handle large number of files efficiently") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Create 10,000 files
        val files = (1 to 10000).map(i => createTestFileEntry(s"splits/file$i.split", version = 1))

        val startTime = System.currentTimeMillis()
        val stateDir = stateWriter.writeState(
          currentVersion = 1,
          newFiles = files,
          removedPaths = Set.empty
        )
        val duration = System.currentTimeMillis() - startTime

        // Should complete in reasonable time (< 5 seconds)
        duration should be < 5000L

        // Verify manifest
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir)

        manifest.numFiles shouldBe 10000
        // With default 50K entries per manifest, should be 1 manifest
        manifest.manifests.size should be >= 1
      } finally {
        cloudProvider.close()
      }
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
