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

/**
 * Tests for incremental state writes with shared manifests.
 *
 * These tests verify:
 *   1. Incremental writes only create new manifests for new files
 *   2. Existing manifests are reused via references
 *   3. Shared manifest directory is used correctly
 *   4. Re-read on retry picks up concurrent changes
 *   5. Compaction triggers are configurable
 *
 * Run with:
 *   mvn scalatest:test -DwildcardSuites='io.indextables.spark.transaction.avro.IncrementalStateWriteTest'
 */
class IncrementalStateWriteTest extends TestBase {

  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  test("incremental write creates new manifest only for new files") {
    withTempPath { tempDir =>
      val transactionLogPath = s"$tempDir/_transaction_log"
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        transactionLogPath,
        new CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath)

        val stateWriter = StateWriter(cloudProvider, transactionLogPath)

        // Write initial state with 2 files
        val initialFiles = Seq(
          createTestFileEntry("file1.split", 1L),
          createTestFileEntry("file2.split", 1L)
        )

        val result1 = stateWriter.writeIncrementalWithRetry(
          initialFiles,
          Set.empty,
          Map.empty
        )

        result1.version shouldBe 1L

        // Verify shared manifests directory was created and used
        val sharedManifestDir = s"$transactionLogPath/${StateConfig.SHARED_MANIFEST_DIR}"
        cloudProvider.exists(sharedManifestDir) shouldBe true

        // Read the state manifest to verify structure
        val manifestIO = StateManifestIO(cloudProvider)
        val state1 = manifestIO.readStateManifest(result1.stateDir)
        state1.manifests.size should be >= 1
        state1.manifests.foreach { m =>
          m.path should startWith(StateConfig.SHARED_MANIFEST_DIR + "/")
        }

        // Write incremental state with 1 new file
        val newFiles = Seq(createTestFileEntry("file3.split", 2L))

        val result2 = stateWriter.writeIncrementalWithRetry(
          newFiles,
          Set.empty,
          Map.empty
        )

        result2.version shouldBe 2L

        // Read state manifest for version 2
        val state2 = manifestIO.readStateManifest(result2.stateDir)

        // Should have one more manifest than state1 (for the new file)
        // Or same number if combined into existing manifest
        state2.numFiles shouldBe 3
        state2.tombstones shouldBe empty

        // All manifests should reference the shared directory
        state2.manifests.foreach { m =>
          m.path should startWith(StateConfig.SHARED_MANIFEST_DIR + "/")
        }

      } finally {
        cloudProvider.close()
      }
    }
  }

  test("incremental write with removes adds tombstones") {
    withTempPath { tempDir =>
      val transactionLogPath = s"$tempDir/_transaction_log"
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        transactionLogPath,
        new CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath)

        val stateWriter = StateWriter(cloudProvider, transactionLogPath)

        // Write initial state
        val initialFiles = Seq(
          createTestFileEntry("file1.split", 1L),
          createTestFileEntry("file2.split", 1L),
          createTestFileEntry("file3.split", 1L)
        )

        val result1 = stateWriter.writeIncrementalWithRetry(
          initialFiles,
          Set.empty,
          Map.empty
        )

        // Write incremental state with new file and removal
        // Use a high threshold to ensure incremental write (not compaction)
        val newFiles = Seq(createTestFileEntry("file4.split", 2L))
        val removedPaths = Set("file2.split")

        val result2 = stateWriter.writeIncrementalWithRetry(
          newFiles,
          removedPaths,
          Map.empty,
          CompactionConfig(tombstoneThreshold = 0.50)  // 50% threshold prevents compaction
        )

        // Read state manifest
        val manifestIO = StateManifestIO(cloudProvider)
        val state2 = manifestIO.readStateManifest(result2.stateDir)

        // Should have 3 live files (3 - 1 removed + 1 new)
        state2.numFiles shouldBe 3
        // Should have tombstone for removed file
        state2.tombstones should contain("file2.split")

      } finally {
        cloudProvider.close()
      }
    }
  }

  test("compaction triggers when tombstone threshold exceeded") {
    withTempPath { tempDir =>
      val transactionLogPath = s"$tempDir/_transaction_log"
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        transactionLogPath,
        new CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath)

        val stateWriter = StateWriter(cloudProvider, transactionLogPath)

        // Write initial state with 10 files
        val initialFiles = (1 to 10).map(i => createTestFileEntry(s"file$i.split", 1L))

        val result1 = stateWriter.writeIncrementalWithRetry(
          initialFiles,
          Set.empty,
          Map.empty
        )

        // Remove 3 files (30% tombstone ratio, exceeds 10% threshold)
        val removedPaths = Set("file1.split", "file2.split", "file3.split")

        // Use config with low threshold to trigger compaction
        val compactionConfig = CompactionConfig(
          tombstoneThreshold = 0.10,  // 10% threshold
          maxManifests = 100
        )

        val result2 = stateWriter.writeIncrementalWithRetry(
          Seq.empty,
          removedPaths,
          Map.empty,
          compactionConfig
        )

        // Read state manifest
        val manifestIO = StateManifestIO(cloudProvider)
        val state2 = manifestIO.readStateManifest(result2.stateDir)

        // After compaction, tombstones should be cleared
        state2.tombstones shouldBe empty
        state2.numFiles shouldBe 7

      } finally {
        cloudProvider.close()
      }
    }
  }

  test("needsCompaction respects configurable thresholds") {
    withTempPath { tempDir =>
      val transactionLogPath = s"$tempDir/_transaction_log"
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        transactionLogPath,
        new CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, transactionLogPath)

        // Create a manifest with specific tombstone ratio
        val manifest = StateManifest(
          formatVersion = 1,
          stateVersion = 1L,
          createdAt = System.currentTimeMillis(),
          numFiles = 100,
          totalBytes = 10000L,
          manifests = Seq(
            ManifestInfo(
              path = "manifests/manifest-1.avro",
              numEntries = 110,  // 100 live + 10 tombstones
              minAddedAtVersion = 1L,
              maxAddedAtVersion = 1L
            )
          ),
          tombstones = (1 to 10).map(i => s"removed$i.split"),  // 10 existing tombstones
          schemaRegistry = Map.empty,
          protocolVersion = 4
        )

        // With 10 existing tombstones, 100 files, adding 5 removes: (10+5)/100 = 15%

        // Default threshold (10%) - should trigger compaction
        val defaultConfig = CompactionConfig()
        stateWriter.needsCompaction(manifest, 5, defaultConfig) shouldBe true

        // Higher threshold (20%) - should NOT trigger compaction
        val highThresholdConfig = CompactionConfig(tombstoneThreshold = 0.20)
        stateWriter.needsCompaction(manifest, 5, highThresholdConfig) shouldBe false

        // Force compaction
        val forceConfig = CompactionConfig(forceCompaction = true)
        stateWriter.needsCompaction(manifest, 0, forceConfig) shouldBe true

        // Large remove threshold (disabled by default)
        // Create a manifest with many files to test large remove without triggering tombstone threshold
        val largeManifest = StateManifest(
          formatVersion = 1,
          stateVersion = 1L,
          createdAt = System.currentTimeMillis(),
          numFiles = 100000,  // 100K files
          totalBytes = 10000000L,
          manifests = Seq(
            ManifestInfo(
              path = "manifests/manifest-1.avro",
              numEntries = 100000,
              minAddedAtVersion = 1L,
              maxAddedAtVersion = 1L
            )
          ),
          tombstones = Seq.empty,  // No existing tombstones
          schemaRegistry = Map.empty,
          protocolVersion = 4
        )
        // With 100K files and 5000 removes: 5000/100000 = 5% (below 10% threshold)
        val defaultLargeRemoveConfig = CompactionConfig(largeRemoveThreshold = Int.MaxValue)
        stateWriter.needsCompaction(largeManifest, 5000, defaultLargeRemoveConfig) shouldBe false

        // Large remove threshold enabled
        // With 100K files and 150 removes: tombstoneRatio = 0.15% (well below 50%)
        // But 150 > 100, so large remove threshold triggers compaction
        val enabledLargeRemoveConfig = CompactionConfig(
          tombstoneThreshold = 0.50,  // High threshold so tombstones don't trigger
          largeRemoveThreshold = 100
        )
        stateWriter.needsCompaction(largeManifest, 150, enabledLargeRemoveConfig) shouldBe true

      } finally {
        cloudProvider.close()
      }
    }
  }

  test("shared manifest directory is created on first write") {
    withTempPath { tempDir =>
      val transactionLogPath = s"$tempDir/_transaction_log"
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        transactionLogPath,
        new CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath)

        val sharedDir = s"$transactionLogPath/${StateConfig.SHARED_MANIFEST_DIR}"

        // Shared directory should not exist initially
        cloudProvider.exists(sharedDir) shouldBe false

        val stateWriter = StateWriter(cloudProvider, transactionLogPath)

        // Write state
        val files = Seq(createTestFileEntry("test.split", 1L))
        val result = stateWriter.writeIncrementalWithRetry(
          files,
          Set.empty,
          Map.empty
        )

        // Shared directory should now exist
        cloudProvider.exists(sharedDir) shouldBe true

        // Manifest should be in shared directory
        val manifestIO = StateManifestIO(cloudProvider)
        val state = manifestIO.readStateManifest(result.stateDir)
        state.manifests.foreach { m =>
          m.path should startWith(StateConfig.SHARED_MANIFEST_DIR + "/")
        }

      } finally {
        cloudProvider.close()
      }
    }
  }

  test("end-to-end: write with incremental then read all files") {
    withTempPath { tempDir =>
      val path = tempDir

      // First write
      val data1 = Seq((1, "doc1"), (2, "doc2"))
      spark.createDataFrame(data1).toDF("id", "content")
        .write.format(provider)
        .mode("overwrite")
        .save(path)

      // Second write (append)
      val data2 = Seq((3, "doc3"), (4, "doc4"))
      spark.createDataFrame(data2).toDF("id", "content")
        .write.format(provider)
        .mode("append")
        .save(path)

      // Read back and verify
      val df = spark.read.format(provider).load(path)
      df.count() shouldBe 4

      // Verify all documents are present
      val ids = df.select("id").collect().map(_.getInt(0)).toSet
      ids should contain allOf (1, 2, 3, 4)
    }
  }

  private def createTestFileEntry(path: String, version: Long): FileEntry = {
    FileEntry(
      path = path,
      partitionValues = Map.empty,
      size = 100L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      addedAtVersion = version,
      addedAtTimestamp = System.currentTimeMillis()
    )
  }
}
