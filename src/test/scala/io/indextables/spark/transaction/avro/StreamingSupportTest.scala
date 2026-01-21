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

class StreamingSupportTest extends TestBase {

  test("getChangesSince should return empty for version at or after current") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Write initial state at version 5
        val files = (1 to 10).map(i => createTestFileEntry(s"file$i.split", version = 5))
        stateWriter.writeState(
          currentVersion = 5,
          newFiles = files,
          removedPaths = Set.empty
        )

        val streamingReader = StreamingStateReader(cloudProvider, tempPath)

        // Request changes since version 5 (current version) - should be empty
        val changes = streamingReader.getChangesSince(5)
        changes.adds shouldBe empty

        // Request changes since version 10 (future version) - should also be empty
        val futureChanges = streamingReader.getChangesSince(10)
        futureChanges.adds shouldBe empty
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("getChangesSince should return files added after specified version") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Write all files at once with different addedAtVersion values
        // This avoids incremental write issues with manifest path resolution
        val files1 = (1 to 10).map(i => createTestFileEntry(s"file$i.split", version = 1))
        val files2 = (11 to 15).map(i => createTestFileEntry(s"file$i.split", version = 5))

        stateWriter.writeState(
          currentVersion = 5,
          newFiles = files1 ++ files2,
          removedPaths = Set.empty,
          forceCompaction = true
        )

        val streamingReader = StreamingStateReader(cloudProvider, tempPath)

        // Get changes since version 1 - should return files from version 5
        val changes = streamingReader.getChangesSince(1)
        changes.adds should have size 5
        changes.adds.map(_.path) should contain allOf(
          "file11.split", "file12.split", "file13.split", "file14.split", "file15.split"
        )
        changes.newVersion shouldBe 5

        // Get changes since version 0 - should return all files
        val allChanges = streamingReader.getChangesSince(0)
        allChanges.adds should have size 15
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("getCurrentVersion should return correct version") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)
        val streamingReader = StreamingStateReader(cloudProvider, tempPath)

        // No state yet
        streamingReader.getCurrentVersion shouldBe 0

        // Write state at version 42
        val files = (1 to 5).map(i => createTestFileEntry(s"file$i.split", version = 42))
        stateWriter.writeState(
          currentVersion = 42,
          newFiles = files,
          removedPaths = Set.empty
        )

        streamingReader.getCurrentVersion shouldBe 42
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("getFilesAtVersion should return only files added at specific version") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Write all files at once with different addedAtVersion values
        // This avoids incremental write issues with manifest path resolution
        val allFiles = Seq(
          (1 to 5).map(i => createTestFileEntry(s"file$i.split", version = 1)),
          (6 to 10).map(i => createTestFileEntry(s"file$i.split", version = 5)),
          (11 to 15).map(i => createTestFileEntry(s"file$i.split", version = 10))
        ).flatten

        stateWriter.writeState(
          currentVersion = 10,
          newFiles = allFiles,
          removedPaths = Set.empty,
          forceCompaction = true
        )

        val streamingReader = StreamingStateReader(cloudProvider, tempPath)

        // Get files at version 5
        val filesAtV5 = streamingReader.getFilesAtVersion(5)
        filesAtV5 should have size 5
        filesAtV5.map(_.path) should contain allOf(
          "file6.split", "file7.split", "file8.split", "file9.split", "file10.split"
        )

        // Get files at version 1
        val filesAtV1 = streamingReader.getFilesAtVersion(1)
        filesAtV1 should have size 5

        // Get files at non-existent version
        val filesAtV99 = streamingReader.getFilesAtVersion(99)
        filesAtV99 shouldBe empty
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("getStateMetadata should return state manifest") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)
        val streamingReader = StreamingStateReader(cloudProvider, tempPath)

        // No state yet
        streamingReader.getStateMetadata shouldBe None

        // Write state
        val files = (1 to 100).map(i => createTestFileEntry(s"file$i.split", version = 42))
        stateWriter.writeState(
          currentVersion = 42,
          newFiles = files,
          removedPaths = Set.empty
        )

        val metadata = streamingReader.getStateMetadata
        metadata shouldBe defined
        metadata.get.stateVersion shouldBe 42
        metadata.get.numFiles shouldBe 100
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("getChangesBetween should return files in version range") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Write all files at once with different addedAtVersion values
        // This avoids incremental write issues with manifest path resolution
        val allFiles = Seq(1, 5, 10, 15).flatMap { v =>
          (1 to 5).map(i => createTestFileEntry(s"v${v}_file$i.split", version = v))
        }

        stateWriter.writeState(
          currentVersion = 15,
          newFiles = allFiles,
          removedPaths = Set.empty,
          forceCompaction = true
        )

        val streamingReader = StreamingStateReader(cloudProvider, tempPath)

        // Get changes between version 5 and 15 (exclusive-inclusive)
        val changes = streamingReader.getChangesBetween(5, 15)
        changes.adds should have size 10 // versions 10 and 15
        changes.adds.map(_.addedAtVersion).distinct should contain allOf(10L, 15L)
        changes.adds.map(_.addedAtVersion).distinct should not contain 5L

        // Get changes between version 0 and 5
        val earlyChanges = streamingReader.getChangesBetween(0, 5)
        earlyChanges.adds should have size 10 // versions 1 and 5
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("toAddActions should convert FileEntries to AddActions") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val streamingReader = StreamingStateReader(cloudProvider, tempPath)

        val entries = Seq(
          FileEntry(
            path = "test.split",
            partitionValues = Map("date" -> "2024-01-01"),
            size = 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            addedAtVersion = 5,
            addedAtTimestamp = System.currentTimeMillis()
          )
        )

        val addActions = streamingReader.toAddActions(entries)

        addActions should have size 1
        addActions.head.path shouldBe "test.split"
        addActions.head.partitionValues shouldBe Map("date" -> "2024-01-01")
        addActions.head.size shouldBe 1000L
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
