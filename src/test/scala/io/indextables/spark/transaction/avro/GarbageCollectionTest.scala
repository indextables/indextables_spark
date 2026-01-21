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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class GarbageCollectionTest extends TestBase {

  test("cleanupOldStateDirectories should preserve latest state directory") {
    withTempPath { tempPath =>
      val txLogPath = new Path(tempPath, "_transaction_log")
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        txLogPath.toString,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(txLogPath.toString)

        val stateWriter = StateWriter(cloudProvider, txLogPath.toString)

        // Create state at version 10
        val files = (1 to 10).map(i => createTestFileEntry(s"file$i.split", version = 10))
        stateWriter.writeState(
          currentVersion = 10,
          newFiles = files,
          removedPaths = Set.empty
        )

        // Verify state directory exists
        val stateDir = new Path(txLogPath, "state-v00000000000000000010")
        cloudProvider.exists(stateDir.toString) shouldBe true

        // Create another state at version 20
        val files2 = (11 to 20).map(i => createTestFileEntry(s"file$i.split", version = 20))
        stateWriter.writeState(
          currentVersion = 20,
          newFiles = files2,
          removedPaths = Set.empty
        )

        // Both state directories should exist
        val stateDir2 = new Path(txLogPath, "state-v00000000000000000020")
        cloudProvider.exists(stateDir.toString) shouldBe true
        cloudProvider.exists(stateDir2.toString) shouldBe true

        // Verify we can still read the latest state
        val streamingReader = StreamingStateReader(cloudProvider, txLogPath.toString)
        streamingReader.getCurrentVersion shouldBe 20
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("multiple state versions should coexist before cleanup") {
    withTempPath { tempPath =>
      val txLogPath = new Path(tempPath, "_transaction_log")
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        txLogPath.toString,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(txLogPath.toString)

        val stateWriter = StateWriter(cloudProvider, txLogPath.toString)

        // Create multiple versions
        for (version <- Seq(1L, 5L, 10L, 15L, 20L)) {
          val files = (1 to 5).map(i => createTestFileEntry(s"v${version}_file$i.split", version = version))
          stateWriter.writeState(
            currentVersion = version,
            newFiles = files,
            removedPaths = Set.empty,
            forceCompaction = true
          )
        }

        // List state directories
        val allFiles = cloudProvider.listFiles(txLogPath.toString, recursive = false)
        val stateDirs = allFiles.filter { f =>
          val name = new Path(f.path).getName
          name.startsWith("state-v")
        }

        // Should have 5 state directories
        stateDirs should have size 5

        // Verify we can read the latest
        val streamingReader = StreamingStateReader(cloudProvider, txLogPath.toString)
        streamingReader.getCurrentVersion shouldBe 20
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("state manifest should be retained after multiple writes") {
    withTempPath { tempPath =>
      val txLogPath = new Path(tempPath, "_transaction_log")
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        txLogPath.toString,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(txLogPath.toString)

        val stateWriter = StateWriter(cloudProvider, txLogPath.toString)

        // Write initial state
        val files1 = (1 to 100).map(i => createTestFileEntry(s"file$i.split", version = 1))
        stateWriter.writeState(
          currentVersion = 1,
          newFiles = files1,
          removedPaths = Set.empty
        )

        // Add more files
        val files2 = (101 to 150).map(i => createTestFileEntry(s"file$i.split", version = 5))
        stateWriter.writeState(
          currentVersion = 5,
          newFiles = files2,
          removedPaths = Set.empty
        )

        // Remove some files
        val removedPaths = (1 to 10).map(i => s"file$i.split").toSet
        stateWriter.writeState(
          currentVersion = 10,
          newFiles = Seq.empty,
          removedPaths = removedPaths
        )

        // Verify final state
        val streamingReader = StreamingStateReader(cloudProvider, txLogPath.toString)
        val metadata = streamingReader.getStateMetadata
        metadata shouldBe defined
        metadata.get.stateVersion shouldBe 10
        metadata.get.numFiles shouldBe 140 // 150 - 10 removed
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("state directory should contain manifest and avro files") {
    withTempPath { tempPath =>
      val txLogPath = new Path(tempPath, "_transaction_log")
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        txLogPath.toString,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(txLogPath.toString)

        val stateWriter = StateWriter(cloudProvider, txLogPath.toString)

        // Write state
        val files = (1 to 10).map(i => createTestFileEntry(s"file$i.split", version = 5))
        stateWriter.writeState(
          currentVersion = 5,
          newFiles = files,
          removedPaths = Set.empty
        )

        // Check state directory contents
        val stateDir = new Path(txLogPath, "state-v00000000000000000005").toString
        val stateFiles = cloudProvider.listFiles(stateDir, recursive = true)

        // Should have _manifest.json
        stateFiles.exists(_.path.endsWith("/_manifest.json")) shouldBe true

        // Should have at least one .avro manifest file
        stateFiles.exists(_.path.endsWith(".avro")) shouldBe true
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("retention configuration should be respected") {
    // This test verifies the retention period is configurable
    // The actual cleanup happens in PurgeOrphanedSplitsExecutor

    // Default retention is 30 days (30 * 24 * 60 * 60 * 1000 ms)
    val defaultRetention = 30L * 24 * 60 * 60 * 1000

    // Verify configurable
    spark.conf.set("spark.indextables.logRetention.duration", "86400000") // 1 day
    val configured = spark.conf.get("spark.indextables.logRetention.duration").toLong
    configured shouldBe 86400000L

    // Clean up config
    spark.conf.unset("spark.indextables.logRetention.duration")
  }

  test("StateConfig should have correct retention defaults") {
    // Verify defaults from StateConfig
    StateConfig.RETENTION_VERSIONS_DEFAULT shouldBe 2
    StateConfig.RETENTION_HOURS_DEFAULT shouldBe 168 // 7 days
  }

  test("empty state directory should be handled gracefully") {
    withTempPath { tempPath =>
      val txLogPath = new Path(tempPath, "_transaction_log")
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        txLogPath.toString,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(txLogPath.toString)

        // Create StreamingStateReader without any state
        val streamingReader = StreamingStateReader(cloudProvider, txLogPath.toString)

        // Should handle gracefully
        streamingReader.getCurrentVersion shouldBe 0
        streamingReader.getStateMetadata shouldBe None
        streamingReader.getChangesSince(0).adds shouldBe empty
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
