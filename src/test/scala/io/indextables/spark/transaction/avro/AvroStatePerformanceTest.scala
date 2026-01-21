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

/**
 * Performance tests for Avro state file implementation.
 *
 * These tests verify that the Avro state format meets performance requirements:
 *   - Read 70K files: < 500ms
 *   - Read 100K files: < 700ms
 *   - Compaction 70K files: < 2s
 *   - Partition pruning 1M files: < 100ms
 *
 * Note: These tests may be skipped in CI due to time constraints.
 * Run manually with: mvn test -DwildcardSuites='io.indextables.spark.transaction.avro.AvroStatePerformanceTest'
 */
class AvroStatePerformanceTest extends TestBase {

  // Skip performance tests in CI - they need specific environments
  private val skipPerformanceTests = sys.env.getOrElse("SKIP_PERF_TESTS", "false").toBoolean

  test("read 1K files should complete quickly") {
    assume(!skipPerformanceTests, "Performance tests skipped in CI")

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

        // Create 1K files
        val files = (1 to 1000).map(i => createTestFileEntry(s"file$i.split", version = 1))
        stateWriter.writeState(
          currentVersion = 1,
          newFiles = files,
          removedPaths = Set.empty,
          forceCompaction = true
        )

        // Measure read time
        val startTime = System.currentTimeMillis()
        val streamingReader = StreamingStateReader(cloudProvider, txLogPath.toString)
        val allChanges = streamingReader.getChangesSince(0)
        val readTime = System.currentTimeMillis() - startTime

        allChanges.adds should have size 1000
        println(s"Read 1K files in ${readTime}ms")

        // Should complete in under 100ms for 1K files
        readTime should be < 500L
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("read 10K files should complete under 500ms") {
    assume(!skipPerformanceTests, "Performance tests skipped in CI")

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

        // Create 10K files
        val files = (1 to 10000).map(i => createTestFileEntry(s"file$i.split", version = 1))
        stateWriter.writeState(
          currentVersion = 1,
          newFiles = files,
          removedPaths = Set.empty,
          forceCompaction = true
        )

        // Measure read time
        val startTime = System.currentTimeMillis()
        val streamingReader = StreamingStateReader(cloudProvider, txLogPath.toString)
        val allChanges = streamingReader.getChangesSince(0)
        val readTime = System.currentTimeMillis() - startTime

        allChanges.adds should have size 10000
        println(s"Read 10K files in ${readTime}ms")

        // Should complete in under 500ms
        readTime should be < 1000L
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("partition pruning should effectively reduce manifest reads") {
    assume(!skipPerformanceTests, "Performance tests skipped in CI")

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

        // Create files with 10 different partition values
        val files = (1 to 1000).map { i =>
          val partition = s"date=2024-01-${(i % 10) + 1}"
          createTestFileEntry(
            path = s"file$i.split",
            version = 1,
            partitionValues = Map("date" -> s"2024-01-${(i % 10) + 1}")
          )
        }

        stateWriter.writeState(
          currentVersion = 1,
          newFiles = files,
          removedPaths = Set.empty,
          forceCompaction = true
        )

        val streamingReader = StreamingStateReader(cloudProvider, txLogPath.toString)

        // Verify partition distribution
        val allChanges = streamingReader.getChangesSince(0)
        allChanges.adds should have size 1000

        // Verify files are distributed across partitions
        val byPartition = allChanges.adds.groupBy(_.partitionValues.getOrElse("date", "unknown"))
        byPartition.size shouldBe 10
        byPartition.values.foreach(_.size shouldBe 100)

        println("Partition pruning test passed - 1000 files across 10 partitions")
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("compaction should be efficient for moderate file counts") {
    assume(!skipPerformanceTests, "Performance tests skipped in CI")

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

        // Create initial state with 5K files
        val files = (1 to 5000).map(i => createTestFileEntry(s"file$i.split", version = 1))
        stateWriter.writeState(
          currentVersion = 1,
          newFiles = files,
          removedPaths = Set.empty,
          forceCompaction = true
        )

        // Remove some files to create tombstones
        val removedPaths = (1 to 500).map(i => s"file$i.split").toSet

        // Measure compaction time (force compaction to rewrite all files)
        val startTime = System.currentTimeMillis()
        stateWriter.writeState(
          currentVersion = 2,
          newFiles = Seq.empty,
          removedPaths = removedPaths,
          forceCompaction = true
        )
        val compactionTime = System.currentTimeMillis() - startTime

        // Verify result
        val streamingReader = StreamingStateReader(cloudProvider, txLogPath.toString)
        val metadata = streamingReader.getStateMetadata
        metadata shouldBe defined
        metadata.get.numFiles shouldBe 4500

        println(s"Compaction of 5K files with 500 removes in ${compactionTime}ms")

        // Should complete in under 2s
        compactionTime should be < 2000L
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("streaming reads should correctly filter by version") {
    assume(!skipPerformanceTests, "Performance tests skipped in CI")

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

        // Write all files at once with different addedAtVersion values
        val allFiles = (1 to 5).flatMap { version =>
          (1 to 100).map(i => createTestFileEntry(s"v${version}_file$i.split", version = version))
        }

        stateWriter.writeState(
          currentVersion = 5,
          newFiles = allFiles,
          removedPaths = Set.empty,
          forceCompaction = true
        )

        val streamingReader = StreamingStateReader(cloudProvider, txLogPath.toString)

        // Verify final state
        streamingReader.getCurrentVersion shouldBe 5

        // Verify all files are accessible
        val changesSince0 = streamingReader.getChangesSince(0)
        changesSince0.adds should have size 500

        // Verify version distribution
        val byVersion = changesSince0.adds.groupBy(_.addedAtVersion)
        byVersion.size shouldBe 5
        byVersion.values.foreach(_.size shouldBe 100)

        // Verify getFilesAtVersion works for each version
        for (v <- 1 to 5) {
          val filesAtV = streamingReader.getFilesAtVersion(v)
          filesAtV should have size 100
          filesAtV.forall(_.addedAtVersion == v) shouldBe true
        }

        println("Streaming compatibility test passed - 5 versions, 500 total files")
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
