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

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.{LastCheckpointInfo, ProtocolAction, ProtocolVersion}
import io.indextables.spark.TestBase

class MigrationTest extends TestBase {

  test("Protocol V4 should include avroState feature") {
    val v4Protocol = ProtocolVersion.v4Protocol()
    v4Protocol.minReaderVersion shouldBe 4
    v4Protocol.minWriterVersion shouldBe 4
    v4Protocol.readerFeatures shouldBe defined
    v4Protocol.readerFeatures.get should contain(ProtocolVersion.FEATURE_AVRO_STATE)
    v4Protocol.writerFeatures shouldBe defined
    v4Protocol.writerFeatures.get should contain(ProtocolVersion.FEATURE_AVRO_STATE)
  }

  test("usesV4Features should return true for V4 protocol") {
    val v4Protocol = ProtocolAction(minReaderVersion = 4, minWriterVersion = 4)
    ProtocolVersion.usesV4Features(v4Protocol) shouldBe true

    val v3Protocol = ProtocolAction(minReaderVersion = 3, minWriterVersion = 3)
    ProtocolVersion.usesV4Features(v3Protocol) shouldBe false

    val v2Protocol = ProtocolAction(minReaderVersion = 2, minWriterVersion = 2)
    ProtocolVersion.usesV4Features(v2Protocol) shouldBe false
  }

  test("requiresV4Reader should return true for avroState feature") {
    ProtocolVersion.requiresV4Reader("avroState") shouldBe true
    ProtocolVersion.requiresV4Reader("schemaDeduplication") shouldBe false
    ProtocolVersion.requiresV4Reader("multiPartCheckpoint") shouldBe false
  }

  test("getMinReaderVersionForFeatures should return V4 for avroState") {
    val v4Features = Set("avroState")
    ProtocolVersion.getMinReaderVersionForFeatures(v4Features) shouldBe 4

    val v3Features = Set("multiPartCheckpoint")
    ProtocolVersion.getMinReaderVersionForFeatures(v3Features) shouldBe 3

    val mixedFeatures = Set("avroState", "multiPartCheckpoint")
    ProtocolVersion.getMinReaderVersionForFeatures(mixedFeatures) shouldBe 4

    val noFeatures = Set.empty[String]
    ProtocolVersion.getMinReaderVersionForFeatures(noFeatures) shouldBe 2
  }

  test("SUPPORTED_READER_FEATURES should include avroState") {
    ProtocolVersion.SUPPORTED_READER_FEATURES should contain("avroState")
  }

  test("SUPPORTED_WRITER_FEATURES should include avroState") {
    ProtocolVersion.SUPPORTED_WRITER_FEATURES should contain("avroState")
  }

  test("migration from JSON to Avro state should preserve all files") {
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
        // This avoids incremental write issues with manifest path resolution
        val files1 = (1 to 50).map(i => createTestFileEntry(s"file$i.split", version = 1))
        val files2 = (51 to 60).map(i => createTestFileEntry(s"file$i.split", version = 2))

        stateWriter.writeState(
          currentVersion = 2,
          newFiles = files1 ++ files2,
          removedPaths = Set.empty,
          forceCompaction = true
        )

        // Read back and verify
        val streamingReader = StreamingStateReader(cloudProvider, txLogPath.toString)
        streamingReader.getCurrentVersion shouldBe 2

        val metadata = streamingReader.getStateMetadata
        metadata shouldBe defined
        metadata.get.numFiles shouldBe 60

        // Verify all files are accessible
        val allChanges = streamingReader.getChangesSince(0)
        allChanges.adds should have size 60

        // Verify files at different versions are retrievable
        val v1Files = streamingReader.getFilesAtVersion(1)
        v1Files should have size 50

        val v2Files = streamingReader.getFilesAtVersion(2)
        v2Files should have size 10
      } finally
        cloudProvider.close()
    }
  }

  test("migration should preserve partition values") {
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

        // Create files with different partition values
        val files = Seq(
          createTestFileEntry("file1.split", version = 1, Map("date" -> "2024-01-01")),
          createTestFileEntry("file2.split", version = 1, Map("date" -> "2024-01-01")),
          createTestFileEntry("file3.split", version = 1, Map("date" -> "2024-01-02")),
          createTestFileEntry("file4.split", version = 1, Map("date" -> "2024-01-02"))
        )

        stateWriter.writeState(
          currentVersion = 1,
          newFiles = files,
          removedPaths = Set.empty
        )

        // Read back and verify partition values
        val streamingReader = StreamingStateReader(cloudProvider, txLogPath.toString)
        val allFiles        = streamingReader.getFilesAtVersion(1)

        allFiles should have size 4
        allFiles.filter(_.partitionValues.get("date").contains("2024-01-01")) should have size 2
        allFiles.filter(_.partitionValues.get("date").contains("2024-01-02")) should have size 2
      } finally
        cloudProvider.close()
    }
  }

  // Helper methods

  private def createTestFileEntry(
    path: String,
    version: Long = 1,
    partitionValues: Map[String, String] = Map.empty
  ): FileEntry =
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
