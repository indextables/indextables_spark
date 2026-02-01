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

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.AddAction
import io.indextables.spark.TestBase

class AvroManifestWriterTest extends TestBase {

  test("writeToBytes should write empty manifest") {
    val writer = new AvroManifestWriter(null)
    val bytes  = writer.writeToBytes(Seq.empty)

    bytes should not be empty

    // Read back and verify
    val reader = new AvroManifestReader(null)
    val result = reader.readFromBytes(bytes)

    result shouldBe empty
  }

  test("writeToBytes should write single entry") {
    val writer = new AvroManifestWriter(null)
    val entry  = createTestFileEntry("splits/file1.split")
    val bytes  = writer.writeToBytes(Seq(entry))

    // Read back and verify
    val reader = new AvroManifestReader(null)
    val result = reader.readFromBytes(bytes)

    result should have size 1
    result.head.path shouldBe "splits/file1.split"
  }

  test("writeToBytes should write multiple entries") {
    val writer  = new AvroManifestWriter(null)
    val entries = (1 to 100).map(i => createTestFileEntry(s"splits/file$i.split", version = i))
    val bytes   = writer.writeToBytes(entries)

    // Read back and verify
    val reader = new AvroManifestReader(null)
    val result = reader.readFromBytes(bytes)

    result should have size 100
  }

  test("writeToBytes should preserve all fields in round-trip") {
    val writer = new AvroManifestWriter(null)
    val entry = FileEntry(
      path = "splits/part=2024-01-01/file.split",
      partitionValues = Map("date" -> "2024-01-01", "region" -> "us-east"),
      size = 123456789L,
      modificationTime = 1705123456789L,
      dataChange = true,
      stats = Some("""{"numRecords": 1000}"""),
      minValues = Some(Map("id" -> "1", "name" -> "Alice")),
      maxValues = Some(Map("id" -> "1000", "name" -> "Zara")),
      numRecords = Some(1000L),
      footerStartOffset = Some(100000L),
      footerEndOffset = Some(123000L),
      hasFooterOffsets = true,
      splitTags = Some(Set("tag1", "tag2")),
      numMergeOps = Some(3),
      docMappingRef = Some("schema_abc123"),
      uncompressedSizeBytes = Some(500000000L),
      addedAtVersion = 42L,
      addedAtTimestamp = 1705123456789L
    )

    val bytes = writer.writeToBytes(Seq(entry))

    // Read back and verify all fields
    val reader = new AvroManifestReader(null)
    val result = reader.readFromBytes(bytes)

    result should have size 1
    val read = result.head

    read.path shouldBe entry.path
    read.partitionValues shouldBe entry.partitionValues
    read.size shouldBe entry.size
    read.modificationTime shouldBe entry.modificationTime
    read.dataChange shouldBe entry.dataChange
    read.stats shouldBe entry.stats
    read.minValues shouldBe entry.minValues
    read.maxValues shouldBe entry.maxValues
    read.numRecords shouldBe entry.numRecords
    read.footerStartOffset shouldBe entry.footerStartOffset
    read.footerEndOffset shouldBe entry.footerEndOffset
    read.hasFooterOffsets shouldBe entry.hasFooterOffsets
    read.splitTags shouldBe entry.splitTags
    read.numMergeOps shouldBe entry.numMergeOps
    read.docMappingRef shouldBe entry.docMappingRef
    read.uncompressedSizeBytes shouldBe entry.uncompressedSizeBytes
    read.addedAtVersion shouldBe entry.addedAtVersion
    read.addedAtTimestamp shouldBe entry.addedAtTimestamp
  }

  test("writeToBytes with zstd compression should produce smaller output") {
    val writer  = new AvroManifestWriter(null)
    val entries = (1 to 1000).map(i => createTestFileEntry(s"splits/file$i.split", version = i))

    val uncompressedBytes = writer.writeToBytes(entries, compression = "none")
    val compressedBytes   = writer.writeToBytes(entries, compression = "zstd")

    // Compressed should be smaller
    compressedBytes.length should be < uncompressedBytes.length

    // Both should be readable
    val reader = new AvroManifestReader(null)
    reader.readFromBytes(uncompressedBytes) should have size 1000
    reader.readFromBytes(compressedBytes) should have size 1000
  }

  test("writeToBytes with snappy compression should work") {
    val writer  = new AvroManifestWriter(null)
    val entries = (1 to 100).map(i => createTestFileEntry(s"splits/file$i.split", version = i))

    val bytes = writer.writeToBytes(entries, compression = "snappy")

    // Should be readable
    val reader = new AvroManifestReader(null)
    val result = reader.readFromBytes(bytes)

    result should have size 100
  }

  test("writeManifest should write to cloud provider") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val writer  = new AvroManifestWriter(cloudProvider)
        val entries = (1 to 50).map(i => createTestFileEntry(s"splits/file$i.split", version = i))

        val manifestPath = s"$tempPath/manifest-test.avro"
        val bytesWritten = writer.writeManifest(manifestPath, entries)

        bytesWritten should be > 0L
        cloudProvider.exists(manifestPath) shouldBe true

        // Read back and verify
        val reader = new AvroManifestReader(cloudProvider)
        val result = reader.readManifest(manifestPath)

        result should have size 50
      } finally
        cloudProvider.close()
    }
  }

  test("writeManifestIfNotExists should not overwrite existing file") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val writer       = new AvroManifestWriter(cloudProvider)
        val manifestPath = s"$tempPath/manifest-test.avro"

        // First write should succeed
        val entries1 = Seq(createTestFileEntry("file1.split"))
        val result1  = writer.writeManifestIfNotExists(manifestPath, entries1)
        result1 shouldBe defined

        // Second write should fail (file exists)
        val entries2 = Seq(createTestFileEntry("file2.split"))
        val result2  = writer.writeManifestIfNotExists(manifestPath, entries2)
        result2 shouldBe None

        // Read back should have first entry
        val reader = new AvroManifestReader(cloudProvider)
        val result = reader.readManifest(manifestPath)

        result should have size 1
        result.head.path shouldBe "file1.split"
      } finally
        cloudProvider.close()
    }
  }

  test("convertFromAddAction should convert AddAction to FileEntry") {
    val writer = new AvroManifestWriter(null)
    val add    = createTestAddAction("splits/test.split")

    val entry = writer.convertFromAddAction(add, version = 42, timestamp = 1705123456789L)

    entry.path shouldBe add.path
    entry.partitionValues shouldBe add.partitionValues
    entry.size shouldBe add.size
    entry.modificationTime shouldBe add.modificationTime
    entry.dataChange shouldBe add.dataChange
    entry.addedAtVersion shouldBe 42
    entry.addedAtTimestamp shouldBe 1705123456789L
  }

  test("generateManifestId should produce unique 8-character hex strings") {
    val writer = new AvroManifestWriter(null)

    val ids = (1 to 100).map(_ => writer.generateManifestId())

    // All IDs should be 8 characters
    ids.foreach(_.length shouldBe 8)

    // All IDs should be unique
    ids.toSet.size shouldBe 100

    // All IDs should be hex characters
    ids.foreach(id => id.forall(c => c.isDigit || ('a' to 'f').contains(c.toLower)) shouldBe true)
  }

  test("computePartitionBounds should compute correct bounds") {
    val writer = new AvroManifestWriter(null)
    val entries = Seq(
      createTestFileEntry("f1.split", partitionValues = Map("date" -> "2024-01-01", "region" -> "us-east")),
      createTestFileEntry("f2.split", partitionValues = Map("date" -> "2024-01-15", "region" -> "us-west")),
      createTestFileEntry("f3.split", partitionValues = Map("date" -> "2024-01-10", "region" -> "eu-west"))
    )

    val bounds = writer.computePartitionBounds(entries)

    bounds shouldBe defined
    val b = bounds.get

    b should contain key "date"
    b("date").min shouldBe Some("2024-01-01")
    b("date").max shouldBe Some("2024-01-15")

    b should contain key "region"
    b("region").min shouldBe Some("eu-west")
    b("region").max shouldBe Some("us-west")
  }

  test("computePartitionBounds should return None for non-partitioned data") {
    val writer = new AvroManifestWriter(null)
    val entries = Seq(
      createTestFileEntry("f1.split", partitionValues = Map.empty),
      createTestFileEntry("f2.split", partitionValues = Map.empty)
    )

    val bounds = writer.computePartitionBounds(entries)

    bounds shouldBe None
  }

  test("computePartitionBounds should return None for empty entries") {
    val writer = new AvroManifestWriter(null)
    val bounds = writer.computePartitionBounds(Seq.empty)

    bounds shouldBe None
  }

  test("createManifestInfo should create correct info") {
    val writer = new AvroManifestWriter(null)
    val entries = Seq(
      createTestFileEntry("f1.split", version = 1, partitionValues = Map("date" -> "2024-01-01")),
      createTestFileEntry("f2.split", version = 5, partitionValues = Map("date" -> "2024-01-15")),
      createTestFileEntry("f3.split", version = 3, partitionValues = Map("date" -> "2024-01-10"))
    )

    val info = writer.createManifestInfo("manifest-abc.avro", entries)

    info.path shouldBe "manifest-abc.avro"
    info.numEntries shouldBe 3
    info.minAddedAtVersion shouldBe 1
    info.maxAddedAtVersion shouldBe 5
    info.partitionBounds shouldBe defined
    info.partitionBounds.get should contain key "date"
  }

  test("writeToBytes should handle large manifest efficiently") {
    val writer  = new AvroManifestWriter(null)
    val entries = (1 to 10000).map(i => createTestFileEntry(s"splits/file$i.split", version = i))

    val startTime     = System.currentTimeMillis()
    val bytes         = writer.writeToBytes(entries, compression = "zstd")
    val writeDuration = System.currentTimeMillis() - startTime

    // Write 10K entries should be reasonably fast (< 2 seconds)
    writeDuration should be < 2000L

    // Read back and verify
    val reader       = new AvroManifestReader(null)
    val readStart    = System.currentTimeMillis()
    val result       = reader.readFromBytes(bytes)
    val readDuration = System.currentTimeMillis() - readStart

    result should have size 10000

    // Read should also be fast (< 1 second)
    readDuration should be < 1000L
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

  private def createTestAddAction(path: String): AddAction =
    AddAction(
      path = path,
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(100L)
    )
}
