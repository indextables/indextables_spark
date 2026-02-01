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

import java.io.{ByteArrayOutputStream, File}

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.AddAction
import io.indextables.spark.TestBase
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}

class AvroManifestReaderTest extends TestBase {

  test("readFromBytes should read empty manifest") {
    val entries = Seq.empty[FileEntry]
    val bytes   = writeManifestToBytes(entries)

    val reader = new AvroManifestReader(null) // cloudProvider not needed for byte reading
    val result = reader.readFromBytes(bytes)

    result shouldBe empty
  }

  test("readFromBytes should read single entry") {
    val entry = createTestFileEntry("splits/file1.split")
    val bytes = writeManifestToBytes(Seq(entry))

    val reader = new AvroManifestReader(null)
    val result = reader.readFromBytes(bytes)

    result should have size 1
    result.head.path shouldBe "splits/file1.split"
    result.head.size shouldBe entry.size
    result.head.addedAtVersion shouldBe entry.addedAtVersion
  }

  test("readFromBytes should read multiple entries") {
    val entries = (1 to 100).map(i => createTestFileEntry(s"splits/file$i.split", version = i))
    val bytes   = writeManifestToBytes(entries)

    val reader = new AvroManifestReader(null)
    val result = reader.readFromBytes(bytes)

    result should have size 100
    result.head.path shouldBe "splits/file1.split"
    result.last.path shouldBe "splits/file100.split"
    result.head.addedAtVersion shouldBe 1
    result.last.addedAtVersion shouldBe 100
  }

  test("readFromBytes should read all fields correctly") {
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

    val bytes  = writeManifestToBytes(Seq(entry))
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

  test("readFromBytes should handle nullable fields with null values") {
    val entry = FileEntry(
      path = "splits/file.split",
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = 1705123456789L,
      dataChange = true,
      stats = None,
      minValues = None,
      maxValues = None,
      numRecords = None,
      footerStartOffset = None,
      footerEndOffset = None,
      hasFooterOffsets = false,
      splitTags = None,
      numMergeOps = None,
      docMappingRef = None,
      uncompressedSizeBytes = None,
      addedAtVersion = 1L,
      addedAtTimestamp = 1705123456789L
    )

    val bytes  = writeManifestToBytes(Seq(entry))
    val reader = new AvroManifestReader(null)
    val result = reader.readFromBytes(bytes)

    result should have size 1
    val read = result.head

    read.stats shouldBe None
    read.minValues shouldBe None
    read.maxValues shouldBe None
    read.numRecords shouldBe None
    read.footerStartOffset shouldBe None
    read.footerEndOffset shouldBe None
    read.hasFooterOffsets shouldBe false
    read.splitTags shouldBe None
    read.numMergeOps shouldBe None
    read.docMappingRef shouldBe None
    read.uncompressedSizeBytes shouldBe None
  }

  test("readManifest should read from cloud provider") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val entries = (1 to 10).map(i => createTestFileEntry(s"splits/file$i.split", version = i))
        val bytes   = writeManifestToBytes(entries)

        val manifestPath = s"$tempPath/manifest-test.avro"
        cloudProvider.writeFile(manifestPath, bytes)

        val reader = new AvroManifestReader(cloudProvider)
        val result = reader.readManifest(manifestPath)

        result should have size 10
        result.head.path shouldBe "splits/file1.split"
        result.last.path shouldBe "splits/file10.split"
      } finally
        cloudProvider.close()
    }
  }

  test("readManifestsParallel should read multiple manifests") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        // Create 3 manifest files
        val manifest1 = writeManifestToBytes((1 to 10).map(i => createTestFileEntry(s"m1/file$i.split", version = i)))
        val manifest2 = writeManifestToBytes((11 to 20).map(i => createTestFileEntry(s"m2/file$i.split", version = i)))
        val manifest3 = writeManifestToBytes((21 to 30).map(i => createTestFileEntry(s"m3/file$i.split", version = i)))

        cloudProvider.writeFile(s"$tempPath/manifest-1.avro", manifest1)
        cloudProvider.writeFile(s"$tempPath/manifest-2.avro", manifest2)
        cloudProvider.writeFile(s"$tempPath/manifest-3.avro", manifest3)

        val reader = new AvroManifestReader(cloudProvider)
        val paths = Seq(
          s"$tempPath/manifest-1.avro",
          s"$tempPath/manifest-2.avro",
          s"$tempPath/manifest-3.avro"
        )

        val result = reader.readManifestsParallel(paths, parallelism = 2)

        result should have size 30
        result.count(_.path.startsWith("m1/")) shouldBe 10
        result.count(_.path.startsWith("m2/")) shouldBe 10
        result.count(_.path.startsWith("m3/")) shouldBe 10
      } finally
        cloudProvider.close()
    }
  }

  test("readManifestSinceVersion should filter by version") {
    val entries = (1 to 100).map(i => createTestFileEntry(s"splits/file$i.split", version = i))
    val bytes   = writeManifestToBytes(entries)

    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val manifestPath = s"$tempPath/manifest.avro"
        cloudProvider.writeFile(manifestPath, bytes)

        val reader = new AvroManifestReader(cloudProvider)
        val result = reader.readManifestSinceVersion(manifestPath, sinceVersion = 50)

        result should have size 50
        result.forall(_.addedAtVersion > 50) shouldBe true
        result.head.addedAtVersion shouldBe 51
        result.last.addedAtVersion shouldBe 100
      } finally
        cloudProvider.close()
    }
  }

  test("toAddAction should convert FileEntry to AddAction") {
    val entry  = createTestFileEntry("splits/file.split", version = 42)
    val reader = new AvroManifestReader(null)

    val addAction = reader.toAddAction(entry)

    addAction.path shouldBe entry.path
    addAction.partitionValues shouldBe entry.partitionValues
    addAction.size shouldBe entry.size
    addAction.modificationTime shouldBe entry.modificationTime
    addAction.dataChange shouldBe entry.dataChange
  }

  test("toAddActions should convert multiple FileEntries") {
    val entries = (1 to 5).map(i => createTestFileEntry(s"splits/file$i.split", version = i))
    val reader  = new AvroManifestReader(null)

    val addActions = reader.toAddActions(entries)

    addActions should have size 5
    addActions.map(_.path) shouldBe entries.map(_.path)
  }

  test("readFromBytes should handle large manifest (1000+ entries)") {
    val entries = (1 to 1000).map(i => createTestFileEntry(s"splits/file$i.split", version = i))
    val bytes   = writeManifestToBytes(entries)

    val reader    = new AvroManifestReader(null)
    val startTime = System.currentTimeMillis()
    val result    = reader.readFromBytes(bytes)
    val duration  = System.currentTimeMillis() - startTime

    result should have size 1000
    // Should complete in reasonable time (< 1 second for 1000 entries)
    duration should be < 1000L
  }

  // Helper methods

  private def createTestFileEntry(path: String, version: Long = 1): FileEntry =
    FileEntry(
      path = path,
      partitionValues = Map.empty,
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

  private def writeManifestToBytes(entries: Seq[FileEntry]): Array[Byte] = {
    val schema         = AvroSchemas.FILE_ENTRY_SCHEMA
    val datumWriter    = new GenericDatumWriter[GenericRecord](schema)
    val baos           = new ByteArrayOutputStream()
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)

    try {
      dataFileWriter.create(schema, baos)
      entries.foreach(entry => dataFileWriter.append(AvroSchemas.toGenericRecord(entry)))
      dataFileWriter.close()
      baos.toByteArray
    } finally
      dataFileWriter.close()
  }
}
