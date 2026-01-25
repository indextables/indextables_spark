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
import io.indextables.spark.transaction.SchemaDeduplication

import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Tests for schema deduplication in Avro state format.
 *
 * These tests validate that:
 * 1. schemaRegistry is properly populated in _manifest.avro
 * 2. docMappingRef is correctly resolved to docMappingJson when reading
 * 3. Schema deduplication works correctly across incremental writes
 */
class AvroSchemaDeduplicationTest extends TestBase {

  // Sample schemas for testing
  private val smallSchema =
    """{"fields":[{"name":"id","type":"i64","fast":true}]}"""

  private val largeSchema =
    """{"fields":[""" + (1 to 100).map(i => s"""{"name":"field$i","type":"text"}""").mkString(",") + "]}"

  test("schemaRegistry should be populated in _manifest.avro") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)

        // Create files with docMappingRef (simulating deduplicated AddActions)
        val schemaHash1 = SchemaDeduplication.computeSchemaHash(smallSchema)
        val schemaHash2 = SchemaDeduplication.computeSchemaHash(largeSchema)

        val files = Seq(
          createTestFileEntry("file1.split", docMappingRef = Some(schemaHash1)),
          createTestFileEntry("file2.split", docMappingRef = Some(schemaHash1)),
          createTestFileEntry("file3.split", docMappingRef = Some(schemaHash2))
        )

        val schemaRegistry = Map(
          schemaHash1 -> smallSchema,
          schemaHash2 -> largeSchema
        )

        val stateDir = stateWriter.writeState(
          currentVersion = 1,
          newFiles = files,
          removedPaths = Set.empty,
          schemaRegistry = schemaRegistry
        )

        // Verify schema registry was written to _manifest.avro
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir)

        manifest.schemaRegistry should have size 2
        manifest.schemaRegistry should contain key schemaHash1
        manifest.schemaRegistry should contain key schemaHash2
        manifest.schemaRegistry(schemaHash1) shouldBe smallSchema
        manifest.schemaRegistry(schemaHash2) shouldBe largeSchema
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("docMappingRef should be resolved to docMappingJson when reading from Avro state") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)
        val manifestReader = AvroManifestReader(cloudProvider)

        // Create files with docMappingRef
        val schemaHash = SchemaDeduplication.computeSchemaHash(largeSchema)

        val files = Seq(
          createTestFileEntry("file1.split", docMappingRef = Some(schemaHash)),
          createTestFileEntry("file2.split", docMappingRef = Some(schemaHash)),
          createTestFileEntry("file3.split", docMappingRef = Some(schemaHash))
        )

        val schemaRegistry = Map(schemaHash -> largeSchema)

        val stateDir = stateWriter.writeState(
          currentVersion = 1,
          newFiles = files,
          removedPaths = Set.empty,
          schemaRegistry = schemaRegistry
        )

        // Read back the manifest and files
        val manifestIO = StateManifestIO(cloudProvider)
        val manifest = manifestIO.readStateManifest(stateDir)

        val manifestPaths = manifestIO.resolveManifestPaths(manifest, tempPath, stateDir)
        val readEntries = manifestReader.readManifestsParallel(manifestPaths)

        // Verify entries have docMappingRef
        readEntries should have size 3
        readEntries.foreach(_.docMappingRef shouldBe Some(schemaHash))

        // Convert to AddActions using schema registry - this should resolve docMappingRef
        val addActions = manifestReader.toAddActions(readEntries, manifest.schemaRegistry)

        // Verify docMappingJson is resolved from registry
        // Note: docMappingRef is preserved for traceability
        addActions should have size 3
        addActions.foreach { add =>
          add.docMappingJson shouldBe Some(largeSchema)
          add.docMappingRef shouldBe Some(schemaHash) // Preserved for traceability
        }
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("schema deduplication should work across incremental Avro writes") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)
        val manifestReader = AvroManifestReader(cloudProvider)

        val schemaHash = SchemaDeduplication.computeSchemaHash(largeSchema)
        val schemaRegistry = Map(schemaHash -> largeSchema)

        // First write - initial state with 50 files
        val initialFiles = (1 to 50).map(i =>
          createTestFileEntry(s"file$i.split", docMappingRef = Some(schemaHash))
        )

        val stateDir1 = stateWriter.writeState(
          currentVersion = 1,
          newFiles = initialFiles,
          removedPaths = Set.empty,
          schemaRegistry = schemaRegistry
        )

        // Verify initial state
        val manifest1 = StateManifestIO(cloudProvider).readStateManifest(stateDir1)
        manifest1.schemaRegistry should have size 1
        manifest1.numFiles shouldBe 50

        // Second write - incremental with same schema
        val newFiles = (51 to 75).map(i =>
          createTestFileEntry(s"file$i.split", docMappingRef = Some(schemaHash))
        )

        val stateDir2 = stateWriter.writeState(
          currentVersion = 2,
          newFiles = newFiles,
          removedPaths = Set.empty,
          schemaRegistry = schemaRegistry, // Same schema registry
          forceCompaction = true // Force compaction to ensure all files are in the new state dir
        )

        // Verify incremental state
        val manifestIO2 = StateManifestIO(cloudProvider)
        val manifest2 = manifestIO2.readStateManifest(stateDir2)
        manifest2.schemaRegistry should have size 1 // Still only 1 schema
        manifest2.numFiles shouldBe 75

        // Read all files and verify docMappingJson is resolved
        val manifestPaths = manifestIO2.resolveManifestPaths(manifest2, tempPath, stateDir2)
        val readEntries = manifestReader.readManifestsParallel(manifestPaths)
        val addActions = manifestReader.toAddActions(readEntries, manifest2.schemaRegistry)

        addActions should have size 75
        addActions.foreach { add =>
          add.docMappingJson shouldBe Some(largeSchema)
          add.docMappingRef shouldBe Some(schemaHash) // Preserved for traceability
        }
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("schema deduplication with multiple different schemas across writes") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)
        val manifestReader = AvroManifestReader(cloudProvider)

        val schemaHash1 = SchemaDeduplication.computeSchemaHash(smallSchema)
        val schemaHash2 = SchemaDeduplication.computeSchemaHash(largeSchema)

        // First write with schema 1
        val files1 = (1 to 30).map(i =>
          createTestFileEntry(s"file$i.split", docMappingRef = Some(schemaHash1))
        )

        val stateDir1 = stateWriter.writeState(
          currentVersion = 1,
          newFiles = files1,
          removedPaths = Set.empty,
          schemaRegistry = Map(schemaHash1 -> smallSchema)
        )

        // Second write adds schema 2 (merged registry)
        val files2 = (31 to 60).map(i =>
          createTestFileEntry(s"file$i.split", docMappingRef = Some(schemaHash2))
        )

        // With forceCompaction, we must pass the full registry since compaction doesn't auto-merge
        val stateDir2 = stateWriter.writeState(
          currentVersion = 2,
          newFiles = files2,
          removedPaths = Set.empty,
          schemaRegistry = Map(schemaHash1 -> smallSchema, schemaHash2 -> largeSchema),
          forceCompaction = true // Force compaction to ensure all files are in new state dir
        )

        // Verify merged schema registry
        val manifestIO2 = StateManifestIO(cloudProvider)
        val manifest2 = manifestIO2.readStateManifest(stateDir2)
        manifest2.schemaRegistry should have size 2
        manifest2.schemaRegistry should contain key schemaHash1
        manifest2.schemaRegistry should contain key schemaHash2

        // Read all files and verify correct schema resolution
        val manifestPaths = manifestIO2.resolveManifestPaths(manifest2, tempPath, stateDir2)
        val readEntries = manifestReader.readManifestsParallel(manifestPaths)
        val addActions = manifestReader.toAddActions(readEntries, manifest2.schemaRegistry)

        addActions should have size 60

        // First 30 should have smallSchema
        val firstBatch = addActions.filter(_.path.matches("file(1[0-9]?|2[0-9]|30|[1-9]).split"))
        firstBatch.foreach(_.docMappingJson shouldBe Some(smallSchema))

        // Last 30 should have largeSchema
        val secondBatch = addActions.filter(_.path.matches("file(3[1-9]|4[0-9]|5[0-9]|60).split"))
        secondBatch.foreach(_.docMappingJson shouldBe Some(largeSchema))
      } finally {
        cloudProvider.close()
      }
    }
  }

  // Note: End-to-end TransactionLog integration with schema deduplication is tested
  // in AvroStateCommandsTest. The unit tests above validate the schema deduplication
  // behavior at the StateWriter layer.

  test("schema deduplication survives compaction in Avro format") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val stateWriter = StateWriter(cloudProvider, tempPath)
        val manifestReader = AvroManifestReader(cloudProvider)

        val schemaHash = SchemaDeduplication.computeSchemaHash(largeSchema)
        val schemaRegistry = Map(schemaHash -> largeSchema)

        // Write initial state with 100 files
        val initialFiles = (1 to 100).map(i =>
          createTestFileEntry(s"file$i.split", docMappingRef = Some(schemaHash))
        )

        stateWriter.writeState(
          currentVersion = 1,
          newFiles = initialFiles,
          removedPaths = Set.empty,
          schemaRegistry = schemaRegistry
        )

        // Remove 20 files (20% ratio triggers compaction)
        // Note: Compaction requires passing the full schema registry because writeCompactedState
        // doesn't automatically merge with the old state's registry (potential improvement opportunity)
        val removedPaths = (1 to 20).map(i => s"file$i.split").toSet
        val stateDir2 = stateWriter.writeState(
          currentVersion = 2,
          newFiles = Seq.empty,
          removedPaths = removedPaths,
          schemaRegistry = schemaRegistry // Pass full registry for compaction
        )

        // Verify state after compaction
        val manifestIO2 = StateManifestIO(cloudProvider)
        val manifest2 = manifestIO2.readStateManifest(stateDir2)
        manifest2.numFiles shouldBe 80
        manifest2.tombstones shouldBe empty // Compacted - no tombstones

        // Schema registry should be preserved
        manifest2.schemaRegistry should have size 1
        manifest2.schemaRegistry should contain key schemaHash

        // Read files and verify docMappingJson resolution still works
        val manifestPaths = manifestIO2.resolveManifestPaths(manifest2, tempPath, stateDir2)
        val readEntries = manifestReader.readManifestsParallel(manifestPaths)
        val addActions = manifestReader.toAddActions(readEntries, manifest2.schemaRegistry)

        addActions should have size 80
        addActions.foreach { add =>
          add.docMappingJson shouldBe Some(largeSchema)
        }
      } finally {
        cloudProvider.close()
      }
    }
  }

  // Helper methods

  private def createTestFileEntry(
      path: String,
      version: Long = 1,
      partitionValues: Map[String, String] = Map.empty,
      docMappingRef: Option[String] = None): FileEntry = {
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
      docMappingRef = docMappingRef,
      uncompressedSizeBytes = None,
      addedAtVersion = version,
      addedAtTimestamp = System.currentTimeMillis()
    )
  }
}
