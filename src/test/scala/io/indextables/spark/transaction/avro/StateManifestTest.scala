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

class StateManifestTest extends TestBase {

  test("parseStateManifest should parse minimal manifest") {
    val json =
      """{
        |  "formatVersion": 1,
        |  "stateVersion": 100,
        |  "createdAt": 1705123456789,
        |  "numFiles": 500,
        |  "totalBytes": 1234567890,
        |  "manifests": [],
        |  "protocolVersion": 4
        |}""".stripMargin

    val io = new StateManifestIO(null)
    val manifest = io.parseStateManifest(json)

    manifest.formatVersion shouldBe 1
    manifest.stateVersion shouldBe 100
    manifest.createdAt shouldBe 1705123456789L
    manifest.numFiles shouldBe 500
    manifest.totalBytes shouldBe 1234567890L
    manifest.manifests shouldBe empty
    manifest.tombstones shouldBe empty
    manifest.schemaRegistry shouldBe empty
    manifest.protocolVersion shouldBe 4
  }

  test("parseStateManifest should parse manifest with manifests list") {
    val json =
      """{
        |  "formatVersion": 1,
        |  "stateVersion": 100,
        |  "createdAt": 1705123456789,
        |  "numFiles": 70000,
        |  "totalBytes": 12345678900,
        |  "manifests": [
        |    {
        |      "path": "manifest-a1b2c3d4.avro",
        |      "numEntries": 50000,
        |      "minAddedAtVersion": 1,
        |      "maxAddedAtVersion": 50
        |    },
        |    {
        |      "path": "manifest-e5f6g7h8.avro",
        |      "numEntries": 20000,
        |      "minAddedAtVersion": 51,
        |      "maxAddedAtVersion": 100
        |    }
        |  ],
        |  "protocolVersion": 4
        |}""".stripMargin

    val io = new StateManifestIO(null)
    val manifest = io.parseStateManifest(json)

    manifest.manifests should have size 2
    manifest.manifests(0).path shouldBe "manifest-a1b2c3d4.avro"
    manifest.manifests(0).numEntries shouldBe 50000
    manifest.manifests(0).minAddedAtVersion shouldBe 1
    manifest.manifests(0).maxAddedAtVersion shouldBe 50
    manifest.manifests(0).partitionBounds shouldBe None

    manifest.manifests(1).path shouldBe "manifest-e5f6g7h8.avro"
    manifest.manifests(1).numEntries shouldBe 20000
    manifest.manifests(1).minAddedAtVersion shouldBe 51
    manifest.manifests(1).maxAddedAtVersion shouldBe 100
  }

  test("parseStateManifest should parse manifest with partition bounds") {
    val json =
      """{
        |  "formatVersion": 1,
        |  "stateVersion": 100,
        |  "createdAt": 1705123456789,
        |  "numFiles": 50000,
        |  "totalBytes": 1234567890,
        |  "manifests": [
        |    {
        |      "path": "manifest-a1b2c3d4.avro",
        |      "numEntries": 50000,
        |      "minAddedAtVersion": 1,
        |      "maxAddedAtVersion": 100,
        |      "partitionBounds": {
        |        "date": {"min": "2024-01-01", "max": "2024-01-15"},
        |        "region": {"min": "us-east", "max": "us-west"}
        |      }
        |    }
        |  ],
        |  "protocolVersion": 4
        |}""".stripMargin

    val io = new StateManifestIO(null)
    val manifest = io.parseStateManifest(json)

    manifest.manifests should have size 1
    manifest.manifests(0).partitionBounds shouldBe defined
    val bounds = manifest.manifests(0).partitionBounds.get

    bounds should contain key "date"
    bounds("date").min shouldBe Some("2024-01-01")
    bounds("date").max shouldBe Some("2024-01-15")

    bounds should contain key "region"
    bounds("region").min shouldBe Some("us-east")
    bounds("region").max shouldBe Some("us-west")
  }

  test("parseStateManifest should parse manifest with tombstones") {
    val json =
      """{
        |  "formatVersion": 1,
        |  "stateVersion": 100,
        |  "createdAt": 1705123456789,
        |  "numFiles": 500,
        |  "totalBytes": 1234567890,
        |  "manifests": [],
        |  "tombstones": [
        |    "splits/part=2024-01-01/split-abc123.split",
        |    "splits/part=2024-01-01/split-def456.split"
        |  ],
        |  "protocolVersion": 4
        |}""".stripMargin

    val io = new StateManifestIO(null)
    val manifest = io.parseStateManifest(json)

    manifest.tombstones should have size 2
    manifest.tombstones should contain("splits/part=2024-01-01/split-abc123.split")
    manifest.tombstones should contain("splits/part=2024-01-01/split-def456.split")
  }

  test("parseStateManifest should parse manifest with schema registry") {
    val json =
      """{
        |  "formatVersion": 1,
        |  "stateVersion": 100,
        |  "createdAt": 1705123456789,
        |  "numFiles": 500,
        |  "totalBytes": 1234567890,
        |  "manifests": [],
        |  "schemaRegistry": {
        |    "_schema_abc123": "{\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}",
        |    "_schema_def456": "{\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}"
        |  },
        |  "protocolVersion": 4
        |}""".stripMargin

    val io = new StateManifestIO(null)
    val manifest = io.parseStateManifest(json)

    manifest.schemaRegistry should have size 2
    manifest.schemaRegistry should contain key "_schema_abc123"
    manifest.schemaRegistry should contain key "_schema_def456"
  }

  test("serializeStateManifest should produce valid JSON") {
    val manifest = StateManifest(
      formatVersion = 1,
      stateVersion = 100,
      createdAt = 1705123456789L,
      numFiles = 70000,
      totalBytes = 12345678900L,
      manifests = Seq(
        ManifestInfo(
          path = "manifest-a1b2c3d4.avro",
          numEntries = 50000,
          minAddedAtVersion = 1,
          maxAddedAtVersion = 50,
          partitionBounds = Some(Map("date" -> PartitionBounds(Some("2024-01-01"), Some("2024-01-15"))))
        ),
        ManifestInfo(
          path = "manifest-e5f6g7h8.avro",
          numEntries = 20000,
          minAddedAtVersion = 51,
          maxAddedAtVersion = 100,
          partitionBounds = None
        )
      ),
      tombstones = Seq("splits/deleted.split"),
      schemaRegistry = Map("_schema_abc" -> "{}"),
      protocolVersion = 4
    )

    val io = new StateManifestIO(null)
    val json = io.serializeStateManifest(manifest)

    // Parse back and verify
    val parsed = io.parseStateManifest(json)

    parsed.formatVersion shouldBe manifest.formatVersion
    parsed.stateVersion shouldBe manifest.stateVersion
    parsed.createdAt shouldBe manifest.createdAt
    parsed.numFiles shouldBe manifest.numFiles
    parsed.totalBytes shouldBe manifest.totalBytes
    parsed.manifests should have size 2
    parsed.tombstones shouldBe manifest.tombstones
    parsed.schemaRegistry shouldBe manifest.schemaRegistry
    parsed.protocolVersion shouldBe manifest.protocolVersion
  }

  test("applyTombstones should filter out removed entries") {
    val entries = Seq(
      createTestFileEntry("file1.split"),
      createTestFileEntry("file2.split"),
      createTestFileEntry("file3.split"),
      createTestFileEntry("file4.split"),
      createTestFileEntry("file5.split")
    )

    val tombstones = Seq("file2.split", "file4.split")

    val io = new StateManifestIO(null)
    val result = io.applyTombstones(entries, tombstones)

    result should have size 3
    result.map(_.path) should contain allOf("file1.split", "file3.split", "file5.split")
    result.map(_.path) should not contain ("file2.split")
    result.map(_.path) should not contain ("file4.split")
  }

  test("applyTombstones should return all entries when tombstones is empty") {
    val entries = Seq(
      createTestFileEntry("file1.split"),
      createTestFileEntry("file2.split")
    )

    val io = new StateManifestIO(null)
    val result = io.applyTombstones(entries, Seq.empty)

    result should have size 2
  }

  test("applyTombstones should handle large tombstone list efficiently") {
    val entries = (1 to 10000).map(i => createTestFileEntry(s"file$i.split"))
    val tombstones = (1 to 5000 by 2).map(i => s"file$i.split") // Remove odd-numbered files

    val io = new StateManifestIO(null)
    val startTime = System.currentTimeMillis()
    val result = io.applyTombstones(entries, tombstones)
    val duration = System.currentTimeMillis() - startTime

    result should have size 7500 // 10000 - 2500 (every other file from 1-5000)
    // Should be efficient (< 100ms for 10K entries, 2.5K tombstones)
    duration should be < 100L
  }

  test("formatStateDir should format version correctly") {
    val io = new StateManifestIO(null)

    io.formatStateDir(1) shouldBe "state-v00000000000000000001"
    io.formatStateDir(100) shouldBe "state-v00000000000000000100"
    io.formatStateDir(123456789012345L) shouldBe "state-v00000123456789012345"
  }

  test("parseStateDirVersion should extract version correctly") {
    val io = new StateManifestIO(null)

    io.parseStateDirVersion("state-v00000000000000000001") shouldBe Some(1L)
    io.parseStateDirVersion("state-v00000000000000000100") shouldBe Some(100L)
    io.parseStateDirVersion("state-v00000123456789012345") shouldBe Some(123456789012345L)
    io.parseStateDirVersion("invalid") shouldBe None
    io.parseStateDirVersion("state-v123") shouldBe None // Too short
  }

  test("readStateManifest and writeStateManifest round-trip") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val io = new StateManifestIO(cloudProvider)

        val original = StateManifest(
          formatVersion = 1,
          stateVersion = 42,
          createdAt = System.currentTimeMillis(),
          numFiles = 1000,
          totalBytes = 5000000L,
          manifests = Seq(
            ManifestInfo("manifest-1.avro", 500, 1, 20, None),
            ManifestInfo("manifest-2.avro", 500, 21, 42, Some(Map("part" -> PartitionBounds(Some("a"), Some("z")))))
          ),
          tombstones = Seq("deleted1.split", "deleted2.split"),
          schemaRegistry = Map("schema1" -> "{}"),
          protocolVersion = 4
        )

        val stateDir = s"$tempPath/state-test"
        cloudProvider.createDirectory(stateDir)

        io.writeStateManifest(stateDir, original)
        val read = io.readStateManifest(stateDir)

        read.formatVersion shouldBe original.formatVersion
        read.stateVersion shouldBe original.stateVersion
        read.numFiles shouldBe original.numFiles
        read.totalBytes shouldBe original.totalBytes
        read.manifests should have size 2
        read.manifests(0).path shouldBe "manifest-1.avro"
        read.manifests(1).partitionBounds shouldBe defined
        read.tombstones shouldBe original.tombstones
        read.schemaRegistry shouldBe original.schemaRegistry
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("writeStateManifestIfNotExists should not overwrite existing manifest") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val io = new StateManifestIO(cloudProvider)

        val stateDir = s"$tempPath/state-test"
        cloudProvider.createDirectory(stateDir)

        val manifest1 = StateManifest(1, 100, System.currentTimeMillis(), 100, 1000L, Seq.empty, Seq.empty, Map.empty, 4)
        val manifest2 = StateManifest(1, 200, System.currentTimeMillis(), 200, 2000L, Seq.empty, Seq.empty, Map.empty, 4)

        // First write should succeed
        val written1 = io.writeStateManifestIfNotExists(stateDir, manifest1)
        written1 shouldBe true

        // Second write should fail (file exists)
        val written2 = io.writeStateManifestIfNotExists(stateDir, manifest2)
        written2 shouldBe false

        // Read back should return first manifest
        val read = io.readStateManifest(stateDir)
        read.stateVersion shouldBe 100
        read.numFiles shouldBe 100
      } finally {
        cloudProvider.close()
      }
    }
  }

  test("stateExists should return correct result") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val io = new StateManifestIO(cloudProvider)

        val stateDir = s"$tempPath/state-test"
        cloudProvider.createDirectory(stateDir)

        // No manifest yet
        io.stateExists(stateDir) shouldBe false

        // Write manifest
        val manifest = StateManifest(1, 1, System.currentTimeMillis(), 0, 0L, Seq.empty, Seq.empty, Map.empty, 4)
        io.writeStateManifest(stateDir, manifest)

        // Now exists
        io.stateExists(stateDir) shouldBe true
      } finally {
        cloudProvider.close()
      }
    }
  }

  // Helper methods

  private def createTestFileEntry(path: String): FileEntry = {
    FileEntry(
      path = path,
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      addedAtVersion = 1L,
      addedAtTimestamp = System.currentTimeMillis()
    )
  }
}
