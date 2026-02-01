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
import io.indextables.spark.TestBase

/**
 * Tests for ManifestGarbageCollector.
 *
 * These tests verify:
 *   1. Reachable manifests are correctly identified from retained state versions 2. Orphaned manifests are identified
 *      and deleted 3. Age-based protection prevents deleting recent manifests 4. Dry-run mode previews without deleting
 *
 * Run with: mvn scalatest:test -DwildcardSuites='io.indextables.spark.transaction.avro.ManifestGarbageCollectorTest'
 */
class ManifestGarbageCollectorTest extends TestBase {

  test("findReachableManifests collects manifests from retained states") {
    withTempPath { tempDir =>
      val transactionLogPath = s"$tempDir/_transaction_log"
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        transactionLogPath,
        new CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath)
        val manifestIO = StateManifestIO(cloudProvider)

        // Create shared manifest directory
        val sharedDir = s"$transactionLogPath/${StateConfig.SHARED_MANIFEST_DIR}"
        cloudProvider.createDirectory(sharedDir)

        // Create states at versions 1, 2, 3 with different manifests
        for (v <- 1L to 3L) {
          val stateDir = s"$transactionLogPath/${manifestIO.formatStateDir(v)}"
          cloudProvider.createDirectory(stateDir)
          val manifest = StateManifest(
            formatVersion = 1,
            stateVersion = v,
            createdAt = System.currentTimeMillis(),
            numFiles = v.toInt,
            totalBytes = v * 100L,
            manifests = Seq(
              ManifestInfo(
                path = s"${StateConfig.SHARED_MANIFEST_DIR}/manifest-v$v.avro",
                numEntries = v.toInt,
                minAddedAtVersion = v,
                maxAddedAtVersion = v
              )
            ),
            tombstones = Seq.empty,
            schemaRegistry = Map.empty,
            protocolVersion = 4
          )
          manifestIO.writeStateManifest(stateDir, manifest)
        }

        // Write _last_checkpoint pointing to version 3
        val checkpointJson = """{"version":3,"size":3,"sizeInBytes":300,"numFiles":3,"createdTime":1234567890000}"""
        manifestIO.writeLastCheckpointIfNewer(transactionLogPath, 3L, checkpointJson)

        // Create GC with retention of 2 versions
        val gc        = ManifestGarbageCollector(cloudProvider, transactionLogPath)
        val gcConfig  = GCConfig(retentionVersions = 2)
        val reachable = gc.findReachableManifests(gcConfig)

        // Should find manifests from versions 3, 2, and 1 (within retention + buffer)
        reachable should contain(s"${StateConfig.SHARED_MANIFEST_DIR}/manifest-v3.avro")
        reachable should contain(s"${StateConfig.SHARED_MANIFEST_DIR}/manifest-v2.avro")
        reachable should contain(s"${StateConfig.SHARED_MANIFEST_DIR}/manifest-v1.avro")

      } finally
        cloudProvider.close()
    }
  }

  test("deleteOrphanedManifests only deletes unreferenced manifests") {
    withTempPath { tempDir =>
      val transactionLogPath = s"$tempDir/_transaction_log"
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        transactionLogPath,
        new CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath)
        val manifestIO = StateManifestIO(cloudProvider)

        // Create shared manifest directory
        val sharedDir = s"$transactionLogPath/${StateConfig.SHARED_MANIFEST_DIR}"
        cloudProvider.createDirectory(sharedDir)

        // Create dummy manifest files
        val referencedManifest = s"$sharedDir/manifest-referenced.avro"
        val orphanedManifest   = s"$sharedDir/manifest-orphaned.avro"

        cloudProvider.writeFile(referencedManifest, "dummy content".getBytes)
        cloudProvider.writeFile(orphanedManifest, "dummy content".getBytes)

        // Wait a bit to ensure file age is >= minManifestAgeHours (we'll use 0)
        Thread.sleep(100)

        // Create state that references only one manifest
        val stateDir = s"$transactionLogPath/${manifestIO.formatStateDir(1L)}"
        cloudProvider.createDirectory(stateDir)
        val manifest = StateManifest(
          formatVersion = 1,
          stateVersion = 1L,
          createdAt = System.currentTimeMillis(),
          numFiles = 1,
          totalBytes = 100L,
          manifests = Seq(
            ManifestInfo(
              path = s"${StateConfig.SHARED_MANIFEST_DIR}/manifest-referenced.avro",
              numEntries = 1,
              minAddedAtVersion = 1L,
              maxAddedAtVersion = 1L
            )
          ),
          tombstones = Seq.empty,
          schemaRegistry = Map.empty,
          protocolVersion = 4
        )
        manifestIO.writeStateManifest(stateDir, manifest)

        // Write _last_checkpoint
        val checkpointJson = """{"version":1,"size":1,"sizeInBytes":100,"numFiles":1,"createdTime":1234567890000}"""
        manifestIO.writeLastCheckpointIfNewer(transactionLogPath, 1L, checkpointJson)

        // Run GC with 0 hour age requirement (for testing)
        val gc       = ManifestGarbageCollector(cloudProvider, transactionLogPath)
        val gcConfig = GCConfig(retentionVersions = 2, minManifestAgeHours = 0)
        val result   = gc.collectGarbage(gcConfig, dryRun = false)

        // Should have found orphaned manifest and deleted it
        result.orphanedManifests shouldBe 1
        result.deletedManifests shouldBe 1

        // Verify orphaned manifest was deleted
        cloudProvider.exists(orphanedManifest) shouldBe false

        // Verify referenced manifest still exists
        cloudProvider.exists(referencedManifest) shouldBe true

      } finally
        cloudProvider.close()
    }
  }

  test("dryRun mode does not delete files") {
    withTempPath { tempDir =>
      val transactionLogPath = s"$tempDir/_transaction_log"
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        transactionLogPath,
        new CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath)
        val manifestIO = StateManifestIO(cloudProvider)

        // Create shared manifest directory with orphaned manifest
        val sharedDir = s"$transactionLogPath/${StateConfig.SHARED_MANIFEST_DIR}"
        cloudProvider.createDirectory(sharedDir)

        val orphanedManifest = s"$sharedDir/manifest-orphaned.avro"
        cloudProvider.writeFile(orphanedManifest, "dummy content".getBytes)

        // Create empty state (no manifests referenced)
        val stateDir = s"$transactionLogPath/${manifestIO.formatStateDir(1L)}"
        cloudProvider.createDirectory(stateDir)
        val manifest = StateManifest(
          formatVersion = 1,
          stateVersion = 1L,
          createdAt = System.currentTimeMillis(),
          numFiles = 0,
          totalBytes = 0L,
          manifests = Seq.empty,
          tombstones = Seq.empty,
          schemaRegistry = Map.empty,
          protocolVersion = 4
        )
        manifestIO.writeStateManifest(stateDir, manifest)

        // Write _last_checkpoint
        val checkpointJson = """{"version":1,"size":0,"sizeInBytes":0,"numFiles":0,"createdTime":1234567890000}"""
        manifestIO.writeLastCheckpointIfNewer(transactionLogPath, 1L, checkpointJson)

        // Run GC in dry-run mode
        val gc       = ManifestGarbageCollector(cloudProvider, transactionLogPath)
        val gcConfig = GCConfig(retentionVersions = 2, minManifestAgeHours = 0)
        val result   = gc.collectGarbage(gcConfig, dryRun = true)

        // Should report orphaned manifest
        result.orphanedManifests shouldBe 1
        result.dryRun shouldBe true

        // But file should still exist
        cloudProvider.exists(orphanedManifest) shouldBe true

      } finally
        cloudProvider.close()
    }
  }

  test("age-based protection prevents deleting recent manifests") {
    withTempPath { tempDir =>
      val transactionLogPath = s"$tempDir/_transaction_log"
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        transactionLogPath,
        new CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath)
        val manifestIO = StateManifestIO(cloudProvider)

        // Create shared manifest directory with orphaned manifest
        val sharedDir = s"$transactionLogPath/${StateConfig.SHARED_MANIFEST_DIR}"
        cloudProvider.createDirectory(sharedDir)

        val orphanedManifest = s"$sharedDir/manifest-orphaned.avro"
        cloudProvider.writeFile(orphanedManifest, "dummy content".getBytes)

        // Create empty state (no manifests referenced)
        val stateDir = s"$transactionLogPath/${manifestIO.formatStateDir(1L)}"
        cloudProvider.createDirectory(stateDir)
        val manifest = StateManifest(
          formatVersion = 1,
          stateVersion = 1L,
          createdAt = System.currentTimeMillis(),
          numFiles = 0,
          totalBytes = 0L,
          manifests = Seq.empty,
          tombstones = Seq.empty,
          schemaRegistry = Map.empty,
          protocolVersion = 4
        )
        manifestIO.writeStateManifest(stateDir, manifest)

        // Write _last_checkpoint
        val checkpointJson = """{"version":1,"size":0,"sizeInBytes":0,"numFiles":0,"createdTime":1234567890000}"""
        manifestIO.writeLastCheckpointIfNewer(transactionLogPath, 1L, checkpointJson)

        // Run GC with 1 hour age requirement - manifest is too recent
        val gc       = ManifestGarbageCollector(cloudProvider, transactionLogPath)
        val gcConfig = GCConfig(retentionVersions = 2, minManifestAgeHours = 1)
        val result   = gc.collectGarbage(gcConfig, dryRun = false)

        // Should not delete the recent manifest due to age protection
        result.deletedManifests shouldBe 0

        // Manifest should still exist
        cloudProvider.exists(orphanedManifest) shouldBe true

      } finally
        cloudProvider.close()
    }
  }
}
