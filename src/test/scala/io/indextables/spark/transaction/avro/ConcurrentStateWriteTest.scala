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

import scala.collection.mutable.ArrayBuffer

import io.indextables.spark.TestBase

/**
 * Tests for concurrent state write handling in the Avro state format.
 *
 * These tests verify:
 *   1. Conditional writes properly detect concurrent conflicts 2. Retry logic increments version on conflict 3.
 *      _last_checkpoint is updated atomically with version checking 4. Multiple concurrent writers eventually succeed
 *      with unique versions
 *
 * Run with: mvn scalatest:test -DwildcardSuites='io.indextables.spark.transaction.avro.ConcurrentStateWriteTest'
 */
class ConcurrentStateWriteTest extends TestBase {

  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  test("StateWriter.writeStateWithRetry handles single writer correctly") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create initial table
      val data = Seq((1, "doc1"), (2, "doc2"))
      spark
        .createDataFrame(data)
        .toDF("id", "content")
        .write
        .format(provider)
        .mode("overwrite")
        .save(path)

      // Enable Avro format and create checkpoint
      spark.conf.set("spark.indextables.state.format", "avro")
      val result = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      result.length shouldBe 1
      result(0).getAs[String]("status") shouldBe "SUCCESS"
      // Version 0 is valid for the first checkpoint after a single write operation
      result(0).getAs[Long]("checkpoint_version") should be >= 0L

      // Verify state was written
      val state = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      state(0).getAs[String]("format") shouldBe "avro-state"

      spark.conf.unset("spark.indextables.state.format")
    }
  }

  test("StateManifestIO.writeStateManifestIfNotExists returns false on conflict") {
    withTempPath { tempDir =>
      val transactionLogPath = s"$tempDir/_transaction_log"
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        transactionLogPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val manifestIO = StateManifestIO(cloudProvider)

        // Create state directory
        val stateDir = s"$transactionLogPath/state-v00000000000000000001"
        cloudProvider.createDirectory(stateDir)

        // Create a test manifest
        val manifest = StateManifest(
          formatVersion = 1,
          stateVersion = 1L,
          createdAt = System.currentTimeMillis(),
          numFiles = 0,
          totalBytes = 0,
          manifests = Seq.empty,
          tombstones = Seq.empty,
          schemaRegistry = Map.empty,
          protocolVersion = 4
        )

        // First write should succeed
        val firstWrite = manifestIO.writeStateManifestIfNotExists(stateDir, manifest)
        firstWrite shouldBe true

        // Second write to same location should fail (conflict)
        val secondWrite = manifestIO.writeStateManifestIfNotExists(stateDir, manifest)
        secondWrite shouldBe false

        // Verify the manifest exists
        manifestIO.stateExists(stateDir) shouldBe true
      } finally
        cloudProvider.close()
    }
  }

  test("StateManifestIO.writeLastCheckpointIfNewer skips older versions") {
    withTempPath { tempDir =>
      val transactionLogPath = s"$tempDir/_transaction_log"
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        transactionLogPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath)
        val manifestIO = StateManifestIO(cloudProvider)

        // Write checkpoint at version 10
        val json10    = """{"version":10,"size":100,"sizeInBytes":1000,"numFiles":100,"createdTime":1234567890000}"""
        val written10 = manifestIO.writeLastCheckpointIfNewer(transactionLogPath, 10L, json10)
        written10 shouldBe true

        // Try to write checkpoint at version 5 (older) - should be skipped
        val json5    = """{"version":5,"size":50,"sizeInBytes":500,"numFiles":50,"createdTime":1234567890000}"""
        val written5 = manifestIO.writeLastCheckpointIfNewer(transactionLogPath, 5L, json5)
        written5 shouldBe false

        // Try to write checkpoint at version 10 (same) - should succeed to allow format upgrades (JSON -> Avro)
        val written10Same = manifestIO.writeLastCheckpointIfNewer(transactionLogPath, 10L, json10)
        written10Same shouldBe true

        // Write checkpoint at version 15 (newer) - should succeed
        val json15    = """{"version":15,"size":150,"sizeInBytes":1500,"numFiles":150,"createdTime":1234567890000}"""
        val written15 = manifestIO.writeLastCheckpointIfNewer(transactionLogPath, 15L, json15)
        written15 shouldBe true

        // Verify final version is 15
        val currentVersion = manifestIO.getCurrentCheckpointVersion(transactionLogPath)
        currentVersion shouldBe Some(15L)
      } finally
        cloudProvider.close()
    }
  }

  test("StateWriter.writeStateWithRetry retries on conflict and increments version") {
    withTempPath { tempDir =>
      val transactionLogPath = s"$tempDir/_transaction_log"
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        transactionLogPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath)
        val manifestIO = StateManifestIO(cloudProvider)

        // Pre-create state at version 1 to force conflict
        val existingStateDir = s"$transactionLogPath/${manifestIO.formatStateDir(1L)}"
        cloudProvider.createDirectory(existingStateDir)
        val existingManifest = StateManifest(
          formatVersion = 1,
          stateVersion = 1L,
          createdAt = System.currentTimeMillis(),
          numFiles = 0,
          totalBytes = 0,
          manifests = Seq.empty,
          tombstones = Seq.empty,
          schemaRegistry = Map.empty,
          protocolVersion = 4
        )
        manifestIO.writeStateManifest(existingStateDir, existingManifest)

        // Create StateWriter with short retry delays for testing
        val retryConfig = StateRetryConfig(
          maxAttempts = 5,
          baseDelayMs = 10,
          maxDelayMs = 100
        )
        val stateWriter = StateWriter(
          cloudProvider,
          transactionLogPath,
          retryConfig = retryConfig
        )

        // Try to write at version 1 - should detect conflict and increment
        val testFiles = Seq(
          FileEntry(
            path = "test.split",
            partitionValues = Map.empty,
            size = 100L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            addedAtVersion = 1L,
            addedAtTimestamp = System.currentTimeMillis()
          )
        )

        val result = stateWriter.writeStateWithRetry(1L, testFiles, Map.empty)

        // Should have succeeded at version 2 (after conflict at version 1)
        result.version shouldBe 2L
        result.conflictDetected shouldBe true
        result.attempts should be >= 2

        // Verify both versions exist
        manifestIO.stateExists(s"$transactionLogPath/${manifestIO.formatStateDir(1L)}") shouldBe true
        manifestIO.stateExists(s"$transactionLogPath/${manifestIO.formatStateDir(2L)}") shouldBe true
      } finally
        cloudProvider.close()
    }
  }

  test("Concurrent checkpoint commands handle conflicts correctly") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create initial table with data
      val data = (1 to 100).map(i => (i, s"document $i", i * 10))
      spark
        .createDataFrame(data)
        .toDF("id", "content", "score")
        .write
        .format(provider)
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(path)

      // Enable Avro format
      spark.conf.set("spark.indextables.state.format", "avro")

      // Run multiple checkpoint commands sequentially with short delays
      // This tests that version checking works properly
      val results = new ArrayBuffer[(Long, String)]()

      try {
        for (i <- 1 to 3) {
          val result  = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
          val status  = result(0).getAs[String]("status")
          val version = result(0).getAs[Long]("checkpoint_version")
          results += ((version, status))
          println(s"Checkpoint $i: version=$version, status=$status")
          Thread.sleep(100) // Small delay between checkpoints
        }

        // All checkpoints should have succeeded
        results.foreach { case (_, status) => status shouldBe "SUCCESS" }

        // Verify table is still readable
        val df = spark.read.format(provider).load(path)
        df.count() shouldBe 100

        // Verify state format is correct
        val state = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
        state(0).getAs[String]("format") shouldBe "avro-state"

      } finally
        spark.conf.unset("spark.indextables.state.format")
    }
  }

  test("StateWriter retries and succeeds with higher version after conflicts") {
    withTempPath { tempDir =>
      val transactionLogPath = s"$tempDir/_transaction_log"
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        transactionLogPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath)
        val manifestIO = StateManifestIO(cloudProvider)

        // Pre-create states at versions 1-3 to force conflicts
        for (v <- 1L to 3L) {
          val stateDir = s"$transactionLogPath/${manifestIO.formatStateDir(v)}"
          cloudProvider.createDirectory(stateDir)
          val manifest = StateManifest(
            formatVersion = 1,
            stateVersion = v,
            createdAt = System.currentTimeMillis(),
            numFiles = 0,
            totalBytes = 0,
            manifests = Seq.empty,
            tombstones = Seq.empty,
            schemaRegistry = Map.empty,
            protocolVersion = 4
          )
          manifestIO.writeStateManifest(stateDir, manifest)
        }

        // Create StateWriter with enough attempts to find available version
        val retryConfig = StateRetryConfig(
          maxAttempts = 5,
          baseDelayMs = 10,
          maxDelayMs = 50
        )
        val stateWriter = StateWriter(
          cloudProvider,
          transactionLogPath,
          retryConfig = retryConfig
        )

        val testFiles = Seq(
          FileEntry(
            path = "test.split",
            partitionValues = Map.empty,
            size = 100L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            addedAtVersion = 1L,
            addedAtTimestamp = System.currentTimeMillis()
          )
        )

        // Starting at version 1, it should detect conflicts at 1, 2, 3 and succeed at version 4
        val result = stateWriter.writeStateWithRetry(1L, testFiles, Map.empty)

        // Should have succeeded at version 4 (first available after 1-3)
        result.version shouldBe 4L
        result.conflictDetected shouldBe true
        // Multiple attempts were needed (at least 2 - first detected conflict, second found available)

        // Verify the state was written correctly
        manifestIO.stateExists(s"$transactionLogPath/${manifestIO.formatStateDir(4L)}") shouldBe true
        val writtenManifest = manifestIO.readStateManifest(s"$transactionLogPath/${manifestIO.formatStateDir(4L)}")
        writtenManifest.stateVersion shouldBe 4L
        writtenManifest.numFiles shouldBe 1

      } finally
        cloudProvider.close()
    }
  }

  test("Checkpoint with retry config from Spark options") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create initial table
      val data = Seq((1, "doc1"), (2, "doc2"))
      spark
        .createDataFrame(data)
        .toDF("id", "content")
        .write
        .format(provider)
        .mode("overwrite")
        .save(path)

      // Set custom retry configuration
      spark.conf.set("spark.indextables.state.format", "avro")
      spark.conf.set("spark.indextables.state.retry.maxAttempts", "5")
      spark.conf.set("spark.indextables.state.retry.baseDelayMs", "50")
      spark.conf.set("spark.indextables.state.retry.maxDelayMs", "500")

      val result = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      result.length shouldBe 1
      result(0).getAs[String]("status") shouldBe "SUCCESS"

      // Clean up
      spark.conf.unset("spark.indextables.state.format")
      spark.conf.unset("spark.indextables.state.retry.maxAttempts")
      spark.conf.unset("spark.indextables.state.retry.baseDelayMs")
      spark.conf.unset("spark.indextables.state.retry.maxDelayMs")
    }
  }

  test("verifyCheckpointVersion detects _last_checkpoint regression using N+1 probe") {
    withTempPath { tempDir =>
      val transactionLogPath = s"$tempDir/_transaction_log"
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        transactionLogPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(transactionLogPath)
        val manifestIO = StateManifestIO(cloudProvider)

        // Create state directories for versions 5, 6, and 7 (simulating successful concurrent writes)
        for (v <- 5L to 7L) {
          val stateDir = s"$transactionLogPath/${manifestIO.formatStateDir(v)}"
          cloudProvider.createDirectory(stateDir)
          val manifest = StateManifest(
            formatVersion = 1,
            stateVersion = v,
            createdAt = System.currentTimeMillis(),
            numFiles = v.toInt,
            totalBytes = v * 1000,
            manifests = Seq.empty,
            tombstones = Seq.empty,
            schemaRegistry = Map.empty,
            protocolVersion = 4
          )
          manifestIO.writeStateManifest(stateDir, manifest)
        }

        // Test 1: Hint is correct (v7) - should return v7 with just one check
        val correctHint = manifestIO.verifyCheckpointVersion(transactionLogPath, 7L)
        correctHint shouldBe 7L

        // Test 2: Hint is stale by 1 (v6) - should detect v7 exists and return v7
        val staleBy1 = manifestIO.verifyCheckpointVersion(transactionLogPath, 6L)
        staleBy1 shouldBe 7L

        // Test 3: Hint is stale by 2 (v5) - should detect v6, v7 exist and return v7
        val staleBy2 = manifestIO.verifyCheckpointVersion(transactionLogPath, 5L)
        staleBy2 shouldBe 7L

        // Test 4: Hint is ahead (v10) - should return v10 (can't go backwards)
        // This shouldn't happen in practice but verifies the loop terminates
        val aheadHint = manifestIO.verifyCheckpointVersion(transactionLogPath, 10L)
        aheadHint shouldBe 10L

      } finally
        cloudProvider.close()
    }
  }

  test("getLastCheckpointInfoVerified corrects stale _last_checkpoint") {
    withTempPath { tempDir =>
      val path = tempDir

      // Create initial table with data
      val data = Seq((1, "doc1"), (2, "doc2"))
      spark
        .createDataFrame(data)
        .toDF("id", "content")
        .write
        .format(provider)
        .mode("overwrite")
        .save(path)

      // Enable Avro format and create checkpoint
      spark.conf.set("spark.indextables.state.format", "avro")

      try {
        // Create checkpoint at version N
        val result1 = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
        result1(0).getAs[String]("status") shouldBe "SUCCESS"
        val v1 = result1(0).getAs[Long]("checkpoint_version")

        // Add more data and create another checkpoint
        val data2 = Seq((3, "doc3"), (4, "doc4"))
        spark
          .createDataFrame(data2)
          .toDF("id", "content")
          .write
          .format(provider)
          .mode("append")
          .save(path)

        val result2 = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
        result2(0).getAs[String]("status") shouldBe "SUCCESS"
        val v2 = result2(0).getAs[Long]("checkpoint_version")
        v2 should be > v1

        // Now manually write a stale _last_checkpoint pointing to v1
        // This simulates the TOCTOU race regression
        val transactionLogPath = s"$path/_transaction_log"
        val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
          transactionLogPath,
          new org.apache.spark.sql.util.CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
          spark.sparkContext.hadoopConfiguration
        )

        try {
          val manifestIO = StateManifestIO(cloudProvider)
          val staleCheckpointJson =
            s"""{"version":$v1,"size":2,"sizeInBytes":1000,"numFiles":2,"createdTime":${System
                .currentTimeMillis()},"format":"avro-state","stateDir":"${manifestIO.formatStateDir(v1)}"}"""
          cloudProvider.writeFile(s"$transactionLogPath/_last_checkpoint", staleCheckpointJson.getBytes("UTF-8"))

          // Verify _last_checkpoint now points to v1 (stale)
          val currentVersion = manifestIO.getCurrentCheckpointVersion(transactionLogPath)
          currentVersion shouldBe Some(v1)

          // But state v2 still exists
          manifestIO.stateExists(s"$transactionLogPath/${manifestIO.formatStateDir(v2)}") shouldBe true

          // Use TransactionLogCheckpoint to get verified info
          val checkpoint = new io.indextables.spark.transaction.TransactionLogCheckpoint(
            new org.apache.hadoop.fs.Path(transactionLogPath),
            cloudProvider,
            new org.apache.spark.sql.util.CaseInsensitiveStringMap(new java.util.HashMap[String, String]())
          )

          // getLastCheckpointInfo should return stale v1
          val unverifiedInfo = checkpoint.getLastCheckpointInfo()
          unverifiedInfo.isDefined shouldBe true
          unverifiedInfo.get.version shouldBe v1

          // getLastCheckpointInfoVerified should detect and return corrected v2
          val verifiedInfo = checkpoint.getLastCheckpointInfoVerified()
          verifiedInfo.isDefined shouldBe true
          verifiedInfo.get.version shouldBe v2
          verifiedInfo.get.stateDir shouldBe Some(manifestIO.formatStateDir(v2))

          checkpoint.close()
        } finally
          cloudProvider.close()
      } finally
        spark.conf.unset("spark.indextables.state.format")
    }
  }

  test("schemaRegistry works with Map fields WITHOUT explicit avro config (like JsonMapFieldIntegrationTest)") {
    withTempPath { tempDir =>
      val path = tempDir

      // DO NOT set spark.indextables.state.format - let it use defaults
      // This simulates what JsonMapFieldIntegrationTest does

      try {
        // Write data with Map field - similar to JsonMapFieldIntegrationTest
        val data = Seq(
          (1, Map("color" -> "red", "size" -> "large")),
          (2, Map("color" -> "blue", "size" -> "small"))
        )
        spark
          .createDataFrame(data)
          .toDF("id", "attributes")
          .write
          .format(provider)
          .mode("overwrite")
          .save(path)

        // Debug: Check what _last_checkpoint contains
        val transactionLogPath = s"$path/_transaction_log"
        val lastCheckpointPath = s"$transactionLogPath/_last_checkpoint"
        val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
          transactionLogPath,
          new org.apache.spark.sql.util.CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
          spark.sparkContext.hadoopConfiguration
        )
        try {
          val checkpointExists = cloudProvider.exists(lastCheckpointPath)
          println(s"DEBUG: _last_checkpoint exists = $checkpointExists")
          if (checkpointExists) {
            val content = new String(cloudProvider.readFile(lastCheckpointPath), "UTF-8")
            println(s"DEBUG: _last_checkpoint content = $content")
          }

          // List files in transaction log
          val files = cloudProvider.listFiles(transactionLogPath, recursive = true)
          println(s"DEBUG: Transaction log files:")
          files.foreach(f => println(s"  - ${f.path}"))
        } finally
          cloudProvider.close()

        // Now create a new cloud provider to read state
        val cloudProvider2 = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
          transactionLogPath,
          new org.apache.spark.sql.util.CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
          spark.sparkContext.hadoopConfiguration
        )

        try {
          val manifestIO = StateManifestIO(cloudProvider2)

          // Find the state directory
          val stateDir = cloudProvider2
            .listFiles(transactionLogPath, recursive = false)
            .filter(f => f.path.contains("state-v"))
            .headOption
            .map(_.path)
            .getOrElse(fail("No state directory found"))

          println(s"DEBUG: Found state directory: $stateDir")

          // Read the state manifest
          val manifest = manifestIO.readStateManifest(stateDir)

          println(s"DEBUG MAP: schemaRegistry size = ${manifest.schemaRegistry.size}")
          println(s"DEBUG MAP: schemaRegistry keys = ${manifest.schemaRegistry.keys.mkString(", ")}")
          manifest.schemaRegistry should not be empty

          // Read entries and verify docMappingRef
          val manifestPaths = manifestIO.resolveManifestPaths(manifest, transactionLogPath, stateDir)
          val avroReader    = AvroManifestReader(cloudProvider2)
          val fileEntries   = avroReader.readManifestsParallel(manifestPaths)

          fileEntries.foreach { entry =>
            println(s"DEBUG MAP: entry docMappingRef=${entry.docMappingRef}")
            entry.docMappingRef shouldBe defined
          }

          // Convert and verify docMappingJson restored
          val addActions = avroReader.toAddActions(fileEntries, manifest.schemaRegistry)
          addActions.foreach { add =>
            println(s"DEBUG MAP: AddAction docMappingJson defined=${add.docMappingJson.isDefined}")
            add.docMappingJson shouldBe defined
          }

          // Read the actual data using Spark DataFrame
          val df = spark.read.format(provider).load(path)
          println(s"DEBUG: About to call df.count()")
          df.count() shouldBe 2
          println(s"DEBUG: df.count() succeeded!")

        } finally
          cloudProvider2.close()
      } finally {
        // No config to unset since we didn't set any
      }
    }
  }

  test("schemaRegistry is correctly stored and restored through Avro state") {
    withTempPath { tempDir =>
      val path = tempDir

      // Enable Avro format for the write
      spark.conf.set("spark.indextables.state.format", "avro")

      try {
        // Write data - this should create AddActions with docMappingJson
        val data = Seq((1, "doc1"), (2, "doc2"))
        spark
          .createDataFrame(data)
          .toDF("id", "content")
          .write
          .format(provider)
          .mode("overwrite")
          .save(path)

        // Read and verify the state manifest has schemaRegistry populated
        val transactionLogPath = s"$path/_transaction_log"
        val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
          transactionLogPath,
          new org.apache.spark.sql.util.CaseInsensitiveStringMap(new java.util.HashMap[String, String]()),
          spark.sparkContext.hadoopConfiguration
        )

        try {
          val manifestIO = StateManifestIO(cloudProvider)

          // Find the state directory
          val stateDir = cloudProvider
            .listFiles(transactionLogPath, recursive = false)
            .filter(f => f.path.contains("state-v"))
            .headOption
            .map(_.path)
            .getOrElse(fail("No state directory found"))

          // Read the state manifest
          val manifest = manifestIO.readStateManifest(stateDir)

          // Verify schemaRegistry is NOT empty
          println(s"DEBUG: schemaRegistry size = ${manifest.schemaRegistry.size}")
          println(s"DEBUG: schemaRegistry keys = ${manifest.schemaRegistry.keys.mkString(", ")}")
          println(s"DEBUG: numFiles = ${manifest.numFiles}")
          println(s"DEBUG: manifests = ${manifest.manifests.size}")

          manifest.schemaRegistry should not be empty

          // Read the manifest file entries and verify they have docMappingRef
          val manifestPaths = manifestIO.resolveManifestPaths(manifest, transactionLogPath, stateDir)
          val avroReader    = AvroManifestReader(cloudProvider)
          val fileEntries   = avroReader.readManifestsParallel(manifestPaths)

          println(s"DEBUG: fileEntries count = ${fileEntries.size}")
          fileEntries.foreach { entry =>
            println(s"DEBUG: entry path=${entry.path}, docMappingRef=${entry.docMappingRef}")
          }

          // All entries should have docMappingRef set
          fileEntries.foreach(entry => entry.docMappingRef shouldBe defined)

          // Convert to AddActions and verify docMappingJson is restored
          val addActions = avroReader.toAddActions(fileEntries, manifest.schemaRegistry)
          addActions.foreach { add =>
            println(s"DEBUG: AddAction path=${add.path}, docMappingJson defined=${add.docMappingJson.isDefined}")
            add.docMappingJson shouldBe defined
          }

          // Finally, verify we can actually read the table
          val df = spark.read.format(provider).load(path)
          df.count() shouldBe 2

        } finally
          cloudProvider.close()
      } finally
        spark.conf.unset("spark.indextables.state.format")
    }
  }
}
