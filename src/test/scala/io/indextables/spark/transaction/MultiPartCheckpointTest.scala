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

package io.indextables.spark.transaction

import io.indextables.spark.TestBase

class MultiPartCheckpointTest extends TestBase {

  test("single-part checkpoint for small action count") {
    withTempPath { tempPath =>
      val tablePath = new org.apache.hadoop.fs.Path(tempPath)
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val transactionLogPath = new org.apache.hadoop.fs.Path(tablePath, "_transaction_log")
        cloudProvider.createDirectory(transactionLogPath.toString)

        // Create checkpoint with small actionsPerPart for testing
        val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
          java.util.Map.of(
            "spark.indextables.checkpoint.actionsPerPart",
            "100",
            "spark.indextables.checkpoint.multiPart.enabled",
            "true"
          )
        )
        val checkpoint = new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options)

        // Create 50 actions (less than actionsPerPart=100)
        val protocolAction = createTestProtocolAction()
        val metadataAction = createTestMetadataAction()
        val addActions     = (1 to 48).map(i => createTestAddAction(s"file$i.split"))

        val allActions: Seq[Action] = Seq(protocolAction, metadataAction) ++ addActions

        // Write checkpoint
        checkpoint.createCheckpoint(10L, allActions)

        // Check _last_checkpoint
        val lastCheckpointInfo = checkpoint.getLastCheckpointInfo()
        lastCheckpointInfo shouldBe defined
        lastCheckpointInfo.get.parts shouldBe None        // Single-part checkpoint
        lastCheckpointInfo.get.checkpointId shouldBe None // No checkpoint ID for single-part

        // Verify single checkpoint file exists (not a manifest)
        cloudProvider.exists(s"$transactionLogPath/00000000000000000010.checkpoint.json") shouldBe true

        // Read checkpoint and verify all actions recovered
        val restored = checkpoint.getActionsFromCheckpoint()
        restored shouldBe defined
        restored.get.length shouldBe 50
      } finally
        cloudProvider.close()
    }
  }

  test("multi-part checkpoint with manifest for large action count") {
    withTempPath { tempPath =>
      val tablePath = new org.apache.hadoop.fs.Path(tempPath)
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val transactionLogPath = new org.apache.hadoop.fs.Path(tablePath, "_transaction_log")
        cloudProvider.createDirectory(transactionLogPath.toString)

        // Create checkpoint with small actionsPerPart for testing
        val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
          java.util.Map.of(
            "spark.indextables.checkpoint.actionsPerPart",
            "50",
            "spark.indextables.checkpoint.multiPart.enabled",
            "true"
          )
        )
        val checkpoint = new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options)

        // Create 200 actions (will need multiple parts at 50 per part)
        val protocolAction = createTestProtocolAction()
        val metadataAction = createTestMetadataAction()
        val addActions     = (1 to 198).map(i => createTestAddAction(s"file$i.split"))

        val allActions: Seq[Action] = Seq(protocolAction, metadataAction) ++ addActions

        // Write checkpoint
        checkpoint.createCheckpoint(20L, allActions)

        // Check _last_checkpoint
        val lastCheckpointInfo = checkpoint.getLastCheckpointInfo()
        lastCheckpointInfo shouldBe defined
        lastCheckpointInfo.get.parts shouldBe defined
        lastCheckpointInfo.get.parts.get should be > 1
        lastCheckpointInfo.get.checkpointId shouldBe defined // Multi-part has checkpoint ID

        // Verify manifest file exists
        val manifestPath = s"$transactionLogPath/00000000000000000020.checkpoint.json"
        cloudProvider.exists(manifestPath) shouldBe true

        // Read manifest and verify parts exist
        val manifestContent = new String(cloudProvider.readFile(manifestPath), "UTF-8")
        val manifest =
          io.indextables.spark.util.JsonUtil.mapper.readValue(manifestContent, classOf[MultiPartCheckpointManifest])

        manifest.checkpointId shouldBe lastCheckpointInfo.get.checkpointId.get
        manifest.parts.length shouldBe lastCheckpointInfo.get.parts.get

        // Verify all part files exist
        manifest.parts.foreach(partFile => cloudProvider.exists(s"$transactionLogPath/$partFile") shouldBe true)

        // Read checkpoint and verify all actions recovered
        val restored = checkpoint.getActionsFromCheckpoint()
        restored shouldBe defined
        restored.get.length shouldBe 200

        // Verify specific action types
        val restoredProtocol = restored.get.collectFirst { case p: ProtocolAction => p }
        val restoredMetadata = restored.get.collectFirst { case m: MetadataAction => m }
        val restoredAdds     = restored.get.collect { case a: AddAction => a }

        restoredProtocol shouldBe defined
        restoredMetadata shouldBe defined
        restoredAdds.length shouldBe 198
      } finally
        cloudProvider.close()
    }
  }

  test("multi-part checkpoint with schema deduplication") {
    withTempPath { tempPath =>
      val tablePath = new org.apache.hadoop.fs.Path(tempPath)
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val transactionLogPath = new org.apache.hadoop.fs.Path(tablePath, "_transaction_log")
        cloudProvider.createDirectory(transactionLogPath.toString)

        val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
          java.util.Map.of(
            "spark.indextables.checkpoint.actionsPerPart",
            "30",
            "spark.indextables.checkpoint.multiPart.enabled",
            "true"
          )
        )
        val checkpoint = new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options)

        // Create large schema for deduplication testing
        val largeSchema =
          """{"fields":[""" + (1 to 100).map(i => s"""{"name":"field$i","type":"text"}""").mkString(",") + "]}"

        // Create actions with the same large schema
        val protocolAction = createTestProtocolAction()
        val metadataAction = createTestMetadataAction()
        val addActions = (1 to 100).map { i =>
          createTestAddAction(s"file$i.split").copy(docMappingJson = Some(largeSchema))
        }

        val allActions: Seq[Action] = Seq(protocolAction, metadataAction) ++ addActions

        // Write checkpoint
        checkpoint.createCheckpoint(30L, allActions)

        // Read checkpoint and verify schemas are restored
        val restored = checkpoint.getActionsFromCheckpoint()
        restored shouldBe defined

        val restoredAdds = restored.get.collect { case a: AddAction => a }
        restoredAdds.length shouldBe 100

        // All AddActions should have docMappingJson restored
        restoredAdds.foreach { add =>
          add.docMappingJson shouldBe Some(largeSchema)
          add.docMappingRef shouldBe None // Should be cleared after restoration
        }
      } finally
        cloudProvider.close()
    }
  }

  test("multi-part disabled falls back to single file") {
    withTempPath { tempPath =>
      val tablePath = new org.apache.hadoop.fs.Path(tempPath)
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val transactionLogPath = new org.apache.hadoop.fs.Path(tablePath, "_transaction_log")
        cloudProvider.createDirectory(transactionLogPath.toString)

        // Disable multi-part
        val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
          java.util.Map.of(
            "spark.indextables.checkpoint.actionsPerPart",
            "10",
            "spark.indextables.checkpoint.multiPart.enabled",
            "false"
          )
        )
        val checkpoint = new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options)

        // Create 100 actions (would be multi-part if enabled)
        val protocolAction = createTestProtocolAction()
        val metadataAction = createTestMetadataAction()
        val addActions     = (1 to 98).map(i => createTestAddAction(s"file$i.split"))

        val allActions: Seq[Action] = Seq(protocolAction, metadataAction) ++ addActions

        // Write checkpoint
        checkpoint.createCheckpoint(40L, allActions)

        // Check _last_checkpoint
        val lastCheckpointInfo = checkpoint.getLastCheckpointInfo()
        lastCheckpointInfo shouldBe defined
        lastCheckpointInfo.get.parts shouldBe None        // Single-part even with many actions
        lastCheckpointInfo.get.checkpointId shouldBe None // No checkpoint ID for single-part

        // Verify single checkpoint file exists
        cloudProvider.exists(s"$transactionLogPath/00000000000000000040.checkpoint.json") shouldBe true

        // Read checkpoint and verify all actions recovered
        val restored = checkpoint.getActionsFromCheckpoint()
        restored shouldBe defined
        restored.get.length shouldBe 100
      } finally
        cloudProvider.close()
    }
  }

  test("part count calculation is correct") {
    withTempPath { tempPath =>
      val tablePath = new org.apache.hadoop.fs.Path(tempPath)
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val transactionLogPath = new org.apache.hadoop.fs.Path(tablePath, "_transaction_log")
        cloudProvider.createDirectory(transactionLogPath.toString)

        val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
          java.util.Map.of(
            "spark.indextables.checkpoint.actionsPerPart",
            "100",
            "spark.indextables.checkpoint.multiPart.enabled",
            "true"
          )
        )
        val checkpoint = new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options)

        // Create 450 actions
        // Protocol(1) + Metadata(1) + 448 AddActions = 450 total
        // Part 1: Protocol + Metadata + 98 AddActions = 100
        // Part 2: 100 AddActions
        // Part 3: 100 AddActions
        // Part 4: 100 AddActions
        // Part 5: 50 AddActions
        // Total: 5 parts
        val protocolAction = createTestProtocolAction()
        val metadataAction = createTestMetadataAction()
        val addActions     = (1 to 448).map(i => createTestAddAction(s"file$i.split"))

        val allActions: Seq[Action] = Seq(protocolAction, metadataAction) ++ addActions

        // Write checkpoint
        checkpoint.createCheckpoint(50L, allActions)

        // Check part count
        val lastCheckpointInfo = checkpoint.getLastCheckpointInfo()
        lastCheckpointInfo shouldBe defined
        lastCheckpointInfo.get.parts shouldBe Some(5)

        // Read and verify
        val restored = checkpoint.getActionsFromCheckpoint()
        restored shouldBe defined
        restored.get.length shouldBe 450
      } finally
        cloudProvider.close()
    }
  }

  test("missing checkpoint part throws error on read") {
    withTempPath { tempPath =>
      val tablePath = new org.apache.hadoop.fs.Path(tempPath)
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val transactionLogPath = new org.apache.hadoop.fs.Path(tablePath, "_transaction_log")
        cloudProvider.createDirectory(transactionLogPath.toString)

        val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
          java.util.Map.of(
            "spark.indextables.checkpoint.actionsPerPart",
            "50",
            "spark.indextables.checkpoint.multiPart.enabled",
            "true"
          )
        )
        val checkpoint = new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options)

        // Create multi-part checkpoint
        val protocolAction = createTestProtocolAction()
        val metadataAction = createTestMetadataAction()
        val addActions     = (1 to 150).map(i => createTestAddAction(s"file$i.split"))

        val allActions: Seq[Action] = Seq(protocolAction, metadataAction) ++ addActions
        checkpoint.createCheckpoint(60L, allActions)

        // Get the manifest to find a part file to delete
        val lastCheckpointInfo = checkpoint.getLastCheckpointInfo()
        val checkpointId       = lastCheckpointInfo.get.checkpointId.get

        // Delete one of the parts (part 2)
        val partToDelete = f"00000000000000000060.checkpoint.$checkpointId.00002.json"
        cloudProvider.deleteFile(s"$transactionLogPath/$partToDelete")

        // Attempt to read should return None (checkpoint read catches exceptions)
        val restored = checkpoint.getActionsFromCheckpoint()
        restored shouldBe None
      } finally
        cloudProvider.close()
    }
  }

  test("backward compatibility: read legacy single-file checkpoint") {
    withTempPath { tempPath =>
      val tablePath = new org.apache.hadoop.fs.Path(tempPath)
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val transactionLogPath = new org.apache.hadoop.fs.Path(tablePath, "_transaction_log")
        cloudProvider.createDirectory(transactionLogPath.toString)

        // Create a legacy checkpoint manually (without parts or checkpointId field)
        val protocolAction = createTestProtocolAction()
        val metadataAction = createTestMetadataAction()
        val addActions     = (1 to 5).map(i => createTestAddAction(s"file$i.split"))

        val allActions: Seq[Action] = Seq(protocolAction, metadataAction) ++ addActions

        // Write checkpoint file directly using StreamingActionWriter
        val checkpointPath = s"$transactionLogPath/00000000000000000070.checkpoint.json"
        StreamingActionWriter.writeActionsStreaming(
          actions = allActions,
          cloudProvider = cloudProvider,
          path = checkpointPath,
          codec = None,
          ifNotExists = false
        )

        // Write legacy _last_checkpoint (without parts or checkpointId field)
        val legacyLastCheckpoint =
          """{"version":70,"size":7,"sizeInBytes":1000,"numFiles":5,"createdTime":1234567890000}"""
        cloudProvider.writeFile(
          s"$transactionLogPath/_last_checkpoint",
          legacyLastCheckpoint.getBytes("UTF-8")
        )

        // Read with new checkpoint code
        val options    = new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
        val checkpoint = new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options)

        val lastCheckpointInfo = checkpoint.getLastCheckpointInfo()
        lastCheckpointInfo shouldBe defined
        lastCheckpointInfo.get.parts shouldBe None        // Legacy: no parts field
        lastCheckpointInfo.get.checkpointId shouldBe None // Legacy: no checkpointId field

        val restored = checkpoint.getActionsFromCheckpoint()
        restored shouldBe defined
        restored.get.length shouldBe 7

        // Verify content
        val restoredProtocol = restored.get.collectFirst { case p: ProtocolAction => p }
        val restoredMetadata = restored.get.collectFirst { case m: MetadataAction => m }
        val restoredAdds     = restored.get.collect { case a: AddAction => a }

        restoredProtocol shouldBe defined
        restoredMetadata shouldBe defined
        restoredAdds.length shouldBe 5
      } finally
        cloudProvider.close()
    }
  }

  test("checkpoint manifest includes format field") {
    withTempPath { tempPath =>
      val tablePath = new org.apache.hadoop.fs.Path(tempPath)
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val transactionLogPath = new org.apache.hadoop.fs.Path(tablePath, "_transaction_log")
        cloudProvider.createDirectory(transactionLogPath.toString)

        val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
          java.util.Map.of(
            "spark.indextables.checkpoint.actionsPerPart",
            "50",
            "spark.indextables.checkpoint.multiPart.enabled",
            "true"
          )
        )
        val checkpoint = new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options)

        // Create multi-part checkpoint
        val protocolAction = createTestProtocolAction()
        val metadataAction = createTestMetadataAction()
        val addActions     = (1 to 100).map(i => createTestAddAction(s"file$i.split"))

        val allActions: Seq[Action] = Seq(protocolAction, metadataAction) ++ addActions
        checkpoint.createCheckpoint(80L, allActions)

        // Read manifest and verify format field
        val manifestPath    = s"$transactionLogPath/00000000000000000080.checkpoint.json"
        val manifestContent = new String(cloudProvider.readFile(manifestPath), "UTF-8")
        val manifest =
          io.indextables.spark.util.JsonUtil.mapper.readValue(manifestContent, classOf[MultiPartCheckpointManifest])

        manifest.format shouldBe "json" // Default format
      } finally
        cloudProvider.close()
    }
  }

  test("upgrade from V2 checkpoint to V3 with schema deduplication") {
    withTempPath { tempPath =>
      val tablePath = new org.apache.hadoop.fs.Path(tempPath)
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val transactionLogPath = new org.apache.hadoop.fs.Path(tablePath, "_transaction_log")
        cloudProvider.createDirectory(transactionLogPath.toString)

        // Step 1: Create a legacy V2 checkpoint manually (no schema dedup)
        val v2Protocol     = ProtocolAction(minReaderVersion = 2, minWriterVersion = 2)
        val metadataAction = createTestMetadataAction()
        val largeSchema    = """{"fields":[{"name":"field1","type":"text"}]}"""
        val addActionsV2 = (1 to 5).map { i =>
          // V2 checkpoint stores docMappingJson inline (no dedup)
          createTestAddAction(s"v2_file$i.split").copy(docMappingJson = Some(largeSchema))
        }

        val v2Actions: Seq[Action] = Seq(v2Protocol, metadataAction) ++ addActionsV2

        // Write V2 checkpoint directly (no dedup, no multi-part)
        val v2CheckpointPath = s"$transactionLogPath/00000000000000000010.checkpoint.json"
        StreamingActionWriter.writeActionsStreaming(
          actions = v2Actions,
          cloudProvider = cloudProvider,
          path = v2CheckpointPath,
          codec = None,
          ifNotExists = false
        )

        // Write V2 _last_checkpoint
        val v2LastCheckpoint = """{"version":10,"size":7,"sizeInBytes":1000,"numFiles":5,"createdTime":1234567890000}"""
        cloudProvider.writeFile(s"$transactionLogPath/_last_checkpoint", v2LastCheckpoint.getBytes("UTF-8"))

        // Step 2: Read V2 checkpoint with new code
        val options    = new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
        val checkpoint = new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options)

        val restoredV2 = checkpoint.getActionsFromCheckpoint()
        restoredV2 shouldBe defined
        restoredV2.get.length shouldBe 7

        // V2 checkpoint should still have docMappingJson inline
        val v2Adds = restoredV2.get.collect { case a: AddAction => a }
        v2Adds.foreach(add => add.docMappingJson shouldBe Some(largeSchema))

        // Step 3: Create new V3 checkpoint with same data + new data
        val v3Protocol = ProtocolAction(minReaderVersion = 3, minWriterVersion = 3)
        val newAddActions = (1 to 10).map { i =>
          createTestAddAction(s"v3_file$i.split").copy(docMappingJson = Some(largeSchema))
        }

        val v3Actions: Seq[Action] = Seq(v3Protocol, metadataAction) ++ addActionsV2 ++ newAddActions
        checkpoint.createCheckpoint(20L, v3Actions)

        // Step 4: Verify V3 checkpoint uses schema deduplication
        val lastCheckpointInfo = checkpoint.getLastCheckpointInfo()
        lastCheckpointInfo shouldBe defined
        lastCheckpointInfo.get.version shouldBe 20L

        val restoredV3 = checkpoint.getActionsFromCheckpoint()
        restoredV3 shouldBe defined
        restoredV3.get.length shouldBe 17 // protocol + metadata + 15 adds

        // All AddActions should have docMappingJson restored after dedup
        val v3Adds = restoredV3.get.collect { case a: AddAction => a }
        v3Adds.length shouldBe 15
        v3Adds.foreach { add =>
          add.docMappingJson shouldBe Some(largeSchema)
          add.docMappingRef shouldBe None // Should be cleared after restoration
        }
      } finally
        cloudProvider.close()
    }
  }

  test("protocol is upgraded to V4 when using multi-part checkpoint") {
    withTempPath { tempPath =>
      val tablePath = new org.apache.hadoop.fs.Path(tempPath)
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val transactionLogPath = new org.apache.hadoop.fs.Path(tablePath, "_transaction_log")
        cloudProvider.createDirectory(transactionLogPath.toString)

        val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
          java.util.Map.of(
            "spark.indextables.checkpoint.actionsPerPart",
            "50",
            "spark.indextables.checkpoint.multiPart.enabled",
            "true"
          )
        )
        val checkpoint = new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options)

        // Start with V2 protocol
        val v2Protocol     = ProtocolAction(minReaderVersion = 2, minWriterVersion = 2)
        val metadataAction = createTestMetadataAction()
        val addActions     = (1 to 150).map(i => createTestAddAction(s"file$i.split"))

        val allActions: Seq[Action] = Seq(v2Protocol, metadataAction) ++ addActions
        checkpoint.createCheckpoint(90L, allActions)

        // Verify multi-part checkpoint was created
        val lastCheckpointInfo = checkpoint.getLastCheckpointInfo()
        lastCheckpointInfo shouldBe defined
        lastCheckpointInfo.get.parts shouldBe defined
        lastCheckpointInfo.get.parts.get should be > 1

        // Read checkpoint and verify protocol was upgraded to V4 (includes V3 features + Avro state)
        val restored = checkpoint.getActionsFromCheckpoint()
        restored shouldBe defined

        val restoredProtocol = restored.get.collectFirst { case p: ProtocolAction => p }
        restoredProtocol shouldBe defined
        restoredProtocol.get.minReaderVersion shouldBe 4 // Upgraded from V2 to V4
        restoredProtocol.get.minWriterVersion shouldBe 4
      } finally
        cloudProvider.close()
    }
  }

  test("protocol is upgraded to V4 when using schema deduplication") {
    withTempPath { tempPath =>
      val tablePath = new org.apache.hadoop.fs.Path(tempPath)
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val transactionLogPath = new org.apache.hadoop.fs.Path(tablePath, "_transaction_log")
        cloudProvider.createDirectory(transactionLogPath.toString)

        // Single-part checkpoint but with schema deduplication
        val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
          java.util.Map.of(
            "spark.indextables.checkpoint.actionsPerPart",
            "1000",
            "spark.indextables.checkpoint.multiPart.enabled",
            "false"
          )
        )
        val checkpoint = new TransactionLogCheckpoint(transactionLogPath, cloudProvider, options)

        // Start with V2 protocol but with docMappingJson (triggers schema dedup)
        val v2Protocol     = ProtocolAction(minReaderVersion = 2, minWriterVersion = 2)
        val metadataAction = createTestMetadataAction()
        val largeSchema    = """{"fields":[{"name":"field1","type":"text"}]}"""
        val addActions = (1 to 10).map { i =>
          createTestAddAction(s"file$i.split").copy(docMappingJson = Some(largeSchema))
        }

        val allActions: Seq[Action] = Seq(v2Protocol, metadataAction) ++ addActions
        checkpoint.createCheckpoint(100L, allActions)

        // Verify single-part checkpoint (not multi-part)
        val lastCheckpointInfo = checkpoint.getLastCheckpointInfo()
        lastCheckpointInfo shouldBe defined
        lastCheckpointInfo.get.parts shouldBe None // Single-part

        // Read checkpoint and verify protocol was upgraded to V4 due to schema dedup
        val restored = checkpoint.getActionsFromCheckpoint()
        restored shouldBe defined

        val restoredProtocol = restored.get.collectFirst { case p: ProtocolAction => p }
        restoredProtocol shouldBe defined
        restoredProtocol.get.minReaderVersion shouldBe 4 // Upgraded from V2 to V4
        restoredProtocol.get.minWriterVersion shouldBe 4
      } finally
        cloudProvider.close()
    }
  }

  // Helper methods
  private def createTestAddAction(path: String): AddAction =
    AddAction(
      path = path,
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(100L)
    )

  private def createTestMetadataAction(): MetadataAction =
    MetadataAction(
      id = "test-id",
      name = Some("test-table"),
      description = None,
      format = FileFormat("indextables", Map.empty),
      schemaString = """{"type":"struct","fields":[]}""",
      partitionColumns = Seq.empty,
      configuration = Map.empty,
      createdTime = Some(System.currentTimeMillis())
    )

  private def createTestProtocolAction(): ProtocolAction =
    ProtocolAction(
      minReaderVersion = 2,
      minWriterVersion = 2
    )
}
