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

class SchemaDeduplicationTest extends TestBase {

  // Sample schemas for testing
  private val smallSchema =
    """{"fields":[{"name":"id","type":"i64","fast":true}]}"""

  private val largeSchema =
    """{"fields":[""" + (1 to 100).map(i => s"""{"name":"field$i","type":"text"}""").mkString(",") + "]}"

  test("computeSchemaHash should produce consistent hashes") {
    val hash1 = SchemaDeduplication.computeSchemaHash(smallSchema)
    val hash2 = SchemaDeduplication.computeSchemaHash(smallSchema)

    hash1 shouldBe hash2
    hash1.length shouldBe 16 // 16 character hash
  }

  test("computeSchemaHash should produce different hashes for different schemas") {
    val hash1 = SchemaDeduplication.computeSchemaHash(smallSchema)
    val hash2 = SchemaDeduplication.computeSchemaHash(largeSchema)

    hash1 should not be hash2
  }

  test("deduplicateSchemas should replace docMappingJson with docMappingRef") {
    val addAction1 = createTestAddAction("file1.split").copy(docMappingJson = Some(smallSchema))
    val addAction2 = createTestAddAction("file2.split").copy(docMappingJson = Some(smallSchema))
    val addAction3 = createTestAddAction("file3.split").copy(docMappingJson = Some(largeSchema))

    val actions: Seq[Action] = Seq(addAction1, addAction2, addAction3)

    val (deduplicated, registry) = SchemaDeduplication.deduplicateSchemas(actions)

    // Should have deduplicated actions
    deduplicated.length shouldBe 3

    // All AddActions should have docMappingRef instead of docMappingJson
    val deduplicatedAdds = deduplicated.collect { case a: AddAction => a }
    deduplicatedAdds.foreach { add =>
      add.docMappingJson shouldBe None
      add.docMappingRef shouldBe defined
    }

    // Should have 2 unique schemas in registry
    registry.size shouldBe 2
    registry.keys.foreach(_.startsWith(SchemaDeduplication.SCHEMA_KEY_PREFIX) shouldBe true)
  }

  test("restoreSchemas should restore docMappingJson from registry") {
    val hash1 = SchemaDeduplication.computeSchemaHash(smallSchema)
    val hash2 = SchemaDeduplication.computeSchemaHash(largeSchema)

    val addAction1 = createTestAddAction("file1.split").copy(
      docMappingJson = None,
      docMappingRef = Some(hash1)
    )
    val addAction2 = createTestAddAction("file2.split").copy(
      docMappingJson = None,
      docMappingRef = Some(hash2)
    )

    val registry = Map(
      s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}$hash1" -> smallSchema,
      s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}$hash2" -> largeSchema
    )

    val restored = SchemaDeduplication.restoreSchemas(Seq(addAction1, addAction2), registry)

    val restoredAdds = restored.collect { case a: AddAction => a }
    restoredAdds.length shouldBe 2

    // docMappingJson should be restored
    restoredAdds(0).docMappingJson shouldBe Some(smallSchema)
    restoredAdds(1).docMappingJson shouldBe Some(largeSchema)

    // docMappingRef should be cleared
    restoredAdds(0).docMappingRef shouldBe None
    restoredAdds(1).docMappingRef shouldBe None
  }

  test("deduplicateSchemas should preserve existing registry entries") {
    val hash1 = SchemaDeduplication.computeSchemaHash(smallSchema)
    val existingRegistry = Map(
      s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}$hash1" -> smallSchema
    )

    // New action with the same schema
    val addAction = createTestAddAction("file.split").copy(docMappingJson = Some(smallSchema))

    val (deduplicated, registry) = SchemaDeduplication.deduplicateSchemas(Seq(addAction), existingRegistry)

    // Should still only have 1 entry (no duplicate)
    registry.size shouldBe 1
    registry.values.head shouldBe smallSchema
  }

  test("needsSchemaRegistration should detect unregistered schemas") {
    val existingRegistry = Map(
      s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}${SchemaDeduplication.computeSchemaHash(smallSchema)}" -> smallSchema
    )

    // smallSchema is already registered
    SchemaDeduplication.needsSchemaRegistration(smallSchema, existingRegistry) shouldBe false

    // largeSchema is not registered
    SchemaDeduplication.needsSchemaRegistration(largeSchema, existingRegistry) shouldBe true
  }

  test("estimateSavings should calculate size reduction") {
    // Create 10 actions with the same large schema
    val actions = (1 to 10).map(i => createTestAddAction(s"file$i.split").copy(docMappingJson = Some(largeSchema)))

    val (originalSize, deduplicatedSize) = SchemaDeduplication.estimateSavings(actions)

    // Original should be 10x the schema size
    originalSize shouldBe (largeSchema.length * 10L)

    // Deduplicated should be much smaller (1 schema + 10 refs)
    deduplicatedSize should be < originalSize
    deduplicatedSize should be < (originalSize / 5) // At least 5x reduction
  }

  test("deduplicateSchemas should handle actions without schemas") {
    val addWithSchema    = createTestAddAction("file1.split").copy(docMappingJson = Some(smallSchema))
    val addWithoutSchema = createTestAddAction("file2.split")
    val protocolAction   = createTestProtocolAction()

    val actions: Seq[Action] = Seq(protocolAction, addWithSchema, addWithoutSchema)

    val (deduplicated, registry) = SchemaDeduplication.deduplicateSchemas(actions)

    deduplicated.length shouldBe 3
    registry.size shouldBe 1 // Only the schema from addWithSchema

    // Protocol action should be unchanged
    deduplicated(0) shouldBe protocolAction

    // AddAction without schema should be unchanged
    val addWithoutSchemaResult = deduplicated(2).asInstanceOf[AddAction]
    addWithoutSchemaResult.docMappingJson shouldBe None
    addWithoutSchemaResult.docMappingRef shouldBe None
  }

  test("restoreSchemas should handle missing schema gracefully") {
    val missingHash = "nonexistenthash1"

    val addAction = createTestAddAction("file.split").copy(
      docMappingJson = None,
      docMappingRef = Some(missingHash)
    )

    val registry = Map.empty[String, String]

    // Should return action unchanged if schema not found
    val restored    = SchemaDeduplication.restoreSchemas(Seq(addAction), registry)
    val restoredAdd = restored.head.asInstanceOf[AddAction]

    // docMappingRef should still be present (couldn't restore)
    restoredAdd.docMappingRef shouldBe Some(missingHash)
    restoredAdd.docMappingJson shouldBe None
  }

  test("extractSchemaRegistry should extract only schema entries") {
    val configuration = Map(
      "spark.indextables.some.config"                  -> "value",
      s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}hash1" -> smallSchema,
      s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}hash2" -> largeSchema,
      "other.config"                                   -> "other"
    )

    val extracted = SchemaDeduplication.extractSchemaRegistry(configuration)

    extracted.size shouldBe 2
    extracted("hash1") shouldBe smallSchema
    extracted("hash2") shouldBe largeSchema
  }

  test("checkpoint roundtrip with schema deduplication") {
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

        val checkpoint = new TransactionLogCheckpoint(
          transactionLogPath,
          cloudProvider,
          new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap())
        )

        // Create actions with schema
        val protocolAction = createTestProtocolAction()
        val metadataAction = createTestMetadataAction()
        val addActions = (1 to 5).map { i =>
          createTestAddAction(s"file$i.split").copy(docMappingJson = Some(largeSchema))
        }

        val allActions: Seq[Action] = Seq(protocolAction, metadataAction) ++ addActions

        // Write checkpoint (should deduplicate schemas)
        checkpoint.createCheckpoint(10L, allActions)

        // Read checkpoint (should restore schemas)
        val restored = checkpoint.getActionsFromCheckpoint()

        restored shouldBe defined
        val restoredActions = restored.get

        // Verify all AddActions have docMappingJson restored
        val restoredAdds = restoredActions.collect { case a: AddAction => a }
        restoredAdds.length shouldBe 5
        restoredAdds.foreach { add =>
          add.docMappingJson shouldBe Some(largeSchema)
          add.docMappingRef shouldBe None // Should be cleared after restoration
        }
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

  test("transaction log roundtrip with schema deduplication - simulates merge scenario") {
    withTempPath { tempPath =>
      val tablePath = new org.apache.hadoop.fs.Path(tempPath)

      // Create options that allow direct TransactionLog usage (for testing)
      val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
        java.util.Map.of(
          "spark.indextables.transaction.allowDirectUsage",
          "true",
          "spark.indextables.checkpoint.enabled",
          "false" // Disable checkpoints to test transaction log directly
        )
      )

      val txLog = new TransactionLog(tablePath, spark, options)

      try {
        // Initialize the table with a schema
        val schema = new org.apache.spark.sql.types.StructType()
          .add("id", org.apache.spark.sql.types.LongType)
          .add("name", org.apache.spark.sql.types.StringType)

        txLog.initialize(schema)

        // Write initial files with docMappingJson (simulates normal write)
        val initialAddActions = (1 to 3).map { i =>
          createTestAddAction(s"split$i.split").copy(docMappingJson = Some(largeSchema))
        }
        txLog.addFiles(initialAddActions)

        // Verify files can be read back with schemas restored
        val filesAfterWrite = txLog.listFiles()
        filesAfterWrite.length shouldBe 3
        filesAfterWrite.foreach(file => file.docMappingJson shouldBe Some(largeSchema))

        // Simulate a merge operation:
        // 1. Remove old splits
        // 2. Add new merged split with docMappingJson
        val removeActions = initialAddActions.map { add =>
          RemoveAction(
            path = add.path,
            deletionTimestamp = Some(System.currentTimeMillis()),
            dataChange = true,
            extendedFileMetadata = None,
            partitionValues = Some(add.partitionValues),
            size = Some(add.size)
          )
        }

        // New merged split - this is what tantivy4java merge produces
        val mergedAddAction = createTestAddAction("merged.split").copy(
          docMappingJson = Some(largeSchema), // Same schema as source splits
          size = 3000L,
          numRecords = Some(300L)
        )

        // Commit the merge (removes + add)
        txLog.commitMergeSplits(removeActions, Seq(mergedAddAction))

        // Verify files can be read back after merge
        val filesAfterMerge = txLog.listFiles()
        filesAfterMerge.length shouldBe 1
        filesAfterMerge.head.path shouldBe "merged.split"
        filesAfterMerge.head.docMappingJson shouldBe Some(largeSchema)
      } finally
        txLog.close()
    }
  }

  test("transaction log roundtrip with DIFFERENT schema after merge") {
    // This test simulates the case where the merged split has a DIFFERENT schema
    // (e.g., tantivy4java produces slightly different docMapping)
    withTempPath { tempPath =>
      val tablePath = new org.apache.hadoop.fs.Path(tempPath)

      val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
        java.util.Map.of(
          "spark.indextables.transaction.allowDirectUsage",
          "true",
          "spark.indextables.checkpoint.enabled",
          "false"
        )
      )

      val txLog = new TransactionLog(tablePath, spark, options)

      try {
        val schema = new org.apache.spark.sql.types.StructType()
          .add("id", org.apache.spark.sql.types.LongType)

        txLog.initialize(schema)

        // Write initial files with schema A
        val schemaA = """{"fields":[{"name":"id","type":"i64","fast":true}]}"""
        val initialAddActions = (1 to 3).map { i =>
          createTestAddAction(s"split$i.split").copy(docMappingJson = Some(schemaA))
        }
        txLog.addFiles(initialAddActions)

        // Verify initial files
        val filesAfterWrite = txLog.listFiles()
        filesAfterWrite.length shouldBe 3

        // Simulate merge with a DIFFERENT schema B
        // (This could happen if tantivy4java produces a different JSON structure)
        val schemaB = """{"fields":[{"name":"id","type":"i64","fast":true,"stored":true}]}"""

        val removeActions = initialAddActions.map { add =>
          RemoveAction(
            path = add.path,
            deletionTimestamp = Some(System.currentTimeMillis()),
            dataChange = true,
            extendedFileMetadata = None,
            partitionValues = Some(add.partitionValues),
            size = Some(add.size)
          )
        }

        val mergedAddAction = createTestAddAction("merged.split").copy(
          docMappingJson = Some(schemaB), // Different schema!
          size = 3000L,
          numRecords = Some(300L)
        )

        // Commit the merge
        txLog.commitMergeSplits(removeActions, Seq(mergedAddAction))

        // Verify files can be read back with the new schema
        val filesAfterMerge = txLog.listFiles()
        filesAfterMerge.length shouldBe 1
        filesAfterMerge.head.path shouldBe "merged.split"
        filesAfterMerge.head.docMappingJson shouldBe Some(schemaB)
      } finally
        txLog.close()
    }
  }

  test("OptimizedTransactionLog merge with schema deduplication") {
    // This test specifically uses OptimizedTransactionLog (the production code path)
    // to ensure schema deduplication works correctly during merge operations
    withTempPath { tempPath =>
      val tablePath = new org.apache.hadoop.fs.Path(tempPath)

      val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
        java.util.Map.of(
          "spark.indextables.checkpoint.enabled",
          "false"
        )
      )

      val txLog = new OptimizedTransactionLog(tablePath, spark, options)

      try {
        // Initialize the table
        val schema = new org.apache.spark.sql.types.StructType()
          .add("id", org.apache.spark.sql.types.LongType)

        txLog.initialize(schema)

        // Write initial files with docMappingJson
        val initialAddActions = (1 to 3).map { i =>
          createTestAddAction(s"split$i.split").copy(docMappingJson = Some(largeSchema))
        }
        txLog.addFiles(initialAddActions)

        // Verify files can be read
        val filesAfterWrite = txLog.listFiles()
        filesAfterWrite.length shouldBe 3
        filesAfterWrite.foreach(file => file.docMappingJson shouldBe Some(largeSchema))

        // Simulate merge operation
        val removeActions = filesAfterWrite.map { add =>
          RemoveAction(
            path = add.path,
            deletionTimestamp = Some(System.currentTimeMillis()),
            dataChange = true,
            extendedFileMetadata = None,
            partitionValues = Some(add.partitionValues),
            size = Some(add.size)
          )
        }

        val mergedAddAction = createTestAddAction("merged.split").copy(
          docMappingJson = Some(largeSchema),
          size = 3000L,
          numRecords = Some(300L)
        )

        // Commit the merge
        txLog.commitMergeSplits(removeActions, Seq(mergedAddAction))

        // Verify files can be read back after merge
        val filesAfterMerge = txLog.listFiles()
        filesAfterMerge.length shouldBe 1
        filesAfterMerge.head.path shouldBe "merged.split"
        filesAfterMerge.head.docMappingJson shouldBe Some(largeSchema)
      } finally
        txLog.close()
    }
  }

  test("merge on table written BEFORE schema deduplication - simulates legacy table upgrade") {
    // This test simulates the real-world scenario where:
    // 1. Table was created BEFORE schema deduplication was implemented
    // 2. AddActions have docMappingJson directly (no docMappingRef)
    // 3. MetadataAction.configuration is empty (no schema registry)
    // 4. After code upgrade, merge runs and produces deduped AddActions
    // 5. Reading should work correctly
    withTempPath { tempPath =>
      val tablePath          = new org.apache.hadoop.fs.Path(tempPath)
      val transactionLogPath = new org.apache.hadoop.fs.Path(tablePath, "_transaction_log")

      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        // Step 1: Manually create a "legacy" transaction log without schema deduplication
        // This simulates a table written before the schema deduplication feature
        cloudProvider.createDirectory(transactionLogPath.toString)

        val schemaA = """{"fields":[{"name":"id","type":"i64","fast":true}]}"""

        // Version 0: Protocol + Metadata (no schema registry in configuration)
        val protocolAction = createTestProtocolAction()
        val metadataAction = MetadataAction(
          id = "legacy-table",
          name = Some("legacy"),
          description = None,
          format = FileFormat("indextables", Map.empty),
          schemaString = """{"type":"struct","fields":[{"name":"id","type":"long"}]}""",
          partitionColumns = Seq.empty,
          configuration = Map.empty, // IMPORTANT: No schema registry - legacy table
          createdTime = Some(System.currentTimeMillis())
        )

        // Write version 0 directly (bypassing schema deduplication)
        StreamingActionWriter.writeActionsStreaming(
          actions = Seq(protocolAction, metadataAction),
          cloudProvider = cloudProvider,
          path = new org.apache.hadoop.fs.Path(transactionLogPath, "00000000000000000000.json").toString,
          codec = None,
          ifNotExists = true
        )

        // Version 1: Add files with docMappingJson (legacy format - no deduplication)
        val legacyAddActions = (1 to 3).map { i =>
          createTestAddAction(s"split$i.split").copy(docMappingJson = Some(schemaA))
        }

        StreamingActionWriter.writeActionsStreaming(
          actions = legacyAddActions,
          cloudProvider = cloudProvider,
          path = new org.apache.hadoop.fs.Path(transactionLogPath, "00000000000000000001.json").toString,
          codec = None,
          ifNotExists = true
        )

        // Step 2: Now open the table with the NEW code (schema deduplication enabled)
        val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(
          java.util.Map.of(
            "spark.indextables.transaction.allowDirectUsage",
            "true",
            "spark.indextables.checkpoint.enabled",
            "false"
          )
        )

        val txLog = new TransactionLog(tablePath, spark, options)

        try {
          // Verify we can read the legacy files
          val legacyFiles = txLog.listFiles()
          legacyFiles.length shouldBe 3
          legacyFiles.foreach { file =>
            file.docMappingJson shouldBe Some(schemaA) // Should have docMappingJson directly
          }

          // Step 3: Simulate a merge operation
          val removeActions = legacyFiles.map { add =>
            RemoveAction(
              path = add.path,
              deletionTimestamp = Some(System.currentTimeMillis()),
              dataChange = true,
              extendedFileMetadata = None,
              partitionValues = Some(add.partitionValues),
              size = Some(add.size)
            )
          }

          val mergedAddAction = createTestAddAction("merged.split").copy(
            docMappingJson = Some(schemaA),
            size = 3000L,
            numRecords = Some(300L)
          )

          // Commit the merge - this should apply schema deduplication
          txLog.commitMergeSplits(removeActions, Seq(mergedAddAction))

          // Step 4: Verify we can read back after merge
          val filesAfterMerge = txLog.listFiles()
          filesAfterMerge.length shouldBe 1
          filesAfterMerge.head.path shouldBe "merged.split"
          filesAfterMerge.head.docMappingJson shouldBe Some(schemaA)
        } finally
          txLog.close()
      } finally
        cloudProvider.close()
    }
  }

  test("multiple transactions with same schema should produce single hash in checkpoint") {
    // This test validates that when multiple transactions write with the same schema,
    // the checkpoint contains only ONE copy of that schema (not duplicates)
    withTempPath { tempPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create a DataFrame with a medium schema (50 fields)
      val numFields  = 50
      val numRecords = 10

      def createDF(batchNum: Int): org.apache.spark.sql.DataFrame = {
        val rows = (1 to numRecords).map { i =>
          val id     = (batchNum - 1) * numRecords + i
          val values = (1 to numFields).map(f => s"value_${id}_$f")
          org.apache.spark.sql.Row.fromSeq(id.toLong +: values)
        }
        val schema = org.apache.spark.sql.types.StructType(
          org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.LongType) +:
            (1 to numFields).map(f =>
              org.apache.spark.sql.types.StructField(s"field_$f", org.apache.spark.sql.types.StringType)
            )
        )
        spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      }

      // Write 12 batches to trigger checkpoint at version 10
      (1 to 12).foreach { i =>
        val mode = if (i == 1) "overwrite" else "append"
        val df   = createDF(i)
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode(mode)
          .option("spark.indextables.checkpoint.enabled", "true")
          .option("spark.indextables.checkpoint.interval", "10")
          .save(tempPath)
      }

      // Read the checkpoint file and verify schema deduplication
      val txLogPath = new org.apache.hadoop.fs.Path(tempPath, "_transaction_log")
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        // Find checkpoint file
        val checkpointPath = new org.apache.hadoop.fs.Path(txLogPath, "00000000000000000010.checkpoint.json").toString
        println(s"DEBUG: Looking for checkpoint at: $checkpointPath")
        println(s"DEBUG: Checkpoint exists: ${cloudProvider.exists(checkpointPath)}")

        // List all files in transaction log directory
        val allFiles = cloudProvider.listFiles(txLogPath.toString)
        println(s"DEBUG: Transaction log files: ${allFiles.mkString(", ")}")

        if (cloudProvider.exists(checkpointPath)) {
          val bytes = cloudProvider.readFile(checkpointPath)
          val decompressedBytes =
            io.indextables.spark.transaction.compression.CompressionUtils.readTransactionFile(bytes)
          val content = new String(decompressedBytes, java.nio.charset.StandardCharsets.UTF_8)
          val lines   = content.split("\n").filter(_.trim.nonEmpty)

          // Debug: show first few lines of checkpoint
          println(s"DEBUG: Checkpoint has ${lines.length} lines")
          lines.take(3).foreach(line => println(s"DEBUG: Line sample: ${line.take(200)}..."))

          // Count MetadataAction entries - checkpoint uses "metaData" key (note capital D)
          val metadataLines = lines.filter(_.contains("\"metaData\""))
          println(s"DEBUG: Found ${metadataLines.length} MetadataAction entries in checkpoint")

          // There should be exactly 1 MetadataAction
          metadataLines.length shouldBe 1

          // Extract schema registry entries from MetadataAction
          val metadataLine     = metadataLines.head
          val schemaKeyPattern = """"docMappingSchema\.[^"]+"""".r
          val schemaKeys       = schemaKeyPattern.findAllIn(metadataLine).toList

          println(s"DEBUG: Found ${schemaKeys.length} schema registry entries: ${schemaKeys.mkString(", ")}")

          // All 12 batches used the SAME schema, so there should be only 1 unique schema hash
          // (The schema keys are like "docMappingSchema.ABC123...")
          val uniqueHashes = schemaKeys.map(_.replace("\"docMappingSchema.", "").replace("\"", "")).distinct
          println(s"DEBUG: Unique schema hashes: ${uniqueHashes.mkString(", ")}")

          // Should have exactly 1 unique schema since all batches use the same schema
          uniqueHashes.length shouldBe 1

          // Count AddActions and verify they all use docMappingRef (not docMappingJson)
          val addActionLines = lines.filter(_.contains("\"add\""))
          val withRef        = addActionLines.count(_.contains("\"docMappingRef\""))
          val withFullJson =
            addActionLines.count(line => line.contains("\"docMappingJson\"") && !line.contains("\"docMappingJson\":null"))

          println(s"DEBUG: ${addActionLines.length} AddActions total, $withRef with docMappingRef, $withFullJson with full docMappingJson")

          // All AddActions should use docMappingRef (deduplicated), none should have full docMappingJson
          withFullJson shouldBe 0
          withRef shouldBe addActionLines.length
        } else {
          fail(s"Checkpoint file not found at $checkpointPath")
        }

        // Verify we can read all data back
        val result = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(tempPath)

        val count = result.count()
        println(s"DEBUG: Read $count rows")
        count shouldBe (12 * numRecords)
      } finally
        cloudProvider.close()
    }
  }

  test("checkpoint triggers missing schema error - reproduces production bug") {
    // This test reproduces the production bug:
    // 1. Create a new table using DataFrame API with LARGE schema (hundreds of fields)
    // 2. Append 13 times with 100 records each (triggers checkpoint at version 10)
    // 3. Verifies that MetadataAction is NOT written to every transaction (only version 1)
    // 4. Verifies that reading works after checkpoint
    withTempPath { tempPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create a DataFrame with a large schema (100 fields) and 100 records
      val numFields  = 100
      val numRecords = 100

      def createLargeDF(batchNum: Int): org.apache.spark.sql.DataFrame = {
        val rows = (1 to numRecords).map { i =>
          val id     = (batchNum - 1) * numRecords + i
          val values = (1 to numFields).map(f => s"value_${id}_$f")
          org.apache.spark.sql.Row.fromSeq(id.toLong +: values)
        }
        val schema = org.apache.spark.sql.types.StructType(
          org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.LongType) +:
            (1 to numFields).map(f =>
              org.apache.spark.sql.types.StructField(s"field_$f", org.apache.spark.sql.types.StringType)
            )
        )
        spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      }

      // First write to create the table
      val df1 = createLargeDF(1)
      df1.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "10")
        .save(tempPath)

      // Append 12 more times to trigger checkpoint at version 10
      (2 to 13).foreach { i =>
        println(s"DEBUG: Writing batch $i")
        val df = createLargeDF(i)
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("append")
          .option("spark.indextables.checkpoint.enabled", "true")
          .option("spark.indextables.checkpoint.interval", "10")
          .save(tempPath)
      }

      // Verify transaction log structure - MetadataAction should NOT be in every version
      val txLogPath = new org.apache.hadoop.fs.Path(tempPath, "_transaction_log")
      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        var metadataCount  = 0
        var addActionCount = 0

        // Read each version and check for MetadataAction
        (0 to 13).foreach { version =>
          val versionFilePath = new org.apache.hadoop.fs.Path(txLogPath, f"$version%020d.json").toString
          if (cloudProvider.exists(versionFilePath)) {
            // Read and decompress if needed
            val bytes = cloudProvider.readFile(versionFilePath)
            val decompressedBytes =
              io.indextables.spark.transaction.compression.CompressionUtils.readTransactionFile(bytes)
            val content = new String(decompressedBytes, java.nio.charset.StandardCharsets.UTF_8)
            val lines   = content.split("\n").filter(_.trim.nonEmpty)

            // Check for metadata and add actions by looking at JSON keys
            val hasMetadata    = lines.exists(_.contains("\"metadataAction\""))
            val addActionLines = lines.filter(_.contains("\"add\""))
            addActionCount += addActionLines.size

            if (hasMetadata) {
              metadataCount += 1
              println(s"DEBUG: Version $version has MetadataAction (${lines.size} actions total)")
            }

            // Check if AddActions use docMappingRef (deduplication) vs docMappingJson
            val withRef  = addActionLines.count(_.contains("\"docMappingRef\""))
            val withJson = addActionLines.count(_.contains("\"docMappingJson\""))
            if (addActionLines.nonEmpty) {
              println(
                s"DEBUG: Version $version: ${addActionLines.size} AddActions ($withRef with ref, $withJson with json)"
              )
            }
          }
        }

        println(s"DEBUG: MetadataAction found in $metadataCount versions, $addActionCount total AddActions")

        // MetadataAction should be in version 0 (initialize) and version 1 (first schema registration)
        // It should NOT be in every version
        metadataCount should be <= 3 // Allow for version 0, 1, and maybe one more
      } finally
        cloudProvider.close()

      // Read back - this should fail with "Missing schema hashes" if deduplication isn't working
      println("DEBUG: Reading back data")
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      // Verify we can read all rows
      val count = result.count()
      println(s"DEBUG: Read $count rows")
      count shouldBe (13 * numRecords)
    }
  }
}
