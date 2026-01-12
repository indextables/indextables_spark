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
    val actions = (1 to 10).map { i =>
      createTestAddAction(s"file$i.split").copy(docMappingJson = Some(largeSchema))
    }

    val (originalSize, deduplicatedSize) = SchemaDeduplication.estimateSavings(actions)

    // Original should be 10x the schema size
    originalSize shouldBe (largeSchema.length * 10L)

    // Deduplicated should be much smaller (1 schema + 10 refs)
    deduplicatedSize should be < originalSize
    deduplicatedSize should be < (originalSize / 5) // At least 5x reduction
  }

  test("deduplicateSchemas should handle actions without schemas") {
    val addWithSchema = createTestAddAction("file1.split").copy(docMappingJson = Some(smallSchema))
    val addWithoutSchema = createTestAddAction("file2.split")
    val protocolAction = createTestProtocolAction()

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
    val restored = SchemaDeduplication.restoreSchemas(Seq(addAction), registry)
    val restoredAdd = restored.head.asInstanceOf[AddAction]

    // docMappingRef should still be present (couldn't restore)
    restoredAdd.docMappingRef shouldBe Some(missingHash)
    restoredAdd.docMappingJson shouldBe None
  }

  test("extractSchemaRegistry should extract only schema entries") {
    val configuration = Map(
      "spark.indextables.some.config" -> "value",
      s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}hash1" -> smallSchema,
      s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}hash2" -> largeSchema,
      "other.config" -> "other"
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
}
