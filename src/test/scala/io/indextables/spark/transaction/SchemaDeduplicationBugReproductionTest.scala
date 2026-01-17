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

import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase

/**
 * Reproduction tests for schema deduplication bugs.
 *
 * Bug #2: Schema hash calculation uses string serialization which is not canonical. Two semantically identical JSON
 * schemas with different property ordering produce different hashes, causing duplicate schema registry entries.
 */
class SchemaDeduplicationBugReproductionTest extends TestBase {

  // ============================================================================
  // BUG #2: Schema hash calculation is not canonical (JSON property ordering)
  // ============================================================================

  test("BUG #2 REPRO: semantically identical JSON with different property order produces SAME hash") {
    // This test reproduces the bug where:
    // Two JSON schemas that are semantically identical but have different
    // property ordering produce DIFFERENT hashes.
    //
    // Expected: Same hash for semantically identical schemas
    // Actual: Different hashes, causing duplicate schema registry entries

    // These two JSON strings represent the SAME schema but with different property ordering
    val schema1 = """{"name":"id","type":"i64","fast":true}"""
    val schema2 = """{"fast":true,"name":"id","type":"i64"}"""
    val schema3 = """{"type":"i64","fast":true,"name":"id"}"""

    // Current implementation uses string hashing, so these will produce different hashes
    val hash1 = SchemaDeduplication.computeSchemaHash(schema1)
    val hash2 = SchemaDeduplication.computeSchemaHash(schema2)
    val hash3 = SchemaDeduplication.computeSchemaHash(schema3)

    println(s"DEBUG: schema1 hash: $hash1")
    println(s"DEBUG: schema2 hash: $hash2")
    println(s"DEBUG: schema3 hash: $hash3")

    // BUG: These assertions currently FAIL because string hashing is not canonical
    // When fixed, all three should produce the same hash
    hash1 shouldBe hash2
    hash2 shouldBe hash3
    hash1 shouldBe hash3
  }

  test("BUG #2 REPRO: whitespace differences in JSON produce SAME hash") {
    // Same content with different whitespace should produce same hash

    val schema1 = """{"name":"id","type":"i64"}"""
    val schema2 = """{ "name" : "id" , "type" : "i64" }"""
    val schema3 = """{
      "name": "id",
      "type": "i64"
    }"""

    val hash1 = SchemaDeduplication.computeSchemaHash(schema1)
    val hash2 = SchemaDeduplication.computeSchemaHash(schema2)
    val hash3 = SchemaDeduplication.computeSchemaHash(schema3)

    println(s"DEBUG: schema1 (compact) hash: $hash1")
    println(s"DEBUG: schema2 (spaced) hash: $hash2")
    println(s"DEBUG: schema3 (multiline) hash: $hash3")

    // BUG: These assertions currently FAIL because string hashing is not canonical
    hash1 shouldBe hash2
    hash2 shouldBe hash3
  }

  test("BUG #2 FIXED: array element ordering for named objects is normalized") {
    // For arrays of objects with "name" fields (like field definitions), order is normalized
    // This ensures different serialization orders produce the same hash

    val schema1 = """{"fields":[{"name":"a"},{"name":"b"},{"name":"c"}]}"""
    val schema2 = """{"fields":[{"name":"c"},{"name":"b"},{"name":"a"}]}"""

    val hash1 = SchemaDeduplication.computeSchemaHash(schema1)
    val hash2 = SchemaDeduplication.computeSchemaHash(schema2)

    println(s"DEBUG: schema1 (a,b,c) hash: $hash1")
    println(s"DEBUG: schema2 (c,b,a) hash: $hash2")

    // With the fix, arrays of named objects are sorted by "name" field
    // So different orderings produce the SAME hash
    hash1 shouldBe hash2
    println("DEBUG: Array order does NOT affect hash (normalized by 'name' field)")
  }

  test("BUG #2: duplicate schema registry entries due to non-canonical hashing") {
    // This test demonstrates how non-canonical hashing can cause
    // the same schema to be registered multiple times with different hashes

    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val txLogPath = new Path(tablePath, "_transaction_log")

      val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
        tempPath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(txLogPath.toString)

        // Two schemas that are semantically identical but string-different
        val schemaA = """{"name":"field1","type":"i64","fast":true}"""
        val schemaB = """{"fast":true,"name":"field1","type":"i64"}"""

        // Create AddActions with these schemas
        val addAction1 = createTestAddAction("file1.split").copy(docMappingJson = Some(schemaA))
        val addAction2 = createTestAddAction("file2.split").copy(docMappingJson = Some(schemaB))

        // Deduplicate
        val (deduplicated, registry) = SchemaDeduplication.deduplicateSchemas(
          Seq(addAction1, addAction2)
        )

        println(s"DEBUG: Registry size: ${registry.size}")
        registry.foreach {
          case (k, v) =>
            println(s"DEBUG: Registry entry: $k -> ${v.take(50)}...")
        }

        // BUG: With current implementation, registry.size == 2 (duplicate entries)
        // With fix, registry.size should == 1 (same schema, same hash)
        registry.size shouldBe 1

        // Both AddActions should reference the same hash
        val refs = deduplicated.collect { case a: AddAction => a.docMappingRef }.flatten
        refs.distinct.size shouldBe 1
      } finally
        cloudProvider.close()
    }
  }

  // ============================================================================
  // BUG #2 CLEANUP: Consolidation of duplicate schema mappings
  // ============================================================================

  test("BUG #2 CLEANUP: consolidateDuplicateSchemas consolidates legacy duplicate mappings") {
    // This test verifies that the consolidation logic properly handles
    // duplicate schema mappings created by the old buggy hash calculation

    // Simulate a registry with duplicate schemas (same content, different old hashes)
    // These represent schemas that were hashed before the canonical fix
    val schema1         = """{"name":"field1","type":"i64","fast":true}"""
    val schema2         = """{"fast":true,"name":"field1","type":"i64"}""" // Same schema, different order
    val schema3         = """{"type":"i64","name":"field1","fast":true}""" // Same schema, different order
    val differentSchema = """{"name":"field2","type":"text","fast":false}"""

    // Old hashes (simulating pre-fix behavior where each would have a different hash)
    val oldHash1 = "oldHashAAAAAAAA"
    val oldHash2 = "oldHashBBBBBBBB"
    val oldHash3 = "oldHashCCCCCCCC"
    val diffHash = "diffHashDDDDDD"

    // Registry with "duplicate" entries (>10 to trigger consolidation)
    val legacyRegistry = Map(
      s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}$oldHash1" -> schema1,
      s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}$oldHash2" -> schema2,
      s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}$oldHash3" -> schema3,
      s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}$diffHash" -> differentSchema
    ) ++ (1 to 8).map(i => s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}padding$i" -> s"""{"id":$i}""").toMap

    // AddActions referencing the old hashes
    val addActions: Seq[Action] = Seq(
      createTestAddAction("file1.split").copy(docMappingRef = Some(oldHash1)),
      createTestAddAction("file2.split").copy(docMappingRef = Some(oldHash2)),
      createTestAddAction("file3.split").copy(docMappingRef = Some(oldHash3)),
      createTestAddAction("file4.split").copy(docMappingRef = Some(oldHash1)),
      createTestAddAction("file5.split").copy(docMappingRef = Some(diffHash))
    )

    println(s"DEBUG: Starting with ${legacyRegistry.size} schema entries")

    // Run consolidation
    val (consolidatedActions, consolidatedRegistry, duplicatesRemoved) =
      SchemaDeduplication.consolidateDuplicateSchemas(addActions, legacyRegistry)

    println(s"DEBUG: After consolidation: ${consolidatedRegistry.count(_._1.startsWith(SchemaDeduplication.SCHEMA_KEY_PREFIX))} schema entries")
    println(s"DEBUG: Duplicates removed: $duplicatesRemoved")

    // Extract the schema entries
    val schemaEntries = consolidatedRegistry.filter(_._1.startsWith(SchemaDeduplication.SCHEMA_KEY_PREFIX))

    // The 3 semantically identical schemas should be consolidated to 1
    // The different schema should remain
    // Plus 8 padding schemas = 10 total
    schemaEntries.size shouldBe 10

    // All AddActions that referenced the duplicate schemas should now reference the same canonical hash
    val addActionRefs = consolidatedActions.collect { case a: AddAction => a.docMappingRef }.flatten
    val uniqueRefs    = addActionRefs.distinct

    println(s"DEBUG: Unique refs after consolidation: ${uniqueRefs.mkString(", ")}")

    // file1, file2, file3, file4 all referenced semantically identical schemas
    // They should now all reference the same canonical hash
    // file5 references a different schema
    val file1Ref = consolidatedActions
      .find(_.asInstanceOf[AddAction].path == "file1.split")
      .get
      .asInstanceOf[AddAction]
      .docMappingRef
      .get
    val file2Ref = consolidatedActions
      .find(_.asInstanceOf[AddAction].path == "file2.split")
      .get
      .asInstanceOf[AddAction]
      .docMappingRef
      .get
    val file3Ref = consolidatedActions
      .find(_.asInstanceOf[AddAction].path == "file3.split")
      .get
      .asInstanceOf[AddAction]
      .docMappingRef
      .get
    val file4Ref = consolidatedActions
      .find(_.asInstanceOf[AddAction].path == "file4.split")
      .get
      .asInstanceOf[AddAction]
      .docMappingRef
      .get
    val file5Ref = consolidatedActions
      .find(_.asInstanceOf[AddAction].path == "file5.split")
      .get
      .asInstanceOf[AddAction]
      .docMappingRef
      .get

    // All identical schemas should map to the same canonical hash
    file1Ref shouldBe file2Ref
    file2Ref shouldBe file3Ref
    file3Ref shouldBe file4Ref

    // The different schema should have a different ref
    file5Ref should not be file1Ref

    // The canonical hash should match what our new algorithm produces
    val expectedCanonicalHash = SchemaDeduplication.computeSchemaHash(schema1)
    file1Ref shouldBe expectedCanonicalHash

    // At least 2 duplicates should have been removed (schema2 and schema3)
    duplicatesRemoved should be >= 2
  }

  test("BUG #2 CLEANUP: consolidation skipped when below threshold") {
    // Consolidation should be skipped when there are fewer than DUPLICATE_DETECTION_THRESHOLD schemas
    val smallRegistry = Map(
      s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}hash1" -> """{"name":"a"}""",
      s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}hash2" -> """{"name":"b"}"""
    )

    val actions: Seq[Action] = Seq(
      createTestAddAction("file1.split").copy(docMappingRef = Some("hash1"))
    )

    val (consolidatedActions, consolidatedRegistry, duplicatesRemoved) =
      SchemaDeduplication.consolidateDuplicateSchemas(actions, smallRegistry)

    // Should return unchanged
    duplicatesRemoved shouldBe 0
    consolidatedRegistry shouldBe smallRegistry
    consolidatedActions shouldBe actions
  }

  // ============================================================================
  // BUG #3: Array element ordering in field definitions
  // ============================================================================

  test("BUG #3 FIXED: field definition arrays with different element order produce SAME hash") {
    // This test verifies the fix where docMappingJson arrays of field definitions
    // are normalized by sorting on the "name" field.
    //
    // Real docMappingJson format: [{"name":"id","type":"i64"},{"name":"content","type":"text"},...]
    // Different splits may serialize fields in different order, but should produce same hash.

    // These represent the SAME schema (same fields) but with different array ordering
    val schema1 = """[{"name":"id","type":"i64","fast":true},{"name":"content","type":"text","fast":false}]"""
    val schema2 = """[{"name":"content","type":"text","fast":false},{"name":"id","type":"i64","fast":true}]"""

    // With same fields but different ordering in nested objects
    val schema3 = """[{"fast":true,"name":"id","type":"i64"},{"fast":false,"name":"content","type":"text"}]"""

    val hash1 = SchemaDeduplication.computeSchemaHash(schema1)
    val hash2 = SchemaDeduplication.computeSchemaHash(schema2)
    val hash3 = SchemaDeduplication.computeSchemaHash(schema3)

    println(s"DEBUG: schema1 (id,content) hash: $hash1")
    println(s"DEBUG: schema2 (content,id) hash: $hash2")
    println(s"DEBUG: schema3 (id,content with sorted keys) hash: $hash3")

    // With the fix, all three should produce the same hash:
    // - Object key order is normalized (existing behavior)
    // - Array element order for named objects is normalized (new behavior)
    hash1 shouldBe hash2 // Array elements sorted by "name" field
    hash1 shouldBe hash3 // Object keys sorted alphabetically
    hash2 shouldBe hash3 // Transitive equality
  }

  test("BUG #3 FIXED: consolidation works for field arrays with different element order") {
    // This simulates the user's issue: 3000+ identical schemas with different array ordering
    // Each is a permutation of the same field definitions - but now they consolidate!

    val fieldA = """{"name":"id","type":"i64","fast":true}"""
    val fieldB = """{"name":"content","type":"text","fast":false}"""
    val fieldC = """{"name":"score","type":"f64","fast":true}"""

    // Generate permutations (3! = 6 permutations for 3 fields)
    val permutations = Seq(
      s"[$fieldA,$fieldB,$fieldC]",
      s"[$fieldA,$fieldC,$fieldB]",
      s"[$fieldB,$fieldA,$fieldC]",
      s"[$fieldB,$fieldC,$fieldA]",
      s"[$fieldC,$fieldA,$fieldB]",
      s"[$fieldC,$fieldB,$fieldA]"
    )

    // Each permutation gets a unique hash (simulating old behavior before fix)
    val oldHashes = permutations.zipWithIndex.map { case (_, i) => s"oldHash${('A' + i).toChar}" }

    // Create a registry with these "duplicate" schemas (simulating legacy data)
    val legacyRegistry = oldHashes
      .zip(permutations)
      .map { case (hash, schema) => s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}$hash" -> schema }
      .toMap ++ (1 to 6)
      .map(i => s"${SchemaDeduplication.SCHEMA_KEY_PREFIX}padding$i" -> s"""[{"name":"pad$i"}]""")
      .toMap

    // Create AddActions referencing these hashes
    val addActions: Seq[Action] = oldHashes.zipWithIndex.map {
      case (hash, i) => createTestAddAction(s"file$i.split").copy(docMappingRef = Some(hash))
    }

    println(s"DEBUG: Registry has ${legacyRegistry.size} schema entries")
    println(s"DEBUG: Permutation hashes (should all be the same now):")
    permutations.foreach { p =>
      val hash = SchemaDeduplication.computeSchemaHash(p)
      println(s"  $hash -> ${p.take(50)}...")
    }

    // Verify all permutations now produce the same hash
    val uniqueHashes = permutations.map(SchemaDeduplication.computeSchemaHash).distinct
    uniqueHashes.size shouldBe 1 // All 6 permutations -> 1 canonical hash

    // Run consolidation
    val (consolidatedActions, consolidatedRegistry, duplicatesRemoved) =
      SchemaDeduplication.consolidateDuplicateSchemas(addActions, legacyRegistry)

    val schemaEntries = consolidatedRegistry.count(_._1.startsWith(SchemaDeduplication.SCHEMA_KEY_PREFIX))

    println(s"DEBUG: After consolidation: $schemaEntries schema entries")
    println(s"DEBUG: Duplicates removed: $duplicatesRemoved")

    // With the fix: 6 permutations -> 1 unique + 6 padding = 7 schema entries
    schemaEntries shouldBe 7

    // 5 duplicates removed (6 permutations consolidated to 1)
    duplicatesRemoved shouldBe 5

    // All AddActions should now reference the same canonical hash
    val actionRefs = consolidatedActions.collect { case a: AddAction => a.docMappingRef }.flatten.distinct
    actionRefs.size shouldBe 1
    actionRefs.head shouldBe uniqueHashes.head
  }

  test("BUG #3 FIXED: 120 permutations of same schema produce 1 hash") {
    // This test verifies the fix: all permutations of the same field set produce the same hash
    // With N fields, there are N! permutations - but they all canonicalize to 1 hash

    // For 4 fields, 4! = 24 permutations
    // For 5 fields, 5! = 120 permutations
    // For 6 fields, 6! = 720 permutations
    // For 7 fields, 7! = 5040 permutations

    val fields = Seq(
      """{"name":"id","type":"i64","fast":true}""",
      """{"name":"title","type":"text","fast":false}""",
      """{"name":"content","type":"text","fast":false}""",
      """{"name":"score","type":"f64","fast":true}""",
      """{"name":"timestamp","type":"date","fast":true}"""
    )

    // Generate all permutations (5! = 120)
    val permutations = fields.permutations.toSeq.map(p => "[" + p.mkString(",") + "]")

    println(s"DEBUG: Generated ${permutations.size} permutations of ${fields.size} fields")

    // Compute hashes for all permutations
    val hashes       = permutations.map(SchemaDeduplication.computeSchemaHash)
    val uniqueHashes = hashes.distinct

    println(s"DEBUG: Unique hashes: ${uniqueHashes.size}")
    println(s"DEBUG: Expected: 1")
    println(s"DEBUG: Actual: ${uniqueHashes.size}")

    // With the fix: all 120 permutations produce the same canonical hash
    uniqueHashes.size shouldBe 1

    // Verify the canonical form is sorted by field name alphabetically
    val canonicalHash = uniqueHashes.head
    println(s"DEBUG: Canonical hash: $canonicalHash")

    // Any permutation should produce this same hash
    val randomPermutation = "[" + scala.util.Random.shuffle(fields).mkString(",") + "]"
    SchemaDeduplication.computeSchemaHash(randomPermutation) shouldBe canonicalHash
  }

  // ============================================================================
  // Helper methods
  // ============================================================================

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
