package com.tantivy4spark

import org.scalatest.funsuite.AnyFunSuite
import _root_.io.indextables.tantivy4java.core.{Index, Schema, SchemaBuilder, Document, IndexWriter}
import _root_.io.indextables.tantivy4java.batch.{BatchDocument, BatchDocumentBuilder}
import _root_.io.indextables.tantivy4java.split.merge.QuickwitSplit
import java.nio.file.{Files, Paths}

/** Scala reproduction of FastFieldBugReproductionTest.java to check for Scala/Java interop issues. */
class FastFieldScalaReproTest extends AnyFunSuite {

  test("Scala: fast field preserved in doc mapping") {
    val tempDir = Files.createTempDirectory("fast-field-test")

    try {
      // Create schema with fast fields - exactly like Java test
      val builder = new SchemaBuilder()
      builder.addStringField("title", true, true, true) // stored=true, indexed=true, fast=true
      builder.addIntegerField("id", true, true, true)   // stored=true, indexed=true, fast=true
      val schema = builder.build()

      // Create index and write document
      val indexPath = tempDir.resolve("test_index").toString
      // Test with reuse=false like Tantivy4Spark does
      val index = new Index(schema, indexPath, false)

      val writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)

      // Use BatchDocumentBuilder like Tantivy4Spark does
      val batch    = new BatchDocumentBuilder()
      val batchDoc = new BatchDocument()
      batchDoc.addText("title", "Test Document")
      batchDoc.addInteger("id", 12345L)
      batch.addDocument(batchDoc)

      writer.addDocumentsBatch(batch)
      batch.close()

      writer.commit()
      writer.close()

      // CRITICAL: This is what Tantivy4Spark does - reload after commit
      println("🔍 BEFORE reload()")
      index.reload()
      println("🔍 AFTER reload()")

      // Convert to split - using EXACT Tantivy4Spark pattern
      val splitPath     = tempDir.resolve("test.split").toString
      val indexUid      = s"tantivy4spark-${java.util.UUID.randomUUID().toString}"
      val sourceId      = "tantivy4spark"
      val nodeId        = "test-node"
      val docMappingUid = "default"
      val partitionId   = 0L
      val now           = java.time.Instant.now()
      val config =
        new QuickwitSplit.SplitConfig(indexUid, sourceId, nodeId, docMappingUid, partitionId, now, now, null, null)
      val metadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config)

      // Get doc mapping JSON
      val docMappingJson = metadata.getDocMappingJson()
      println(s"🔍 SCALA TEST: Doc Mapping JSON: $docMappingJson")

      // Check if fast=true is present
      assert(docMappingJson != null, "Doc mapping JSON should not be null")
      assert(
        docMappingJson.contains("\"fast\":true"),
        s"Doc mapping JSON should contain fast:true. Actual JSON: $docMappingJson"
      )

      println("✅ SCALA TEST PASSED: fast=true found in docMappingJson")

      index.close()
      schema.close()

    } finally {
      // Cleanup
      def deleteRecursively(file: java.io.File): Unit = {
        if (file.isDirectory) file.listFiles.foreach(deleteRecursively)
        file.delete()
      }
      deleteRecursively(tempDir.toFile)
    }
  }
}
