package com.tantivy4spark

import org.scalatest.funsuite.AnyFunSuite
import com.tantivy4java._
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._

/**
 * Test if tantivy4java's QuickwitSplit.mergeSplits() preserves fast field configuration.
 */
class FastFieldMergeTest extends AnyFunSuite {

  test("tantivy4java: merge operation preserves fast fields") {
    val tempDir = Files.createTempDirectory("fast-field-merge-test")

    try {
      // Create schema with fast fields
      val builder = new SchemaBuilder()
      builder.addStringField("title", true, true, true)  // fast=true
      builder.addIntegerField("id", true, true, true)    // fast=true
      val schema = builder.build()

      // Create first split
      val indexPath1 = tempDir.resolve("index1").toString
      val index1 = new Index(schema, indexPath1, false)
      val writer1 = index1.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)

      val batch1 = new BatchDocumentBuilder()
      val batchDoc1 = new BatchDocument()
      batchDoc1.addText("title", "Document 1")
      batchDoc1.addInteger("id", 1L)
      batch1.addDocument(batchDoc1)
      writer1.addDocumentsBatch(batch1)
      batch1.close()

      writer1.commit()
      writer1.close()
      index1.reload()

      val splitPath1 = tempDir.resolve("split1.split").toString
      val indexUid1 = s"tantivy4spark-${java.util.UUID.randomUUID().toString}"
      val config1 = new QuickwitSplit.SplitConfig(indexUid1, "test-source", "test-node", "default",
        0L, java.time.Instant.now(), java.time.Instant.now(), null, null)
      val metadata1 = QuickwitSplit.convertIndexFromPath(indexPath1, splitPath1, config1)

      val docMapping1 = metadata1.getDocMappingJson()
      println(s"🔍 SPLIT 1: Doc Mapping JSON: $docMapping1")
      assert(docMapping1.contains("\"fast\":true"), s"Split 1 should have fast=true. Actual: $docMapping1")

      index1.close()

      // Create second split
      val indexPath2 = tempDir.resolve("index2").toString
      val index2 = new Index(schema, indexPath2, false)
      val writer2 = index2.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)

      val batch2 = new BatchDocumentBuilder()
      val batchDoc2 = new BatchDocument()
      batchDoc2.addText("title", "Document 2")
      batchDoc2.addInteger("id", 2L)
      batch2.addDocument(batchDoc2)
      writer2.addDocumentsBatch(batch2)
      batch2.close()

      writer2.commit()
      writer2.close()
      index2.reload()

      val splitPath2 = tempDir.resolve("split2.split").toString
      val indexUid2 = s"tantivy4spark-${java.util.UUID.randomUUID().toString}"
      val config2 = new QuickwitSplit.SplitConfig(indexUid2, "test-source", "test-node", "default",
        0L, java.time.Instant.now(), java.time.Instant.now(), null, null)
      val metadata2 = QuickwitSplit.convertIndexFromPath(indexPath2, splitPath2, config2)

      val docMapping2 = metadata2.getDocMappingJson()
      println(s"🔍 SPLIT 2: Doc Mapping JSON: $docMapping2")
      assert(docMapping2.contains("\"fast\":true"), s"Split 2 should have fast=true. Actual: $docMapping2")

      index2.close()

      // MERGE the two splits
      println(s"🔧 Merging splits...")
      val inputSplitPaths = java.util.Arrays.asList(splitPath1, splitPath2)
      val mergedSplitPath = tempDir.resolve("merged.split").toString
      val mergedIndexUid = s"tantivy4spark-${java.util.UUID.randomUUID().toString}"
      val mergeConfig = new QuickwitSplit.MergeConfig(mergedIndexUid, "test-source", "test-node")

      val mergedMetadata = QuickwitSplit.mergeSplits(inputSplitPaths, mergedSplitPath, mergeConfig)

      // Check if merged split preserves fast=true
      val mergedDocMapping = mergedMetadata.getDocMappingJson()
      println(s"🔍 MERGED SPLIT: Doc Mapping JSON: $mergedDocMapping")

      // THE KEY TEST: Does merge preserve fast=true?
      assert(mergedDocMapping != null, "Merged doc mapping should not be null")
      assert(mergedDocMapping.contains("\"fast\":true"),
        s"Merged split should preserve fast=true from source splits. Actual: $mergedDocMapping")

      println(s"✅ TEST PASSED: Merge operation preserved fast=true")

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
