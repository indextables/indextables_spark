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

import java.io.ByteArrayOutputStream

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.compression.{CompressionUtils, GzipCompressionCodec}
import io.indextables.spark.TestBase

class StreamingActionWriterTest extends TestBase {

  test("writeActionsStreaming should write actions to file") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val actions = Seq(
          createTestAddAction("file1.split"),
          createTestAddAction("file2.split")
        )

        val filePath = s"$tempPath/test.json"
        val result = StreamingActionWriter.writeActionsStreaming(
          actions = actions,
          cloudProvider = cloudProvider,
          path = filePath,
          codec = None,
          ifNotExists = false
        )

        result shouldBe true
        cloudProvider.exists(filePath) shouldBe true

        // Read back and verify content
        val content = new String(cloudProvider.readFile(filePath), "UTF-8")
        val lines   = content.split("\n").filter(_.nonEmpty)
        lines.length shouldBe 2
        lines(0) should include("file1.split")
        lines(1) should include("file2.split")
      } finally
        cloudProvider.close()
    }
  }

  test("writeActionsStreaming should write actions with compression") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val actions = Seq(
          createTestAddAction("file1.split"),
          createTestAddAction("file2.split"),
          createTestAddAction("file3.split")
        )

        val filePath = s"$tempPath/test_compressed.json"
        val codec    = Some(new GzipCompressionCodec(6))

        val result = StreamingActionWriter.writeActionsStreaming(
          actions = actions,
          cloudProvider = cloudProvider,
          path = filePath,
          codec = codec,
          ifNotExists = false
        )

        result shouldBe true
        cloudProvider.exists(filePath) shouldBe true

        // Read compressed content and decompress
        val compressedContent   = cloudProvider.readFile(filePath)
        val decompressedContent = CompressionUtils.readTransactionFile(compressedContent)
        val content             = new String(decompressedContent, "UTF-8")
        val lines               = content.split("\n").filter(_.nonEmpty)

        lines.length shouldBe 3
        lines(0) should include("file1.split")
      } finally
        cloudProvider.close()
    }
  }

  test("writeActionsStreaming with ifNotExists should fail if file exists") {
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val actions  = Seq(createTestAddAction("file1.split"))
        val filePath = s"$tempPath/existing.json"

        // Write first time
        val result1 = StreamingActionWriter.writeActionsStreaming(
          actions = actions,
          cloudProvider = cloudProvider,
          path = filePath,
          codec = None,
          ifNotExists = true
        )
        result1 shouldBe true

        // Second write should return false
        val result2 = StreamingActionWriter.writeActionsStreaming(
          actions = actions,
          cloudProvider = cloudProvider,
          path = filePath,
          codec = None,
          ifNotExists = true
        )
        result2 shouldBe false
      } finally
        cloudProvider.close()
    }
  }

  test("writeActionsToStream should write newline-delimited JSON") {
    val actions = Seq(
      createTestAddAction("file1.split"),
      createTestMetadataAction(),
      createTestProtocolAction()
    )

    val baos = new ByteArrayOutputStream()
    StreamingActionWriter.writeActionsToStream(actions, baos)

    val content = new String(baos.toByteArray, "UTF-8")
    val lines   = content.split("\n").filter(_.nonEmpty)

    lines.length shouldBe 3

    // Verify action wrapping format
    lines(0) should include("\"add\":")
    lines(1) should include("\"metaData\":")
    lines(2) should include("\"protocol\":")
  }

  test("wrapAction should correctly wrap all action types") {
    val addAction      = createTestAddAction("test.split")
    val metadataAction = createTestMetadataAction()
    val protocolAction = createTestProtocolAction()
    val removeAction   = createTestRemoveAction("removed.split")
    val skipAction     = createTestSkipAction("skipped.split")

    StreamingActionWriter.wrapAction(addAction).keys should contain("add")
    StreamingActionWriter.wrapAction(metadataAction).keys should contain("metaData")
    StreamingActionWriter.wrapAction(protocolAction).keys should contain("protocol")
    StreamingActionWriter.wrapAction(removeAction).keys should contain("remove")
    StreamingActionWriter.wrapAction(skipAction).keys should contain("mergeskip")
  }

  test("estimateSerializedSize should estimate size correctly") {
    val smallDocMappingJson = """{"fields":[{"name":"id"}]}"""
    val largeDocMappingJson = "a" * 100000 // 100KB

    val smallAction = createTestAddAction("small.split").copy(docMappingJson = Some(smallDocMappingJson))
    val largeAction = createTestAddAction("large.split").copy(docMappingJson = Some(largeDocMappingJson))

    val smallSize = StreamingActionWriter.estimateSerializedSize(Seq(smallAction))
    val largeSize = StreamingActionWriter.estimateSerializedSize(Seq(largeAction))

    smallSize should be < 1000L
    largeSize should be > 100000L
  }

  test("streaming write should handle many actions without OOM") {
    // This test verifies that we can write many actions without OOM
    // Not a true stress test, but validates the streaming approach works
    withTempPath { tempPath =>
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        // Create 1000 actions with moderate-sized docMappingJson
        val docMappingJson = """{"fields":[""" + (1 to 100).map(i => s"""{"name":"field$i"}""").mkString(",") + "]}"
        val actions = (1 to 1000).map { i =>
          createTestAddAction(s"file$i.split").copy(docMappingJson = Some(docMappingJson))
        }

        val filePath = s"$tempPath/many_actions.json"
        val result = StreamingActionWriter.writeActionsStreaming(
          actions = actions,
          cloudProvider = cloudProvider,
          path = filePath,
          codec = None,
          ifNotExists = false
        )

        result shouldBe true
        cloudProvider.exists(filePath) shouldBe true

        // Read back and verify count
        val content = new String(cloudProvider.readFile(filePath), "UTF-8")
        val lines   = content.split("\n").filter(_.nonEmpty)
        lines.length shouldBe 1000
      } finally
        cloudProvider.close()
    }
  }

  // Helper methods to create test actions
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

  private def createTestRemoveAction(path: String): RemoveAction =
    RemoveAction(
      path = path,
      deletionTimestamp = Some(System.currentTimeMillis()),
      dataChange = true,
      extendedFileMetadata = None,
      partitionValues = None,
      size = None
    )

  private def createTestSkipAction(path: String): SkipAction =
    SkipAction(
      path = path,
      skipTimestamp = System.currentTimeMillis(),
      reason = "test skip",
      operation = "test"
    )
}
