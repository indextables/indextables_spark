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

import scala.collection.JavaConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase

class ProtocolVersionTest extends TestBase {

  test("new table should have protocol version 4/4") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val txLog     = TransactionLogFactory.create(tablePath, spark)

      try {
        val schema = getTestSchema()
        txLog.initialize(schema)

        val protocol = txLog.getProtocol()
        protocol.minReaderVersion shouldBe 4
        protocol.minWriterVersion shouldBe 4
      } finally
        txLog.close()
    }
  }

  test("protocol should be included in version 0") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val txLog     = TransactionLogFactory.create(tablePath, spark)

      try {
        val schema = getTestSchema()
        txLog.initialize(schema)

        // Read version 0 directly
        val actions = txLog.readVersion(0)

        val hasProtocol = actions.exists(_.isInstanceOf[ProtocolAction])
        hasProtocol shouldBe true

        val protocolAction = actions.collectFirst { case p: ProtocolAction => p }
        protocolAction shouldBe defined
        protocolAction.get.minReaderVersion shouldBe 4
        protocolAction.get.minWriterVersion shouldBe 4
      } finally
        txLog.close()
    }
  }

  test("legacy table without protocol should default to version 1/1") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val fs        = tablePath.getFileSystem(spark.sparkContext.hadoopConfiguration)

      // Create a legacy table by manually writing only metadata (no protocol)
      fs.mkdirs(tablePath)
      val transactionLogPath = new Path(tablePath, "_transaction_log")
      fs.mkdirs(transactionLogPath)

      val schema = getTestSchema()
      val metadata = MetadataAction(
        id = java.util.UUID.randomUUID().toString,
        name = None,
        description = None,
        format = FileFormat("tantivy4spark", Map.empty),
        schemaString = schema.json,
        partitionColumns = Seq.empty,
        configuration = Map.empty,
        createdTime = Some(System.currentTimeMillis())
      )

      // Write metadata directly without protocol (simulating legacy table)
      val versionFile = new Path(transactionLogPath, "00000000000000000000.json")
      val jsonContent = s"""{"metaData":${io.indextables.spark.util.JsonUtil.mapper.writeValueAsString(metadata)}}"""
      val out         = fs.create(versionFile)
      out.write(jsonContent.getBytes("UTF-8"))
      out.close()

      // Now read protocol - should default to 1/1
      val txLog = TransactionLogFactory.create(tablePath, spark)
      try {
        val protocol = txLog.getProtocol()
        protocol.minReaderVersion shouldBe 1
        protocol.minWriterVersion shouldBe 1
      } finally
        txLog.close()
    }
  }

  test("should reject reader when minReaderVersion too high") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val txLog     = TransactionLogFactory.create(tablePath, spark)

      try {
        val schema = getTestSchema()
        txLog.initialize(schema)

        // Manually upgrade protocol to version 999/999
        txLog.upgradeProtocol(999, 999)

        // Attempt to read should throw exception
        val exception = intercept[ProtocolVersionException] {
          txLog.listFiles()
        }
        exception.getMessage should include("minReaderVersion = 999")
        exception.getMessage should include("upgrade")
      } finally
        txLog.close()
    }
  }

  test("should reject writer when minWriterVersion too high") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val txLog     = TransactionLogFactory.create(tablePath, spark)

      try {
        val schema = getTestSchema()
        txLog.initialize(schema)

        // Manually upgrade protocol to version 2/999
        txLog.upgradeProtocol(2, 999)

        // Attempt to write should throw exception
        val addAction = AddAction(
          path = "s3://bucket/file.split",
          partitionValues = Map.empty,
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true
        )

        val exception = intercept[ProtocolVersionException] {
          txLog.addFiles(Seq(addAction))
        }
        exception.getMessage should include("minWriterVersion = 999")
        exception.getMessage should include("upgrade")
      } finally
        txLog.close()
    }
  }

  test("should allow reading table with lower protocol version") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val txLog     = TransactionLogFactory.create(tablePath, spark)

      try {
        val schema = getTestSchema()
        txLog.initialize(schema)

        // Current version (3/3) should be able to read
        val files = txLog.listFiles()
        files shouldBe empty
      } finally
        txLog.close()
    }
  }

  test("protocol upgrade should preserve higher version") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val txLog     = TransactionLogFactory.create(tablePath, spark)

      try {
        val schema = getTestSchema()
        txLog.initialize(schema)

        val initialProtocol = txLog.getProtocol()
        initialProtocol.minReaderVersion shouldBe 4
        initialProtocol.minWriterVersion shouldBe 4

        // Try to downgrade (should not work)
        txLog.upgradeProtocol(1, 1)

        val protocol = txLog.getProtocol()
        protocol.minReaderVersion shouldBe 4 // Should not downgrade
        protocol.minWriterVersion shouldBe 4

        // Upgrade to higher version
        txLog.upgradeProtocol(5, 5)

        val upgradedProtocol = txLog.getProtocol()
        upgradedProtocol.minReaderVersion shouldBe 5 // Should upgrade
        upgradedProtocol.minWriterVersion shouldBe 5
      } finally
        txLog.close()
    }
  }

  test("protocol should be cached") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val txLog     = TransactionLogFactory.create(tablePath, spark)

      try {
        val schema = getTestSchema()
        txLog.initialize(schema)

        // First call should cache
        val protocol1 = txLog.getProtocol()

        // Second call should use cache
        val protocol2 = txLog.getProtocol()

        protocol1.minReaderVersion shouldBe protocol2.minReaderVersion
        protocol1.minWriterVersion shouldBe protocol2.minWriterVersion

        // Check cache stats
        val stats = txLog.getCacheStats()
        stats shouldBe defined
      } finally
        txLog.close()
    }
  }

  test("protocol check can be disabled via configuration") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val options = new CaseInsensitiveStringMap(
        Map(
          ProtocolVersion.PROTOCOL_CHECK_ENABLED -> "false"
        ).asJava
      )
      val txLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        val schema = getTestSchema()
        txLog.initialize(schema)

        // Manually upgrade to version 999/999
        txLog.upgradeProtocol(999, 999)

        // Should NOT throw exception because checks are disabled
        val files = txLog.listFiles()
        files shouldBe empty
      } finally
        txLog.close()
    }
  }

  test("protocol should persist in checkpoints") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.checkpoint.enabled"  -> "true",
          "spark.indextables.checkpoint.interval" -> "3"
        ).asJava
      )
      val txLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        val schema = getTestSchema()
        txLog.initialize(schema)

        // Add some files to trigger checkpoint
        for (i <- 1 to 5) {
          val addAction = AddAction(
            path = s"s3://bucket/file$i.split",
            partitionValues = Map.empty,
            size = 1000L * i,
            modificationTime = System.currentTimeMillis(),
            dataChange = true
          )
          txLog.addFiles(Seq(addAction))
        }

        // Checkpoint should have been created
        val checkpointVersion = txLog.getLastCheckpointVersion()
        checkpointVersion shouldBe defined

        // Protocol should still be readable
        val protocol = txLog.getProtocol()
        protocol.minReaderVersion shouldBe 4
        protocol.minWriterVersion shouldBe 4
      } finally
        txLog.close()
    }
  }

  test("protocol serialization and deserialization") {
    val protocol = ProtocolAction(
      minReaderVersion = 3,
      minWriterVersion = 3,
      readerFeatures = Some(Set("feature1", "feature2")),
      writerFeatures = Some(Set("feature3", "feature4"))
    )

    val json         = io.indextables.spark.util.JsonUtil.mapper.writeValueAsString(protocol)
    val deserialized = io.indextables.spark.util.JsonUtil.mapper.readValue(json, classOf[ProtocolAction])

    deserialized.minReaderVersion shouldBe protocol.minReaderVersion
    deserialized.minWriterVersion shouldBe protocol.minWriterVersion
    deserialized.readerFeatures shouldBe protocol.readerFeatures
    deserialized.writerFeatures shouldBe protocol.writerFeatures
  }

  test("protocol constants validation") {
    ProtocolVersion.CURRENT_READER_VERSION should be >= ProtocolVersion.MIN_READER_VERSION
    ProtocolVersion.CURRENT_WRITER_VERSION should be >= ProtocolVersion.MIN_WRITER_VERSION

    ProtocolVersion.isReaderVersionSupported(1) shouldBe true
    ProtocolVersion.isReaderVersionSupported(2) shouldBe true
    ProtocolVersion.isReaderVersionSupported(3) shouldBe true
    ProtocolVersion.isReaderVersionSupported(4) shouldBe true
    ProtocolVersion.isReaderVersionSupported(999) shouldBe false

    ProtocolVersion.isWriterVersionSupported(1) shouldBe true
    ProtocolVersion.isWriterVersionSupported(2) shouldBe true
    ProtocolVersion.isWriterVersionSupported(3) shouldBe true
    ProtocolVersion.isWriterVersionSupported(4) shouldBe true
    ProtocolVersion.isWriterVersionSupported(999) shouldBe false
  }

  test("default protocol should be version 4/4") {
    val protocol = ProtocolVersion.defaultProtocol()
    protocol.minReaderVersion shouldBe 4
    protocol.minWriterVersion shouldBe 4
    protocol.readerFeatures shouldBe empty
    protocol.writerFeatures shouldBe empty
  }

  test("legacy protocol should be version 1/1") {
    val protocol = ProtocolVersion.legacyProtocol()
    protocol.minReaderVersion shouldBe 1
    protocol.minWriterVersion shouldBe 1
    protocol.readerFeatures shouldBe empty
    protocol.writerFeatures shouldBe empty
  }

  test("protocol auto-upgrade can be disabled") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val options = new CaseInsensitiveStringMap(
        Map(
          ProtocolVersion.PROTOCOL_AUTO_UPGRADE -> "false"
        ).asJava
      )
      val txLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        val schema = getTestSchema()
        txLog.initialize(schema)

        val initialProtocol = txLog.getProtocol()
        initialProtocol.minReaderVersion shouldBe 4
        initialProtocol.minWriterVersion shouldBe 4

        // Try to upgrade with auto-upgrade disabled
        txLog.upgradeProtocol(5, 5)

        // Protocol should remain unchanged
        val protocol = txLog.getProtocol()
        protocol.minReaderVersion shouldBe 4
        protocol.minWriterVersion shouldBe 4
      } finally
        txLog.close()
    }
  }

  // V3-specific tests

  test("V3 protocol features should be recognized") {
    ProtocolVersion.FEATURE_MULTI_PART_CHECKPOINT shouldBe "multiPartCheckpoint"
    ProtocolVersion.FEATURE_SCHEMA_DEDUPLICATION shouldBe "schemaDeduplication"

    ProtocolVersion.requiresV3Reader("multiPartCheckpoint") shouldBe true
    ProtocolVersion.requiresV3Reader("schemaDeduplication") shouldBe true
    ProtocolVersion.requiresV3Reader("skippedFiles") shouldBe false
    ProtocolVersion.requiresV3Reader("footerOffsets") shouldBe false
  }

  test("v2Protocol helper should return version 2/2") {
    val protocol = ProtocolVersion.v2Protocol()
    protocol.minReaderVersion shouldBe 2
    protocol.minWriterVersion shouldBe 2
  }

  test("usesV3Features should correctly identify V3 protocol") {
    ProtocolVersion.usesV3Features(ProtocolAction(1, 1)) shouldBe false
    ProtocolVersion.usesV3Features(ProtocolAction(2, 2)) shouldBe false
    ProtocolVersion.usesV3Features(ProtocolAction(3, 3)) shouldBe true
    ProtocolVersion.usesV3Features(ProtocolAction(4, 4)) shouldBe true
  }

  test("getMinReaderVersionForFeatures should return correct version") {
    // No V3 features
    ProtocolVersion.getMinReaderVersionForFeatures(Set("skippedFiles")) shouldBe 2
    ProtocolVersion.getMinReaderVersionForFeatures(Set("footerOffsets")) shouldBe 2
    ProtocolVersion.getMinReaderVersionForFeatures(Set.empty) shouldBe 2

    // V3 features
    ProtocolVersion.getMinReaderVersionForFeatures(Set("multiPartCheckpoint")) shouldBe 3
    ProtocolVersion.getMinReaderVersionForFeatures(Set("schemaDeduplication")) shouldBe 3
    ProtocolVersion.getMinReaderVersionForFeatures(Set("skippedFiles", "multiPartCheckpoint")) shouldBe 3
  }

  test("validateReaderVersion should throw on unsupported version") {
    // V4 reader should accept V1, V2, V3, V4
    ProtocolVersion.validateReaderVersion(ProtocolAction(1, 1))
    ProtocolVersion.validateReaderVersion(ProtocolAction(2, 2))
    ProtocolVersion.validateReaderVersion(ProtocolAction(3, 3))
    ProtocolVersion.validateReaderVersion(ProtocolAction(4, 4))

    // V4 reader should reject V5+
    val exception = intercept[ProtocolVersionException] {
      ProtocolVersion.validateReaderVersion(ProtocolAction(5, 5))
    }
    exception.getMessage should include("version 5")
    exception.getMessage should include("upgrade")
  }

  test("validateWriterVersion should throw on unsupported version") {
    // V4 writer should accept V1, V2, V3, V4
    ProtocolVersion.validateWriterVersion(ProtocolAction(1, 1))
    ProtocolVersion.validateWriterVersion(ProtocolAction(2, 2))
    ProtocolVersion.validateWriterVersion(ProtocolAction(3, 3))
    ProtocolVersion.validateWriterVersion(ProtocolAction(4, 4))

    // V4 writer should reject V5+
    val exception = intercept[ProtocolVersionException] {
      ProtocolVersion.validateWriterVersion(ProtocolAction(5, 5))
    }
    exception.getMessage should include("version 5")
    exception.getMessage should include("upgrade")
  }

  test("checkpoint with schema deduplication should upgrade to V4") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.checkpoint.enabled"  -> "true",
          "spark.indextables.checkpoint.interval" -> "3"
        ).asJava
      )
      val txLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        val schema = getTestSchema()
        txLog.initialize(schema)

        // Create AddActions with docMappingJson that will trigger schema deduplication
        val largeSchema = """{"fields":[{"name":"field1","type":"text"}]}"""
        for (i <- 1 to 5) {
          val addAction = AddAction(
            path = s"s3://bucket/file$i.split",
            partitionValues = Map.empty,
            size = 1000L * i,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            docMappingJson = Some(largeSchema)
          )
          txLog.addFiles(Seq(addAction))
        }

        // Checkpoint should have been created with V3 protocol
        val checkpointVersion = txLog.getLastCheckpointVersion()
        checkpointVersion shouldBe defined

        // Protocol should be V4 (includes schema deduplication + Avro state)
        val protocol = txLog.getProtocol()
        protocol.minReaderVersion shouldBe 4
        protocol.minWriterVersion shouldBe 4
      } finally
        txLog.close()
    }
  }
}
