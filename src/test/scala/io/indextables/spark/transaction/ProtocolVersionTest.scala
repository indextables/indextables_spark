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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase

class ProtocolVersionTest extends TestBase {

  test("new table should have protocol version 2/2") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val txLog     = TransactionLogFactory.create(tablePath, spark)

      try {
        val schema = getTestSchema()
        txLog.initialize(schema)

        val protocol = txLog.getProtocol()
        protocol.minReaderVersion shouldBe 2
        protocol.minWriterVersion shouldBe 2
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
        protocolAction.get.minReaderVersion shouldBe 2
        protocolAction.get.minWriterVersion shouldBe 2
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

        // Current version (2/2) should be able to read
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
        initialProtocol.minReaderVersion shouldBe 2
        initialProtocol.minWriterVersion shouldBe 2

        // Try to downgrade (should not work)
        txLog.upgradeProtocol(1, 1)

        val protocol = txLog.getProtocol()
        protocol.minReaderVersion shouldBe 2 // Should not downgrade
        protocol.minWriterVersion shouldBe 2

        // Upgrade to higher version
        txLog.upgradeProtocol(3, 3)

        val upgradedProtocol = txLog.getProtocol()
        upgradedProtocol.minReaderVersion shouldBe 3 // Should upgrade
        upgradedProtocol.minWriterVersion shouldBe 3
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
        protocol.minReaderVersion shouldBe 2
        protocol.minWriterVersion shouldBe 2
      } finally
        txLog.close()
    }
  }

  test("protocol serialization and deserialization") {
    val protocol = ProtocolAction(
      minReaderVersion = 2,
      minWriterVersion = 2,
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
    ProtocolVersion.isReaderVersionSupported(999) shouldBe false

    ProtocolVersion.isWriterVersionSupported(1) shouldBe true
    ProtocolVersion.isWriterVersionSupported(2) shouldBe true
    ProtocolVersion.isWriterVersionSupported(999) shouldBe false
  }

  test("default protocol should be version 2/2") {
    val protocol = ProtocolVersion.defaultProtocol()
    protocol.minReaderVersion shouldBe 2
    protocol.minWriterVersion shouldBe 2
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
        initialProtocol.minReaderVersion shouldBe 2
        initialProtocol.minWriterVersion shouldBe 2

        // Try to upgrade with auto-upgrade disabled
        txLog.upgradeProtocol(3, 3)

        // Protocol should remain unchanged
        val protocol = txLog.getProtocol()
        protocol.minReaderVersion shouldBe 2
        protocol.minWriterVersion shouldBe 2
      } finally
        txLog.close()
    }
  }
}
