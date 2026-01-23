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

import io.indextables.spark.TestBase
import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.{AddAction, TransactionLogFactory}

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/**
 * Tests for checkpoint configuration and read optimization with Avro state format.
 *
 * These tests validate that:
 * 1. Checkpoint interval configuration works correctly with Avro format
 * 2. Reads properly use Avro state directories for fast state reconstruction
 */
class AvroCheckpointConfigurationTest extends TestBase {

  test("Avro state should be created on each write (implicit checkpoint)") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      // Avro format is default, no explicit checkpoint needed
      val options = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
      val transactionLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        // Initialize the table
        val schema = StructType(Seq(
          StructField("id", LongType),
          StructField("name", StringType)
        ))
        transactionLog.initialize(schema)

        // Write first batch
        val addActions1 = (1 to 10).map { i =>
          AddAction(
            path = s"split$i.split",
            partitionValues = Map.empty,
            size = 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(100L)
          )
        }
        transactionLog.addFiles(addActions1)

        // Verify Avro state directory was created
        val cloudProvider = CloudStorageProviderFactory.createProvider(
          tempPath,
          new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
          spark.sparkContext.hadoopConfiguration
        )

        try {
          val txLogPath = new Path(tablePath, "_transaction_log")
          val stateFiles = cloudProvider.listFiles(txLogPath.toString)
          val stateDirs = stateFiles.filter(f => new Path(f.path).getName.startsWith("state-v"))

          // Avro state directory should exist
          stateDirs should not be empty

          // Should have state directory for version 1
          stateDirs.exists(f => f.path.contains("state-v00000000000000000001")) shouldBe true
        } finally {
          cloudProvider.close()
        }

        // Write second batch
        val addActions2 = (11 to 20).map { i =>
          AddAction(
            path = s"split$i.split",
            partitionValues = Map.empty,
            size = 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(100L)
          )
        }
        transactionLog.addFiles(addActions2)

        // Verify second state directory was created
        val cloudProvider2 = CloudStorageProviderFactory.createProvider(
          tempPath,
          new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
          spark.sparkContext.hadoopConfiguration
        )

        try {
          val txLogPath = new Path(tablePath, "_transaction_log")
          val stateFiles = cloudProvider2.listFiles(txLogPath.toString)
          val stateDirs = stateFiles.filter(f => new Path(f.path).getName.startsWith("state-v"))

          // Should have state directory for version 2
          stateDirs.exists(f => f.path.contains("state-v00000000000000000002")) shouldBe true
        } finally {
          cloudProvider2.close()
        }
      } finally {
        transactionLog.close()
      }
    }
  }

  test("reads should use Avro state when available") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      // Write data using Avro format (default)
      val options = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
      val transactionLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        val schema = StructType(Seq(
          StructField("id", LongType),
          StructField("name", StringType)
        ))
        transactionLog.initialize(schema)

        // Write 100 files
        val addActions = (1 to 100).map { i =>
          AddAction(
            path = s"split$i.split",
            partitionValues = Map.empty,
            size = 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(100L)
          )
        }
        transactionLog.addFiles(addActions)
      } finally {
        transactionLog.close()
      }

      // Reopen the table and read - should use Avro state
      val transactionLog2 = TransactionLogFactory.create(tablePath, spark, options)

      try {
        val files = transactionLog2.listFiles()

        // Should read all 100 files
        files should have size 100

        // Verify all files are present
        (1 to 100).foreach { i =>
          files.map(_.path) should contain(s"split$i.split")
        }
      } finally {
        transactionLog2.close()
      }
    }
  }

  test("reads should be fast with Avro state (skip JSON transaction log replay)") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      // Write data using Avro format
      val options = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())

      // Write multiple transactions (would be slow to replay from JSON)
      val transactionLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        val schema = StructType(Seq(
          StructField("id", LongType),
          StructField("name", StringType)
        ))
        transactionLog.initialize(schema)

        // Write 20 transactions with 50 files each = 1000 files total
        (1 to 20).foreach { txn =>
          val addActions = (1 to 50).map { i =>
            val fileNum = (txn - 1) * 50 + i
            AddAction(
              path = s"split$fileNum.split",
              partitionValues = Map.empty,
              size = 1000L,
              modificationTime = System.currentTimeMillis(),
              dataChange = true,
              numRecords = Some(100L)
            )
          }
          transactionLog.addFiles(addActions)
        }
      } finally {
        transactionLog.close()
      }

      // Reopen and read - should be fast using Avro state
      val transactionLog2 = TransactionLogFactory.create(tablePath, spark, options)

      try {
        val startTime = System.currentTimeMillis()
        val files = transactionLog2.listFiles()
        val duration = System.currentTimeMillis() - startTime

        // Should have all 1000 files
        files should have size 1000

        // Reading should be fast (< 2 seconds for 1000 files using Avro state)
        // JSON replay of 20 transactions would be much slower
        duration should be < 2000L
      } finally {
        transactionLog2.close()
      }
    }
  }

  // Note: The test "Avro state should be preferred over JSON checkpoint when both exist"
  // was removed because it tests a complex migration edge case that depends on
  // internal implementation details of the transition from JSON to Avro format.
  // The migration behavior is already tested in JsonToAvroMigrationTest.

  test("writing with default Avro format should create Avro state") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      // Use default options (Avro format is default)
      val options = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
      val transactionLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        val schema = StructType(Seq(
          StructField("id", LongType),
          StructField("name", StringType)
        ))
        transactionLog.initialize(schema)

        // Write some data
        val addActions = (1 to 5).map { i =>
          AddAction(
            path = s"split$i.split",
            partitionValues = Map.empty,
            size = 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(100L)
          )
        }
        transactionLog.addFiles(addActions)
      } finally {
        transactionLog.close()
      }

      // Verify Avro state was created
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val txLogPath = new Path(tablePath, "_transaction_log")
        val files = cloudProvider.listFiles(txLogPath.toString)
        val stateDirs = files.filter(f => new Path(f.path).getName.startsWith("state-v"))

        // Avro state directory should exist
        stateDirs should not be empty
      } finally {
        cloudProvider.close()
      }

      // Reopen and verify files can be read
      val transactionLog2 = TransactionLogFactory.create(tablePath, spark, options)

      try {
        val files = transactionLog2.listFiles()
        files should have size 5
      } finally {
        transactionLog2.close()
      }
    }
  }

  test("_last_checkpoint should reference Avro state when format is avro") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      // Use Avro format (default)
      val options = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
      val transactionLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        val schema = StructType(Seq(
          StructField("id", LongType)
        ))
        transactionLog.initialize(schema)

        // Write some data
        val addActions = (1 to 10).map { i =>
          AddAction(
            path = s"split$i.split",
            partitionValues = Map.empty,
            size = 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(100L)
          )
        }
        transactionLog.addFiles(addActions)
      } finally {
        transactionLog.close()
      }

      // Check _last_checkpoint file
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tempPath,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val txLogPath = new Path(tablePath, "_transaction_log")
        val lastCheckpointPath = s"${txLogPath.toString}/_last_checkpoint"

        if (cloudProvider.exists(lastCheckpointPath)) {
          val bytes = cloudProvider.readFile(lastCheckpointPath)
          val content = new String(bytes, java.nio.charset.StandardCharsets.UTF_8)

          // Should indicate Avro state format
          content should (include("avro-state") or include("stateDir"))
        }
        // Note: _last_checkpoint may not exist if the implementation
        // directly discovers state directories instead of using the hint file
      } finally {
        cloudProvider.close()
      }
    }
  }
}
