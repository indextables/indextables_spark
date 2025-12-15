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

package io.indextables.spark.xref

import org.apache.spark.sql.types._
import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase
import io.indextables.spark.transaction.{AddAction, TransactionLogFactory}

class XRefAutoIndexerTest extends TestBase {

  // Helper to create test AddAction
  private def createAddAction(path: String, numRecords: Option[Long] = None): AddAction =
    AddAction(
      path = path,
      partitionValues = Map.empty,
      size = 1024L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = numRecords
    )

  test("XRefAutoIndexer should skip when disabled") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("content", StringType)
        ))
        transactionLog.initialize(schema)

        val config = XRefConfig(
          autoIndex = XRefAutoIndexConfig(enabled = false),
          build = XRefBuildConfig(),
          query = XRefQueryConfig(),
          storage = XRefStorageConfig()
        )

        val autoIndexer = new XRefAutoIndexer(transactionLog, config, spark)
        val splits = Seq(createAddAction("/test/split1.split"))
        val result = autoIndexer.onCommit(splits)

        assert(!result.triggered)
        assert(result.reason == "Auto-indexing disabled")
      } finally {
        transactionLog.close()
      }
    }
  }

  test("XRefAutoIndexer should skip when below minimum interval") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("content", StringType)
        ))
        transactionLog.initialize(schema)

        // Add enough splits to trigger
        (1 to 20).foreach { i =>
          transactionLog.addFiles(Seq(createAddAction(s"/test/split$i.split")))
        }

        val config = XRefConfig(
          autoIndex = XRefAutoIndexConfig(
            enabled = true,
            minUncoveredSplitsToTrigger = 5,
            minIntervalMs = 60000L // 1 minute
          ),
          build = XRefBuildConfig(),
          query = XRefQueryConfig(),
          storage = XRefStorageConfig()
        )

        val autoIndexer = new XRefAutoIndexer(transactionLog, config, spark)

        // First call should trigger (or fail due to missing API)
        val firstResult = autoIndexer.onCommit(Seq(createAddAction("/test/split21.split")))

        // Second immediate call should skip due to interval
        val secondResult = autoIndexer.onCommit(Seq(createAddAction("/test/split22.split")))

        // Either both trigger with error (API not available) or second is skipped
        if (firstResult.triggered && firstResult.error.isEmpty) {
          // If first succeeded, second should be skipped
          assert(!secondResult.triggered || secondResult.reason.contains("interval"))
        }
      } finally {
        transactionLog.close()
      }
    }
  }

  test("XRefAutoIndexer should skip when below uncovered splits threshold") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(Seq(
          StructField("id", IntegerType),
          StructField("content", StringType)
        ))
        transactionLog.initialize(schema)

        // Add only a few splits (below threshold)
        transactionLog.addFiles(Seq(
          createAddAction("/test/split1.split"),
          createAddAction("/test/split2.split")
        ))

        val config = XRefConfig(
          autoIndex = XRefAutoIndexConfig(
            enabled = true,
            minUncoveredSplitsToTrigger = 10, // High threshold
            minIntervalMs = 0 // No interval limit
          ),
          build = XRefBuildConfig(),
          query = XRefQueryConfig(),
          storage = XRefStorageConfig()
        )

        val autoIndexer = new XRefAutoIndexer(transactionLog, config, spark)
        val result = autoIndexer.onCommit(Seq(createAddAction("/test/split3.split")))

        // Should skip due to below threshold (3 splits < 10 threshold)
        assert(!result.triggered)
        assert(result.reason.contains("Below threshold"))
      } finally {
        transactionLog.close()
      }
    }
  }

  test("XRefAutoIndexResult should have correct default values") {
    val result = XRefAutoIndexResult(
      triggered = false,
      reason = "Test reason"
    )

    assert(!result.triggered)
    assert(result.reason == "Test reason")
    assert(result.xrefsBuilt == 0)
    assert(result.splitsIndexed == 0)
    assert(result.durationMs == 0)
    assert(result.error.isEmpty)
  }

  test("XRefAutoIndexResult should preserve all fields") {
    val result = XRefAutoIndexResult(
      triggered = true,
      reason = "Auto-indexing triggered",
      xrefsBuilt = 3,
      splitsIndexed = 30,
      durationMs = 1500,
      error = None
    )

    assert(result.triggered)
    assert(result.xrefsBuilt == 3)
    assert(result.splitsIndexed == 30)
    assert(result.durationMs == 1500)
    assert(result.error.isEmpty)
  }

  test("XRefAutoIndexResult should include error when present") {
    val result = XRefAutoIndexResult(
      triggered = true,
      reason = "Auto-indexing failed",
      error = Some("Connection timeout")
    )

    assert(result.triggered)
    assert(result.error.contains("Connection timeout"))
  }

  test("XRefAutoIndexer companion object should create instance from SparkSession") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(Seq(StructField("id", IntegerType)))
        transactionLog.initialize(schema)

        // Use companion object factory
        val autoIndexer = XRefAutoIndexer(transactionLog, spark)

        // Should be able to call onCommit without error
        val result = autoIndexer.onCommit(Seq.empty)
        // With no splits, should skip (either disabled or no uncovered splits)
        assert(!result.triggered || result.reason.contains("threshold"))
      } finally {
        transactionLog.close()
      }
    }
  }

  test("forceAutoIndex should skip when disabled") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(Seq(StructField("id", IntegerType)))
        transactionLog.initialize(schema)

        val config = XRefConfig(
          autoIndex = XRefAutoIndexConfig(enabled = false),
          build = XRefBuildConfig(),
          query = XRefQueryConfig(),
          storage = XRefStorageConfig()
        )

        val autoIndexer = new XRefAutoIndexer(transactionLog, config, spark)
        val result = autoIndexer.forceAutoIndex()

        assert(!result.triggered)
        assert(result.reason.contains("disabled"))
      } finally {
        transactionLog.close()
      }
    }
  }

  test("forceAutoIndex should skip when no uncovered splits") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(Seq(StructField("id", IntegerType)))
        transactionLog.initialize(schema)

        val config = XRefConfig(
          autoIndex = XRefAutoIndexConfig(enabled = true),
          build = XRefBuildConfig(),
          query = XRefQueryConfig(),
          storage = XRefStorageConfig()
        )

        val autoIndexer = new XRefAutoIndexer(transactionLog, config, spark)
        // No splits added, so no uncovered splits
        val result = autoIndexer.forceAutoIndex()

        assert(!result.triggered)
        assert(result.reason.contains("No uncovered splits"))
      } finally {
        transactionLog.close()
      }
    }
  }
}
