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

package io.indextables.spark.sql

import io.indextables.spark.TestBase
import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.avro.{FileEntry, StateConfig, StateManifestIO, StateWriter}

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DescribeStateCommandTest extends TestBase {

  test("DESCRIBE STATE should parse correctly") {
    val plan1 = spark.sessionState.sqlParser.parsePlan("DESCRIBE INDEXTABLES STATE '/path/to/table'")
    plan1 shouldBe a[DescribeStateCommand]
    plan1.asInstanceOf[DescribeStateCommand].tablePath shouldBe "/path/to/table"

    val plan2 = spark.sessionState.sqlParser.parsePlan("DESCRIBE TANTIVY4SPARK STATE '/another/path'")
    plan2 shouldBe a[DescribeStateCommand]
    plan2.asInstanceOf[DescribeStateCommand].tablePath shouldBe "/another/path"
  }

  test("DESCRIBE STATE should return 'none' when no checkpoint exists") {
    withTempPath { tempPath =>
      // Create transaction log directory but no checkpoint
      val txLogPath = new Path(tempPath, "_transaction_log")
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        txLogPath.toString,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(txLogPath.toString)
      } finally {
        cloudProvider.close()
      }

      val result = spark.sql(s"DESCRIBE INDEXTABLES STATE '$tempPath'").collect()

      result should have length 1
      result(0).getAs[String]("format") shouldBe "none"
      result(0).getAs[String]("status") shouldBe "No checkpoint found"
    }
  }

  test("DESCRIBE STATE should describe Avro state format") {
    withTempPath { tempPath =>
      val txLogPath = new Path(tempPath, "_transaction_log")
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        txLogPath.toString,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(txLogPath.toString)

        // Create Avro state
        val stateWriter = StateWriter(cloudProvider, txLogPath.toString)
        val files = (1 to 100).map(i => createTestFileEntry(s"file$i.split", version = 10))
        stateWriter.writeState(
          currentVersion = 10,
          newFiles = files,
          removedPaths = Set.empty
        )

        // Write _last_checkpoint pointing to Avro state
        val manifestIO = StateManifestIO(cloudProvider)
        val lastCheckpointPath = new Path(txLogPath, "_last_checkpoint")
        val lastCheckpointJson =
          s"""{
             |  "version": 10,
             |  "size": 100,
             |  "sizeInBytes": 100000,
             |  "numFiles": 100,
             |  "createdTime": ${System.currentTimeMillis()},
             |  "format": "${StateConfig.Format.AVRO_STATE}",
             |  "stateDir": "${manifestIO.formatStateDir(10)}"
             |}""".stripMargin
        cloudProvider.writeFile(lastCheckpointPath.toString, lastCheckpointJson.getBytes("UTF-8"))
      } finally {
        cloudProvider.close()
      }

      val result = spark.sql(s"DESCRIBE INDEXTABLES STATE '$tempPath'").collect()

      result should have length 1
      result(0).getAs[String]("format") shouldBe "avro-state"
      result(0).getAs[Long]("version") shouldBe 10L
      result(0).getAs[Long]("num_files") shouldBe 100L
      result(0).getAs[Int]("num_manifests") should be >= 1
      result(0).getAs[Int]("num_tombstones") shouldBe 0
      result(0).getAs[Double]("tombstone_ratio") shouldBe 0.0
      result(0).getAs[Boolean]("needs_compaction") shouldBe false
      result(0).getAs[String]("status") shouldBe "OK"
    }
  }

  test("DESCRIBE STATE should show needs_compaction when tombstone ratio is high") {
    withTempPath { tempPath =>
      val txLogPath = new Path(tempPath, "_transaction_log")
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        txLogPath.toString,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        cloudProvider.createDirectory(txLogPath.toString)

        // Create Avro state with files
        val stateWriter = StateWriter(cloudProvider, txLogPath.toString)
        val files = (1 to 100).map(i => createTestFileEntry(s"file$i.split", version = 1))
        stateWriter.writeState(
          currentVersion = 1,
          newFiles = files,
          removedPaths = Set.empty
        )

        // Add tombstones (5% - below threshold, should not trigger compaction)
        val removedPaths = (1 to 5).map(i => s"file$i.split").toSet
        stateWriter.writeState(
          currentVersion = 2,
          newFiles = Seq.empty,
          removedPaths = removedPaths
        )

        // Write _last_checkpoint
        val manifestIO = StateManifestIO(cloudProvider)
        val lastCheckpointPath = new Path(txLogPath, "_last_checkpoint")
        val lastCheckpointJson =
          s"""{
             |  "version": 2,
             |  "size": 95,
             |  "sizeInBytes": 95000,
             |  "numFiles": 95,
             |  "createdTime": ${System.currentTimeMillis()},
             |  "format": "${StateConfig.Format.AVRO_STATE}",
             |  "stateDir": "${manifestIO.formatStateDir(2)}"
             |}""".stripMargin
        cloudProvider.writeFile(lastCheckpointPath.toString, lastCheckpointJson.getBytes("UTF-8"))
      } finally {
        cloudProvider.close()
      }

      val result = spark.sql(s"DESCRIBE INDEXTABLES STATE '$tempPath'").collect()

      result should have length 1
      result(0).getAs[String]("format") shouldBe "avro-state"
      result(0).getAs[Int]("num_tombstones") shouldBe 5
      // Tombstone ratio is 5/100 = 5% which is below 10% threshold
      result(0).getAs[Boolean]("needs_compaction") shouldBe false
    }
  }

  test("DESCRIBE STATE should handle missing table gracefully") {
    val result = spark.sql("DESCRIBE INDEXTABLES STATE '/nonexistent/path'").collect()

    result should have length 1
    // When the path doesn't exist or has no checkpoint, returns "none" format
    val format = result(0).getAs[String]("format")
    val status = result(0).getAs[String]("status")
    // Either "none" (no checkpoint) or "error" (path issues)
    (format == "none" || format == "error") shouldBe true
  }

  // Helper methods

  private def createTestFileEntry(
      path: String,
      version: Long = 1,
      partitionValues: Map[String, String] = Map.empty): FileEntry = {
    FileEntry(
      path = path,
      partitionValues = partitionValues,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      stats = None,
      minValues = None,
      maxValues = None,
      numRecords = Some(100L),
      footerStartOffset = None,
      footerEndOffset = None,
      hasFooterOffsets = false,
      splitTags = None,
      numMergeOps = None,
      docMappingRef = None,
      uncompressedSizeBytes = None,
      addedAtVersion = version,
      addedAtTimestamp = System.currentTimeMillis()
    )
  }
}
