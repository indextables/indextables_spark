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

import java.io.File

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.compression.CompressionUtils
import io.indextables.spark.util.JsonUtil
import io.indextables.spark.TestBase
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach

/**
 * Test suite to verify that checkpoint creation applies statistics truncation.
 *
 * This ensures that checkpoints created from old transaction files with long statistics will have those statistics
 * truncated, preventing checkpoint bloat.
 */
class CheckpointStatisticsTruncationTest
    extends AnyFunSuite
    with TestBase
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  test("checkpoint should truncate long statistics from transaction files") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = new File(tempDir, "test_table").getAbsolutePath

      // Create a table with a field that will have long statistics
      val longValue = "x" * 300 // 300 characters - exceeds default 256 threshold
      val data = Seq(
        ("doc1", longValue, 100),
        ("doc2", longValue + "y", 200),
        ("doc3", longValue + "z", 300)
      ).toDF("id", "description", "score")

      // Write initial data (will create transaction file with long statistics)
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "score")
        .option("spark.indextables.checkpoint.enabled", "false") // Disable auto checkpoint
        .mode("overwrite")
        .save(tablePath)

      // Append more data to create additional transaction files
      for (i <- 1 to 5) {
        val appendData = Seq(
          (s"doc${i * 10}", longValue + s"_$i", i * 100)
        ).toDF("id", "description", "score")

        appendData.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.checkpoint.enabled", "false") // Still no checkpoint
          .mode("append")
          .save(tablePath)
      }

      // Now trigger a checkpoint manually by enabling it and doing one more write
      val finalData = Seq(
        ("doc_final", longValue + "_final", 999)
      ).toDF("id", "description", "score")

      finalData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")          // Create checkpoint immediately
        .option("spark.indextables.stats.truncation.enabled", "true")  // Ensure truncation enabled
        .option("spark.indextables.stats.truncation.maxLength", "256") // Use default threshold
        .mode("append")
        .save(tablePath)

      // Read the checkpoint file
      val fs                 = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
      val transactionLogPath = new Path(tablePath, "_transaction_log")
      val files              = fs.listStatus(transactionLogPath)

      // Find the checkpoint file (should be the highest version .checkpoint.json)
      val checkpointFiles = files
        .filter(_.getPath.getName.endsWith(".checkpoint.json"))
        .sortBy(_.getPath.getName)
        .reverse

      assert(checkpointFiles.nonEmpty, "Should have created at least one checkpoint file")

      val checkpointFile = checkpointFiles.head
      println(s"Reading checkpoint file: ${checkpointFile.getPath.getName}")

      // Read and parse checkpoint content
      val rawBytes = org.apache.hadoop.io.IOUtils.readFullyToByteArray(fs.open(checkpointFile.getPath))
      // Decompress if needed (handles both compressed and uncompressed files)
      val decompressedBytes = CompressionUtils.readTransactionFile(rawBytes)
      val checkpointContent = new String(decompressedBytes, "UTF-8")
      val checkpointLines   = checkpointContent.split("\n").filter(_.nonEmpty)

      println(s"Checkpoint has ${checkpointLines.length} lines")

      // Parse each line and check AddActions for statistics
      var addActionCount          = 0
      var addActionsWithStats     = 0
      var addActionsWithLongStats = 0

      checkpointLines.foreach { line =>
        val json = JsonUtil.mapper.readTree(line)
        if (json.has("add")) {
          addActionCount += 1
          val add = json.get("add")

          // Check if it has statistics
          if (add.has("stats")) {
            val stats = add.get("stats")
            if (stats.has("minValues") || stats.has("maxValues")) {
              addActionsWithStats += 1

              // Check if any statistics values are > 256 characters
              Seq("minValues", "maxValues").foreach { field =>
                if (stats.has(field)) {
                  val values = stats.get(field)
                  values.fieldNames().forEachRemaining { fieldName =>
                    val value = values.get(fieldName).asText()
                    if (value.length > 256) {
                      addActionsWithLongStats += 1
                      println(s"⚠️ Found long statistic in checkpoint: field=$fieldName, length=${value.length}")
                    }
                  }
                }
              }
            }
          }
        }
      }

      println(s"Checkpoint analysis:")
      println(s"  Total AddActions: $addActionCount")
      println(s"  AddActions with statistics: $addActionsWithStats")
      println(s"  AddActions with long statistics (>256 chars): $addActionsWithLongStats")

      // The key assertion: checkpoint should NOT have long statistics
      assert(
        addActionsWithLongStats === 0,
        s"Checkpoint should have truncated long statistics, but found $addActionsWithLongStats AddActions with stats > 256 characters"
      )

      println("✅ Checkpoint successfully truncated all long statistics")
    }
  }

  ignore("checkpoint should preserve partition column statistics") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = new File(tempDir, "partitioned_table").getAbsolutePath

      // Create partition value and a data column with long value
      val partitionValue = "2024-01-01"
      val longDataValue  = "x" * 300 // This will have stats truncated
      val data = Seq(
        (partitionValue, "doc1", longDataValue, 100),
        (partitionValue, "doc2", longDataValue + "y", 200)
      ).toDF("partition_col", "id", "content", "score")

      // Write with partitioning and create checkpoint
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("partition_col")
        .option("spark.indextables.indexing.fastfields", "score,partition_col")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .option("spark.indextables.stats.truncation.enabled", "true")
        .mode("overwrite")
        .save(tablePath)

      // Read checkpoint
      val fs                 = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
      val transactionLogPath = new Path(tablePath, "_transaction_log")
      val files              = fs.listStatus(transactionLogPath)

      val checkpointFiles = files
        .filter(_.getPath.getName.endsWith(".checkpoint.json"))
        .sortBy(_.getPath.getName)
        .reverse

      assert(checkpointFiles.nonEmpty, "Should have created checkpoint")

      val rawBytes2 = org.apache.hadoop.io.IOUtils.readFullyToByteArray(fs.open(checkpointFiles.head.getPath))
      // Decompress if needed (handles both compressed and uncompressed files)
      val decompressedBytes2 = CompressionUtils.readTransactionFile(rawBytes2)
      val checkpointContent  = new String(decompressedBytes2, "UTF-8")
      val checkpointLines    = checkpointContent.split("\n").filter(_.nonEmpty)

      var partitionStatsFound = false
      var dataStatsFound      = false
      var dataStatsLong       = false

      checkpointLines.foreach { line =>
        val json = JsonUtil.mapper.readTree(line)
        if (json.has("add")) {
          val add = json.get("add")
          if (add.has("stats")) {
            val stats = add.get("stats")
            Seq("minValues", "maxValues").foreach { field =>
              if (stats.has(field)) {
                val values = stats.get(field)
                // Check partition column stats (should be preserved)
                if (values.has("partition_col")) {
                  partitionStatsFound = true
                  val partValue = values.get("partition_col").asText()
                  println(s"✅ Partition column statistic preserved: value=$partValue")
                }
                // Check data column stats (should be truncated if long)
                if (values.has("content")) {
                  dataStatsFound = true
                  val contentValue = values.get("content").asText()
                  if (contentValue.length > 256) {
                    dataStatsLong = true
                    println(s"⚠️ Data column statistic NOT truncated: length=${contentValue.length}")
                  }
                }
              }
            }
          }
        }
      }

      assert(partitionStatsFound, "Should have partition column statistics in checkpoint")
      assert(!dataStatsLong, "Data column statistics should be truncated in checkpoint")

      println("✅ Checkpoint preserved partition stats and truncated data column stats")
    }
  }
}
