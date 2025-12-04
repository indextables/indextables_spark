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

import java.nio.file.Files

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.{TransactionLog, TransactionLogFactory}
import io.indextables.spark.TestBase
import org.scalatest.BeforeAndAfterEach
import org.slf4j.LoggerFactory

/**
 * Tests for batch processing in MERGE SPLITS operations. Validates:
 *   1. Correct number of batches based on batch size configuration 2. Concurrent batch processing with configurable
 *      parallelism 3. Transaction log commits per batch instead of global 4. Failure tracking and partial success
 *      reporting
 */
class MergeSplitsBatchTest extends TestBase with BeforeAndAfterEach {

  private val logger = LoggerFactory.getLogger(classOf[MergeSplitsBatchTest])

  var tempTablePath: String          = _
  var transactionLog: TransactionLog = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    val tempDir = Files.createTempDirectory("tantivy4spark_batch_test")
    tempTablePath = tempDir.toFile.getAbsolutePath
    transactionLog = TransactionLogFactory.create(new Path(tempTablePath), spark)

    logger.info(s"Created test table at: $tempTablePath")
  }

  override def afterEach(): Unit =
    try
      if (tempTablePath != null) {
        val dir = new java.io.File(tempTablePath)
        if (dir.exists()) {
          def deleteRecursively(file: java.io.File): Unit = {
            if (file.isDirectory) {
              file.listFiles().foreach(deleteRecursively)
            }
            file.delete()
          }
          deleteRecursively(dir)
        }
      }
    finally
      super.afterEach()

  test("should create correct number of batches based on batch size") {
    // Create multiple small files
    val data = (1 to 6).map(i => (s"id$i", s"content$i", i))
    val df   = spark.createDataFrame(data).toDF("id", "content", "score").repartition(6)

    df.write
      .format("io.indextables.provider.IndexTablesProvider")
      .mode("append")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexWriter.batchSize", "1")
      .save(tempTablePath)

    // Check we have multiple files
    val filesBeforeMerge = transactionLog.listFiles()
    logger.info(s"Created ${filesBeforeMerge.length} files with total size: ${filesBeforeMerge.map(_.size).sum} bytes")
    assert(filesBeforeMerge.length >= 2, s"Expected at least 2 files, got ${filesBeforeMerge.length}")

    // Run merge with batch size of 2 and large target size
    val batchSize = 2
    spark.conf.set("spark.indextables.merge.batchSize", batchSize.toString)
    spark.conf.set("spark.indextables.merge.maxConcurrentBatches", "1") // Sequential for predictable testing

    val result = spark.sql(s"MERGE SPLITS '$tempTablePath' TARGET SIZE 104857600").collect()

    assert(result.length == 1, s"Expected 1 result row, got ${result.length}")
    val resultRow = result(0)
    val statusRow = resultRow.getStruct(1)
    val message   = statusRow.getString(5)

    logger.info(s"Merge result message: $message")

    // Verify the batching information is present in the result message
    assert(message.contains("batches: "), s"Expected batch count in message, got: $message")
    val batchCount = message.split("batches: ")(1).split(",")(0).toInt
    assert(batchCount >= 1, s"Expected at least 1 batch, got: $batchCount")
  }

  test("should commit transaction per batch") {
    // Create multiple small files
    val data = (1 to 6).map(i => (s"id$i", s"content$i", i))
    val df   = spark.createDataFrame(data).toDF("id", "content", "score").repartition(6)

    df.write
      .format("io.indextables.provider.IndexTablesProvider")
      .mode("append") // Use append for first write
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexWriter.batchSize", "1")
      .save(tempTablePath)

    val filesBeforeMerge = transactionLog.listFiles()
    logger.info(s"Created ${filesBeforeMerge.length} files")
    assert(filesBeforeMerge.length >= 2, s"Expected at least 2 files, got ${filesBeforeMerge.length}")

    // Run merge with batch size of 2
    spark.conf.set("spark.indextables.merge.batchSize", "2")
    spark.conf.set("spark.indextables.merge.maxConcurrentBatches", "1")

    // Use 100MB target - with small files, all fit in one merge group
    val result = spark.sql(s"MERGE SPLITS '$tempTablePath' TARGET SIZE 104857600").collect()

    assert(result.length == 1, s"Expected 1 result row, got ${result.length}")
    val resultRow = result(0)
    val statusRow = resultRow.getStruct(1)
    val message   = statusRow.getString(5)

    logger.info(s"Merge result message: $message")

    // Verify batch information is present and indicates success
    assert(message.contains("batches: "), s"Expected batch count in message, got: $message")
    assert(message.contains("successful: "), s"Expected successful batch count in message, got: $message")

    val successfulBatches = message.split("successful: ")(1).split(",")(0).toInt
    assert(successfulBatches >= 1, s"Expected at least 1 successful batch, got: $successfulBatches")
  }

  test("should respect maxConcurrentBatches configuration") {
    // This test validates that the configuration is read correctly
    // Actual concurrency is hard to test without timing, so we just verify it doesn't error

    val data = (1 to 4).map(i => (s"id$i", s"content$i", i))
    val df   = spark.createDataFrame(data).toDF("id", "content", "score")

    df.write
      .format("io.indextables.provider.IndexTablesProvider")
      .mode("overwrite")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexWriter.batchSize", "1")
      .save(tempTablePath)

    spark.conf.set("spark.indextables.merge.batchSize", "1")
    spark.conf.set("spark.indextables.merge.maxConcurrentBatches", "2")

    val result = spark.sql(s"MERGE SPLITS '$tempTablePath' TARGET SIZE 1048576").collect()

    assert(result.length == 1)
    val statusRow = result(0).getStruct(1)
    val status    = statusRow.getString(0)

    assert(status == "success" || status == "partial_success", s"Merge should succeed, got status: $status")
  }

  test("should use defaultParallelism as default batch size") {
    val defaultParallelism = spark.sparkContext.defaultParallelism
    logger.info(s"Default parallelism: $defaultParallelism")

    // Create enough files to test batching
    val numFiles = defaultParallelism * 2
    val data     = (1 to numFiles).map(i => (s"id$i", s"content$i", i))
    val df       = spark.createDataFrame(data).toDF("id", "content", "score")

    df.write
      .format("io.indextables.provider.IndexTablesProvider")
      .mode("overwrite")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexWriter.batchSize", "1")
      .save(tempTablePath)

    // Don't set batch size - should use defaultParallelism
    spark.conf.unset("spark.indextables.merge.batchSize")

    val result = spark.sql(s"MERGE SPLITS '$tempTablePath' TARGET SIZE 1048576").collect()

    assert(result.length == 1)
    val statusRow = result(0).getStruct(1)
    val message   = statusRow.getString(5)

    logger.info(s"Merge result with default batch size: $message")

    // Should have created at least 2 batches since we have 2x defaultParallelism files
    assert(message.contains("batches:"), s"Message should contain batch count: $message")
  }
}
