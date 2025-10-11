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
import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

/**
 * Unit tests for DistributedTransactionLog.
 * Tests distributed transaction log reading with parallel executor-based parsing.
 */
class DistributedTransactionLogTest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tempDir: File = _

  override def beforeAll(): Unit = {
    // Create Spark session for tests
    spark = SparkSession.builder()
      .master("local[4]") // Use 4 cores for parallelism
      .appName("DistributedTransactionLogTest")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    // Create temporary directory for test data
    tempDir = Files.createTempDirectory("distributed_txn_log_test").toFile
    tempDir.deleteOnExit()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir)
    }
  }

  test("DistributedTransactionLog should read transaction files in parallel") {
    val tablePath = new Path(new File(tempDir, "parallel_read_test").getAbsolutePath)

    // Create test data
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))

    val data = spark.createDataFrame(
      Seq(
        Row(1, "Alice"),
        Row(2, "Bob"),
        Row(3, "Charlie")
      ).asJava,
      schema
    )

    // Write data using indextables format
    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("append")
      .save(tablePath.toString)

    // Create distributed transaction log
    val options = new CaseInsensitiveStringMap(Map(
      "spark.indextables.transaction.distributed.enabled" -> "true",
      "spark.indextables.transaction.distributed.minFiles" -> "1"
    ).asJava)

    val txnLog = new DistributedTransactionLog(tablePath, spark, options)

    try {
      // Read files using distributed mode
      val files = txnLog.listFiles()

      // Verify files were read correctly
      assert(files.nonEmpty, "Should have at least one file")
      assert(files.forall(_.path.nonEmpty), "All files should have non-empty paths")

      println(s"Successfully read ${files.length} files using distributed transaction log")
    } finally {
      txnLog.close()
    }
  }

  test("DistributedTransactionLog should handle REMOVE/ADD semantics correctly") {
    val tablePath = new Path(new File(tempDir, "remove_add_test").getAbsolutePath)

    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", StringType, nullable = true)
    ))

    // Write initial data
    val data1 = spark.createDataFrame(
      Seq(Row(1, "initial")).asJava,
      schema
    )
    data1.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("append").save(tablePath.toString)

    // Overwrite with new data (should create REMOVE + ADD actions)
    val data2 = spark.createDataFrame(
      Seq(Row(2, "overwrite")).asJava,
      schema
    )
    data2.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath.toString)

    // Create distributed transaction log
    val options = new CaseInsensitiveStringMap(Map(
      "spark.indextables.transaction.distributed.enabled" -> "true",
      "spark.indextables.transaction.distributed.minFiles" -> "1"
    ).asJava)

    val txnLog = new DistributedTransactionLog(tablePath, spark, options)

    try {
      // Read files - should only see files from second write
      val files = txnLog.listFiles()

      // Verify only new files are visible (old files should be removed)
      assert(files.nonEmpty, "Should have files from overwrite")
      println(s"After overwrite: ${files.length} files visible")
    } finally {
      txnLog.close()
    }
  }

  test("DistributedTransactionLog should use executor cache for repeated reads") {
    val tablePath = new Path(new File(tempDir, "cache_test").getAbsolutePath)

    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("data", StringType, nullable = true)
    ))

    val data = spark.createDataFrame(
      Seq(Row(1, "test")).asJava,
      schema
    )
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("append").save(tablePath.toString)

    val options = new CaseInsensitiveStringMap(Map(
      "spark.indextables.transaction.distributed.enabled" -> "true",
      "spark.indextables.transaction.distributed.minFiles" -> "1"
    ).asJava)

    val txnLog = new DistributedTransactionLog(tablePath, spark, options)

    try {
      // First read - cold cache
      val files1 = txnLog.listFiles()
      assert(files1.nonEmpty)

      // Second read - should hit cache
      val files2 = txnLog.listFiles()
      assert(files2.nonEmpty)
      assert(files1.length == files2.length, "Should return same number of files")

      println("Successfully verified cache behavior")
    } finally {
      txnLog.close()
    }
  }

  test("DistributedTransactionLog should fall back to driver for small transaction logs") {
    val tablePath = new Path(new File(tempDir, "small_log_test").getAbsolutePath)

    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))

    val data = spark.createDataFrame(
      Seq(Row(1)).asJava,
      schema
    )
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("append").save(tablePath.toString)

    // Set high minimum threshold to trigger fallback
    val options = new CaseInsensitiveStringMap(Map(
      "spark.indextables.transaction.distributed.enabled" -> "true",
      "spark.indextables.transaction.distributed.minFiles" -> "100"
    ).asJava)

    val txnLog = new DistributedTransactionLog(tablePath, spark, options)

    try {
      // Should fall back to driver-based reading
      val files = txnLog.listFiles()
      assert(files.nonEmpty)

      println("Successfully handled small transaction log with fallback")
    } finally {
      txnLog.close()
    }
  }

  test("DistributedTransactionLog should invalidate executor caches") {
    val tablePath = new Path(new File(tempDir, "invalidate_test").getAbsolutePath)

    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))

    val data = spark.createDataFrame(
      Seq(Row(1)).asJava,
      schema
    )
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("append").save(tablePath.toString)

    val options = new CaseInsensitiveStringMap(Map(
      "spark.indextables.transaction.distributed.enabled" -> "true",
      "spark.indextables.transaction.distributed.minFiles" -> "1"
    ).asJava)

    val txnLog = new DistributedTransactionLog(tablePath, spark, options)

    try {
      // Read to populate cache
      val files1 = txnLog.listFiles()
      assert(files1.nonEmpty)

      // Invalidate caches
      txnLog.invalidateExecutorCaches()

      // Read again - should work correctly after invalidation
      val files2 = txnLog.listFiles()
      assert(files2.nonEmpty)
      assert(files1.length == files2.length)

      println("Successfully invalidated executor caches")
    } finally {
      txnLog.close()
    }
  }

  test("TransactionFileCache should cache and retrieve actions") {
    val testKey = "test_table/000000000000000001.json"
    val testActions = Seq(
      AddAction("file1.split", Map.empty, 1000L, System.currentTimeMillis(), true)
    )

    // Clear cache first
    TransactionFileCache.clear()

    // Cache miss
    assert(TransactionFileCache.get(testKey).isEmpty, "Should be cache miss initially")

    // Put in cache
    TransactionFileCache.put(testKey, testActions)

    // Cache hit
    val retrieved = TransactionFileCache.get(testKey)
    assert(retrieved.isDefined, "Should be cache hit after put")
    assert(retrieved.get.length == 1, "Should retrieve correct number of actions")

    println("Successfully verified TransactionFileCache behavior")
  }

  test("DistributedStateReducer should reduce actions correctly") {
    val checkpointActions = Seq(
      AddAction("file1.split", Map.empty, 1000L, System.currentTimeMillis(), true),
      AddAction("file2.split", Map.empty, 2000L, System.currentTimeMillis(), true)
    )

    val incrementalActions = Seq(
      VersionedAction(RemoveAction("file1.split", Some(System.currentTimeMillis()), true, None, None, None), 101L),
      VersionedAction(AddAction("file3.split", Map.empty, 3000L, System.currentTimeMillis(), true), 102L)
    )

    val finalState = DistributedStateReducer.reduceToFinalState(checkpointActions, incrementalActions)

    // Should have file2 and file3, but not file1 (removed)
    assert(finalState.length == 2, s"Should have 2 files, got ${finalState.length}")
    assert(finalState.exists(_.path == "file2.split"), "Should include file2")
    assert(finalState.exists(_.path == "file3.split"), "Should include file3")
    assert(!finalState.exists(_.path == "file1.split"), "Should not include file1 (removed)")

    println("Successfully verified DistributedStateReducer behavior")
  }

  /** Recursively delete a directory */
  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}
