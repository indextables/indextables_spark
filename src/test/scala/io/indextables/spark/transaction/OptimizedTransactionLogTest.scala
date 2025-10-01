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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

class OptimizedTransactionLogTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private var testDir: String     = _

  override def beforeAll(): Unit = {
    // Set log level to DEBUG for transaction log
    org.apache.log4j.Logger.getLogger("io.indextables.spark.transaction").setLevel(org.apache.log4j.Level.DEBUG)

    spark = SparkSession
      .builder()
      .appName("OptimizedTransactionLogTest")
      .master("local[*]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.Tantivy4SparkExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  override def beforeEach(): Unit =
    testDir = Files.createTempDirectory("optimized_txn_log_test").toString

  override def afterEach(): Unit =
    // Clean up test directory
    if (testDir != null) {
      import scala.reflect.io.Directory
      val directory = new Directory(new java.io.File(testDir))
      directory.deleteRecursively()
    }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  test("OptimizedTransactionLog should initialize with schema") {
    val options = new CaseInsensitiveStringMap(
      Map(
        "spark.indextables.transaction.optimized.enabled" -> "true"
      ).asJava
    )

    val tablePath = new Path(testDir, "test_table")
    val txnLog    = new OptimizedTransactionLog(tablePath, spark, options)

    try {
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, false),
          StructField("name", StringType, true)
        )
      )

      txnLog.initialize(schema)

      // Verify metadata
      val metadata = txnLog.getMetadata()
      assert(metadata != null)
      assert(metadata.schemaString == schema.json)
    } finally
      txnLog.close()
  }

  test("OptimizedTransactionLog should add and list files") {
    val options = new CaseInsensitiveStringMap(
      Map(
        "spark.indextables.transaction.optimized.enabled" -> "true"
      ).asJava
    )

    val tablePath = new Path(testDir, "test_table")
    val txnLog    = new OptimizedTransactionLog(tablePath, spark, options)

    try {
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, false),
          StructField("content", StringType, true)
        )
      )

      txnLog.initialize(schema)

      // Add some files
      val addActions = Seq(
        AddAction(
          path = "file1.split",
          partitionValues = Map.empty,
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(100L)
        ),
        AddAction(
          path = "file2.split",
          partitionValues = Map.empty,
          size = 2000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(200L)
        )
      )

      val version = txnLog.addFiles(addActions)
      assert(version >= 0)

      // List files
      val files = txnLog.listFiles()
      assert(files.size == 2)
      assert(files.map(_.path).toSet == Set("file1.split", "file2.split"))

      // Check row count
      val rowCount = txnLog.getTotalRowCount()
      assert(rowCount == 300L)
    } finally
      txnLog.close()
  }

  test("OptimizedTransactionLog should handle overwrite operations") {
    val options = new CaseInsensitiveStringMap(
      Map(
        "spark.indextables.transaction.optimized.enabled" -> "true",
        "spark.indextables.parallel.read.enabled"         -> "false" // Disable parallel read to avoid caching issues
      ).asJava
    )

    val tablePath = new Path(testDir, "test_table")
    val txnLog    = new OptimizedTransactionLog(tablePath, spark, options)

    try {
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, false),
          StructField("value", DoubleType, true)
        )
      )

      txnLog.initialize(schema)

      // Add initial files
      val initialFiles = Seq(
        AddAction(
          path = "file1.split",
          partitionValues = Map.empty,
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true
        ),
        AddAction(
          path = "file2.split",
          partitionValues = Map.empty,
          size = 2000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true
        )
      )

      txnLog.addFiles(initialFiles)

      // Overwrite with new files
      val overwriteFiles = Seq(
        AddAction(
          path = "file3.split",
          partitionValues = Map.empty,
          size = 3000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true
        )
      )

      val overwriteVersion = txnLog.overwriteFiles(overwriteFiles)
      assert(overwriteVersion >= 1)
      println(s"Overwrite version: $overwriteVersion")

      // List transaction log files for debugging
      val txnLogPath = new java.io.File(testDir, "test_table/_transaction_log")
      if (txnLogPath.exists()) {
        val txnFiles = txnLogPath.listFiles().filter(_.getName.endsWith(".json")).sorted
        println(s"Transaction log files: ${txnFiles.map(_.getName).mkString(", ")}")

        // Read version 2 file to see what's in it
        val v2File = txnFiles.find(_.getName.startsWith("00000000000000000002"))
        v2File.foreach { file =>
          val content = scala.io.Source.fromFile(file).getLines().toList
          println(s"Version 2 content (${content.size} lines):")
          content.foreach(line => println(s"  $line"))
        }
      }

      // Verify only overwrite files remain
      val files = txnLog.listFiles()

      // Debug output
      println(s"Initial files were: file1.split, file2.split")
      println(s"Files after overwrite: ${files.map(_.path)}")
      println(s"Number of files: ${files.size}")
      println(s"Expected: 1 file (file3.split)")

      // The calculation should be: initial 2 files removed, 1 file added = 1 file total
      assert(files.size == 1, s"Expected 1 file but got ${files.size}: ${files.map(_.path)}")
      assert(files.head.path == "file3.split")
    } finally
      txnLog.close()
  }

  test("TransactionLogFactory should create optimized log by default") {
    val options = new CaseInsensitiveStringMap(
      Map(
        "spark.indextables.transaction.optimized.enabled" -> "true"
      ).asJava
    )

    val tablePath = new Path(testDir, "test_table")
    val txnLog    = TransactionLogFactory.create(tablePath, spark, options)

    try {
      // Should create TransactionLogAdapter wrapping OptimizedTransactionLog
      assert(txnLog.isInstanceOf[TransactionLogAdapter])

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, false)
        )
      )

      txnLog.initialize(schema)

      // Test basic functionality
      val addAction = AddAction(
        path = "file1.split",
        partitionValues = Map.empty,
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )

      val version = txnLog.addFile(addAction)
      assert(version >= 0)

      val files = txnLog.listFiles()
      assert(files.size == 1)
    } finally
      txnLog.close()
  }

  test("TransactionLogFactory should create standard log when optimized is disabled") {
    val options = new CaseInsensitiveStringMap(
      Map(
        "spark.indextables.transaction.optimized.enabled" -> "false"
      ).asJava
    )

    val tablePath = new Path(testDir, "test_table")
    val txnLog    = TransactionLogFactory.create(tablePath, spark, options)

    try {
      // Should create standard TransactionLog
      assert(txnLog.isInstanceOf[TransactionLog])
      assert(!txnLog.isInstanceOf[TransactionLogAdapter])
    } finally
      txnLog.close()
  }
}
