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

package io.indextables.spark.sync

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests for DistributedSourceScanner — verifies that distributed Delta and Parquet scans
 * produce the same file set as the existing single-call readers.
 */
class DistributedSourceScannerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("DistributedSourceScannerTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(
        "spark.sql.extensions",
        "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
          "io.delta.sql.DeltaSparkSessionExtension"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.indextables.aws.accessKey", "test-access-key")
      .config("spark.indextables.aws.secretKey", "test-secret-key")
      .config("spark.indextables.aws.region", "us-east-1")
      .config("spark.indextables.s3.endpoint", "http://localhost:10101")
      .config("spark.indextables.s3.pathStyleAccess", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("scanner-test").toString
    try
      f(path)
    finally
      deleteRecursively(new File(path))
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  private val emptyCredentials: Map[String, String] = Map.empty

  // ─── Delta Tests ───

  test("distributed Delta scan should produce same file count as DeltaLogReader.getAllFiles()") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_table").getAbsolutePath
      createDeltaTable(deltaPath, numFiles = 3, rowsPerFile = 10)

      // Single-call path
      val reader      = new DeltaLogReader(deltaPath, emptyCredentials)
      val singleFiles = reader.getAllFiles()

      // Distributed path
      val scanner    = new DistributedSourceScanner(spark)
      val distResult = scanner.scanDeltaTable(deltaPath, emptyCredentials)
      val distFiles  = distResult.filesRDD.collect().toSeq

      distFiles.size shouldBe singleFiles.size
    }
  }

  test("distributed Delta scan should return correct file paths") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_paths").getAbsolutePath
      createDeltaTable(deltaPath, numFiles = 2, rowsPerFile = 5)

      val reader         = new DeltaLogReader(deltaPath, emptyCredentials)
      val singlePaths    = reader.getAllFiles().map(_.path).toSet

      val scanner        = new DistributedSourceScanner(spark)
      val distResult     = scanner.scanDeltaTable(deltaPath, emptyCredentials)
      val distPaths      = distResult.filesRDD.map(_.path).collect().toSet

      distPaths shouldBe singlePaths
    }
  }

  test("distributed Delta scan should return correct version") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_version").getAbsolutePath
      createDeltaTable(deltaPath, numFiles = 1, rowsPerFile = 5)

      val reader      = new DeltaLogReader(deltaPath, emptyCredentials)
      val readerVersion = reader.currentVersion()

      val scanner    = new DistributedSourceScanner(spark)
      val distResult = scanner.scanDeltaTable(deltaPath, emptyCredentials)

      distResult.version shouldBe defined
      distResult.version.get shouldBe readerVersion
    }
  }

  test("distributed Delta scan should return correct partition columns") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_partitioned").getAbsolutePath
      createPartitionedDeltaTable(deltaPath)

      val reader      = new DeltaLogReader(deltaPath, emptyCredentials)
      val readerParts = reader.partitionColumns()

      val scanner    = new DistributedSourceScanner(spark)
      val distResult = scanner.scanDeltaTable(deltaPath, emptyCredentials)

      distResult.partitionColumns.sorted shouldBe readerParts.sorted
    }
  }

  test("distributed Delta scan should preserve partition values") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_partvals").getAbsolutePath
      createPartitionedDeltaTable(deltaPath)

      val reader      = new DeltaLogReader(deltaPath, emptyCredentials)
      val singleFiles = reader.getAllFiles()

      val scanner   = new DistributedSourceScanner(spark)
      val distResult = scanner.scanDeltaTable(deltaPath, emptyCredentials)
      val distFiles  = distResult.filesRDD.collect().toSeq

      // Both should have partition values
      singleFiles.foreach(_.partitionValues should not be empty)
      distFiles.foreach(_.partitionValues should not be empty)

      // Same set of partition value maps
      singleFiles.map(_.partitionValues).toSet shouldBe distFiles.map(_.partitionValues).toSet
    }
  }

  test("distributed Delta scan should preserve file sizes") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_sizes").getAbsolutePath
      createDeltaTable(deltaPath, numFiles = 2, rowsPerFile = 10)

      val reader      = new DeltaLogReader(deltaPath, emptyCredentials)
      val singleSizes = reader.getAllFiles().map(f => f.path -> f.size).toMap

      val scanner    = new DistributedSourceScanner(spark)
      val distResult = scanner.scanDeltaTable(deltaPath, emptyCredentials)
      val distSizes  = distResult.filesRDD.map(f => f.path -> f.size).collectAsMap().toMap

      distSizes shouldBe singleSizes
    }
  }

  // ─── Parquet Tests ───

  test("distributed Parquet scan should produce same file count as ParquetDirectoryReader.getAllFiles()") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_data").getAbsolutePath
      createParquetData(parquetPath, numFiles = 3)

      val reader      = new ParquetDirectoryReader(parquetPath, emptyCredentials)
      val singleFiles = reader.getAllFiles()

      val scanner    = new DistributedSourceScanner(spark)
      val distResult = scanner.scanParquetDirectory(parquetPath, emptyCredentials)
      val distFiles  = distResult.filesRDD.collect().toSeq

      distFiles.size shouldBe singleFiles.size
    }
  }

  test("distributed Parquet scan of partitioned directory should detect partition columns") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_partitioned").getAbsolutePath
      createPartitionedParquetData(parquetPath)

      val scanner    = new DistributedSourceScanner(spark)
      val distResult = scanner.scanParquetDirectory(parquetPath, emptyCredentials)

      distResult.partitionColumns should contain("region")
    }
  }

  // ─── PartitionFilter Backward Compatibility ───

  test("distributed Delta scan with None PartitionFilter returns same file count") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_nofilter").getAbsolutePath
      createDeltaTable(deltaPath, numFiles = 2, rowsPerFile = 10)

      val scanner      = new DistributedSourceScanner(spark)
      val resultNoFilter = scanner.scanDeltaTable(deltaPath, emptyCredentials, partitionFilter = None)
      val filesNoFilter  = resultNoFilter.filesRDD.collect().toSeq

      val resultDefault  = scanner.scanDeltaTable(deltaPath, emptyCredentials)
      val filesDefault   = resultDefault.filesRDD.collect().toSeq

      filesNoFilter.size shouldBe filesDefault.size
      filesNoFilter.map(_.path).toSet shouldBe filesDefault.map(_.path).toSet
    }
  }

  test("distributed Parquet scan with None PartitionFilter returns same file count") {
    withTempPath { tempDir =>
      val parquetPath = new File(tempDir, "parquet_nofilter").getAbsolutePath
      createParquetData(parquetPath, numFiles = 3)

      val scanner        = new DistributedSourceScanner(spark)
      val resultNoFilter = scanner.scanParquetDirectory(parquetPath, emptyCredentials, partitionFilter = None)
      val resultDefault  = scanner.scanParquetDirectory(parquetPath, emptyCredentials)

      resultNoFilter.filesRDD.collect().size shouldBe resultDefault.filesRDD.collect().size
    }
  }

  // ─── Fallback Tests ───

  test("distributed Delta scan should throw on non-existent path") {
    val scanner = new DistributedSourceScanner(spark)
    intercept[Exception] {
      scanner.scanDeltaTable("/nonexistent/delta/table", emptyCredentials)
    }
  }

  test("distributed Parquet scan on non-existent path should return empty result") {
    val scanner = new DistributedSourceScanner(spark)
    val result  = scanner.scanParquetDirectory("/nonexistent/parquet/data", emptyCredentials)
    result.filesRDD.collect() shouldBe empty
  }

  // ─── Helpers ───

  private def createDeltaTable(path: String, numFiles: Int, rowsPerFile: Int): Unit = {
    val ss = spark
    import ss.implicits._
    val data = (0 until numFiles * rowsPerFile).map(i => (i.toLong, s"name_$i", i * 1.5, i % 2 == 0))
    data
      .toDF("id", "name", "score", "active")
      .repartition(numFiles)
      .write
      .format("delta")
      .save(path)
    // Force a checkpoint so distributed scan (which requires _last_checkpoint) can work
    io.delta.tables.DeltaTable.forPath(spark, path).toDF
    val deltaLog = org.apache.spark.sql.delta.DeltaLog.forTable(spark, path)
    deltaLog.checkpoint()
  }

  private def createPartitionedDeltaTable(path: String): Unit = {
    val ss = spark
    import ss.implicits._
    val data = Seq(
      (1L, "alice", "east"),
      (2L, "bob", "west"),
      (3L, "carol", "east"),
      (4L, "dave", "west")
    )
    data
      .toDF("id", "name", "region")
      .write
      .format("delta")
      .partitionBy("region")
      .save(path)
    // Force a checkpoint so distributed scan (which requires _last_checkpoint) can work
    val deltaLog = org.apache.spark.sql.delta.DeltaLog.forTable(spark, path)
    deltaLog.checkpoint()
  }

  private def createParquetData(path: String, numFiles: Int): Unit = {
    val ss = spark
    import ss.implicits._
    val data = (0 until numFiles * 10).map(i => (i.toLong, s"name_$i", i * 1.5))
    data
      .toDF("id", "name", "score")
      .repartition(numFiles)
      .write
      .parquet(path)
  }

  private def createPartitionedParquetData(path: String): Unit = {
    val ss = spark
    import ss.implicits._
    val data = Seq(
      (1L, "alice", "east"),
      (2L, "bob", "west"),
      (3L, "carol", "east")
    )
    data
      .toDF("id", "name", "region")
      .write
      .partitionBy("region")
      .parquet(path)
  }
}
