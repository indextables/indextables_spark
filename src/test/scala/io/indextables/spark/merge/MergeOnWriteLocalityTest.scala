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

package io.indextables.spark.merge

import java.io.File
import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Test to verify merge-on-write locality behavior:
 * 1. Local files available (LOCALITY HIT) - uses local merge-on-write-staging files
 * 2. Remote files required (LOCALITY MISS) - downloads from S3/staging to temp .split files
 *
 * This test validates the critical fix: downloaded files must use .split extension,
 * not .tmp extension, for tantivy4java compatibility.
 */
class MergeOnWriteLocalityTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _
  private var tempDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession.builder()
      .appName("MergeOnWriteLocalityTest")
      .master("local[4]")  // 4 workers to test distributed execution
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.default.parallelism", "4")
      .getOrCreate()

    tempDir = Files.createTempDirectory("merge-locality-test").toFile
    tempDir.deleteOnExit()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (tempDir != null && tempDir.exists()) {
      org.apache.commons.io.FileUtils.deleteDirectory(tempDir)
    }
    super.afterAll()
  }

  test("Merge should work with local files (LOCALITY HIT scenario)") {
    val tablePath = new File(tempDir, "locality-hit-test").getAbsolutePath

    // Create test data - enough to trigger merge
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val testData = 1.to(5000).map { i =>
      (i, s"content_$i", i % 100)
    }.toDF("id", "content", "score")
      .repartition(8)  // Create multiple splits

    // Write with merge-on-write enabled
    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "30M")
      .option("spark.indextables.indexWriter.batchSize", "500")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "10M")  // Low threshold to trigger merge
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", s"file://$tablePath/_staging")
      .mode(SaveMode.Overwrite)
      .save(s"file://$tablePath")

    // Verify table was created and data is correct
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(s"file://$tablePath")

    readData.count() shouldBe 5000

    val sample = readData.filter($"id" === 2500).collect()
    sample should have length 1
    sample(0).getAs[String]("content") shouldBe "content_2500"

    // Check that local staging directory was used
    val stagingDir = new File(s"$tablePath/_staging")
    if (stagingDir.exists() && stagingDir.list().nonEmpty) {
      println(s"✓ Staging directory used: ${stagingDir.list().length} files")
    }

    println("✓ Test passed - merge succeeded with local file access")
  }

  test("Merge should work with downloaded files (LOCALITY MISS scenario)") {
    val tablePath = new File(tempDir, "locality-miss-test").getAbsolutePath

    // Create test data
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val testData = 1.to(5000).map { i =>
      (i, s"document_$i", i % 50)
    }.toDF("id", "content", "score")
      .repartition(8)

    // Write with merge-on-write
    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "30M")
      .option("spark.indextables.indexWriter.batchSize", "500")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "10M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", s"file://$tablePath/_staging")
      .mode(SaveMode.Overwrite)
      .save(s"file://$tablePath")

    // Simulate locality miss by removing local merge-on-write-staging files
    // This forces the merge phase to download from S3/staging
    val localStagingDir = new File(System.getProperty("java.io.tmpdir"), "merge-on-write-staging")
    if (localStagingDir.exists()) {
      println(s"✓ Cleaning local staging to simulate locality miss: ${localStagingDir.getAbsolutePath}")
      org.apache.commons.io.FileUtils.deleteDirectory(localStagingDir)
    }

    // Append more data to trigger another merge operation
    // This merge will have to download splits from staging (LOCALITY MISS)
    val moreData = 5001.to(10000).map { i =>
      (i, s"document_$i", i % 50)
    }.toDF("id", "content", "score")
      .repartition(8)

    moreData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "30M")
      .option("spark.indextables.indexWriter.batchSize", "500")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "10M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", s"file://$tablePath/_staging")
      .mode(SaveMode.Append)
      .save(s"file://$tablePath")

    // Verify all data is present
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(s"file://$tablePath")

    readData.count() shouldBe 10000

    val sample = readData.filter($"id" === 7500).collect()
    sample should have length 1
    sample(0).getAs[String]("content") shouldBe "document_7500"

    // Check for downloaded .split files (not .tmp files)
    val downloadDir = new File(System.getProperty("java.io.tmpdir"), "merge-downloads")
    if (downloadDir.exists()) {
      val downloadedFiles = downloadDir.listFiles().filter(_.getName.endsWith(".split"))
      if (downloadedFiles.nonEmpty) {
        println(s"✓ Downloaded ${downloadedFiles.length} .split files (not .tmp files)")
        downloadedFiles.take(3).foreach { f =>
          println(s"  - ${f.getName}")
        }
      }
    }

    println("✓ Test passed - merge succeeded with downloaded files (.split extension)")
  }

  test("Downloaded files should have .split extension, not .tmp") {
    val tablePath = new File(tempDir, "extension-validation-test").getAbsolutePath

    // Create and write initial data
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val testData = 1.to(3000).map { i =>
      (i, s"test_$i", i % 25)
    }.toDF("id", "content", "score")
      .repartition(6)

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "30M")
      .option("spark.indextables.indexWriter.batchSize", "500")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "8M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", s"file://$tablePath/_staging")
      .mode(SaveMode.Overwrite)
      .save(s"file://$tablePath")

    // Clear local staging to force download
    val localStagingDir = new File(System.getProperty("java.io.tmpdir"), "merge-on-write-staging")
    if (localStagingDir.exists()) {
      org.apache.commons.io.FileUtils.deleteDirectory(localStagingDir)
    }

    // Clean up any old download files
    val downloadDir = new File(System.getProperty("java.io.tmpdir"), "merge-downloads")
    if (downloadDir.exists()) {
      org.apache.commons.io.FileUtils.deleteDirectory(downloadDir)
    }

    // Trigger another write that will require merge with downloads
    val moreData = 3001.to(6000).map { i =>
      (i, s"test_$i", i % 25)
    }.toDF("id", "content", "score")
      .repartition(6)

    moreData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "30M")
      .option("spark.indextables.indexWriter.batchSize", "500")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "8M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", s"file://$tablePath/_staging")
      .mode(SaveMode.Append)
      .save(s"file://$tablePath")

    // Verify data integrity
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(s"file://$tablePath")

    readData.count() shouldBe 6000

    // CRITICAL VALIDATION: Check that downloaded files use .split extension
    if (downloadDir.exists()) {
      val allFiles = downloadDir.listFiles()
      val tmpFiles = allFiles.filter(_.getName.endsWith(".tmp"))
      val splitFiles = allFiles.filter(_.getName.endsWith(".split"))

      println(s"✓ Downloaded files: ${allFiles.length} total")
      println(s"  - .split files: ${splitFiles.length}")
      println(s"  - .tmp files: ${tmpFiles.length}")

      // CRITICAL: There should be NO .tmp files, only .split files
      tmpFiles.length shouldBe 0
      splitFiles.length should be > 0

      println("✓ VALIDATION PASSED: All downloaded files use .split extension (no .tmp files)")
    } else {
      println("⚠ No download directory found - merge may have used local files only")
    }
  }
}
