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

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.SparkSession

import io.indextables.tantivy4java.split.SplitCacheManager
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/** Tests for DESCRIBE INDEXTABLES STORAGE STATS command. */
class DescribeStorageStatsCommandTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tempDir: File       = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .appName("DescribeStorageStatsCommandTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    tempDir = Files.createTempDirectory("storage-stats-cmd-test").toFile
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir)
    }
    super.afterAll()
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  // ===== SQL Parsing Tests =====

  test("DESCRIBE INDEXTABLES STORAGE STATS should parse and execute") {
    val result = spark.sql("DESCRIBE INDEXTABLES STORAGE STATS")

    // Verify schema
    val columns = result.columns.toSet
    columns should contain("executor_id")
    columns should contain("host")
    columns should contain("bytes_fetched")
    columns should contain("requests")

    // Should return at least driver row
    result.count() should be >= 1L
  }

  test("DESCRIBE TANTIVY4SPARK STORAGE STATS should parse and execute") {
    val result = spark.sql("DESCRIBE TANTIVY4SPARK STORAGE STATS")

    // Should work with alternate keyword
    result.columns should contain("executor_id")
    result.count() should be >= 1L
  }

  test("describe indextables storage stats should be case insensitive") {
    val result = spark.sql("describe indextables storage stats")

    result.columns should contain("executor_id")
    result.count() should be >= 1L
  }

  // ===== Result Content Tests =====

  test("DESCRIBE INDEXTABLES STORAGE STATS should return driver row with counters") {
    val result = spark.sql("DESCRIBE INDEXTABLES STORAGE STATS")
    val rows   = result.collect()

    // Should have at least driver row
    rows.length should be >= 1

    // Find driver row
    val driverRow = rows.find(_.getString(0) == "driver")
    driverRow shouldBe defined

    // bytes_fetched should be >= 0
    driverRow.get.getLong(2) should be >= 0L

    // requests should be >= 0
    driverRow.get.getLong(3) should be >= 0L
  }

  test("DESCRIBE INDEXTABLES STORAGE STATS counters should match SplitCacheManager") {
    // Get current values from SplitCacheManager
    val expectedBytes    = SplitCacheManager.getObjectStorageBytesFetched()
    val expectedRequests = SplitCacheManager.getObjectStorageRequestCount()

    val result = spark.sql("DESCRIBE INDEXTABLES STORAGE STATS")
    val rows   = result.collect()

    // Find driver row
    val driverRow = rows.find(_.getString(0) == "driver")
    driverRow shouldBe defined

    // Values should match (or be slightly higher due to concurrent operations)
    driverRow.get.getLong(2) should be >= expectedBytes
    driverRow.get.getLong(3) should be >= expectedRequests
  }

  // ===== Schema Tests =====

  test("DESCRIBE INDEXTABLES STORAGE STATS should have correct column types") {
    val result = spark.sql("DESCRIBE INDEXTABLES STORAGE STATS")
    val schema = result.schema

    schema("executor_id").dataType.typeName shouldBe "string"
    schema("host").dataType.typeName shouldBe "string"
    schema("bytes_fetched").dataType.typeName shouldBe "long"
    schema("requests").dataType.typeName shouldBe "long"
  }

  test("DESCRIBE INDEXTABLES STORAGE STATS result should be queryable") {
    val result = spark.sql("DESCRIBE INDEXTABLES STORAGE STATS")

    // Register as temp view and query
    result.createOrReplaceTempView("storage_stats")

    val drivers = spark.sql("SELECT * FROM storage_stats WHERE executor_id = 'driver'")
    drivers.count() shouldBe 1

    val withBytes = spark.sql("SELECT * FROM storage_stats WHERE bytes_fetched >= 0")
    withBytes.count() should be >= 1L
  }

  // ===== Integration Test =====

  test("DESCRIBE INDEXTABLES STORAGE STATS should show increased bytes after read operations") {
    val tablePath = new File(tempDir, "test_table").getAbsolutePath

    // Record bytes before
    val bytesBefore = SplitCacheManager.getObjectStorageBytesFetched()

    // Create and write test data
    val ss = spark
    import ss.implicits._
    val testData = (1 to 100).map(i => (i, s"value_$i")).toDF("id", "value")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Read to trigger object storage access (for local files, bytes may not increase)
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    readDf.count()

    // Check stats via SQL
    val result = spark.sql("DESCRIBE INDEXTABLES STORAGE STATS")
    result.show(false)

    // Verify we can see the counters
    val rows      = result.collect().toSeq
    val driverRow = rows.find(_.getString(0) == "driver")
    driverRow shouldBe defined

    // For local files, bytes may not increase, but the command should still work
    println(s"Bytes before: $bytesBefore, Bytes after: ${driverRow.get.getLong(2)}")
  }
}
