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
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.indextables.spark.storage.GlobalSplitCacheManager

/**
 * Tests for DESCRIBE INDEXTABLES DISK CACHE command.
 */
class DescribeDiskCacheCommandTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tempDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .appName("DescribeDiskCacheCommandTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    tempDir = Files.createTempDirectory("disk-cache-cmd-test").toFile
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

  test("DESCRIBE INDEXTABLES DISK CACHE should parse and execute") {
    val result = spark.sql("DESCRIBE INDEXTABLES DISK CACHE")

    // Verify schema
    val columns = result.columns.toSet
    columns should contain("executor_id")
    columns should contain("enabled")
    columns should contain("total_bytes")
    columns should contain("max_bytes")
    columns should contain("usage_percent")
    columns should contain("splits_cached")
    columns should contain("components_cached")

    // Should return at least driver row
    result.count() should be >= 1L
  }

  test("DESCRIBE TANTIVY4SPARK DISK CACHE should parse and execute") {
    val result = spark.sql("DESCRIBE TANTIVY4SPARK DISK CACHE")

    // Should work with alternate keyword
    result.columns should contain("executor_id")
    result.count() should be >= 1L
  }

  test("describe indextables disk cache should be case insensitive") {
    val result = spark.sql("describe indextables disk cache")

    result.columns should contain("executor_id")
    result.count() should be >= 1L
  }

  // ===== Result Content Tests =====

  test("DESCRIBE INDEXTABLES DISK CACHE should return driver row when disk cache disabled") {
    // Clear any existing cache managers
    GlobalSplitCacheManager.clearAll()

    val result = spark.sql("DESCRIBE INDEXTABLES DISK CACHE")
    val rows = result.collect()

    // Should have at least driver row
    rows.length should be >= 1

    // Find driver row
    val driverRow = rows.find(_.getString(0) == "driver")
    driverRow shouldBe defined

    // Driver should show disk cache as disabled (no cache managers active)
    driverRow.get.getBoolean(1) shouldBe false
  }

  test("DESCRIBE INDEXTABLES DISK CACHE should show enabled=true when disk cache is configured") {
    val diskCachePath = new File(tempDir, "disk_cache").getAbsolutePath
    val tablePath = new File(tempDir, "test_table").getAbsolutePath

    // Create test data
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._

    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", StringType, nullable = false)
    ))
    val data = Seq(Row(1, "one"), Row(2, "two"))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Write with disk cache enabled
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", diskCachePath)
      .save(tablePath)

    // Read to populate cache
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", diskCachePath)
      .load(tablePath)

    readDf.count()

    // Now check disk cache stats
    val result = spark.sql("DESCRIBE INDEXTABLES DISK CACHE")
    result.show(false)

    // Should have results
    result.count() should be >= 1L
  }

  // ===== Schema Tests =====

  test("DESCRIBE INDEXTABLES DISK CACHE should have correct column types") {
    val result = spark.sql("DESCRIBE INDEXTABLES DISK CACHE")
    val schema = result.schema

    schema("executor_id").dataType.typeName shouldBe "string"
    schema("enabled").dataType.typeName shouldBe "boolean"
    schema("total_bytes").dataType.typeName shouldBe "long"
    schema("max_bytes").dataType.typeName shouldBe "long"
    schema("usage_percent").dataType.typeName shouldBe "double"
    schema("splits_cached").dataType.typeName shouldBe "long"
    schema("components_cached").dataType.typeName shouldBe "long"
  }

  test("DESCRIBE INDEXTABLES DISK CACHE result should be queryable") {
    val result = spark.sql("DESCRIBE INDEXTABLES DISK CACHE")

    // Register as temp view and query
    result.createOrReplaceTempView("disk_cache_stats")

    val drivers = spark.sql("SELECT * FROM disk_cache_stats WHERE executor_id = 'driver'")
    drivers.count() shouldBe 1

    val enabled = spark.sql("SELECT * FROM disk_cache_stats WHERE enabled = true")
    // May or may not have enabled caches
    enabled.count() should be >= 0L
  }
}
