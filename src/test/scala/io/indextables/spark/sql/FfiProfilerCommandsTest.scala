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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import io.indextables.tantivy4java.split.FfiProfiler
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/** Tests for FFI Profiler SQL commands. */
class FfiProfilerCommandsTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .appName("FfiProfilerCommandsTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    // Ensure profiler is disabled after tests
    try FfiProfiler.disable()
    catch { case _: Exception => }

    if (spark != null) {
      spark.stop()
      spark = null
    }
    super.afterAll()
  }

  // ===== ENABLE tests =====

  test("ENABLE INDEXTABLES PROFILER should parse and execute") {
    val result = spark.sql("ENABLE INDEXTABLES PROFILER")

    result.columns should contain("status")
    result.columns should contain("host_count")
    result.columns should contain("message")

    val rows = result.collect()
    rows should have length 1
    rows(0).getString(0) shouldBe "enabled"
    FfiProfiler.isEnabled shouldBe true
  }

  test("ENABLE TANTIVY4SPARK PROFILER should work as alias") {
    val result = spark.sql("ENABLE TANTIVY4SPARK PROFILER")
    val rows = result.collect()
    rows(0).getString(0) shouldBe "enabled"
  }

  test("enable indextables profiler should be case insensitive") {
    val result = spark.sql("enable indextables profiler")
    val rows = result.collect()
    rows(0).getString(0) shouldBe "enabled"
  }

  // ===== DISABLE tests =====

  test("DISABLE INDEXTABLES PROFILER should parse and execute") {
    FfiProfiler.enable()
    val result = spark.sql("DISABLE INDEXTABLES PROFILER")

    val rows = result.collect()
    rows should have length 1
    rows(0).getString(0) shouldBe "disabled"
    FfiProfiler.isEnabled shouldBe false
  }

  test("DISABLE TANTIVY4SPARK PROFILER should work as alias") {
    FfiProfiler.enable()
    val result = spark.sql("DISABLE TANTIVY4SPARK PROFILER")
    val rows = result.collect()
    rows(0).getString(0) shouldBe "disabled"
  }

  // ===== DESCRIBE PROFILER tests =====

  test("DESCRIBE INDEXTABLES PROFILER should return section schema") {
    val result = spark.sql("DESCRIBE INDEXTABLES PROFILER")
    val columns = result.columns.toSeq
    columns shouldBe Seq("section", "category", "calls", "total_ms", "avg_us", "min_us", "max_us")
  }

  test("DESCRIBE INDEXTABLES PROFILER should return empty when profiler never enabled") {
    FfiProfiler.disable()
    FfiProfiler.reset() // clear any leftover counters
    val result = spark.sql("DESCRIBE INDEXTABLES PROFILER")
    result.count() shouldBe 0
  }

  test("DESCRIBE INDEXTABLES PROFILER schema types should be correct") {
    val result = spark.sql("DESCRIBE INDEXTABLES PROFILER")
    val schema = result.schema
    schema("section").dataType.typeName shouldBe "string"
    schema("category").dataType.typeName shouldBe "string"
    schema("calls").dataType.typeName shouldBe "long"
    schema("total_ms").dataType.typeName shouldBe "double"
    schema("avg_us").dataType.typeName shouldBe "double"
    schema("min_us").dataType.typeName shouldBe "double"
    schema("max_us").dataType.typeName shouldBe "double"
  }

  // ===== DESCRIBE PROFILER CACHE tests =====

  test("DESCRIBE INDEXTABLES PROFILER CACHE should return cache schema") {
    val result = spark.sql("DESCRIBE INDEXTABLES PROFILER CACHE")
    val columns = result.columns.toSeq
    columns shouldBe Seq("cache", "hits", "misses", "hit_rate")
  }

  test("DESCRIBE INDEXTABLES PROFILER CACHE schema types should be correct") {
    val result = spark.sql("DESCRIBE INDEXTABLES PROFILER CACHE")
    val schema = result.schema
    schema("cache").dataType.typeName shouldBe "string"
    schema("hits").dataType.typeName shouldBe "long"
    schema("misses").dataType.typeName shouldBe "long"
    schema("hit_rate").dataType.typeName shouldBe "double"
  }

  test("DESCRIBE TANTIVY4SPARK PROFILER CACHE should work as alias") {
    val result = spark.sql("DESCRIBE TANTIVY4SPARK PROFILER CACHE")
    result.columns.toSeq shouldBe Seq("cache", "hits", "misses", "hit_rate")
  }

  // ===== RESET PROFILER tests =====

  test("RESET INDEXTABLES PROFILER should return section schema") {
    val result = spark.sql("RESET INDEXTABLES PROFILER")
    val columns = result.columns.toSeq
    columns shouldBe Seq("section", "category", "calls", "total_ms", "avg_us", "min_us", "max_us")
  }

  test("RESET INDEXTABLES PROFILER should zero counters") {
    FfiProfiler.enable()
    // Reset to clear
    spark.sql("RESET INDEXTABLES PROFILER").collect()
    // Describe should now be empty (or have only zeros which are filtered)
    val result = spark.sql("DESCRIBE INDEXTABLES PROFILER")
    result.count() shouldBe 0
  }

  test("RESET INDEXTABLES PROFILER CACHE should return cache schema") {
    val result = spark.sql("RESET INDEXTABLES PROFILER CACHE")
    val columns = result.columns.toSeq
    columns shouldBe Seq("cache", "hits", "misses", "hit_rate")
  }

  test("RESET TANTIVY4SPARK PROFILER should work as alias") {
    val result = spark.sql("RESET TANTIVY4SPARK PROFILER")
    result.columns.toSeq shouldBe Seq("section", "category", "calls", "total_ms", "avg_us", "min_us", "max_us")
  }

  // ===== Result queryability tests =====

  test("DESCRIBE INDEXTABLES PROFILER result should be queryable as temp view") {
    spark.sql("ENABLE INDEXTABLES PROFILER")
    val result = spark.sql("DESCRIBE INDEXTABLES PROFILER")
    result.createOrReplaceTempView("profiler_sections")

    val searchSections = spark.sql("SELECT * FROM profiler_sections WHERE category = 'search'")
    searchSections.count() should be >= 0L

    spark.sql("DISABLE INDEXTABLES PROFILER")
  }

  test("DESCRIBE INDEXTABLES PROFILER CACHE result should be queryable as temp view") {
    val result = spark.sql("DESCRIBE INDEXTABLES PROFILER CACHE")
    result.createOrReplaceTempView("profiler_cache")

    val cacheRows = spark.sql("SELECT * FROM profiler_cache WHERE hit_rate > 0.5")
    cacheRows.count() should be >= 0L
  }

  // ===== Integration test with real data =====

  test("profiler should capture non-zero section counters after index read") {
    val tempPath = Files.createTempDirectory("ffi-profiler-test").toString
    try {
      val schema = StructType(Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false)
      ))
      val data = Seq(Row(1, "Alice"), Row(2, "Bob"), Row(3, "Charlie"))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      val format = "io.indextables.spark.core.IndexTables4SparkTableProvider"
      df.write.format(format).mode("overwrite").save(tempPath)

      // Enable profiler, reset to get a clean baseline
      spark.sql("ENABLE INDEXTABLES PROFILER")
      spark.sql("RESET INDEXTABLES PROFILER").collect()

      // Read the index — this exercises the native read path
      val readDf = spark.read.format(format).load(tempPath)
      readDf.collect() // force execution

      // Check that profiler captured something
      val result = spark.sql("DESCRIBE INDEXTABLES PROFILER")
      val rows = result.collect()
      rows.length should be > 0

      // At least one section should have non-zero calls
      val totalCalls = rows.map(_.getLong(2)).sum
      totalCalls should be > 0L

      // Verify reset clears them
      spark.sql("RESET INDEXTABLES PROFILER").collect()
      val afterReset = spark.sql("DESCRIBE INDEXTABLES PROFILER").collect()
      afterReset.length shouldBe 0

      spark.sql("DISABLE INDEXTABLES PROFILER")
    } finally {
      // Clean up temp directory
      try {
        val dir = new java.io.File(tempPath)
        if (dir.exists()) {
          dir.listFiles().foreach(_.delete())
          dir.delete()
        }
      } catch { case _: Exception => }
    }
  }

  // ===== SQL passthrough test =====

  test("non-profiler SQL should still pass through") {
    val result = spark.sql("SELECT 1 AS x")
    result.collect()(0).getInt(0) shouldBe 1
  }
}
