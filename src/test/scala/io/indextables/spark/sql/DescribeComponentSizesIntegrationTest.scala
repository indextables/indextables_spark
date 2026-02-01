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

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Integration tests for DESCRIBE INDEXTABLES COMPONENT SIZES command.
 *
 * Tests actual execution against real IndexTables tables.
 */
class DescribeComponentSizesIntegrationTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession         = _
  private var testDir: java.nio.file.Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .appName("DescribeComponentSizesIntegrationTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    testDir = Files.createTempDirectory("component_sizes_test_")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    if (testDir != null) {
      deleteDirectory(testDir.toFile)
    }
    super.afterAll()
  }

  private def deleteDirectory(dir: java.io.File): Unit =
    if (dir.exists()) {
      dir.listFiles().foreach { file =>
        if (file.isDirectory) deleteDirectory(file)
        else file.delete()
      }
      dir.delete()
    }

  // ===== Schema Tests =====

  test("DESCRIBE COMPONENT SIZES should have correct output schema") {
    val tablePath = createTestTable("schema_test")

    val result = spark.sql(s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath'")

    // Verify schema
    val columns = result.columns.toSet
    columns should contain("split_path")
    columns should contain("partition_values")
    columns should contain("component_key")
    columns should contain("size_bytes")
    columns should contain("component_type")
    columns should contain("field_name")

    // Verify column types
    val schema = result.schema
    schema("split_path").dataType.typeName shouldBe "string"
    schema("partition_values").dataType.typeName shouldBe "string"
    schema("component_key").dataType.typeName shouldBe "string"
    schema("size_bytes").dataType.typeName shouldBe "long"
    schema("component_type").dataType.typeName shouldBe "string"
    schema("field_name").dataType.typeName shouldBe "string"
  }

  // ===== Basic Execution Tests =====

  test("DESCRIBE COMPONENT SIZES should return component sizes for single split") {
    val tablePath = createTestTable("single_split")

    val result = spark.sql(s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath'")
    val rows   = result.collect()

    // Should return at least some component sizes
    rows.length should be >= 1

    // All rows should have valid split_path
    rows.foreach { row =>
      row.getString(0) should not be null // split_path
      row.getString(2) should not be null // component_key
      row.getLong(3) should be >= 0L      // size_bytes
      row.getString(4) should not be null // component_type
    }
  }

  test("DESCRIBE COMPONENT SIZES should return fastfield components") {
    val tablePath = createTestTableWithFastField("fastfield_test")

    val result = spark.sql(s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath'")
    result.createOrReplaceTempView("components_fastfield")

    val fastfieldComponents = spark
      .sql(
        "SELECT * FROM components_fastfield WHERE component_type = 'fastfield'"
      )
      .collect()

    // Should have at least one fastfield component (for the configured fast field)
    fastfieldComponents.length should be >= 1

    // Fastfield components should have field_name set
    fastfieldComponents.foreach { row =>
      row.getString(5) should not be null // field_name
    }
  }

  test("DESCRIBE COMPONENT SIZES should return segment-level components with null field_name") {
    val tablePath = createTestTable("segment_level")

    val result = spark.sql(s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath'")
    result.createOrReplaceTempView("components_segment")

    // Look for segment-level components (those starting with _)
    val segmentComponents = spark
      .sql(
        "SELECT * FROM components_segment WHERE component_key LIKE '\\_%'"
      )
      .collect()

    // Segment-level components should exist
    segmentComponents.length should be >= 1

    // Segment-level components should have null field_name
    segmentComponents.foreach { row =>
      row.isNullAt(5) shouldBe true // field_name should be null
    }
  }

  // ===== Component Type Tests =====

  test("DESCRIBE COMPONENT SIZES should return correct component types") {
    val tablePath = createTestTableWithFastField("component_types")

    val result = spark.sql(s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath'")
    result.createOrReplaceTempView("components_types")

    val componentTypes = spark
      .sql(
        "SELECT DISTINCT component_type FROM components_types"
      )
      .collect()
      .map(_.getString(0))
      .toSet

    // Should have at least some recognized component types
    val knownTypes = Set("fastfield", "fieldnorm", "term", "postings", "positions", "store")
    (componentTypes & knownTypes).size should be >= 1
  }

  // ===== Partition Filtering Tests =====

  test("DESCRIBE COMPONENT SIZES with WHERE clause should filter by partition") {
    val tablePath = createPartitionedTable("partitioned_filter")

    // Get all components
    val allResult = spark.sql(s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath'")
    val allRows   = allResult.collect()

    // Get filtered components
    val filteredResult = spark.sql(
      s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath' WHERE year = '2024'"
    )
    val filteredRows = filteredResult.collect()

    // Filtered should have fewer or equal rows
    filteredRows.length should be <= allRows.length

    // All filtered rows should have partition_values containing 2024
    filteredRows.foreach { row =>
      val partitionValues = row.getString(1)
      if (partitionValues != null) {
        partitionValues should include("2024")
      }
    }
  }

  test("DESCRIBE COMPONENT SIZES should return empty for non-matching WHERE predicate") {
    val tablePath = createPartitionedTable("no_match")

    val result = spark.sql(
      s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath' WHERE year = '9999'"
    )
    val rows = result.collect()

    rows.length shouldBe 0
  }

  // ===== Unpartitioned Table Tests =====

  test("DESCRIBE COMPONENT SIZES should work with unpartitioned table") {
    val tablePath = createTestTable("unpartitioned")

    val result = spark.sql(s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath'")
    val rows   = result.collect()

    rows.length should be >= 1

    // Partition values should be null for unpartitioned table
    rows.foreach { row =>
      row.isNullAt(1) shouldBe true // partition_values should be null
    }
  }

  // ===== Queryability Tests =====

  test("DESCRIBE COMPONENT SIZES result should be queryable") {
    val tablePath = createTestTable("queryable")

    val result = spark.sql(s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath'")
    result.createOrReplaceTempView("components_query")

    // Should be able to aggregate
    val totalSize = spark.sql("SELECT SUM(size_bytes) as total FROM components_query").head().getLong(0)
    totalSize should be >= 0L

    // Should be able to filter
    val filteredCount = spark
      .sql(
        "SELECT COUNT(*) FROM components_query WHERE size_bytes > 0"
      )
      .head()
      .getLong(0)
    filteredCount should be >= 0L

    // Should be able to group
    val groupedResult = spark
      .sql(
        "SELECT component_type, COUNT(*) as cnt FROM components_query GROUP BY component_type"
      )
      .collect()
    groupedResult.length should be >= 1
  }

  // ===== Error Handling Tests =====

  test("DESCRIBE COMPONENT SIZES should handle non-existent table path gracefully") {
    val nonExistentPath = s"${testDir.toString}/does_not_exist"

    val ex = intercept[Exception] {
      spark.sql(s"DESCRIBE INDEXTABLES COMPONENT SIZES '$nonExistentPath'").collect()
    }
    // Should throw an exception for non-existent table
    ex should not be null
  }

  // ===== Helper Methods =====

  private def createTestTable(name: String): String = {
    val tablePath = s"${testDir.toString}/$name"

    val data = Seq(
      (1, "hello world", 10.5),
      (2, "test data", 20.3),
      (3, "sample text", 15.7)
    )
    val df = spark.createDataFrame(data).toDF("id", "content", "score")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(tablePath)

    tablePath
  }

  private def createTestTableWithFastField(name: String): String = {
    val tablePath = s"${testDir.toString}/$name"

    val data = Seq(
      (1, "hello world", 10.5),
      (2, "test data", 20.3),
      (3, "sample text", 15.7)
    )
    val df = spark.createDataFrame(data).toDF("id", "content", "score")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(tablePath)

    tablePath
  }

  private def createPartitionedTable(name: String): String = {
    val tablePath = s"${testDir.toString}/$name"

    val data = Seq(
      (1, "hello", "2023", "us-east"),
      (2, "world", "2024", "us-east"),
      (3, "test", "2024", "us-west")
    )
    val df = spark.createDataFrame(data).toDF("id", "content", "year", "region")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("year", "region")
      .mode("overwrite")
      .save(tablePath)

    tablePath
  }
}
