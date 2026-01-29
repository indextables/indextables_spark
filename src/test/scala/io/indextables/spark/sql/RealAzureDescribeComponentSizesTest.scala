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

import java.util.UUID

import io.indextables.spark.RealAzureTestBase

/**
 * Real Azure Blob Storage integration tests for DESCRIBE INDEXTABLES COMPONENT SIZES command.
 *
 * Tests critical functionality specific to Azure:
 *   - Component size enumeration from splits stored on Azure Blob Storage
 *   - Credential handling for cloud operations (account key or OAuth)
 *   - Partition filtering with WHERE clause
 *   - Fast field and segment-level component detection
 *
 * Credentials are loaded from multiple sources with priority:
 *   1. System properties: test.azure.storageAccount, test.azure.accountKey
 *   2. ~/.azure/credentials file
 *   3. Environment variables: AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY
 */
class RealAzureDescribeComponentSizesTest extends RealAzureTestBase {

  // Generate unique test run ID to avoid conflicts
  private val testRunId = UUID.randomUUID().toString.substring(0, 8)

  // Test base path will be determined once we have credentials
  private var testBasePath: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    if (hasAzureCredentials()) {
      val account = getStorageAccount.get
      testBasePath = s"abfss://$testContainer@$account.dfs.core.windows.net/describe-component-sizes-test-$testRunId"
      println(s"📍 Test base path: $testBasePath")
    } else {
      println(s"⚠️  No Azure credentials found - tests will be skipped")
    }
  }

  override def afterAll(): Unit = {
    // Clean up test data
    if (hasAzureCredentials() && testBasePath != null) {
      try {
        val hadoopConf = spark.sparkContext.hadoopConfiguration
        val basePath   = new org.apache.hadoop.fs.Path(testBasePath)
        val fs         = basePath.getFileSystem(hadoopConf)
        if (fs.exists(basePath)) {
          fs.delete(basePath, true)
          println(s"🗑️  Cleaned up test data at $testBasePath")
        }
      } catch {
        case ex: Exception =>
          println(s"⚠️  Failed to clean up test data: ${ex.getMessage}")
      }
    }
    super.afterAll()
  }

  test("DESCRIBE INDEXTABLES COMPONENT SIZES should return component sizes for table on Azure") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    val tablePath = s"$testBasePath/component_sizes_basic"

    // Create a table with data
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq(
      (1, "hello world", 10.5),
      (2, "test data", 20.3),
      (3, "sample text", 15.7)
    )
    val df = data.toDF("id", "content", "score")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(tablePath)

    println(s"✅ Created table at $tablePath")

    // Execute DESCRIBE COMPONENT SIZES
    val result = spark.sql(s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath'")
    val rows   = result.collect()

    println(s"📊 Retrieved ${rows.length} component size entries")

    // Verify schema
    val columns = result.columns.toSet
    columns should contain("split_path")
    columns should contain("partition_values")
    columns should contain("component_key")
    columns should contain("size_bytes")
    columns should contain("component_type")
    columns should contain("field_name")

    // Verify we have results
    rows.length should be >= 1

    // Verify all rows have valid data
    rows.foreach { row =>
      row.getString(0) should not be null // split_path
      row.getString(2) should not be null // component_key
      row.getLong(3) should be >= 0L      // size_bytes
      row.getString(4) should not be null // component_type
    }

    // Verify we have fastfield components
    result.createOrReplaceTempView("components")
    val fastfieldCount = spark.sql(
      "SELECT COUNT(*) FROM components WHERE component_type = 'fastfield'"
    ).head().getLong(0)

    println(s"📊 Found $fastfieldCount fastfield components")
    fastfieldCount should be >= 1L

    println(s"✅ DESCRIBE COMPONENT SIZES completed successfully on Azure")
  }

  test("DESCRIBE INDEXTABLES COMPONENT SIZES with partitioned table on Azure") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    val tablePath = s"$testBasePath/component_sizes_partitioned"

    // Create a partitioned table
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq(
      (1, "hello", "2023", "us-east"),
      (2, "world", "2024", "us-east"),
      (3, "test", "2024", "us-west"),
      (4, "data", "2024", "us-west")
    )
    val df = data.toDF("id", "content", "year", "region")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("year", "region")
      .mode("overwrite")
      .save(tablePath)

    println(s"✅ Created partitioned table at $tablePath")

    // Get all component sizes
    val allResult = spark.sql(s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath'")
    val allRows   = allResult.collect()

    println(s"📊 Total components across all partitions: ${allRows.length}")

    // Get filtered component sizes for year=2024
    val filteredResult = spark.sql(
      s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath' WHERE year = '2024'"
    )
    val filteredRows = filteredResult.collect()

    println(s"📊 Components for year=2024: ${filteredRows.length}")

    // Filtered should have fewer or equal rows than all
    filteredRows.length should be <= allRows.length

    // Verify partition_values contain 2024 for filtered results
    filteredRows.foreach { row =>
      val partitionValues = row.getString(1)
      if (partitionValues != null) {
        partitionValues should include("2024")
      }
    }

    println(s"✅ Partition filtering works correctly on Azure")
  }

  test("DESCRIBE INDEXTABLES COMPONENT SIZES result should be queryable on Azure") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    val tablePath = s"$testBasePath/component_sizes_queryable"

    // Create a table
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq(
      (1, "hello world", 100.0),
      (2, "test data", 200.0),
      (3, "sample text", 300.0)
    )
    val df = data.toDF("id", "content", "score")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(tablePath)

    println(s"✅ Created table at $tablePath")

    // Execute and create temp view
    val result = spark.sql(s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath'")
    result.createOrReplaceTempView("azure_components")

    // Test aggregation
    val totalSize = spark.sql("SELECT SUM(size_bytes) as total FROM azure_components").head().getLong(0)
    println(s"📊 Total size across all components: $totalSize bytes")
    totalSize should be > 0L

    // Test grouping by component type
    val typeBreakdown = spark.sql(
      "SELECT component_type, COUNT(*) as cnt, SUM(size_bytes) as total_bytes " +
        "FROM azure_components GROUP BY component_type ORDER BY total_bytes DESC"
    ).collect()

    println(s"📊 Component type breakdown:")
    typeBreakdown.foreach { row =>
      println(s"   - ${row.getString(0)}: ${row.getLong(1)} components, ${row.getLong(2)} bytes")
    }

    typeBreakdown.length should be >= 1

    println(s"✅ Query operations work correctly on Azure component sizes result")
  }

  test("DESCRIBE INDEXTABLES COMPONENT SIZES should handle empty WHERE result on Azure") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    val tablePath = s"$testBasePath/component_sizes_empty_filter"

    // Create a partitioned table
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq(
      (1, "hello", "2024"),
      (2, "world", "2024")
    )
    val df = data.toDF("id", "content", "year")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("year")
      .mode("overwrite")
      .save(tablePath)

    println(s"✅ Created table at $tablePath")

    // Query with non-matching filter
    val result = spark.sql(
      s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath' WHERE year = '9999'"
    )
    val rows = result.collect()

    rows.length shouldBe 0
    println(s"✅ Empty result returned correctly for non-matching filter on Azure")
  }
}
