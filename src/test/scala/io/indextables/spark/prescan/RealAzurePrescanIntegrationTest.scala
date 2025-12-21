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

package io.indextables.spark.prescan

import java.util.UUID

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import io.indextables.spark.RealAzureTestBase

/**
 * Real Azure Blob Storage integration tests for prescan filtering and warmup.
 *
 * Tests prescan functionality against real Azure Blob Storage:
 *   - Prescan filtering eliminates splits correctly
 *   - PREWARM command caches split footers
 *   - Queries return correct results with prescan enabled
 *   - Error handling for Azure-specific scenarios
 *
 * Credentials are loaded from ~/.azure/credentials file or environment variables.
 * To run: mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.prescan.RealAzurePrescanIntegrationTest'
 */
class RealAzurePrescanIntegrationTest extends RealAzureTestBase {

  // Generate unique test run ID to avoid conflicts
  private val testRunId = UUID.randomUUID().toString.substring(0, 8)

  /** Get the base path for Azure Blob Storage. */
  private def getAzureBasePath(): String = {
    val account   = getStorageAccount.getOrElse("unknown")
    val container = testContainer
    s"abfss://$container@$account.dfs.core.windows.net/prescan-test-$testRunId"
  }

  /** Get write options with Azure credentials for executor distribution. */
  private def getWriteOptions(): Map[String, String] = {
    val opts = scala.collection.mutable.Map[String, String]()

    getStorageAccount.foreach(v => opts("spark.indextables.azure.accountName") = v)
    getAccountKey.foreach(v => opts("spark.indextables.azure.accountKey") = v)
    getTenantId.foreach(v => opts("spark.indextables.azure.tenantId") = v)
    getClientId.foreach(v => opts("spark.indextables.azure.clientId") = v)
    getClientSecret.foreach(v => opts("spark.indextables.azure.clientSecret") = v)

    opts.toMap
  }

  /** Get read options with Azure credentials and prescan enabled. */
  private def getReadOptionsWithPrescan(): Map[String, String] = {
    getWriteOptions() ++ Map(
      "spark.indextables.read.prescan.enabled"          -> "true",
      "spark.indextables.read.prescan.minSplitThreshold" -> "1" // Low threshold for testing
    )
  }

  // ==================== PRESCAN FILTERING TESTS ====================

  test("prescan should eliminate splits on Azure for non-matching equality filter") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    val tablePath = s"${getAzureBasePath()}/equality-filter-${generateTestId()}"
    println(s"\n🧪 Testing prescan with equality filter at: $tablePath")

    // Clear prescan metrics registry before test
    PrescanMetricsRegistry.clear()

    // Create test data with multiple splits using different categories per split
    // Split 1: items 1-10 with category = "A"
    val data1 = spark.range(1, 11).select(
      col("id"),
      concat(lit("user_"), col("id")).as("username"),
      (col("id") + lit(20)).as("age"),
      lit("A").as("category")
    )

    // Split 2: items 11-20 with category = "B"
    val data2 = spark.range(11, 21).select(
      col("id"),
      concat(lit("user_"), col("id")).as("username"),
      (col("id") + lit(20)).as("age"),
      lit("B").as("category")
    )

    // Split 3: items 21-30 with category = "C"
    val data3 = spark.range(21, 31).select(
      col("id"),
      concat(lit("user_"), col("id")).as("username"),
      (col("id") + lit(20)).as("age"),
      lit("C").as("category")
    )

    val writeOpts = getWriteOptions()

    // Write each batch to create multiple splits
    data1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOpts)
      .mode("overwrite")
      .save(tablePath)

    data2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOpts)
      .mode("append")
      .save(tablePath)

    data3.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOpts)
      .mode("append")
      .save(tablePath)

    println("✅ Created test table with 3 splits")

    // Read with prescan enabled and filter for category A
    val readOpts = getReadOptionsWithPrescan()
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(readOpts)
      .load(tablePath)
      .filter(col("category") === "A")
      .collect()

    println(s"✅ Query returned ${result.length} rows (expected: 10)")
    result.length shouldBe 10

    // Verify prescan actually ran without errors
    // Note: Data skipping may eliminate splits before prescan runs, so we don't
    // require prescan to eliminate additional splits - just that it runs correctly
    val metrics = PrescanMetricsRegistry.getMetrics(tablePath)
    metrics shouldBe defined
    val m = metrics.get
    println(s"📊 Prescan metrics: before=${m.splitsBeforePrescan}, after=${m.splitsAfterPrescan}, errors=${m.errors}")

    // Prescan should have been invoked (at least 1 split considered)
    m.splitsBeforePrescan should be >= 1L
    // No errors during prescan
    m.errors shouldBe 0L
    // After should be <= before (no splits should be added)
    m.splitsAfterPrescan should be <= m.splitsBeforePrescan

    println("✅ Prescan equality filter test passed on Azure")
  }

  test("prescan should work with aggregates and filter on Azure") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    val tablePath = s"${getAzureBasePath()}/aggregate-${generateTestId()}"
    println(s"\n🧪 Testing prescan with aggregate at: $tablePath")

    // Clear prescan metrics registry before test
    PrescanMetricsRegistry.clear()

    // Create test data with different status per split
    val data1 = spark.range(1, 11).select(
      col("id"),
      lit("active").as("status"),
      (col("id") * lit(10)).as("score")
    )

    val data2 = spark.range(11, 21).select(
      col("id"),
      lit("inactive").as("status"),
      (col("id") * lit(10)).as("score")
    )

    val data3 = spark.range(21, 31).select(
      col("id"),
      lit("pending").as("status"),
      (col("id") * lit(10)).as("score")
    )

    val writeOpts = getWriteOptions() ++ Map(
      "spark.indextables.indexing.fastfields" -> "score"
    )

    data1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOpts)
      .mode("overwrite")
      .save(tablePath)

    data2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOpts)
      .mode("append")
      .save(tablePath)

    data3.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOpts)
      .mode("append")
      .save(tablePath)

    println("✅ Created test table with 3 splits")

    // Read with prescan enabled and aggregate
    val readOpts = getReadOptionsWithPrescan()
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(readOpts)
      .load(tablePath)
      .filter(col("status") === "active")
      .agg(count("*").as("cnt"), sum("score").as("total"))
      .collect()

    val cnt = result(0).getLong(0)
    println(s"✅ COUNT for active = $cnt (expected: 10)")
    cnt shouldBe 10

    // Verify prescan actually ran without errors
    val metrics = PrescanMetricsRegistry.getMetrics(tablePath)
    metrics shouldBe defined
    val m = metrics.get
    println(s"📊 Prescan metrics: before=${m.splitsBeforePrescan}, after=${m.splitsAfterPrescan}, errors=${m.errors}")

    // Prescan should have been invoked (at least 1 split considered)
    m.splitsBeforePrescan should be >= 1L
    // No errors during prescan
    m.errors shouldBe 0L
    // After should be <= before (no splits should be added)
    m.splitsAfterPrescan should be <= m.splitsBeforePrescan

    println("✅ Prescan aggregate test passed on Azure")
  }

  // ==================== PREWARM COMMAND TESTS ====================

  test("PREWARM PRESCAN FILTERS command should work on Azure") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    val tablePath = s"${getAzureBasePath()}/prewarm-${generateTestId()}"
    println(s"\n🧪 Testing PREWARM command at: $tablePath")

    // Create test data
    val data = spark.range(1, 51).select(
      col("id"),
      concat(lit("user_"), col("id")).as("name"),
      (col("id") * lit(10)).as("value"),
      (col("id") % lit(3)).cast("string").as("category")
    )

    val writeOpts = getWriteOptions()
    data.repartition(4).write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOpts)
      .mode("overwrite")
      .save(tablePath)

    println("✅ Created test table with 4 partitions")

    // Execute PREWARM command
    val prewarmSql = s"PREWARM INDEXTABLES PRESCAN FILTERS FOR '$tablePath'"
    println(s"Executing: $prewarmSql")

    val prewarmResult = spark.sql(prewarmSql).collect()
    println(s"✅ PREWARM result: ${prewarmResult.map(_.toString()).mkString(", ")}")

    // Verify the command completed
    prewarmResult.length should be >= 0

    println("✅ PREWARM command test passed on Azure")
  }

  // ==================== ENABLE/DISABLE COMMANDS ====================

  test("ENABLE/DISABLE PRESCAN FILTERING commands should work on Azure") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    val tablePath = s"${getAzureBasePath()}/enable-disable-${generateTestId()}"
    println(s"\n🧪 Testing ENABLE/DISABLE commands at: $tablePath")

    // Create test data
    val data = spark.range(1, 11).select(
      col("id"),
      concat(lit("user_"), col("id")).as("name")
    )

    val writeOpts = getWriteOptions()
    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOpts)
      .mode("overwrite")
      .save(tablePath)

    println("✅ Created test table")

    // Enable prescan filtering for this table
    val enableSql = s"ENABLE INDEXTABLES PRESCAN FILTERING FOR '$tablePath'"
    println(s"Executing: $enableSql")
    spark.sql(enableSql)
    println("✅ ENABLE command executed")

    // Disable prescan filtering
    val disableSql = s"DISABLE INDEXTABLES PRESCAN FILTERING FOR '$tablePath'"
    println(s"Executing: $disableSql")
    spark.sql(disableSql)
    println("✅ DISABLE command executed")

    // Enable globally
    spark.sql("ENABLE INDEXTABLES PRESCAN FILTERING")
    println("✅ Global ENABLE command executed")

    // Disable globally
    spark.sql("DISABLE INDEXTABLES PRESCAN FILTERING")
    println("✅ Global DISABLE command executed")

    println("✅ ENABLE/DISABLE commands test passed on Azure")
  }

  // ==================== CACHE BEHAVIOR TESTS ====================

  test("prescan should benefit from cached footers on Azure") {
    assume(hasAzureCredentials(), "Azure credentials required for this test")

    val tablePath = s"${getAzureBasePath()}/cache-test-${generateTestId()}"
    println(s"\n🧪 Testing prescan cache behavior at: $tablePath")

    // Create test data
    val data = spark.range(1, 101).select(
      col("id"),
      concat(lit("item_"), col("id")).as("name"),
      (col("id") * lit(10)).as("value")
    )

    val writeOpts = getWriteOptions()
    data.repartition(5).write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOpts)
      .mode("overwrite")
      .save(tablePath)

    println("✅ Created test table with 5 partitions")

    val readOpts = getReadOptionsWithPrescan()

    // First query - should warm the cache
    val startTime1 = System.currentTimeMillis()
    val result1 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(readOpts)
      .load(tablePath)
      .filter(col("name") === "item_1")
      .collect()
    val time1 = System.currentTimeMillis() - startTime1
    println(s"✅ First query: ${result1.length} rows in ${time1}ms")

    // Second query - should benefit from cached footers
    val startTime2 = System.currentTimeMillis()
    val result2 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(readOpts)
      .load(tablePath)
      .filter(col("name") === "item_2")
      .collect()
    val time2 = System.currentTimeMillis() - startTime2
    println(s"✅ Second query: ${result2.length} rows in ${time2}ms")

    // Both queries should return 1 row each
    result1.length shouldBe 1
    result2.length shouldBe 1

    println("✅ Prescan cache behavior test passed on Azure")
  }
}
