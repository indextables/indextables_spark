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

package io.indextables.spark.xref

import java.util.UUID

import org.apache.spark.sql.functions._

import io.indextables.spark.RealAzureTestBase

/**
 * Real Azure Blob Storage integration tests for XRef (Cross-Reference) indexing.
 *
 * These tests validate XRef functionality against real Azure Blob Storage:
 *   - XRef build with splits stored in Azure
 *   - XRef query routing with Azure-based indexes
 *   - Mixed XRef coverage (covered + uncovered splits)
 *   - Credential propagation to tantivy4java XRef API
 *
 * Prerequisites:
 *   - Azure credentials configured in ~/.azure/credentials or environment variables
 *   - Azure Blob Storage container accessible for testing
 *
 * Credentials are loaded from (in order of priority):
 *   1. System properties: -Dtest.azure.storageAccount=... -Dtest.azure.accountKey=...
 *   2. ~/.azure/credentials file with [default] section
 *   3. Environment variables: AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY
 *
 * Run with: mvn test -Dtest=RealAzureXRefIntegrationTest
 */
class RealAzureXRefIntegrationTest extends RealAzureTestBase {

  private val DataSourceFormat = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  // Generate unique test path to avoid conflicts between test runs
  private val testRunId = UUID.randomUUID().toString.substring(0, 8)

  private def getTestTablePath(suffix: String): String = {
    val account = getStorageAccount.getOrElse("unknown")
    s"abfss://$testContainer@$account.dfs.core.windows.net/xref-test-$testRunId/$suffix"
  }

  override def afterAll(): Unit = {
    // Clean up all test data
    if (hasAzureCredentials()) {
      cleanupTestData(getTestTablePath(""))
    }
    super.afterAll()
  }

  private def cleanupTestData(path: String): Unit = {
    try {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(path), hadoopConf)
      val hadoopPath = new org.apache.hadoop.fs.Path(path)

      if (fs.exists(hadoopPath)) {
        fs.delete(hadoopPath, true)
      }
    } catch {
      case _: Exception => // Best-effort cleanup
    }
  }

  /**
   * Configure Hadoop for Azure access using the credentials from RealAzureTestBase.
   */
  private def configureHadoopForAzure(): Unit = {
    val account = getStorageAccount.getOrElse {
      fail("Azure storage account not configured")
    }
    val key = getAccountKey.getOrElse {
      fail("Azure account key not configured")
    }

    // Configure Hadoop for ABFS access
    spark.sparkContext.hadoopConfiguration.set(
      s"fs.azure.account.key.$account.dfs.core.windows.net",
      key
    )
    spark.sparkContext.hadoopConfiguration.set(
      s"fs.azure.account.key.$account.blob.core.windows.net",
      key
    )

    // Configure IndexTables4Spark Azure settings
    spark.conf.set("spark.indextables.azure.accountName", account)
    spark.conf.set("spark.indextables.azure.accountKey", key)

    println(s"Configured Hadoop and IndexTables for Azure access: $account")
  }

  /**
   * Test XRef build and query on real Azure Blob Storage.
   *
   * This test verifies:
   * - XRef indexes can be built from splits stored in Azure
   * - XRef query routing works with Azure-based indexes
   * - Results are correct when querying through XRef
   */
  test("XRef build and query on Azure") {
    assume(hasAzureCredentials(), "Azure credentials required for Azure XRef test")

    configureHadoopForAzure()

    val tablePath = getTestTablePath("basic")
    val spark = this.spark
    import spark.implicits._

    println(s"Starting XRef Azure integration test at: $tablePath")

    try {
      // Configure for XRef testing
      spark.conf.set("spark.indextables.indexWriter.batchSize", "100")
      spark.conf.set("spark.indextables.xref.query.enabled", "true")
      spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "3")

      // Create test data with multiple splits
      (0 until 10).foreach { batchIdx =>
        val data = (0 until 100).map { i =>
          val globalId = batchIdx * 100 + i
          (
            globalId,
            s"term_batch${batchIdx}_row$i",
            s"batch_$batchIdx",
            globalId * 10
          )
        }
        val df = data.toDF("id", "searchable_term", "batch_marker", "value")

        df.write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "value")
          .mode("append")
          .save(tablePath)
      }

      println(s"Wrote 10 batches of test data to Azure")

      // Build XRef index
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")
      println(s"XRef index build completed")

      // Verify XRef was created in transaction log
      val xrefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
      val xrefCount = xrefResult.count()
      assert(xrefCount >= 1, s"XRef entry should be created in transaction log, got $xrefCount")
      println(s"Verified $xrefCount XRef entries in transaction log")

      // Query using XRef
      val dfAll = spark.read.format(DataSourceFormat).load(tablePath)

      // Term query - should use XRef routing
      val termResult = dfAll.filter(col("searchable_term") === "term_batch5_row50")
      assert(termResult.count() == 1, "Term query should return exactly 1 row")

      val row = termResult.collect().head
      assert(row.getAs[Int]("id") == 550, s"Expected id=550, got ${row.getAs[Int]("id")}")

      // Aggregate query
      val countResult = dfAll.agg(count("*")).collect().head.getLong(0)
      assert(countResult == 1000, s"Expected 1000 total rows, got $countResult")

      println(s"XRef Azure integration test PASSED")

    } finally {
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.query.enabled")
      spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
      cleanupTestData(tablePath)
    }
  }

  /**
   * Test XRef with mixed coverage on Azure - verifies uncovered splits are included.
   *
   * This test catches the bug where uncovered splits were incorrectly excluded
   * from query results when XRef routing was active.
   */
  test("XRef mixed coverage on Azure - uncovered splits must be included") {
    assume(hasAzureCredentials(), "Azure credentials required for Azure XRef test")

    configureHadoopForAzure()

    val tablePath = getTestTablePath("mixed-coverage")
    val spark = this.spark
    import spark.implicits._

    println(s"Starting XRef mixed coverage Azure test at: $tablePath")

    try {
      spark.conf.set("spark.indextables.indexWriter.batchSize", "100")
      spark.conf.set("spark.indextables.xref.query.enabled", "true")
      spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "2")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2") // Lower threshold for WHERE-filtered builds

      // Create partitioned data with IDENTICAL searchable terms across all partitions
      val sharedTerms = (0 until 10).map(i => s"shared_term_$i")
      val partitions = Seq("covered_1", "covered_2", "uncovered_1", "uncovered_2")

      partitions.foreach { partition =>
        val data = (0 until 100).map { i =>
          (
            s"${partition}_$i",
            sharedTerms(i % 10),
            partition,
            i * 10
          )
        }
        val df = data.toDF("id", "searchable_term", "partition_key", "value")

        df.write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "value")
          .partitionBy("partition_key")
          .mode("append")
          .save(tablePath)
      }

      println(s"Created partitioned data on Azure")

      // Build XRef ONLY for covered partitions
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath' WHERE partition_key IN ('covered_1', 'covered_2')")
      println(s"Built XRef for covered partitions only")

      // Verify XRef was created for covered partitions
      val xrefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
      val xrefCount = xrefResult.count()
      assert(xrefCount >= 1, s"XRef entry should be created for covered partitions, got $xrefCount")
      println(s"Verified $xrefCount XRef entries in transaction log")

      val dfAll = spark.read.format(DataSourceFormat).load(tablePath)

      // Query for shared term - should return from ALL partitions (including uncovered)
      val sharedTermQuery = dfAll.filter(col("searchable_term") === "shared_term_0")
      val sharedTermCount = sharedTermQuery.count()

      // CRITICAL: Should return 40 rows (10 per partition × 4 partitions)
      // BUG: Without fix, uncovered partitions would be excluded, returning only 20 rows
      assert(sharedTermCount == 40,
        s"Query for shared term should return 40 rows (from ALL partitions), got $sharedTermCount. " +
        "This indicates uncovered splits are being incorrectly excluded on Azure!")

      println(s"Shared term query returned $sharedTermCount rows (expected 40)")

      // Verify each partition has results
      partitions.foreach { partition =>
        val partitionCount = dfAll
          .filter(col("searchable_term") === "shared_term_0")
          .filter(col("partition_key") === partition)
          .count()
        assert(partitionCount == 10,
          s"Partition $partition should have 10 rows, got $partitionCount")
      }

      println(s"All partitions (covered and uncovered) returned correct results")

      // Add data only to uncovered partitions and verify it's found
      val uncoveredOnlyData = Seq(
        ("unique_1", "only_in_uncovered", "uncovered_1", 999),
        ("unique_2", "only_in_uncovered", "uncovered_2", 999)
      ).toDF("id", "searchable_term", "partition_key", "value")

      uncoveredOnlyData.write.format(DataSourceFormat)
        .option("spark.indextables.indexing.fastfields", "value")
        .partitionBy("partition_key")
        .mode("append")
        .save(tablePath)

      val dfUpdated = spark.read.format(DataSourceFormat).load(tablePath)
      val uncoveredOnlyCount = dfUpdated.filter(col("searchable_term") === "only_in_uncovered").count()

      assert(uncoveredOnlyCount == 2,
        s"Query for term only in uncovered splits should return 2 rows, got $uncoveredOnlyCount")

      println(s"Data in uncovered splits found correctly")
      println(s"XRef mixed coverage Azure test PASSED")

    } finally {
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.query.enabled")
      spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
      cleanupTestData(tablePath)
    }
  }

  /**
   * Test XRef aggregate pushdown on Azure.
   */
  test("XRef aggregate pushdown on Azure") {
    assume(hasAzureCredentials(), "Azure credentials required for Azure XRef test")

    configureHadoopForAzure()

    val tablePath = getTestTablePath("aggregates")
    val spark = this.spark
    import spark.implicits._

    println(s"Starting XRef aggregate pushdown Azure test at: $tablePath")

    try {
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.query.enabled", "true")
      spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "3")

      // Create test data with categories
      val categories = Seq("A", "B", "C", "D")
      (0 until 20).foreach { batchIdx =>
        val category = categories(batchIdx % 4)
        val data = (0 until 50).map { i =>
          (batchIdx * 50 + i, category, (i + 1).toLong, (i % 10).toDouble)
        }
        val df = data.toDF("id", "category", "amount", "discount")

        df.write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "amount,discount")
          .mode("append")
          .save(tablePath)
      }

      println(s"Created test data with categories on Azure")

      // Build XRef
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")
      println(s"Built XRef index")

      // Verify XRef was created
      val xrefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
      val xrefCount = xrefResult.count()
      assert(xrefCount >= 1, s"XRef entry should be created in transaction log, got $xrefCount")
      println(s"Verified $xrefCount XRef entries in transaction log")

      val dfAll = spark.read.format(DataSourceFormat).load(tablePath)

      // COUNT across all data
      val totalCount = dfAll.agg(count("*")).collect().head.getLong(0)
      assert(totalCount == 1000, s"Expected 1000 total rows, got $totalCount")

      // SUM with filter
      val sumResult = dfAll
        .filter(col("category") === "A")
        .agg(sum("amount"))
        .collect().head.getLong(0)

      // 5 batches with category A, 50 rows each, amounts 1-50
      // Sum per batch = 1+2+...+50 = 1275
      // Total = 1275 * 5 = 6375
      assert(sumResult == 6375L, s"Expected SUM=6375 for category A, got $sumResult")

      // AVG
      val avgResult = dfAll
        .filter(col("amount") > 25)
        .agg(avg("amount"))
        .collect().head.getDouble(0)

      // Amounts > 25: 26-50 (25 values per batch), average = 38
      assert(avgResult >= 37.0 && avgResult <= 39.0, s"Expected AVG around 38, got $avgResult")

      // MIN/MAX
      val minMax = dfAll.agg(min("amount"), max("amount")).collect().head
      assert(minMax.getLong(0) == 1, s"Expected MIN=1, got ${minMax.getLong(0)}")
      assert(minMax.getLong(1) == 50, s"Expected MAX=50, got ${minMax.getLong(1)}")

      println(s"All aggregate queries returned correct results")
      println(s"XRef aggregate pushdown Azure test PASSED")

    } finally {
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.query.enabled")
      spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
      cleanupTestData(tablePath)
    }
  }
}
