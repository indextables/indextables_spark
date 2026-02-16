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

import java.util.UUID

import org.apache.spark.sql.SparkSession

import io.indextables.spark.RealAzureTestBase

/**
 * Real Azure Blob Storage integration tests for BUILD INDEXTABLES COMPANION FOR DELTA.
 *
 * These tests require:
 * 1. Azure credentials (via ~/.azure/credentials, system properties, or environment variables)
 * 2. An Azure Blob Storage container for test data
 * 3. Delta Lake support configured for Azure (delta-standalone)
 *
 * Tests are automatically skipped if credentials are not available.
 */
class RealAzureSyncToExternalTest extends RealAzureTestBase {

  private val testRunId = UUID.randomUUID().toString.substring(0, 8)

  private lazy val hasAzureWasbs: Boolean = {
    try {
      Class.forName("org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  private lazy val hasDeltaSparkDataSource: Boolean = {
    try {
      Class.forName("io.delta.sql.DeltaSparkSessionExtension")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  // Use wasbs:// (Blob endpoint) instead of abfss:// (DFS endpoint) because storage
  // accounts with BlobStorageEvents or SoftDelete enabled reject DFS requests with 409.
  private def azureBasePath: String = {
    val account = getStorageAccount.getOrElse("devstoreaccount1")
    s"wasbs://$testContainer@$account.blob.core.windows.net/sync-test-$testRunId"
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Recreate SparkSession with Delta extensions if available
    if (hasDeltaSparkDataSource) {
      // Capture credentials before stopping session
      val account = getStorageAccount
      val key = getAccountKey
      val connStr = getConnectionString

      spark.stop()
      val builder = SparkSession.builder()
        .appName("RealAzureSyncTest")
        .master("local[2]")
        .config("spark.sql.warehouse.dir",
          java.nio.file.Files.createTempDirectory("spark-warehouse").toString)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.extensions",
          "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
            "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")

      // Re-apply Azure credentials
      account.foreach(a => builder.config("spark.indextables.azure.accountName", a))
      key.foreach(k => builder.config("spark.indextables.azure.accountKey", k))
      connStr.foreach(c => builder.config("spark.indextables.azure.connectionString", c))

      spark = builder.getOrCreate()
      spark.sparkContext.setLogLevel("WARN")

      // Set Hadoop credentials for wasbs:// (Blob endpoint) for Delta Lake writes.
      // We use the Blob endpoint instead of the DFS endpoint because storage accounts
      // with BlobStorageEvents or SoftDelete enabled reject DFS requests with 409.
      (account, key) match {
        case (Some(a), Some(k)) =>
          val hadoopConf = spark.sparkContext.hadoopConfiguration
          hadoopConf.set(s"fs.azure.account.key.$a.blob.core.windows.net", k)
        case _ =>
      }
    }
  }

  // --- Parsing Tests (no credentials needed) ---

  test("BUILD INDEXTABLES COMPANION FOR DELTA should parse with Azure paths") {
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA 'abfss://container@account.dfs.core.windows.net/delta' " +
        s"AT LOCATION 'abfss://container@account.dfs.core.windows.net/index' DRY RUN"
    )

    val columns = result.columns.toSet
    columns should contain("table_path")
    columns should contain("source_path")
    columns should contain("status")
    columns should contain("source_version")
    columns should contain("splits_created")
    columns should contain("splits_invalidated")
    columns should contain("parquet_files_indexed")
    columns should contain("parquet_bytes_downloaded")
    columns should contain("split_bytes_uploaded")
    columns should contain("duration_ms")
    columns should contain("message")
  }

  test("SYNC command should return error for non-existent Azure Delta table") {
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/nonexistent_delta_${testRunId}' AT LOCATION '/tmp/nonexistent_index_${testRunId}'"
    )
    val rows = result.collect()
    rows.length shouldBe 1
    rows(0).getString(2) shouldBe "error" // status column
  }

  // --- Real Azure Integration Tests ---

  test("SYNC should create companion index from Azure Delta table") {
    assume(hasAzureCredentials() && hasDeltaSparkDataSource && hasAzureWasbs,
      "Azure credentials, Delta Spark DataSource, or Azure WASBS driver not available - skipping test")

    val deltaPath = s"$azureBasePath/delta_table"
    val indexPath = s"$azureBasePath/companion_index"

    // Step 1: Create a Delta table on Azure
    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "hello world", "active", 100.0),
      (2, "foo bar baz", "inactive", 200.0),
      (3, "test content", "active", 300.0),
      (4, "spark delta", "active", 150.0),
      (5, "companion mode", "inactive", 250.0)
    ).toDF("id", "content", "status", "score")

    df.write
      .format("delta")
      .mode("overwrite")
      .save(deltaPath)

    // Step 2: Run SYNC to create companion index
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    )

    val rows = result.collect()
    rows.length shouldBe 1

    val row = rows(0)
    row.getString(0) shouldBe indexPath // table_path
    row.getString(1) shouldBe deltaPath // source_path
    row.getString(2) shouldBe "success" // status
    row.getInt(4) should be > 0 // splits_created
    row.getInt(6) should be > 0 // parquet_files_indexed
  }

  test("SYNC should support DRY RUN mode on Azure") {
    assume(hasAzureCredentials() && hasDeltaSparkDataSource && hasAzureWasbs,
      "Azure credentials, Delta Spark DataSource, or Azure WASBS driver not available - skipping test")

    val deltaPath = s"$azureBasePath/delta_dryrun"
    val indexPath = s"$azureBasePath/companion_dryrun"

    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "test data", "active")
    ).toDF("id", "content", "status")

    df.write
      .format("delta")
      .mode("overwrite")
      .save(deltaPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath' DRY RUN"
    )

    val rows = result.collect()
    rows.length shouldBe 1
    rows(0).getString(2) shouldBe "dry_run"
  }

  test("SYNC should handle partitioned Delta tables on Azure") {
    assume(hasAzureCredentials() && hasDeltaSparkDataSource && hasAzureWasbs,
      "Azure credentials, Delta Spark DataSource, or Azure WASBS driver not available - skipping test")

    val deltaPath = s"$azureBasePath/delta_partitioned"
    val indexPath = s"$azureBasePath/companion_partitioned"

    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "data-a", "2024-01-01"),
      (2, "data-b", "2024-01-01"),
      (3, "data-c", "2024-01-02"),
      (4, "data-d", "2024-01-02"),
      (5, "data-e", "2024-01-03")
    ).toDF("id", "content", "date")

    df.write
      .format("delta")
      .partitionBy("date")
      .mode("overwrite")
      .save(deltaPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    )

    val rows = result.collect()
    rows.length shouldBe 1
    rows(0).getString(2) shouldBe "success"
    rows(0).getInt(6) should be > 0
  }

  test("incremental SYNC should detect no-op when already synced on Azure") {
    assume(hasAzureCredentials() && hasDeltaSparkDataSource && hasAzureWasbs,
      "Azure credentials, Delta Spark DataSource, or Azure WASBS driver not available - skipping test")

    val deltaPath = s"$azureBasePath/delta_incremental"
    val indexPath = s"$azureBasePath/companion_incremental"

    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "initial data", "active")
    ).toDF("id", "content", "status")

    df.write
      .format("delta")
      .mode("overwrite")
      .save(deltaPath)

    // First sync
    val result1 = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    )
    result1.collect()(0).getString(2) shouldBe "success"

    // Second sync (same delta version)
    val result2 = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    )
    result2.collect()(0).getString(2) shouldBe "no_action"
  }

  test("SYNC with FASTFIELDS MODE on Azure should be reflected in companion splits") {
    assume(hasAzureCredentials() && hasDeltaSparkDataSource && hasAzureWasbs,
      "Azure credentials, Delta Spark DataSource, or Azure WASBS driver not available - skipping test")

    val deltaPath = s"$azureBasePath/delta_fastfield"
    val indexPath = s"$azureBasePath/companion_fastfield"

    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "test content", 100.0)
    ).toDF("id", "content", "score")

    df.write
      .format("delta")
      .mode("overwrite")
      .save(deltaPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE DISABLED AT LOCATION '$indexPath'"
    )

    val rows = result.collect()
    rows.length shouldBe 1
    rows(0).getString(2) shouldBe "success"

    // Verify the companion splits have the correct fast field mode
    import org.apache.hadoop.fs.Path
    import io.indextables.spark.transaction.TransactionLogFactory
    val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
    try {
      val files = txLog.listFiles()
      files should not be empty
      files.foreach { file =>
        file.companionFastFieldMode shouldBe Some("DISABLED")
      }
    } finally {
      txLog.close()
    }
  }
}
