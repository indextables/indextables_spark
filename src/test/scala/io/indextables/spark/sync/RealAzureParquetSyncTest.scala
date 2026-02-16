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

import io.indextables.spark.RealAzureTestBase

/**
 * Real Azure Blob Storage integration tests for BUILD INDEXTABLES COMPANION FOR PARQUET.
 *
 * These tests require:
 * 1. Azure credentials (via ~/.azure/credentials, system properties, or environment variables)
 * 2. An Azure Blob Storage container for test data
 *
 * Tests are automatically skipped if credentials are not available.
 */
class RealAzureParquetSyncTest extends RealAzureTestBase {

  private val testRunId = UUID.randomUUID().toString.substring(0, 8)

  private lazy val hasAzureWasbs: Boolean = {
    try {
      Class.forName("org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  private def azureBasePath: String = {
    val account = getStorageAccount.getOrElse("devstoreaccount1")
    s"wasbs://$testContainer@$account.blob.core.windows.net/parquet-sync-test-$testRunId"
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Set Hadoop credentials for wasbs:// if available
    (getStorageAccount, getAccountKey) match {
      case (Some(a), Some(k)) =>
        val hadoopConf = spark.sparkContext.hadoopConfiguration
        hadoopConf.set(s"fs.azure.account.key.$a.blob.core.windows.net", k)
      case _ =>
    }
  }

  // --- Parsing Tests (no credentials needed) ---

  test("output schema should parse correctly with Azure paths") {
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET 'abfss://container@account.dfs.core.windows.net/parquet' " +
        s"AT LOCATION 'abfss://container@account.dfs.core.windows.net/index' DRY RUN"
    )
    val columns = result.columns.toSet
    columns should contain("table_path")
    columns should contain("source_path")
    columns should contain("status")
    columns should contain("source_version")
    result.columns.length shouldBe 11
  }

  // --- Real Azure Integration Tests ---

  test("create companion from Azure parquet directory") {
    assume(hasAzureCredentials() && hasAzureWasbs,
      "Azure credentials or WASBS driver not available - skipping test")

    val parquetPath = s"$azureBasePath/parquet_data"
    val indexPath = s"$azureBasePath/companion_index"

    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "hello world", "active", 100.0),
      (2, "foo bar baz", "inactive", 200.0),
      (3, "test content", "active", 300.0)
    ).toDF("id", "content", "status", "score")

    df.write.parquet(parquetPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath'"
    )

    val rows = result.collect()
    rows.length shouldBe 1
    rows(0).getString(2) shouldBe "success"
    rows(0).isNullAt(3) shouldBe true // source_version (null for parquet)
    rows(0).getInt(4) should be > 0
    rows(0).getInt(6) should be > 0
  }

  test("partitioned parquet on Azure") {
    assume(hasAzureCredentials() && hasAzureWasbs,
      "Azure credentials or WASBS driver not available - skipping test")

    val parquetPath = s"$azureBasePath/parquet_partitioned"
    val indexPath = s"$azureBasePath/companion_partitioned"

    val _spark = spark
    import _spark.implicits._
    Seq(
      (1, "data-a", "east"),
      (2, "data-b", "east"),
      (3, "data-c", "west")
    ).toDF("id", "content", "region")
      .write.partitionBy("region")
      .parquet(parquetPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath'"
    )
    result.collect()(0).getString(2) shouldBe "success"
  }

  test("DRY RUN on Azure parquet") {
    assume(hasAzureCredentials() && hasAzureWasbs,
      "Azure credentials or WASBS driver not available - skipping test")

    val parquetPath = s"$azureBasePath/parquet_dryrun"
    val indexPath = s"$azureBasePath/companion_dryrun"

    val _spark = spark
    import _spark.implicits._
    Seq((1, "test")).toDF("id", "content").write.parquet(parquetPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath' DRY RUN"
    )
    result.collect()(0).getString(2) shouldBe "dry_run"
  }

  test("FASTFIELDS MODE on Azure parquet") {
    assume(hasAzureCredentials() && hasAzureWasbs,
      "Azure credentials or WASBS driver not available - skipping test")

    val parquetPath = s"$azureBasePath/parquet_ff"
    val indexPath = s"$azureBasePath/companion_ff"

    val _spark = spark
    import _spark.implicits._
    Seq((1, "test", 100.0)).toDF("id", "content", "score").write.parquet(parquetPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' " +
        s"FASTFIELDS MODE DISABLED AT LOCATION '$indexPath'"
    )

    val rows = result.collect()
    rows.length shouldBe 1
    rows(0).getString(2) shouldBe "success"

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
