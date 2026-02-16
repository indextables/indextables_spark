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
 * Real Azure + Iceberg catalog integration tests for BUILD INDEXTABLES COMPANION FOR ICEBERG.
 *
 * These tests require:
 * 1. Azure credentials (via ~/.azure/credentials, system properties, or environment variables)
 * 2. Iceberg catalog config in ~/.iceberg/credentials
 * 3. A pre-existing Iceberg table accessible via the catalog with Azure storage
 *
 * Tests are automatically skipped if credentials are not available.
 */
class RealAzureIcebergSyncTest extends RealAzureTestBase with IcebergTestSupport {

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
    s"wasbs://$testContainer@$account.blob.core.windows.net/iceberg-sync-test-$testRunId"
  }

  private def hasIcebergConfig: Boolean =
    restCatalogConfig.isDefined || glueCatalogConfig.isDefined || hmsCatalogConfig.isDefined

  private def icebergConfig: IcebergCatalogTestConfig =
    restCatalogConfig.orElse(glueCatalogConfig).orElse(hmsCatalogConfig).get

  override def beforeAll(): Unit = {
    super.beforeAll()
    loadIcebergCredentials()

    // Set Hadoop credentials for wasbs:// if available
    (getStorageAccount, getAccountKey) match {
      case (Some(a), Some(k)) =>
        val hadoopConf = spark.sparkContext.hadoopConfiguration
        hadoopConf.set(s"fs.azure.account.key.$a.blob.core.windows.net", k)
      case _ =>
    }
  }

  override def afterEach(): Unit = {
    clearSparkIcebergConfig(spark)
    super.afterEach()
  }

  // --- Real Azure + Iceberg Integration Tests ---

  test("create companion from Iceberg table with Azure storage backend") {
    assume(hasAzureCredentials() && hasAzureWasbs && hasIcebergConfig,
      "Azure credentials, WASBS driver, or Iceberg catalog config not available - skipping test")

    val config = icebergConfig
    configureSparkForCatalog(spark, config)

    val indexPath = s"$azureBasePath/iceberg_companion"

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '${config.tableIdentifier}' " +
        s"AT LOCATION '$indexPath'"
    )

    val rows = result.collect()
    rows.length shouldBe 1
    rows(0).getString(2) shouldBe "success"
    rows(0).isNullAt(3) shouldBe false // source_version (snapshot ID)
    rows(0).getInt(4) should be > 0
  }

  test("DRY RUN on Azure with Iceberg source") {
    assume(hasAzureCredentials() && hasAzureWasbs && hasIcebergConfig,
      "Azure credentials, WASBS driver, or Iceberg catalog config not available - skipping test")

    val config = icebergConfig
    configureSparkForCatalog(spark, config)

    val indexPath = s"$azureBasePath/iceberg_dryrun"

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '${config.tableIdentifier}' " +
        s"AT LOCATION '$indexPath' DRY RUN"
    )
    result.collect()(0).getString(2) shouldBe "dry_run"
  }

  test("FROM SNAPSHOT on Azure with Iceberg source") {
    assume(hasAzureCredentials() && hasAzureWasbs && hasIcebergConfig,
      "Azure credentials, WASBS driver, or Iceberg catalog config not available - skipping test")

    val config = icebergConfig
    configureSparkForCatalog(spark, config)

    val indexPath = s"$azureBasePath/iceberg_snapshot"

    // First sync to get snapshot ID
    val firstResult = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '${config.tableIdentifier}' " +
        s"AT LOCATION '$indexPath'"
    )
    val firstRow = firstResult.collect()(0)
    assume(firstRow.getString(2) == "success", "First sync must succeed")
    val snapshotId = firstRow.getAs[Long](3)

    // Use snapshot ID for FROM SNAPSHOT
    val indexPath2 = s"$azureBasePath/iceberg_snapshot_2"
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '${config.tableIdentifier}' " +
        s"FROM SNAPSHOT $snapshotId " +
        s"AT LOCATION '$indexPath2'"
    )
    val row = result.collect()(0)
    row.getString(2) shouldBe "success"
    row.getAs[Long](3) shouldBe snapshotId
  }

  test("read-back from Azure Iceberg companion") {
    assume(hasAzureCredentials() && hasAzureWasbs && hasIcebergConfig,
      "Azure credentials, WASBS driver, or Iceberg catalog config not available - skipping test")

    val config = icebergConfig
    configureSparkForCatalog(spark, config)

    val indexPath = s"$azureBasePath/iceberg_readback"

    spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '${config.tableIdentifier}' " +
        s"AT LOCATION '$indexPath'"
    ).collect()(0).getString(2) shouldBe "success"

    val companionDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(indexPath)

    companionDf.count() should be > 0L
  }
}
