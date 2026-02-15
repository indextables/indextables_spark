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

import java.io.{File, FileInputStream}
import java.util.{Properties, UUID}

import scala.util.Using

import io.indextables.spark.RealS3TestBase

/**
 * Real S3 + Iceberg REST catalog integration tests for BUILD INDEXTABLES COMPANION FROM ICEBERG.
 *
 * These tests require:
 * 1. AWS credentials in ~/.aws/credentials
 * 2. Iceberg REST catalog config in ~/.iceberg/credentials (or environment variables)
 * 3. A pre-existing Iceberg table accessible via the REST catalog
 *
 * Tests are automatically skipped if credentials are not available.
 */
class RealS3IcebergSyncTest extends RealS3TestBase with IcebergTestSupport {

  private val S3_BUCKET = "test-tantivy4sparkbucket"
  private val S3_REGION = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"
  private val testRunId = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/iceberg-sync-test-$testRunId"

  private var awsCredentials: Option[(String, String)] = None

  private def loadAwsCredentials(): Option[(String, String)] = {
    val home = System.getProperty("user.home")
    val credFile = new File(s"$home/.aws/credentials")

    if (!credFile.exists()) return None

    Using(new FileInputStream(credFile)) { fis =>
      val props = new Properties()
      props.load(fis)
      val accessKey = props.getProperty("aws_access_key_id")
      val secretKey = props.getProperty("aws_secret_access_key")
      if (accessKey != null && secretKey != null) Some((accessKey, secretKey))
      else None
    }.getOrElse(None)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    awsCredentials = loadAwsCredentials()
    loadIcebergCredentials()

    if (awsCredentials.isDefined) {
      val (accessKey, secretKey) = awsCredentials.get
      spark.conf.set("spark.indextables.aws.accessKey", accessKey)
      spark.conf.set("spark.indextables.aws.secretKey", secretKey)
      spark.conf.set("spark.indextables.aws.region", S3_REGION)

      val hadoopConf = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
      hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
      hadoopConf.set("spark.indextables.aws.region", S3_REGION)
      hadoopConf.set("fs.s3a.access.key", accessKey)
      hadoopConf.set("fs.s3a.secret.key", secretKey)
      hadoopConf.set("fs.s3a.endpoint.region", S3_REGION)
    }
  }

  override def afterEach(): Unit = {
    clearSparkIcebergConfig(spark)
    super.afterEach()
  }

  // --- Parsing Test (no credentials needed) ---

  test("BUILD COMPANION FROM ICEBERG should parse with catalog config") {
    val result = spark.sql(
      "BUILD INDEXTABLES COMPANION FROM ICEBERG 'default.test_table' " +
        "CATALOG 'my_catalog' " +
        "AT LOCATION '/tmp/iceberg_index' DRY RUN"
    )
    val columns = result.columns.toSet
    columns should contain("table_path")
    columns should contain("source_path")
    columns should contain("status")
    columns should contain("source_version")
    result.columns.length shouldBe 11
  }

  // --- Real S3 + Iceberg Integration Tests ---

  test("create companion from REST catalog table") {
    assume(awsCredentials.isDefined && restCatalogConfig.isDefined,
      "AWS credentials or Iceberg REST catalog config not available - skipping test")

    val config = restCatalogConfig.get
    configureSparkForCatalog(spark, config)

    val indexPath = s"$testBasePath/iceberg_companion"

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FROM ICEBERG '${config.tableIdentifier}' " +
        s"AT LOCATION '$indexPath'"
    )

    val rows = result.collect()
    rows.length shouldBe 1
    val row = rows(0)
    row.getString(2) shouldBe "success"
    row.isNullAt(3) shouldBe false // source_version (Iceberg snapshot ID)
    row.getInt(4) should be > 0   // splits_created
    row.getInt(6) should be > 0   // parquet_files_indexed

    // Verify metadata records sourceFormat=iceberg
    import org.apache.hadoop.fs.Path
    import io.indextables.spark.transaction.TransactionLogFactory
    val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
    try {
      val metadata = txLog.getMetadata()
      metadata.configuration("indextables.companion.sourceFormat") shouldBe "iceberg"
      metadata.configuration("indextables.companion.enabled") shouldBe "true"
    } finally {
      txLog.close()
    }
  }

  test("DRY RUN on Iceberg table") {
    assume(awsCredentials.isDefined && restCatalogConfig.isDefined,
      "AWS credentials or Iceberg REST catalog config not available - skipping test")

    val config = restCatalogConfig.get
    configureSparkForCatalog(spark, config)

    val indexPath = s"$testBasePath/iceberg_dryrun"

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FROM ICEBERG '${config.tableIdentifier}' " +
        s"AT LOCATION '$indexPath' DRY RUN"
    )
    result.collect()(0).getString(2) shouldBe "dry_run"
  }

  test("FROM SNAPSHOT time travel on Iceberg table") {
    assume(awsCredentials.isDefined && restCatalogConfig.isDefined,
      "AWS credentials or Iceberg REST catalog config not available - skipping test")

    val config = restCatalogConfig.get
    configureSparkForCatalog(spark, config)

    val indexPath = s"$testBasePath/iceberg_snapshot"

    // First: get the current snapshot ID via a normal sync
    val firstResult = spark.sql(
      s"BUILD INDEXTABLES COMPANION FROM ICEBERG '${config.tableIdentifier}' " +
        s"AT LOCATION '$indexPath'"
    )
    val firstRow = firstResult.collect()(0)
    assume(firstRow.getString(2) == "success", "First sync must succeed")
    val snapshotId = firstRow.getAs[Long](3)

    // Use that snapshot ID for a FROM SNAPSHOT query (should result in no_action since same snapshot)
    val indexPath2 = s"$testBasePath/iceberg_snapshot_2"
    val secondResult = spark.sql(
      s"BUILD INDEXTABLES COMPANION FROM ICEBERG '${config.tableIdentifier}' " +
        s"FROM SNAPSHOT $snapshotId " +
        s"AT LOCATION '$indexPath2'"
    )
    val secondRow = secondResult.collect()(0)
    secondRow.getString(2) shouldBe "success"
    secondRow.getAs[Long](3) shouldBe snapshotId
  }

  test("read-back from Iceberg companion returns correct data") {
    assume(awsCredentials.isDefined && restCatalogConfig.isDefined,
      "AWS credentials or Iceberg REST catalog config not available - skipping test")

    val config = restCatalogConfig.get
    configureSparkForCatalog(spark, config)

    val indexPath = s"$testBasePath/iceberg_readback"

    val syncResult = spark.sql(
      s"BUILD INDEXTABLES COMPANION FROM ICEBERG '${config.tableIdentifier}' " +
        s"AT LOCATION '$indexPath'"
    )
    syncResult.collect()(0).getString(2) shouldBe "success"

    val (accessKey, secretKey) = awsCredentials.get
    val companionDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(indexPath)

    val count = companionDf.count()
    count should be > 0L
  }

  test("re-sync with same snapshot is no_action") {
    assume(awsCredentials.isDefined && restCatalogConfig.isDefined,
      "AWS credentials or Iceberg REST catalog config not available - skipping test")

    val config = restCatalogConfig.get
    configureSparkForCatalog(spark, config)

    val indexPath = s"$testBasePath/iceberg_resync"

    // First sync
    spark.sql(
      s"BUILD INDEXTABLES COMPANION FROM ICEBERG '${config.tableIdentifier}' " +
        s"AT LOCATION '$indexPath'"
    ).collect()(0).getString(2) shouldBe "success"

    // Re-sync with same snapshot should be no_action
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FROM ICEBERG '${config.tableIdentifier}' " +
        s"AT LOCATION '$indexPath'"
    )
    result.collect()(0).getString(2) shouldBe "no_action"
  }
}
