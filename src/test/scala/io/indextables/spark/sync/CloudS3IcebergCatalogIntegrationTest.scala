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

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLogFactory
import io.indextables.spark.CloudS3TestBase

/**
 * Per-catalog-type integration tests for BUILD INDEXTABLES COMPANION FOR ICEBERG.
 *
 * Tests REST, Glue, and HMS catalog types independently, each gated on catalog-specific credentials from
 * ~/.iceberg/credentials.
 *
 * Requires:
 *   1. AWS credentials in ~/.aws/credentials 2. Per-catalog config in ~/.iceberg/credentials (see IcebergTestSupport
 *      for format) 3. Pre-existing Iceberg tables accessible via each configured catalog
 */
class CloudS3IcebergCatalogIntegrationTest extends CloudS3TestBase with IcebergTestSupport {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/iceberg-catalog-test-$testRunId"

  private var awsCredentials: Option[(String, String)] = None

  private def loadAwsCredentials(): Option[(String, String)] = {
    val home     = System.getProperty("user.home")
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

  // ═══════════════════════════════════════════
  //  REST Catalog Tests
  // ═══════════════════════════════════════════

  test("REST catalog: list files and create companion") {
    assume(
      awsCredentials.isDefined && restCatalogConfig.isDefined,
      "AWS credentials or Iceberg REST catalog config not available - skipping test"
    )

    val config = restCatalogConfig.get
    configureSparkForCatalog(spark, config)

    val indexPath = s"$testBasePath/rest_companion"

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '${config.tableIdentifier}' " +
        s"AT LOCATION '$indexPath'"
    )

    val row = result.collect()(0)
    row.getString(2) shouldBe "success"
    row.getInt(4) should be > 0 // splits_created
    row.getInt(6) should be > 0 // parquet_files_indexed
  }

  test("REST catalog: catalog name recorded in metadata") {
    assume(
      awsCredentials.isDefined && restCatalogConfig.isDefined,
      "AWS credentials or Iceberg REST catalog config not available - skipping test"
    )

    val config = restCatalogConfig.get
    configureSparkForCatalog(spark, config)

    val indexPath = s"$testBasePath/rest_metadata"

    spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR ICEBERG '${config.tableIdentifier}' " +
          s"AT LOCATION '$indexPath'"
      )
      .collect()(0)
      .getString(2) shouldBe "success"

    val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
    try {
      val metadata = txLog.getMetadata()
      metadata.configuration("indextables.companion.sourceFormat") shouldBe "iceberg"
      metadata.configuration("indextables.companion.icebergCatalog") shouldBe "default"
    } finally
      txLog.close()
  }

  test("REST catalog: read-back returns data") {
    assume(
      awsCredentials.isDefined && restCatalogConfig.isDefined,
      "AWS credentials or Iceberg REST catalog config not available - skipping test"
    )

    val config = restCatalogConfig.get
    configureSparkForCatalog(spark, config)

    val indexPath = s"$testBasePath/rest_readback"

    spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR ICEBERG '${config.tableIdentifier}' " +
          s"AT LOCATION '$indexPath'"
      )
      .collect()(0)
      .getString(2) shouldBe "success"

    val (accessKey, secretKey) = awsCredentials.get
    val companionDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(indexPath)

    companionDf.count() should be > 0L
  }

  // ═══════════════════════════════════════════
  //  Glue Catalog Tests
  // ═══════════════════════════════════════════

  test("Glue catalog: list files and create companion") {
    assume(
      awsCredentials.isDefined && glueCatalogConfig.isDefined,
      "AWS credentials or Iceberg Glue catalog config not available - skipping test"
    )

    val config = glueCatalogConfig.get
    configureSparkForCatalog(spark, config)

    val indexPath = s"$testBasePath/glue_companion"

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '${config.tableIdentifier}' " +
        s"AT LOCATION '$indexPath'"
    )

    val row = result.collect()(0)
    row.getString(2) shouldBe "success"
    row.getInt(4) should be > 0
  }

  test("Glue catalog: AWS credentials used for S3 data access") {
    assume(
      awsCredentials.isDefined && glueCatalogConfig.isDefined,
      "AWS credentials or Iceberg Glue catalog config not available - skipping test"
    )

    val config = glueCatalogConfig.get
    configureSparkForCatalog(spark, config)

    val indexPath = s"$testBasePath/glue_creds"

    spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR ICEBERG '${config.tableIdentifier}' " +
          s"AT LOCATION '$indexPath'"
      )
      .collect()(0)
      .getString(2) shouldBe "success"

    // Verify data is actually readable (proves S3 credentials worked)
    val (accessKey, secretKey) = awsCredentials.get
    val companionDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(indexPath)

    companionDf.count() should be > 0L
  }

  test("Glue catalog: read-back returns data") {
    assume(
      awsCredentials.isDefined && glueCatalogConfig.isDefined,
      "AWS credentials or Iceberg Glue catalog config not available - skipping test"
    )

    val config = glueCatalogConfig.get
    configureSparkForCatalog(spark, config)

    val indexPath = s"$testBasePath/glue_readback"

    spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR ICEBERG '${config.tableIdentifier}' " +
          s"AT LOCATION '$indexPath'"
      )
      .collect()(0)
      .getString(2) shouldBe "success"

    val (accessKey, secretKey) = awsCredentials.get
    val companionDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(indexPath)

    companionDf.count() should be > 0L
  }

  // ═══════════════════════════════════════════
  //  HMS (Hive Metastore) Catalog Tests
  // ═══════════════════════════════════════════

  test("HMS catalog: list files and create companion") {
    assume(
      awsCredentials.isDefined && hmsCatalogConfig.isDefined,
      "AWS credentials or Iceberg HMS catalog config not available - skipping test"
    )

    val config = hmsCatalogConfig.get
    configureSparkForCatalog(spark, config)

    val indexPath = s"$testBasePath/hms_companion"

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR ICEBERG '${config.tableIdentifier}' " +
        s"AT LOCATION '$indexPath'"
    )

    val row = result.collect()(0)
    row.getString(2) shouldBe "success"
    row.getInt(4) should be > 0
  }

  test("HMS catalog: catalog recorded in metadata") {
    assume(
      awsCredentials.isDefined && hmsCatalogConfig.isDefined,
      "AWS credentials or Iceberg HMS catalog config not available - skipping test"
    )

    val config = hmsCatalogConfig.get
    configureSparkForCatalog(spark, config)

    val indexPath = s"$testBasePath/hms_metadata"

    spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR ICEBERG '${config.tableIdentifier}' " +
          s"AT LOCATION '$indexPath'"
      )
      .collect()(0)
      .getString(2) shouldBe "success"

    val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
    try {
      val metadata = txLog.getMetadata()
      metadata.configuration("indextables.companion.sourceFormat") shouldBe "iceberg"
      metadata.configuration should contain key "indextables.companion.icebergCatalog"
    } finally
      txLog.close()
  }

  test("HMS catalog: read-back returns data") {
    assume(
      awsCredentials.isDefined && hmsCatalogConfig.isDefined,
      "AWS credentials or Iceberg HMS catalog config not available - skipping test"
    )

    val config = hmsCatalogConfig.get
    configureSparkForCatalog(spark, config)

    val indexPath = s"$testBasePath/hms_readback"

    spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR ICEBERG '${config.tableIdentifier}' " +
          s"AT LOCATION '$indexPath'"
      )
      .collect()(0)
      .getString(2) shouldBe "success"

    val (accessKey, secretKey) = awsCredentials.get
    val companionDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(indexPath)

    companionDf.count() should be > 0L
  }
}
