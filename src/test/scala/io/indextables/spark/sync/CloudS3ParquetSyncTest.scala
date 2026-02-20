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

import io.indextables.spark.CloudS3TestBase

/**
 * Real S3 integration tests for BUILD INDEXTABLES COMPANION FOR PARQUET.
 *
 * These tests require:
 *   1. AWS credentials in ~/.aws/credentials 2. An S3 bucket for test data
 *
 * Tests are automatically skipped if credentials are not available.
 */
class CloudS3ParquetSyncTest extends CloudS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/parquet-sync-test-$testRunId"

  private var awsCredentials: Option[(String, String)] = None

  private def loadAwsCredentials(): Option[(String, String)] = {
    val home     = System.getProperty("user.home")
    val credFile = new File(s"$home/.aws/credentials")

    if (!credFile.exists()) {
      println("Warning: ~/.aws/credentials file not found. Test will be skipped.")
      return None
    }

    Using(new FileInputStream(credFile)) { fis =>
      val props = new Properties()
      props.load(fis)

      val accessKey = props.getProperty("aws_access_key_id")
      val secretKey = props.getProperty("aws_secret_access_key")

      if (accessKey != null && secretKey != null) {
        Some((accessKey, secretKey))
      } else {
        None
      }
    }.getOrElse(None)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    awsCredentials = loadAwsCredentials()

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

  // --- Parsing Tests (no credentials needed) ---

  test("output schema should have source_version column") {
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '/nonexistent/parquet' AT LOCATION '/nonexistent/index' DRY RUN"
    )
    val columns = result.columns.toSet
    columns should contain("table_path")
    columns should contain("source_path")
    columns should contain("status")
    columns should contain("source_version")
    columns should contain("splits_created")
    columns should contain("parquet_files_indexed")
    result.columns.length shouldBe 11
  }

  // --- Real S3 Integration Tests ---

  test("create companion from S3 parquet directory") {
    assume(awsCredentials.isDefined, "AWS credentials not available - skipping test")

    val parquetPath = s"$testBasePath/parquet_data"
    val indexPath   = s"$testBasePath/companion_index"

    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "hello world", "active", 100.0),
      (2, "foo bar baz", "inactive", 200.0),
      (3, "test content", "active", 300.0),
      (4, "spark parquet", "active", 150.0),
      (5, "companion mode", "inactive", 250.0)
    ).toDF("id", "content", "status", "score")

    df.write.parquet(parquetPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath'"
    )

    val rows = result.collect()
    rows.length shouldBe 1
    val row = rows(0)
    row.getString(0) shouldBe indexPath   // table_path
    row.getString(1) shouldBe parquetPath // source_path
    row.getString(2) shouldBe "success"   // status
    row.isNullAt(3) shouldBe true         // source_version (null for parquet)
    row.getInt(4) should be > 0           // splits_created
    row.getInt(6) should be > 0           // parquet_files_indexed
  }

  test("partitioned parquet on S3 should detect partitions") {
    assume(awsCredentials.isDefined, "AWS credentials not available - skipping test")

    val parquetPath = s"$testBasePath/parquet_partitioned"
    val indexPath   = s"$testBasePath/companion_partitioned"

    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "data-a", "2024-01-01"),
      (2, "data-b", "2024-01-01"),
      (3, "data-c", "2024-01-02"),
      (4, "data-d", "2024-01-02"),
      (5, "data-e", "2024-01-03")
    ).toDF("id", "content", "date")

    df.write.partitionBy("date").parquet(parquetPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath'"
    )

    val rows = result.collect()
    rows.length shouldBe 1
    rows(0).getString(2) shouldBe "success"
    rows(0).getInt(6) should be > 0
  }

  test("DRY RUN on S3 parquet") {
    assume(awsCredentials.isDefined, "AWS credentials not available - skipping test")

    val parquetPath = s"$testBasePath/parquet_dryrun"
    val indexPath   = s"$testBasePath/companion_dryrun"

    val _spark = spark
    import _spark.implicits._
    Seq((1, "test data")).toDF("id", "content").write.parquet(parquetPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath' DRY RUN"
    )
    result.collect()(0).getString(2) shouldBe "dry_run"
  }

  test("read-back from S3 parquet companion returns correct data") {
    assume(awsCredentials.isDefined, "AWS credentials not available - skipping test")

    val parquetPath = s"$testBasePath/parquet_readback"
    val indexPath   = s"$testBasePath/companion_readback"

    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "hello world", 100.0),
      (2, "foo bar", 200.0),
      (3, "test data", 300.0)
    ).toDF("id", "content", "score")

    df.write.parquet(parquetPath)

    spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath'"
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

    val allRecords = companionDf.collect()
    allRecords.length shouldBe 3
  }
}
