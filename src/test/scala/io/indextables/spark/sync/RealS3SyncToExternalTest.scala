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

import org.apache.spark.sql.SparkSession

import io.indextables.spark.RealS3TestBase

/**
 * Real S3 integration tests for BUILD INDEXTABLES COMPANION FOR DELTA.
 *
 * These tests require:
 *   1. AWS credentials in ~/.aws/credentials 2. An S3 bucket for test data (configured via S3_BUCKET constant) 3. A
 *      Delta table on S3 (or tests that create one)
 *
 * Tests are automatically skipped if credentials are not available.
 */
class RealS3SyncToExternalTest extends RealS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/sync-test-$testRunId"

  private lazy val hasDeltaSparkDataSource: Boolean =
    try {
      Class.forName("io.delta.sql.DeltaSparkSessionExtension")
      true
    } catch {
      case _: ClassNotFoundException => false
    }

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
        println("Loaded AWS credentials from ~/.aws/credentials")
        Some((accessKey, secretKey))
      } else {
        println("Warning: Could not load credentials from ~/.aws/credentials")
        None
      }
    }.getOrElse {
      println("Warning: Error reading ~/.aws/credentials file")
      None
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Recreate SparkSession with Delta extensions if available
    if (hasDeltaSparkDataSource) {
      spark.stop()
      spark = SparkSession
        .builder()
        .appName("RealS3SyncTest")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", java.nio.file.Files.createTempDirectory("spark-warehouse").toString)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config(
          "spark.sql.extensions",
          "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
    }

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

      // Set standard Hadoop S3A credentials for Delta Lake writes
      hadoopConf.set("fs.s3a.access.key", accessKey)
      hadoopConf.set("fs.s3a.secret.key", secretKey)
      hadoopConf.set("fs.s3a.endpoint.region", S3_REGION)
    }
  }

  // --- Parsing Tests (no credentials needed) ---

  test("BUILD INDEXTABLES COMPANION FOR DELTA should parse with S3 paths") {
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$testBasePath/delta_source' AT LOCATION '$testBasePath/index_dest' DRY RUN"
    )

    // DRY RUN should return a result even without a real Delta table
    // (it will fail with a meaningful error about the Delta table not existing)
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

  test("SYNC command output schema should have 11 columns") {
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '/nonexistent/delta' AT LOCATION '/nonexistent/index' DRY RUN"
    )
    result.columns.length shouldBe 11
  }

  test("SYNC command should return error status for non-existent Delta table") {
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/nonexistent_delta_$testRunId' AT LOCATION '/tmp/nonexistent_index_$testRunId'"
    )
    val rows = result.collect()
    rows.length shouldBe 1
    rows(0).getString(2) shouldBe "error" // status column
  }

  // --- Real S3 Integration Tests ---

  test("SYNC should create companion index from S3 Delta table") {
    assume(
      awsCredentials.isDefined && hasDeltaSparkDataSource,
      "AWS credentials or Delta Spark DataSource not available - skipping test"
    )

    val deltaPath = s"$testBasePath/delta_table"
    val indexPath = s"$testBasePath/companion_index"

    // Step 1: Create a Delta table on S3
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
    row.getInt(4) should be > 0         // splits_created
    row.getInt(6) should be > 0         // parquet_files_indexed
  }

  test("SYNC should support DRY RUN mode on S3") {
    assume(
      awsCredentials.isDefined && hasDeltaSparkDataSource,
      "AWS credentials or Delta Spark DataSource not available - skipping test"
    )

    val deltaPath = s"$testBasePath/delta_dryrun"
    val indexPath = s"$testBasePath/companion_dryrun"

    // Create a Delta table
    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "test data", "active")
    ).toDF("id", "content", "status")

    df.write
      .format("delta")
      .mode("overwrite")
      .save(deltaPath)

    // Run SYNC with DRY RUN
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath' DRY RUN"
    )

    val rows = result.collect()
    rows.length shouldBe 1
    rows(0).getString(2) shouldBe "dry_run" // status
  }

  test("SYNC should handle partitioned Delta tables on S3") {
    assume(
      awsCredentials.isDefined && hasDeltaSparkDataSource,
      "AWS credentials or Delta Spark DataSource not available - skipping test"
    )

    val deltaPath = s"$testBasePath/delta_partitioned"
    val indexPath = s"$testBasePath/companion_partitioned"

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
    rows(0).getInt(6) should be > 0 // parquet_files_indexed
  }

  test("incremental SYNC should detect no-op when already synced") {
    assume(
      awsCredentials.isDefined && hasDeltaSparkDataSource,
      "AWS credentials or Delta Spark DataSource not available - skipping test"
    )

    val deltaPath = s"$testBasePath/delta_incremental"
    val indexPath = s"$testBasePath/companion_incremental"

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

    // Second sync (same delta version) - should be no_action
    val result2 = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    )
    val row2 = result2.collect()(0)
    row2.getString(2) shouldBe "no_action"
  }

  test("SYNC with multi-level partitioned Delta table should create readable companion splits") {
    assume(
      awsCredentials.isDefined && hasDeltaSparkDataSource,
      "AWS credentials or Delta Spark DataSource not available - skipping test"
    )

    val deltaPath = s"$testBasePath/delta_multi_partition"
    val indexPath = s"$testBasePath/companion_multi_partition"

    // Step 1: Create a Delta table partitioned by load_date AND load_hour
    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "morning report", "2025-01-01", "08"),
      (2, "mid-morning update", "2025-01-01", "10"),
      (3, "afternoon summary", "2025-01-01", "14"),
      (4, "early data", "2025-01-02", "06"),
      (5, "noon analysis", "2025-01-02", "12"),
      (6, "evening wrap", "2025-01-02", "18"),
      (7, "late night batch", "2025-01-03", "22"),
      (8, "overnight sync", "2025-01-03", "03")
    ).toDF("id", "content", "load_date", "load_hour")

    df.write
      .format("delta")
      .partitionBy("load_date", "load_hour")
      .mode("overwrite")
      .save(deltaPath)

    // Step 2: Build companion index
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    )

    val rows = result.collect()
    rows.length shouldBe 1
    val row = rows(0)
    row.getString(2) shouldBe "success" // status
    row.getInt(4) should be > 0         // splits_created
    row.getInt(6) should be > 0         // parquet_files_indexed

    // Step 3: Verify transaction log has correct partition structure
    import org.apache.hadoop.fs.Path
    import io.indextables.spark.transaction.TransactionLogFactory
    val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
    try {
      val files = txLog.listFiles()
      files should not be empty

      // Every AddAction should have both partition keys
      files.foreach { file =>
        file.partitionValues should contain key "load_date"
        file.partitionValues should contain key "load_hour"
        // Path should include partition directory structure
        file.path should include("load_date=")
        file.path should include("load_hour=")
      }

      // Verify we have files for multiple partitions
      val uniquePartitions = files.map(_.partitionValues).toSet
      uniquePartitions.size should be >= 3 // at least 3 distinct (load_date, load_hour) combos
    } finally
      txLog.close()

    // Step 4: Read the companion index back and verify splits are physically accessible in S3
    val (accessKey, secretKey) = awsCredentials.get
    val companionDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(indexPath)

    // Force actual split reading (collect, not count which uses aggregate pushdown)
    val allRecords = companionDf.collect()
    allRecords.length shouldBe 8

    // Verify partition columns are present in the schema
    companionDf.schema.fieldNames should contain("load_date")
    companionDf.schema.fieldNames should contain("load_hour")

    // Verify partition filtering works
    val jan01Records = companionDf.filter($"load_date" === "2025-01-01").collect()
    jan01Records.length shouldBe 3

    val jan02Hour12 = companionDf
      .filter($"load_date" === "2025-01-02" && $"load_hour" === "12")
      .collect()
    jan02Hour12.length shouldBe 1
  }

  test("SYNC with FASTFIELDS MODE should be reflected in companion splits") {
    assume(
      awsCredentials.isDefined && hasDeltaSparkDataSource,
      "AWS credentials or Delta Spark DataSource not available - skipping test"
    )

    val deltaPath = s"$testBasePath/delta_fastfield"
    val indexPath = s"$testBasePath/companion_fastfield"

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
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE PARQUET_ONLY AT LOCATION '$indexPath'"
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
      files.foreach(file => file.companionFastFieldMode shouldBe Some("PARQUET_ONLY"))
    } finally
      txLog.close()
  }
}
