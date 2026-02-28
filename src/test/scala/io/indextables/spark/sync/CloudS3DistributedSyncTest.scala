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

import io.indextables.spark.CloudS3TestBase

/**
 * Real S3 integration tests for the distributed log read feature of BUILD INDEXTABLES COMPANION.
 *
 * Validates that the distributed transaction log scanning (DistributedSourceScanner) works correctly
 * with real S3 paths, credentials, and Delta/Parquet data. Tests cover:
 *   - S3 credential propagation to executors via broadcast variables
 *   - S3 path format handling (s3a:// scheme normalization)
 *   - Distributed vs non-distributed path equivalence on real cloud storage
 *   - Incremental sync with distributed enabled on S3
 *   - Fallback behavior when distributed scan fails
 *
 * These tests require:
 *   1. AWS credentials in ~/.aws/credentials
 *   2. An S3 bucket for test data (configured via S3_BUCKET constant)
 *
 * Tests are automatically skipped if credentials are not available.
 */
class CloudS3DistributedSyncTest extends CloudS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/dist-sync-test-$testRunId"

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
        Some((accessKey, secretKey))
      } else {
        None
      }
    }.getOrElse(None)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    if (hasDeltaSparkDataSource) {
      spark.stop()
      spark = SparkSession
        .builder()
        .appName("CloudS3DistributedSyncTest")
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
      hadoopConf.set("fs.s3a.access.key", accessKey)
      hadoopConf.set("fs.s3a.secret.key", secretKey)
      hadoopConf.set("fs.s3a.endpoint.region", S3_REGION)
    }
  }

  // ─── Delta: Distributed vs Non-Distributed Equivalence ───

  test("distributed and non-distributed Delta sync should produce identical results on S3") {
    assume(
      awsCredentials.isDefined && hasDeltaSparkDataSource,
      "AWS credentials or Delta Spark DataSource not available - skipping test"
    )

    val deltaPath   = s"$testBasePath/delta_compare"
    val distPath    = s"$testBasePath/companion_dist"
    val nonDistPath = s"$testBasePath/companion_nondist"

    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "hello world", "active", 100.0),
      (2, "foo bar baz", "inactive", 200.0),
      (3, "test content", "active", 300.0),
      (4, "spark delta", "active", 150.0),
      (5, "companion mode", "inactive", 250.0)
    ).toDF("id", "content", "status", "score")

    df.repartition(3).write.format("delta").mode("overwrite").save(deltaPath)

    // Build with distributed enabled (default)
    val distResult = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$distPath'"
    ).collect()

    // Build with distributed disabled
    spark.conf.set("spark.indextables.companion.sync.distributedLogRead.enabled", "false")
    try {
      val nonDistResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$nonDistPath'"
      ).collect()

      distResult(0).getString(2) shouldBe "success"
      nonDistResult(0).getString(2) shouldBe "success"
      distResult(0).getInt(6) shouldBe nonDistResult(0).getInt(6) // same parquet_files_indexed
    } finally {
      spark.conf.unset("spark.indextables.companion.sync.distributedLogRead.enabled")
    }
  }

  // ─── Delta: Distributed Sync on S3 ───

  test("distributed Delta sync should succeed on S3") {
    assume(
      awsCredentials.isDefined && hasDeltaSparkDataSource,
      "AWS credentials or Delta Spark DataSource not available - skipping test"
    )

    val deltaPath = s"$testBasePath/delta_dist"
    val indexPath = s"$testBasePath/companion_delta_dist"

    val _spark = spark
    import _spark.implicits._
    val df = (0 until 50).map(i => (i.toLong, s"name_$i", i * 1.5, i % 2 == 0))
      .toDF("id", "name", "score", "active")

    df.repartition(5).write.format("delta").mode("overwrite").save(deltaPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    ).collect()

    result(0).getString(2) shouldBe "success"
    result(0).getInt(4) should be > 0 // splits_created
    result(0).getInt(6) shouldBe 5    // parquet_files_indexed (5 partitions)
  }

  test("distributed incremental Delta sync should detect new files on S3") {
    assume(
      awsCredentials.isDefined && hasDeltaSparkDataSource,
      "AWS credentials or Delta Spark DataSource not available - skipping test"
    )

    val deltaPath = s"$testBasePath/delta_incr"
    val indexPath = s"$testBasePath/companion_incr"

    val _spark = spark
    import _spark.implicits._

    // Initial write
    (0 until 20).map(i => (i.toLong, s"name_$i", i * 1.5))
      .toDF("id", "name", "score")
      .repartition(2)
      .write.format("delta").mode("overwrite").save(deltaPath)

    val result1 = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    ).collect()
    result1(0).getString(2) shouldBe "success"

    // Append more data
    (100 until 110).map(i => (i.toLong, s"name_$i", i * 1.5))
      .toDF("id", "name", "score")
      .repartition(1)
      .write.format("delta").mode("append").save(deltaPath)

    // Incremental sync should detect the new file
    val result2 = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    ).collect()
    result2(0).getString(2) shouldBe "success"
    result2(0).getInt(6) shouldBe 1 // only the new file
  }

  test("distributed Delta sync with partitions should preserve partition values on S3") {
    assume(
      awsCredentials.isDefined && hasDeltaSparkDataSource,
      "AWS credentials or Delta Spark DataSource not available - skipping test"
    )

    val deltaPath = s"$testBasePath/delta_partitioned"
    val indexPath = s"$testBasePath/companion_partitioned"

    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "alice", "east"),
      (2, "bob", "west"),
      (3, "carol", "east"),
      (4, "dave", "west"),
      (5, "eve", "north")
    ).toDF("id", "name", "region")

    df.write.format("delta").partitionBy("region").mode("overwrite").save(deltaPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    ).collect()

    result(0).getString(2) shouldBe "success"
    result(0).getInt(6) should be > 0

    // Verify partition structure in transaction log
    import org.apache.hadoop.fs.Path
    import io.indextables.spark.transaction.TransactionLogFactory
    val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
    try {
      val files = txLog.listFiles()
      files should not be empty
      files.foreach { file =>
        file.partitionValues should contain key "region"
        file.companionSourceFiles shouldBe defined
      }
      val uniqueRegions = files.map(_.partitionValues("region")).toSet
      uniqueRegions.size should be >= 2
    } finally
      txLog.close()
  }

  // ─── Parquet: Distributed Sync on S3 ───

  test("distributed Parquet sync should succeed on S3") {
    assume(awsCredentials.isDefined, "AWS credentials not available - skipping test")

    val parquetPath = s"$testBasePath/parquet_dist"
    val indexPath   = s"$testBasePath/companion_parquet_dist"

    val _spark = spark
    import _spark.implicits._
    val df = (0 until 30).map(i => (i.toLong, s"name_$i", i * 1.5))
      .toDF("id", "name", "score")

    df.repartition(3).write.parquet(parquetPath)

    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$indexPath'"
    ).collect()

    result(0).getString(2) shouldBe "success"
    result(0).getInt(6) shouldBe 3
  }

  test("distributed and non-distributed Parquet sync should produce identical results on S3") {
    assume(awsCredentials.isDefined, "AWS credentials not available - skipping test")

    val parquetPath = s"$testBasePath/parquet_compare"
    val distPath    = s"$testBasePath/companion_parquet_dist_cmp"
    val nonDistPath = s"$testBasePath/companion_parquet_nondist_cmp"

    val _spark = spark
    import _spark.implicits._
    val df = Seq(
      (1, "hello world", 100.0),
      (2, "foo bar", 200.0),
      (3, "test data", 300.0)
    ).toDF("id", "content", "score")

    df.write.parquet(parquetPath)

    // Distributed (default)
    val distResult = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$distPath'"
    ).collect()

    // Non-distributed
    spark.conf.set("spark.indextables.companion.sync.distributedLogRead.enabled", "false")
    try {
      val nonDistResult = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR PARQUET '$parquetPath' AT LOCATION '$nonDistPath'"
      ).collect()

      distResult(0).getString(2) shouldBe "success"
      nonDistResult(0).getString(2) shouldBe "success"
      distResult(0).getInt(6) shouldBe nonDistResult(0).getInt(6)
    } finally {
      spark.conf.unset("spark.indextables.companion.sync.distributedLogRead.enabled")
    }
  }

  // NOTE: Partitioned Parquet companion sync on S3 has a pre-existing issue also present
  // in CloudS3ParquetSyncTest."partitioned parquet on S3 should detect partitions".
  // This test exercises the distributed path but the underlying issue is in the sync executor.
  test("distributed Parquet sync with partitions should succeed on S3") {
    assume(awsCredentials.isDefined, "AWS credentials not available - skipping test")

    val parquetPath = s"$testBasePath/parquet_partitioned"
    val indexPath   = s"$testBasePath/companion_parquet_partitioned"

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
    ).collect()

    result(0).getString(2) shouldBe "success"
    result(0).getInt(6) should be > 0
  }

  // ─── DistributedSourceScanner direct validation on S3 ───

  test("DistributedSourceScanner.scanParquetDirectory should return relative paths on S3") {
    assume(awsCredentials.isDefined, "AWS credentials not available - skipping test")

    val parquetPath = s"$testBasePath/parquet_scanner_paths"

    val _spark = spark
    import _spark.implicits._
    Seq((1, "test"), (2, "data")).toDF("id", "name").write.parquet(parquetPath)

    val credentials = Map(
      "spark.indextables.aws.accessKey" -> awsCredentials.get._1,
      "spark.indextables.aws.secretKey" -> awsCredentials.get._2,
      "spark.indextables.aws.region"    -> S3_REGION
    )

    val scanner = new DistributedSourceScanner(spark)
    val result  = scanner.scanParquetDirectory(parquetPath, credentials)
    val files   = result.filesRDD.collect()

    files should not be empty
    // Paths should be relative (no s3a:// prefix, no bucket name)
    files.foreach { f =>
      f.path should not startWith ("s3")
      f.path should not startWith ("/")
      f.path should endWith (".parquet")
      f.size should be > 0L
    }
  }

  test("DistributedSourceScanner.scanDeltaTable should return correct files on S3") {
    assume(
      awsCredentials.isDefined && hasDeltaSparkDataSource,
      "AWS credentials or Delta Spark DataSource not available - skipping test"
    )

    val deltaPath = s"$testBasePath/delta_scanner_direct"

    val _spark = spark
    import _spark.implicits._
    (0 until 10).map(i => (i.toLong, s"name_$i"))
      .toDF("id", "name")
      .repartition(2)
      .write.format("delta").mode("overwrite").save(deltaPath)

    // Force a checkpoint so the distributed scanner (which requires _last_checkpoint) can work
    val deltaLog = org.apache.spark.sql.delta.DeltaLog.forTable(spark, deltaPath)
    deltaLog.checkpoint()

    val credentials = Map(
      "spark.indextables.aws.accessKey" -> awsCredentials.get._1,
      "spark.indextables.aws.secretKey" -> awsCredentials.get._2,
      "spark.indextables.aws.region"    -> S3_REGION
    )

    // Compare distributed scan with single-call reader
    val reader      = new DeltaLogReader(deltaPath, credentials)
    val singleFiles = reader.getAllFiles()

    val scanner    = new DistributedSourceScanner(spark)
    val distResult = scanner.scanDeltaTable(deltaPath, credentials)
    val distFiles  = distResult.filesRDD.collect()

    distFiles.size shouldBe singleFiles.size
    distFiles.map(_.path).toSet shouldBe singleFiles.map(_.path).toSet
    distResult.version shouldBe defined
  }

  // ─── Fallback behavior ───

  test("distributed fallback should work when Delta has no checkpoint on S3") {
    assume(
      awsCredentials.isDefined && hasDeltaSparkDataSource,
      "AWS credentials or Delta Spark DataSource not available - skipping test"
    )

    // Small Delta table with no checkpoint — distributed scan will fail,
    // should fall back to single-call path transparently
    val deltaPath = s"$testBasePath/delta_no_checkpoint"
    val indexPath = s"$testBasePath/companion_no_checkpoint"

    val _spark = spark
    import _spark.implicits._
    Seq((1, "single row")).toDF("id", "content")
      .write.format("delta").mode("overwrite").save(deltaPath)

    // This should succeed via fallback even without a checkpoint
    val result = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    ).collect()

    result(0).getString(2) shouldBe "success"
    result(0).getInt(6) should be > 0
  }
}
