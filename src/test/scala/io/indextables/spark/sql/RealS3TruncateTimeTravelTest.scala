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

package io.indextables.spark.sql

import java.io.File
import java.util.{Properties, UUID}

import scala.util.Using

import org.apache.hadoop.fs.{FileSystem, Path}

import io.indextables.spark.RealS3TestBase

/**
 * Real AWS S3 integration tests for TRUNCATE INDEXTABLES TIME TRAVEL command.
 *
 * Tests critical functionality specific to S3:
 *   - Transaction log file deletion on S3
 *   - Checkpoint creation and preservation on S3
 *   - Credential handling for cloud operations
 *   - Data integrity after truncation on S3
 *
 * Credentials are loaded from ~/.aws/credentials file.
 */
class RealS3TruncateTimeTravelTest extends RealS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/truncate-time-travel-test-$testRunId"

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None
  private var fs: FileSystem                           = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    if (awsCredentials.isDefined) {
      val (accessKey, secretKey) = awsCredentials.get

      // Configure Hadoop config FIRST before creating FileSystem
      val hadoopConf = spark.sparkContext.hadoopConfiguration

      // Set standard S3A properties for Hadoop FileSystem
      hadoopConf.set("fs.s3a.access.key", accessKey)
      hadoopConf.set("fs.s3a.secret.key", secretKey)
      hadoopConf.set("fs.s3a.endpoint.region", S3_REGION)

      // Also set indextables properties for CloudStorageProvider
      hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
      hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
      hadoopConf.set("spark.indextables.aws.region", S3_REGION)

      // Configure Spark for real S3 access
      spark.conf.set("spark.indextables.aws.accessKey", accessKey)
      spark.conf.set("spark.indextables.aws.secretKey", secretKey)
      spark.conf.set("spark.indextables.aws.region", S3_REGION)

      // Set checkpoint interval low for testing
      spark.conf.set("spark.indextables.checkpoint.interval", "5")

      // Initialize FileSystem AFTER setting Hadoop config
      val testPath = new Path(testBasePath)
      fs = testPath.getFileSystem(hadoopConf)

      println(s"🔐 AWS credentials loaded successfully")
      println(s"🌊 Configured Spark for S3 access to bucket: $S3_BUCKET in region: $S3_REGION")
      println(s"📍 Test base path: $testBasePath")
    } else {
      println(s"⚠️  No AWS credentials found in ~/.aws/credentials - tests will be skipped")
    }
  }

  override def afterAll(): Unit = {
    // Clean up test data
    if (awsCredentials.isDefined && fs != null) {
      try {
        val basePath = new Path(testBasePath)
        if (fs.exists(basePath)) {
          fs.delete(basePath, true)
          println(s"🗑️  Cleaned up test data at $testBasePath")
        }
      } catch {
        case ex: Exception =>
          println(s"⚠️  Failed to clean up test data: ${ex.getMessage}")
      }
    }
    super.afterAll()
  }

  /** Load AWS credentials from ~/.aws/credentials file. */
  private def loadAwsCredentials(): Option[(String, String)] =
    try {
      val home     = System.getProperty("user.home")
      val credFile = new File(s"$home/.aws/credentials")

      if (!credFile.exists()) {
        println(s"AWS credentials file not found at: ${credFile.getAbsolutePath}")
        return None
      }

      Using(new java.io.FileInputStream(credFile)) { fis =>
        val props = new Properties()
        props.load(fis)

        val accessKey = props.getProperty("aws_access_key_id")
        val secretKey = props.getProperty("aws_secret_access_key")

        if (accessKey != null && secretKey != null) {
          Some((accessKey, secretKey))
        } else {
          println(s"AWS credentials not found in ~/.aws/credentials")
          None
        }
      }.toOption.flatten
    } catch {
      case ex: Exception =>
        println(s"Failed to read AWS credentials: ${ex.getMessage}")
        None
    }

  /** Count transaction log files in S3. */
  private def countTransactionLogFiles(tablePath: String): (Int, Int) = {
    val txLogPath = new Path(tablePath, "_transaction_log")
    if (!fs.exists(txLogPath)) {
      return (0, 0)
    }

    val files             = fs.listStatus(txLogPath)
    val versionPattern    = """^\d{20}\.json$""".r
    val checkpointPattern = """^\d{20}\.checkpoint.*\.json$""".r

    val versionCount    = files.count(f => versionPattern.findFirstIn(f.getPath.getName).isDefined)
    val checkpointCount = files.count(f => checkpointPattern.findFirstIn(f.getPath.getName).isDefined)

    (versionCount, checkpointCount)
  }

  test("TRUNCATE INDEXTABLES TIME TRAVEL should work on real S3") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/truncate_basic_test"

    // Write data with multiple transactions
    val sparkSession = spark
    import sparkSession.implicits._

    // Write 8 transactions to create version files
    for (i <- 1 to 8) {
      val data = Seq((i, s"value_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    println(s"✅ Created table with 8 transactions at $tablePath")

    // Verify we have version files before truncation
    val (versionsBefore, _) = countTransactionLogFiles(tablePath)
    println(s"📊 Version files before truncation: $versionsBefore")
    versionsBefore should be >= 8

    // Truncate time travel
    val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'")
    val row    = result.collect().head

    println(s"📋 Truncation result: status=${row.getString(1)}, versions_deleted=${row.getLong(3)}")

    row.getString(1) shouldBe "SUCCESS"
    row.getLong(2) should be >= 0L // checkpoint_version
    row.getLong(3) should be >= 0L // versions_deleted

    // Verify version files were deleted
    val (versionsAfter, checkpointsAfter) = countTransactionLogFiles(tablePath)
    println(s"📊 Version files after truncation: $versionsAfter, checkpoints: $checkpointsAfter")
    versionsAfter should be < versionsBefore
    checkpointsAfter should be >= 1 // At least one checkpoint should remain

    // Verify data integrity
    val finalCount = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()
    finalCount shouldBe 8
    println(s"✅ Data integrity verified: $finalCount records")
  }

  test("TRUNCATE INDEXTABLES TIME TRAVEL DRY RUN should not delete files on S3") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/truncate_dry_run_test"

    // Write data with multiple transactions
    val sparkSession = spark
    import sparkSession.implicits._

    for (i <- 1 to 6) {
      val data = Seq((i, s"value_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    println(s"✅ Created table with 6 transactions at $tablePath")

    val (versionsBefore, _) = countTransactionLogFiles(tablePath)

    // Run DRY RUN
    val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath' DRY RUN")
    val row    = result.collect().head

    println(s"📋 DRY RUN result: status=${row.getString(1)}, would_delete=${row.getLong(3)}")

    row.getString(1) shouldBe "DRY_RUN"

    // Verify NO files were deleted
    val (versionsAfter, _) = countTransactionLogFiles(tablePath)
    versionsAfter shouldBe versionsBefore
    println(s"✅ DRY RUN verified: no files deleted (before=$versionsBefore, after=$versionsAfter)")
  }

  test("TRUNCATE INDEXTABLES TIME TRAVEL should preserve data integrity on S3 with many transactions") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/truncate_integrity_test"

    // Write data with many transactions to trigger multiple checkpoints
    val sparkSession = spark
    import sparkSession.implicits._

    for (i <- 1 to 15) {
      val data = Seq((i, s"value_$i", i * 10.0)).toDF("id", "value", "score")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    println(s"✅ Created table with 15 transactions at $tablePath")

    // Get data before truncation
    val dataBefore = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val countBefore = dataBefore.count()
    val sumBefore   = dataBefore.agg(Map("score" -> "sum")).collect().head.getDouble(0)

    // Truncate
    val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'")
    val row    = result.collect().head

    println(s"📋 Truncation result: status=${row.getString(1)}, versions_deleted=${row.getLong(3)}")
    row.getString(1) shouldBe "SUCCESS"

    // Verify data integrity
    val dataAfter = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val countAfter = dataAfter.count()
    val sumAfter   = dataAfter.agg(Map("score" -> "sum")).collect().head.getDouble(0)

    countAfter shouldBe countBefore
    sumAfter shouldBe sumBefore
    countAfter shouldBe 15
    sumAfter shouldBe 1200.0

    println(s"✅ Data integrity verified after truncation:")
    println(s"   - Count: before=$countBefore, after=$countAfter")
    println(s"   - Sum(score): before=$sumBefore, after=$sumAfter")
  }

  test("TRUNCATE should handle multiple existing checkpoints with uncheckpointed version on S3") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/truncate_multi_checkpoint_test"

    // Use checkpoint interval of 10 for this test
    spark.conf.set("spark.indextables.checkpoint.interval", "10")

    try {
      val sparkSession = spark
      import sparkSession.implicits._

      // Write 21 transactions to create:
      // - Checkpoint at version 10
      // - Checkpoint at version 20
      // - Version 21 (uncheckpointed - the "extra" version after checkpoint)
      for (i <- 1 to 21) {
        val data = Seq((i, s"value_$i", i * 100.0)).toDF("id", "value", "amount")
        data.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode(if (i == 1) "overwrite" else "append")
          .save(tablePath)
      }

      println(s"✅ Created table with 21 transactions at $tablePath")

      // Verify we have multiple checkpoints before truncation
      val (versionsBefore, checkpointsBefore) = countTransactionLogFiles(tablePath)
      println(s"📊 Before truncation: $versionsBefore version files, $checkpointsBefore checkpoint files")
      versionsBefore should be >= 21
      checkpointsBefore should be >= 2 // Should have checkpoints at v10 and v20

      // Get data state before truncation
      val dataBefore = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)
      val countBefore = dataBefore.count()
      val sumBefore   = dataBefore.agg(Map("amount" -> "sum")).collect().head.getDouble(0)
      val idsBefore   = dataBefore.select("id").collect().map(_.getInt(0)).sorted

      println(s"📊 Data before truncation: count=$countBefore, sum(amount)=$sumBefore")
      countBefore shouldBe 21
      sumBefore shouldBe 23100.0 // sum of 100+200+...+2100 = 100*(1+2+...+21) = 100*231 = 23100

      // Truncate time travel
      val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'")
      val row    = result.collect().head

      println(s"📋 Truncation result:")
      println(s"   - status: ${row.getString(1)}")
      println(s"   - checkpoint_version: ${row.getLong(2)}")
      println(s"   - versions_deleted: ${row.getLong(3)}")
      println(s"   - checkpoints_deleted: ${row.getLong(4)}")

      row.getString(1) shouldBe "SUCCESS"
      row.getLong(3) should be >= 20L // Should delete versions 0-19 or 0-20
      row.getLong(4) should be >= 1L  // Should delete at least checkpoint at v10

      // Verify transaction log state after truncation
      val (versionsAfter, checkpointsAfter) = countTransactionLogFiles(tablePath)
      println(s"📊 After truncation: $versionsAfter version files, $checkpointsAfter checkpoint files")
      versionsAfter should be < versionsBefore
      checkpointsAfter shouldBe 1 // Only latest checkpoint should remain

      // CRITICAL: Verify ALL data is still readable after truncation
      val dataAfter = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      val countAfter = dataAfter.count()
      val sumAfter   = dataAfter.agg(Map("amount" -> "sum")).collect().head.getDouble(0)
      val idsAfter   = dataAfter.select("id").collect().map(_.getInt(0)).sorted

      println(s"📊 Data after truncation: count=$countAfter, sum(amount)=$sumAfter")

      // Verify exact data integrity
      countAfter shouldBe countBefore
      sumAfter shouldBe sumBefore
      idsAfter shouldBe idsBefore

      // Verify each individual record is readable
      for (expectedId <- 1 to 21) {
        val record = dataAfter.filter(s"id = $expectedId").collect()
        record.length shouldBe 1
        record.head.getString(1) shouldBe s"value_$expectedId"
        record.head.getDouble(2) shouldBe (expectedId * 100.0)
      }

      println(s"✅ All 21 records verified individually after truncation")
      println(s"✅ Data integrity fully verified:")
      println(s"   - Count: before=$countBefore, after=$countAfter")
      println(s"   - Sum(amount): before=$sumBefore, after=$sumAfter")
      println(s"   - All IDs match: ${idsBefore.mkString(",")}")

    } finally
      // Restore checkpoint interval
      spark.conf.set("spark.indextables.checkpoint.interval", "5")
  }
}
