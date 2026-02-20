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

import java.util.{Properties, UUID}

import scala.util.Using

import org.apache.spark.sql.SaveMode

import org.apache.hadoop.fs.{FileSystem, Path}

import io.indextables.spark.purge.PurgeOnWriteTransactionCounter
import io.indextables.spark.CloudS3TestBase

/**
 * Real AWS S3 integration tests for purge-on-write feature with Avro state format.
 *
 * Tests purge-on-write functionality specific to S3 with Avro:
 *   - Trigger after N writes with S3 paths and Avro state directories
 *   - Split file cleanup on S3 with Avro format
 *   - State directory preservation during purge
 *   - Credential propagation from write options to purge executor
 *
 * Credentials are loaded from ~/.aws/credentials file.
 */
class CloudS3AvroPurgeOnWriteTest extends CloudS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/avro-purge-on-write-test-$testRunId"

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None
  private var fs: FileSystem                           = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    // Use Avro format (the new default) - this is the key difference from JSON test
    spark.conf.set("spark.indextables.state.format", "avro")

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

      // Create FileSystem instance
      fs = new Path(testBasePath).getFileSystem(hadoopConf)

      println(s"✅ AWS credentials loaded successfully")
      println(s"✅ Test base path: $testBasePath")
      println(s"✅ Using Avro state format")
    } else {
      println("⚠️  No AWS credentials found - tests will be skipped")
    }
  }

  override def afterAll(): Unit =
    try
      // Cleanup test directory if it exists
      if (awsCredentials.isDefined && fs != null) {
        val basePath = new Path(testBasePath)
        if (fs.exists(basePath)) {
          println(s"🧹 Cleaning up S3 test directory: $testBasePath")
          fs.delete(basePath, true)
        }
      }
    finally
      super.afterAll()

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Clear all transaction counters before each test
    PurgeOnWriteTransactionCounter.clearAll()
  }

  test("Avro: purge-on-write should trigger after N writes on S3") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val tablePath = s"$testBasePath/avro_trigger_after_writes"

    // Enable purge-on-write with triggerAfterWrites=3
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "3")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "24")
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "true")

    val df = spark.range(50).toDF("id")

    // Write 1: counter = 1
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(tablePath)
    assert(PurgeOnWriteTransactionCounter.get(tablePath) === 1)

    // Write 2: counter = 2
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(tablePath)
    assert(PurgeOnWriteTransactionCounter.get(tablePath) === 2)

    // Write 3: counter should reset to 0 after purge triggers
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(tablePath)
    assert(
      PurgeOnWriteTransactionCounter.get(tablePath) === 0,
      "Counter should reset to 0 after purge triggers on 3rd write"
    )

    // Verify Avro state directory exists
    val txLogPath = new Path(tablePath, "_transaction_log")
    val stateDirs = fs.listStatus(txLogPath).filter(_.getPath.getName.startsWith("state-v"))
    assert(stateDirs.nonEmpty, "Avro state directory should exist")
  }

  test("Avro: purge-on-write should clean up old orphaned split files on S3") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val tablePath = s"$testBasePath/avro_cleanup_splits"

    // Enable purge-on-write with 0-hour retention and disabled check (for testing)
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "2")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "0") // 0 hours
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")  // Disable check for testing

    val df = spark.range(50).toDF("id")

    // Write 1: creates split files with Avro state
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Create orphaned split files on S3
    val orphan1 = new Path(tablePath, s"orphan1_${UUID.randomUUID()}.split")
    val orphan2 = new Path(tablePath, s"orphan2_${UUID.randomUUID()}.split")
    fs.create(orphan1).close()
    fs.create(orphan2).close()

    assert(fs.exists(orphan1), "Orphan1 should exist before purge")
    assert(fs.exists(orphan2), "Orphan2 should exist before purge")

    // Wait 2 seconds for files to age
    println(s"⏳ Sleeping 2 seconds to age files past 0-hour retention period...")
    Thread.sleep(2000)

    // Write 2: should trigger purge and clean up old orphaned files
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Verify orphaned files are deleted
    assert(!fs.exists(orphan1), "Orphan1 should be deleted after purge")
    assert(!fs.exists(orphan2), "Orphan2 should be deleted after purge")

    // Verify Avro state directory is preserved
    val txLogPath = new Path(tablePath, "_transaction_log")
    val stateDirs = fs.listStatus(txLogPath).filter(_.getPath.getName.startsWith("state-v"))
    assert(stateDirs.nonEmpty, "Avro state directory should be preserved after purge")
  }

  test("Avro: purge-on-write should preserve state directories during cleanup") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val tablePath              = s"$testBasePath/avro_preserve_state"
    val (accessKey, secretKey) = awsCredentials.get

    val df = spark.range(10).toDF("id")

    // Write 8 times to trigger checkpoint (interval=5) and create Avro state directories
    (1 to 8).foreach { i =>
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) SaveMode.Overwrite else SaveMode.Append)
        .option("spark.indextables.aws.accessKey", accessKey)
        .option("spark.indextables.aws.secretKey", secretKey)
        .option("spark.indextables.aws.region", S3_REGION)
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "5")
        .option("spark.indextables.purgeOnWrite.enabled", "true")
        .option("spark.indextables.purgeOnWrite.triggerAfterWrites", "10") // Trigger after 10 writes
        .option("spark.indextables.purgeOnWrite.splitRetentionHours", "0")
        .option("spark.indextables.purge.retentionCheckEnabled", "false")
        .save(tablePath)
    }

    val txLogPath = new Path(tablePath, "_transaction_log")

    // Verify Avro state directory was created
    val stateDirectories = fs.listStatus(txLogPath).filter(_.getPath.getName.startsWith("state-v"))
    assert(stateDirectories.nonEmpty, "Avro state directory should exist after checkpoint")
    println(s"✅ Found ${stateDirectories.length} Avro state directories")

    // Wait 2 seconds for state to age
    println(s"⏳ Sleeping 2 seconds...")
    Thread.sleep(2000)

    // Write 2 more times to reach triggerAfterWrites=10 and trigger purge
    (1 to 2).foreach { _ =>
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(SaveMode.Append)
        .option("spark.indextables.aws.accessKey", accessKey)
        .option("spark.indextables.aws.secretKey", secretKey)
        .option("spark.indextables.aws.region", S3_REGION)
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "5")
        .option("spark.indextables.purgeOnWrite.enabled", "true")
        .option("spark.indextables.purgeOnWrite.triggerAfterWrites", "10")
        .option("spark.indextables.purgeOnWrite.splitRetentionHours", "0")
        .option("spark.indextables.purge.retentionCheckEnabled", "false")
        .save(tablePath)
    }

    // Verify Avro state directories are preserved (latest should always be kept)
    val stateDirectoriesAfter = fs.listStatus(txLogPath).filter(_.getPath.getName.startsWith("state-v"))
    assert(stateDirectoriesAfter.nonEmpty, "At least one Avro state directory should be preserved after purge")
    println(s"✅ ${stateDirectoriesAfter.length} Avro state directories preserved after purge")

    // Verify data integrity
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    val count = result.count()
    assert(count === 100, s"Expected 100 records (10 writes × 10 each), got $count")
    println(s"✅ Data integrity verified: $count records")
  }

  /** Load AWS credentials from ~/.aws/credentials file. */
  private def loadAwsCredentials(): Option[(String, String)] =
    try {
      val home     = System.getProperty("user.home")
      val credFile = new java.io.File(s"$home/.aws/credentials")

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
}
