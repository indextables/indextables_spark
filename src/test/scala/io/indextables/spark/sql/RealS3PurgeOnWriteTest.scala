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

import org.apache.spark.sql.SaveMode

import org.apache.hadoop.fs.{FileSystem, Path}

import io.indextables.spark.purge.PurgeOnWriteTransactionCounter
import io.indextables.spark.RealS3TestBase

/**
 * Real AWS S3 integration tests for purge-on-write feature.
 *
 * Tests purge-on-write functionality specific to S3:
 *   - Trigger after N writes with S3 paths
 *   - Split file cleanup on S3
 *   - Transaction log cleanup on S3
 *   - Credential propagation from write options to purge executor
 *   - Uses Thread.sleep() with 0-hour retention since setTimes() doesn't work on S3
 *
 * Credentials are loaded from ~/.aws/credentials file.
 */
class RealS3PurgeOnWriteTest extends RealS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/purge-on-write-test-$testRunId"

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None
  private var fs: FileSystem                           = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    // Use JSON format since this test validates JSON transaction log purge behavior
    // With Avro state format (the new default), transaction log structure differs
    spark.conf.set("spark.indextables.state.format", "json")

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

  test("purge-on-write should trigger after N writes on S3") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val tablePath = s"$testBasePath/trigger_after_writes"

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
  }

  test("purge-on-write should clean up old orphaned split files on S3 using sleep") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val tablePath = s"$testBasePath/cleanup_splits_sleep"

    // Enable purge-on-write with 0-hour retention and disabled check (for testing with sleep)
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "2")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "0") // 0 hours
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")  // Disable check for testing

    val df = spark.range(50).toDF("id")

    // Write 1: creates split files
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
  }

  test("purge-on-write should clean up old transaction log files on S3 using sleep") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val tablePath              = s"$testBasePath/cleanup_txlog_sleep"
    val (accessKey, secretKey) = awsCredentials.get

    val df = spark.range(10).toDF("id")

    // Write 10 times to trigger checkpoint (default interval is 10) and create transaction log files 0-9
    (1 to 10).foreach { i =>
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) SaveMode.Overwrite else SaveMode.Append)
        .option("spark.indextables.aws.accessKey", accessKey)
        .option("spark.indextables.aws.secretKey", secretKey)
        .option("spark.indextables.aws.region", S3_REGION)
        .option("spark.indextables.checkpoint.enabled", "true") // Enable checkpoints
        .option("spark.indextables.checkpoint.interval", "10")  // Checkpoint every 10 writes
        .option("spark.indextables.purgeOnWrite.enabled", "true")
        .option("spark.indextables.purgeOnWrite.triggerAfterWrites", "12") // Trigger after 12 writes
        .option("spark.indextables.purgeOnWrite.splitRetentionHours", "0")
        .option("spark.indextables.purgeOnWrite.txLogRetentionHours", "0")
        .option("spark.indextables.purge.retentionCheckEnabled", "false")
        .save(tablePath)
    }

    val txLogPath = new Path(tablePath, "_transaction_log")

    // Verify checkpoint was created
    val checkpointFiles = fs.listStatus(txLogPath).filter(_.getPath.getName.contains("checkpoint"))
    assert(checkpointFiles.nonEmpty, "Checkpoint file should exist")

    // Verify early transaction log files exist
    assert(fs.exists(new Path(txLogPath, "00000000000000000000.json")))
    assert(fs.exists(new Path(txLogPath, "00000000000000000001.json")))

    // Wait 2 seconds for transaction logs to age
    println(s"⏳ Sleeping 2 seconds to age transaction logs...")
    Thread.sleep(2000)

    // Write 2 more times to reach triggerAfterWrites=12 and trigger purge
    (1 to 2).foreach { _ =>
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(SaveMode.Append)
        .option("spark.indextables.aws.accessKey", accessKey)
        .option("spark.indextables.aws.secretKey", secretKey)
        .option("spark.indextables.aws.region", S3_REGION)
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "10")
        .option("spark.indextables.purgeOnWrite.enabled", "true")
        .option("spark.indextables.purgeOnWrite.triggerAfterWrites", "12")
        .option("spark.indextables.purgeOnWrite.splitRetentionHours", "0")
        .option("spark.indextables.purgeOnWrite.txLogRetentionHours", "0")
        .option("spark.indextables.purge.retentionCheckEnabled", "false")
        .save(tablePath)
    }

    // Wait for purge to complete (purge runs asynchronously in background)
    println(s"⏳ Waiting 5 seconds for purge to complete on S3...")
    Thread.sleep(5000)

    // Verify old transaction log files before checkpoint are deleted
    // Checkpoint is at version 10, so versions 0-8 should be candidates for deletion
    assert(
      !fs.exists(new Path(txLogPath, "00000000000000000000.json")),
      "Old version 0 (before checkpoint) should be deleted"
    )
    assert(
      !fs.exists(new Path(txLogPath, "00000000000000000001.json")),
      "Old version 1 (before checkpoint) should be deleted"
    )

    // Verify recent files after checkpoint are kept
    assert(
      fs.exists(new Path(txLogPath, "00000000000000000010.json")) ||
        fs.exists(new Path(txLogPath, "00000000000000000011.json")),
      "Recent version (10 or 11) should be kept"
    )
  }

  test("purge-on-write should propagate S3 credentials from write options") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val tablePath = s"$testBasePath/credential_propagation"

    // Enable purge-on-write
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "1")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "24")

    val (accessKey, secretKey) = awsCredentials.get
    val df                     = spark.range(50).toDF("id")

    // Write with explicit S3 credentials - purge should inherit these
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .save(tablePath)

    // Should complete without errors (credential propagation successful)
    assert(fs.exists(new Path(tablePath, "_transaction_log")))

    // Verify data is readable
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    assert(result.count() === 50)
  }

  test("purge-on-write counter should be per-table on S3") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val table1Path = s"$testBasePath/table1"
    val table2Path = s"$testBasePath/table2"

    // Enable purge-on-write
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "5")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")

    val df = spark.range(50).toDF("id")

    // Write to table1 twice
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(table1Path)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(table1Path)

    // Write to table2 three times
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .save(table2Path)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(table2Path)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Append)
      .save(table2Path)

    // Verify separate counters
    assert(PurgeOnWriteTransactionCounter.get(table1Path) === 2)
    assert(PurgeOnWriteTransactionCounter.get(table2Path) === 3)
  }

  test("purge-on-write should trigger after merge-on-write with S3 credential propagation") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val tablePath = s"$testBasePath/merge_then_purge"

    val (accessKey, secretKey) = awsCredentials.get

    // Enable both merge-on-write and purge-on-write
    // Use small splits to trigger merge quickly
    val df = spark.range(100).toDF("id")

    // Write multiple times to create many small splits
    (1 to 5).foreach { i =>
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) SaveMode.Overwrite else SaveMode.Append)
        .option("spark.indextables.aws.accessKey", accessKey)
        .option("spark.indextables.aws.secretKey", secretKey)
        .option("spark.indextables.aws.region", S3_REGION)
        .option("spark.indextables.mergeOnWrite.enabled", "true")
        .option("spark.indextables.mergeOnWrite.targetSize", "100M")
        .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "1.0") // Low threshold
        .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")         // Low requirement for testing
        .option("spark.indextables.purgeOnWrite.enabled", "true")
        .option("spark.indextables.purgeOnWrite.triggerAfterMerge", "true")
        .option("spark.indextables.purgeOnWrite.splitRetentionHours", "0") // Delete immediately for testing
        .option("spark.indextables.purge.retentionCheckEnabled", "false")  // Disable safety check
        .save(tablePath)
    }

    // Wait for purge to complete (uses sleep since we can't control timing on S3)
    Thread.sleep(2000)

    // Verify data is still readable (merge+purge didn't corrupt data)
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(tablePath)

    // Should have 500 rows (100 * 5 writes)
    assert(result.count() === 500, "Data should be intact after merge+purge")

    println("✅ Merge-on-write + purge-on-write completed successfully with S3 credentials")
  }

  test("purge-on-write should handle merge-on-write credential propagation on S3") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val tablePath = s"$testBasePath/credential_merge_purge"

    val (accessKey, secretKey) = awsCredentials.get

    // Clear counter
    PurgeOnWriteTransactionCounter.clearAll()

    val df = spark.range(50).toDF("id")

    // Write 3 times with both merge and purge enabled, passing credentials via options
    (1 to 3).foreach { i =>
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) SaveMode.Overwrite else SaveMode.Append)
        .option("spark.indextables.aws.accessKey", accessKey)
        .option("spark.indextables.aws.secretKey", secretKey)
        .option("spark.indextables.aws.region", S3_REGION)
        .option("spark.indextables.mergeOnWrite.enabled", "true")
        .option("spark.indextables.mergeOnWrite.targetSize", "50M")
        .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "1.0")
        .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
        .option("spark.indextables.purgeOnWrite.enabled", "true")
        .option("spark.indextables.purgeOnWrite.triggerAfterMerge", "true")
        .option("spark.indextables.purgeOnWrite.triggerAfterWrites", "0") // Only trigger after merge
        .option("spark.indextables.purgeOnWrite.splitRetentionHours", "24")
        .save(tablePath)
    }

    // Verify data integrity
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(tablePath)

    assert(result.count() === 150, "Data should be intact with credential propagation through merge+purge")
    println("✅ Credential propagation verified for merge+purge on S3")
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
}
