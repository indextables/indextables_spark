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
import org.apache.spark.sql.SaveMode

import io.indextables.spark.RealS3TestBase
import io.indextables.spark.purge.PurgeOnWriteTransactionCounter

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

  override def afterAll(): Unit = {
    try {
      // Cleanup test directory if it exists
      if (awsCredentials.isDefined && fs != null) {
        val basePath = new Path(testBasePath)
        if (fs.exists(basePath)) {
          println(s"🧹 Cleaning up S3 test directory: $testBasePath")
          fs.delete(basePath, true)
        }
      }
    } finally {
      super.afterAll()
    }
  }

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
    assert(PurgeOnWriteTransactionCounter.get(tablePath) === 0,
      "Counter should reset to 0 after purge triggers on 3rd write")
  }

  test("purge-on-write should clean up old orphaned split files on S3 using sleep") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 tests")

    val tablePath = s"$testBasePath/cleanup_splits_sleep"

    // Enable purge-on-write with 0-hour retention and disabled check (for testing with sleep)
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "2")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "0") // 0 hours
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false") // Disable check for testing

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

    val tablePath = s"$testBasePath/cleanup_txlog_sleep"

    // Enable purge-on-write with 0-hour retention for testing
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "5")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "0")
    spark.conf.set("spark.indextables.purgeOnWrite.txLogRetentionHours", "0")
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")

    val df = spark.range(10).toDF("id")

    // Write 3 times to create transaction log files 0-2
    (1 to 3).foreach { i =>
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) SaveMode.Overwrite else SaveMode.Append)
        .save(tablePath)
    }

    val txLogPath = new Path(tablePath, "_transaction_log")

    // Verify files 0-2 exist
    assert(fs.exists(new Path(txLogPath, "00000000000000000000.json")))
    assert(fs.exists(new Path(txLogPath, "00000000000000000001.json")))
    assert(fs.exists(new Path(txLogPath, "00000000000000000002.json")))

    // Wait 2 seconds for transaction logs to age
    println(s"⏳ Sleeping 2 seconds to age transaction logs...")
    Thread.sleep(2000)

    // Write 2 more times to reach triggerAfterWrites=5 and trigger purge
    (1 to 2).foreach { _ =>
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(SaveMode.Append)
        .save(tablePath)
    }

    // Verify old transaction log files 0-1 are deleted (keeping recent 2-4)
    assert(!fs.exists(new Path(txLogPath, "00000000000000000000.json")),
      "Old version 0 should be deleted")
    assert(!fs.exists(new Path(txLogPath, "00000000000000000001.json")),
      "Old version 1 should be deleted")

    // Verify recent files are kept
    assert(fs.exists(new Path(txLogPath, "00000000000000000004.json")),
      "Recent version 4 should be kept")
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
    val df = spark.range(50).toDF("id")

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
