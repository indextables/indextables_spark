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
import io.indextables.spark.RealAzureTestBase

/**
 * Real Azure Blob Storage integration tests for purge-on-write feature.
 *
 * Tests purge-on-write functionality specific to Azure:
 *   - Trigger after N writes with Azure paths
 *   - Split file cleanup on Azure Blob Storage
 *   - Transaction log cleanup on Azure
 *   - Credential propagation from write options to purge executor
 *   - Uses Thread.sleep() with 0-hour retention since setTimes() doesn't work on Azure
 *
 * Credentials are loaded from ~/.azure/credentials file.
 */
class RealAzurePurgeOnWriteTest extends RealAzureTestBase {

  private val AZURE_STORAGE_ACCOUNT = "indextablestesting"
  private val AZURE_CONTAINER       = "test-container"
  private val AZURE_BASE_PATH       = s"azure://$AZURE_CONTAINER"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$AZURE_BASE_PATH/purge-on-write-test-$testRunId"

  // Azure credentials loaded from ~/.azure/credentials
  private var azureCredentials: Option[(String, String)] = None
  private var fs: FileSystem                             = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load Azure credentials from ~/.azure/credentials
    azureCredentials = loadAzureCredentials()

    if (azureCredentials.isDefined) {
      val (accountName, accountKey) = azureCredentials.get

      // Configure Hadoop config FIRST before creating FileSystem
      val hadoopConf = spark.sparkContext.hadoopConfiguration

      // Set standard Azure properties for Hadoop FileSystem
      hadoopConf.set(s"fs.azure.account.key.$accountName.blob.core.windows.net", accountKey)

      // Also set indextables properties for CloudStorageProvider
      hadoopConf.set("spark.indextables.azure.accountName", accountName)
      hadoopConf.set("spark.indextables.azure.accountKey", accountKey)

      // Configure Spark for real Azure access
      spark.conf.set("spark.indextables.azure.accountName", accountName)
      spark.conf.set("spark.indextables.azure.accountKey", accountKey)

      // Create FileSystem instance
      fs = new Path(testBasePath).getFileSystem(hadoopConf)

      println(s"✅ Azure credentials loaded successfully")
      println(s"✅ Test base path: $testBasePath")
    } else {
      println("⚠️  No Azure credentials found - tests will be skipped")
    }
  }

  override def afterAll(): Unit =
    try
      // Cleanup test directory if it exists
      if (azureCredentials.isDefined && fs != null) {
        val basePath = new Path(testBasePath)
        if (fs.exists(basePath)) {
          println(s"🧹 Cleaning up Azure test directory: $testBasePath")
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

  test("purge-on-write should trigger after N writes on Azure") {
    assume(azureCredentials.isDefined, "Azure credentials required for Azure tests")

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

  test("purge-on-write should clean up old orphaned split files on Azure using sleep") {
    assume(azureCredentials.isDefined, "Azure credentials required for Azure tests")

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

    // Create orphaned split files on Azure
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

  test("purge-on-write should clean up old transaction log files on Azure using sleep") {
    assume(azureCredentials.isDefined, "Azure credentials required for Azure tests")

    val tablePath                 = s"$testBasePath/cleanup_txlog_sleep"
    val (accountName, accountKey) = azureCredentials.get

    val df = spark.range(10).toDF("id")

    // Write 10 times to trigger checkpoint (default interval is 10) and create transaction log files 0-9
    (1 to 10).foreach { i =>
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) SaveMode.Overwrite else SaveMode.Append)
        .option("spark.indextables.azure.accountName", accountName)
        .option("spark.indextables.azure.accountKey", accountKey)
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
        .option("spark.indextables.azure.accountName", accountName)
        .option("spark.indextables.azure.accountKey", accountKey)
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
    println(s"⏳ Waiting 5 seconds for purge to complete on Azure...")
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

  test("purge-on-write should propagate Azure credentials from write options") {
    assume(azureCredentials.isDefined, "Azure credentials required for Azure tests")

    val tablePath = s"$testBasePath/credential_propagation"

    // Enable purge-on-write
    spark.conf.set("spark.indextables.purgeOnWrite.enabled", "true")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterWrites", "1")
    spark.conf.set("spark.indextables.purgeOnWrite.triggerAfterMerge", "false")
    spark.conf.set("spark.indextables.purgeOnWrite.splitRetentionHours", "24")

    val (accountName, accountKey) = azureCredentials.get
    val df                        = spark.range(50).toDF("id")

    // Write with explicit Azure credentials - purge should inherit these
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode(SaveMode.Overwrite)
      .option("spark.indextables.azure.accountName", accountName)
      .option("spark.indextables.azure.accountKey", accountKey)
      .save(tablePath)

    // Should complete without errors (credential propagation successful)
    assert(fs.exists(new Path(tablePath, "_transaction_log")))

    // Verify data is readable
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    assert(result.count() === 50)
  }

  test("purge-on-write counter should be per-table on Azure") {
    assume(azureCredentials.isDefined, "Azure credentials required for Azure tests")

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

  test("purge-on-write should trigger after merge-on-write with Azure credential propagation") {
    assume(azureCredentials.isDefined, "Azure credentials required for Azure tests")

    val tablePath                 = s"$testBasePath/merge_then_purge"
    val (accountName, accountKey) = azureCredentials.get
    val df                        = spark.range(100).toDF("id")

    // Write multiple times to create many small splits
    // This should trigger merge-on-write, and then purge-on-write after merge completes
    (1 to 5).foreach { i =>
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) SaveMode.Overwrite else SaveMode.Append)
        .option("spark.indextables.azure.accountName", accountName)
        .option("spark.indextables.azure.accountKey", accountKey)
        .option("spark.indextables.mergeOnWrite.enabled", "true")
        .option("spark.indextables.mergeOnWrite.targetSize", "100M")
        .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "1.0") // Low threshold
        .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
        .option("spark.indextables.purgeOnWrite.enabled", "true")
        .option("spark.indextables.purgeOnWrite.triggerAfterMerge", "true")
        .option("spark.indextables.purgeOnWrite.splitRetentionHours", "0") // Immediate deletion
        .option("spark.indextables.purge.retentionCheckEnabled", "false")
        .save(tablePath)
    }

    // Wait for purge to complete (Azure can be slower than S3)
    Thread.sleep(2000)

    // Verify data integrity - all writes should be preserved
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.azure.accountName", accountName)
      .option("spark.indextables.azure.accountKey", accountKey)
      .load(tablePath)

    assert(result.count() === 500, "Data should be intact after merge+purge on Azure")
  }

  test("purge-on-write should handle merge-on-write credential propagation on Azure") {
    assume(azureCredentials.isDefined, "Azure credentials required for Azure tests")

    val tablePath                 = s"$testBasePath/credential_merge_purge"
    val (accountName, accountKey) = azureCredentials.get

    PurgeOnWriteTransactionCounter.clearAll()
    val df = spark.range(50).toDF("id")

    // Write 3 times with both merge and purge enabled
    // This validates that Azure credentials flow through:
    // write → merge-on-write → purge-on-write
    (1 to 3).foreach { i =>
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) SaveMode.Overwrite else SaveMode.Append)
        .option("spark.indextables.azure.accountName", accountName)
        .option("spark.indextables.azure.accountKey", accountKey)
        .option("spark.indextables.mergeOnWrite.enabled", "true")
        .option("spark.indextables.mergeOnWrite.targetSize", "50M")
        .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "1.0")
        .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
        .option("spark.indextables.purgeOnWrite.enabled", "true")
        .option("spark.indextables.purgeOnWrite.triggerAfterMerge", "true")
        .option("spark.indextables.purgeOnWrite.triggerAfterWrites", "0")
        .option("spark.indextables.purgeOnWrite.splitRetentionHours", "24")
        .save(tablePath)
    }

    // Verify data integrity - credentials propagated correctly through entire chain
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.azure.accountName", accountName)
      .option("spark.indextables.azure.accountKey", accountKey)
      .load(tablePath)

    assert(result.count() === 150, "Data should be intact with credential propagation through merge+purge on Azure")
  }

  /** Load Azure credentials from ~/.azure/credentials file. */
  private def loadAzureCredentials(): Option[(String, String)] =
    try {
      val home     = System.getProperty("user.home")
      val credFile = new File(s"$home/.azure/credentials")

      if (!credFile.exists()) {
        println(s"Azure credentials file not found at: ${credFile.getAbsolutePath}")
        return None
      }

      Using(new java.io.FileInputStream(credFile)) { fis =>
        val props = new Properties()
        props.load(fis)

        val accountName = props.getProperty("account_name")
        val accountKey  = props.getProperty("account_key")

        if (accountName != null && accountKey != null) {
          Some((accountName, accountKey))
        } else {
          println(s"Azure credentials not found in ~/.azure/credentials")
          None
        }
      }.toOption.flatten
    } catch {
      case ex: Exception =>
        println(s"Failed to read Azure credentials: ${ex.getMessage}")
        None
    }
}
