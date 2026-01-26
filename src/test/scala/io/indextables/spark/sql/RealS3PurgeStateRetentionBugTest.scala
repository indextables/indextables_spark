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
 * Real AWS S3 test to reproduce the PURGE LOG RETENTION bug.
 *
 * Bug description: PURGE INDEXTABLE ignores LOG RETENTION timeframe and
 * aggressively deletes state directories before they expire. Fresh state
 * directories (< 1 minute old) are being reported as "expired" even with
 * 24 HOURS LOG RETENTION.
 *
 * Reproduction steps:
 *   1. Create multiple transactions/states on a table
 *   2. Run PURGE DRY RUN with LOG RETENTION 24 HOURS
 *   3. EXPECTED: 0 expired states found (all states are < 1 minute old)
 *   4. ACTUAL BUG: All but current version are reported as expired
 *
 * Credentials are loaded from ~/.aws/credentials file.
 */
class RealS3PurgeStateRetentionBugTest extends RealS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/purge-retention-bug-$testRunId"

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None
  private var fs: FileSystem                           = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    // Use Avro format (the new default) - this is where the bug is reported
    spark.conf.set("spark.indextables.state.format", "avro")
    spark.conf.set("spark.indextables.checkpoint.enabled", "true")
    spark.conf.set("spark.indextables.checkpoint.interval", "3")

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

      // Initialize FileSystem AFTER setting Hadoop config
      val testPath = new Path(testBasePath)
      fs = testPath.getFileSystem(hadoopConf)

      println(s"🔐 AWS credentials loaded successfully")
      println(s"🌊 Configured Spark for S3 access to bucket: $S3_BUCKET in region: $S3_REGION")
      println(s"📍 Test base path: $testBasePath")
      println(s"✅ Using Avro state format")
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

  test("S3 BUG REPRO: PURGE with 24 HOURS LOG RETENTION should NOT find fresh states as expired") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/retention_bug_test"

    val sparkSession = spark
    import sparkSession.implicits._

    // Step 1: Write data multiple times to create multiple state versions
    println("=== Step 1: Writing data in 12 batches to create multiple state versions ===")
    (1 to 12).foreach { i =>
      val data = Seq((i, s"record_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
      println(s"  Batch $i written")
    }

    // Step 2: List state directories and their modification times
    val txLogPath = new Path(s"$tablePath/_transaction_log")
    val txLogContents = fs.listStatus(txLogPath)

    val stateDirectories = txLogContents.filter(_.getPath.getName.startsWith("state-v"))
      .sortBy(_.getPath.getName)

    println(s"\n=== Step 2: State directories on S3 (all freshly created) ===")
    println(s"Found ${stateDirectories.length} state directories:")
    stateDirectories.foreach { status =>
      val modTime = new java.util.Date(status.getModificationTime)
      val ageMs = System.currentTimeMillis() - status.getModificationTime
      val ageMinutes = ageMs / 60000.0
      println(s"  - ${status.getPath.getName}: modified=$modTime, age=${ageMinutes.formatted("%.2f")} minutes")
    }

    // Verify all states are fresh (< 5 minutes old)
    val now = System.currentTimeMillis()
    val maxAgeMs = 5 * 60 * 1000  // 5 minutes
    stateDirectories.foreach { status =>
      val ageMs = now - status.getModificationTime
      assert(ageMs < maxAgeMs,
        s"State ${status.getPath.getName} should be fresh (< 5 min), but is ${ageMs / 60000.0} minutes old")
    }
    println(s"✓ All ${stateDirectories.length} state directories are fresh (< 5 minutes old)")

    // Step 3: Run PURGE DRY RUN with 24 HOURS LOG RETENTION
    println(s"\n=== Step 3: Running PURGE DRY RUN with 24 HOURS LOG RETENTION ===")
    val result = spark.sql(
      s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS TRANSACTION LOG RETENTION 24 HOURS DRY RUN"
    ).collect()

    val metrics = result(0).getStruct(1)
    val status = metrics.getString(0)
    val orphanedFilesFound = metrics.getLong(1)
    val expiredStatesFound = metrics.getLong(6)   // expired_states_found
    val expiredStatesDeleted = metrics.getLong(7) // expired_states_deleted
    val isDryRun = metrics.getBoolean(8)

    println(s"\nPURGE DRY RUN Results:")
    println(s"  status: $status")
    println(s"  orphaned_files_found: $orphanedFilesFound")
    println(s"  expired_states_found: $expiredStatesFound")
    println(s"  expired_states_deleted: $expiredStatesDeleted (should be 0 for DRY RUN)")
    println(s"  dry_run: $isDryRun")

    // Step 4: THE KEY ASSERTION - This is where the bug manifests
    // All states are < 1 minute old, but with 24 HOURS retention, NONE should be expired
    if (expiredStatesFound > 0) {
      println(s"\n❌ BUG REPRODUCED: Found $expiredStatesFound 'expired' state directories")
      println(s"   But all state directories are < 5 minutes old!")
      println(s"   With 24 HOURS LOG RETENTION, expected: 0 expired states")
      println(s"   State directories that would be incorrectly deleted:")
      stateDirectories.foreach { status =>
        val ageHours = (now - status.getModificationTime) / (60.0 * 60.0 * 1000.0)
        println(s"     - ${status.getPath.getName}: ${ageHours.formatted("%.4f")} hours old (< 24 hours!)")
      }
    }

    assert(expiredStatesFound == 0,
      s"BUG: PURGE found $expiredStatesFound 'expired' state directories, " +
      s"but all ${stateDirectories.length} state directories are < 5 minutes old and should NOT be expired " +
      s"with 24 HOURS LOG RETENTION. The LOG RETENTION parameter is being ignored!")

    println(s"\n✅ Test passed - no fresh states incorrectly marked as expired")
  }

  test("S3 BUG REPRO: Compare 0 HOURS vs 24 HOURS LOG RETENTION behavior") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/retention_compare_test"

    val sparkSession = spark
    import sparkSession.implicits._

    // Create multiple state versions
    println("=== Creating table with multiple state versions ===")
    (1 to 12).foreach { i =>
      val data = Seq((i, s"record_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Count state directories
    val txLogPath = new Path(s"$tablePath/_transaction_log")
    val stateDirectories = fs.listStatus(txLogPath)
      .filter(_.getPath.getName.startsWith("state-v"))
    println(s"Created ${stateDirectories.length} state directories")

    // Disable minimum retention check for 0-hour test
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")

    // Test 1: PURGE with 0 HOURS LOG RETENTION - should find expired states
    println(s"\n=== Test 1: PURGE DRY RUN with 0 HOURS LOG RETENTION ===")
    val result0h = spark.sql(
      s"PURGE INDEXTABLE '$tablePath' OLDER THAN 0 HOURS TRANSACTION LOG RETENTION 0 HOURS DRY RUN"
    ).collect()

    val metrics0h = result0h(0).getStruct(1)
    val expiredStates0h = metrics0h.getLong(6)
    println(s"  expired_states_found with 0 HOURS: $expiredStates0h")

    // With 0 hour retention, all but the latest state should be expired
    // (Correct behavior: should find expired states)
    assert(expiredStates0h > 0,
      s"With 0 HOURS retention, expected to find expired states, but found $expiredStates0h")
    println(s"  ✓ Correct: Found $expiredStates0h expired states with 0 HOURS retention")

    // Test 2: PURGE with 24 HOURS LOG RETENTION - should NOT find expired states
    println(s"\n=== Test 2: PURGE DRY RUN with 24 HOURS LOG RETENTION ===")
    val result24h = spark.sql(
      s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS TRANSACTION LOG RETENTION 24 HOURS DRY RUN"
    ).collect()

    val metrics24h = result24h(0).getStruct(1)
    val expiredStates24h = metrics24h.getLong(6)
    println(s"  expired_states_found with 24 HOURS: $expiredStates24h")

    // THE BUG: If expiredStates24h == expiredStates0h, the retention is being ignored
    if (expiredStates24h > 0 && expiredStates24h == expiredStates0h) {
      println(s"\n❌ BUG CONFIRMED: LOG RETENTION parameter is being IGNORED!")
      println(s"   0 HOURS retention found: $expiredStates0h expired states")
      println(s"   24 HOURS retention found: $expiredStates24h expired states")
      println(s"   These should be DIFFERENT - fresh states should not be expired with 24h retention")
    }

    assert(expiredStates24h == 0,
      s"BUG: With 24 HOURS LOG RETENTION, expected 0 expired states (all are fresh), " +
      s"but found $expiredStates24h. With 0 HOURS retention found $expiredStates0h. " +
      s"The LOG RETENTION parameter appears to be ignored!")

    // Re-enable retention check
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "true")

    println(s"\n✅ Test passed - LOG RETENTION correctly affects expired state count")
  }

  test("S3: Verify state directory modification times are being read correctly") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/modtime_debug_test"

    val sparkSession = spark
    import sparkSession.implicits._

    // Create state directories
    println("=== Creating state directories ===")
    (1 to 6).foreach { i =>
      val data = Seq((i, s"record_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Read modification times from S3
    val txLogPath = new Path(s"$tablePath/_transaction_log")
    val stateDirectories = fs.listStatus(txLogPath)
      .filter(_.getPath.getName.startsWith("state-v"))
      .sortBy(_.getPath.getName)

    println(s"\n=== State directory modification times from S3 ===")
    val now = System.currentTimeMillis()
    val cutoffTime24h = now - (24L * 60 * 60 * 1000)

    var allFresh = true
    stateDirectories.foreach { status =>
      val modTime = status.getModificationTime
      val modDate = new java.util.Date(modTime)
      val ageHours = (now - modTime) / (60.0 * 60.0 * 1000.0)
      val isExpired24h = modTime < cutoffTime24h

      println(s"  ${status.getPath.getName}:")
      println(s"    modificationTime: $modTime ($modDate)")
      println(s"    age: ${ageHours.formatted("%.4f")} hours")
      println(s"    expired with 24h retention: $isExpired24h")

      if (isExpired24h) {
        allFresh = false
        println(s"    ⚠️  WARNING: This state shows as expired but was just created!")
      }
    }

    if (!allFresh) {
      println(s"\n❌ POSSIBLE BUG: Some fresh state directories show modification times > 24 hours ago")
      println(s"   This could indicate:")
      println(s"   1. S3 modification time is not being read correctly")
      println(s"   2. A bug in how we calculate the cutoff time")
      println(s"   3. Clock skew between local and S3")
    }

    // Verify modification times are reasonable (within last 5 minutes)
    // Allow for minor clock skew between local machine and S3 (up to 5 seconds in the future)
    stateDirectories.foreach { status =>
      val ageMs = now - status.getModificationTime
      val ageMinutes = ageMs / 60000.0
      assert(ageMs > -5000, s"Modification time should not be more than 5s in the future due to clock skew")
      assert(ageMs < 5 * 60 * 1000,  // 5 minutes
        s"State ${status.getPath.getName} should be fresh (< 5 min), " +
        s"but reports age of ${ageMinutes.formatted("%.2f")} minutes. " +
        s"modTime=${status.getModificationTime}, now=$now")
    }

    println(s"\n✅ All state directory modification times are correct and fresh")
  }

  test("S3 BUG REPRO: Immediate PURGE after write with various retention values") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/immediate_purge_test"

    val sparkSession = spark
    import sparkSession.implicits._

    // Create state directories
    println("=== Creating table with 6 writes ===")
    (1 to 6).foreach { i =>
      val data = Seq((i, s"record_$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Disable retention check
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")

    // Test various retention values
    val retentionHours = Seq(0, 1, 6, 12, 24, 168)

    println(s"\n=== Testing PURGE with different LOG RETENTION values ===")
    val results = retentionHours.map { hours =>
      val result = spark.sql(
        s"PURGE INDEXTABLE '$tablePath' OLDER THAN $hours HOURS TRANSACTION LOG RETENTION $hours HOURS DRY RUN"
      ).collect()

      val metrics = result(0).getStruct(1)
      val expiredStates = metrics.getLong(6)

      println(s"  LOG RETENTION $hours HOURS: expired_states_found = $expiredStates")
      (hours, expiredStates)
    }

    // Re-enable retention check
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "true")

    // Analyze results
    println(s"\n=== Analysis ===")
    val (zeroHourCount, _) = results.find(_._1 == 0).get
    val nonZeroResults = results.filter(_._1 > 0)

    // BUG CHECK: All non-zero retention values should return 0 expired states
    // (because all states are fresh)
    val buggyResults = nonZeroResults.filter(_._2 > 0)
    if (buggyResults.nonEmpty) {
      println(s"❌ BUG DETECTED: The following retention values incorrectly found expired states:")
      buggyResults.foreach { case (hours, count) =>
        println(s"   - $hours HOURS: found $count expired states (should be 0)")
      }
    }

    // With 0 hours, we should find expired states
    assert(results.find(_._1 == 0).get._2 > 0,
      "With 0 HOURS retention, should find expired states")

    // With >= 1 hour, we should NOT find expired states (all are fresh)
    nonZeroResults.foreach { case (hours, expiredCount) =>
      assert(expiredCount == 0,
        s"BUG: With $hours HOURS LOG RETENTION, expected 0 expired states " +
        s"(all states are < 1 minute old), but found $expiredCount")
    }

    println(s"\n✅ All retention values behave correctly")
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
