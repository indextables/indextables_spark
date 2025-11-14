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
 * Real AWS S3 integration tests for PURGE INDEXTABLE command.
 *
 * Tests critical functionality specific to S3:
 *   - Modification time handling from S3 API
 *   - Recursive directory listing in S3
 *   - Retention period filtering with S3 modification times
 *   - Deletion operations on S3 objects
 *   - Partitioned table handling in S3
 *
 * Credentials are loaded from ~/.aws/credentials file.
 */
class RealS3PurgeIndexTableTest extends RealS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/purge-test-$testRunId"

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

  test("PURGE INDEXTABLE should receive and use S3 modification times correctly") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/mod_time_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test1"), (2, "test2"), (3, "test3")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned files - S3 will set modification time to current time
    val orphan1 = new Path(s"$tablePath/orphan1_${UUID.randomUUID()}.split")
    val orphan2 = new Path(s"$tablePath/orphan2_${UUID.randomUUID()}.split")

    fs.create(orphan1).close()
    fs.create(orphan2).close()

    // Verify we can READ modification times from S3
    val orphan1Status = fs.getFileStatus(orphan1)
    val orphan2Status = fs.getFileStatus(orphan2)

    println(s"📅 Orphan1 S3 modification time: ${new java.util.Date(orphan1Status.getModificationTime)}")
    println(s"📅 Orphan2 S3 modification time: ${new java.util.Date(orphan2Status.getModificationTime)}")

    // Verify modification times are recent (within last minute)
    val now = System.currentTimeMillis()
    assert(
      math.abs(now - orphan1Status.getModificationTime) < 60000,
      s"S3 modification time should be recent, got ${orphan1Status.getModificationTime}"
    )

    // Purge with 24 hour retention (minimum) - files are too recent to delete
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS").collect()
    val metrics = result(0).getStruct(1)

    // Verify purge found orphaned files but didn't delete them (too recent)
    assert(metrics.getLong(1) == 2, s"Expected 2 orphaned files found, got ${metrics.getLong(1)}")
    assert(
      metrics.getLong(2) == 0,
      s"Expected 0 files deleted (too recent), got ${metrics.getLong(2)}"
    )

    // Verify both files still exist (not deleted due to retention)
    assert(fs.exists(orphan1), "Recent orphan1 should be kept")
    assert(fs.exists(orphan2), "Recent orphan2 should be kept")

    // Verify table data is intact
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 3, "Table should still have 3 rows")

    println("✅ S3 modification times correctly received and used for retention filtering")
  }

  test("PURGE INDEXTABLE should handle S3 partitioned tables correctly") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/partitioned_test"

    // Write partitioned data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      (1, "Alice", "2024-01-01"),
      (2, "Bob", "2024-01-01"),
      (3, "Charlie", "2024-01-02")
    ).toDF("id", "name", "date")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("date")
      .mode("overwrite")
      .save(tablePath)

    // Create orphaned files in different S3 partitions
    val orphan1 = new Path(s"$tablePath/date=2024-01-01/orphaned_${UUID.randomUUID()}.split")
    val orphan2 = new Path(s"$tablePath/date=2024-01-02/orphaned_${UUID.randomUUID()}.split")

    fs.create(orphan1).close()
    fs.create(orphan2).close()

    // Verify we can read modification times from S3 for files in partitions
    val orphan1Status = fs.getFileStatus(orphan1)
    val orphan2Status = fs.getFileStatus(orphan2)

    println(s"📂 Created orphaned files in S3 partitions:")
    println(s"   - $orphan1 (modified: ${new java.util.Date(orphan1Status.getModificationTime)})")
    println(s"   - $orphan2 (modified: ${new java.util.Date(orphan2Status.getModificationTime)})")

    // Verify modification times are recent
    val now = System.currentTimeMillis()
    assert(
      math.abs(now - orphan1Status.getModificationTime) < 60000,
      "S3 modification time should be recent for partition file"
    )

    // Use DRY RUN to verify recursive listing finds files in partitions
    // Note: S3 doesn't support setTimes(), so we can't test with old files
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    // Verify both orphaned files were found via recursive S3 listing in partitions
    assert(metrics.getString(0) == "DRY_RUN", "Status should be DRY_RUN")
    assert(metrics.getLong(1) == 2, s"Expected 2 orphaned files found in S3 partitions, got ${metrics.getLong(1)}")

    // Verify both files still exist (DRY RUN doesn't delete)
    assert(fs.exists(orphan1), "Orphan 1 should still exist in S3 after DRY RUN")
    assert(fs.exists(orphan2), "Orphan 2 should still exist in S3 after DRY RUN")

    // Verify table is still intact
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 3, "Table should still have 3 rows")

    println("✅ S3 partitioned table recursive listing works correctly")
  }

  test("PURGE INDEXTABLE should handle S3 recursive listing correctly") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/recursive_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned files at different S3 path depths
    val rootOrphan   = new Path(s"$tablePath/orphaned_root_${UUID.randomUUID()}.split")
    val nestedOrphan = new Path(s"$tablePath/nested/dir/orphaned_nested_${UUID.randomUUID()}.split")

    // Create nested directory
    fs.mkdirs(nestedOrphan.getParent)

    fs.create(rootOrphan).close()
    fs.create(nestedOrphan).close()

    // Verify we can read modification times from S3 at different depths
    val rootStatus   = fs.getFileStatus(rootOrphan)
    val nestedStatus = fs.getFileStatus(nestedOrphan)

    println(s"📁 Created orphaned files at different S3 depths:")
    println(s"   - Root: $rootOrphan (modified: ${new java.util.Date(rootStatus.getModificationTime)})")
    println(s"   - Nested: $nestedOrphan (modified: ${new java.util.Date(nestedStatus.getModificationTime)})")

    // Verify modification times are recent
    val now = System.currentTimeMillis()
    assert(
      math.abs(now - rootStatus.getModificationTime) < 60000,
      "S3 modification time should be recent for root file"
    )
    assert(
      math.abs(now - nestedStatus.getModificationTime) < 60000,
      "S3 modification time should be recent for nested file"
    )

    // Use DRY RUN to verify recursive listing finds files at all depths
    // Note: S3 doesn't support setTimes(), so we can't test with old files
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    // Verify both files were found via recursive S3 listing
    assert(metrics.getString(0) == "DRY_RUN", "Status should be DRY_RUN")
    assert(
      metrics.getLong(1) == 2,
      s"Expected 2 files found via recursive S3 listing, got ${metrics.getLong(1)}"
    )

    // Verify both files still exist (DRY RUN doesn't delete)
    assert(fs.exists(rootOrphan), "Root orphan should still exist after DRY RUN")
    assert(fs.exists(nestedOrphan), "Nested orphan should still exist after DRY RUN")

    println("✅ S3 recursive listing works correctly at all path depths")
  }

  test("PURGE INDEXTABLE DRY RUN should not delete files from S3") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/dry_run_test"

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned file in S3
    val orphan = new Path(s"$tablePath/orphaned_${UUID.randomUUID()}.split")
    fs.create(orphan).close()

    // Verify we can read modification time from S3
    val orphanStatus = fs.getFileStatus(orphan)
    println(s"📄 Created orphaned file in S3: $orphan (modified: ${new java.util.Date(orphanStatus.getModificationTime)})")

    // Verify modification time is recent
    val now = System.currentTimeMillis()
    assert(
      math.abs(now - orphanStatus.getModificationTime) < 60000,
      "S3 modification time should be recent"
    )

    // Run DRY RUN purge with 24 hour retention (file is too recent)
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    // Verify DRY RUN found the orphan but didn't delete it from S3
    assert(metrics.getString(0) == "DRY_RUN", "Status should be DRY_RUN")
    assert(metrics.getLong(1) == 1, "Should find 1 orphaned file")
    assert(metrics.getLong(2) == 0, "Should delete 0 files in DRY RUN")

    // Verify file still exists in S3
    assert(fs.exists(orphan), "Orphaned file should still exist in S3 after DRY RUN")

    println("✅ S3 DRY RUN mode works correctly")
  }

  test("PURGE INDEXTABLE should delete old files from S3 using short retention and sleep") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/short_retention_test"

    // Set very short retention period (1 second = 1000ms)
    spark.conf.set("spark.indextables.logRetention.duration", "1000")

    // Write data
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq((1, "test1"), (2, "test2"), (3, "test3")).toDF("id", "value")
    data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Create orphaned files in S3
    val orphan1 = new Path(s"$tablePath/orphan1_${UUID.randomUUID()}.split")
    val orphan2 = new Path(s"$tablePath/orphan2_${UUID.randomUUID()}.split")

    fs.create(orphan1).close()
    fs.create(orphan2).close()

    // Verify files were created and have recent modification times
    val orphan1Status = fs.getFileStatus(orphan1)
    val orphan2Status = fs.getFileStatus(orphan2)

    println(s"📅 Created orphaned files in S3:")
    println(s"   - $orphan1 (modified: ${new java.util.Date(orphan1Status.getModificationTime)})")
    println(s"   - $orphan2 (modified: ${new java.util.Date(orphan2Status.getModificationTime)})")

    // Wait 2 seconds for files to exceed 1-second retention period
    println(s"⏳ Sleeping 2 seconds to age files past 1-second retention period...")
    Thread.sleep(2000)

    // Purge with OLDER THAN 1 HOURS (files are 2+ seconds old, which is > 1 hour)
    // NOTE: The retention period check compares file age against the HOURS parameter
    // Using 1 HOURS means files must be older than 1 hour, but our files are only 2 seconds old
    // Let's use a very small value: OLDER THAN 0 HOURS means delete immediately if age > 0
    // Actually, minimum is enforced, so we need to disable the check
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")
    val result = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 0 HOURS").collect()
    val metrics = result(0).getStruct(1)

    println(s"📊 Purge results:")
    println(s"   - Found: ${metrics.getLong(1)}")
    println(s"   - Deleted: ${metrics.getLong(2)}")

    // Verify purge found and deleted both orphaned files
    assert(metrics.getLong(1) == 2, s"Expected 2 orphaned files found, got ${metrics.getLong(1)}")
    assert(metrics.getLong(2) == 2, s"Expected 2 files deleted (aged past 1 second), got ${metrics.getLong(2)}")

    // Verify both files no longer exist in S3
    assert(!fs.exists(orphan1), "Old orphan1 should be deleted from S3")
    assert(!fs.exists(orphan2), "Old orphan2 should be deleted from S3")

    // Verify table data is intact
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 3, "Table should still have 3 rows")

    println("✅ S3 purge with short retention and sleep successfully deleted old files")
  }
}
