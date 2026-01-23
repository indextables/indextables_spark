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
 * Real AWS S3 integration tests for PURGE INDEXTABLE command with Avro state format.
 *
 * Tests critical functionality specific to S3 with Avro:
 *   - Modification time handling from S3 API with Avro state directories
 *   - State directory preservation during purge
 *   - Orphaned split file cleanup
 *   - Partitioned table handling with Avro format
 *
 * Credentials are loaded from ~/.aws/credentials file.
 */
class RealS3AvroPurgeIndexTableTest extends RealS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/avro-purge-test-$testRunId"

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

  test("Avro: PURGE INDEXTABLE should preserve Avro state directories") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/avro_state_preservation"

    // Write data multiple times to trigger checkpoint
    val sparkSession = spark
    import sparkSession.implicits._

    // Write 6 times to trigger checkpoint (interval=5)
    (1 to 6).foreach { i =>
      val data = Seq((i, s"test$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "5")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Verify Avro state directory was created
    val txLogPath = new Path(tablePath, "_transaction_log")
    val stateDirectories = fs.listStatus(txLogPath).filter(_.getPath.getName.startsWith("state-v"))
    assert(stateDirectories.nonEmpty, "Avro state directory should exist after checkpoint")
    println(s"✅ Found ${stateDirectories.length} Avro state directories on S3")

    // Create orphaned files
    val orphan1 = new Path(s"$tablePath/orphan1_${UUID.randomUUID()}.split")
    val orphan2 = new Path(s"$tablePath/orphan2_${UUID.randomUUID()}.split")
    fs.create(orphan1).close()
    fs.create(orphan2).close()

    // Run PURGE with DRY RUN to verify it finds orphans but doesn't touch state
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    println(s"📊 PURGE DRY RUN results: ${metrics.getLong(1)} orphaned files found")

    // Verify Avro state directories are still there
    val stateDirectoriesAfter = fs.listStatus(txLogPath).filter(_.getPath.getName.startsWith("state-v"))
    assert(
      stateDirectoriesAfter.length >= stateDirectories.length,
      "Avro state directories should be preserved after PURGE DRY RUN"
    )

    // Verify data integrity
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 6, s"Table should still have 6 rows, got ${afterRead.count()}")

    println("✅ Avro state directories preserved correctly during PURGE")
  }

  test("Avro: PURGE INDEXTABLE should handle orphaned files with Avro state") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/avro_orphan_cleanup"

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

    println("✅ Avro: S3 modification times correctly received and used for retention filtering")
  }

  test("Avro: PURGE INDEXTABLE should handle partitioned tables with Avro state") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/avro_partitioned_test"

    // Write partitioned data with checkpoint to trigger Avro state
    val sparkSession = spark
    import sparkSession.implicits._

    // Write 6 times to trigger checkpoint
    (1 to 6).foreach { i =>
      val data = Seq(
        (i, "Alice", "2024-01-01"),
        (i + 10, "Bob", "2024-01-02")
      ).toDF("id", "name", "date")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "5")
        .partitionBy("date")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Verify Avro state directory exists
    val txLogPath = new Path(tablePath, "_transaction_log")
    val stateDirectories = fs.listStatus(txLogPath).filter(_.getPath.getName.startsWith("state-v"))
    assert(stateDirectories.nonEmpty, "Avro state directory should exist")
    println(s"✅ Avro state directory created for partitioned table")

    // Create orphaned files in different S3 partitions
    val orphan1 = new Path(s"$tablePath/date=2024-01-01/orphaned_${UUID.randomUUID()}.split")
    val orphan2 = new Path(s"$tablePath/date=2024-01-02/orphaned_${UUID.randomUUID()}.split")

    fs.create(orphan1).close()
    fs.create(orphan2).close()

    println(s"📂 Created orphaned files in S3 partitions:")
    println(s"   - $orphan1")
    println(s"   - $orphan2")

    // Use DRY RUN to verify recursive listing finds files in partitions
    val result  = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS DRY RUN").collect()
    val metrics = result(0).getStruct(1)

    // Verify both orphaned files were found via recursive S3 listing in partitions
    assert(metrics.getString(0) == "DRY_RUN", "Status should be DRY_RUN")
    assert(metrics.getLong(1) == 2, s"Expected 2 orphaned files found in S3 partitions, got ${metrics.getLong(1)}")

    // Verify both files still exist (DRY RUN doesn't delete)
    assert(fs.exists(orphan1), "Orphan 1 should still exist in S3 after DRY RUN")
    assert(fs.exists(orphan2), "Orphan 2 should still exist in S3 after DRY RUN")

    // Verify Avro state is preserved
    val stateDirectoriesAfter = fs.listStatus(txLogPath).filter(_.getPath.getName.startsWith("state-v"))
    assert(stateDirectoriesAfter.nonEmpty, "Avro state directory should be preserved")

    // Verify table is still intact
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 12, s"Table should have 12 rows (6 writes × 2 rows), got ${afterRead.count()}")

    println("✅ Avro: S3 partitioned table with Avro state handled correctly")
  }

  test("Avro: PURGE should list Avro state directory files correctly") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/avro_state_listing"

    // Write data with checkpoint to create Avro state
    val sparkSession = spark
    import sparkSession.implicits._

    (1 to 6).foreach { i =>
      val data = Seq((i, s"test$i")).toDF("id", "value")
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "5")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Verify Avro state structure
    val txLogPath = new Path(tablePath, "_transaction_log")
    val allFiles = fs.listStatus(txLogPath)

    println(s"📁 Transaction log contents:")
    allFiles.foreach { status =>
      val name = status.getPath.getName
      val typeName = if (status.isDirectory) "DIR " else "FILE"
      println(s"   - [$typeName] $name")
    }

    // Verify state directory exists
    val stateDirectories = allFiles.filter(_.getPath.getName.startsWith("state-v"))
    assert(stateDirectories.nonEmpty, "Should have at least one Avro state directory")

    // Verify _last_checkpoint exists
    val lastCheckpoint = allFiles.find(_.getPath.getName == "_last_checkpoint")
    assert(lastCheckpoint.isDefined, "_last_checkpoint should exist")

    // Run PURGE DRY RUN - should not interfere with state structure
    val result = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 0 HOURS DRY RUN").collect()
    println(s"📊 PURGE DRY RUN completed")

    // Verify data integrity after purge planning
    val afterRead = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(afterRead.count() == 6, s"Table should still have 6 rows")

    println("✅ Avro state directory structure preserved during PURGE operations")
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
