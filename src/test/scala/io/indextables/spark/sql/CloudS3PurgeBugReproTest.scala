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

import io.indextables.spark.CloudS3TestBase

/**
 * Real AWS S3 integration test for PURGE bug fix.
 *
 * This test reproduces the bug where PURGE incorrectly identifies newly written files as orphaned when using the Avro
 * state format. The bug was caused by PurgeOrphanedSplitsExecutor not reading files from Avro state directories
 * (state-v*) - it only read from JSON checkpoints.
 *
 * The fix adds getFilesFromAvroState() which reads file entries from all retained Avro state directories, ensuring
 * files referenced in the transaction log are not marked as orphaned.
 *
 * Credentials are loaded from ~/.aws/credentials file.
 */
class CloudS3PurgeBugReproTest extends CloudS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/purge-bug-repro-$testRunId"

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None
  private var fs: FileSystem                           = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    // Use Avro format (the new default) - this is critical for reproducing the bug
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

  test("S3: PURGE should NOT identify newly written files as orphaned (bug reproduction)") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/purge_bug_test"

    val sparkSession = spark
    import sparkSession.implicits._

    // Create a simple table with some data
    val df = Seq(
      (1, "Alice", 100),
      (2, "Bob", 200),
      (3, "Carol", 300)
    ).toDF("id", "name", "value")

    // Write the data - this creates Avro state (state-v*) instead of JSON checkpoint
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    println(s"=== Table written to $tablePath ===")

    // List the split files on S3
    val tableDir = new Path(tablePath)
    val splitFiles = fs
      .listStatus(tableDir)
      .filter(_.getPath.getName.endsWith(".split"))
    println(s"Split files written: ${splitFiles.length}")
    splitFiles.foreach(f => println(s"  - ${f.getPath.getName}"))

    // Verify transaction log structure
    val txLogPath  = new Path(tablePath, "_transaction_log")
    val txLogFiles = fs.listStatus(txLogPath)
    println(s"\nTransaction log contents:")
    txLogFiles.foreach { f =>
      val typeName = if (f.isDirectory) "DIR " else "FILE"
      println(s"  - [$typeName] ${f.getPath.getName}")
    }

    // Check for Avro state directory
    val stateDirectories = txLogFiles.filter(_.getPath.getName.startsWith("state-v"))
    println(s"\nAvro state directories found: ${stateDirectories.length}")
    stateDirectories.foreach(d => println(s"  - ${d.getPath.getName}"))

    // Read back to verify data
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    val count = readDf.count()
    println(s"Row count: $count")
    assert(count == 3, s"Expected 3 rows but got $count")

    // Now run PURGE with DRY RUN
    println("\n=== Running PURGE DRY RUN ===")
    val purgeResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' DRY RUN")
    purgeResult.show(false)

    val purgeRows     = purgeResult.collect()
    val metrics       = purgeRows.head.getAs[org.apache.spark.sql.Row]("metrics")
    val orphanedFiles = metrics.getAs[Long]("orphaned_files_found")

    println(s"\nOrphaned files identified: $orphanedFiles")

    // THIS IS THE KEY ASSERTION - no files should be orphaned in a brand new table
    // Before the fix, this would fail with orphanedFiles > 0
    assert(
      orphanedFiles == 0,
      s"Expected 0 orphaned files but found $orphanedFiles - " +
        "PURGE is incorrectly identifying valid files as orphaned! " +
        "This indicates the bug where Avro state directories are not being read."
    )

    println("✅ Test passed - no files incorrectly identified as orphaned on S3")
  }

  test("S3: PURGE should correctly identify ONLY orphaned files, not valid ones") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/mixed_orphan_test"

    val sparkSession = spark
    import sparkSession.implicits._

    // Create a table with data
    val df = Seq(
      (1, "Alice", 100),
      (2, "Bob", 200)
    ).toDF("id", "name", "value")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    println(s"=== Table written to $tablePath ===")

    // Count valid split files
    val tableDir = new Path(tablePath)
    val validSplitFiles = fs
      .listStatus(tableDir)
      .filter(_.getPath.getName.endsWith(".split"))
    println(s"Valid split files: ${validSplitFiles.length}")

    // Create some ACTUAL orphaned files (not in transaction log)
    val orphan1 = new Path(s"$tablePath/orphan1_${UUID.randomUUID()}.split")
    val orphan2 = new Path(s"$tablePath/orphan2_${UUID.randomUUID()}.split")
    fs.create(orphan1).close()
    fs.create(orphan2).close()
    println(s"Created 2 orphaned files:")
    println(s"  - ${orphan1.getName}")
    println(s"  - ${orphan2.getName}")

    // Run PURGE with DRY RUN
    println("\n=== Running PURGE DRY RUN ===")
    val purgeResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' DRY RUN")
    purgeResult.show(false)

    val purgeRows          = purgeResult.collect()
    val metrics            = purgeRows.head.getAs[org.apache.spark.sql.Row]("metrics")
    val orphanedFilesFound = metrics.getAs[Long]("orphaned_files_found")

    println(s"\nOrphaned files identified: $orphanedFilesFound")

    // Should find exactly 2 orphaned files (the ones we created), not the valid ones
    assert(
      orphanedFilesFound == 2,
      s"Expected exactly 2 orphaned files but found $orphanedFilesFound - " +
        "PURGE should only identify the orphaned files we created, not valid files."
    )

    // Verify valid files are NOT being counted as orphaned
    // Total files = valid + orphans, only orphans should be identified
    val totalSplitFiles = fs.listStatus(tableDir).count(_.getPath.getName.endsWith(".split"))
    println(s"Total split files on disk: $totalSplitFiles")
    println(s"Valid files: ${validSplitFiles.length}")
    println(s"Orphaned files: $orphanedFilesFound")

    assert(totalSplitFiles == validSplitFiles.length + 2, "Total files should equal valid + 2 orphans")

    // Verify data integrity
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    assert(readDf.count() == 2, "Table should still have 2 rows")

    println("✅ Test passed - PURGE correctly distinguished orphaned from valid files on S3")
  }

  test("S3: PURGE should work after multiple writes with Avro state") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/multi_write_test"

    val sparkSession = spark
    import sparkSession.implicits._

    // Write data multiple times
    println("=== Writing data in 5 batches ===")
    (1 to 5).foreach { batch =>
      val df = Seq(
        (batch * 10 + 1, s"User${batch}A", batch * 100),
        (batch * 10 + 2, s"User${batch}B", batch * 100 + 50)
      ).toDF("id", "name", "value")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (batch == 1) "overwrite" else "append")
        .save(tablePath)

      println(s"  Batch $batch written")
    }

    // Verify all data
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    val rowCount = readDf.count()
    println(s"\nTotal rows after 5 writes: $rowCount")
    assert(rowCount == 10, s"Expected 10 rows but got $rowCount")

    // Check transaction log structure
    val txLogPath        = new Path(tablePath, "_transaction_log")
    val txLogFiles       = fs.listStatus(txLogPath)
    val stateDirectories = txLogFiles.filter(_.getPath.getName.startsWith("state-v"))
    println(s"Avro state directories: ${stateDirectories.length}")

    // Run PURGE DRY RUN
    println("\n=== Running PURGE DRY RUN ===")
    val purgeResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' DRY RUN")
    purgeResult.show(false)

    val purgeRows     = purgeResult.collect()
    val metrics       = purgeRows.head.getAs[org.apache.spark.sql.Row]("metrics")
    val orphanedFiles = metrics.getAs[Long]("orphaned_files_found")

    println(s"\nOrphaned files identified: $orphanedFiles")

    // No files should be orphaned - all are referenced in transaction log
    assert(orphanedFiles == 0, s"Expected 0 orphaned files after multiple writes but found $orphanedFiles")

    println("✅ Test passed - multiple writes with Avro state handled correctly on S3")
  }

  test("S3: PURGE should work with partitioned tables and Avro state") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/partitioned_purge_test"

    val sparkSession = spark
    import sparkSession.implicits._

    // Write partitioned data
    val df = Seq(
      (1, "Alice", "2024-01-01"),
      (2, "Bob", "2024-01-01"),
      (3, "Carol", "2024-01-02"),
      (4, "Dave", "2024-01-02")
    ).toDF("id", "name", "date")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("date")
      .mode("overwrite")
      .save(tablePath)

    println(s"=== Partitioned table written to $tablePath ===")

    // List partitions
    val tableDir = new Path(tablePath)
    val partitions = fs
      .listStatus(tableDir)
      .filter(s => s.isDirectory && s.getPath.getName.startsWith("date="))
    println(s"Partitions created: ${partitions.length}")
    partitions.foreach(p => println(s"  - ${p.getPath.getName}"))

    // Verify all data
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    val rowCount = readDf.count()
    println(s"Total rows: $rowCount")
    assert(rowCount == 4, s"Expected 4 rows but got $rowCount")

    // Run PURGE DRY RUN
    println("\n=== Running PURGE DRY RUN ===")
    val purgeResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' DRY RUN")
    purgeResult.show(false)

    val purgeRows     = purgeResult.collect()
    val metrics       = purgeRows.head.getAs[org.apache.spark.sql.Row]("metrics")
    val orphanedFiles = metrics.getAs[Long]("orphaned_files_found")

    println(s"\nOrphaned files identified: $orphanedFiles")

    // No files should be orphaned
    assert(orphanedFiles == 0, s"Expected 0 orphaned files in partitioned table but found $orphanedFiles")

    println("✅ Test passed - partitioned table with Avro state handled correctly on S3")
  }

  test("S3: PURGE with 0 hours retention should delete old state directories (bug reproduction)") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/txlog_retention_test"

    val sparkSession = spark
    import sparkSession.implicits._

    // Write data multiple times to create multiple state versions
    println("=== Writing data in 3 batches to create multiple state versions ===")
    (1 to 3).foreach { batch =>
      val df = Seq(
        (batch * 10 + 1, s"User${batch}A"),
        (batch * 10 + 2, s"User${batch}B")
      ).toDF("id", "name")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (batch == 1) "overwrite" else "append")
        .save(tablePath)

      println(s"  Batch $batch written")
    }

    // Verify state directories were created
    val txLogPath     = new Path(tablePath, "_transaction_log")
    val txLogContents = fs.listStatus(txLogPath)

    println(s"\nTransaction log contents:")
    txLogContents.foreach { f =>
      val typeName = if (f.isDirectory) "DIR " else "FILE"
      println(s"  - [$typeName] ${f.getPath.getName}")
    }

    val stateDirectories = txLogContents.filter(_.getPath.getName.startsWith("state-v"))
    val versionFiles = txLogContents.filter { f =>
      val name = f.getPath.getName
      name.endsWith(".json") && !name.contains("checkpoint") && !name.startsWith("_")
    }

    println(s"\nState directories found: ${stateDirectories.length}")
    stateDirectories.foreach(d => println(s"  - ${d.getPath.getName}"))
    println(s"Version files found: ${versionFiles.length}")
    versionFiles.foreach(f => println(s"  - ${f.getPath.getName}"))

    // With 3 writes, we should have:
    // - At least 1 state directory (latest state)
    // - Multiple version files (00000000000000000000.json, etc.)
    assert(stateDirectories.length >= 1, s"Expected at least 1 state directory, found ${stateDirectories.length}")

    // Verify data integrity
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    val rowCount = readDf.count()
    println(s"\nTotal rows: $rowCount")
    assert(rowCount == 6, s"Expected 6 rows but got $rowCount")

    // Disable retention check to allow 0 hour retention
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")

    // Run PURGE with 0 hours retention - this should delete old transaction log files
    println("\n=== Running PURGE DRY RUN with 0 hours TX LOG retention ===")
    val purgeResult =
      spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 0 HOURS TRANSACTION LOG RETENTION 0 HOURS DRY RUN")
    purgeResult.show(false)

    val purgeRows            = purgeResult.collect()
    val metrics              = purgeRows.head.getAs[org.apache.spark.sql.Row]("metrics")
    val orphanedFiles        = metrics.getAs[Long]("orphaned_files_found")
    val expiredStatesFound   = metrics.getAs[Long]("expired_states_found")
    val expiredStatesDeleted = metrics.getAs[Long]("expired_states_deleted")
    val isDryRun             = metrics.getAs[Boolean]("dry_run")

    println(s"\nOrphaned files identified: $orphanedFiles")
    println(s"Expired states found: $expiredStatesFound")
    println(s"Expired states deleted: $expiredStatesDeleted")
    println(s"Is dry run: $isDryRun")

    // No valid files should be orphaned
    assert(orphanedFiles == 0, s"Expected 0 orphaned files but found $orphanedFiles")

    // With 0 hour retention, old state directories should be marked for deletion
    // After 3 writes, we have state-v1, state-v2, state-v3 - the first 2 should be expired
    assert(
      expiredStatesFound > 0,
      s"Expected expired state directories to be found with 0 hour retention, " +
        s"but found $expiredStatesFound. This indicates the state retention is not working correctly."
    )

    // In DRY RUN mode, expiredStatesDeleted should be 0
    assert(isDryRun, "Expected this to be a dry run")
    assert(expiredStatesDeleted == 0, s"Expected 0 states deleted in DRY RUN mode but got $expiredStatesDeleted")

    // Reset config
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "true")

    println("✅ Test passed - transaction log retention works correctly on S3")
  }

  test("S3: PURGE (NOT dry run) should actually delete old state directories") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/actual_purge_test"

    val sparkSession = spark
    import sparkSession.implicits._

    // Write data multiple times to create multiple state versions
    println("=== Writing data in 4 batches to create multiple state versions ===")
    (1 to 4).foreach { batch =>
      val df = Seq(
        (batch * 10 + 1, s"User${batch}A"),
        (batch * 10 + 2, s"User${batch}B")
      ).toDF("id", "name")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (batch == 1) "overwrite" else "append")
        .save(tablePath)

      println(s"  Batch $batch written")
    }

    // Count state directories BEFORE purge
    val txLogPath = new Path(tablePath, "_transaction_log")
    val statesBefore = fs
      .listStatus(txLogPath)
      .filter(_.getPath.getName.startsWith("state-v"))

    println(s"\nState directories BEFORE purge: ${statesBefore.length}")
    statesBefore.sortBy(_.getPath.getName).foreach(d => println(s"  - ${d.getPath.getName}"))

    // Verify data integrity before purge
    val rowCountBefore = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()
    println(s"Total rows before purge: $rowCountBefore")
    assert(rowCountBefore == 8, s"Expected 8 rows but got $rowCountBefore")

    // Disable retention check to allow 0 hour retention
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")

    // Run ACTUAL PURGE (NOT dry run) with 0 hours retention
    println("\n=== Running ACTUAL PURGE with 0 hours TX LOG retention (NOT DRY RUN) ===")
    val purgeResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 0 HOURS TRANSACTION LOG RETENTION 0 HOURS")
    purgeResult.show(false)

    val purgeRows            = purgeResult.collect()
    val metrics              = purgeRows.head.getAs[org.apache.spark.sql.Row]("metrics")
    val status               = metrics.getAs[String]("status")
    val orphanedFilesFound   = metrics.getAs[Long]("orphaned_files_found")
    val orphanedFilesDeleted = metrics.getAs[Long]("orphaned_files_deleted")
    val expiredStatesFound   = metrics.getAs[Long]("expired_states_found")
    val expiredStatesDeleted = metrics.getAs[Long]("expired_states_deleted")
    val isDryRun             = metrics.getAs[Boolean]("dry_run")
    val message              = metrics.getAs[String]("message")

    println(s"\nStatus: $status")
    println(s"Orphaned files found: $orphanedFilesFound")
    println(s"Orphaned files deleted: $orphanedFilesDeleted")
    println(s"Expired states found: $expiredStatesFound")
    println(s"Expired states deleted: $expiredStatesDeleted")
    println(s"Is dry run: $isDryRun")
    println(s"Message: $message")

    // Verify this was NOT a dry run
    assert(!isDryRun, "Expected this to NOT be a dry run")

    // No valid files should be orphaned
    assert(orphanedFilesFound == 0, s"Expected 0 orphaned files but found $orphanedFilesFound")

    // With 4 writes we have 4 state versions, the old ones should be expired
    assert(expiredStatesFound > 0, s"Expected expired state directories to be found, but found $expiredStatesFound")

    // In NON-dry run mode, the expired states should actually be deleted
    assert(
      expiredStatesDeleted > 0,
      s"Expected states to actually be deleted but expiredStatesDeleted=$expiredStatesDeleted"
    )

    // expiredStatesDeleted should equal expiredStatesFound (all found should be deleted)
    assert(
      expiredStatesDeleted == expiredStatesFound,
      s"Expected all found states to be deleted: found=$expiredStatesFound, deleted=$expiredStatesDeleted"
    )

    // Count state directories AFTER purge
    val statesAfter = fs
      .listStatus(txLogPath)
      .filter(_.getPath.getName.startsWith("state-v"))

    println(s"\nState directories AFTER purge: ${statesAfter.length}")
    statesAfter.sortBy(_.getPath.getName).foreach(d => println(s"  - ${d.getPath.getName}"))

    // Verify fewer state directories exist after purge
    val expectedStatesAfter = statesBefore.length - expiredStatesDeleted.toInt
    assert(
      statesAfter.length == expectedStatesAfter,
      s"Expected $expectedStatesAfter state directories after purge but found ${statesAfter.length}"
    )

    // Most importantly: verify data is STILL readable after purge
    println("\n=== Verifying data integrity AFTER purge ===")
    val rowCountAfter = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()
    println(s"Total rows after purge: $rowCountAfter")
    assert(rowCountAfter == 8, s"Expected 8 rows after purge but got $rowCountAfter - DATA CORRUPTION!")

    // Reset config
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "true")

    println("✅ Test passed - PURGE actually deleted old state directories and data is still intact")
  }

  test("S3: PURGE should delete files from dropped partitions") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/drop_partition_purge_test"

    val sparkSession = spark
    import sparkSession.implicits._

    // Create a partitioned table with multiple partitions
    val df = Seq(
      (1, "Alice", "2024-01-01"),
      (2, "Bob", "2024-01-01"),
      (3, "Carol", "2024-01-02"),
      (4, "Dave", "2024-01-02"),
      (5, "Eve", "2024-01-03"),
      (6, "Frank", "2024-01-03")
    ).toDF("id", "name", "date")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("date")
      .mode("overwrite")
      .save(tablePath)

    println(s"=== Partitioned table written to $tablePath ===")

    // Count split files BEFORE drop
    val tableDir = new Path(tablePath)
    val splitsBefore = fs
      .listStatus(
        tableDir,
        new org.apache.hadoop.fs.PathFilter {
          override def accept(path: Path): Boolean = path.getName.endsWith(".split")
        }
      )
      .toSeq ++ fs.listStatus(tableDir).filter(_.isDirectory).flatMap { partition =>
      fs.listStatus(partition.getPath).filter(_.getPath.getName.endsWith(".split"))
    }

    println(s"\nTotal split files BEFORE drop: ${splitsBefore.length}")
    splitsBefore.foreach(f => println(s"  - ${f.getPath.toString.replace(tablePath, "")}"))

    // Verify row count
    val rowCountBefore = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()
    println(s"Total rows before drop: $rowCountBefore")
    assert(rowCountBefore == 6, s"Expected 6 rows but got $rowCountBefore")

    // Count files in partition to be dropped
    val partition2Drop = new Path(tablePath, "date=2024-01-02")
    val filesInDroppedPartition = fs
      .listStatus(partition2Drop)
      .filter(_.getPath.getName.endsWith(".split"))
      .length
    println(s"\nFiles in partition 'date=2024-01-02': $filesInDroppedPartition")

    // Drop one partition
    println("\n=== Dropping partition date=2024-01-02 ===")
    spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$tablePath' WHERE date = '2024-01-02'").show(false)

    // Verify row count after drop (logically removed)
    val rowCountAfterDrop = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()
    println(s"Total rows after drop: $rowCountAfterDrop")
    assert(rowCountAfterDrop == 4, s"Expected 4 rows after drop but got $rowCountAfterDrop")

    // Files should still exist on disk (logical delete only)
    val splitsAfterDrop = splitsBefore.filter(f => fs.exists(f.getPath))
    println(s"Split files still on disk after logical drop: ${splitsAfterDrop.length}")
    assert(splitsAfterDrop.length == splitsBefore.length, "Files should still exist after logical drop")

    // Disable retention check
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")

    // Run ACTUAL PURGE to delete the dropped partition files
    println("\n=== Running ACTUAL PURGE with 0 hours retention ===")
    val purgeResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 0 HOURS")
    purgeResult.show(false)

    val metrics              = purgeResult.collect().head.getAs[org.apache.spark.sql.Row]("metrics")
    val orphanedFilesFound   = metrics.getAs[Long]("orphaned_files_found")
    val orphanedFilesDeleted = metrics.getAs[Long]("orphaned_files_deleted")

    println(s"\nOrphaned files found: $orphanedFilesFound")
    println(s"Orphaned files deleted: $orphanedFilesDeleted")

    // Files from dropped partition should be identified and deleted as orphaned
    assert(
      orphanedFilesFound >= filesInDroppedPartition,
      s"Expected at least $filesInDroppedPartition orphaned files (from dropped partition), but found $orphanedFilesFound"
    )
    assert(
      orphanedFilesDeleted == orphanedFilesFound,
      s"All orphaned files should be deleted: found=$orphanedFilesFound, deleted=$orphanedFilesDeleted"
    )

    // Verify dropped partition files are actually deleted from disk
    val droppedPartitionExists = fs.exists(partition2Drop) &&
      fs.listStatus(partition2Drop).exists(_.getPath.getName.endsWith(".split"))
    println(s"Dropped partition files still exist: $droppedPartitionExists")

    // Count remaining split files
    val splitsAfterPurge = fs
      .listStatus(
        tableDir,
        new org.apache.hadoop.fs.PathFilter {
          override def accept(path: Path): Boolean = path.getName.endsWith(".split")
        }
      )
      .toSeq ++ fs.listStatus(tableDir).filter(s => s.isDirectory && s.getPath.getName.startsWith("date=")).flatMap {
      partition =>
        if (fs.exists(partition.getPath)) fs.listStatus(partition.getPath).filter(_.getPath.getName.endsWith(".split"))
        else Seq.empty
    }
    println(s"Total split files AFTER purge: ${splitsAfterPurge.length}")

    // Verify data integrity - remaining partitions should still work
    println("\n=== Verifying data integrity AFTER purge ===")
    val rowCountAfterPurge = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()
    println(s"Total rows after purge: $rowCountAfterPurge")
    assert(rowCountAfterPurge == 4, s"Expected 4 rows after purge but got $rowCountAfterPurge")

    // Reset config
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "true")

    println("✅ Test passed - PURGE correctly deleted files from dropped partition")
  }

  test("S3: PURGE should delete old source splits after MERGE") {
    assume(awsCredentials.isDefined, "AWS credentials required for this test")

    val tablePath = s"$testBasePath/merge_purge_test"

    val sparkSession = spark
    import sparkSession.implicits._

    // Write multiple small batches to create many small splits
    println("=== Writing 5 batches of data (small splits) ===")
    (1 to 5).foreach { batch =>
      val df =
        (1 to 10).map(i => (batch * 100 + i, s"User_${batch}_$i", batch * 1000 + i)).toDF("id", "name", "value")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(if (batch == 1) "overwrite" else "append")
        .save(tablePath)

      println(s"  Batch $batch written")
    }

    // Count split files BEFORE merge
    val tableDir = new Path(tablePath)
    val splitsBefore = fs
      .listStatus(tableDir)
      .filter(_.getPath.getName.endsWith(".split"))

    println(s"\nSplit files BEFORE merge: ${splitsBefore.length}")

    // Verify row count
    val rowCountBefore = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()
    println(s"Total rows before merge: $rowCountBefore")
    assert(rowCountBefore == 50, s"Expected 50 rows but got $rowCountBefore")

    // Run MERGE to consolidate splits
    println("\n=== Running MERGE SPLITS ===")
    val mergeResult = spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 100M")
    mergeResult.show(false)

    // Count split files AFTER merge (on disk)
    // NOTE: Merge creates new merged files but does NOT delete old files immediately
    // Old files are marked with RemoveAction in transaction log but stay on disk until PURGE
    val splitsAfterMergeOnDisk = fs
      .listStatus(tableDir)
      .filter(_.getPath.getName.endsWith(".split"))

    println(s"\nSplit files on disk AFTER merge: ${splitsAfterMergeOnDisk.length}")
    println(s"(Old files still exist on disk - merge just marks them for removal)")

    // After merge, we should have MORE files on disk (old + merged) until purge
    assert(
      splitsAfterMergeOnDisk.length >= splitsBefore.length,
      s"Expected at least ${splitsBefore.length} files on disk after merge (old files not deleted yet)"
    )

    // Verify data integrity after merge
    val rowCountAfterMerge = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()
    println(s"Total rows after merge: $rowCountAfterMerge")
    assert(rowCountAfterMerge == 50, s"Expected 50 rows after merge but got $rowCountAfterMerge")

    // OLD source splits should still be on disk (merge creates RemoveActions but doesn't delete files)
    println(s"\nTotal split files on disk after merge: ${splitsAfterMergeOnDisk.length}")
    println(s"Original splits: ${splitsBefore.length}, so ~${splitsBefore.length} old files await purge")

    // Disable retention check
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "false")

    // Run DRY RUN first to see what would be deleted
    println("\n=== Running PURGE DRY RUN ===")
    val dryRunResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 0 HOURS DRY RUN")
    dryRunResult.show(false)

    val dryRunMetrics      = dryRunResult.collect().head.getAs[org.apache.spark.sql.Row]("metrics")
    val orphanedFilesFound = dryRunMetrics.getAs[Long]("orphaned_files_found")
    println(s"Orphaned files to be deleted: $orphanedFilesFound")

    // Run ACTUAL PURGE to delete the merged-from source splits
    println("\n=== Running ACTUAL PURGE with 0 hours retention ===")
    val purgeResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 0 HOURS")
    purgeResult.show(false)

    val metrics              = purgeResult.collect().head.getAs[org.apache.spark.sql.Row]("metrics")
    val orphanedFilesDeleted = metrics.getAs[Long]("orphaned_files_deleted")

    println(s"\nOrphaned files deleted: $orphanedFilesDeleted")

    // After merge, there should be orphaned source splits to delete
    // (The original splits that were merged into larger ones)
    assert(
      orphanedFilesDeleted == orphanedFilesFound,
      s"All found orphans should be deleted: found=$orphanedFilesFound, deleted=$orphanedFilesDeleted"
    )

    // Count split files AFTER purge
    val splitsAfterPurge = fs
      .listStatus(tableDir)
      .filter(_.getPath.getName.endsWith(".split"))

    println(s"Split files AFTER purge: ${splitsAfterPurge.length}")

    // After purge, only the merged (referenced) splits should remain
    // Should have fewer files than before purge
    assert(
      splitsAfterPurge.length < splitsAfterMergeOnDisk.length,
      s"Expected fewer splits after purge: had ${splitsAfterMergeOnDisk.length} on disk, now ${splitsAfterPurge.length}"
    )

    // Most importantly: verify data integrity after purge
    println("\n=== Verifying data integrity AFTER purge ===")
    val rowCountAfterPurge = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()
    println(s"Total rows after purge: $rowCountAfterPurge")
    assert(rowCountAfterPurge == 50, s"Expected 50 rows after purge but got $rowCountAfterPurge - DATA CORRUPTION!")

    // Reset config
    spark.conf.set("spark.indextables.purge.retentionCheckEnabled", "true")

    println("✅ Test passed - PURGE correctly deleted old source splits after MERGE")
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
