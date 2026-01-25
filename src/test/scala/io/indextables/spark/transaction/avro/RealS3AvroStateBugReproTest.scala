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

package io.indextables.spark.transaction.avro

import org.apache.spark.sql.functions._

import io.indextables.spark.RealS3TestBase
import io.indextables.spark.transaction.EnhancedTransactionLogCache

/**
 * Real S3 integration tests for reproducing Avro state bugs.
 *
 * These tests attempt to reproduce bugs that occur on S3 but not locally:
 *   1. MERGE SPLITS visibility bug - second transaction's splits invisible to merge
 *   2. DROP PARTITIONS bug - incorrect 'table is empty' error
 *
 * Prerequisites:
 *   - AWS credentials configured via ~/.aws/credentials or environment variables
 *   - S3 bucket accessible for testing (default: test-tantivy4sparkbucket in us-east-2)
 *
 * Run with:
 *   mvn scalatest:test -DwildcardSuites='io.indextables.spark.transaction.avro.RealS3AvroStateBugReproTest'
 */
class RealS3AvroStateBugReproTest extends RealS3TestBase {

  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  // Use same bucket and region as other RealS3 tests
  private val S3_BUCKET = System.getProperty("test.s3.bucket", "test-tantivy4sparkbucket")
  private val S3_REGION = System.getProperty("test.s3.region", "us-east-2")

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    assume(awsCredentials.isDefined, "AWS credentials not available - skipping tests")

    val (accessKey, secretKey) = awsCredentials.get

    // Configure Spark for real S3 access
    spark.conf.set("spark.indextables.aws.accessKey", accessKey)
    spark.conf.set("spark.indextables.aws.secretKey", secretKey)
    spark.conf.set("spark.indextables.aws.region", S3_REGION)

    // Also configure Hadoop config for CloudStorageProvider
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
    hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
    hadoopConf.set("spark.indextables.aws.region", S3_REGION)

    println(s"AWS credentials loaded for S3 bug reproduction tests")
    println(s"Test bucket: $S3_BUCKET, region: $S3_REGION")
  }

  /** Load AWS credentials from ~/.aws/credentials file. */
  private def loadAwsCredentials(): Option[(String, String)] =
    try {
      import java.io.{File, FileInputStream}
      import java.util.Properties
      import scala.util.Using

      val home = System.getProperty("user.home")
      val credFile = new File(s"$home/.aws/credentials")

      if (!credFile.exists()) {
        println("~/.aws/credentials file not found")
        return None
      }

      val props = new Properties()
      Using(new FileInputStream(credFile)) { stream =>
        props.load(stream)
      }

      // Try to get credentials from [default] profile
      val accessKey = Option(props.getProperty("aws_access_key_id"))
      val secretKey = Option(props.getProperty("aws_secret_access_key"))

      (accessKey, secretKey) match {
        case (Some(ak), Some(sk)) => Some((ak, sk))
        case _ =>
          println("AWS credentials not found in ~/.aws/credentials")
          None
      }
    } catch {
      case e: Exception =>
        println(s"Error reading AWS credentials: ${e.getMessage}")
        None
    }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Enable Avro state format for all tests
    spark.conf.set("spark.indextables.state.format", "avro")
    // Clear global caches to simulate fresh session
    EnhancedTransactionLogCache.clearGlobalCaches()
  }

  override def afterEach(): Unit = {
    // Reset to default
    spark.conf.unset("spark.indextables.state.format")
    EnhancedTransactionLogCache.clearGlobalCaches()
    super.afterEach()
  }

  /** Get write options with AWS credentials for executor distribution. */
  private def getWriteOptions(): Map[String, String] = {
    val (accessKey, secretKey) = awsCredentials.get
    Map(
      "spark.indextables.aws.accessKey" -> accessKey,
      "spark.indextables.aws.secretKey" -> secretKey,
      "spark.indextables.aws.region"    -> S3_REGION,
      "spark.indextables.indexing.fastfields" -> "id,score"
    )
  }

  /** Get read options with AWS credentials for executor distribution. */
  private def getReadOptions(): Map[String, String] = {
    val (accessKey, secretKey) = awsCredentials.get
    Map(
      "spark.indextables.aws.accessKey" -> accessKey,
      "spark.indextables.aws.secretKey" -> secretKey,
      "spark.indextables.aws.region"    -> S3_REGION
    )
  }

  // ==========================================================================
  // BUG REPRODUCTION: MERGE SPLITS visibility after multiple appends
  // User reported: first append creates 3 splits, second creates 4 splits,
  // MERGE only sees the 3 splits from first append
  // ==========================================================================

  test("S3 BUG REPRO: MERGE SPLITS should see ALL files from multiple append transactions") {
    val testId = generateTestId()
    val path = s"s3a://$S3_BUCKET/merge-visibility-bug-$testId"

    println(s"=== S3 BUG REPRO: MERGE SPLITS visibility test at: $path ===")

    // Clear global caches to ensure fresh state
    EnhancedTransactionLogCache.clearGlobalCaches()

    // ========================================
    // First append: create 3 splits (3 batches of data)
    // ========================================
    println("\n--- First append: writing 3 batches (should create 3 splits) ---")
    (1 to 3).foreach { batch =>
      val data = (1 to 10).map(i => (batch * 100 + i, s"Batch1-$batch-$i", batch * 10))
      spark.createDataFrame(data).toDF("id", "name", "score")
        .write.format(provider)
        .options(getWriteOptions())
        .mode("append")
        .save(path)
      println(s"  Wrote batch $batch (IDs ${batch * 100 + 1} to ${batch * 100 + 10})")
    }

    // Check files after first append
    val filesAfterAppend1 = listS3TransactionLogFiles(path)
    println(s"After first append: ${filesAfterAppend1.size} state directories")
    filesAfterAppend1.foreach(f => println(s"  - $f"))

    // Read to verify
    val count1 = spark.read.format(provider).options(getReadOptions()).load(path).count()
    println(s"Row count after first append: $count1 (expected 30)")
    count1 shouldBe 30

    // Clear caches between appends to simulate separate sessions
    EnhancedTransactionLogCache.clearGlobalCaches()
    println("\n--- Cleared global caches (simulating new session) ---")

    // ========================================
    // Second append: create 4 more splits (4 batches of data)
    // ========================================
    println("\n--- Second append: writing 4 batches (should create 4 splits) ---")
    (4 to 7).foreach { batch =>
      val data = (1 to 10).map(i => (batch * 100 + i, s"Batch2-$batch-$i", batch * 10))
      spark.createDataFrame(data).toDF("id", "name", "score")
        .write.format(provider)
        .options(getWriteOptions())
        .mode("append")
        .save(path)
      println(s"  Wrote batch $batch (IDs ${batch * 100 + 1} to ${batch * 100 + 10})")
    }

    // Check files after second append
    val filesAfterAppend2 = listS3TransactionLogFiles(path)
    println(s"After second append: ${filesAfterAppend2.size} state directories")
    filesAfterAppend2.foreach(f => println(s"  - $f"))

    // Verify total row count
    EnhancedTransactionLogCache.clearGlobalCaches()
    val count2 = spark.read.format(provider).options(getReadOptions()).load(path).count()
    println(s"Total row count: $count2 (expected 70)")
    count2 shouldBe 70

    // Read _last_checkpoint to see what state it points to
    val lastCheckpoint = readLastCheckpoint(path)
    println(s"_last_checkpoint content: $lastCheckpoint")

    // Clear caches to simulate a new session for MERGE
    EnhancedTransactionLogCache.clearGlobalCaches()
    println("\n--- Cleared global caches before MERGE (simulating new session) ---")

    // ========================================
    // Run MERGE SPLITS
    // ========================================
    println("\n--- Running MERGE SPLITS ---")
    val mergeResult = spark.sql(s"MERGE SPLITS '$path' TARGET SIZE 100M").collect()

    // Check merge result
    val metricsRow = mergeResult(0).getStruct(1)
    val status = metricsRow.getAs[String]("status")
    val mergedFiles = Option(metricsRow.getAs[java.lang.Long]("merged_files")).map(_.toLong).getOrElse(0L)
    println(s"Merge status: $status")
    println(s"Merged files: $mergedFiles")

    // CRITICAL ASSERTION: The merge should have seen ALL files
    // If only 3 files were merged (from first append), this indicates the bug
    if (status == "success") {
      // We expect at least 6-7 files to be merged (from both appends)
      assert(mergedFiles >= 6L,
        s"BUG DETECTED: MERGE only saw $mergedFiles files, but expected at least 6 from both appends. " +
          "This indicates the second append's files are not visible to MERGE.")
    }

    // Verify data integrity after merge
    EnhancedTransactionLogCache.clearGlobalCaches()
    val afterMergeCount = spark.read.format(provider).options(getReadOptions()).load(path).count()
    println(s"After merge row count: $afterMergeCount (expected 70)")
    afterMergeCount shouldBe 70

    // Verify files from both appends are represented in the data
    val batch1Count = spark.read.format(provider).options(getReadOptions()).load(path)
      .filter(col("id") < 400).count()
    val batch2Count = spark.read.format(provider).options(getReadOptions()).load(path)
      .filter(col("id") >= 400).count()

    println(s"Batch1 rows (id < 400): $batch1Count (expected 30)")
    println(s"Batch2 rows (id >= 400): $batch2Count (expected 40)")

    batch1Count shouldBe 30
    batch2Count shouldBe 40

    println("\n=== TEST PASSED: All files visible to MERGE SPLITS on S3 ===")
  }

  // ==========================================================================
  // BUG REPRODUCTION: DROP PARTITIONS with separate inserts
  // User reported: insert to partition 01, insert to partition 02,
  // drop partition 02 returns "table is empty", drop partition 01 succeeds
  // but makes partition 02 unreadable
  // ==========================================================================

  test("S3 BUG REPRO: DROP PARTITIONS should work with separate inserts to different partitions") {
    val testId = generateTestId()
    val path = s"s3a://$S3_BUCKET/drop-partitions-bug-$testId"

    println(s"=== S3 BUG REPRO: DROP PARTITIONS test at: $path ===")

    // Clear global caches to ensure fresh state
    EnhancedTransactionLogCache.clearGlobalCaches()

    // ========================================
    // First insert: partition 01
    // ========================================
    println("\n--- Insert 1: partition=01 ---")
    val data1 = Seq(
      (1, "Alice", "01", 100),
      (2, "Bob", "01", 200)
    )
    spark.createDataFrame(data1).toDF("id", "name", "partition_key", "score")
      .write.format(provider)
      .options(getWriteOptions())
      .partitionBy("partition_key")
      .mode("overwrite")
      .save(path)

    val count1 = spark.read.format(provider).options(getReadOptions()).load(path).count()
    println(s"After insert 1: count=$count1 (expected 2)")
    count1 shouldBe 2

    // Check state after first insert
    val filesAfterInsert1 = listS3TransactionLogFiles(path)
    println(s"State directories after insert 1: ${filesAfterInsert1.mkString(", ")}")

    // Clear caches to simulate separate session
    EnhancedTransactionLogCache.clearGlobalCaches()
    println("\n--- Cleared global caches (simulating new session) ---")

    // ========================================
    // Second insert: partition 02 (separate transaction)
    // ========================================
    println("\n--- Insert 2: partition=02 ---")
    val data2 = Seq(
      (3, "Charlie", "02", 150),
      (4, "Diana", "02", 250)
    )
    spark.createDataFrame(data2).toDF("id", "name", "partition_key", "score")
      .write.format(provider)
      .options(getWriteOptions())
      .partitionBy("partition_key")
      .mode("append")
      .save(path)

    // Verify both partitions exist
    EnhancedTransactionLogCache.clearGlobalCaches()
    val count2 = spark.read.format(provider).options(getReadOptions()).load(path).count()
    println(s"After insert 2: count=$count2 (expected 4)")
    count2 shouldBe 4

    val partition01Count = spark.read.format(provider).options(getReadOptions()).load(path)
      .filter(col("partition_key") === "01").count()
    val partition02Count = spark.read.format(provider).options(getReadOptions()).load(path)
      .filter(col("partition_key") === "02").count()
    println(s"Partition counts: 01=$partition01Count, 02=$partition02Count")
    partition01Count shouldBe 2
    partition02Count shouldBe 2

    // Check state after second insert
    val filesAfterInsert2 = listS3TransactionLogFiles(path)
    println(s"State directories after insert 2: ${filesAfterInsert2.mkString(", ")}")

    // Read _last_checkpoint
    val lastCheckpoint = readLastCheckpoint(path)
    println(s"_last_checkpoint content: $lastCheckpoint")

    // Clear caches before drop
    EnhancedTransactionLogCache.clearGlobalCaches()
    println("\n--- Cleared global caches before DROP (simulating new session) ---")

    // ========================================
    // Drop partition 02
    // BUG: User reports getting "no_action" "table is empty"
    // ========================================
    println("\n--- Drop partition 02 ---")
    val dropResult = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$path' WHERE partition_key = '02'").collect()
    val dropStatus = dropResult(0).getAs[String]("status")
    val dropMessage = dropResult(0).getAs[String]("message")
    println(s"Drop result: status=$dropStatus, message=$dropMessage")

    // CRITICAL ASSERTION: Drop should succeed, not return "table is empty"
    assert(dropStatus == "success",
      s"BUG DETECTED: Expected 'success' but got '$dropStatus' with message '$dropMessage'. " +
        "This indicates partition 02 files are not visible.")

    // Verify partition 01 still readable
    EnhancedTransactionLogCache.clearGlobalCaches()
    val afterDrop01 = spark.read.format(provider).options(getReadOptions()).load(path)
      .filter(col("partition_key") === "01").count()
    val afterDrop02 = spark.read.format(provider).options(getReadOptions()).load(path)
      .filter(col("partition_key") === "02").count()
    println(s"After drop: partition 01=$afterDrop01 (expected 2), partition 02=$afterDrop02 (expected 0)")

    afterDrop01 shouldBe 2
    afterDrop02 shouldBe 0

    println("\n=== TEST PASSED: DROP PARTITIONS works correctly on S3 ===")
  }

  test("S3 BUG REPRO: DROP PARTITIONS with explicit Avro checkpoints between inserts") {
    val testId = generateTestId()
    val path = s"s3a://$S3_BUCKET/drop-partitions-checkpoint-bug-$testId"

    println(s"=== S3 BUG REPRO: DROP PARTITIONS with checkpoints at: $path ===")

    // Clear global caches
    EnhancedTransactionLogCache.clearGlobalCaches()

    // ========================================
    // First insert: partition 01
    // ========================================
    println("\n--- Insert 1: partition=01 ---")
    val data1 = Seq(
      (1, "Alice", "01", 100),
      (2, "Bob", "01", 200)
    )
    spark.createDataFrame(data1).toDF("id", "name", "partition_key", "score")
      .write.format(provider)
      .options(getWriteOptions())
      .partitionBy("partition_key")
      .mode("overwrite")
      .save(path)

    // Create checkpoint after first insert
    println("--- Creating checkpoint after insert 1 ---")
    spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

    val state1 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
    println(s"State format after insert 1: ${state1(0).getAs[String]("format")}")
    state1(0).getAs[String]("format") shouldBe "avro-state"

    val count1 = spark.read.format(provider).options(getReadOptions()).load(path).count()
    println(s"After insert 1: count=$count1")
    count1 shouldBe 2

    // Clear caches
    EnhancedTransactionLogCache.clearGlobalCaches()
    println("\n--- Cleared global caches ---")

    // ========================================
    // Second insert: partition 02
    // ========================================
    println("\n--- Insert 2: partition=02 ---")
    val data2 = Seq(
      (3, "Charlie", "02", 150),
      (4, "Diana", "02", 250)
    )
    spark.createDataFrame(data2).toDF("id", "name", "partition_key", "score")
      .write.format(provider)
      .options(getWriteOptions())
      .partitionBy("partition_key")
      .mode("append")
      .save(path)

    // Create checkpoint after second insert
    println("--- Creating checkpoint after insert 2 ---")
    spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

    val state2 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
    println(s"State format after insert 2: ${state2(0).getAs[String]("format")}")
    state2(0).getAs[String]("format") shouldBe "avro-state"

    // Verify counts
    EnhancedTransactionLogCache.clearGlobalCaches()
    val count2 = spark.read.format(provider).options(getReadOptions()).load(path).count()
    println(s"After insert 2: count=$count2")
    count2 shouldBe 4

    val p01 = spark.read.format(provider).options(getReadOptions()).load(path)
      .filter(col("partition_key") === "01").count()
    val p02 = spark.read.format(provider).options(getReadOptions()).load(path)
      .filter(col("partition_key") === "02").count()
    println(s"Partition counts: 01=$p01, 02=$p02")
    p01 shouldBe 2
    p02 shouldBe 2

    // Clear caches before drop
    EnhancedTransactionLogCache.clearGlobalCaches()
    println("\n--- Cleared global caches before DROP ---")

    // ========================================
    // Drop partition 02
    // ========================================
    println("\n--- Drop partition 02 ---")
    val dropResult = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$path' WHERE partition_key = '02'").collect()
    val dropStatus = dropResult(0).getAs[String]("status")
    val dropMessage = dropResult(0).getAs[String]("message")
    println(s"Drop result: status=$dropStatus, message=$dropMessage")

    assert(dropStatus == "success",
      s"BUG DETECTED: Expected 'success' but got '$dropStatus': $dropMessage")

    // Verify partition 01 still readable
    EnhancedTransactionLogCache.clearGlobalCaches()
    val afterDrop01 = spark.read.format(provider).options(getReadOptions()).load(path)
      .filter(col("partition_key") === "01").count()
    val afterDrop02 = spark.read.format(provider).options(getReadOptions()).load(path)
      .filter(col("partition_key") === "02").count()
    println(s"After drop: partition 01=$afterDrop01, partition 02=$afterDrop02")

    afterDrop01 shouldBe 2
    afterDrop02 shouldBe 0

    println("\n=== TEST PASSED ===")
  }

  test("S3 BUG REPRO: Transaction log state version tracking across sessions") {
    val testId = generateTestId()
    val path = s"s3a://$S3_BUCKET/state-tracking-bug-$testId"

    println(s"=== S3 BUG REPRO: State version tracking at: $path ===")

    // Clear caches
    EnhancedTransactionLogCache.clearGlobalCaches()

    // ========================================
    // First write
    // ========================================
    println("\n--- First write ---")
    val data1 = (1 to 5).map(i => (i, s"First-$i", i * 10))
    spark.createDataFrame(data1).toDF("id", "name", "score")
      .write.format(provider)
      .options(getWriteOptions())
      .mode("overwrite")
      .save(path)

    val filesAfterWrite1 = listS3TransactionLogFiles(path)
    println(s"State directories after first write: ${filesAfterWrite1.mkString(", ")}")

    // Clear caches
    EnhancedTransactionLogCache.clearGlobalCaches()
    println("\n--- Cleared global caches ---")

    // ========================================
    // Second write (append)
    // ========================================
    println("\n--- Second write (append) ---")
    val data2 = (6 to 10).map(i => (i, s"Second-$i", i * 10))
    spark.createDataFrame(data2).toDF("id", "name", "score")
      .write.format(provider)
      .options(getWriteOptions())
      .mode("append")
      .save(path)

    val filesAfterWrite2 = listS3TransactionLogFiles(path)
    println(s"State directories after second write: ${filesAfterWrite2.mkString(", ")}")

    // Read _last_checkpoint
    val lastCheckpoint = readLastCheckpoint(path)
    println(s"_last_checkpoint content: $lastCheckpoint")

    // Verify state version incremented
    assert(filesAfterWrite2.size >= 2,
      s"Expected at least 2 state directories after two writes, got ${filesAfterWrite2.size}")

    // Clear caches and read with fresh state
    EnhancedTransactionLogCache.clearGlobalCaches()
    println("\n--- Reading with fresh cache ---")

    val totalCount = spark.read.format(provider).options(getReadOptions()).load(path).count()
    println(s"Total row count: $totalCount (expected 10)")
    totalCount shouldBe 10

    // Verify data from both writes
    val firstCount = spark.read.format(provider).options(getReadOptions()).load(path)
      .filter(col("id") <= 5).count()
    val secondCount = spark.read.format(provider).options(getReadOptions()).load(path)
      .filter(col("id") > 5).count()

    println(s"First write rows (id <= 5): $firstCount (expected 5)")
    println(s"Second write rows (id > 5): $secondCount (expected 5)")

    firstCount shouldBe 5
    secondCount shouldBe 5

    println("\n=== TEST PASSED ===")
  }

  // ==========================================================================
  // Helper methods
  // ==========================================================================

  /** List state directories in the transaction log on S3. */
  private def listS3TransactionLogFiles(tablePath: String): Seq[String] = {
    import org.apache.spark.sql.util.CaseInsensitiveStringMap
    import scala.jdk.CollectionConverters._

    val txLogPath = s"$tablePath/_transaction_log"
    val (accessKey, secretKey) = awsCredentials.get

    val options = new CaseInsensitiveStringMap(Map(
      "spark.indextables.aws.accessKey" -> accessKey,
      "spark.indextables.aws.secretKey" -> secretKey,
      "spark.indextables.aws.region" -> S3_REGION
    ).asJava)

    val provider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
      txLogPath, options, spark.sparkContext.hadoopConfiguration)

    try {
      val files = provider.listFiles(txLogPath, recursive = false)
      files
        .filter(f => f.path.contains("state-v") && f.isDirectory)
        .map(_.path)
        .sorted
    } finally {
      provider.close()
    }
  }

  /** Read _last_checkpoint content from S3. */
  private def readLastCheckpoint(tablePath: String): String = {
    import org.apache.spark.sql.util.CaseInsensitiveStringMap
    import scala.jdk.CollectionConverters._

    val lastCheckpointPath = s"$tablePath/_transaction_log/_last_checkpoint"
    val (accessKey, secretKey) = awsCredentials.get

    val options = new CaseInsensitiveStringMap(Map(
      "spark.indextables.aws.accessKey" -> accessKey,
      "spark.indextables.aws.secretKey" -> secretKey,
      "spark.indextables.aws.region" -> S3_REGION
    ).asJava)

    val provider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
      lastCheckpointPath, options, spark.sparkContext.hadoopConfiguration)

    try {
      if (provider.exists(lastCheckpointPath)) {
        new String(provider.readFile(lastCheckpointPath), "UTF-8")
      } else {
        "<not found>"
      }
    } finally {
      provider.close()
    }
  }
}
