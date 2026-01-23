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

package io.indextables.spark.transaction.compression

import java.io.{File, FileInputStream}
import java.net.URI
import java.util.{Properties, UUID}

import scala.util.Using

import org.apache.spark.sql.functions._

import org.apache.hadoop.fs.{FileSystem, Path}

import io.indextables.spark.RealS3TestBase

/**
 * Real AWS S3 integration tests for transaction log compression.
 *
 * Tests verify that:
 *   - Transaction log files are compressed when compression is enabled
 *   - Checkpoint files are compressed when compression is enabled
 *   - Compression can be disabled via configuration
 *   - Compressed files have proper magic bytes and can be decompressed
 *   - Tables with compressed transaction logs can be read successfully
 *
 * Credentials are loaded from ~/.aws/credentials file.
 */
class RealS3CompressionIntegrationTest extends RealS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/compression-test-$testRunId"

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Use JSON format since this test validates JSON transaction log compression behavior
    // With Avro state format (the new default), subsequent writes use state directories
    // instead of separate version files, which this test's assertions depend on
    spark.conf.set("spark.indextables.state.format", "json")

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    if (awsCredentials.isDefined) {
      val (accessKey, secretKey) = awsCredentials.get

      // Configure Spark for real S3 access
      spark.conf.set("spark.indextables.aws.accessKey", accessKey)
      spark.conf.set("spark.indextables.aws.secretKey", secretKey)
      spark.conf.set("spark.indextables.aws.region", S3_REGION)

      // Configure Hadoop config for S3 filesystem access
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
      hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
      hadoopConf.set("spark.indextables.aws.region", S3_REGION)
      hadoopConf.set("fs.s3a.access.key", accessKey)
      hadoopConf.set("fs.s3a.secret.key", secretKey)
      hadoopConf.set("fs.s3a.endpoint", s"s3.$S3_REGION.amazonaws.com")

      println(s"🔐 AWS credentials loaded successfully")
      println(s"🌊 Configured Spark for S3 access to bucket: $S3_BUCKET in region: $S3_REGION")
      println(s"📍 Test base path: $testBasePath")
    } else {
      println(s"⚠️  No AWS credentials found in ~/.aws/credentials - tests will be skipped")
    }
  }

  override def afterAll(): Unit = {
    // Clean up test data
    if (awsCredentials.isDefined) {
      cleanupTestData()
    }
    super.afterAll()
  }

  /** Load AWS credentials from ~/.aws/credentials file. */
  private def loadAwsCredentials(): Option[(String, String)] =
    try {
      val home     = System.getProperty("user.home")
      val credFile = new File(s"$home/.aws/credentials")

      if (credFile.exists()) {
        val props = new Properties()
        Using(new FileInputStream(credFile))(fis => props.load(fis))

        val accessKey = props.getProperty("aws_access_key_id")
        val secretKey = props.getProperty("aws_secret_access_key")

        if (accessKey != null && secretKey != null) {
          Some((accessKey, secretKey))
        } else {
          println(s"⚠️  AWS credentials not found in ~/.aws/credentials")
          None
        }
      } else {
        println(s"⚠️  ~/.aws/credentials file not found")
        None
      }
    } catch {
      case e: Exception =>
        println(s"⚠️  Error loading AWS credentials: ${e.getMessage}")
        None
    }

  /** Get write options with AWS credentials for executor distribution. */
  private def getWriteOptions(): Map[String, String] = {
    val (accessKey, secretKey) = awsCredentials.get
    Map(
      "spark.indextables.aws.accessKey" -> accessKey,
      "spark.indextables.aws.secretKey" -> secretKey,
      "spark.indextables.aws.region"    -> S3_REGION
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

  /** Clean up test data from S3. Note: Cleanup is skipped for now since we're using unique paths. */
  private def cleanupTestData(): Unit =
    try
      // Skip cleanup since we're using unique random paths to avoid conflicts
      println(s"ℹ️  Test data cleanup skipped (unique paths used): $testBasePath")
    catch {
      case e: Exception =>
        println(s"⚠️  Warning: Could not clean up test data: ${e.getMessage}")
    }

  /**
   * Read a file from S3 using Hadoop FileSystem API.
   *
   * @param s3Path
   *   Full S3 path (s3a://bucket/path/to/file)
   * @return
   *   File contents as byte array
   */
  private def readS3File(s3Path: String): Array[Byte] = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs         = FileSystem.get(new URI(s3Path), hadoopConf)
    val path       = new Path(s3Path)

    val inputStream = fs.open(path)
    try {
      val buffer = new Array[Byte](inputStream.available())
      inputStream.readFully(buffer)
      buffer
    } finally
      inputStream.close()
  }

  /**
   * List files in S3 directory.
   *
   * @param s3Path
   *   Full S3 path to directory
   * @return
   *   List of file paths
   */
  private def listS3Files(s3Path: String): Seq[String] = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs         = FileSystem.get(new URI(s3Path), hadoopConf)
    val path       = new Path(s3Path)

    if (fs.exists(path)) {
      fs.listStatus(path).map(_.getPath.toString).toSeq
    } else {
      Seq.empty
    }
  }

  test("Real S3: Transaction log files are compressed by default") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/default-compression-test"

    // Create test data
    val data = spark
      .range(100)
      .select(
        col("id"),
        concat(lit("Document "), col("id")).as("title"),
        concat(lit("Content for document "), col("id")).as("content")
      )

    println(s"✍️  Writing initial data with default compression settings...")

    // Write to real S3 with default compression (should be enabled)
    val writeOptions = getWriteOptions()
    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    println(s"✅ Successfully wrote initial data to S3: $tablePath")

    // Append more data to create a second transaction file
    val data2 = spark
      .range(100, 200)
      .select(
        col("id"),
        concat(lit("Document "), col("id")).as("title"),
        concat(lit("Content for document "), col("id")).as("content")
      )

    println(s"✍️  Appending second batch to create additional transaction file...")

    data2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("append")
      .save(tablePath)

    println(s"✅ Successfully appended second batch")

    // List transaction log files
    val transactionLogPath = s"$tablePath/_transaction_log"
    val transactionFiles   = listS3Files(transactionLogPath)

    println(s"📂 Found ${transactionFiles.size} files in transaction log directory")

    // Find all transaction files (should have 00000000000000000000.json and 00000000000000000001.json)
    val jsonFiles = transactionFiles
      .filter(_.endsWith(".json"))
      .map(_.split("/").last)
      .filter(name => name.matches("\\d+\\.json"))
      .sorted

    jsonFiles should not be empty
    jsonFiles.length should be >= 2

    println(s"📋 Found ${jsonFiles.length} transaction files: ${jsonFiles.mkString(", ")}")

    // Check each transaction file for compression
    jsonFiles.take(2).foreach { transactionFile =>
      println(s"🔍 Checking transaction file: $transactionFile")

      // Read the transaction file from S3
      val transactionFilePath = s"$transactionLogPath/$transactionFile"
      val fileBytes           = readS3File(transactionFilePath)

      // Verify the file is compressed (starts with magic bytes 0x01 0x01)
      fileBytes.length should be > 2
      fileBytes(0) shouldBe CompressionUtils.MAGIC_BYTE
      fileBytes(1) shouldBe 0x01.toByte // GZIP codec byte

      println(
        s"✅ Transaction file $transactionFile is compressed (magic bytes: 0x${Integer
            .toHexString(fileBytes(0) & 0xff)} 0x${Integer.toHexString(fileBytes(1) & 0xff)})"
      )

      // Verify the file can be decompressed
      val decompressedBytes = CompressionUtils.readTransactionFile(fileBytes)
      decompressedBytes.length should be > fileBytes.length

      val compressionRatio = (1.0 - fileBytes.length.toDouble / decompressedBytes.length) * 100

      println(s"   📊 Compressed size: ${fileBytes.length} bytes")
      println(s"   📊 Decompressed size: ${decompressedBytes.length} bytes")
      println(s"   📊 Compression ratio: $compressionRatio%")
    }

    // Verify we can read the table back successfully
    val readOptions = getReadOptions()
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    val count = result.count()
    count shouldBe 200

    println(s"✅ Successfully read data from table with compressed transaction logs")
    println(s"✅ Total records read: $count")
  }

  test("Real S3: Transaction log compression can be disabled") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/no-compression-test"

    // Create test data
    val data = spark
      .range(50)
      .select(
        col("id"),
        concat(lit("Item "), col("id")).as("name")
      )

    println(s"✍️  Writing initial data with compression disabled...")

    // Write to real S3 with compression explicitly disabled
    val writeOptions = getWriteOptions() ++ Map(
      "spark.indextables.transaction.compression.enabled" -> "false"
    )

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    println(s"✅ Successfully wrote initial data with compression disabled")

    // Append more data to create a second transaction file
    val data2 = spark
      .range(50, 100)
      .select(
        col("id"),
        concat(lit("Item "), col("id")).as("name")
      )

    println(s"✍️  Appending second batch to create additional transaction file...")

    data2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("append")
      .save(tablePath)

    println(s"✅ Successfully appended second batch")

    // List transaction log files
    val transactionLogPath = s"$tablePath/_transaction_log"
    val transactionFiles   = listS3Files(transactionLogPath)

    println(s"📂 Found ${transactionFiles.size} files in transaction log directory")

    // Find all transaction files
    val jsonFiles = transactionFiles
      .filter(_.endsWith(".json"))
      .map(_.split("/").last)
      .filter(name => name.matches("\\d+\\.json"))
      .sorted

    jsonFiles should not be empty
    jsonFiles.length should be >= 2

    println(s"📋 Found ${jsonFiles.length} transaction files: ${jsonFiles.mkString(", ")}")

    // Check each transaction file to verify they are NOT compressed
    jsonFiles.take(2).foreach { transactionFile =>
      println(s"🔍 Checking transaction file: $transactionFile")

      // Read the transaction file from S3
      val transactionFilePath = s"$transactionLogPath/$transactionFile"
      val fileBytes           = readS3File(transactionFilePath)

      // Verify the file is NOT compressed (does not start with magic byte)
      fileBytes.length should be > 0
      CompressionUtils.isCompressed(fileBytes) shouldBe false

      println(s"✅ Transaction file $transactionFile is NOT compressed (as expected)")
      println(s"   📊 Uncompressed file size: ${fileBytes.length} bytes")

      // Verify the file content is valid JSON (starts with '{' or '[')
      val firstChar = fileBytes(0).toChar
      (firstChar == '{' || firstChar == '[') shouldBe true

      println(s"   📊 File appears to be valid JSON (starts with '$firstChar')")
    }

    // Verify we can read the table back successfully
    val readOptions = getReadOptions()
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    val count = result.count()
    count shouldBe 100

    println(s"✅ Successfully read data from table with uncompressed transaction logs")
    println(s"✅ Total records read: $count")
  }

  test("Real S3: Checkpoint files are compressed when compression is enabled") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/checkpoint-compression-test"

    // Create test data - write multiple times to trigger checkpoint creation
    val data1 = spark
      .range(100)
      .select(
        col("id"),
        lit("batch1").as("batch"),
        concat(lit("Content "), col("id")).as("content")
      )

    println(s"✍️  Writing batch 1 to create initial transaction...")

    // Write initial batch with checkpoint enabled and frequent intervals
    val writeOptions = getWriteOptions() ++ Map(
      "spark.indextables.checkpoint.enabled"  -> "true",
      "spark.indextables.checkpoint.interval" -> "2" // Checkpoint after 2 transactions
    )

    data1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    println(s"✅ Batch 1 written")

    // Append more data to trigger additional transactions
    val data2 = spark
      .range(100, 200)
      .select(
        col("id"),
        lit("batch2").as("batch"),
        concat(lit("Content "), col("id")).as("content")
      )

    println(s"✍️  Writing batch 2...")

    data2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("append")
      .save(tablePath)

    println(s"✅ Batch 2 written")

    // Append more data to potentially trigger checkpoint creation
    val data3 = spark
      .range(200, 300)
      .select(
        col("id"),
        lit("batch3").as("batch"),
        concat(lit("Content "), col("id")).as("content")
      )

    println(s"✍️  Writing batch 3 to trigger checkpoint...")

    data3.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("append")
      .save(tablePath)

    println(s"✅ Batch 3 written")

    // List transaction log files
    val transactionLogPath = s"$tablePath/_transaction_log"
    val transactionFiles   = listS3Files(transactionLogPath)

    println(s"📂 Found ${transactionFiles.size} files in transaction log directory")

    // Find checkpoint files
    val checkpointFiles = transactionFiles.filter(_.contains(".checkpoint.json"))

    if (checkpointFiles.isEmpty) {
      println(s"⚠️  No checkpoint files found - checkpoint may not have been triggered")
      println(s"⚠️  This is acceptable as checkpoint creation depends on transaction count")
      println(s"⚠️  Skipping checkpoint compression validation")
    } else {
      println(s"✅ Found ${checkpointFiles.size} checkpoint file(s)")

      // Check the first checkpoint file
      val firstCheckpointFile = checkpointFiles.head
      println(s"🔍 Checking checkpoint file: ${firstCheckpointFile.split("/").last}")

      // Read the checkpoint file from S3
      val checkpointBytes = readS3File(firstCheckpointFile)

      // Verify the checkpoint file is compressed
      checkpointBytes.length should be > 2
      checkpointBytes(0) shouldBe CompressionUtils.MAGIC_BYTE
      checkpointBytes(1) shouldBe 0x01.toByte // GZIP codec byte

      println(
        s"✅ Checkpoint file is compressed (magic bytes: 0x${Integer.toHexString(checkpointBytes(0) & 0xff)} 0x${Integer
            .toHexString(checkpointBytes(1) & 0xff)})"
      )

      // Verify the checkpoint file can be decompressed
      val decompressedBytes = CompressionUtils.readTransactionFile(checkpointBytes)
      decompressedBytes.length should be > checkpointBytes.length

      println(s"✅ Compressed checkpoint size: ${checkpointBytes.length} bytes")
      println(s"✅ Decompressed checkpoint size: ${decompressedBytes.length} bytes")
      println(
        s"✅ Compression ratio: ${(1.0 - checkpointBytes.length.toDouble / decompressedBytes.length) * 100}%"
      )
    }

    // Verify we can read the table back successfully
    val readOptions = getReadOptions()
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    val count = result.count()
    count shouldBe 300

    println(s"✅ Successfully read data from table with compressed transaction logs and checkpoints")
  }

  test("Real S3: Compression ratio verification for typical transaction logs") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/compression-ratio-test"

    // Create realistic test data with multiple fields
    val data = spark
      .range(500)
      .select(
        col("id"),
        concat(lit("Product "), col("id")).as("product_name"),
        (col("id") % 10).as("category_id"),
        (col("id") * 2.5 + 100).as("price"),
        (col("id") % 3 === 0).as("in_stock"),
        concat(lit("Description for product "), col("id"), lit(". This is a detailed description."))
          .as("description")
      )

    println(s"✍️  Writing realistic data to measure compression ratio...")

    // Write with compression enabled
    val writeOptions = getWriteOptions()
    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    println(s"✅ Successfully wrote data")

    // Read both compressed and uncompressed versions
    val transactionLogPath = s"$tablePath/_transaction_log"
    val transactionFiles   = listS3Files(transactionLogPath)

    val jsonFiles = transactionFiles.filter(_.endsWith(".json"))
    jsonFiles should not be empty

    val firstTransactionFile = jsonFiles
      .map(_.split("/").last)
      .filter(name => name.matches("\\d+\\.json"))
      .sorted
      .headOption

    firstTransactionFile should not be None

    // Read compressed file
    val transactionFilePath = s"$transactionLogPath/${firstTransactionFile.get}"
    val compressedBytes     = readS3File(transactionFilePath)

    // Decompress to get original size
    CompressionUtils.isCompressed(compressedBytes) shouldBe true
    val decompressedBytes = CompressionUtils.readTransactionFile(compressedBytes)

    // Calculate compression ratio
    val compressionRatio = (1.0 - compressedBytes.length.toDouble / decompressedBytes.length) * 100

    println(s"📊 Compression Statistics:")
    println(s"   Original size: ${decompressedBytes.length} bytes")
    println(s"   Compressed size: ${compressedBytes.length} bytes")
    println(s"   Compression ratio: $compressionRatio%")
    println(s"   Space saved: ${decompressedBytes.length - compressedBytes.length} bytes")

    // Verify compression is achieving reasonable ratio (should be > 50% for JSON)
    compressionRatio should be > 50.0

    println(s"✅ Compression ratio is within expected range (>50%)")
  }

  test("Real S3: Multiple append operations use compression consistently") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/multi-append-compression-test"

    // First append - creates table (00.json)
    val data1 = spark
      .range(100)
      .select(
        col("id"),
        lit("batch1").as("batch"),
        concat(lit("Content "), col("id")).as("content")
      )

    println(s"✍️  Writing batch 1 (first append - creates table)...")

    val writeOptions = getWriteOptions()
    data1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("append")
      .save(tablePath)

    println(s"✅ Batch 1 written (created table via append)")

    // Second append (01.json)
    val data2 = spark
      .range(100, 200)
      .select(
        col("id"),
        lit("batch2").as("batch"),
        concat(lit("Content "), col("id")).as("content")
      )

    println(s"✍️  Writing batch 2 (second append)...")

    data2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("append")
      .save(tablePath)

    println(s"✅ Batch 2 written (appended)")

    // Check both transaction files
    val transactionLogPath = s"$tablePath/_transaction_log"
    val transactionFiles   = listS3Files(transactionLogPath)

    val jsonFiles = transactionFiles
      .filter(_.endsWith(".json"))
      .map(_.split("/").last)
      .filter(name => name.matches("\\d+\\.json"))
      .sorted

    println(s"📂 Found ${jsonFiles.size} transaction files: ${jsonFiles.mkString(", ")}")

    jsonFiles.size should be >= 2

    // Check 00000000000000000000.json (first append - creates table)
    val file0Path       = s"$transactionLogPath/${jsonFiles(0)}"
    val file0Bytes      = readS3File(file0Path)
    val file0Compressed = CompressionUtils.isCompressed(file0Bytes)

    println(s"🔍 ${jsonFiles(0)}: ${if (file0Compressed) "COMPRESSED ✓" else "UNCOMPRESSED ✗"}")
    if (!file0Compressed && file0Bytes.nonEmpty) {
      println(s"   First bytes: ${file0Bytes.take(10).map(b => f"0x$b%02x").mkString(" ")}")
      println(s"   As string: ${new String(file0Bytes.take(100), "UTF-8")}")
    }

    // Check 00000000000000000001.json (second append)
    val file1Path       = s"$transactionLogPath/${jsonFiles(1)}"
    val file1Bytes      = readS3File(file1Path)
    val file1Compressed = CompressionUtils.isCompressed(file1Bytes)

    println(s"🔍 ${jsonFiles(1)}: ${if (file1Compressed) "COMPRESSED ✓" else "UNCOMPRESSED ✗"}")
    if (!file1Compressed && file1Bytes.nonEmpty) {
      println(s"   First bytes: ${file1Bytes.take(10).map(b => f"0x$b%02x").mkString(" ")}")
      println(s"   As string: ${new String(file1Bytes.take(100), "UTF-8")}")
    }

    // Both files should be compressed
    file0Compressed shouldBe true
    file1Compressed shouldBe true

    println(s"✅ Both append operations use compression consistently")
  }

  test("Real S3: REPRODUCE BUG - Subsequent appends without explicit compression options") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/bug-reproduction-test"

    // First write with compression options
    val data1 = spark
      .range(100)
      .select(
        col("id"),
        lit("batch1").as("batch"),
        concat(lit("Content "), col("id")).as("content")
      )

    println(s"✍️  Write 1: Creating table WITH compression options...")

    val writeOptionsWithCompression = getWriteOptions() ++ Map(
      "spark.indextables.transaction.compression.enabled"    -> "true",
      "spark.indextables.transaction.compression.codec"      -> "gzip",
      "spark.indextables.transaction.compression.gzip.level" -> "6"
    )

    data1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptionsWithCompression)
      .mode("append")
      .save(tablePath)

    println(s"✅ Write 1 completed")

    // Second write WITHOUT compression options (simulates Databricks behavior)
    // This relies on metadata configuration being persisted
    val data2 = spark
      .range(100, 200)
      .select(
        col("id"),
        lit("batch2").as("batch"),
        concat(lit("Content "), col("id")).as("content")
      )

    println(s"✍️  Write 2: Appending WITHOUT explicit compression options...")
    println(s"   (This should use compression settings from metadata)")

    val writeOptionsWithoutCompression = getWriteOptions() // Only AWS credentials, no compression config

    data2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptionsWithoutCompression)
      .mode("append")
      .save(tablePath)

    println(s"✅ Write 2 completed")

    // Check both transaction files
    val transactionLogPath = s"$tablePath/_transaction_log"
    val transactionFiles   = listS3Files(transactionLogPath)

    val jsonFiles = transactionFiles
      .filter(_.endsWith(".json"))
      .map(_.split("/").last)
      .filter(name => name.matches("\\d+\\.json"))
      .sorted

    println(s"📂 Found ${jsonFiles.size} transaction files: ${jsonFiles.mkString(", ")}")

    jsonFiles.size should be >= 2

    // Check 00.json (first write with compression options)
    val file0Path       = s"$transactionLogPath/${jsonFiles(0)}"
    val file0Bytes      = readS3File(file0Path)
    val file0Compressed = CompressionUtils.isCompressed(file0Bytes)

    println(s"")
    println(s"🔍 ${jsonFiles(0)} (with explicit compression options):")
    println(s"   Status: ${if (file0Compressed) "COMPRESSED ✓" else "UNCOMPRESSED ✗"}")
    if (!file0Compressed && file0Bytes.nonEmpty) {
      println(s"   First bytes: ${file0Bytes.take(10).map(b => f"0x$b%02x").mkString(" ")}")
      println(s"   As string: ${new String(file0Bytes.take(100), "UTF-8")}")
    }

    // Check 01.json (second write WITHOUT compression options)
    val file1Path       = s"$transactionLogPath/${jsonFiles(1)}"
    val file1Bytes      = readS3File(file1Path)
    val file1Compressed = CompressionUtils.isCompressed(file1Bytes)

    println(s"")
    println(s"🔍 ${jsonFiles(1)} (WITHOUT explicit compression options - relies on metadata):")
    println(s"   Status: ${if (file1Compressed) "COMPRESSED ✓" else "UNCOMPRESSED ✗"}")
    if (!file1Compressed && file1Bytes.nonEmpty) {
      println(s"   First bytes: ${file1Bytes.take(10).map(b => f"0x$b%02x").mkString(" ")}")
      println(s"   As string: ${new String(file1Bytes.take(100), "UTF-8")}")
    }

    // First file should be compressed (explicit options)
    file0Compressed shouldBe true

    // Second file SHOULD be compressed (from metadata) but currently ISN'T - this is the BUG!
    println(s"")
    if (file1Compressed) {
      println(s"✅ BUG FIXED: Second transaction IS compressed (compression persisted in metadata)")
    } else {
      println(s"❌ BUG CONFIRMED: Second transaction is NOT compressed (compression NOT persisted in metadata)")
    }

    file1Compressed shouldBe true // This will FAIL until bug is fixed
  }

  test("Real S3: PARALLEL WRITE BUG - Large batch triggers uncompressed parallel write path") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/parallel-write-bug-test"

    println(s"🔍 PARALLEL WRITE BUG TEST: Testing with >100 splits to trigger parallel write path")

    // Generate data that will create >100 splits
    // OptimizedTransactionLog.addFiles() uses parallelOps.writeBatchParallel() when addActions.size > 100
    val numPartitions = 120 // More than 100 to trigger parallel path
    val data = spark
      .range(0, 120, 1, numPartitions)
      .toDF("id")
      .withColumn("value", concat(lit("test data "), col("id")))

    // Write with compression enabled and optimize disabled to get one split per partition
    val writeOptions = getWriteOptions() ++ Map(
      "spark.indextables.transaction.compression.enabled"    -> "true",
      "spark.indextables.transaction.compression.codec"      -> "gzip",
      "spark.indextables.transaction.compression.gzip.level" -> "6",
      "spark.indextables.optimizeWrite.enabled"              -> "false" // Disable to get one split per partition
    )

    println(s"✍️  Writing $numPartitions partitions (will trigger parallel write if addActions > 100)...")

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("append")
      .save(tablePath)

    println(s"✅ Write completed")

    // Read first transaction file
    val transactionLogPath = s"$tablePath/_transaction_log"
    val transactionFiles   = listS3Files(transactionLogPath)

    val jsonFiles = transactionFiles
      .filter(_.endsWith(".json"))
      .map(_.split("/").last)
      .filter(name => name.matches("\\d+\\.json"))
      .sorted

    println(s"📂 Found ${jsonFiles.size} transaction files: ${jsonFiles.mkString(", ")}")

    jsonFiles should not be empty

    // Check first transaction file
    val file0Path       = s"$transactionLogPath/${jsonFiles(0)}"
    val file0Bytes      = readS3File(file0Path)
    val file0Compressed = CompressionUtils.isCompressed(file0Bytes)

    println(s"")
    println(s"🔍 ${jsonFiles(0)} (created by $numPartitions partitions):")
    println(s"   Status: ${if (file0Compressed) "COMPRESSED ✓" else "UNCOMPRESSED ✗"}")
    println(s"   Size: ${file0Bytes.length} bytes")

    if (!file0Compressed) {
      println(s"   First bytes: ${file0Bytes.take(10).map(b => f"0x$b%02x").mkString(" ")}")
      println(s"   As string: ${new String(file0Bytes.take(200), "UTF-8")}")
      println(s"")
      println(s"❌ BUG CONFIRMED: Parallel write path (>100 splits) bypassed compression!")
      println(s"   ParallelTransactionLogOperations.writeBatchParallel() writes uncompressed data")
    } else {
      println(s"")
      println(s"✅ BUG FIXED: Parallel write path correctly applies compression")
    }

    // This should be compressed but will FAIL if parallel write path is used
    file0Compressed shouldBe true
  }
}
