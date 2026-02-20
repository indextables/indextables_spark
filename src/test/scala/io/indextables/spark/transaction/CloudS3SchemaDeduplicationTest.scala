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

package io.indextables.spark.transaction

import java.io.{File, FileInputStream}
import java.util.{Properties, UUID}

import scala.util.Using

import io.indextables.spark.CloudS3TestBase

/**
 * Real S3 test for schema deduplication bug.
 *
 * This test reproduces the production bug where:
 *   1. MetadataAction appears in every transaction (not just the first few) 2. After version 11 (post-checkpoint),
 *      writes fail with "Missing schema hashes" 3. Each subsequent failure adds more missing hashes
 *
 * The bug does NOT reproduce on local filesystem but happens on real S3.
 */
class CloudS3SchemaDeduplicationTest extends CloudS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/schema-dedup-test-$testRunId"

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    if (awsCredentials.isDefined) {
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

      println(s"📦 CloudS3SchemaDeduplicationTest: Configured for S3 bucket $S3_BUCKET in region $S3_REGION")
    }
  }

  override def afterAll(): Unit = {
    // Clean up test data
    if (awsCredentials.isDefined) {
      try
        println(s"🧹 Cleaning up test data at $testBasePath")
      // Note: Actual cleanup would require S3 delete operations
      catch {
        case e: Exception =>
          println(s"⚠️ Warning: Failed to clean up test data: ${e.getMessage}")
      }
    }
    super.afterAll()
  }

  /** Load AWS credentials from ~/.aws/credentials file. */
  private def loadAwsCredentials(): Option[(String, String)] = {
    val home     = System.getProperty("user.home")
    val credFile = new File(s"$home/.aws/credentials")

    if (!credFile.exists()) {
      println("⚠️ Warning: ~/.aws/credentials file not found. Test will be skipped.")
      return None
    }

    Using(new FileInputStream(credFile)) { fis =>
      val props = new Properties()
      props.load(fis)

      val accessKey = props.getProperty("aws_access_key_id")
      val secretKey = props.getProperty("aws_secret_access_key")

      if (accessKey != null && secretKey != null) {
        println("✅ Loaded AWS credentials from ~/.aws/credentials")
        Some((accessKey, secretKey))
      } else {
        println("⚠️ Warning: Could not load credentials from ~/.aws/credentials")
        None
      }
    }.getOrElse {
      println("⚠️ Warning: Error reading ~/.aws/credentials file")
      None
    }
  }

  test("schema deduplication with large schema - reproduces production bug on S3") {
    assume(awsCredentials.isDefined, "AWS credentials not available - skipping test")

    val tablePath      = s"$testBasePath/large_schema_test"
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create a DataFrame with a large schema (100 fields) - similar to production
    val numFields  = 100
    val numRecords = 100

    def createLargeDF(batchNum: Int): org.apache.spark.sql.DataFrame = {
      val rows = (1 to numRecords).map { i =>
        val id     = (batchNum - 1) * numRecords + i
        val values = (1 to numFields).map(f => s"value_${id}_$f")
        org.apache.spark.sql.Row.fromSeq(id.toLong +: values)
      }
      val schema = org.apache.spark.sql.types.StructType(
        org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.LongType) +:
          (1 to numFields).map(f =>
            org.apache.spark.sql.types.StructField(s"field_$f", org.apache.spark.sql.types.StringType)
          )
      )
      spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    }

    // First write to create the table
    println(s"📝 Writing batch 1 to $tablePath")
    val df1 = createLargeDF(1)
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.checkpoint.enabled", "true")
      .option("spark.indextables.checkpoint.interval", "10")
      .save(tablePath)

    // Append 12 more times - checkpoint triggers at version 10
    (2 to 13).foreach { i =>
      println(s"📝 Writing batch $i (version $i)")

      try {
        val df = createLargeDF(i)
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("append")
          .option("spark.indextables.checkpoint.enabled", "true")
          .option("spark.indextables.checkpoint.interval", "10")
          .save(tablePath)
        println(s"✅ Batch $i written successfully")
      } catch {
        case e: Exception =>
          println(s"❌ Batch $i FAILED: ${e.getMessage}")
          // If it's the "Missing schema hashes" error, show the full message
          if (e.getMessage.contains("Missing schema hashes")) {
            println(s"💥 REPRODUCTION CONFIRMED: Got 'Missing schema hashes' error at batch $i")
            println(s"   Full error: ${e.getMessage}")

            // This is the expected failure for the bug - we've reproduced it
            // Let's continue and see if more hashes are missing on next attempt
            if (i <= 12) {
              println(s"📝 Attempting batch $i again to see if more hashes are missing...")
              try {
                val df = createLargeDF(i)
                df.write
                  .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
                  .mode("append")
                  .option("spark.indextables.checkpoint.enabled", "true")
                  .option("spark.indextables.checkpoint.interval", "10")
                  .save(tablePath)
              } catch {
                case e2: Exception =>
                  println(s"❌ Second attempt FAILED: ${e2.getMessage}")
                  if (e2.getMessage.length > e.getMessage.length) {
                    println(s"💥 CONFIRMED: Error message grew on retry (more missing hashes)")
                  }
              }
            }
          }
          throw e
      }
    }

    // Verify we can read all data back
    println("📖 Reading back data")
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val count = result.count()
    println(s"✅ Read $count rows")
    assert(count == 13 * numRecords, s"Expected ${13 * numRecords} rows, got $count")
  }

  test("schema deduplication - verify MetadataAction presence in transactions") {
    assume(awsCredentials.isDefined, "AWS credentials not available - skipping test")

    val tablePath      = s"$testBasePath/metadata_presence_test"
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create a simple schema (fewer fields for easier debugging)
    val numFields  = 50
    val numRecords = 10

    def createDF(batchNum: Int): org.apache.spark.sql.DataFrame = {
      val rows = (1 to numRecords).map { i =>
        val id     = (batchNum - 1) * numRecords + i
        val values = (1 to numFields).map(f => s"value_${id}_$f")
        org.apache.spark.sql.Row.fromSeq(id.toLong +: values)
      }
      val schema = org.apache.spark.sql.types.StructType(
        org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.LongType) +:
          (1 to numFields).map(f =>
            org.apache.spark.sql.types.StructField(s"field_$f", org.apache.spark.sql.types.StringType)
          )
      )
      spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    }

    // Write 5 batches (not enough to trigger checkpoint)
    (1 to 5).foreach { i =>
      val mode = if (i == 1) "overwrite" else "append"
      println(s"📝 Writing batch $i (mode=$mode)")

      val df = createDF(i)
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(mode)
        .option("spark.indextables.checkpoint.enabled", "false") // Disable checkpoint for clarity
        .save(tablePath)
    }

    // Now read the transaction log files and check for MetadataAction presence
    val cloudProvider = io.indextables.spark.io.CloudStorageProviderFactory.createProvider(
      tablePath,
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
      spark.sparkContext.hadoopConfiguration
    )

    try {
      val txLogPath        = tablePath + "/_transaction_log"
      var metadataVersions = List.empty[Long]

      (0 to 5).foreach { version =>
        val versionFilePath = f"$txLogPath/$version%020d.json"
        if (cloudProvider.exists(versionFilePath)) {
          val bytes = cloudProvider.readFile(versionFilePath)
          val decompressedBytes =
            io.indextables.spark.transaction.compression.CompressionUtils.readTransactionFile(bytes)
          val content = new String(decompressedBytes, java.nio.charset.StandardCharsets.UTF_8)

          val hasMetadata = content.contains("\"metadataAction\"")
          if (hasMetadata) {
            metadataVersions = metadataVersions :+ version.toLong
            println(s"📋 Version $version: HAS MetadataAction")
          } else {
            println(s"📋 Version $version: NO MetadataAction")
          }
        }
      }

      println(s"\n📊 Summary: MetadataAction found in versions: ${metadataVersions.mkString(", ")}")

      // MetadataAction should ONLY be in version 0 (init) and version 1 (first schema registration)
      // If it appears in every version, that's the bug
      if (metadataVersions.size > 2) {
        println(s"⚠️ WARNING: MetadataAction found in ${metadataVersions.size} versions - this suggests the bug!")
        println(s"   Expected: versions 0-1 only")
        println(s"   Actual: versions ${metadataVersions.mkString(", ")}")
      }

      assert(
        metadataVersions.size <= 2,
        s"MetadataAction should only be in 2 versions (init + first schema), found in ${metadataVersions.size} versions: ${metadataVersions
            .mkString(", ")}"
      )
    } finally
      cloudProvider.close()
  }
}
