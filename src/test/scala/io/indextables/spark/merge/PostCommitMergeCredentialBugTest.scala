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

package io.indextables.spark.merge

import java.io.{File, FileInputStream}
import java.util.{Properties, UUID}

import scala.util.Using

import io.indextables.spark.TestBase
import org.scalatest.BeforeAndAfterEach

/**
 * Test that reproduces the AWS credential bug in post-commit merge-on-write.
 *
 * BUG: When credentials are passed as write options (not in spark conf), the write succeeds but the post-commit merge
 * fails with: "The authorization header is malformed; a non-empty Access Key (AKID) must be provided"
 *
 * This happens because:
 *   1. Write options are copied to enrichedHadoopConf (works for write) 2. Post-commit merge only receives writeOptions
 *      (CaseInsensitiveStringMap) 3. enrichedHadoopConf is NOT passed to merge executor 4. Merge executor can't find
 *      AWS credentials
 */
class PostCommitMergeCredentialBugTest extends TestBase with BeforeAndAfterEach {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[PostCommitMergeCredentialBugTest])

  // S3 test configuration
  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"s3a://$S3_BUCKET/credential-bug-test-$testRunId"

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials (same as CloudS3 tests)
    awsCredentials = loadAwsCredentials()

    if (awsCredentials.isDefined) {
      logger.info(s"🔐 AWS credentials loaded from ~/.aws/credentials")
      logger.info(s"🌊 Test base path: $testBasePath")
    } else {
      logger.warn(s"⚠️  No AWS credentials found in ~/.aws/credentials - tests will be skipped")
    }
  }

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
          logger.warn(s"⚠️  AWS credentials not found in ~/.aws/credentials")
          None
        }
      } else {
        logger.warn(s"⚠️  ~/.aws/credentials file not found")
        None
      }
    } catch {
      case e: Exception =>
        logger.warn(s"⚠️  Error loading AWS credentials: ${e.getMessage}")
        None
    }

  override def beforeEach(): Unit = {
    super.beforeEach()

    // CRITICAL: Unset any credentials from spark conf and hadoop conf
    // This ensures we're testing that credentials ONLY come from write options
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.unset("spark.indextables.aws.accessKey")
    hadoopConf.unset("spark.indextables.aws.secretKey")
    hadoopConf.unset("spark.indextables.aws.sessionToken")
    hadoopConf.unset("spark.indextables.aws.region")

    try {
      spark.conf.unset("spark.indextables.aws.accessKey")
      spark.conf.unset("spark.indextables.aws.secretKey")
      spark.conf.unset("spark.indextables.aws.sessionToken")
      spark.conf.unset("spark.indextables.aws.region")

      // CRITICAL: Remove the localhost S3 endpoint set by TestBase
      // We need to test with real S3, not a mock endpoint
      spark.conf.unset("spark.indextables.s3.endpoint")
    } catch {
      case _: NoSuchElementException => // Config not set, that's fine
    }

    logger.info(s"🧹 Cleared any existing AWS credentials and endpoint from spark conf and hadoop conf")
  }

  test("REPRODUCE BUG: credentials passed as options fail in post-commit merge") {
    assume(awsCredentials.isDefined, "AWS credentials not available in ~/.aws/credentials")

    val (accessKey, secretKey) = awsCredentials.get
    val tablePath              = s"$testBasePath/bug-reproduction"
    logger.info(s"🔍 REPRODUCING BUG: Credentials ONLY in write options at: $tablePath")

    // Create test data that will trigger merge (many small partitions with low threshold)
    val df = spark
      .range(0, 1000)
      .repartition(20) // Create 20 small partitions to trigger merge
      .selectExpr("id", "CAST(id AS STRING) as text")

    logger.info("📝 Writing data with credentials as OPTIONS (not spark conf)...")

    try {
      // CRITICAL: Pass credentials as OPTIONS to df.write
      // This is the exact usage pattern from the bug report
      df.write
        .format("io.indextables.provider.IndexTablesProvider")
        .mode("overwrite")
        .option("spark.indextables.mergeOnWrite.enabled", "true")
        .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "0.1") // Very low threshold to trigger merge
        .option("spark.indextables.mergeOnWrite.targetSize", "1M")            // Small target for multiple groups
        .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")         // Allow test to run
        // AWS credentials passed as OPTIONS (this is the bug scenario)
        .option("spark.indextables.aws.accessKey", accessKey)
        .option("spark.indextables.aws.secretKey", secretKey)
        .option("spark.indextables.aws.region", S3_REGION)
        .save(tablePath)

      // If we get here, the bug is fixed!
      logger.info("✅ Write and post-commit merge succeeded with credentials as options!")

      // Verify data integrity
      val result = spark.read
        .format("io.indextables.provider.IndexTablesProvider")
        .option("spark.indextables.aws.accessKey", accessKey)
        .option("spark.indextables.aws.secretKey", secretKey)
        .option("spark.indextables.aws.region", S3_REGION)
        .load(tablePath)

      result.count() shouldBe 1000

      logger.info("✅ Data integrity verified - bug is FIXED!")

    } catch {
      case e: Exception if e.getMessage.contains("authorization header is malformed") =>
        logger.error("❌ BUG REPRODUCED: Post-commit merge failed with credential error!")
        logger.error(s"   Error: ${e.getMessage}")
        fail("Post-commit merge failed with AWS credential error - credentials not passed from write options")

      case e: Exception =>
        logger.error(s"❌ Unexpected error: ${e.getMessage}", e)
        throw e

    } finally
      // Cleanup S3 test data
      try {
        import org.apache.hadoop.fs.Path
        val path = new Path(tablePath)
        val fs   = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
        fs.delete(path, true)
        logger.info(s"🧹 Cleaned up test data at: $tablePath")
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to cleanup test data: ${e.getMessage}")
      }
  }

  test("WORKAROUND: credentials in spark conf work (baseline test)") {
    assume(awsCredentials.isDefined, "AWS credentials not available in ~/.aws/credentials")

    val (accessKey, secretKey) = awsCredentials.get
    val testId                 = java.util.UUID.randomUUID().toString.take(8)
    val tablePath              = s"$testBasePath/workaround-$testId"
    logger.info(s"🔍 Testing workaround with credentials in spark conf at: $tablePath")

    // Set credentials in spark conf (workaround approach)
    spark.conf.set("spark.indextables.aws.accessKey", accessKey)
    spark.conf.set("spark.indextables.aws.secretKey", secretKey)
    spark.conf.set("spark.indextables.aws.region", S3_REGION)

    val df = spark
      .range(0, 1000)
      .repartition(20)
      .selectExpr("id", "CAST(id AS STRING) as text")

    try {
      // Write WITHOUT credentials in options (they're in spark conf)
      df.write
        .format("io.indextables.provider.IndexTablesProvider")
        .mode("overwrite")
        .option("spark.indextables.mergeOnWrite.enabled", "true")
        .option("spark.indextables.mergeOnWrite.mergeGroupMultiplier", "0.1")
        .option("spark.indextables.mergeOnWrite.targetSize", "1M")
        .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
        .save(tablePath)

      logger.info("✅ Workaround succeeded: credentials in spark conf work!")

      // Verify
      val result = spark.read
        .format("io.indextables.provider.IndexTablesProvider")
        .load(tablePath)

      result.count() shouldBe 1000

    } finally {
      // Cleanup spark conf
      spark.conf.unset("spark.indextables.aws.accessKey")
      spark.conf.unset("spark.indextables.aws.secretKey")
      spark.conf.unset("spark.indextables.aws.region")

      // Cleanup S3
      try {
        import org.apache.hadoop.fs.Path
        val path = new Path(tablePath)
        val fs   = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
        fs.delete(path, true)
        logger.info(s"🧹 Cleaned up test data at: $tablePath")
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to cleanup test data: ${e.getMessage}")
      }
    }
  }
}
