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

package io.indextables.spark.storage

import io.indextables.spark.RealS3TestBase
import org.apache.spark.sql.functions._
import java.io.{File, FileInputStream}
import java.util.Properties
import java.util.UUID
import scala.util.Using
import java.sql.Date
import java.time.LocalDate

/**
 * Real AWS S3 integration tests using test-tantivy4sparkbucket in us-east-2.
 *
 * Tests all functionality that was previously tested with S3Mock:
 *   - Basic write/read operations
 *   - Multiple data types (string, numeric, boolean, date)
 *   - Complex queries and filtering
 *   - Multiple datasets
 *   - IndexQuery operations
 *   - Cache statistics
 *   - Transaction log verification
 *
 * Credentials are loaded from ~/.aws/credentials file.
 */
class RealS3IntegrationJustMergeTest extends RealS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/real-s3-test-$testRunId"

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    if (awsCredentials.isDefined) {
      val (accessKey, secretKey) = awsCredentials.get

      // Configure Spark for real S3 access (no path-style access needed)
      spark.conf.set("spark.indextables.aws.accessKey", accessKey)
      spark.conf.set("spark.indextables.aws.secretKey", secretKey)
      spark.conf.set("spark.indextables.aws.region", S3_REGION)
      // Note: awsPathStyleAccess deliberately NOT set - real S3 uses virtual-hosted-style URLs

      // ALSO configure Hadoop config so CloudStorageProvider can find the region
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
      hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
      hadoopConf.set("spark.indextables.aws.region", S3_REGION)
      println(s"🔧 DEBUG: Set Hadoop config spark.indextables.aws.region=$S3_REGION")

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

  /**
   * Clean up test data from S3. Note: Cleanup is skipped for now since we're using tantivy4java direct S3 access. Test
   * data will use unique paths to avoid conflicts.
   */
  private def cleanupTestData(): Unit =
    try
      // For now, skip cleanup since we're testing direct S3 access via tantivy4java
      // Test data uses unique random paths to avoid conflicts
      println(s"ℹ️  Test data cleanup skipped (unique paths used): $testBasePath")
    catch {
      case e: Exception =>
        println(s"⚠️  Warning: Could not clean up test data: ${e.getMessage}")
    }

  test("Real S3: MERGE SPLITS basic functionality validation") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/merge-validation-test"

    // Create substantial test data to force multiple splits
    val data = spark
      .range(2000)
      .select(
        col("id"),
        concat(
          lit("This is a comprehensive content string for document "),
          col("id"),
          lit(". It contains substantial text to ensure splits are large enough. "),
          lit("Additional content to reach meaningful split sizes for merge operations. "),
          lit("More text content to create realistic split file sizes.")
        ).as("content"),
        (col("id") % 50).cast("string").as("category")
      )

    println(s"✍️  Writing substantial test data for MERGE SPLITS validation...")

    val writeOptions = getWriteOptions() ++ Map(
      "spark.indextables.indexwriter.batchSize" -> "100" // Force multiple splits
    )

    // Write in multiple phases to ensure multiple splits
    data
      .filter(col("id") < 1000)
      .write
      .format("io.indextables.spark.core.Tantivy4SparkTableProvider")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    data
      .filter(col("id") >= 1000)
      .write
      .format("io.indextables.spark.core.Tantivy4SparkTableProvider")
      .options(writeOptions)
      .mode("append")
      .save(tablePath)

    println(s"✅ Successfully wrote multi-phase data to create multiple splits")

    // Verify data exists before merge
    val readOptions = getReadOptions()
    val preMergeData = spark.read
      .format("io.indextables.spark.core.Tantivy4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    val preMergeCount = preMergeData.count()
    preMergeCount shouldBe 2000

    println(s"✅ Pre-merge verification: $preMergeCount records")

    // Execute MERGE SPLITS command
    println(s"🔧 Executing MERGE SPLITS operation...")
    import io.indextables.spark.sql.Tantivy4SparkSqlParser
    val sqlParser3 = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    val mergeCommand4 = sqlParser3
      .parsePlan(s"MERGE SPLITS '$tablePath' TARGET SIZE 2097152")
      .asInstanceOf[io.indextables.spark.sql.MergeSplitsCommand]
    mergeCommand4.run(spark)

    println(s"✅ MERGE SPLITS operation completed successfully")

    // Verify data integrity after merge
    val postMergeData = spark.read
      .format("io.indextables.spark.core.Tantivy4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    val postMergeCount = postMergeData.count()
    postMergeCount shouldBe 2000

    // Verify data content integrity
    val categoryCheck = postMergeData.filter(col("category") === "25").count()
    categoryCheck should be > 0L

    println(s"✅ Post-merge verification: $postMergeCount records")
    println(s"✅ Data integrity preserved (category 25 records: $categoryCheck)")
    println(s"✅ MERGE SPLITS basic functionality validation successful")
  }
}
