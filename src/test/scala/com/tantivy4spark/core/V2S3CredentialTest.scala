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

package com.tantivy4spark.core

import com.tantivy4spark.TestBase
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import io.findify.s3mock.S3Mock
import java.net.ServerSocket
import scala.util.Using
import org.apache.spark.sql.functions._

/**
 * Test to demonstrate the credential propagation issue in V2 read path. Specifically tests that AWS credentials are
 * properly passed from driver to executors when using SplitSearchEngine.fromSplitFile in Tantivy4SparkPartitionReader.
 */
class V2S3CredentialTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  private val TEST_BUCKET       = "test-v2-credential-bucket"
  private val ACCESS_KEY        = "v2-test-access-key"
  private val SECRET_KEY        = "v2-test-secret-key"
  private val SESSION_TOKEN     = "v2-test-session-token"
  private var s3MockPort: Int   = _
  private var s3Mock: S3Mock    = _
  private var s3MockDir: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Find available port
    s3MockPort = findAvailablePort()

    // Start S3Mock server with unique directory to prevent file locking
    s3MockDir = s"/tmp/s3-v2-credential-${System.currentTimeMillis()}"
    s3Mock = S3Mock(port = s3MockPort, dir = s3MockDir)
    s3Mock.start

    // Create S3 client to set up test bucket
    val s3Client = software.amazon.awssdk.services.s3.S3Client
      .builder()
      .endpointOverride(java.net.URI.create(s"http://localhost:$s3MockPort"))
      .credentialsProvider(
        software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
          software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
        )
      )
      .region(software.amazon.awssdk.regions.Region.US_EAST_1)
      .forcePathStyle(true)
      .build()

    try {
      // Create the bucket
      s3Client.createBucket(
        software.amazon.awssdk.services.s3.model.CreateBucketRequest
          .builder()
          .bucket(TEST_BUCKET)
          .build()
      )
      println(s"✅ Created S3 bucket for V2 credential test: $TEST_BUCKET")
    } catch {
      case ex: software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException =>
        println(s"ℹ️  Bucket $TEST_BUCKET already exists, reusing it")
      case ex: Exception =>
        println(s"⚠️  Failed to create bucket $TEST_BUCKET: ${ex.getMessage}")
    } finally
      s3Client.close()

    // Configure Spark session with AWS credentials
    spark.conf.set("spark.indextables.aws.accessKey", ACCESS_KEY)
    spark.conf.set("spark.indextables.aws.secretKey", SECRET_KEY)
    spark.conf.set("spark.indextables.aws.sessionToken", SESSION_TOKEN)
    spark.conf.set("spark.indextables.s3.endpoint", s"http://localhost:$s3MockPort")
    spark.conf.set("spark.indextables.aws.region", "us-east-1")

    println(s"✅ S3Mock server started on port $s3MockPort for V2 credential test")
  }

  override def afterAll(): Unit = {
    if (s3Mock != null) {
      s3Mock.shutdown
    }

    if (s3MockDir != null) {
      try {
        import java.io.File
        def deleteDirectory(file: File): Unit = {
          if (file.isDirectory) {
            file.listFiles().foreach(deleteDirectory)
          }
          file.delete()
        }
        deleteDirectory(new File(s3MockDir))
        println(s"✅ Cleaned up S3Mock directory: $s3MockDir")
      } catch {
        case e: Exception =>
          println(s"⚠️  Failed to clean up S3Mock directory $s3MockDir: ${e.getMessage}")
      }
    }

    super.afterAll()
  }

  private def findAvailablePort(): Int =
    Using.resource(new ServerSocket(0))(socket => socket.getLocalPort)

  ignore("should demonstrate V2 read path credential propagation issue") {
    // Set debug level to see more detailed credential propagation logs
    val originalLevel = spark.sparkContext.getLocalProperty("spark.sql.adaptive.logLevel")
    spark.conf.set("spark.sql.adaptive.logLevel", "DEBUG")
    // Create test data
    val data = spark
      .range(10)
      .select(
        col("id"),
        concat(lit("V2 Test Document "), col("id")).as("title"),
        concat(lit("V2 test content for document "), col("id")).as("content")
      )

    val s3Path = s"s3a://$TEST_BUCKET/v2-credential-test-data"

    // Write using V2 API (this should work since write path was already fixed)
    data.write
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider") // Force V2 API
      .option("spark.indextables.aws.accessKey", ACCESS_KEY)
      .option("spark.indextables.aws.secretKey", SECRET_KEY)
      .option("spark.indextables.aws.sessionToken", SESSION_TOKEN)
      .option("spark.indextables.s3.endpoint", s"http://localhost:$s3MockPort")
      .option("spark.indextables.aws.region", "us-east-1")
      .mode("overwrite")
      .save(s3Path)

    println(s"✅ Successfully wrote data to S3 using V2 API: $s3Path")

    // Read using V2 API - this is where the credential propagation issue should manifest
    val result = spark.read
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider") // Force V2 API
      .option("spark.indextables.aws.accessKey", ACCESS_KEY)
      .option("spark.indextables.aws.secretKey", SECRET_KEY)
      .option("spark.indextables.aws.sessionToken", SESSION_TOKEN)
      .option("spark.indextables.s3.endpoint", s"http://localhost:$s3MockPort")
      .option("spark.indextables.aws.region", "us-east-1")
      .load(s3Path)

    // This should fail if credentials aren't properly propagated to SplitSearchEngine.fromSplitFile
    println(s"🔍 Attempting to read data back using V2 API...")
    val count = result.count()
    println(s"✅ Successfully read $count records using V2 API")
    count shouldBe 10

    // Test that data access actually works
    result.show(5, false)

    // Test filtering to ensure SplitSearchEngine is actually being used
    val filtered = result.filter(col("id") < 5).count()
    println(s"✅ Successfully filtered data: $filtered records with id < 5")
    filtered shouldBe 5
  }

  ignore("should verify credential propagation in broadcast configuration") {
    // This test specifically examines the broadcast configuration path
    val data = spark
      .range(5)
      .select(
        col("id"),
        lit("Broadcast Test").as("title")
      )

    val s3Path = s"s3a://$TEST_BUCKET/broadcast-credential-test"

    // Write data first
    data.write
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", ACCESS_KEY)
      .option("spark.indextables.aws.secretKey", SECRET_KEY)
      .option("spark.indextables.aws.sessionToken", SESSION_TOKEN)
      .option("spark.indextables.aws.endpoint", s"http://localhost:$s3MockPort")
      .option("spark.indextables.aws.region", "us-east-1")
      .mode("overwrite")
      .save(s3Path)

    // Clear any existing Spark session config to force reliance on broadcast
    val originalAccessKey    = spark.conf.getOption("spark.indextables.aws.accessKey")
    val originalSecretKey    = spark.conf.getOption("spark.indextables.aws.secretKey")
    val originalSessionToken = spark.conf.getOption("spark.indextables.aws.sessionToken")

    try {
      // Remove session-level config to force broadcast usage
      spark.conf.unset("spark.indextables.aws.accessKey")
      spark.conf.unset("spark.indextables.aws.secretKey")
      spark.conf.unset("spark.indextables.aws.sessionToken")

      // This read should rely entirely on broadcast config from read options
      val result = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.indextables.aws.accessKey", ACCESS_KEY)
        .option("spark.indextables.aws.secretKey", SECRET_KEY)
        .option("spark.indextables.aws.sessionToken", SESSION_TOKEN)
        .option("spark.indextables.s3.endpoint", s"http://localhost:$s3MockPort")
        .option("spark.indextables.aws.region", "us-east-1")
        .load(s3Path)

      println(s"🔍 Testing broadcast credential propagation...")
      val count = result.count()
      println(s"✅ Broadcast credential propagation test passed: $count records")
      count shouldBe 5

    } finally {
      // Restore original session config
      originalAccessKey.foreach(spark.conf.set("spark.indextables.aws.accessKey", _))
      originalSecretKey.foreach(spark.conf.set("spark.indextables.aws.secretKey", _))
      originalSessionToken.foreach(spark.conf.set("spark.indextables.aws.sessionToken", _))
    }
  }
}
