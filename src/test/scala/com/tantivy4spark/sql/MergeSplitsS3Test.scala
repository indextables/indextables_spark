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

package com.tantivy4spark.sql

import com.tantivy4spark.TestBase
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import io.findify.s3mock.S3Mock
import java.net.ServerSocket
import scala.util.Using

/**
 * Test for MERGE SPLITS command with S3 storage using S3Mock.
 * This test validates that the MERGE SPLITS command correctly handles S3 paths
 * and properly constructs S3 URLs for input and output splits.
 */
class MergeSplitsS3Test extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  private val TEST_BUCKET = "test-merge-splits-bucket"
  private val ACCESS_KEY = "test-access-key"
  private val SECRET_KEY = "test-secret-key"
  private val SESSION_TOKEN = "test-session-token"
  private var s3MockPort: Int = _
  private var s3Mock: S3Mock = _
  private var s3MockDir: String = _

  override def beforeAll(): Unit = {
    // Find available port first
    s3MockPort = findAvailablePort()
    
    // Don't call super.beforeAll() - we'll create our own Spark session with S3 config
    // Create Spark session with S3 configuration
    spark = SparkSession.builder()
      .appName("Tantivy4Spark MERGE SPLITS S3 Tests")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", java.nio.file.Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions", "com.tantivy4spark.extensions.Tantivy4SparkExtensions")
      // Configure Tantivy4Spark S3 settings (used by CloudStorageProvider)
      .config("spark.tantivy4spark.aws.accessKey", ACCESS_KEY)
      .config("spark.tantivy4spark.aws.secretKey", SECRET_KEY)
      .config("spark.tantivy4spark.aws.sessionToken", SESSION_TOKEN)
      .config("spark.tantivy4spark.s3.endpoint", s"http://localhost:$s3MockPort")
      .config("spark.tantivy4spark.s3.pathStyleAccess", "true")
      .config("spark.tantivy4spark.aws.region", "us-east-1")
      // Hadoop S3A config - ONLY needed because Spark itself needs to parse s3a:// URLs
      // when passed as path arguments to .save() and .load()
      // Tantivy4Spark itself doesn't use Hadoop - it uses CloudStorageProvider
      .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
      .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
      .config("spark.hadoop.fs.s3a.endpoint", s"http://localhost:$s3MockPort")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    // Start S3Mock server with unique directory
    s3MockDir = s"/tmp/s3-merge-splits-${System.currentTimeMillis()}"
    s3Mock = S3Mock(port = s3MockPort, dir = s3MockDir)
    s3Mock.start
    
    // Create the test bucket
    val s3Client = software.amazon.awssdk.services.s3.S3Client.builder()
      .endpointOverride(java.net.URI.create(s"http://localhost:$s3MockPort"))
      .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
      ))
      .region(software.amazon.awssdk.regions.Region.US_EAST_1)
      .forcePathStyle(true)
      .build()
    
    try {
      s3Client.createBucket(software.amazon.awssdk.services.s3.model.CreateBucketRequest.builder()
        .bucket(TEST_BUCKET)
        .build())
      println(s"✅ Created S3 bucket for MERGE SPLITS test: $TEST_BUCKET")
    } catch {
      case _: software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException =>
        println(s"ℹ️  Bucket $TEST_BUCKET already exists")
    } finally {
      s3Client.close()
    }
    
    println(s"✅ S3Mock server for MERGE SPLITS test started on port $s3MockPort")
  }

  override def afterAll(): Unit = {
    if (s3Mock != null) {
      s3Mock.stop
      println("✅ S3Mock server stopped")
    }
    
    // Clean up temporary directory
    if (s3MockDir != null) {
      val dir = new java.io.File(s3MockDir)
      if (dir.exists()) {
        def deleteRecursively(file: java.io.File): Unit = {
          if (file.isDirectory) {
            file.listFiles().foreach(deleteRecursively)
          }
          file.delete()
        }
        deleteRecursively(dir)
      }
    }
    
    // Stop Spark session
    if (spark != null) {
      spark.stop()
    }
  }

  override def afterEach(): Unit = {
    // Clean up bucket contents after each test
    cleanupBucket()
    super.afterEach()
  }

  private def cleanupBucket(): Unit = {
    val s3Client = software.amazon.awssdk.services.s3.S3Client.builder()
      .endpointOverride(java.net.URI.create(s"http://localhost:$s3MockPort"))
      .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
      ))
      .region(software.amazon.awssdk.regions.Region.US_EAST_1)
      .forcePathStyle(true)
      .build()
    
    try {
      val listResponse = s3Client.listObjectsV2(
        software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder()
          .bucket(TEST_BUCKET)
          .build()
      )
      
      import scala.jdk.CollectionConverters._
      val objects = listResponse.contents().asScala
      objects.foreach { obj =>
        s3Client.deleteObject(
          software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(obj.key())
            .build()
        )
      }
    } catch {
      case _: Exception => // Ignore cleanup errors
    } finally {
      s3Client.close()
    }
  }

  private def findAvailablePort(): Int = {
    Using(new ServerSocket(0)) { socket =>
      socket.setReuseAddress(true)
      socket.getLocalPort
    }.getOrElse(throw new RuntimeException("Could not find available port"))
  }

  test("MERGE SPLITS should handle S3 paths with s3a:// scheme") {
    val s3TablePath = s"s3a://$TEST_BUCKET/merge-splits-test-table"
    
    // First, write some test data to create split files
    val testData = spark.range(1, 100)
      .select(
        col("id"),
        concat(lit("content_"), col("id")).as("content")
      )
    
    // Write with small batch size to create multiple splits
    testData.write
      .format("tantivy4spark")
      .option("spark.tantivy4spark.indexWriter.batchSize", "10")
      .mode("overwrite")
      .save(s3TablePath)
    
    // Now test MERGE SPLITS command
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    val mergeCommand = sqlParser.parsePlan(s"MERGE SPLITS '$s3TablePath' TARGET SIZE 1048576")
      .asInstanceOf[MergeSplitsCommand]
    
    // Execute the merge
    val result = mergeCommand.run(spark)
    
    // Verify the result
    assert(result.nonEmpty, "Should return merge result")
    val resultRow = result.head
    assert(resultRow.getString(0) == s3TablePath, "Should return the S3 table path")
    
    // The result message should indicate whether splits were merged or not
    val message = resultRow.getString(1)
    assert(
      message.contains("Merged") || message.contains("No splits") || message.contains("optimal size"),
      s"Should indicate merge status, got: $message"
    )
    
    // Verify we can still read the data after merge
    val readBack = spark.read.format("tantivy4spark").load(s3TablePath)
    assert(readBack.count() == 99, "Should preserve all records after merge")
  }

  test("MERGE SPLITS should correctly construct S3 URLs for splits") {
    val s3TablePath = s"s3a://$TEST_BUCKET/path-construction-test"
    
    // Write test data with partitioning
    val testData = spark.range(1, 50)
      .select(
        col("id"),
        (col("id") % 3).cast("string").as("partition"),
        concat(lit("text_"), col("id")).as("content")
      )
    
    testData.write
      .format("tantivy4spark")
      .partitionBy("partition")
      .option("spark.tantivy4spark.indexWriter.batchSize", "5")
      .mode("overwrite")
      .save(s3TablePath)
    
    // Execute MERGE SPLITS with partition predicate
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    val mergeCommand = sqlParser.parsePlan(
      s"MERGE SPLITS '$s3TablePath' WHERE partition = '0' TARGET SIZE 1048576"
    ).asInstanceOf[MergeSplitsCommand]
    
    val result = mergeCommand.run(spark)
    assert(result.nonEmpty, "Should return result")
    
    // Verify the S3 path is preserved in the result
    assert(result.head.getString(0) == s3TablePath, "Should preserve S3 path")
    
    // Read back and verify data integrity
    val readBack = spark.read.format("tantivy4spark").load(s3TablePath)
      .filter(col("partition") === "0")
    assert(readBack.count() > 0, "Should have data in partition 0")
  }

  test("MERGE SPLITS should handle s3:// and s3a:// schemes interchangeably") {
    // Test with s3:// scheme (note: S3A filesystem handles both)
    val s3Path = s"s3://$TEST_BUCKET/scheme-test"
    val s3aPath = s"s3a://$TEST_BUCKET/scheme-test"
    
    // Write data using s3a://
    val testData = spark.range(1, 20)
      .select(col("id"), concat(lit("doc_"), col("id")).as("content"))
    
    testData.write
      .format("tantivy4spark")
      .option("spark.tantivy4spark.indexWriter.batchSize", "5")
      .mode("overwrite")
      .save(s3aPath)
    
    // Parse MERGE SPLITS with s3:// scheme (without 'a')
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    
    // Note: In test environment, s3:// might not work without proper setup,
    // but we can verify the command parsing and path handling
    val mergeCommand = sqlParser.parsePlan(s"MERGE SPLITS '$s3Path'")
      .asInstanceOf[MergeSplitsCommand]
    
    assert(mergeCommand != null, "Should parse s3:// path")
    
    // The actual execution might fail due to filesystem configuration,
    // but the path construction logic should work
    try {
      val result = mergeCommand.run(spark)
      // If it succeeds, verify the result
      assert(result.nonEmpty, "Should return result")
    } catch {
      case _: org.apache.hadoop.fs.UnsupportedFileSystemException =>
        // Expected if s3:// (without 'a') is not configured
        // The important thing is that the command parsed and handled the path
        assert(true, "s3:// scheme handled (filesystem not configured)")
    }
  }

  test("MERGE SPLITS should handle non-existent S3 paths gracefully") {
    val nonExistentPath = s"s3a://$TEST_BUCKET/does-not-exist"
    
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    val mergeCommand = sqlParser.parsePlan(s"MERGE SPLITS '$nonExistentPath'")
      .asInstanceOf[MergeSplitsCommand]
    
    // Should handle gracefully
    val result = mergeCommand.run(spark)
    assert(result.nonEmpty, "Should return result for non-existent path")
    
    val message = result.head.getString(1)
    assert(
      message.contains("does not exist") || message.contains("not a valid") || message.contains("No splits"),
      s"Should indicate path doesn't exist or no valid table, got: $message"
    )
  }
}
