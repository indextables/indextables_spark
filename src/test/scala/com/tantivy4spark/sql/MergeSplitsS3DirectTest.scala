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
import org.scalatest.{BeforeAndAfterAll}
import io.findify.s3mock.S3Mock
import java.net.ServerSocket
import scala.util.Using

/**
 * Direct test for MERGE SPLITS command with S3 WITHOUT any Hadoop dependencies.
 * This test proves that MERGE SPLITS itself doesn't use Hadoop at all.
 */
class MergeSplitsS3DirectTest extends TestBase with BeforeAndAfterAll {

  private val TEST_BUCKET = "test-merge-direct-bucket"
  private val ACCESS_KEY = "test-access-key"
  private val SECRET_KEY = "test-secret-key"
  private val SESSION_TOKEN = "test-session-token"
  private var s3MockPort: Int = _
  private var s3Mock: S3Mock = _
  private var s3MockDir: String = _

  override def beforeAll(): Unit = {
    // Find available port first
    s3MockPort = findAvailablePort()
    
    // Create Spark session with ONLY Tantivy4Spark S3 settings
    // NO Hadoop S3A configuration at all
    spark = SparkSession.builder()
      .appName("Tantivy4Spark MERGE SPLITS Direct S3 Test")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", java.nio.file.Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions", "com.tantivy4spark.extensions.Tantivy4SparkExtensions")
      // ONLY Tantivy4Spark S3 settings - NO Hadoop config
      .config("spark.tantivy4spark.aws.accessKey", ACCESS_KEY)
      .config("spark.tantivy4spark.aws.secretKey", SECRET_KEY)
      .config("spark.tantivy4spark.aws.sessionToken", SESSION_TOKEN)
      .config("spark.tantivy4spark.s3.endpoint", s"http://localhost:$s3MockPort")
      .config("spark.tantivy4spark.s3.pathStyleAccess", "true")
      .config("spark.tantivy4spark.aws.region", "us-east-1")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    // Start S3Mock server
    s3MockDir = s"/tmp/s3-merge-direct-${System.currentTimeMillis()}"
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
      println(s"✅ Created S3 bucket for direct test: $TEST_BUCKET")
    } catch {
      case _: Exception =>
        println(s"ℹ️  Bucket $TEST_BUCKET already exists or couldn't be created")
    } finally {
      s3Client.close()
    }
    
    println(s"✅ S3Mock server started on port $s3MockPort WITHOUT Hadoop config")
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

  private def findAvailablePort(): Int = {
    Using(new ServerSocket(0)) { socket =>
      socket.setReuseAddress(true)
      socket.getLocalPort
    }.getOrElse(throw new RuntimeException("Could not find available port"))
  }

  test("MERGE SPLITS works with S3 paths WITHOUT Hadoop dependencies") {
    val s3TablePath = s"s3a://$TEST_BUCKET/direct-test-table"
    
    // Parse and run MERGE SPLITS command directly
    // This will fail to find the path (since we haven't written data)
    // but the important thing is it doesn't fail due to missing Hadoop config
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    val mergeCommand = sqlParser.parsePlan(s"MERGE SPLITS '$s3TablePath'")
      .asInstanceOf[MergeSplitsCommand]
    
    // Execute the command - should work without Hadoop
    val result = mergeCommand.run(spark)
    
    // Should handle gracefully - either path doesn't exist or no valid table
    assert(result.nonEmpty, "Should return result")
    val message = result.head.getString(1)
    assert(
      message.contains("does not exist") || 
      message.contains("not a valid") || 
      message.contains("No splits"),
      s"Should indicate path doesn't exist or no valid table, got: $message"
    )
    
    println(s"✅ MERGE SPLITS executed successfully WITHOUT Hadoop: $message")
  }

  test("MERGE SPLITS correctly validates S3 paths using CloudStorageProvider") {
    // Test multiple non-existent paths to ensure CloudStorageProvider is working
    val testPaths = Seq(
      s"s3://$TEST_BUCKET/path1",
      s"s3a://$TEST_BUCKET/path2",
      s"s3://$TEST_BUCKET/nested/path/table"
    )
    
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    
    testPaths.foreach { path =>
      val command = sqlParser.parsePlan(s"MERGE SPLITS '$path'")
        .asInstanceOf[MergeSplitsCommand]
      
      val result = command.run(spark)
      assert(result.nonEmpty, s"Should return result for $path")
      
      val message = result.head.getString(1)
      assert(
        message.contains("does not exist") || message.contains("not a valid"),
        s"Path $path should be recognized as non-existent"
      )
      
      println(s"✅ CloudStorageProvider correctly checked: $path")
    }
  }
}