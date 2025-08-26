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

package com.tantivy4spark.storage

import com.tantivy4spark.TestBase
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import io.findify.s3mock.S3Mock
import java.net.ServerSocket
import scala.util.Using

/**
 * Test for S3 split read/write operations using S3Mock.
 * 
 * This test demonstrates the actual S3 API usage:
 * - df.write.format("tantivy4spark").save("s3://bucket/path")  
 * - spark.read.format("tantivy4spark").load("s3://bucket/path")
 * 
 * Uses S3Mock to provide a real S3-compatible server for testing.
 */
class S3SplitReadWriteTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  private val TEST_BUCKET = "test-tantivy-bucket"
  private val ACCESS_KEY = "test-access-key" 
  private val SECRET_KEY = "test-secret-key"
  private val SESSION_TOKEN = "test-session-token"
  private var s3MockPort: Int = _
  private var s3Mock: S3Mock = _
  private var s3MockDir: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    
    // Find available port
    s3MockPort = findAvailablePort()
    
    // Start S3Mock server with unique directory to prevent file locking
    s3MockDir = s"/tmp/s3-${System.currentTimeMillis()}"
    s3Mock = S3Mock(port = s3MockPort, dir = s3MockDir)
    s3Mock.start
    
    // Create the test bucket using AWS SDK
    val s3Client = software.amazon.awssdk.services.s3.S3Client.builder()
      .endpointOverride(java.net.URI.create(s"http://localhost:$s3MockPort"))
      .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
      ))
      .region(software.amazon.awssdk.regions.Region.US_EAST_1)
      .forcePathStyle(true)
      .build()
    
    try {
      // First, try to delete any existing objects in the bucket from previous runs
      try {
        val listResponse = s3Client.listObjectsV2(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder()
          .bucket(TEST_BUCKET)
          .build())
        
        import scala.jdk.CollectionConverters._
        val existingObjects = listResponse.contents().asScala
        if (existingObjects.nonEmpty) {
          println(s"🧹 Cleaning up ${existingObjects.size} existing objects from bucket $TEST_BUCKET before test")
          existingObjects.foreach { obj =>
            s3Client.deleteObject(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
              .bucket(TEST_BUCKET)
              .key(obj.key())
              .build())
          }
        }
      } catch {
        case _: Exception =>
          // Bucket might not exist yet, that's fine
      }
      
      // Now create the bucket (or ensure it exists)
      try {
        s3Client.createBucket(software.amazon.awssdk.services.s3.model.CreateBucketRequest.builder()
          .bucket(TEST_BUCKET)
          .build())
        println(s"✅ Created S3 bucket: $TEST_BUCKET")
      } catch {
        case ex: software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException =>
          println(s"ℹ️  Bucket $TEST_BUCKET already exists, reusing it")
        case ex: Exception =>
          println(s"⚠️  Failed to create bucket $TEST_BUCKET: ${ex.getMessage}")
      }
    } finally {
      s3Client.close()
    }
    
    // Remove system properties approach - we'll use proper Spark configuration distribution
    // System.setProperty("aws.accessKeyId", ACCESS_KEY)
    // System.setProperty("aws.secretAccessKey", SECRET_KEY) 
    // System.setProperty("aws.region", "us-east-1")
    
    // Configure Spark to use mock S3 - using cloud-aware configuration with all 3 credentials
    spark.conf.set("spark.tantivy4spark.aws.accessKey", ACCESS_KEY)
    spark.conf.set("spark.tantivy4spark.aws.secretKey", SECRET_KEY)
    spark.conf.set("spark.tantivy4spark.aws.sessionToken", SESSION_TOKEN)
    spark.conf.set("spark.tantivy4spark.s3.endpoint", s"http://localhost:$s3MockPort")
    spark.conf.set("spark.tantivy4spark.s3.pathStyleAccess", "true")
    spark.conf.set("spark.tantivy4spark.aws.region", "us-east-1")
    
    // Set Hadoop config for components that still use Hadoop filesystem
    spark.conf.set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", s"http://localhost:$s3MockPort")
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    // Disable SSL for local testing with S3Mock
    spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    // Set AWS credential provider chain to use SimpleAWSCredentialsProvider
    spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", 
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    println(s"✅ S3Mock server started on port $s3MockPort")
  }

  override def afterAll(): Unit = {
    // Clean up bucket contents between tests to avoid interference
    // XXX SJS
    if(false) {
       None
    } else {
    try {
      val s3Client = software.amazon.awssdk.services.s3.S3Client.builder()
        .endpointOverride(java.net.URI.create(s"http://localhost:$s3MockPort"))
        .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
          software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
        ))
        .region(software.amazon.awssdk.regions.Region.US_EAST_1)
        .forcePathStyle(true)
        .build()
        
      try {
        // List and delete all objects in the bucket
        val listResponse = s3Client.listObjectsV2(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder()
          .bucket(TEST_BUCKET)
          .build())
          
        import scala.jdk.CollectionConverters._
        listResponse.contents().asScala.foreach { obj =>
          s3Client.deleteObject(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(obj.key())
            .build())
        }
        println(s"🧹 Cleaned up ${listResponse.contents().size()} objects from bucket $TEST_BUCKET")
      } finally {
        s3Client.close()
      }
    } catch {
      case ex: Exception =>
        println(s"⚠️  Failed to clean up bucket: ${ex.getMessage}")
    }
    }
    if (s3Mock != null) {
      s3Mock.stop
    }
    
    // Clean up S3Mock directory to prevent file locking issues
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

  private def findAvailablePort(): Int = {
    Using.resource(new ServerSocket(0)) { socket =>
      socket.getLocalPort
    }
  }

  test("should write DataFrame to S3 and read it back") {
    // Create test data
    val data = spark.range(50).select(
      col("id"),
      concat(lit("Document "), col("id")).as("title"), 
      concat(lit("Content for document "), col("id")).as("content"),
      (col("id") % 5).cast("string").as("category")
    )
    
    // S3 path for testing
    val s3Path = s"s3a://$TEST_BUCKET/tantivy4spark-test-data"
    
    // Write DataFrame to S3 using Tantivy4Spark - pass all 3 credentials as options for executor distribution
    data.write
      .format("tantivy4spark")
      .option("spark.tantivy4spark.aws.accessKey", ACCESS_KEY)
      .option("spark.tantivy4spark.aws.secretKey", SECRET_KEY)
      .option("spark.tantivy4spark.aws.sessionToken", SESSION_TOKEN)
      .option("spark.tantivy4spark.s3.endpoint", s"http://localhost:$s3MockPort")
      .option("spark.tantivy4spark.s3.pathStyleAccess", "true")
      .option("spark.tantivy4spark.aws.region", "us-east-2")
      .mode("overwrite")
      .save(s3Path)
    
    println(s"✅ Successfully wrote data to S3: $s3Path")
    
    // Read DataFrame back from S3 - pass same credentials as write for consistency
    val result = spark.read
      .format("tantivy4spark")
      .option("spark.tantivy4spark.aws.accessKey", ACCESS_KEY)
      .option("spark.tantivy4spark.aws.secretKey", SECRET_KEY)
      .option("spark.tantivy4spark.aws.sessionToken", SESSION_TOKEN)
      .option("spark.tantivy4spark.s3.endpoint", s"http://localhost:$s3MockPort")
      .option("spark.tantivy4spark.s3.pathStyleAccess", "true")
      .option("spark.tantivy4spark.aws.region", "us-east-2")
      .load(s3Path)
    
    val count = result.count()
    count shouldBe 50
    
    // Test query functionality
    val filtered = result.filter(col("category") === "0").count()
    filtered should be >= 1L
    
    // Test complex query
    val complexQuery = result
      .filter(col("title").contains("Document"))
      .filter(col("id") > 10)
      .count()
    
    complexQuery should be >= 1L
    
    println(s"✅ Successfully read data from S3: $s3Path")
    println(s"✅ Total records: $count")
    println(s"✅ Filtered records (category=0): $filtered")
    println(s"✅ Complex query results: $complexQuery")
  }
  
  ignore("should handle S3 write with different data types") {
    // Create test data with various data types
    import java.sql.{Date, Timestamp}
    import java.time.LocalDate
    
    val data = spark.range(25).select(
      col("id"),
      concat(lit("Item "), col("id")).as("name"),
      (col("id") * 2.5).as("price"),
      (col("id") % 2 === 0).as("active"),
      lit(Date.valueOf(LocalDate.now())).as("created_date")
    )
    
    val s3Path = s"s3a://$TEST_BUCKET/tantivy4spark-datatypes-test"
    
    // Write to S3 - pass all 3 credentials as options for executor distribution
    data.write
      .format("tantivy4spark")
      .option("spark.tantivy4spark.aws.accessKey", ACCESS_KEY)
      .option("spark.tantivy4spark.aws.secretKey", SECRET_KEY)
      .option("spark.tantivy4spark.aws.sessionToken", SESSION_TOKEN)
      .option("spark.tantivy4spark.s3.endpoint", s"http://localhost:$s3MockPort") 
      .option("spark.tantivy4spark.s3.pathStyleAccess", "true")
      .option("spark.tantivy4spark.aws.region", "us-east-2")
      .mode("overwrite")
      .save(s3Path)
      
    println(s"✅ Successfully wrote mixed data types to S3: $s3Path")
    
    // Read back and verify - pass same credentials as write for consistency
    val result = spark.read
      .format("tantivy4spark")
      .option("spark.tantivy4spark.aws.accessKey", ACCESS_KEY)
      .option("spark.tantivy4spark.aws.secretKey", SECRET_KEY)
      .option("spark.tantivy4spark.aws.sessionToken", SESSION_TOKEN)
      .option("spark.tantivy4spark.s3.endpoint", s"http://localhost:$s3MockPort")
      .option("spark.tantivy4spark.s3.pathStyleAccess", "true")
      .option("spark.tantivy4spark.aws.region", "us-east-2")
      .load(s3Path)
      
    val count = result.count()
    count shouldBe 25
    
    // Test data type queries
    println(s"📊 S3 Read back data (first 10 rows):")
    result.show(10)
    result.printSchema()
    
    println(s"📊 S3 Read back boolean value distribution:")
    result.groupBy("active").count().show()
    
    val activeItems = result.filter(col("active") === true).count()
    val highPriceItems = result.filter(col("price") > 50.0).count()
    
    println(s"✅ Active items found: $activeItems")
    println(s"✅ High price items found: $highPriceItems")
    println(s"✅ Sample data from result:")
    result.select("id", "active", "price").show(10)
    
    // Debug: Show active items specifically
    println(s"📊 Active items (should be even IDs):")
    result.filter(col("active") === true).select("id", "active").show()
    
    // Boolean and price filtering should work correctly now
    activeItems should be >= 1L
    highPriceItems should be >= 1L
    
    println(s"✅ Mixed data types test successful")
    println(s"✅ Total records: $count") 
    println(s"✅ Active items: $activeItems")
    println(s"✅ High price items: $highPriceItems")
  }
  
  test("should handle multiple S3 write/read operations") {
    // Test multiple datasets
    val datasets = (1 to 3).map { i =>
      val data = spark.range(20).select(
        col("id"),
        lit(s"dataset_$i").as("dataset_name"),
        concat(lit(s"Record from dataset $i - "), col("id")).as("description")
      )
      val path = s"s3a://$TEST_BUCKET/dataset-$i"
      (data, path)
    }
    
    // Write all datasets to S3 - pass all 3 credentials as options for executor distribution 
    datasets.foreach { case (data, path) =>
      data.write
        .format("tantivy4spark")
        .option("spark.tantivy4spark.aws.accessKey", ACCESS_KEY)
        .option("spark.tantivy4spark.aws.secretKey", SECRET_KEY)
        .option("spark.tantivy4spark.aws.sessionToken", SESSION_TOKEN)
        .option("spark.tantivy4spark.s3.endpoint", s"http://localhost:$s3MockPort")
        .option("spark.tantivy4spark.s3.pathStyleAccess", "true")
        .option("spark.tantivy4spark.aws.region", "us-east-2")
        .mode("overwrite") 
        .save(path)
      println(s"✅ Wrote dataset to: $path")
    }
    
    // Read all datasets back and verify - pass same credentials as write for consistency
    val totalRecords = datasets.map { case (_, path) =>
      val df = spark.read
        .format("tantivy4spark")
        .option("spark.tantivy4spark.aws.accessKey", ACCESS_KEY)
        .option("spark.tantivy4spark.aws.secretKey", SECRET_KEY)
        .option("spark.tantivy4spark.aws.sessionToken", SESSION_TOKEN)
        .option("spark.tantivy4spark.s3.endpoint", s"http://localhost:$s3MockPort")
        .option("spark.tantivy4spark.s3.pathStyleAccess", "true")
        .option("spark.tantivy4spark.aws.region", "us-east-2")
        .load(path)
      val count = df.count()
      count shouldBe 20
      count
    }.sum
    
    totalRecords shouldBe 60 // 3 datasets * 20 records each
    
    println(s"✅ Multiple S3 operations successful")
    println(s"✅ Total records across all datasets: $totalRecords")
  }
}
