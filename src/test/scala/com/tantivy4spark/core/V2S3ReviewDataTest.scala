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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import io.findify.s3mock.S3Mock
import java.net.ServerSocket
import scala.util.Using

/**
 * Test V2 DataSource with S3 storage using review data with date filtering.
 * 
 * This test validates that the V2 DataSource correctly handles:
 * - S3 storage operations
 * - Review data with mixed data types (text, integer, date strings)
 * - String date filtering and pushdown
 */
class V2S3ReviewDataTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

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
    
    // Start S3Mock server
    s3MockDir = s"/tmp/s3-${System.currentTimeMillis()}"
    s3Mock = S3Mock(port = s3MockPort, dir = s3MockDir)
    s3Mock.start
    
    // Create test bucket
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
    } catch {
      case _: software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException => // OK
    } finally {
      s3Client.close()
    }
    
    // Configure Spark for S3Mock
    spark.conf.set("spark.tantivy4spark.aws.accessKey", ACCESS_KEY)
    spark.conf.set("spark.tantivy4spark.aws.secretKey", SECRET_KEY)
    spark.conf.set("spark.tantivy4spark.aws.sessionToken", SESSION_TOKEN)
    spark.conf.set("spark.tantivy4spark.s3.endpoint", s"http://localhost:$s3MockPort")
    spark.conf.set("spark.tantivy4spark.s3.pathStyleAccess", "true")
    spark.conf.set("spark.tantivy4spark.aws.region", "us-east-1")
  }

  override def afterAll(): Unit = {
    if (s3Mock != null) {
      s3Mock.stop
    }
    super.afterAll()
  }

  private def findAvailablePort(): Int = {
    Using.resource(new ServerSocket(0)) { socket =>
      socket.getLocalPort
    }
  }

  ignore("should write and read review data locally with date filtering") {
    val localPath = s"file:///tmp/review_data_test_${System.currentTimeMillis()}"
    
    // Define schema explicitly (signup_date will be converted from String to Date)
    val schema = StructType(Array(
      StructField("review_text", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("signup_date", StringType, nullable = false)
    ))
    
    // Create test data with 10 rows and various dates
    val reviewData = Seq(
      ("Great product, highly recommend!", 25, "2024-01-15"),
      ("Not bad, could be better", 34, "2024-01-20"),
      ("Excellent service and quality", 42, "2024-01-15"),
      ("Poor experience overall", 29, "2024-02-01"),
      ("Amazing value for money", 31, "2024-01-15"),
      ("Could not be happier", 38, "2024-02-10"),
      ("Disappointed with purchase", 45, "2024-01-20"),
      ("Fantastic product!", 27, "2024-01-15"),
      ("Average quality", 33, "2024-02-01"),
      ("Will buy again", 36, "2024-02-10")
    )
    
    val rowData = reviewData.map { case (text, age, date) => Row(text, age, date) }
    val initialDf = spark.createDataFrame(
      spark.sparkContext.parallelize(rowData),
      schema
    )
    
    // Convert signup_date from String to Date type
    val df = initialDf.withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
    
    println(s"🔍 TEST: Writing data with schema: ${df.schema}")
    
    // Write data locally using V2 DataSource
    df.write
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .mode("overwrite")
      .save(localPath)
    
    // Read data back locally
    val readDf = spark.read
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .load(localPath)
    
    println(s"🔍 TEST: Read data with schema: ${readDf.schema}")
    
    // Verify basic read functionality
    val totalRows = readDf.count()
    println(s"🔍 TEST: Total rows read: $totalRows")
    totalRows shouldBe 10
    
    // Test date filtering - should return 4 rows with signup_date='2024-01-15'
    println(s"🔍 TEST: About to apply date filter...")
    val dateFilteredDf = readDf.filter("signup_date = '2024-01-15'")
    println(s"🔍 TEST: Date filter applied using SQL string, collecting results...")
    val filteredRows = dateFilteredDf.collect()
    
    // Validate correct number of rows returned
    println(s"🔍 TEST: Found ${filteredRows.length} rows, expecting 4")
    filteredRows.length shouldBe 4
    
    // Validate that all returned rows have the correct signup_date
    for (row <- filteredRows) {
      row.getDate(2) shouldBe java.sql.Date.valueOf("2024-01-15")
    }
    
    // Verify the specific reviews that should be returned
    val reviewTexts = filteredRows.map(_.getString(0)).sorted
    val expectedTexts = Array(
      "Amazing value for money",
      "Excellent service and quality", 
      "Fantastic product!",
      "Great product, highly recommend!"
    ).sorted
    
    reviewTexts shouldBe expectedTexts
    
    // Test another date filter - should return 2 rows with signup_date='2024-01-20'
    val dateFilter2Df = readDf.filter("signup_date = '2024-01-20'")
    val filtered2Rows = dateFilter2Df.collect()
    
    filtered2Rows.length shouldBe 2
    filtered2Rows.foreach { row =>
      row.getDate(2) shouldBe java.sql.Date.valueOf("2024-01-20")
    }
    
    // Test age filtering combined with date filtering
    val combinedFilterDf = readDf.filter("signup_date = '2024-01-15' AND age > 30")
    val combinedRows = combinedFilterDf.collect()
    
    // Should return 1 row: "Excellent service and quality", age 42, "2024-01-15"
    combinedRows.length shouldBe 1
    combinedRows(0).getString(0) shouldBe "Excellent service and quality"
    combinedRows(0).getInt(1) shouldBe 42
    combinedRows(0).getDate(2) shouldBe java.sql.Date.valueOf("2024-01-15")
    
    // Verify schema is preserved (signup_date is now DateType)
    val expectedSchema = StructType(Array(
      StructField("review_text", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true), 
      StructField("signup_date", DateType, nullable = true)
    ))
    
    readDf.schema shouldBe expectedSchema
  }
  
  ignore("should handle empty results for non-matching date filter") {
    val s3Path = s"s3a://$TEST_BUCKET/review_data_empty_test"
    
    // Create small dataset
    val reviewData = Seq(
      ("Test review", 25, "2024-01-15"),
      ("Another review", 30, "2024-02-01")
    )
    
    val schema = StructType(Array(
      StructField("review_text", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("signup_date", StringType, nullable = false)
    ))
    
    val rowData = reviewData.map { case (text, age, date) => Row(text, age, date) }
    val initialDf = spark.createDataFrame(
      spark.sparkContext.parallelize(rowData),
      schema
    )
    
    // Convert signup_date from String to Date type
    val df = initialDf.withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
    
    df.write
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .option("spark.tantivy4spark.aws.accessKey", ACCESS_KEY)
      .option("spark.tantivy4spark.aws.secretKey", SECRET_KEY)
      .option("spark.tantivy4spark.aws.sessionToken", SESSION_TOKEN)
      .option("spark.tantivy4spark.s3.endpoint", s"http://localhost:$s3MockPort")
      .option("spark.tantivy4spark.s3.pathStyleAccess", "true")
      .option("spark.tantivy4spark.aws.region", "us-east-1")
      .mode("overwrite")
      .save(s3Path)
    
    val readDf = spark.read
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .option("spark.tantivy4spark.aws.accessKey", ACCESS_KEY)
      .option("spark.tantivy4spark.aws.secretKey", SECRET_KEY)
      .option("spark.tantivy4spark.aws.sessionToken", SESSION_TOKEN)
      .option("spark.tantivy4spark.s3.endpoint", s"http://localhost:$s3MockPort")
      .option("spark.tantivy4spark.s3.pathStyleAccess", "true")
      .option("spark.tantivy4spark.aws.region", "us-east-1")
      .load(s3Path)
    
    // Test filtering for a date that doesn't exist
    val emptyResults = readDf.filter("signup_date = '2025-12-31'").collect()
    emptyResults.length shouldBe 0
  }
  
  ignore("should validate text search functionality on review data") {
    val s3Path = s"s3a://$TEST_BUCKET/review_text_search_test"
    
    val reviewData = Seq(
      ("Great product, highly recommend!", 25, "2024-01-15"),
      ("Excellent service and quality", 42, "2024-01-15"),
      ("Poor experience overall", 29, "2024-02-01"),
      ("Amazing product quality", 31, "2024-01-20"),
      ("Good service", 38, "2024-02-10")
    )
    
    val schema = StructType(Array(
      StructField("review_text", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("signup_date", StringType, nullable = false)
    ))
    
    val rowData = reviewData.map { case (text, age, date) => Row(text, age, date) }
    val initialDf = spark.createDataFrame(
      spark.sparkContext.parallelize(rowData),
      schema
    )
    
    // Convert signup_date from String to Date type
    val df = initialDf.withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
    
    df.write
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .option("spark.tantivy4spark.aws.accessKey", ACCESS_KEY)
      .option("spark.tantivy4spark.aws.secretKey", SECRET_KEY)
      .option("spark.tantivy4spark.aws.sessionToken", SESSION_TOKEN)
      .option("spark.tantivy4spark.s3.endpoint", s"http://localhost:$s3MockPort")
      .option("spark.tantivy4spark.s3.pathStyleAccess", "true")
      .option("spark.tantivy4spark.aws.region", "us-east-1")
      .mode("overwrite")
      .save(s3Path)
    
    val readDf = spark.read
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .option("spark.tantivy4spark.aws.accessKey", ACCESS_KEY)
      .option("spark.tantivy4spark.aws.secretKey", SECRET_KEY)
      .option("spark.tantivy4spark.aws.sessionToken", SESSION_TOKEN)
      .option("spark.tantivy4spark.s3.endpoint", s"http://localhost:$s3MockPort")
      .option("spark.tantivy4spark.s3.pathStyleAccess", "true")
      .option("spark.tantivy4spark.aws.region", "us-east-1")
      .load(s3Path)
    
    // Test text filtering - should find reviews containing "product"
    val textFilterDf = readDf.filter("review_text LIKE '%product%'")
    val textResults = textFilterDf.collect()
    
    // Should return 2 rows containing "product"
    textResults.length shouldBe 2
    textResults.foreach { row =>
      row.getString(0) should include("product")
    }
  }
}