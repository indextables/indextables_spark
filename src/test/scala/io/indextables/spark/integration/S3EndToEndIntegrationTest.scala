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

package io.indextables.spark.integration

import java.io.{File, FileInputStream}
import java.util.Properties
import java.util.UUID

// Import Scala collection conversions and implicits
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import io.indextables.spark.TestBase
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * End-to-end integration test with real AWS S3 bucket.
 *
 * Tests the complete workflow:
 *   1. Load AWS credentials from ~/.aws/credentials 2. Write data to S3 bucket s3://tantivy4java-testing 3. Read data
 *      back from S3 4. Perform searches and queries 5. Test cache statistics 6. Clean up test data
 *
 * This test uses a real S3 bucket in us-east-2 region.
 */
class S3EndToEndIntegrationTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test path to avoid conflicts
  private val testRunId     = UUID.randomUUID().toString.substring(0, 8)
  private val testTablePath = s"$S3_BASE_PATH/integration-test-$testRunId"

  private var awsCredentials: Option[AWSCredentials] = None

  case class AWSCredentials(
    accessKey: String,
    secretKey: String,
    sessionToken: Option[String] = None)

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAWSCredentials()

    awsCredentials match {
      case Some(creds) =>
        println(s"🔐 AWS credentials loaded successfully")
        configureSparkForS3(creds)

      case None =>
        fail("Could not load AWS credentials from ~/.aws/credentials. Please ensure credentials are configured.")
    }
  }

  override def afterAll(): Unit = {
    // Clean up test data
    cleanupTestData()
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Ensure clean state for each test
    spark.sparkContext.setLogLevel("WARN")
  }

  /** Load AWS credentials from ~/.aws/credentials file */
  private def loadAWSCredentials(): Option[AWSCredentials] = {
    val credentialsFile = new File(System.getProperty("user.home"), ".aws/credentials")

    if (!credentialsFile.exists()) {
      println(s"❌ AWS credentials file not found: ${credentialsFile.getPath}")
      return None
    }

    Try {
      val props       = new Properties()
      val inputStream = new FileInputStream(credentialsFile)
      try {
        props.load(inputStream)

        // Try default profile first, then any available profile
        val profiles = Seq("default", "testing", "dev").map { profile =>
          val accessKey = Option(props.getProperty(s"${profile}_aws_access_key_id"))
            .orElse(Option(props.getProperty("aws_access_key_id")))
          val secretKey = Option(props.getProperty(s"${profile}_aws_secret_access_key"))
            .orElse(Option(props.getProperty("aws_secret_access_key")))
          val sessionToken = Option(props.getProperty(s"${profile}_aws_session_token"))
            .orElse(Option(props.getProperty("aws_session_token")))

          (accessKey, secretKey) match {
            case (Some(ak), Some(sk)) => Some(AWSCredentials(ak, sk, sessionToken))
            case _                    => None
          }
        }

        profiles.flatten.headOption

      } finally
        inputStream.close()
    } match {
      case Success(creds) => creds
      case Failure(e) =>
        println(s"❌ Failed to load AWS credentials: ${e.getMessage}")
        None
    }
  }

  /** Configure Spark session with AWS credentials for S3 access */
  private def configureSparkForS3(creds: AWSCredentials): Unit = {
    // Configure Hadoop AWS settings
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", creds.accessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", creds.secretKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.region", S3_REGION)

    // Configure session token if available (for temporary credentials)
    creds.sessionToken.foreach { token =>
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", token)
      spark.sparkContext.hadoopConfiguration
        .set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    }

    // Configure IndexTables4Spark S3 settings
    spark.conf.set("spark.indextables.aws.accessKey", creds.accessKey)
    spark.conf.set("spark.indextables.aws.secretKey", creds.secretKey)
    spark.conf.set("spark.indextables.aws.region", S3_REGION)

    creds.sessionToken.foreach(token => spark.conf.set("spark.indextables.aws.sessionToken", token))

    // Enable S3A optimizations
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.multipart.size", "67108864") // 64MB
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "bytebuffer")

    println(s"🌊 Configured Spark for S3 access to bucket: $S3_BUCKET in region: $S3_REGION")
  }

  /** Clean up test data from S3 */
  private def cleanupTestData(): Unit =
    try {
      println(s"🧹 Cleaning up test data from: $testTablePath")

      // Use Hadoop FileSystem to delete test data
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = org.apache.hadoop.fs.FileSystem.get(
        new java.net.URI(testTablePath),
        hadoopConf
      )

      val path = new org.apache.hadoop.fs.Path(testTablePath)
      if (fs.exists(path)) {
        fs.delete(path, true) // recursive delete
        println(s"✅ Cleaned up test data successfully")
      } else {
        println(s"ℹ️  Test data path did not exist: $testTablePath")
      }

    } catch {
      case e: Exception =>
        println(s"⚠️  Warning: Could not clean up test data: ${e.getMessage}")
    }

  ignore("End-to-end S3 integration: Write and read large dataset") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 integration test")

    println(s"🚀 Starting end-to-end S3 integration test")
    println(s"📍 Test table path: $testTablePath")

    // Generate realistic test dataset
    val testData = generateLargeTestDataset(1000) // 1000 records for meaningful test

    println(s"📊 Generated ${testData.count()} test records")

    // Step 1: Write data to S3
    println(s"✍️  Step 1: Writing data to S3...")
    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(testTablePath)

    println(s"✅ Successfully wrote data to S3")

    // Step 2: Read data back from S3
    println(s"📖 Step 2: Reading data from S3...")
    val readData  = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(testTablePath)
    val readCount = readData.count()

    assert(readCount == 1000, s"Expected 1000 records, got $readCount")
    println(s"✅ Successfully read $readCount records from S3")

    // Step 3: Verify data integrity
    println(s"🔍 Step 3: Verifying data integrity...")
    val sampleData = readData.limit(5).collect()

    sampleData.foreach { row =>
      assert(row.getString(row.fieldIndex("title")) != null, "Title should not be null")
      assert(row.getString(row.fieldIndex("content")) != null, "Content should not be null")
      assert(row.getString(row.fieldIndex("category")) != null, "Category should not be null")
    }

    println(s"✅ Data integrity verified")

    // Step 4: Test search functionality
    println(s"🔎 Step 4: Testing search functionality...")

    // Standard DataFrame filters
    val technologyDocs = readData.filter(col("category") === "technology").count()
    println(s"📊 Found $technologyDocs technology documents")

    val activeDocs = readData.filter(col("status") === "active").count()
    println(s"📊 Found $activeDocs active documents")

    // Text search
    val sparkDocs = readData.filter(col("content").contains("spark")).count()
    println(s"📊 Found $sparkDocs documents containing 'spark'")

    assert(technologyDocs > 0, "Should find some technology documents")
    assert(activeDocs > 0, "Should find some active documents")

    println(s"✅ Search functionality verified")
  }

  ignore("S3 integration: IndexQuery native search operations") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 integration test")

    println(s"🔍 Testing IndexQuery operations with S3 data")

    // Generate test data with rich text content for search testing
    val searchTestData  = generateSearchTestDataset(500)
    val searchTablePath = s"$S3_BASE_PATH/search-test-$testRunId"

    // Write search test data
    searchTestData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(searchTablePath)

    try {
      val searchData =
        spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(searchTablePath)

      // Test various IndexQuery operations using SQL
      searchData.createOrReplaceTempView("search_table")

      val sparkDocs = spark.sql("SELECT * FROM search_table WHERE content indexquery 'spark'").count()
      println(s"📊 IndexQuery 'spark': $sparkDocs documents")

      val mlDocs = spark.sql("SELECT * FROM search_table WHERE content indexquery 'machine AND learning'").count()
      println(s"📊 IndexQuery 'machine AND learning': $mlDocs documents")

      val allFieldsDocs = spark.sql("SELECT * FROM search_table WHERE _indexall indexquery 'apache OR python'").count()
      println(s"📊 IndexQueryAll 'apache OR python': $allFieldsDocs documents")

      assert(sparkDocs > 0, "Should find documents with spark")
      assert(mlDocs > 0, "Should find documents with machine learning")
      assert(allFieldsDocs > 0, "Should find documents with apache or python")

      println(s"✅ IndexQuery operations verified")

    } finally
      // Clean up search test data
      cleanupPath(searchTablePath)
  }

  ignore("S3 integration: Cache statistics monitoring") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 integration test")

    println(s"📊 Testing cache statistics with S3 data")

    // Use existing test data for cache statistics
    val readData = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(testTablePath)

    // Perform some operations to populate cache
    val count1 = readData.filter(col("category") === "technology").count()
    val count2 = readData.filter(col("status") === "active").count()
    val count3 = readData.filter(col("content").contains("data")).count()

    println(s"📊 Performed cache-warming queries: tech=$count1, active=$count2, data=$count3")

    // Test cache statistics SQL command
    println(s"📈 Collecting cache statistics...")
    val cacheStats = spark.sql(s"SHOW SPLIT CACHE STATS FOR '$testTablePath'")
    val statsRows  = cacheStats.collect()

    assert(statsRows.length > 0, "Should return cache statistics")

    // Verify expected cache types
    val cacheTypes    = statsRows.map(_.getString(2)).distinct
    val expectedTypes = Set("ByteRangeCache", "FooterCache", "FastFieldCache", "SplitCache", "Total")

    println(s"📊 Found cache types: ${cacheTypes.mkString(", ")}")

    // Should have at least some of the expected cache types
    assert(
      cacheTypes.exists(expectedTypes.contains),
      s"Should have expected cache types. Found: ${cacheTypes.mkString(", ")}"
    )

    // Verify schema
    val schema = cacheStats.schema
    val expectedColumns =
      Seq("executor_id", "hostname", "cache_type", "hits", "misses", "hit_rate", "evictions", "size_bytes", "size_mb")
    val actualColumns = schema.fieldNames

    assert(
      actualColumns.sameElements(expectedColumns),
      s"Schema mismatch. Expected: ${expectedColumns.mkString(", ")}, Got: ${actualColumns.mkString(", ")}"
    )

    // Print sample statistics
    println(s"📊 Sample cache statistics:")
    statsRows.take(5).foreach { row =>
      val executorId = row.getString(0)
      val hostname   = row.getString(1)
      val cacheType  = row.getString(2)
      val hits       = row.getLong(3)
      val misses     = row.getLong(4)
      val hitRate    = row.getDouble(5)

      println(f"  $executorId%-3s | $hostname%-20s | $cacheType%-15s | $hits%6d hits | $misses%6d misses | $hitRate%6.3f hit rate")
    }

    println(s"✅ Cache statistics verified")
  }

  ignore("S3 integration: Multiple operations and transaction log verification") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 integration test")

    println(s"🔄 Testing multiple S3 operations and transaction log")

    val multiOpTablePath = s"$S3_BASE_PATH/multi-op-test-$testRunId"

    try {
      // Step 1: Initial write
      val initialData = generateTestDataset(300, "initial")
      initialData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(multiOpTablePath)

      val count1 =
        spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(multiOpTablePath).count()
      assert(count1 == 300, s"Initial write: expected 300, got $count1")
      println(s"📝 Initial write: $count1 records")

      // Step 2: Append more data
      val appendData = generateTestDataset(200, "append")
      appendData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(multiOpTablePath)

      val count2 =
        spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(multiOpTablePath).count()
      assert(count2 == 500, s"After append: expected 500, got $count2")
      println(s"📝 After append: $count2 records")

      // Step 3: Overwrite data
      val overwriteData = generateTestDataset(400, "overwrite")
      overwriteData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(multiOpTablePath)

      val count3 =
        spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(multiOpTablePath).count()
      assert(count3 == 400, s"After overwrite: expected 400, got $count3")
      println(s"📝 After overwrite: $count3 records")

      // Step 4: Verify transaction log exists
      val transactionLogPath = s"$multiOpTablePath/_transaction_log"
      val hadoopConf         = spark.sparkContext.hadoopConfiguration
      val fs                 = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(transactionLogPath), hadoopConf)
      val logPath            = new org.apache.hadoop.fs.Path(transactionLogPath)

      assert(fs.exists(logPath), "Transaction log should exist")

      val logFiles = fs.listStatus(logPath).filter(!_.isDirectory).map(_.getPath.getName)
      println(s"📋 Transaction log files: ${logFiles.mkString(", ")}")

      assert(logFiles.exists(_.endsWith(".json")), "Should have JSON transaction log files")

      println(s"✅ Multiple operations and transaction log verified")

    } finally
      cleanupPath(multiOpTablePath)
  }

  /** Generate a large realistic test dataset */
  private def generateLargeTestDataset(numRecords: Int): DataFrame = {
    val categories = Array("technology", "science", "business", "health", "education")
    val statuses   = Array("active", "inactive", "pending", "archived")

    val data = Range(1, numRecords + 1).map { i =>
      val category = categories(i % categories.length)
      val status   = statuses(i % statuses.length)

      (
        i,                    // id
        s"Document Title $i", // title
        s"This is the content for document $i. It contains information about $category " +
          s"and various topics including spark, machine learning, data science, and analytics. " +
          s"The document has status $status and contains searchable text for testing purposes.", // content
        category,                                                                                // category
        status,                                                                                  // status
        java.sql.Timestamp.valueOf(s"2024-01-${(i % 28) + 1} 10:00:00"), // created_at
        85.5 + (i % 100) // score
      )
    }

    spark
      .createDataFrame(spark.sparkContext.parallelize(data))
      .toDF("id", "title", "content", "category", "status", "created_at", "score")
  }

  /** Generate test dataset optimized for search testing */
  private def generateSearchTestDataset(numRecords: Int): DataFrame = {
    val searchTerms = Array("spark", "machine learning", "apache", "python", "data science", "analytics")
    val frameworks  = Array("apache spark", "tensorflow", "pytorch", "scikit-learn")

    val data = Range(1, numRecords + 1).map { i =>
      val searchTerm = searchTerms(i % searchTerms.length)
      val framework  = frameworks(i % frameworks.length)

      (
        i,
        s"Research Paper $i: $searchTerm",
        s"This document discusses $searchTerm and its applications. " +
          s"It covers $framework and related technologies. " +
          s"The research includes machine learning algorithms, data processing, " +
          s"and distributed computing concepts. Modern applications use apache spark " +
          s"for big data processing and python for data science workflows.",
        "research",
        "published"
      )
    }

    spark.createDataFrame(spark.sparkContext.parallelize(data)).toDF("id", "title", "content", "category", "status")
  }

  /** Generate basic test dataset */
  private def generateTestDataset(numRecords: Int, suffix: String): DataFrame = {
    val data = Range(1, numRecords + 1).map { i =>
      (
        s"${suffix}_$i",
        s"Title $suffix $i",
        s"Content for $suffix document $i with various text for testing",
        "test",
        "active"
      )
    }

    spark.createDataFrame(spark.sparkContext.parallelize(data)).toDF("id", "title", "content", "category", "status")
  }

  /** Helper method to clean up a specific S3 path */
  private def cleanupPath(path: String): Unit =
    try {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs         = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(path), hadoopConf)
      val hadoopPath = new org.apache.hadoop.fs.Path(path)

      if (fs.exists(hadoopPath)) {
        fs.delete(hadoopPath, true)
        println(s"🧹 Cleaned up: $path")
      }
    } catch {
      case e: Exception =>
        println(s"⚠️  Warning: Could not clean up $path: ${e.getMessage}")
    }
}
