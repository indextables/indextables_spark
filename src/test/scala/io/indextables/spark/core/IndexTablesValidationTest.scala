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

package io.indextables.spark.core

import java.io.File
import java.io.FileInputStream
import java.util.Properties
import java.util.UUID

import scala.util.Using

import org.apache.spark.sql.functions._

import io.indextables.spark.RealS3TestBase

/**
 * Test to validate that spark.indextables.* configurations actually work during read, write, and merge operations,
 * including real S3 operations.
 */
class IndexTablesValidationTest extends RealS3TestBase {

  // S3 configuration for real AWS testing
  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/indextables-validation-$testRunId"

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Try to load AWS credentials for S3 tests
    awsCredentials = loadAwsCredentials()

    if (awsCredentials.isDefined) {
      val (accessKey, secretKey) = awsCredentials.get

      // Configure Spark for real S3 access using spark.indextables prefix
      spark.conf.set("spark.indextables.aws.accessKey", accessKey)
      spark.conf.set("spark.indextables.aws.secretKey", secretKey)
      spark.conf.set("spark.indextables.aws.region", S3_REGION)

      // Also configure Hadoop config using spark.indextables prefix
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
      hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
      hadoopConf.set("spark.indextables.aws.region", S3_REGION)

      println(s"🔐 AWS credentials loaded for IndexTables validation tests")
      println(s"🌊 S3 test path: $testBasePath")
    } else {
      println(s"⚠️  No AWS credentials found - S3 tests will be skipped")
    }
  }

  override def afterAll(): Unit = {
    // Clean up S3 test data
    if (awsCredentials.isDefined) {
      cleanupS3TestData()
    }
    super.afterAll()
  }

  /** Load AWS credentials from ~/.aws/credentials file. */
  private def loadAwsCredentials(): Option[(String, String)] =
    try {
      val homeDir         = System.getProperty("user.home")
      val credentialsFile = new File(homeDir, ".aws/credentials")

      if (credentialsFile.exists()) {
        Using(new FileInputStream(credentialsFile)) { fis =>
          val properties = new Properties()
          properties.load(fis)

          val accessKey = properties.getProperty("aws_access_key_id")
          val secretKey = properties.getProperty("aws_secret_access_key")

          if (accessKey != null && secretKey != null) {
            Some((accessKey, secretKey))
          } else {
            None
          }
        }.getOrElse(None)
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }

  /** Clean up S3 test data. */
  private def cleanupS3TestData(): Unit =
    try {
      // Use Spark to list and delete test data
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs         = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(testBasePath), hadoopConf)
      val path       = new org.apache.hadoop.fs.Path(testBasePath)

      if (fs.exists(path)) {
        fs.delete(path, true) // recursive delete
        println(s"🧹 Cleaned up S3 test data at: $testBasePath")
      }
    } catch {
      case e: Exception =>
        println(s"⚠️  Failed to clean up S3 test data: ${e.getMessage}")
    }

  test("IndexTables provider should work for basic write and read") {
    withTempPath { path =>
      // Create test data
      val testData = spark
        .range(0, 3)
        .select(
          col("id"),
          concat(lit("test_"), col("id")).as("title"),
          concat(lit("content_"), col("id")).as("content")
        )

      // Write using IndexTables provider with spark.indextables.* configurations
      testData.write
        .format("io.indextables.provider.IndexTablesProvider")
        .option("spark.indextables.indexing.typemap.title", "string")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexWriter.batchSize", "1000")
        .option("spark.indextables.cache.maxSize", "50000000")
        .mode("overwrite")
        .save(path.toString)

      // Verify files were created
      val pathDir = new File(path.toString)
      pathDir.exists() should be(true)
      pathDir.listFiles().filter(_.getName.endsWith(".split")).length should be > 0

      // Read using IndexTables provider with spark.indextables.* configurations
      val readData = spark.read
        .format("io.indextables.provider.IndexTablesProvider")
        .option("spark.indextables.cache.maxSize", "40000000")
        .option("spark.indextables.docBatch.enabled", "true")
        .load(path.toString)

      // Validate data
      readData.count() should equal(3)
      val titles = readData.select("title").collect().map(_.getString(0))
      titles should contain("test_0")
      titles should contain("test_1")
      titles should contain("test_2")
    }
  }

  test("Mixed spark.tantivy4spark and spark.indextables configurations should work") {
    withTempPath { path =>
      val testData = spark
        .range(0, 2)
        .select(
          col("id"),
          concat(lit("mixed_"), col("id")).as("name")
        )

      // Write with mixed configurations
      testData.write
        .format("io.indextables.provider.IndexTablesProvider")
        .option("spark.indextables.indexing.typemap.name", "string")  // Original prefix
        .option("spark.indextables.indexWriter.batchSize", "500")     // New prefix
        .option("spark.indextables.cache.maxSize", "30000000")        // Original prefix
        .option("spark.indextables.indexWriter.heapSize", "50000000") // 50MB (meets 15MB/thread * 2 threads minimum)
        .mode("overwrite")
        .save(path.toString)

      // Verify write succeeded
      val pathDir = new File(path.toString)
      pathDir.exists() should be(true)

      // Read with mixed configurations
      val readData = spark.read
        .format("io.indextables.provider.IndexTablesProvider")
        .option("spark.indextables.cache.maxSize", "20000000") // Original prefix
        .option("spark.indextables.docBatch.enabled", "true")  // New prefix
        .load(path.toString)

      readData.count() should equal(2)
    }
  }

  test("ConfigNormalization utility should work correctly") {
    import io.indextables.spark.util.ConfigNormalization

    // Test basic normalization
    ConfigNormalization.normalizeKey("spark.indextables.cache.maxSize") should equal(
      "spark.indextables.cache.maxSize"
    )

    ConfigNormalization.normalizeKey("spark.indextables.cache.maxSize") should equal(
      "spark.indextables.cache.maxSize"
    )

    // Test key identification
    ConfigNormalization.isTantivyKey("spark.indextables.indexWriter.batchSize") should be(true)
    ConfigNormalization.isTantivyKey("spark.indextables.indexWriter.batchSize") should be(true)
    ConfigNormalization.isTantivyKey("spark.sql.adaptive.enabled") should be(false)

    // Test configuration filtering
    val mixedConfigs = Map(
      "spark.indextables.cache.maxSize"         -> "100000000",
      "spark.indextables.indexWriter.batchSize" -> "5000",
      "spark.sql.adaptive.enabled"              -> "true"
    )

    val normalized = ConfigNormalization.filterAndNormalizeTantivyConfigs(mixedConfigs)
    normalized should contain key "spark.indextables.cache.maxSize"
    normalized should contain key "spark.indextables.indexWriter.batchSize"
    normalized.keys should not contain "spark.sql.adaptive.enabled"
    normalized("spark.indextables.indexWriter.batchSize") should equal("5000")
  }

  test("IndexTables4SparkOptions should handle spark.indextables configurations") {
    import scala.jdk.CollectionConverters._
    import org.apache.spark.sql.util.CaseInsensitiveStringMap

    val options = new CaseInsensitiveStringMap(
      Map(
        "spark.indextables.indexing.typemap.title"     -> "string",
        "spark.indextables.indexing.typemap.content"   -> "text",
        "spark.indextables.indexing.tokenizer.summary" -> "raw"
      ).asJava
    )

    val tantivyOptions = new IndexTables4SparkOptions(options)

    // Field type mapping should extract both prefixes
    val fieldTypes = tantivyOptions.getFieldTypeMapping
    fieldTypes should contain key "title"
    fieldTypes should contain key "content"
    fieldTypes("title") should equal("string")
    fieldTypes("content") should equal("text")

    // Tokenizer overrides should extract spark.indextables configs
    val tokenizers = tantivyOptions.getTokenizerOverrides
    tokenizers should contain key "summary"
    tokenizers("summary") should equal("raw")
  }

  test("Session-level spark.indextables configurations should work") {
    withTempPath { path =>
      // Set session-level configurations
      spark.conf.set("spark.indextables.indexWriter.batchSize", "2000")
      spark.conf.set("spark.indextables.cache.maxSize", "60000000")

      try {
        val testData = spark
          .range(0, 2)
          .select(
            col("id"),
            concat(lit("session_"), col("id")).as("title")
          )

        // Write using session configurations (should inherit the spark.indextables settings)
        testData.write
          .format("io.indextables.provider.IndexTablesProvider")
          .option("spark.indextables.indexing.typemap.title", "string")
          .mode("overwrite")
          .save(path.toString)

        // Verify write succeeded
        val pathDir = new File(path.toString)
        pathDir.exists() should be(true)

        // Read data back
        val readData = spark.read
          .format("io.indextables.provider.IndexTablesProvider")
          .load(path.toString)

        readData.count() should equal(2)

      } finally {
        // Clean up session configurations
        spark.conf.unset("spark.indextables.indexWriter.batchSize")
        spark.conf.unset("spark.indextables.cache.maxSize")
      }
    }
  }

  test("S3 write with spark.indextables configurations should work") {
    assume(awsCredentials.isDefined, "AWS credentials not available - skipping S3 test")

    val testPath = s"$testBasePath/s3-indextables-write"

    val testData = spark
      .range(0, 5)
      .select(
        col("id"),
        concat(lit("s3_test_"), col("id")).as("title"),
        concat(lit("S3 content for record "), col("id")).as("content")
      )

    // Write using spark.indextables configurations with S3
    testData.write
      .format("io.indextables.provider.IndexTablesProvider")
      .option("spark.indextables.indexing.typemap.title", "string")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexWriter.batchSize", "3000")
      .option("spark.indextables.cache.maxSize", "80000000")
      .option("spark.indextables.s3.maxConcurrency", "2")
      .mode("overwrite")
      .save(testPath)

    // Verify write succeeded by reading back
    val readData = spark.read
      .format("io.indextables.provider.IndexTablesProvider")
      .option("spark.indextables.cache.maxSize", "70000000")
      .option("spark.indextables.docBatch.enabled", "true")
      .load(testPath)

    readData.count() should equal(5)

    val titles = readData.select("title").collect().map(_.getString(0)).sorted
    titles should contain("s3_test_0")
    titles should contain("s3_test_4")
  }

  test("S3 read with mixed spark.tantivy4spark and spark.indextables configurations should work") {
    assume(awsCredentials.isDefined, "AWS credentials not available - skipping S3 test")

    val testPath = s"$testBasePath/s3-mixed-configs"

    val testData = spark
      .range(0, 3)
      .select(
        col("id"),
        concat(lit("mixed_s3_"), col("id")).as("name"),
        (col("id") * 100).as("score")
      )

    // Write with mixed configurations
    testData.write
      .format("io.indextables.provider.IndexTablesProvider")
      .option("spark.indextables.indexing.typemap.name", "string")  // Original prefix
      .option("spark.indextables.indexWriter.batchSize", "2000")    // New prefix
      .option("spark.indextables.cache.maxSize", "50000000")        // Original prefix
      .option("spark.indextables.indexWriter.heapSize", "50000000") // 50MB (meets 15MB/thread * 2 threads minimum)
      .mode("overwrite")
      .save(testPath)

    // Read with mixed configurations
    val readData = spark.read
      .format("io.indextables.provider.IndexTablesProvider")
      .option("spark.indextables.cache.maxSize", "40000000") // Original prefix
      .option("spark.indextables.docBatch.enabled", "true")  // New prefix
      .load(testPath)

    readData.count() should equal(3)

    val names = readData.select("name").collect().map(_.getString(0))
    names should contain("mixed_s3_0")
    names should contain("mixed_s3_2")
  }

  test("S3 IndexQuery operations with spark.indextables configurations should work") {
    assume(awsCredentials.isDefined, "AWS credentials not available - skipping S3 test")

    val testPath = s"$testBasePath/s3-indexquery-test"

    val testData = spark
      .createDataFrame(
        Seq(
          ("1", "Apache Spark", "Distributed computing framework for big data processing"),
          ("2", "Apache Hadoop", "Framework for distributed storage and processing"),
          ("3", "Machine Learning", "AI algorithms and statistical models for data analysis"),
          ("4", "Data Science", "Interdisciplinary field using scientific methods")
        )
      )
      .toDF("id", "title", "description")

    // Write with text field configuration for IndexQuery support
    testData.write
      .format("io.indextables.provider.IndexTablesProvider")
      .option("spark.indextables.indexing.typemap.title", "string")     // String for exact match
      .option("spark.indextables.indexing.typemap.description", "text") // Text for IndexQuery
      .option("spark.indextables.indexWriter.batchSize", "2000")
      .option("spark.indextables.cache.maxSize", "60000000")
      .mode("overwrite")
      .save(testPath)

    // Read and test IndexQuery operations
    val readData = spark.read
      .format("io.indextables.provider.IndexTablesProvider")
      .option("spark.indextables.cache.maxSize", "50000000")
      .load(testPath)

    // Test exact string matching on title field
    val sparkResults = readData.filter(col("title") === "Apache Spark").collect()
    sparkResults.length should equal(1)
    sparkResults(0).getString(1) should equal("Apache Spark")

    // Test IndexQuery on description field (text type) using SQL
    readData.createOrReplaceTempView("s3_indexquery_data")
    val queryResults =
      spark.sql("SELECT * FROM s3_indexquery_data WHERE description indexquery 'distributed AND framework'").collect()
    queryResults.length should be >= 1

    // Use count() instead of collect() to avoid partition limits
    val totalCount = readData.count()
    totalCount should equal(4)
  }
}
