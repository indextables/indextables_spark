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

import java.io.{File, FileInputStream}
import java.util.Properties
import java.util.UUID

import scala.util.Using

import org.apache.spark.sql.functions._

import io.indextables.spark.RealS3TestBase

/**
 * Real AWS S3 integration test for multi-dimensional GROUP BY aggregations.
 *
 * This test validates that multi-dimensional GROUP BY operations work correctly with real S3 storage, using AWS
 * credentials from ~/.aws/credentials.
 */
class RealS3MultiDimensionalGroupByTest extends RealS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/real-s3-multidim-groupby-test-$testRunId"

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

      // Configure Hadoop config for CloudStorageProvider
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
      hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
      hadoopConf.set("spark.indextables.aws.region", S3_REGION)

      println(s"🔐 AWS credentials loaded successfully")
      println(s"🌊 Configured Spark for S3 access to bucket: $S3_BUCKET in region: $S3_REGION")
      println(s"📍 Test base path: $testBasePath")
    } else {
      println(s"⚠️  No AWS credentials found in ~/.aws/credentials - tests will be skipped")
    }
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

  test("Multi-dimensional GROUP BY should work correctly with real S3 storage") {
    assume(awsCredentials.isDefined, "AWS credentials not available")

    val tablePath = s"$testBasePath/multidim-groupby-table"

    println(s"📝 Creating test data for multi-dimensional GROUP BY test")

    // Create test data with multiple dimensions: region, category, and status
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.Row

    val schema = StructType(
      Seq(
        StructField("region", StringType, nullable = false),
        StructField("category", StringType, nullable = false),
        StructField("status", StringType, nullable = false),
        StructField("sales", IntegerType, nullable = false),
        StructField("rating", DoubleType, nullable = false)
      )
    )

    val testData = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("us-east", "electronics", "active", 100, 4.5),
          Row("us-east", "electronics", "active", 150, 4.7),
          Row("us-east", "electronics", "inactive", 75, 3.8),
          Row("us-east", "books", "active", 50, 4.2),
          Row("us-east", "books", "active", 60, 4.3),
          Row("us-east", "books", "inactive", 30, 3.5),
          Row("us-west", "electronics", "active", 200, 4.9),
          Row("us-west", "electronics", "active", 180, 4.8),
          Row("us-west", "electronics", "inactive", 90, 3.9),
          Row("us-west", "books", "active", 70, 4.4),
          Row("us-west", "books", "active", 80, 4.6),
          Row("us-west", "books", "inactive", 40, 3.6),
          Row("eu-west", "electronics", "active", 120, 4.1),
          Row("eu-west", "electronics", "inactive", 110, 4.0),
          Row("eu-west", "books", "active", 55, 4.2),
          Row("eu-west", "books", "inactive", 45, 3.7)
        )
      ),
      schema
    )

    println(s"🚀 Writing test data to S3: $tablePath")

    // Write test data to S3 with fast fields for aggregation
    // GROUP BY columns (region, category, status) must also be fast fields
    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(getWriteOptions())
      .option("spark.indextables.indexing.fastfields", "sales,rating,region,category,status")
      .mode("overwrite")
      .save(tablePath)

    println(s"✅ Test data written successfully to S3")

    // Read data back and perform multi-dimensional GROUP BY
    println(s"📖 Reading data from S3 and performing multi-dimensional GROUP BY")

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(getReadOptions())
      .load(tablePath)

    // Perform 3-dimensional GROUP BY: region, category, status
    // Temporarily simplified to test SUM only
    val groupByResult = df
      .groupBy("region", "category", "status")
      .agg(
        count("*").as("count"),
        sum("sales").as("total_sales")
      )
      .orderBy("region", "category", "status")

    println(s"🔍 Multi-dimensional GROUP BY results:")
    groupByResult.show(false)

    // Collect results for validation
    val results = groupByResult.collect()

    // Expected results based on test data
    val expectedResults = Map(
      ("eu-west", "books", "active")         -> (1L, 55.0),
      ("eu-west", "books", "inactive")       -> (1L, 45.0),
      ("eu-west", "electronics", "active")   -> (1L, 120.0),
      ("eu-west", "electronics", "inactive") -> (1L, 110.0),
      ("us-east", "books", "active")         -> (2L, 110.0),
      ("us-east", "books", "inactive")       -> (1L, 30.0),
      ("us-east", "electronics", "active")   -> (2L, 250.0),
      ("us-east", "electronics", "inactive") -> (1L, 75.0),
      ("us-west", "books", "active")         -> (2L, 150.0),
      ("us-west", "books", "inactive")       -> (1L, 40.0),
      ("us-west", "electronics", "active")   -> (2L, 380.0),
      ("us-west", "electronics", "inactive") -> (1L, 90.0)
    )

    // Validate results
    println(s"✅ Validating multi-dimensional GROUP BY results...")

    // Print actual results for debugging
    println(s"📊 Actual results from query:")
    results.foreach { row =>
      val region     = row.getAs[String]("region")
      val category   = row.getAs[String]("category")
      val status     = row.getAs[String]("status")
      val count      = row.getAs[Long]("count")
      val totalSales = row.getAs[Long]("total_sales")
      println(s"  ($region, $category, $status) -> count=$count, sales=$totalSales")
    }

    results.foreach { row =>
      val region     = row.getAs[String]("region")
      val category   = row.getAs[String]("category")
      val status     = row.getAs[String]("status")
      val count      = row.getAs[Long]("count")
      val totalSales = row.getAs[Long]("total_sales")

      val key = (region, category, status)
      expectedResults.get(key) match {
        case Some((expectedCount, expectedSales)) =>
          assert(count == expectedCount, s"Count mismatch for $key: expected $expectedCount, got $count")
          assert(
            totalSales == expectedSales.toLong,
            s"Total sales mismatch for $key: expected ${expectedSales.toLong}, got $totalSales"
          )
          println(s"✅ Validated: $key -> count=$count, sales=$totalSales")
        case None =>
          fail(s"Unexpected result for key: $key")
      }
    }

    // Verify we got all expected results
    assert(results.length == expectedResults.size, s"Expected ${expectedResults.size} groups, got ${results.length}")

    println(s"✅ All multi-dimensional GROUP BY validations passed!")
    println(s"📊 Successfully validated ${results.length} aggregation groups")

    // Verify pushdown is happening by checking the physical plan
    val physicalPlan = groupByResult.queryExecution.executedPlan.toString()
    assert(
      physicalPlan.contains("IndexTables4SparkGroupByAggregateScan"),
      "Expected GROUP BY pushdown to use IndexTables4SparkGroupByAggregateScan"
    )

    println(s"✅ Verified GROUP BY pushdown is active in physical plan")
  }

  test("Two-dimensional GROUP BY with real S3 should aggregate correctly") {
    assume(awsCredentials.isDefined, "AWS credentials not available")

    val tablePath = s"$testBasePath/twodim-groupby-table"

    println(s"📝 Creating test data for two-dimensional GROUP BY test")

    // Create simpler test data with two dimensions: category and status
    val testData = spark
      .createDataFrame(
        Seq(
          ("electronics", "active", 100),
          ("electronics", "active", 150),
          ("electronics", "inactive", 75),
          ("books", "active", 50),
          ("books", "active", 60),
          ("books", "inactive", 30)
        )
      )
      .toDF("category", "status", "value")

    println(s"🚀 Writing test data to S3: $tablePath")

    // GROUP BY columns (category, status) must also be fast fields
    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(getWriteOptions())
      .option("spark.indextables.indexing.fastfields", "value,category,status")
      .mode("overwrite")
      .save(tablePath)

    println(s"✅ Test data written successfully to S3")

    // Read and perform 2-dimensional GROUP BY
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(getReadOptions())
      .load(tablePath)

    val groupByResult = df
      .groupBy("category", "status")
      .agg(
        count("*").as("count"),
        sum("value").as("total_value")
      )
      .orderBy("category", "status")

    println(s"🔍 Two-dimensional GROUP BY results:")
    groupByResult.show(false)

    val results = groupByResult.collect()

    // Validate specific expected values
    val booksActive = results
      .find(row =>
        row.getAs[String]("category") == "books" &&
          row.getAs[String]("status") == "active"
      )
      .get

    assert(booksActive.getAs[Long]("count") == 2L, "Books active count should be 2")
    assert(booksActive.getAs[Long]("total_value") == 110L, "Books active total should be 110")

    val electronicsActive = results
      .find(row =>
        row.getAs[String]("category") == "electronics" &&
          row.getAs[String]("status") == "active"
      )
      .get

    assert(electronicsActive.getAs[Long]("count") == 2L, "Electronics active count should be 2")
    assert(electronicsActive.getAs[Long]("total_value") == 250L, "Electronics active total should be 250")

    println(s"✅ Two-dimensional GROUP BY test passed!")
  }
}
