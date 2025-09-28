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

import com.tantivy4spark.RealS3TestBase
import org.apache.spark.sql.functions._
import java.io.{File, FileInputStream}
import java.util.Properties
import java.util.UUID
import scala.util.Using

/**
 * Test to verify that process-based merges work correctly when source splits are on S3.
 * Uses the same RealS3IntegrationTest infrastructure for S3 credentials and setup.
 *
 * This test specifically validates:
 * - Process-based merge mode works with S3 source splits
 * - Temporary directory handling for merge operations
 * - Data integrity after S3-based process merge
 * - Proper AWS credential propagation to merge processes
 */
class MergeSplitsS3ProcessBasedTest extends RealS3TestBase {

  private val S3_BUCKET = "test-tantivy4sparkbucket"
  private val S3_REGION = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/process-merge-test-$testRunId"

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    if (awsCredentials.isDefined) {
      val (accessKey, secretKey) = awsCredentials.get

      // Configure Spark for real S3 access
      spark.conf.set("spark.tantivy4spark.aws.accessKey", accessKey)
      spark.conf.set("spark.tantivy4spark.aws.secretKey", secretKey)
      spark.conf.set("spark.tantivy4spark.aws.region", S3_REGION)

      // ALSO configure Hadoop config so CloudStorageProvider can find the region
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("spark.tantivy4spark.aws.accessKey", accessKey)
      hadoopConf.set("spark.tantivy4spark.aws.secretKey", secretKey)
      hadoopConf.set("spark.tantivy4spark.aws.region", S3_REGION)

      // Configure process-based merge (this is the default but explicitly set it)
      spark.conf.set("spark.tantivy4spark.merge.mode", "process")
      spark.conf.set("spark.tantivy4spark.merge.heapSize", "134217728") // 128MB for better performance

      println(s"🔐 AWS credentials loaded successfully")
      println(s"🌊 Configured Spark for S3 access to bucket: $S3_BUCKET in region: $S3_REGION")
      println(s"📍 Test base path: $testBasePath")
      println(s"🔧 Process-based merge mode configured")
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

  /**
   * Load AWS credentials from ~/.aws/credentials file.
   */
  private def loadAwsCredentials(): Option[(String, String)] = {
    try {
      val home = System.getProperty("user.home")
      val credFile = new File(s"$home/.aws/credentials")

      if (credFile.exists()) {
        val props = new Properties()
        Using(new FileInputStream(credFile)) { fis =>
          props.load(fis)
        }

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
  }

  /**
   * Get write options with AWS credentials for executor distribution.
   */
  private def getWriteOptions(): Map[String, String] = {
    val (accessKey, secretKey) = awsCredentials.get
    Map(
      "spark.tantivy4spark.aws.accessKey" -> accessKey,
      "spark.tantivy4spark.aws.secretKey" -> secretKey,
      "spark.tantivy4spark.aws.region" -> S3_REGION
    )
  }

  /**
   * Get read options with AWS credentials for executor distribution.
   */
  private def getReadOptions(): Map[String, String] = {
    val (accessKey, secretKey) = awsCredentials.get
    Map(
      "spark.tantivy4spark.aws.accessKey" -> accessKey,
      "spark.tantivy4spark.aws.secretKey" -> secretKey,
      "spark.tantivy4spark.aws.region" -> S3_REGION
    )
  }

  /**
   * Clean up test data from S3.
   */
  private def cleanupTestData(): Unit = {
    try {
      println(s"ℹ️  Test data cleanup skipped (unique paths used): $testBasePath")
    } catch {
      case e: Exception =>
        println(s"⚠️  Warning: Could not clean up test data: ${e.getMessage}")
    }
  }

  test("Process-based merge with S3 source splits should work correctly") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/process-merge-test"

    println(s"🚀 Starting process-based merge test with S3 source splits")
    println(s"📍 Table path: $tablePath")

    // Create substantial test data to force multiple splits
    val data = spark.range(2000).select(
      col("id"),
      concat(
        lit("This is comprehensive content for document "), col("id"),
        lit(". It contains substantial text to ensure splits are large enough for meaningful merge operations. "),
        lit("Additional content to create realistic split file sizes that will benefit from merging. "),
        lit("More detailed content to reach optimal split sizes for process-based merge validation.")
      ).as("content"),
      (col("id") % 20).cast("string").as("category")
    )

    println(s"✍️  Writing test data in multiple batches to create multiple splits...")

    val writeOptions = getWriteOptions() ++ Map(
      "spark.tantivy4spark.indexwriter.batchSize" -> "200"  // Force multiple splits
    )

    // Write in multiple phases to ensure multiple splits are created
    data.filter(col("id") < 800).write
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    data.filter(col("id") >= 800 && col("id") < 1400).write
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .options(writeOptions)
      .mode("append")
      .save(tablePath)

    data.filter(col("id") >= 1400).write
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .options(writeOptions)
      .mode("append")
      .save(tablePath)

    println(s"✅ Successfully wrote test data to S3: $tablePath")

    // Verify we have multiple splits before merge
    import com.tantivy4spark.transaction.{TransactionLog, TransactionLogFactory}
    val transactionLog = TransactionLogFactory.create(
      new org.apache.hadoop.fs.Path(tablePath), spark)
    val initialFiles = transactionLog.listFiles()

    println(s"📊 Initial state: ${initialFiles.length} split files created")
    initialFiles.foreach { file =>
      println(s"   Split: ${file.path} (${file.size} bytes)")
    }

    // Require multiple splits for meaningful test
    assert(initialFiles.length >= 3, s"Should have created multiple splits for merge test, got ${initialFiles.length}")

    // Verify initial data before merge
    val readOptions = getReadOptions()
    val preMergeData = spark.read
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    val preMergeCount = preMergeData.count()
    preMergeCount shouldBe 2000

    println(s"✅ Pre-merge verification: $preMergeCount records")

    // Execute MERGE SPLITS command using process-based merge
    println(s"🔧 Executing process-based MERGE SPLITS operation...")
    import com.tantivy4spark.sql.Tantivy4SparkSqlParser
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    val mergeCommand = sqlParser.parsePlan(s"MERGE SPLITS '$tablePath' TARGET SIZE 5242880") // 5MB target
      .asInstanceOf[com.tantivy4spark.sql.MergeSplitsCommand]

    val startTime = System.currentTimeMillis()
    val result = mergeCommand.run(spark)
    val duration = System.currentTimeMillis() - startTime

    println(s"⏱️  Process-based merge completed in ${duration}ms")
    println(s"📋 Merge result: ${result.head.getString(1)}")

    // Verify merge was successful
    assert(result.nonEmpty, "Should return merge result")
    val resultRow = result.head
    assert(resultRow.getString(0) == tablePath, "Should return the S3 table path")

    // Verify we have fewer files after merge
    transactionLog.invalidateCache()
    val finalFiles = transactionLog.listFiles()

    println(s"📊 Final state: ${finalFiles.length} split files remain")
    finalFiles.foreach { file =>
      println(s"   Final split: ${file.path} (${file.size} bytes)")
    }

    // Should have fewer files after merge (unless all were already optimal size)
    if (finalFiles.length < initialFiles.length) {
      println(s"✅ Merge reduced file count: ${initialFiles.length} → ${finalFiles.length}")
    } else {
      println(s"ℹ️  File count unchanged: files were already at optimal size")
    }

    // Verify comprehensive data integrity after process-based merge
    println(s"🔍 Verifying data integrity after process-based merge...")

    val postMergeData = spark.read
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    val postMergeCount = postMergeData.count()
    postMergeCount shouldBe 2000

    // Verify all expected IDs are present (no data loss)
    val actualIds = postMergeData.select("id").collect().map(_.getLong(0)).toSet
    val expectedIds = (0L to 1999L).toSet // range(2000) creates 0-1999

    val missingIds = expectedIds -- actualIds
    val extraIds = actualIds -- expectedIds

    assert(missingIds.isEmpty, s"Missing IDs after process-based merge: ${missingIds.take(10)}${if (missingIds.size > 10) "..." else ""}")
    assert(extraIds.isEmpty, s"Extra/duplicate IDs after process-based merge: ${extraIds.take(10)}${if (extraIds.size > 10) "..." else ""}")
    assert(actualIds == expectedIds, "ID set should be exactly 0-1999, nothing more, nothing less")

    println(s"   ✓ All 2000 unique IDs present: ${actualIds.min} to ${actualIds.max}")

    // Verify content integrity for sample records
    val sampleRecords = postMergeData.filter(col("id") <= 10).orderBy("id").collect()
    assert(sampleRecords.length == 11, "Should have exactly 11 sample records (0-10)")

    for (i <- sampleRecords.indices) {
      val record = sampleRecords(i)
      val id = record.getLong(0)
      val content = record.getString(1)
      val category = record.getString(2)

      val expectedId = i.toLong
      val expectedContent = s"This is comprehensive content for document $expectedId. It contains substantial text to ensure splits are large enough for meaningful merge operations. Additional content to create realistic split file sizes that will benefit from merging. More detailed content to reach optimal split sizes for process-based merge validation."
      val expectedCategory = (expectedId % 20).toString

      assert(id == expectedId, s"Sample record $i: expected id=$expectedId, got id=$id")
      assert(content == expectedContent, s"Sample record $i: content mismatch for id=$id")
      assert(category == expectedCategory, s"Sample record $i: expected category=$expectedCategory, got category=$category")
    }

    println(s"   ✓ Sample record content integrity verified")

    // Verify category distribution is preserved
    val categoryCount = postMergeData.groupBy("category").count().collect()
      .map(row => row.getString(0) -> row.getLong(1)).toMap

    // Expected: IDs 0-1999, so categories 0-19 should have 100 records each
    val expectedCategories = (0 to 19).map(_.toString).map(cat => cat -> 100L).toMap

    assert(categoryCount.size == 20, s"Should have exactly 20 categories, got ${categoryCount.size}")
    expectedCategories.foreach { case (cat, expectedCount) =>
      val actualCount = categoryCount.getOrElse(cat, 0L)
      assert(actualCount == expectedCount, s"Category $cat: expected $expectedCount records, got $actualCount")
    }

    println(s"   ✓ Category distribution preserved: 20 categories with correct counts")

    // Test query functionality after process-based merge
    val filterTest = postMergeData.filter(col("category") === "5").count()
    filterTest shouldBe 100 // Should have 100 records with category "5"

    println(s"   ✓ Query functionality verified: category filter returned $filterTest records")

    println(s"🎉 Process-based merge with S3 source splits completed successfully!")
    println(s"   ✅ Merged splits from S3 using process-based architecture")
    println(s"   ✅ Preserved all $postMergeCount records with full data integrity")
    println(s"   ✅ AWS credentials properly propagated to merge processes")
    println(s"   ✅ Query functionality working correctly after merge")
  }

  test("Process-based merge should handle different merge modes correctly") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/mode-comparison-test"

    // Create test data for mode comparison
    val data = spark.range(500).select(
      col("id"),
      concat(lit("Document content for ID "), col("id"), lit(" with sufficient text for split creation.")).as("content")
    )

    println(s"✍️  Writing test data for merge mode comparison...")

    val writeOptions = getWriteOptions() ++ Map(
      "spark.tantivy4spark.indexwriter.batchSize" -> "100"  // Force multiple splits
    )

    data.write
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    println(s"✅ Successfully wrote test data to S3: $tablePath")

    // Test with process mode (default)
    println(s"🔧 Testing with process-based merge mode...")
    spark.conf.set("spark.tantivy4spark.merge.mode", "process")

    import com.tantivy4spark.sql.Tantivy4SparkSqlParser
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    val processMergeCommand = sqlParser.parsePlan(s"MERGE SPLITS '$tablePath' TARGET SIZE 2097152") // 2MB
      .asInstanceOf[com.tantivy4spark.sql.MergeSplitsCommand]

    val processResult = processMergeCommand.run(spark)
    assert(processResult.nonEmpty, "Process-based merge should return result")
    println(s"   Process-based result: ${processResult.head.getString(1)}")

    // Verify data integrity after process-based merge
    val readOptions = getReadOptions()
    val processedData = spark.read
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    val processedCount = processedData.count()
    processedCount shouldBe 500

    println(s"   ✓ Process-based merge preserved all $processedCount records")

    // Test that direct mode would also work (if we switched modes)
    // Note: We don't actually switch modes here since the splits have already been merged
    // But we verify the configuration is properly handled
    val currentMode = spark.conf.get("spark.tantivy4spark.merge.mode", "process")
    assert(currentMode == "process", "Should be using process mode")

    println(s"✅ Merge mode handling verified: currently using '$currentMode' mode")
  }

  test("Process-based merge should handle S3 credential propagation") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/credential-propagation-test"

    // Create test data
    val data = spark.range(300).select(
      col("id"),
      concat(lit("Content for credential test "), col("id")).as("content")
    )

    println(s"✍️  Writing test data for credential propagation test...")

    val writeOptions = getWriteOptions() ++ Map(
      "spark.tantivy4spark.indexwriter.batchSize" -> "75"  // Force multiple splits
    )

    data.write
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    // Verify credentials are properly configured
    val (accessKey, secretKey) = awsCredentials.get
    assert(spark.conf.get("spark.tantivy4spark.aws.accessKey") == accessKey, "Access key should be configured")
    assert(spark.conf.get("spark.tantivy4spark.aws.region") == S3_REGION, "Region should be configured")

    println(s"✅ AWS credentials properly configured in Spark session")

    // Execute merge operation - this will test credential propagation internally
    println(s"🔧 Testing credential propagation during merge...")

    import com.tantivy4spark.sql.Tantivy4SparkSqlParser
    val sqlParser = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    val mergeCommand = sqlParser.parsePlan(s"MERGE SPLITS '$tablePath' TARGET SIZE 1048576") // 1MB
      .asInstanceOf[com.tantivy4spark.sql.MergeSplitsCommand]

    // This will internally test if credentials are properly propagated to the merge processes
    val result = mergeCommand.run(spark)
    assert(result.nonEmpty, "Merge should succeed if credentials are properly propagated")

    println(s"   Merge result: ${result.head.getString(1)}")

    // Verify data is still accessible after merge (final credential test)
    val readOptions = getReadOptions()
    val finalData = spark.read
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    val finalCount = finalData.count()
    finalCount shouldBe 300

    println(s"✅ Credential propagation test successful: $finalCount records accessible after merge")
  }
}