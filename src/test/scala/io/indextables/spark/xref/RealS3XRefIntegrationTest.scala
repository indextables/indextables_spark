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

package io.indextables.spark.xref

import java.util.UUID

import org.apache.spark.sql.functions._

import io.indextables.spark.RealS3TestBase

/**
 * Real AWS S3 integration tests for XRef (Cross-Reference) indexing.
 *
 * These tests validate XRef functionality against real S3 storage:
 *   - XRef build with splits stored in S3
 *   - XRef query routing with S3-based indexes
 *   - Mixed XRef coverage (covered + uncovered splits)
 *   - Credential propagation to tantivy4java XRef API
 *
 * Prerequisites:
 *   - AWS credentials configured in ~/.aws/credentials
 *   - S3 bucket accessible for testing
 *
 * Run with: mvn test -Dtest=RealS3XRefIntegrationTest
 */
class RealS3XRefIntegrationTest extends RealS3TestBase {

  private val DataSourceFormat = "io.indextables.spark.core.IndexTables4SparkTableProvider"
  private val S3_BUCKET = "test-tantivy4sparkbucket"
  private val S3_REGION = "us-east-2"

  // Generate unique test path to avoid conflicts between test runs
  private val testRunId = UUID.randomUUID().toString.substring(0, 8)
  private def testTablePath: String = s"s3a://$S3_BUCKET/xref-test-$testRunId"

  override def beforeAll(): Unit = {
    super.beforeAll()

    if (hasAwsCredentials()) {
      configureSparkForS3()
    }
  }

  override def afterAll(): Unit = {
    // Clean up test data
    cleanupTestData(testTablePath)
    super.afterAll()
  }

  private def configureSparkForS3(): Unit = {
    // Load credentials from ~/.aws/credentials (INI format with [default] section)
    val credentialsFile = new java.io.File(System.getProperty("user.home"), ".aws/credentials")
    if (credentialsFile.exists()) {
      try {
        val lines = scala.io.Source.fromFile(credentialsFile).getLines().toList
        var inDefaultSection = false
        var accessKey: Option[String] = None
        var secretKey: Option[String] = None
        var sessionToken: Option[String] = None

        for (line <- lines) {
          val trimmed = line.trim
          if (trimmed == "[default]") {
            inDefaultSection = true
          } else if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
            inDefaultSection = false
          } else if (inDefaultSection && trimmed.contains("=")) {
            val parts = trimmed.split("=", 2)
            if (parts.length == 2) {
              val key = parts(0).trim
              val value = parts(1).trim
              key match {
                case "aws_access_key_id" => accessKey = Some(value)
                case "aws_secret_access_key" => secretKey = Some(value)
                case "aws_session_token" => sessionToken = Some(value)
                case _ => // ignore other keys
              }
            }
          }
        }

        accessKey.foreach { key =>
          spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
          spark.conf.set("spark.indextables.aws.accessKey", key)
        }

        secretKey.foreach { key =>
          spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", key)
          spark.conf.set("spark.indextables.aws.secretKey", key)
        }

        sessionToken.foreach { token =>
          spark.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", token)
          spark.conf.set("spark.indextables.aws.sessionToken", token)
          spark.sparkContext.hadoopConfiguration
            .set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        }

        spark.sparkContext.hadoopConfiguration.set("fs.s3a.region", S3_REGION)
        spark.conf.set("spark.indextables.aws.region", S3_REGION)

        // S3A optimizations
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.multipart.size", "67108864")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "bytebuffer")

      } catch {
        case _: Exception => // Credentials will be loaded from environment or IAM role
      }
    }
  }

  private def cleanupTestData(path: String): Unit = {
    try {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(path), hadoopConf)
      val hadoopPath = new org.apache.hadoop.fs.Path(path)

      if (fs.exists(hadoopPath)) {
        fs.delete(hadoopPath, true)
      }
    } catch {
      case _: Exception => // Best-effort cleanup
    }
  }

  /**
   * Test XRef build and query on real S3 storage.
   *
   * This test verifies:
   * - XRef indexes can be built from splits stored in S3
   * - XRef query routing works with S3-based indexes
   * - Results are correct when querying through XRef
   */
  test("XRef build and query on S3") {
    assume(hasAwsCredentials(), "AWS credentials required for S3 XRef test")

    val tablePath = s"$testTablePath/basic"
    val spark = this.spark
    import spark.implicits._

    println(s"Starting XRef S3 integration test at: $tablePath")

    try {
      // Configure for XRef testing
      spark.conf.set("spark.indextables.indexWriter.batchSize", "100")
      spark.conf.set("spark.indextables.xref.query.enabled", "true")
      spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "3")

      // Create test data with multiple splits
      (0 until 10).foreach { batchIdx =>
        val data = (0 until 100).map { i =>
          val globalId = batchIdx * 100 + i
          (
            globalId,
            s"term_batch${batchIdx}_row$i",
            s"batch_$batchIdx",
            globalId * 10
          )
        }
        val df = data.toDF("id", "searchable_term", "batch_marker", "value")

        df.write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "value")
          .mode("append")
          .save(tablePath)
      }

      println(s"Wrote 10 batches of test data to S3")

      // Build XRef index
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")
      println(s"XRef index build completed")

      // Verify XRef was created in transaction log
      val xrefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
      val xrefCount = xrefResult.count()
      assert(xrefCount >= 1, s"XRef entry should be created in transaction log, got $xrefCount")
      println(s"Verified $xrefCount XRef entries in transaction log")

      // Query using XRef
      val dfAll = spark.read.format(DataSourceFormat).load(tablePath)

      // Term query - should use XRef routing
      val termResult = dfAll.filter(col("searchable_term") === "term_batch5_row50")
      assert(termResult.count() == 1, "Term query should return exactly 1 row")

      val row = termResult.collect().head
      assert(row.getAs[Int]("id") == 550, s"Expected id=550, got ${row.getAs[Int]("id")}")

      // Aggregate query
      val countResult = dfAll.agg(count("*")).collect().head.getLong(0)
      assert(countResult == 1000, s"Expected 1000 total rows, got $countResult")

      println(s"XRef S3 integration test PASSED")

    } finally {
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.query.enabled")
      spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
      cleanupTestData(tablePath)
    }
  }

  /**
   * Test XRef with mixed coverage on S3 - verifies uncovered splits are included.
   *
   * This test catches the bug where uncovered splits were incorrectly excluded
   * from query results when XRef routing was active.
   */
  test("XRef mixed coverage on S3 - uncovered splits must be included") {
    assume(hasAwsCredentials(), "AWS credentials required for S3 XRef test")

    val tablePath = s"$testTablePath/mixed-coverage"
    val spark = this.spark
    import spark.implicits._

    println(s"Starting XRef mixed coverage S3 test at: $tablePath")

    try {
      spark.conf.set("spark.indextables.indexWriter.batchSize", "100")
      spark.conf.set("spark.indextables.xref.query.enabled", "true")
      spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "2")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2") // Lower threshold for WHERE-filtered builds

      // Create partitioned data with IDENTICAL searchable terms across all partitions
      val sharedTerms = (0 until 10).map(i => s"shared_term_$i")
      val partitions = Seq("covered_1", "covered_2", "uncovered_1", "uncovered_2")

      partitions.foreach { partition =>
        val data = (0 until 100).map { i =>
          (
            s"${partition}_$i",
            sharedTerms(i % 10),
            partition,
            i * 10
          )
        }
        val df = data.toDF("id", "searchable_term", "partition_key", "value")

        df.write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "value")
          .partitionBy("partition_key")
          .mode("append")
          .save(tablePath)
      }

      println(s"Created partitioned data on S3")

      // Build XRef ONLY for covered partitions
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath' WHERE partition_key IN ('covered_1', 'covered_2')")
      println(s"Built XRef for covered partitions only")

      // Verify XRef was created for covered partitions
      val xrefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
      val xrefCount = xrefResult.count()
      assert(xrefCount >= 1, s"XRef entry should be created for covered partitions, got $xrefCount")
      println(s"Verified $xrefCount XRef entries in transaction log")

      val dfAll = spark.read.format(DataSourceFormat).load(tablePath)

      // Query for shared term - should return from ALL partitions (including uncovered)
      val sharedTermQuery = dfAll.filter(col("searchable_term") === "shared_term_0")
      val sharedTermCount = sharedTermQuery.count()

      // CRITICAL: Should return 40 rows (10 per partition × 4 partitions)
      // BUG: Without fix, uncovered partitions would be excluded, returning only 20 rows
      assert(sharedTermCount == 40,
        s"Query for shared term should return 40 rows (from ALL partitions), got $sharedTermCount. " +
        "This indicates uncovered splits are being incorrectly excluded on S3!")

      println(s"Shared term query returned $sharedTermCount rows (expected 40)")

      // Verify each partition has results
      partitions.foreach { partition =>
        val partitionCount = dfAll
          .filter(col("searchable_term") === "shared_term_0")
          .filter(col("partition_key") === partition)
          .count()
        assert(partitionCount == 10,
          s"Partition $partition should have 10 rows, got $partitionCount")
      }

      println(s"All partitions (covered and uncovered) returned correct results")

      // Add data only to uncovered partitions and verify it's found
      val uncoveredOnlyData = Seq(
        ("unique_1", "only_in_uncovered", "uncovered_1", 999),
        ("unique_2", "only_in_uncovered", "uncovered_2", 999)
      ).toDF("id", "searchable_term", "partition_key", "value")

      uncoveredOnlyData.write.format(DataSourceFormat)
        .option("spark.indextables.indexing.fastfields", "value")
        .partitionBy("partition_key")
        .mode("append")
        .save(tablePath)

      val dfUpdated = spark.read.format(DataSourceFormat).load(tablePath)
      val uncoveredOnlyCount = dfUpdated.filter(col("searchable_term") === "only_in_uncovered").count()

      assert(uncoveredOnlyCount == 2,
        s"Query for term only in uncovered splits should return 2 rows, got $uncoveredOnlyCount")

      println(s"Data in uncovered splits found correctly")
      println(s"XRef mixed coverage S3 test PASSED")

    } finally {
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.query.enabled")
      spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
      cleanupTestData(tablePath)
    }
  }

  /**
   * Test XRef local cache feature on S3.
   *
   * This test verifies:
   * - XRef splits are lazily downloaded to local disk when localCache.enabled=true
   * - Repeated queries use the cached XRef (no re-download)
   * - Cache directory is created and contains XRef files
   */
  test("XRef local cache on S3") {
    assume(hasAwsCredentials(), "AWS credentials required for S3 XRef test")

    val tablePath = s"$testTablePath/local-cache"
    val spark = this.spark
    import spark.implicits._

    // Use a dedicated cache directory for this test
    val cacheDir = new java.io.File(System.getProperty("java.io.tmpdir"), s"xref-cache-test-$testRunId")

    println(s"Starting XRef local cache S3 test at: $tablePath")
    println(s"Cache directory: ${cacheDir.getAbsolutePath}")

    try {
      // Configure for XRef testing WITH local cache enabled
      spark.conf.set("spark.indextables.indexWriter.batchSize", "100")
      spark.conf.set("spark.indextables.xref.query.enabled", "true")
      spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "3")
      spark.conf.set("spark.indextables.xref.localCache.enabled", "true")
      spark.conf.set("spark.indextables.xref.localCache.directory", cacheDir.getAbsolutePath)

      // Create test data with multiple splits
      (0 until 5).foreach { batchIdx =>
        val data = (0 until 100).map { i =>
          val globalId = batchIdx * 100 + i
          (globalId, s"term_batch${batchIdx}_row$i", s"batch_$batchIdx", globalId * 10)
        }
        val df = data.toDF("id", "searchable_term", "batch_marker", "value")

        df.write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "value")
          .mode("append")
          .save(tablePath)
      }

      println(s"Wrote 5 batches of test data to S3")

      // Build XRef index
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")
      println(s"XRef index build completed")

      // First query - should download XRef to local cache
      val dfAll = spark.read.format(DataSourceFormat).load(tablePath)
      val result1 = dfAll.filter(col("searchable_term") === "term_batch2_row50").count()
      assert(result1 == 1, s"First query should return 1 row, got $result1")
      println(s"First query completed (XRef should be downloaded to cache)")

      // Check cache directory has files
      val cacheSubdir = new java.io.File(cacheDir, "_xref_cache")
      if (cacheSubdir.exists()) {
        val cachedFiles = cacheSubdir.listFiles()
        println(s"Cache directory contains ${cachedFiles.length} files")
        cachedFiles.foreach(f => println(s"  - ${f.getName} (${f.length()} bytes)"))
        assert(cachedFiles.nonEmpty, "Cache directory should contain XRef files")
      } else {
        println(s"Note: Cache subdirectory not created (may be running on driver only)")
      }

      // Second query - should use cached XRef (no re-download)
      val result2 = dfAll.filter(col("searchable_term") === "term_batch3_row25").count()
      assert(result2 == 1, s"Second query should return 1 row, got $result2")
      println(s"Second query completed (should use cached XRef)")

      // Verify aggregate also works with cache
      val countResult = dfAll.agg(count("*")).collect().head.getLong(0)
      assert(countResult == 500, s"Expected 500 total rows, got $countResult")

      println(s"XRef local cache S3 test PASSED")

    } finally {
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.query.enabled")
      spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
      spark.conf.unset("spark.indextables.xref.localCache.enabled")
      spark.conf.unset("spark.indextables.xref.localCache.directory")

      // Clean up cache directory
      if (cacheDir.exists()) {
        deleteRecursively(cacheDir)
      }
      cleanupTestData(tablePath)
    }
  }

  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  /**
   * Test XRef aggregate pushdown on S3.
   */
  test("XRef aggregate pushdown on S3") {
    assume(hasAwsCredentials(), "AWS credentials required for S3 XRef test")

    val tablePath = s"$testTablePath/aggregates"
    val spark = this.spark
    import spark.implicits._

    println(s"Starting XRef aggregate pushdown S3 test at: $tablePath")

    try {
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.query.enabled", "true")
      spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "3")

      // Create test data with categories
      val categories = Seq("A", "B", "C", "D")
      (0 until 20).foreach { batchIdx =>
        val category = categories(batchIdx % 4)
        val data = (0 until 50).map { i =>
          (batchIdx * 50 + i, category, (i + 1).toLong, (i % 10).toDouble)
        }
        val df = data.toDF("id", "category", "amount", "discount")

        df.write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "amount,discount")
          .mode("append")
          .save(tablePath)
      }

      println(s"Created test data with categories on S3")

      // Build XRef
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")
      println(s"Built XRef index")

      // Verify XRef was created
      val xrefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
      val xrefCount = xrefResult.count()
      assert(xrefCount >= 1, s"XRef entry should be created in transaction log, got $xrefCount")
      println(s"Verified $xrefCount XRef entries in transaction log")

      val dfAll = spark.read.format(DataSourceFormat).load(tablePath)

      // COUNT across all data
      val totalCount = dfAll.agg(count("*")).collect().head.getLong(0)
      assert(totalCount == 1000, s"Expected 1000 total rows, got $totalCount")

      // SUM with filter
      val sumResult = dfAll
        .filter(col("category") === "A")
        .agg(sum("amount"))
        .collect().head.getLong(0)

      // 5 batches with category A, 50 rows each, amounts 1-50
      // Sum per batch = 1+2+...+50 = 1275
      // Total = 1275 * 5 = 6375
      assert(sumResult == 6375L, s"Expected SUM=6375 for category A, got $sumResult")

      // AVG
      val avgResult = dfAll
        .filter(col("amount") > 25)
        .agg(avg("amount"))
        .collect().head.getDouble(0)

      // Amounts > 25: 26-50 (25 values per batch), average = 38
      assert(avgResult >= 37.0 && avgResult <= 39.0, s"Expected AVG around 38, got $avgResult")

      // MIN/MAX
      val minMax = dfAll.agg(min("amount"), max("amount")).collect().head
      assert(minMax.getLong(0) == 1, s"Expected MIN=1, got ${minMax.getLong(0)}")
      assert(minMax.getLong(1) == 50, s"Expected MAX=50, got ${minMax.getLong(1)}")

      println(s"All aggregate queries returned correct results")
      println(s"XRef aggregate pushdown S3 test PASSED")

    } finally {
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.query.enabled")
      spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
      cleanupTestData(tablePath)
    }
  }
}
