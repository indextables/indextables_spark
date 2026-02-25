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

package io.indextables.spark.prewarm

import java.io.{File, FileInputStream}
import java.nio.file.Files
import java.util.{Properties, UUID}

import scala.util.Using

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.hadoop.fs.{FileSystem, Path}

import io.indextables.spark.CloudS3TestBase
import io.indextables.spark.storage.GlobalSplitCacheManager

/**
 * End-to-end test for PARQUET_COLUMNS prewarm on a companion index built from a real Delta table on S3.
 *
 * Test flow:
 *   1. Write a Delta table to S3 with at least 10 parquet files
 *   2. BUILD INDEXTABLES COMPANION on that Delta table to create an index in S3
 *   3. PREWARM INDEXTABLES CACHE ... FOR SEGMENTS (PARQUET_COLUMNS) ON FIELDS (score)
 *   4. Read back the companion index selecting only the prewarmed column (score)
 *   5. Print debug output (run with TANTIVY4JAVA_PERFLOG=1 for full detail)
 *
 * Prerequisites:
 *   - AWS credentials in ~/.aws/credentials
 *   - Delta Lake Spark extension on classpath
 *   - Run with: TANTIVY4JAVA_PERFLOG=1 mvn test-compile scalatest:test \
 *       -DwildcardSuites='io.indextables.spark.prewarm.CloudS3CompanionParquetColumnsPrewarmTest'
 */
class CloudS3CompanionParquetColumnsPrewarmTest extends CloudS3TestBase {

  private val S3_BUCKET    = System.getProperty("test.s3.bucket", "test-tantivy4sparkbucket")
  private val S3_REGION    = System.getProperty("test.s3.region", "us-east-2")
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/companion-parquet-cols-prewarm-$testRunId"

  private var awsCredentials: Option[(String, String)] = None
  private var fs: FileSystem                           = _
  private var diskCachePath: String                    = _

  private lazy val hasDeltaSparkDataSource: Boolean =
    try {
      Class.forName("io.delta.sql.DeltaSparkSessionExtension")
      true
    } catch {
      case _: ClassNotFoundException => false
    }

  override def beforeAll(): Unit = {
    super.beforeAll()

    awsCredentials = loadAwsCredentials()

    // Recreate SparkSession with Delta extensions if available
    if (hasDeltaSparkDataSource) {
      spark.stop()
      spark = SparkSession
        .builder()
        .appName("CloudS3CompanionParquetColumnsPrewarmTest")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config(
          "spark.sql.extensions",
          "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
    }

    if (awsCredentials.isDefined) {
      val (accessKey, secretKey) = awsCredentials.get

      val hadoopConf = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("fs.s3a.access.key", accessKey)
      hadoopConf.set("fs.s3a.secret.key", secretKey)
      hadoopConf.set("fs.s3a.endpoint.region", S3_REGION)
      hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
      hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
      hadoopConf.set("spark.indextables.aws.region", S3_REGION)

      spark.conf.set("spark.indextables.aws.accessKey", accessKey)
      spark.conf.set("spark.indextables.aws.secretKey", secretKey)
      spark.conf.set("spark.indextables.aws.region", S3_REGION)

      fs = new Path(testBasePath).getFileSystem(hadoopConf)

      println(s"AWS credentials loaded successfully")
      println(s"Test base path: $testBasePath")
    } else {
      println("No AWS credentials found - tests will be skipped")
    }

    diskCachePath = Files.createTempDirectory("companion_parquet_cols_disk_cache_").toFile.getAbsolutePath
    println(s"Disk cache path: $diskCachePath")
  }

  override def afterAll(): Unit =
    try {
      if (awsCredentials.isDefined && fs != null) {
        val basePath = new Path(testBasePath)
        if (fs.exists(basePath)) {
          println(s"Cleaning up S3 test directory: $testBasePath")
          fs.delete(basePath, true)
        }
      }
      if (diskCachePath != null) {
        deleteRecursively(new File(diskCachePath))
      }
    } finally
      super.afterAll()

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  private def loadAwsCredentials(): Option[(String, String)] =
    try {
      val home     = System.getProperty("user.home")
      val credFile = new File(s"$home/.aws/credentials")
      if (!credFile.exists()) {
        println(s"AWS credentials file not found at: ${credFile.getAbsolutePath}")
        return None
      }
      Using(new FileInputStream(credFile)) { fis =>
        val props = new Properties()
        props.load(fis)
        val accessKey = props.getProperty("aws_access_key_id")
        val secretKey = props.getProperty("aws_secret_access_key")
        if (accessKey != null && secretKey != null) Some((accessKey, secretKey))
        else {
          println("AWS credentials not found in ~/.aws/credentials")
          None
        }
      }.toOption.flatten
    } catch {
      case ex: Exception =>
        println(s"Failed to read AWS credentials: ${ex.getMessage}")
        None
    }

  test("PARQUET_COLUMNS prewarm on companion index from Delta table with 10+ parquet files") {
    assume(
      awsCredentials.isDefined && hasDeltaSparkDataSource,
      "AWS credentials and Delta Spark DataSource required - skipping test"
    )

    val (accessKey, secretKey) = awsCredentials.get
    val deltaPath              = s"$testBasePath/delta_source"
    val indexPath               = s"$testBasePath/companion_index"

    // Enable disk cache
    spark.conf.set("spark.indextables.cache.disk.enabled", "true")
    spark.conf.set("spark.indextables.cache.disk.path", diskCachePath)
    spark.conf.set("spark.indextables.cache.disk.maxSize", "1G")

    val ss = spark
    import ss.implicits._

    // -------------------------------------------------------------------------
    // Step 1: Create a Delta table on S3 with at least 10 parquet files
    // -------------------------------------------------------------------------
    println("\n" + "=" * 80)
    println("STEP 1: Creating Delta table with 10+ parquet files on S3")
    println("=" * 80)

    val numRows = 1000
    val testData = (1 to numRows).map { i =>
      (
        i.toLong,                                                          // id
        s"document_title_$i",                                              // title
        s"This is the full text body for document number $i with words",   // body
        s"category_${i % 5}",                                             // category
        i * 2.5,                                                           // score
        i % 100,                                                           // priority
        s"2025-${"%02d".format((i % 12) + 1)}-${"%02d".format((i % 28) + 1)}" // date_str
      )
    }.toDF("id", "title", "body", "category", "score", "priority", "date_str")

    // Repartition to 12 to guarantee at least 10 parquet files
    testData
      .repartition(12)
      .write
      .format("delta")
      .mode("overwrite")
      .save(deltaPath)

    // Verify we got enough parquet files
    val deltaFiles = fs.listStatus(new Path(deltaPath)).filter(_.getPath.getName.endsWith(".parquet"))
    println(s"Delta table created at: $deltaPath")
    println(s"Number of parquet files: ${deltaFiles.length}")
    assert(deltaFiles.length >= 10, s"Expected at least 10 parquet files, got ${deltaFiles.length}")

    // Verify row count via Delta read
    val deltaReadDf = spark.read.format("delta").load(deltaPath)
    val deltaRowCount = deltaReadDf.count()
    println(s"Delta table row count: $deltaRowCount")
    assert(deltaRowCount == numRows, s"Expected $numRows rows, got $deltaRowCount")

    // -------------------------------------------------------------------------
    // Step 2: BUILD INDEXTABLES COMPANION on the Delta table
    // -------------------------------------------------------------------------
    println("\n" + "=" * 80)
    println("STEP 2: Building companion index from Delta table")
    println("=" * 80)

    val buildResult = spark.sql(
      s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
    )
    val buildRows = buildResult.collect()
    buildRows.foreach(row => println(s"  BUILD result: $row"))

    assert(buildRows.length == 1, "BUILD should return exactly 1 row")
    val buildRow = buildRows(0)
    val buildStatus = buildRow.getString(buildRow.fieldIndex("status"))
    assert(buildStatus == "success", s"BUILD should succeed, got: $buildStatus")

    val splitsCreated = buildRow.getInt(buildRow.fieldIndex("splits_created"))
    val parquetFilesIndexed = buildRow.getInt(buildRow.fieldIndex("parquet_files_indexed"))
    println(s"Companion index created at: $indexPath")
    println(s"Splits created: $splitsCreated")
    println(s"Parquet files indexed: $parquetFilesIndexed")
    assert(splitsCreated > 0, "Should create at least one split")
    assert(parquetFilesIndexed >= 10, s"Should index at least 10 parquet files, got $parquetFilesIndexed")

    // -------------------------------------------------------------------------
    // Step 3: Flush caches and PREWARM with PARQUET_COLUMNS on the 'score' field
    // -------------------------------------------------------------------------
    println("\n" + "=" * 80)
    println("STEP 3: Flushing caches and prewarming PARQUET_COLUMNS for 'score' field")
    println("=" * 80)

    spark.sql("FLUSH INDEXTABLES DISK CACHE").collect()
    GlobalSplitCacheManager.flushAllCaches()
    Thread.sleep(500)
    println("Caches flushed")

    val prewarmSql = s"PREWARM INDEXTABLES CACHE '$indexPath' FOR SEGMENTS (PARQUET_COLUMNS) ON FIELDS (score)"
    println(s"Executing: $prewarmSql")

    val prewarmStart = System.currentTimeMillis()
    val prewarmResult = spark.sql(prewarmSql)
    val prewarmRows = prewarmResult.collect()
    val prewarmDuration = System.currentTimeMillis() - prewarmStart

    println(s"\nPrewarm results ({prewarmRows.length} rows):")
    prewarmResult.show(truncate = false)
    prewarmRows.foreach { row =>
      println(s"  Row: $row")
      // Print all columns individually for debug
      row.schema.fieldNames.zipWithIndex.foreach { case (name, idx) =>
        println(s"    $name = ${if (row.isNullAt(idx)) "NULL" else row.get(idx)}")
      }
    }
    println(s"Prewarm wall-clock duration: ${prewarmDuration}ms")

    assert(prewarmRows.nonEmpty, "Prewarm should return results")
    val prewarmStatus = prewarmRows.head.getAs[String]("status")
    println(s"Prewarm status: $prewarmStatus")

    val splitsPrewarmed = prewarmRows.map(_.getAs[Int]("splits_prewarmed")).sum
    println(s"Total splits prewarmed: $splitsPrewarmed")

    // -------------------------------------------------------------------------
    // Step 4a: CONTROL - Read ALL columns (no projection) to get baseline count
    // -------------------------------------------------------------------------
    println("\n" + "=" * 80)
    println("STEP 4a: CONTROL - reading ALL columns to establish baseline row count")
    println("=" * 80)

    val companionDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(indexPath)

    println(s"Companion schema:\n${companionDf.schema.treeString}")

    val allColsStart = System.currentTimeMillis()
    val allCols = companionDf.collect()
    val allColsDuration = System.currentTimeMillis() - allColsStart
    println(s"CONTROL: All columns returned {allCols.length} rows in {allColsDuration}ms")

    // Also check count() which uses aggregate pushdown
    val countStart = System.currentTimeMillis()
    val aggCount = companionDf.count()
    val countDuration = System.currentTimeMillis() - countStart
    println(s"CONTROL: count() = $aggCount in ${countDuration}ms (aggregate pushdown)")

    // -------------------------------------------------------------------------
    // Step 4b: Read selecting ONLY the prewarmed column (score)
    // -------------------------------------------------------------------------
    println("\n" + "=" * 80)
    println("STEP 4b: Reading companion index - selecting ONLY 'score' column (prewarmed)")
    println("=" * 80)

    // Flush caches again so the select("score") read is a clean exercise of the PARQUET_COLUMNS path
    spark.sql("FLUSH INDEXTABLES DISK CACHE").collect()
    GlobalSplitCacheManager.flushAllCaches()
    Thread.sleep(500)

    // Re-prewarm PARQUET_COLUMNS for score
    println("Re-prewarming PARQUET_COLUMNS for 'score' after flush...")
    val prewarmResult2 = spark.sql(
      s"PREWARM INDEXTABLES CACHE '$indexPath' FOR SEGMENTS (PARQUET_COLUMNS) ON FIELDS (score)"
    )
    prewarmResult2.show(truncate = false)

    val companionDf2 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(indexPath)

    val readStart = System.currentTimeMillis()
    val scoreOnly = companionDf2.select("score").collect()
    val readDuration = System.currentTimeMillis() - readStart

    println(s"Rows returned (score only): ${scoreOnly.length}")
    println(s"Read wall-clock duration: ${readDuration}ms")
    println(s"Sample scores (first 20):")
    scoreOnly.take(20).foreach(row => println(s"  score = ${row.get(0)}"))

    if (scoreOnly.length != numRows) {
      println(s"BUG? Expected $numRows rows from companion select('score'), got ${scoreOnly.length}")
      println(s"  CONTROL read (all columns) returned: ${allCols.length} rows")
      println(s"  CONTROL count() returned: $aggCount")
      println(s"  This may indicate a PARQUET_COLUMNS retrieval bug where projected")
      println(s"  reads return fewer rows than the full table")
    } else {
      println(s"Row count matches expected: $numRows")
    }

    if (scoreOnly.nonEmpty) {
      val scores = scoreOnly.map(_.getDouble(0))
      println(s"Score range: min=${scores.min}, max=${scores.max}")
    }

    // -------------------------------------------------------------------------
    // Step 5: Select score WITHOUT any prewarm (flush + read)
    // -------------------------------------------------------------------------
    println("\n" + "=" * 80)
    println("STEP 5: CONTROL - select('score') WITHOUT PARQUET_COLUMNS prewarm")
    println("=" * 80)

    spark.sql("FLUSH INDEXTABLES DISK CACHE").collect()
    GlobalSplitCacheManager.flushAllCaches()
    Thread.sleep(500)
    println("Caches flushed - NO prewarm this time")

    val companionDf3 = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .load(indexPath)

    val noPrewarmStart = System.currentTimeMillis()
    val noPrewarmScore = companionDf3.select("score").collect()
    val noPrewarmDuration = System.currentTimeMillis() - noPrewarmStart

    println(s"Rows returned (score only, NO prewarm): ${noPrewarmScore.length}")
    println(s"Read wall-clock duration: ${noPrewarmDuration}ms")

    if (noPrewarmScore.length != scoreOnly.length) {
      println(s"INTERESTING: Row count differs WITH prewarm (${scoreOnly.length}) vs WITHOUT prewarm (${noPrewarmScore.length})")
    } else {
      println(s"Row count same with and without prewarm: ${scoreOnly.length}")
    }

    // -------------------------------------------------------------------------
    // Step 6: Filtered read - score > 1000
    // -------------------------------------------------------------------------
    println("\n" + "=" * 80)
    println("STEP 6: Filtered read - score > 1000")
    println("=" * 80)

    val filteredStart = System.currentTimeMillis()
    val filteredScores = companionDf3.select("score").filter(col("score") > 1000.0).collect()
    val filteredDuration = System.currentTimeMillis() - filteredStart

    println(s"Filtered rows (score > 1000): ${filteredScores.length}")
    println(s"Filtered read duration: ${filteredDuration}ms")
    println(s"Sample filtered scores (first 10):")
    filteredScores.take(10).foreach(row => println(s"  score = ${row.get(0)}"))

    val expectedFiltered = (401 to numRows).length
    if (filteredScores.length != expectedFiltered) {
      println(s"WARNING: Expected $expectedFiltered rows with score > 1000, got ${filteredScores.length}")
    } else {
      println(s"Filtered row count matches expected: $expectedFiltered")
    }

    // -------------------------------------------------------------------------
    // Step 7: Summary
    // -------------------------------------------------------------------------
    println("\n" + "=" * 80)
    println("SUMMARY")
    println("=" * 80)
    println(s"Delta parquet files:         ${deltaFiles.length}")
    println(s"Companion splits:            $splitsCreated")
    println(s"Parquet files indexed:       $parquetFilesIndexed")
    println(s"Splits prewarmed:            $splitsPrewarmed")
    println(s"Prewarm status:              $prewarmStatus")
    println(s"CONTROL all-cols rows:       ${allCols.length}")
    println(s"CONTROL count():             $aggCount")
    println(s"select('score') WITH prewarm:    ${scoreOnly.length}")
    println(s"select('score') WITHOUT prewarm: ${noPrewarmScore.length}")
    println(s"Filtered rows read:          ${filteredScores.length}")
    println(s"Prewarm duration:            ${prewarmDuration}ms")
    println(s"All-cols scan duration:      ${allColsDuration}ms")
    println(s"Score-only scan duration:    ${readDuration}ms")
    println(s"No-prewarm score duration:   ${noPrewarmDuration}ms")
    println(s"Filtered scan duration:      ${filteredDuration}ms")
    println()
    println("NOTE: Run with TANTIVY4JAVA_PERFLOG=1 for detailed native performance logging")
    println("=" * 80)
  }
}
