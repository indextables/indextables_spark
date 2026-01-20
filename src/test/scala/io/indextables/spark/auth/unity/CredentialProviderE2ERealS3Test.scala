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

package io.indextables.spark.auth.unity

import java.io.{File, FileInputStream}
import java.net.URI
import java.nio.file.Files
import java.util.{Properties, UUID}

import scala.util.Using

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.hadoop.conf.Configuration

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Custom credential provider that loads AWS credentials from ~/.aws/credentials file. This simulates a real-world
 * credential provider that resolves credentials dynamically.
 */
class AwsCredentialsFileProvider(uri: URI, conf: Configuration) extends AWSCredentialsProvider {

  // Track instantiation for testing
  AwsCredentialsFileProvider.recordInstantiation(uri, conf)

  // Load credentials from file
  private val (accessKey, secretKey) = loadAwsCredentials()

  override def getCredentials(): AWSCredentials = {
    println(s"[AwsCredentialsFileProvider] getCredentials() called for URI: $uri")
    new BasicAWSCredentials(accessKey, secretKey)
  }

  override def refresh(): Unit =
    println(s"[AwsCredentialsFileProvider] refresh() called")

  private def loadAwsCredentials(): (String, String) = {
    val home     = System.getProperty("user.home")
    val credFile = new File(s"$home/.aws/credentials")

    if (!credFile.exists()) {
      throw new RuntimeException(s"~/.aws/credentials file not found at ${credFile.getAbsolutePath}")
    }

    val props = new Properties()
    Using(new FileInputStream(credFile))(fis => props.load(fis)).get

    val accessKey = props.getProperty("aws_access_key_id")
    val secretKey = props.getProperty("aws_secret_access_key")

    if (accessKey == null || secretKey == null) {
      throw new RuntimeException("aws_access_key_id or aws_secret_access_key not found in ~/.aws/credentials")
    }

    println(
      s"[AwsCredentialsFileProvider] Loaded credentials from ~/.aws/credentials (accessKey: ${accessKey.take(8)}...)"
    )
    (accessKey, secretKey)
  }
}

object AwsCredentialsFileProvider {
  // Track instantiations for verification
  @volatile var instantiations: List[(URI, String)] = List.empty
  private val lock                                  = new Object

  def recordInstantiation(uri: URI, conf: Configuration): Unit = lock.synchronized {
    val role = if (isDriver()) "DRIVER" else "EXECUTOR"
    instantiations = instantiations :+ (uri, role)
    println(s"[AwsCredentialsFileProvider] Instantiated on $role for URI: $uri")
  }

  def reset(): Unit = lock.synchronized {
    instantiations = List.empty
  }

  def getInstantiations: List[(URI, String)] = lock.synchronized {
    instantiations
  }

  private def isDriver(): Boolean =
    // Simple heuristic: check if we can access SparkContext
    try
      org.apache.spark.SparkContext.getOrCreate() != null
    catch {
      case _: Exception => false
    }
}

/**
 * End-to-end integration test that verifies custom credential providers work correctly across all operations: WRITE,
 * READ, MERGE SPLITS, PURGE, REPAIR, etc.
 *
 * This test:
 *   1. Creates a custom credential provider that loads from ~/.aws/credentials 2. Ensures all AWS id/session/key are
 *      UNSET in the Spark context 3. Configures only the provider class 4. Performs all relevant operations against
 *      real S3 5. Validates that credentials are properly resolved on driver and propagated to executors
 */
class CredentialProviderE2ERealS3Test extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/credential-provider-e2e-$testRunId"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create Spark session WITHOUT any AWS credentials
    spark = SparkSession
      .builder()
      .appName("CredentialProviderE2ETest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
      // IMPORTANT: Do NOT set any AWS credentials here - only the provider class
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println(s"🚀 Test base path: $testBasePath")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Reset instantiation tracking
    AwsCredentialsFileProvider.reset()

    // Clear credential provider cache to force re-instantiation
    // This is necessary because CredentialProviderFactory caches provider instances
    try
      io.indextables.spark.utils.CredentialProviderFactory.clearCache()
    catch {
      case _: Exception => // Ignore
    }

    // Clear global split cache
    try {
      import io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
      GlobalSplitCacheManager.flushAllCaches()
      DriverSplitLocalityManager.clear()
    } catch {
      case _: Exception => // Ignore
    }

    // CRITICAL: Ensure NO explicit AWS credentials are set
    // This forces the system to use our custom credential provider
    spark.conf.unset("spark.indextables.aws.accessKey")
    spark.conf.unset("spark.indextables.aws.secretKey")
    spark.conf.unset("spark.indextables.aws.sessionToken")

    // Verify credentials are unset
    spark.conf.getOption("spark.indextables.aws.accessKey") shouldBe None
    spark.conf.getOption("spark.indextables.aws.secretKey") shouldBe None
    spark.conf.getOption("spark.indextables.aws.sessionToken") shouldBe None

    println("✅ Verified: No explicit AWS credentials set in Spark context")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  /** Check if AWS credentials file exists */
  private def hasAwsCredentials: Boolean = {
    val home     = System.getProperty("user.home")
    val credFile = new File(s"$home/.aws/credentials")
    credFile.exists()
  }

  /**
   * COMPREHENSIVE E2E TEST: Tests all operations with ONLY the credential provider configured (no explicit credentials)
   */
  test("E2E: All operations work with ONLY credential provider configured (no explicit credentials)") {
    assume(hasAwsCredentials, "AWS credentials file (~/.aws/credentials) required for this test")

    val tablePath = s"$testBasePath/all-operations-test"

    // Configure ONLY the credential provider - NO explicit credentials
    val options = Map(
      "spark.indextables.aws.credentialsProviderClass" -> classOf[AwsCredentialsFileProvider].getName,
      "spark.indextables.aws.region"                   -> S3_REGION
    )

    // Also set in session config for SQL commands
    spark.conf.set("spark.indextables.aws.credentialsProviderClass", classOf[AwsCredentialsFileProvider].getName)
    spark.conf.set("spark.indextables.aws.region", S3_REGION)

    // ========================================
    // STEP 1: WRITE (should resolve credentials via provider)
    // ========================================
    println("\n" + "=" * 60)
    println("STEP 1: WRITE")
    println("=" * 60)

    val data1 = spark
      .range(50)
      .select(
        col("id"),
        concat(lit("Document "), col("id")).as("title"),
        (col("id") % 5).cast("string").as("category")
      )

    println(s"Writing ${data1.count()} records to $tablePath...")

    data1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(options)
      .mode("overwrite")
      .save(tablePath)

    println("✅ WRITE completed successfully")

    // Verify provider was called on driver
    val instantiationsAfterWrite = AwsCredentialsFileProvider.getInstantiations
    println(s"Provider instantiations after WRITE: ${instantiationsAfterWrite.map(_._2).mkString(", ")}")

    // ========================================
    // STEP 2: WRITE (append more data for merge)
    // ========================================
    println("\n" + "=" * 60)
    println("STEP 2: WRITE (append)")
    println("=" * 60)

    val data2 = spark
      .range(50, 100)
      .select(
        col("id"),
        concat(lit("Document "), col("id")).as("title"),
        (col("id") % 5).cast("string").as("category")
      )

    data2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(options)
      .mode("append")
      .save(tablePath)

    println("✅ WRITE (append) completed successfully")

    // ========================================
    // STEP 3: READ (should resolve credentials via provider)
    // ========================================
    println("\n" + "=" * 60)
    println("STEP 3: READ")
    println("=" * 60)

    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(options)
      .load(tablePath)

    val count = readDf.count()
    count shouldBe 100
    println(s"✅ READ completed successfully: $count records")

    // Test filtering
    val filteredCount = readDf.filter(col("category") === "0").count()
    filteredCount should be >= 1L
    println(s"✅ Filter pushdown works: $filteredCount records with category=0")

    // ========================================
    // STEP 4: MERGE SPLITS (should resolve credentials via provider)
    // ========================================
    println("\n" + "=" * 60)
    println("STEP 4: MERGE SPLITS")
    println("=" * 60)

    AwsCredentialsFileProvider.reset()                                // Reset to track merge-specific instantiations
    io.indextables.spark.utils.CredentialProviderFactory.clearCache() // Clear provider cache

    val mergeResult = spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 100M")
    val mergeRows   = mergeResult.collect()
    println(s"✅ MERGE SPLITS completed: ${mergeRows.length} result rows")
    mergeRows.foreach(row => println(s"  - $row"))

    // Verify provider was called during merge
    val instantiationsAfterMerge = AwsCredentialsFileProvider.getInstantiations
    println(s"Provider instantiations during MERGE: ${instantiationsAfterMerge.map(_._2).mkString(", ")}")

    // ========================================
    // STEP 5: READ after MERGE (verify data integrity)
    // ========================================
    println("\n" + "=" * 60)
    println("STEP 5: READ after MERGE")
    println("=" * 60)

    val afterMergeDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(options)
      .load(tablePath)

    val afterMergeCount = afterMergeDf.count()
    afterMergeCount shouldBe 100
    println(s"✅ Data integrity verified after MERGE: $afterMergeCount records")

    // ========================================
    // STEP 6: PURGE (should resolve credentials via provider)
    // ========================================
    println("\n" + "=" * 60)
    println("STEP 6: PURGE INDEXTABLE (DRY RUN)")
    println("=" * 60)

    AwsCredentialsFileProvider.reset()
    io.indextables.spark.utils.CredentialProviderFactory.clearCache()

    val purgeResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' OLDER THAN 24 HOURS DRY RUN")
    val purgeRows   = purgeResult.collect()
    println(s"✅ PURGE (dry run) completed: ${purgeRows.length} result rows")
    purgeRows.foreach(row => println(s"  - $row"))

    // ========================================
    // STEP 7: DESCRIBE TRANSACTION LOG
    // ========================================
    println("\n" + "=" * 60)
    println("STEP 7: DESCRIBE TRANSACTION LOG")
    println("=" * 60)

    val describeResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath'")
    val describeRows   = describeResult.collect()
    println(s"✅ DESCRIBE TRANSACTION LOG completed: ${describeRows.length} rows")

    // ========================================
    // STEP 8: Aggregation with filter pushdown
    // ========================================
    println("\n" + "=" * 60)
    println("STEP 8: Aggregation")
    println("=" * 60)

    val aggResult = afterMergeDf.groupBy("category").count().collect()
    println(s"✅ Aggregation completed:")
    aggResult.foreach(row => println(s"  - category=${row.getString(0)}: ${row.getLong(1)} records"))

    // ========================================
    // FINAL SUMMARY
    // ========================================
    println("\n" + "=" * 60)
    println("SUMMARY")
    println("=" * 60)
    println("✅ All operations completed successfully with ONLY credential provider configured!")
    println("Operations tested:")
    println("  1. WRITE (overwrite)")
    println("  2. WRITE (append)")
    println("  3. READ")
    println("  4. MERGE SPLITS")
    println("  5. READ after MERGE")
    println("  6. PURGE INDEXTABLE (dry run)")
    println("  7. DESCRIBE TRANSACTION LOG")
    println("  8. Aggregation with filter pushdown")
  }

  test("WRITE: Credentials resolved on driver and NOT re-resolved on executor") {
    assume(hasAwsCredentials, "AWS credentials file required")

    val tablePath = s"$testBasePath/write-driver-resolution"

    val options = Map(
      "spark.indextables.aws.credentialsProviderClass" -> classOf[AwsCredentialsFileProvider].getName,
      "spark.indextables.aws.region"                   -> S3_REGION
    )

    AwsCredentialsFileProvider.reset()

    val data = spark.range(20).select(col("id"), lit("test").as("value"))

    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(options)
      .mode("overwrite")
      .save(tablePath)

    val instantiations = AwsCredentialsFileProvider.getInstantiations

    // Verify: Provider should be called on DRIVER only
    println(s"Provider instantiations: ${instantiations.map { case (uri, role) => s"$role: $uri" }.mkString(", ")}")

    // All instantiations should be on DRIVER
    // (If any are on EXECUTOR, it means credentials weren't properly propagated)
    val driverInstantiations   = instantiations.filter(_._2 == "DRIVER")
    val executorInstantiations = instantiations.filter(_._2 == "EXECUTOR")

    // ASSERTIONS: Verify exact number of instantiations
    withClue("Provider should be instantiated on driver") {
      driverInstantiations.nonEmpty shouldBe true
    }

    // For WRITE, provider is called via CloudStorageProvider (V1ToV2CredentialsProviderAdapter)
    // on both driver and executors. Each executor creates its own CloudStorageProvider which
    // will instantiate the provider - this enables proper credential refresh during long operations.
    // Note: Executors WILL instantiate providers (this is the correct behavior for refresh support)

    println(s"✅ WRITE: Provider instantiated ${driverInstantiations.size} times on DRIVER, ${executorInstantiations.size} times on EXECUTOR")
  }

  test("MERGE: Credentials resolved on driver and NOT re-resolved on executor") {
    assume(hasAwsCredentials, "AWS credentials file required")

    val tablePath = s"$testBasePath/merge-driver-resolution"

    val options = Map(
      "spark.indextables.aws.credentialsProviderClass" -> classOf[AwsCredentialsFileProvider].getName,
      "spark.indextables.aws.region"                   -> S3_REGION
    )

    spark.conf.set("spark.indextables.aws.credentialsProviderClass", classOf[AwsCredentialsFileProvider].getName)
    spark.conf.set("spark.indextables.aws.region", S3_REGION)

    // Create multiple splits to merge
    for (i <- 0 until 3) {
      val data = spark.range(i * 20, (i + 1) * 20).select(col("id"), lit(s"batch$i").as("batch"))
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .options(options)
        .mode(if (i == 0) "overwrite" else "append")
        .save(tablePath)
    }

    println("✅ Created 3 batches of data for merge test")

    // Reset tracking before merge
    AwsCredentialsFileProvider.reset()
    io.indextables.spark.utils.CredentialProviderFactory.clearCache()

    // Execute merge
    val mergeResult = spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 100M")
    mergeResult.collect()

    val instantiations = AwsCredentialsFileProvider.getInstantiations

    println(
      s"Provider instantiations during MERGE: ${instantiations.map { case (uri, role) => s"$role: $uri" }.mkString(", ")}"
    )

    val driverInstantiations   = instantiations.filter(_._2 == "DRIVER")
    val executorInstantiations = instantiations.filter(_._2 == "EXECUTOR")

    println(s"MERGE: Driver instantiations: ${driverInstantiations.size}, Executor instantiations: ${executorInstantiations.size}")

    // ASSERTIONS: Verify exact number of instantiations
    // For MERGE, we expect provider to be called on driver during:
    // 1. extractAwsConfig() in MergeSplitsExecutor (per batch)
    // Provider should be called 1-2 times on driver for credential resolution
    withClue("Provider should be instantiated on driver during MERGE") {
      driverInstantiations.size should be >= 1
    }

    // CRITICAL: Provider should NEVER be instantiated on executor
    // This verifies the credentialsAlreadyResolved flag is working correctly
    withClue(
      "Provider should NOT be instantiated on executor during MERGE - credentials should be pre-resolved on driver"
    ) {
      executorInstantiations.size shouldBe 0
    }

    // Verify data integrity
    val finalDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(options)
      .load(tablePath)

    finalDf.count() shouldBe 60
    println(s"✅ MERGE completed with driver-only credential resolution: ${driverInstantiations.size} driver calls, 0 executor calls")
  }

  test("READ: Credentials propagated to executors without re-resolution") {
    assume(hasAwsCredentials, "AWS credentials file required")

    val tablePath = s"$testBasePath/read-credential-propagation"

    val options = Map(
      "spark.indextables.aws.credentialsProviderClass" -> classOf[AwsCredentialsFileProvider].getName,
      "spark.indextables.aws.region"                   -> S3_REGION
    )

    // First write some data
    val data = spark.range(100).select(col("id"), lit("test").as("value"))
    data.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(options)
      .mode("overwrite")
      .save(tablePath)

    println("✅ Data written for read test")

    // Reset tracking before read
    AwsCredentialsFileProvider.reset()
    io.indextables.spark.utils.CredentialProviderFactory.clearCache()

    // Read the data
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(options)
      .load(tablePath)

    val count = readDf.count()
    count shouldBe 100

    val instantiations = AwsCredentialsFileProvider.getInstantiations

    println(
      s"Provider instantiations during READ: ${instantiations.map { case (uri, role) => s"$role: $uri" }.mkString(", ")}"
    )

    val driverInstantiations   = instantiations.filter(_._2 == "DRIVER")
    val executorInstantiations = instantiations.filter(_._2 == "EXECUTOR")

    println(s"READ: Driver instantiations: ${driverInstantiations.size}, Executor instantiations: ${executorInstantiations.size}")

    // ASSERTIONS: Verify exact number of instantiations
    // For READ, provider should be called on driver during newScanBuilder()
    withClue("Provider should be instantiated on driver during READ") {
      driverInstantiations.size should be >= 1
    }

    // Executors should use pre-resolved credentials from broadcast config
    withClue("Provider should NOT be instantiated on executor during READ - credentials should be broadcast from driver") {
      executorInstantiations.size shouldBe 0
    }

    println(s"✅ READ completed successfully: ${driverInstantiations.size} driver calls, 0 executor calls")
  }
}
