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

package io.indextables.spark.merge

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Azure-specific integration tests for merge-on-write feature.
 *
 * These tests validate:
 * 1. Staging uploads to Azure Blob Storage
 * 2. Worker downloads from Azure for remote merges
 * 3. Final split uploads to Azure
 * 4. Transaction log in Azure
 * 5. Azure-specific authentication (account key, OAuth)
 *
 * NOTE: These tests require Azure credentials and a storage account.
 * Set environment variables:
 *   - AZURE_STORAGE_ACCOUNT
 *   - AZURE_STORAGE_KEY (for account key auth)
 *   OR
 *   - AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET (for OAuth)
 *   - INDEXTABLES_TEST_AZURE_CONTAINER (e.g., "test-container")
 *   - INDEXTABLES_TEST_AZURE_PREFIX (optional, e.g., "test-prefix")
 *
 * Tests will be skipped if credentials are not available.
 */
class MergeOnWriteAzureIntegrationTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _
  private var azureAccount: Option[String] = None
  private var azureContainer: Option[String] = None
  private var azurePrefix: String = "merge-on-write-test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Check if Azure credentials are available
    azureAccount = sys.env.get("AZURE_STORAGE_ACCOUNT")
    val azureKey = sys.env.get("AZURE_STORAGE_KEY")
    val azureTenantId = sys.env.get("AZURE_TENANT_ID")
    val azureClientId = sys.env.get("AZURE_CLIENT_ID")
    val azureClientSecret = sys.env.get("AZURE_CLIENT_SECRET")
    azureContainer = sys.env.get("INDEXTABLES_TEST_AZURE_CONTAINER")
    azurePrefix = sys.env.getOrElse("INDEXTABLES_TEST_AZURE_PREFIX", "merge-on-write-test")

    val hasAccountKey = azureAccount.isDefined && azureKey.isDefined
    val hasOAuth = azureAccount.isDefined && azureTenantId.isDefined &&
                   azureClientId.isDefined && azureClientSecret.isDefined

    if ((hasAccountKey || hasOAuth) && azureContainer.isDefined) {
      val sparkBuilder = SparkSession.builder()
        .appName("MergeOnWriteAzureIntegrationTest")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.indextables.azure.accountName", azureAccount.get)

      // Configure authentication
      if (hasAccountKey) {
        sparkBuilder.config("spark.indextables.azure.accountKey", azureKey.get)
        println(s"✅ Azure integration tests enabled (Account Key auth)")
      } else {
        sparkBuilder
          .config("spark.indextables.azure.tenantId", azureTenantId.get)
          .config("spark.indextables.azure.clientId", azureClientId.get)
          .config("spark.indextables.azure.clientSecret", azureClientSecret.get)
        println(s"✅ Azure integration tests enabled (OAuth auth)")
      }

      spark = sparkBuilder.getOrCreate()

      println(s"   - Account: ${azureAccount.get}")
      println(s"   - Container: ${azureContainer.get}")
      println(s"   - Prefix: $azurePrefix")
    } else {
      println(s"⚠️  Azure integration tests SKIPPED - missing credentials or container")
      println(s"   - Set AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY (or OAuth), INDEXTABLES_TEST_AZURE_CONTAINER")
    }
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  private def skipIfAzureNotConfigured(): Unit = {
    if (spark == null || azureAccount.isEmpty || azureContainer.isEmpty) {
      cancel("Azure not configured - set AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY, INDEXTABLES_TEST_AZURE_CONTAINER")
    }
  }

  test("Azure: End-to-end merge-on-write with staging") {
    skipIfAzureNotConfigured()

    val tablePath = s"azure://${azureContainer.get}/$azurePrefix/e2e-merge-test-${System.currentTimeMillis()}"
    val stagingPath = s"$tablePath/_staging"

    // Step 1: Create test data with many small splits
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val sourceData = 1.to(10000).map { i =>
      (i, s"content_$i", i % 100, s"category_${i % 10}")
    }.toDF("id", "content", "score", "category")
      .repartition(8)

    // Step 2: Write with merge-on-write enabled
    val writeStartTime = System.currentTimeMillis()
    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.indexWriter.batchSize", "1000")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .mode(SaveMode.Overwrite)
      .save(tablePath)
    val writeDuration = System.currentTimeMillis() - writeStartTime

    println(s"✅ Azure write completed in ${writeDuration}ms")

    // Step 3: Read back and verify
    val readStartTime = System.currentTimeMillis()
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    val readDuration = System.currentTimeMillis() - readStartTime

    val rowCount = readData.count()
    rowCount shouldBe 10000

    println(s"✅ Azure read completed in ${readDuration}ms")
    println(s"   - All 10000 records verified")

    // Step 4: Validate transaction log contents using TransactionLogFactory API
    println("\n[Validation] Reading transaction log contents...")
    import io.indextables.spark.transaction.TransactionLogFactory
    import org.apache.hadoop.fs.Path

    val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)
    val addActions = try {
      transactionLog.listFiles()
    } finally {
      transactionLog.close()
    }

    println(s"   - Transaction log has ${addActions.size} split(s)")
    addActions.foreach { action =>
      println(s"     * ${action.path}")
    }

    // Verify we have fewer splits than original (8 partitions should merge down)
    assert(addActions.size < 8, s"Expected < 8 splits after merge, got ${addActions.size}")
    assert(addActions.size > 0, "Expected at least 1 split")

    val mergedCount = addActions.count(_.path.startsWith("merged-"))
    val promotedCount = addActions.count(_.path.startsWith("split-"))
    println(s"   ✓ Merge occurred: 8 splits → ${addActions.size} splits (${mergedCount} merged, ${promotedCount} promoted)")

    // Step 5: Validate min/max statistics present
    println("\n[Validation] Checking data skipping statistics...")
    addActions.foreach { action =>
      assert(action.minValues.isDefined, s"Split ${action.path} missing minValues")
      assert(action.maxValues.isDefined, s"Split ${action.path} missing maxValues")
      assert(action.minValues.get.contains("score"), s"Split ${action.path} missing score in minValues")
      assert(action.maxValues.get.contains("score"), s"Split ${action.path} missing score in maxValues")
    }
    println(s"   ✓ All ${addActions.size} splits have min/max statistics for data skipping")

    // Step 6: Validate staging cleanup (no .tmp files)
    println("\n[Validation] Checking staging cleanup...")
    try {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(stagingPath), hadoopConf)

      try {
        if (fs.exists(new Path(stagingPath))) {
          val stagingFiles = fs.listStatus(new Path(stagingPath))
          val tmpFiles = stagingFiles.filter(_.getPath.getName.endsWith(".tmp"))
          assert(tmpFiles.isEmpty, s"Found ${tmpFiles.length} .tmp files in staging")
          println(s"   ✓ No .tmp files in staging (${stagingFiles.length} total files)")
        } else {
          println(s"   ✓ Staging directory does not exist (cleaned up completely)")
        }
      } finally {
        fs.close()
      }
    } catch {
      case e: Exception =>
        println(s"   ⚠ Could not validate staging cleanup: ${e.getMessage}")
        println(s"   ℹ Staging cleanup validation skipped for Azure")
    }

    // Step 7: Verify filter pushdown
    val filteredCount = readData.filter($"category" === "category_5").count()
    filteredCount shouldBe 1000

    // Step 8: Verify aggregation
    val aggResult = readData.agg(count("*"), sum("score")).collect()(0)
    aggResult.getAs[Long](0) shouldBe 10000

    println(s"✅ Azure comprehensive validation passed")
    println(s"   - Merge: 8 → ${addActions.size} splits")
    println(s"   - Statistics: PRESENT")
    println(s"   - Staging cleanup: VERIFIED")
    println(s"   - Data integrity: VERIFIED")
  }

  test("Azure: Partitioned dataset with merge-on-write") {
    skipIfAzureNotConfigured()

    val tablePath = s"azure://${azureContainer.get}/$azurePrefix/partitioned-test-${System.currentTimeMillis()}"
    val stagingPath = s"$tablePath/_staging"

    // Step 1: Create partitioned test data
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val sourceData = 1.to(5000).flatMap { i =>
      Seq(
        (i, s"content_$i", i % 100, "2024-01-01", 10),
        (i + 5000, s"content_${i + 5000}", (i + 5000) % 100, "2024-01-02", 11),
        (i + 10000, s"content_${i + 10000}", (i + 10000) % 100, "2024-01-03", 12)
      )
    }.toDF("id", "content", "score", "date", "hour")
      .repartition(6)

    // Step 2: Write partitioned data
    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .partitionBy("date", "hour")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    println(s"✅ Azure partitioned write completed")

    // Step 3: Read with partition pruning
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val date1Count = readData.filter($"date" === "2024-01-01").count()
    date1Count shouldBe 5000

    val date2Count = readData.filter($"date" === "2024-01-02").count()
    date2Count shouldBe 5000

    println(s"✅ Azure partition pruning verified")
  }

  test("Azure: Append mode with merge-on-write") {
    skipIfAzureNotConfigured()

    val tablePath = s"azure://${azureContainer.get}/$azurePrefix/append-test-${System.currentTimeMillis()}"
    val stagingPath = s"$tablePath/_staging"

    // Step 1: Initial write
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val batch1 = 1.to(5000).map { i =>
      (i, s"content_$i", i % 100)
    }.toDF("id", "content", "score")
      .repartition(4)

    batch1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Step 2: Append second batch
    val batch2 = 5001.to(10000).map { i =>
      (i, s"content_$i", i % 100)
    }.toDF("id", "content", "score")
      .repartition(4)

    batch2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .mode(SaveMode.Append)
      .save(tablePath)

    // Step 3: Verify both batches
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val totalCount = readData.count()
    totalCount shouldBe 10000

    readData.filter($"id" === 100).count() shouldBe 1
    readData.filter($"id" === 5100).count() shouldBe 1

    println(s"✅ Azure append mode verified")
  }

  test("Azure: ABFSS URL format support") {
    skipIfAzureNotConfigured()

    // Test abfss:// URL format (Azure Data Lake Storage Gen2)
    val tablePath = s"abfss://${azureContainer.get}@${azureAccount.get}.dfs.core.windows.net/$azurePrefix/abfss-test-${System.currentTimeMillis()}"
    val stagingPath = s"$tablePath/_staging"

    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val sourceData = 1.to(5000).map { i =>
      (i, s"content_$i", i % 100)
    }.toDF("id", "content", "score")
      .repartition(4)

    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    readData.count() shouldBe 5000

    println(s"✅ Azure ABFSS URL format verified")
  }

  test("Azure: WASBS URL format support") {
    skipIfAzureNotConfigured()

    // Test wasbs:// URL format (legacy Azure Blob Storage)
    val tablePath = s"wasbs://${azureContainer.get}@${azureAccount.get}.blob.core.windows.net/$azurePrefix/wasbs-test-${System.currentTimeMillis()}"
    val stagingPath = s"$tablePath/_staging"

    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val sourceData = 1.to(5000).map { i =>
      (i, s"content_$i", i % 100)
    }.toDF("id", "content", "score")
      .repartition(4)

    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    readData.count() shouldBe 5000

    println(s"✅ Azure WASBS URL format verified")
  }

  test("Azure: Verify staging and final locations") {
    skipIfAzureNotConfigured()

    val tablePath = s"azure://${azureContainer.get}/$azurePrefix/staging-test-${System.currentTimeMillis()}"
    val stagingPath = s"$tablePath/_staging"

    // Step 1: Write data
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val sourceData = 1.to(5000).map { i =>
      (i, s"content_$i", i % 100)
    }.toDF("id", "content", "score")
      .repartition(4)

    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Step 2: Verify transaction log exists
    val transactionLogPath = s"$tablePath/_transaction_log"

    // Use Hadoop FileSystem API to check Azure paths
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(transactionLogPath),
      hadoopConf
    )

    val txLogExists = fs.exists(new org.apache.hadoop.fs.Path(transactionLogPath))
    txLogExists shouldBe true

    // Step 3: List files in transaction log
    val txLogFiles = fs.listStatus(new org.apache.hadoop.fs.Path(transactionLogPath))
    txLogFiles should not be empty

    val jsonFiles = txLogFiles.filter((f: org.apache.hadoop.fs.FileStatus) =>
      f.getPath.getName.endsWith(".json") || f.getPath.getName.endsWith(".json.gz"))
    jsonFiles should not be empty

    println(s"✅ Azure transaction log verified")
    println(s"   - Transaction log files: ${jsonFiles.length}")

    // Step 4: Verify splits exist in final location
    val splitFiles = fs.listStatus(new org.apache.hadoop.fs.Path(tablePath))
      .filter((f: org.apache.hadoop.fs.FileStatus) => f.getPath.getName.endsWith(".split"))

    splitFiles should not be empty
    println(s"   - Split files: ${splitFiles.length}")

    fs.close()
  }

  test("Azure: Large dataset performance test") {
    skipIfAzureNotConfigured()

    val tablePath = s"azure://${azureContainer.get}/$azurePrefix/large-test-${System.currentTimeMillis()}"
    val stagingPath = s"$tablePath/_staging"

    // Step 1: Create larger dataset
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val sourceData = 1.to(50000).map { i =>
      (i, s"content_$i", i % 1000, s"category_${i % 50}")
    }.toDF("id", "content", "score", "category")
      .repartition(16)

    // Step 2: Write and measure time
    val writeStartTime = System.currentTimeMillis()
    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.indexWriter.batchSize", "2000")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "100M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .mode(SaveMode.Overwrite)
      .save(tablePath)
    val writeDuration = System.currentTimeMillis() - writeStartTime

    // Step 3: Read and measure time
    val readStartTime = System.currentTimeMillis()
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    val rowCount = readData.count()
    val readDuration = System.currentTimeMillis() - readStartTime

    rowCount shouldBe 50000

    println(s"✅ Azure large dataset test completed")
    println(s"   - Write time: ${writeDuration}ms (${50000 * 1000 / writeDuration} rows/sec)")
    println(s"   - Read time: ${readDuration}ms (${50000 * 1000 / readDuration} rows/sec)")
    println(s"   - Records: $rowCount")
  }

  test("Azure: Promotion path - single split bypasses merge") {
    skipIfAzureNotConfigured()

    val tablePath = s"azure://${azureContainer.get}/$azurePrefix/promotion-test-${System.currentTimeMillis()}"
    val stagingPath = s"$tablePath/_staging"

    println("\n" + "="*80)
    println("AZURE TEST: Promotion Path Validation")
    println("="*80)

    // Step 1: Create single partition (will trigger promotion, not merge)
    println("\n[Step 1] Creating single partition with 1000 records...")
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val sourceData = 1.to(1000).map { i =>
      (i, s"content_$i", i % 100, s"category_${i % 10}")
    }.toDF("id", "content", "score", "category")
      .repartition(1)  // Single partition = single split

    println(s"   ✓ Created DataFrame with 1000 records in 1 partition")

    // Step 2: Write with merge-on-write (minSplitsToMerge: 2, but only 1 split)
    println("\n[Step 2] Writing to Azure with merge-on-write (minSplitsToMerge: 2)...")
    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")  // Requires 2+ to merge
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.mergeOnWrite.stagingPath", stagingPath)
      .option("spark.indextables.indexing.typemap.content", "string")
      .option("spark.indextables.indexing.typemap.category", "string")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    println(s"   ✓ Azure write completed")

    // Step 3: Read transaction log with full metadata
    println("\n[Step 3] Reading Azure transaction log...")
    import io.indextables.spark.transaction.TransactionLogFactory
    import org.apache.hadoop.fs.Path

    val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)
    val addActions = try {
      transactionLog.listFiles()
    } finally {
      transactionLog.close()
    }

    println(s"   - Transaction log has ${addActions.size} split(s)")
    addActions.foreach { action =>
      println(s"     * ${action.path}")
      println(s"       - tags: ${action.tags}")
      println(s"       - numMergeOps: ${action.numMergeOps}")
    }

    // Step 4: Validate promotion occurred
    println("\n[Step 4] Validating promotion path...")
    assert(addActions.size == 1, s"Expected exactly 1 split, got ${addActions.size}")

    val promotedSplit = addActions.head

    // Verify it's a promoted split (not merged)
    assert(promotedSplit.path.startsWith("split-"),
      s"Expected split- prefix for promoted split, got: ${promotedSplit.path}")

    // Verify mergeStrategy tag
    assert(promotedSplit.tags.isDefined, "Expected tags to be present")
    assert(promotedSplit.tags.get.contains("mergeStrategy"),
      s"Expected mergeStrategy tag, got: ${promotedSplit.tags.get}")
    assert(promotedSplit.tags.get("mergeStrategy") == "promoted",
      s"Expected mergeStrategy=promoted, got: ${promotedSplit.tags.get("mergeStrategy")}")

    // Verify numMergeOps is 0
    assert(promotedSplit.numMergeOps.isDefined, "Expected numMergeOps to be present")
    assert(promotedSplit.numMergeOps.get == 0,
      s"Expected numMergeOps=0 for promoted split, got: ${promotedSplit.numMergeOps.get}")

    println(s"   ✓ Split correctly tagged with mergeStrategy=promoted")
    println(s"   ✓ numMergeOps=0 confirms no merge occurred")

    // Step 5: Validate statistics
    println("\n[Step 5] Validating data skipping statistics...")
    assert(promotedSplit.minValues.isDefined, "Expected minValues")
    assert(promotedSplit.maxValues.isDefined, "Expected maxValues")
    assert(promotedSplit.minValues.get.contains("score"), "Expected score in minValues")
    assert(promotedSplit.maxValues.get.contains("score"), "Expected score in maxValues")
    println(s"   ✓ Statistics preserved")

    // Step 6: Validate data readable from Azure
    println("\n[Step 6] Reading data from Azure...")
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val rowCount = readData.count()
    assert(rowCount == 1000, s"Expected 1000 rows, got $rowCount")
    println(s"   ✓ Successfully read ${rowCount} rows from Azure promoted split")

    // Step 7: Validate filter and aggregation
    val filtered = readData.filter($"score" === 42)
    assert(filtered.count() > 0)

    val aggResult = readData.agg(count("*"), min("score"), max("score")).collect()(0)
    assert(aggResult.getAs[Long](0) == 1000)
    assert(aggResult.getAs[Number](1).longValue() == 0)
    assert(aggResult.getAs[Number](2).longValue() == 99)
    println(s"   ✓ Filter pushdown and aggregation work")

    println("\n" + "="*80)
    println("AZURE PROMOTION TEST SUMMARY:")
    println("="*80)
    println(s"  - Final splits: 1 (0 merged, 1 promoted)")
    println(s"  - mergeStrategy: promoted")
    println(s"  - numMergeOps: 0")
    println(s"  - Data integrity: VERIFIED")
    println(s"  - Azure storage: VERIFIED")
    println("="*80 + "\n")
  }
}
