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

package io.indextables.spark

import org.apache.spark.sql.functions._

/**
 * Comprehensive end-to-end integration test for real Azure Blob Storage operations.
 *
 * This test validates the complete IndexTables4Spark workflow with Azure:
 *   1. Write DataFrames to Azure Blob Storage 2. Read DataFrames back from Azure 3. Query with filters and IndexQuery
 *      operators 4. Execute MERGE SPLITS operations 5. Validate data integrity throughout
 *
 * URL Format Support:
 *   - azure://container/path (used in tests below)
 *   - abfss://container@account.dfs.core.windows.net/path (normalized to azure:// internally)
 *   - wasb://, wasbs://, abfs:// (all supported and normalized to azure://)
 *
 * Note: All Azure URL formats are automatically normalized to azure:// protocol for tantivy4java compatibility during
 * read operations.
 *
 * Prerequisites:
 *   - Azure credentials configured via system properties, ~/.azure/credentials, or environment variables
 *   - Azure container accessible for testing
 *
 * Set system properties: -Dtest.azure.container=your-test-container -Dtest.azure.storageAccount=yourstorageaccount
 * -Dtest.azure.accountKey=your-account-key (optional if using ~/.azure/credentials)
 */
class RealAzureEndToEndTest extends RealAzureTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Skip all tests if Azure credentials are not available
    assume(
      hasAzureCredentials(),
      "Azure credentials not available - skipping tests"
    )
  }

  test("should write and read DataFrame from Azure Blob Storage") {
    val testId    = generateTestId()
    val azurePath = s"azure://$testContainer/test-dataframe-$testId"
    val testData = Seq(
      ("doc1", "Apache Spark is amazing", 100),
      ("doc2", "Azure Blob Storage integration", 200),
      ("doc3", "IndexTables4Spark with Azure", 300)
    )

    val df = spark.createDataFrame(testData).toDF("id", "content", "score")

    // Write to Azure
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(azurePath)

    println(s"✅ Wrote DataFrame to Azure: $azurePath")

    // Read from Azure
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    val readData = readDf.orderBy("id").collect()

    readData.length shouldBe 3
    readData(0).getString(0) shouldBe "doc1"
    readData(1).getString(0) shouldBe "doc2"
    readData(2).getString(0) shouldBe "doc3"

    println("✅ Successfully read DataFrame from Azure")
  }

  test("should support IndexQuery operations on Azure data") {
    val testId    = generateTestId()
    val azurePath = s"azure://$testContainer/test-indexquery-$testId"
    val testData = Seq(
      ("doc1", "machine learning with python"),
      ("doc2", "deep learning and neural networks"),
      ("doc3", "python programming language"),
      ("doc4", "artificial intelligence research")
    )

    val df = spark.createDataFrame(testData).toDF("id", "content")

    // Write to Azure with text field type for full-text search
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(azurePath)

    println(s"✅ Wrote DataFrame with text fields to Azure: $azurePath")

    // Read and query with IndexQuery
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    // Query for "python"
    val pythonDocs = readDf.filter("content indexquery 'python'").collect()

    pythonDocs.length shouldBe 2
    pythonDocs.exists(_.getString(0) == "doc1") shouldBe true
    pythonDocs.exists(_.getString(0) == "doc3") shouldBe true

    println("✅ Successfully executed IndexQuery operations on Azure data")
  }

  test("should support partitioned datasets on Azure") {
    val testId    = generateTestId()
    val azurePath = s"azure://$testContainer/test-partitioned-$testId"
    val testData = Seq(
      ("2024-01-01", 10, "Morning logs"),
      ("2024-01-01", 11, "Midday logs"),
      ("2024-01-02", 10, "Morning logs day 2"),
      ("2024-01-02", 11, "Midday logs day 2")
    )

    val df = spark.createDataFrame(testData).toDF("date", "hour", "message")

    // Write partitioned data to Azure
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("date", "hour")
      .mode("overwrite")
      .save(azurePath)

    println(s"✅ Wrote partitioned DataFrame to Azure: $azurePath")

    // Read with partition pruning
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    // Query specific partition
    val filtered = readDf.filter(col("date") === "2024-01-01" && col("hour") === 10).collect()

    filtered.length shouldBe 1
    filtered(0).getString(2) shouldBe "Morning logs"

    println("✅ Successfully queried partitioned dataset on Azure")
  }

  test("should execute MERGE SPLITS on Azure data") {
    val testId    = generateTestId()
    val azurePath = s"azure://$testContainer/test-merge-$testId"

    // Create multiple small splits
    val testData1 = Seq(("doc1", "content1", 100), ("doc2", "content2", 200))
    val testData2 = Seq(("doc3", "content3", 300), ("doc4", "content4", 400))

    val df1 = spark.createDataFrame(testData1).toDF("id", "content", "score")
    val df2 = spark.createDataFrame(testData2).toDF("id", "content", "score")

    // Write first batch
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(azurePath)

    // Append second batch
    df2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("append")
      .save(azurePath)

    println(s"✅ Wrote multiple batches to Azure: $azurePath")

    // Execute merge splits
    spark.sql(s"MERGE SPLITS '$azurePath' TARGET SIZE 1M")

    println("✅ Successfully executed MERGE SPLITS on Azure data")

    // Verify data integrity after merge
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    val allDocs = readDf.orderBy("id").collect()

    allDocs.length shouldBe 4
    allDocs(0).getString(0) shouldBe "doc1"
    allDocs(1).getString(0) shouldBe "doc2"
    allDocs(2).getString(0) shouldBe "doc3"
    allDocs(3).getString(0) shouldBe "doc4"

    println("✅ Data integrity validated after MERGE SPLITS")
  }

  test("should support aggregate operations on Azure data") {
    val testId    = generateTestId()
    val azurePath = s"azure://$testContainer/test-aggregates-$testId"
    val testData = Seq(
      ("product1", 100),
      ("product2", 200),
      ("product3", 300),
      ("product4", 400)
    )

    val df = spark.createDataFrame(testData).toDF("product", "score")

    // Write to Azure with fast field configuration
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(azurePath)

    println(s"✅ Wrote DataFrame with fast fields to Azure: $azurePath")

    // Read and perform aggregations
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    // Test aggregate pushdown
    val count = readDf.count()
    count shouldBe 4

    val sumScore = readDf.agg(sum("score")).collect()(0).getLong(0)
    sumScore shouldBe 1000L

    println("✅ Successfully executed aggregate operations on Azure data")
  }

  test("should handle large writes with multiple partitions") {
    val testId    = generateTestId()
    val azurePath = s"azure://$testContainer/test-large-$testId"

    // Create larger dataset
    val testData = (1 to 1000).map(i => (s"doc$i", s"content for document $i", i * 10))

    val df = spark.createDataFrame(testData).toDF("id", "content", "score")

    // Write with multiple partitions
    df.repartition(4)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(azurePath)

    println(s"✅ Wrote large DataFrame to Azure: $azurePath")

    // Read and validate count
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    val count = readDf.count()
    count shouldBe 1000

    // Validate filtering works
    val filtered = readDf.filter(col("score") > 5000).count()
    filtered should be > 0L

    println("✅ Successfully handled large write with multiple partitions")
  }

  test("should support overwrite mode on Azure") {
    val testId    = generateTestId()
    val azurePath = s"azure://$testContainer/test-overwrite-$testId"

    // Initial write
    val testData1 = Seq(("doc1", "original content"))
    val df1       = spark.createDataFrame(testData1).toDF("id", "content")

    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(azurePath)

    println(s"✅ Initial write to Azure: $azurePath")

    // Overwrite
    val testData2 = Seq(("doc2", "new content"), ("doc3", "more new content"))
    val df2       = spark.createDataFrame(testData2).toDF("id", "content")

    df2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(azurePath)

    println("✅ Overwrite completed")

    // Validate only new data exists
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    val allDocs = readDf.collect()

    allDocs.length shouldBe 2
    allDocs.exists(_.getString(0) == "doc1") shouldBe false
    allDocs.exists(_.getString(0) == "doc2") shouldBe true
    allDocs.exists(_.getString(0) == "doc3") shouldBe true

    println("✅ Successfully validated overwrite mode on Azure")
  }

  test("should support cross-field search with _indexall") {
    val testId    = generateTestId()
    val azurePath = s"azure://$testContainer/test-indexall-$testId"
    val testData = Seq(
      ("doc1", "Apache Spark", "Distributed computing"),
      ("doc2", "Azure Cloud", "Microsoft services"),
      ("doc3", "Python Programming", "Data science tools")
    )

    val df = spark.createDataFrame(testData).toDF("id", "title", "description")

    // Write to Azure with text fields
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.typemap.description", "text")
      .mode("overwrite")
      .save(azurePath)

    println(s"✅ Wrote DataFrame with multiple text fields to Azure: $azurePath")

    // Read and query with _indexall
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    val sparkDocs = readDf.filter("_indexall indexquery 'Spark'").collect()

    sparkDocs.length shouldBe 1
    sparkDocs(0).getString(0) shouldBe "doc1"

    println("✅ Successfully executed cross-field search with _indexall")
  }

  test("should validate full lifecycle with abfss:// URL scheme") {
    val testId = generateTestId()

    // Use Spark's modern ADLS Gen2 URL format: abfss://
    val storageAcct = getStorageAccount.getOrElse("devstoreaccount1")
    val abfssPath   = s"abfss://$testContainer@$storageAcct.dfs.core.windows.net/test-abfss-$testId"

    val testData = Seq(
      ("doc1", "Gen2 with abfss protocol", 100),
      ("doc2", "Azure Data Lake Storage ADLS", 200),
      ("doc3", "Hierarchical namespace", 300)
    )

    val df = spark.createDataFrame(testData).toDF("id", "content", "score")

    // Write using abfss:// scheme (should normalize to azure:// internally)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(abfssPath)

    println(s"✅ Wrote DataFrame using abfss:// scheme: $abfssPath")

    // Read using abfss:// scheme (normalization should work)
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(abfssPath)

    val readData = readDf.orderBy("id").collect()

    // Validate basic read
    readData.length shouldBe 3
    readData(0).getString(0) shouldBe "doc1"
    readData(1).getString(0) shouldBe "doc2"
    readData(2).getString(0) shouldBe "doc3"

    println("✅ Successfully read DataFrame using abfss:// scheme")

    // Validate IndexQuery operations work with abfss://
    val filteredDf = readDf.filter("content indexquery 'ADLS'").collect()
    filteredDf.length shouldBe 1
    filteredDf(0).getString(0) shouldBe "doc2"

    println("✅ IndexQuery operations work with abfss:// scheme")

    // Validate count works with abfss://
    val rowCount = readDf.count()
    rowCount shouldBe 3

    println("✅ Count operations work with abfss:// scheme")

    // Test append mode with abfss://
    val appendData = Seq(("doc4", "Additional ADLS data", 400))
    val appendDf   = spark.createDataFrame(appendData).toDF("id", "content", "score")

    appendDf.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("append")
      .save(abfssPath)

    println("✅ Append mode works with abfss:// scheme")

    // Validate appended data
    val afterAppend = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(abfssPath)

    val appendedCount = afterAppend.count()
    appendedCount shouldBe 4

    println("✅ Data integrity validated after append with abfss:// scheme")

    // Test MERGE SPLITS with abfss://
    spark.sql(s"MERGE SPLITS '$abfssPath' TARGET SIZE 1M")

    println("✅ MERGE SPLITS works with abfss:// scheme")

    // Validate data integrity after merge
    val afterMerge = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(abfssPath)

    val mergeCount = afterMerge.count()
    mergeCount shouldBe 4

    println("✅ Data integrity validated after MERGE SPLITS with abfss:// scheme")
    println("✅ FULL LIFECYCLE VALIDATED: abfss:// URL normalization working end-to-end")
  }

  test("should support mixed Azure URL schemes (azure:// and abfss://) for same table") {
    val testId = generateTestId()

    // Write using abfss:// scheme
    val storageAcct = getStorageAccount.getOrElse("devstoreaccount1")
    val abfssPath   = s"abfss://$testContainer@$storageAcct.dfs.core.windows.net/test-mixed-$testId"

    val testData = Seq(
      ("doc1", "Written with abfss scheme", 100),
      ("doc2", "Azure URL normalization", 200)
    )

    val df = spark.createDataFrame(testData).toDF("id", "content", "score")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(abfssPath)

    println(s"✅ Wrote data using abfss:// scheme: $abfssPath")

    // Read using azure:// scheme (should read the same data)
    val azurePath = s"azure://$testContainer/test-mixed-$testId"

    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(azurePath)

    val readData = readDf.orderBy("id").collect()

    // Should read the same data regardless of URL scheme
    readData.length shouldBe 2
    readData(0).getString(0) shouldBe "doc1"
    readData(1).getString(0) shouldBe "doc2"

    println("✅ Successfully read data written with abfss:// using azure:// scheme")

    // Append using azure:// scheme
    val appendData = Seq(("doc3", "Appended with azure scheme", 300))
    val appendDf   = spark.createDataFrame(appendData).toDF("id", "content", "score")

    appendDf.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("append")
      .save(azurePath)

    println("✅ Appended data using azure:// scheme")

    // Read back using abfss:// scheme
    val finalDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(abfssPath)

    val finalCount = finalDf.count()
    finalCount shouldBe 3

    println("✅ Successfully read data using abfss:// after append with azure:// scheme")
    println("✅ MIXED URL SCHEME VALIDATION PASSED: azure:// and abfss:// are interchangeable")
  }

  test("should validate abfss:// URL with partitioned datasets") {
    val testId      = generateTestId()
    val storageAcct = getStorageAccount.getOrElse("devstoreaccount1")
    val abfssPath   = s"abfss://$testContainer@$storageAcct.dfs.core.windows.net/test-partitioned-abfss-$testId"

    val testData = Seq(
      ("2024-01-01", 10, "ADLS partitioned data 1"),
      ("2024-01-01", 11, "ADLS partitioned data 2"),
      ("2024-01-02", 10, "ADLS partitioned data 3"),
      ("2024-01-02", 11, "ADLS partitioned data 4")
    )

    val df = spark.createDataFrame(testData).toDF("date", "hour", "message")

    // Write partitioned data using abfss://
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("date", "hour")
      .mode("overwrite")
      .save(abfssPath)

    println(s"✅ Wrote partitioned data using abfss:// scheme: $abfssPath")

    // Read with partition pruning using abfss://
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(abfssPath)

    // Query specific partition
    val filtered = readDf.filter(col("date") === "2024-01-01" && col("hour") === 10).collect()

    filtered.length shouldBe 1
    filtered(0).getString(2) shouldBe "ADLS partitioned data 1"

    println("✅ Partition pruning works with abfss:// scheme")

    // Test MERGE SPLITS on partitioned data with abfss://
    spark.sql(s"""
      MERGE SPLITS '$abfssPath'
      WHERE date = '2024-01-01' AND hour = 10
      TARGET SIZE 1M
    """)

    println("✅ Partition-aware MERGE SPLITS works with abfss:// scheme")

    // Validate data integrity
    val afterMerge = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(abfssPath)

    val totalCount = afterMerge.count()
    totalCount shouldBe 4

    println("✅ Partitioned dataset full lifecycle validated with abfss:// scheme")
  }

  test("should authenticate using OAuth bearer token with explicit credentials") {
    // Fail if OAuth credentials not available
    assert(
      hasOAuthCredentials(),
      "❌ OAuth credentials not available - test FAILED. Configure ~/.azure/credentials with Service Principal credentials (tenant_id, client_id, client_secret)"
    )

    val tenantId     = getTenantId.get
    val clientId     = getClientId.get
    val clientSecret = getClientSecret.get

    val testId      = generateTestId()
    val storageAcct = getStorageAccount.getOrElse("devstoreaccount1")
    val abfssPath   = s"abfss://$testContainer@$storageAcct.dfs.core.windows.net/test-oauth-explicit-$testId"

    val testData = Seq(
      ("doc1", "OAuth authentication test", 100),
      ("doc2", "Service Principal authentication", 200),
      ("doc3", "Azure Active Directory", 300)
    )

    val df = spark.createDataFrame(testData).toDF("id", "content", "score")

    // Write using OAuth credentials
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.azure.accountName", storageAcct)
      .option("spark.indextables.azure.tenantId", tenantId)
      .option("spark.indextables.azure.clientId", clientId)
      .option("spark.indextables.azure.clientSecret", clientSecret)
      .mode("overwrite")
      .save(abfssPath)

    println(s"✅ Wrote DataFrame using OAuth authentication: $abfssPath")

    // Read using OAuth credentials
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.azure.accountName", storageAcct)
      .option("spark.indextables.azure.tenantId", tenantId)
      .option("spark.indextables.azure.clientId", clientId)
      .option("spark.indextables.azure.clientSecret", clientSecret)
      .load(abfssPath)

    val readData = readDf.orderBy("id").collect()

    readData.length shouldBe 3
    readData(0).getString(0) shouldBe "doc1"
    readData(1).getString(0) shouldBe "doc2"
    readData(2).getString(0) shouldBe "doc3"

    println("✅ Successfully read DataFrame using OAuth authentication")
  }

  test("should authenticate using OAuth bearer token from ~/.azure/credentials file") {
    // Fail if OAuth credentials not available from credentials file
    assert(
      hasOAuthCredentials(),
      "❌ OAuth credentials not available in ~/.azure/credentials - test FAILED. Add tenant_id, client_id, client_secret to [default] section"
    )

    val testId      = generateTestId()
    val storageAcct = getStorageAccount.getOrElse("devstoreaccount1")
    val abfssPath   = s"abfss://$testContainer@$storageAcct.dfs.core.windows.net/test-oauth-file-$testId"

    val testData = Seq(
      ("doc1", "OAuth from credentials file", 100),
      ("doc2", "Service Principal from file", 200)
    )

    val df = spark.createDataFrame(testData).toDF("id", "content", "score")

    // Write using OAuth credentials from file (no explicit credentials needed)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(abfssPath)

    println(s"✅ Wrote DataFrame using OAuth from ~/.azure/credentials: $abfssPath")

    // Read using OAuth credentials from file
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(abfssPath)

    val readData = readDf.orderBy("id").collect()

    readData.length shouldBe 2
    readData(0).getString(0) shouldBe "doc1"
    readData(1).getString(0) shouldBe "doc2"

    println("✅ Successfully read DataFrame using OAuth from ~/.azure/credentials")
  }

  test("should support OAuth with MERGE SPLITS operations") {
    // Fail if OAuth credentials not available
    assert(
      hasOAuthCredentials(),
      "❌ OAuth credentials not available - test FAILED. Configure ~/.azure/credentials with Service Principal credentials (tenant_id, client_id, client_secret)"
    )

    val tenantId     = getTenantId.get
    val clientId     = getClientId.get
    val clientSecret = getClientSecret.get

    val testId      = generateTestId()
    val storageAcct = getStorageAccount.getOrElse("devstoreaccount1")
    val abfssPath   = s"abfss://$testContainer@$storageAcct.dfs.core.windows.net/test-oauth-merge-$testId"

    val testData = Seq(
      ("doc1", "OAuth merge test 1", 100),
      ("doc2", "OAuth merge test 2", 200),
      ("doc3", "OAuth merge test 3", 300)
    )

    val df = spark.createDataFrame(testData).toDF("id", "content", "score")

    // Configure OAuth credentials at session level
    spark.conf.set("spark.indextables.azure.accountName", storageAcct)
    spark.conf.set("spark.indextables.azure.tenantId", tenantId)
    spark.conf.set("spark.indextables.azure.clientId", clientId)
    spark.conf.set("spark.indextables.azure.clientSecret", clientSecret)

    // Write data
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(abfssPath)

    println(s"✅ Wrote DataFrame using OAuth for MERGE SPLITS test: $abfssPath")

    // Execute MERGE SPLITS with OAuth
    spark.sql(s"MERGE SPLITS '$abfssPath' TARGET SIZE 1M")

    println("✅ MERGE SPLITS executed successfully with OAuth authentication")

    // Verify data integrity
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(abfssPath)

    val rowCount = readDf.count()
    rowCount shouldBe 3

    println("✅ Data integrity verified after MERGE SPLITS with OAuth")
  }

}
