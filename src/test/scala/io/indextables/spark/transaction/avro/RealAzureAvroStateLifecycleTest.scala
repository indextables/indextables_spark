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

package io.indextables.spark.transaction.avro

import org.apache.spark.sql.functions._

import io.indextables.spark.RealAzureTestBase

/**
 * Full lifecycle integration tests for Avro state format on real Azure Blob Storage.
 *
 * These tests validate the Avro transaction log format works correctly through
 * the complete table lifecycle on Azure:
 *   1. Write data and create Avro checkpoint
 *   2. Verify state format is "avro-state"
 *   3. Read data from Avro state
 *   4. Append data (incremental write after Avro checkpoint)
 *   5. Merge splits with Avro state
 *   6. Drop partition with Avro state
 *   7. Truncate time travel with Avro state
 *   8. Create new Avro checkpoint
 *   9. Verify final state and data integrity
 *
 * Prerequisites:
 *   - Azure credentials configured via ~/.azure/credentials or environment variables
 *   - Azure container accessible for testing
 *
 * Run with:
 *   mvn test -DwildcardSuites='io.indextables.spark.transaction.avro.RealAzureAvroStateLifecycleTest'
 *
 * Configure container via system property:
 *   -Dtest.azure.container=your-test-container
 */
class RealAzureAvroStateLifecycleTest extends RealAzureTestBase {

  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  override def beforeAll(): Unit = {
    super.beforeAll()
    assume(
      hasAzureCredentials(),
      "Azure credentials not available - skipping tests"
    )
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Enable Avro state format for all tests
    spark.conf.set("spark.indextables.state.format", "avro")
  }

  override def afterEach(): Unit = {
    // Reset to default
    spark.conf.unset("spark.indextables.state.format")
    super.afterEach()
  }

  test("Avro state full lifecycle on Azure: write -> checkpoint -> read -> merge -> drop partition -> truncate -> checkpoint -> read") {
    val testId = generateTestId()
    val azurePath = s"azure://$testContainer/avro-lifecycle-$testId"

    println(s"🚀 Starting Avro state lifecycle test at: $azurePath")

    // ========================================
    // Step 1: Write partitioned data
    // ========================================
    println("\n📝 Step 1: Writing partitioned data...")
    val initialData = Seq(
      (1, "Alice", "2024-01-01", 100),
      (2, "Bob", "2024-01-01", 200),
      (3, "Charlie", "2024-01-02", 150),
      (4, "Diana", "2024-01-02", 250),
      (5, "Eve", "2024-01-03", 175),
      (6, "Frank", "2024-01-03", 225)
    )

    val df1 = spark.createDataFrame(initialData).toDF("id", "name", "date", "score")

    df1.write
      .format(provider)
      .partitionBy("date")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(azurePath)

    println("✅ Step 1 complete: Wrote 6 records in 3 partitions")

    // ========================================
    // Step 2: Create Avro checkpoint
    // ========================================
    println("\n💾 Step 2: Creating Avro checkpoint...")
    val checkpointResult1 = spark.sql(s"CHECKPOINT INDEXTABLES '$azurePath'").collect()
    println(s"  ➤ Checkpoint result: ${checkpointResult1.map(_.toString).mkString(", ")}")

    // Verify state format is avro-state
    val stateResult1 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$azurePath'").collect()
    stateResult1.length shouldBe 1
    stateResult1(0).getAs[String]("format") shouldBe "avro-state"

    println(s"✅ Step 2 complete: Avro checkpoint created, format=${stateResult1(0).getAs[String]("format")}")

    // ========================================
    // Step 3: Read from Avro state and verify
    // ========================================
    println("\n📖 Step 3: Reading from Avro state...")
    val read1 = spark.read.format(provider).load(azurePath)

    val count1 = read1.count()
    count1 shouldBe 6

    // Verify partition filtering works with Avro state
    val jan1Data = read1.filter(col("date") === "2024-01-01").collect()
    jan1Data.length shouldBe 2
    jan1Data.map(_.getAs[String]("name")).toSet shouldBe Set("Alice", "Bob")

    // Verify aggregations work with Avro state
    val sumScore1 = read1.agg(sum("score")).collect()(0).getLong(0)
    sumScore1 shouldBe 1100L

    println(s"✅ Step 3 complete: Read $count1 records from Avro state, sum(score)=$sumScore1")

    // ========================================
    // Step 4: Append data (incremental after Avro checkpoint)
    // ========================================
    println("\n📝 Step 4: Appending data after Avro checkpoint...")
    val appendData = Seq(
      (7, "Grace", "2024-01-01", 180),
      (8, "Henry", "2024-01-02", 220)
    )
    val dfAppend = spark.createDataFrame(appendData).toDF("id", "name", "date", "score")

    dfAppend.write
      .format(provider)
      .partitionBy("date")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("append")
      .save(azurePath)

    // Verify incremental read (Avro checkpoint + JSON transaction log)
    val read2 = spark.read.format(provider).load(azurePath)
    read2.count() shouldBe 8

    println("✅ Step 4 complete: Appended 2 records, now have 8 total")

    // ========================================
    // Step 5: Merge splits with Avro state
    // ========================================
    println("\n🔀 Step 5: Merging splits...")
    val mergeResult = spark.sql(s"MERGE SPLITS '$azurePath' TARGET SIZE 100M").collect()
    println(s"  ➤ Merge result: ${mergeResult.map(_.toString).mkString(", ")}")

    // Verify data integrity after merge
    val read3 = spark.read.format(provider).load(azurePath)
    read3.count() shouldBe 8

    val allNames = read3.collect().map(_.getAs[String]("name")).toSet
    allNames shouldBe Set("Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry")

    println("✅ Step 5 complete: Merged splits, data integrity verified")

    // ========================================
    // Step 6: Create Avro checkpoint after merge
    // ========================================
    println("\n💾 Step 6: Creating Avro checkpoint after merge...")
    val checkpointResult2 = spark.sql(s"CHECKPOINT INDEXTABLES '$azurePath'").collect()
    println(s"  ➤ Checkpoint result: ${checkpointResult2.map(_.toString).mkString(", ")}")

    // Verify still avro-state format
    val stateResult2 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$azurePath'").collect()
    stateResult2(0).getAs[String]("format") shouldBe "avro-state"

    println(s"✅ Step 6 complete: Avro checkpoint after merge")

    // ========================================
    // Step 7: Drop partition with Avro state
    // ========================================
    println("\n🗑️ Step 7: Dropping partition (date = '2024-01-02')...")
    val dropResult = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$azurePath' WHERE date = '2024-01-02'").collect()
    println(s"  ➤ Drop result: ${dropResult.map(_.toString).mkString(", ")}")

    // Verify data after drop
    val read4 = spark.read.format(provider).load(azurePath)
    read4.count() shouldBe 5

    val jan2Count = read4.filter(col("date") === "2024-01-02").count()
    jan2Count shouldBe 0

    println("✅ Step 7 complete: Dropped partition, 5 records remaining")

    // ========================================
    // Step 8: Truncate time travel with Avro state
    // ========================================
    println("\n✂️ Step 8: Truncating time travel...")
    val truncateResult = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$azurePath'").collect()
    println(s"  ➤ Truncate result: ${truncateResult.map(_.toString).mkString(", ")}")

    // Verify data still accessible after truncate
    val read5 = spark.read.format(provider).load(azurePath)
    read5.count() shouldBe 5

    println("✅ Step 8 complete: Truncated time travel, data preserved")

    // ========================================
    // Step 9: Append more data
    // ========================================
    println("\n📝 Step 9: Appending more data...")
    val finalAppendData = Seq(
      (9, "Ivan", "2024-01-04", 300),
      (10, "Julia", "2024-01-04", 350)
    )

    val dfFinalAppend = spark.createDataFrame(finalAppendData).toDF("id", "name", "date", "score")

    dfFinalAppend.write
      .format(provider)
      .partitionBy("date")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("append")
      .save(azurePath)

    println("✅ Step 9 complete: Appended 2 more records")

    // ========================================
    // Step 10: Final Avro checkpoint
    // ========================================
    println("\n💾 Step 10: Creating final Avro checkpoint...")
    val checkpointResult3 = spark.sql(s"CHECKPOINT INDEXTABLES '$azurePath'").collect()
    println(s"  ➤ Checkpoint result: ${checkpointResult3.map(_.toString).mkString(", ")}")

    // Verify final state
    val stateResultFinal = spark.sql(s"DESCRIBE INDEXTABLES STATE '$azurePath'").collect()
    stateResultFinal(0).getAs[String]("format") shouldBe "avro-state"

    println(s"✅ Step 10 complete: Final Avro checkpoint created")

    // ========================================
    // Step 11: Final verification
    // ========================================
    println("\n📖 Step 11: Final verification...")
    val finalRead = spark.read.format(provider).load(azurePath)

    val finalCount = finalRead.count()
    finalCount shouldBe 7

    val finalNames = finalRead.collect().map(_.getAs[String]("name")).toSet
    finalNames shouldBe Set("Alice", "Bob", "Grace", "Eve", "Frank", "Ivan", "Julia")

    // Verify aggregations
    val finalSum = finalRead.agg(sum("score")).collect()(0).getLong(0)
    finalSum shouldBe 1530L

    println(s"✅ Step 11 complete: Final count=$finalCount, sum(score)=$finalSum")

    println(s"""
    |============================================
    |🎉 AZURE AVRO STATE LIFECYCLE TEST COMPLETED
    |============================================
    |Path: $azurePath
    |Format: avro-state (verified at each checkpoint)
    |Final records: $finalCount
    |Final sum(score): $finalSum
    |============================================
    """.stripMargin)
  }

  test("Avro state on Azure: schema registry preserves docMappingJson") {
    val testId = generateTestId()
    val azurePath = s"azure://$testContainer/avro-schema-registry-$testId"

    println(s"🚀 Testing schema registry on Azure at: $azurePath")

    // Write data with text field
    val data = Seq(
      (1, "machine learning with python"),
      (2, "deep learning and neural networks"),
      (3, "python programming language")
    )

    val df = spark.createDataFrame(data).toDF("id", "content")

    df.write
      .format(provider)
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(azurePath)

    println("✅ Wrote data with text field")

    // Create Avro checkpoint
    spark.sql(s"CHECKPOINT INDEXTABLES '$azurePath'").collect()

    // Verify state format
    val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$azurePath'").collect()
    stateResult(0).getAs[String]("format") shouldBe "avro-state"

    println("✅ Created Avro checkpoint")

    // Read and verify IndexQuery works
    val readDf = spark.read.format(provider).load(azurePath)

    val pythonDocs = readDf.filter("content indexquery 'python'").collect()
    pythonDocs.length shouldBe 2

    println(s"✅ IndexQuery works after Avro checkpoint (found ${pythonDocs.length} docs)")
  }

  test("Avro state on Azure with abfss:// URL scheme") {
    val testId = generateTestId()
    val storageAcct = getStorageAccount.getOrElse("devstoreaccount1")
    val abfssPath = s"abfss://$testContainer@$storageAcct.dfs.core.windows.net/avro-abfss-$testId"

    println(s"🚀 Testing Avro state with abfss:// at: $abfssPath")

    // Write data
    val data = Seq(
      (1, "ADLS Gen2 with Avro state", 100),
      (2, "Azure Data Lake Storage", 200),
      (3, "Hierarchical namespace", 300)
    )

    val df = spark.createDataFrame(data).toDF("id", "content", "score")

    df.write
      .format(provider)
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(abfssPath)

    println("✅ Wrote data using abfss://")

    // Create Avro checkpoint
    spark.sql(s"CHECKPOINT INDEXTABLES '$abfssPath'").collect()

    // Verify state format
    val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$abfssPath'").collect()
    stateResult(0).getAs[String]("format") shouldBe "avro-state"

    println("✅ Created Avro checkpoint using abfss://")

    // Read and verify
    val readDf = spark.read.format(provider).load(abfssPath)
    readDf.count() shouldBe 3

    // Verify aggregations
    val sumScore = readDf.agg(sum("score")).collect()(0).getLong(0)
    sumScore shouldBe 600L

    println(s"✅ Read from Avro state using abfss://, sum(score)=$sumScore")
  }

  test("Avro state on Azure: partition pruning with manifest bounds") {
    val testId = generateTestId()
    val azurePath = s"azure://$testContainer/avro-partition-pruning-$testId"

    println(s"🚀 Testing partition pruning on Azure at: $azurePath")

    // Create data with multiple partitions
    val data = (1 to 100).map { i =>
      val region = s"region_${i % 10}"
      (i, s"name_$i", region, i * 10)
    }

    val df = spark.createDataFrame(data).toDF("id", "name", "region", "score")

    df.write
      .format(provider)
      .partitionBy("region")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(azurePath)

    println("✅ Wrote 100 records across 10 partitions")

    // Create Avro checkpoint
    spark.sql(s"CHECKPOINT INDEXTABLES '$azurePath'").collect()

    // Verify state
    val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$azurePath'").collect()
    stateResult(0).getAs[String]("format") shouldBe "avro-state"

    println("✅ Created Avro checkpoint")

    // Test partition pruning
    val readDf = spark.read.format(provider).load(azurePath)

    // Single partition query
    val region0 = readDf.filter(col("region") === "region_0").count()
    region0 shouldBe 10

    // Multiple partitions with IN
    val multiRegion = readDf.filter(col("region").isin("region_0", "region_1", "region_2")).count()
    multiRegion shouldBe 30

    println("✅ Partition pruning works correctly with Avro state on Azure")
  }

  test("Avro state on Azure with OAuth authentication") {
    // Skip if OAuth credentials not available
    assume(
      hasOAuthCredentials(),
      "OAuth credentials not available - skipping test"
    )

    val tenantId = getTenantId.get
    val clientId = getClientId.get
    val clientSecret = getClientSecret.get
    val storageAcct = getStorageAccount.getOrElse("devstoreaccount1")

    val testId = generateTestId()
    val abfssPath = s"abfss://$testContainer@$storageAcct.dfs.core.windows.net/avro-oauth-$testId"

    println(s"🚀 Testing Avro state with OAuth at: $abfssPath")

    // Configure OAuth
    spark.conf.set("spark.indextables.azure.accountName", storageAcct)
    spark.conf.set("spark.indextables.azure.tenantId", tenantId)
    spark.conf.set("spark.indextables.azure.clientId", clientId)
    spark.conf.set("spark.indextables.azure.clientSecret", clientSecret)

    // Write data
    val data = Seq(
      (1, "OAuth with Avro state", 100),
      (2, "Service Principal auth", 200)
    )

    val df = spark.createDataFrame(data).toDF("id", "content", "score")

    df.write
      .format(provider)
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(abfssPath)

    println("✅ Wrote data with OAuth")

    // Create Avro checkpoint
    spark.sql(s"CHECKPOINT INDEXTABLES '$abfssPath'").collect()

    // Verify state format
    val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$abfssPath'").collect()
    stateResult(0).getAs[String]("format") shouldBe "avro-state"

    println("✅ Created Avro checkpoint with OAuth")

    // Read and verify
    val readDf = spark.read.format(provider).load(abfssPath)
    readDf.count() shouldBe 2

    println("✅ Avro state works correctly with OAuth authentication")
  }
}
