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

import io.indextables.spark.RealS3TestBase

/**
 * Full lifecycle integration tests for Avro state format on real AWS S3.
 *
 * These tests validate the Avro transaction log format works correctly through
 * the complete table lifecycle:
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
 *   - AWS credentials configured via ~/.aws/credentials or environment variables
 *   - S3 bucket accessible for testing (default: test-tantivy4sparkbucket in us-east-2)
 *
 * Run with:
 *   mvn scalatest:test -DwildcardSuites='io.indextables.spark.transaction.avro.RealS3AvroStateLifecycleTest'
 */
class RealS3AvroStateLifecycleTest extends RealS3TestBase {

  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  // Use same bucket and region as other RealS3 tests
  private val S3_BUCKET = System.getProperty("test.s3.bucket", "test-tantivy4sparkbucket")
  private val S3_REGION = System.getProperty("test.s3.region", "us-east-2")

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    assume(awsCredentials.isDefined, "AWS credentials not available - skipping tests")

    val (accessKey, secretKey) = awsCredentials.get

    // Configure Spark for real S3 access
    spark.conf.set("spark.indextables.aws.accessKey", accessKey)
    spark.conf.set("spark.indextables.aws.secretKey", secretKey)
    spark.conf.set("spark.indextables.aws.region", S3_REGION)

    // Also configure Hadoop config for CloudStorageProvider
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
    hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
    hadoopConf.set("spark.indextables.aws.region", S3_REGION)

    println(s"🔐 AWS credentials loaded for Avro state lifecycle tests")
    println(s"📍 Test bucket: $S3_BUCKET, region: $S3_REGION")
  }

  /** Load AWS credentials from ~/.aws/credentials file. */
  private def loadAwsCredentials(): Option[(String, String)] =
    try {
      import java.io.{File, FileInputStream}
      import java.util.Properties
      import scala.util.Using

      val home = System.getProperty("user.home")
      val credFile = new File(s"$home/.aws/credentials")

      if (!credFile.exists()) {
        println("⚠️  ~/.aws/credentials file not found")
        return None
      }

      val props = new Properties()
      Using(new FileInputStream(credFile)) { stream =>
        props.load(stream)
      }

      // Try to get credentials from [default] profile
      val accessKey = Option(props.getProperty("aws_access_key_id"))
      val secretKey = Option(props.getProperty("aws_secret_access_key"))

      (accessKey, secretKey) match {
        case (Some(ak), Some(sk)) => Some((ak, sk))
        case _ =>
          println("⚠️  AWS credentials not found in ~/.aws/credentials")
          None
      }
    } catch {
      case e: Exception =>
        println(s"⚠️  Error reading AWS credentials: ${e.getMessage}")
        None
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
  private def getReadOptions(): Map[String, String] = getWriteOptions()

  test("Avro state full lifecycle: write -> checkpoint -> read -> merge -> drop partition -> truncate -> checkpoint -> read") {
    val testId = generateTestId()
    val path = s"s3a://$S3_BUCKET/avro-test-lifecycle-$testId"

    println(s"🚀 Starting Avro state lifecycle test at: $path")

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
      .options(getWriteOptions())
      .partitionBy("date")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(path)

    println("✅ Step 1 complete: Wrote 6 records in 3 partitions")

    // ========================================
    // Step 2: Create Avro checkpoint
    // ========================================
    println("\n💾 Step 2: Creating Avro checkpoint...")
    val checkpointResult1 = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
    println(s"  ➤ Checkpoint result: ${checkpointResult1.map(_.toString).mkString(", ")}")

    // Verify state format is avro-state
    val stateResult1 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
    stateResult1.length shouldBe 1
    stateResult1(0).getAs[String]("format") shouldBe "avro-state"

    println(s"✅ Step 2 complete: Avro checkpoint created, format=${stateResult1(0).getAs[String]("format")}")

    // ========================================
    // Step 3: Read from Avro state and verify
    // ========================================
    println("\n📖 Step 3: Reading from Avro state...")
    val read1 = spark.read.format(provider).options(getReadOptions()).load(path)

    val count1 = read1.count()
    count1 shouldBe 6

    // Verify partition filtering works with Avro state
    val jan1Data = read1.filter(col("date") === "2024-01-01").collect()
    jan1Data.length shouldBe 2
    jan1Data.map(_.getAs[String]("name")).toSet shouldBe Set("Alice", "Bob")

    // Verify aggregations work with Avro state
    val sumScore1 = read1.agg(sum("score")).collect()(0).getLong(0)
    sumScore1 shouldBe 1100L // 100+200+150+250+175+225

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
      .options(getWriteOptions())
      .partitionBy("date")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("append")
      .save(path)

    // Verify incremental read (Avro checkpoint + JSON transaction log)
    val read2 = spark.read.format(provider).options(getReadOptions()).load(path)
    read2.count() shouldBe 8

    println("✅ Step 4 complete: Appended 2 records, now have 8 total")

    // ========================================
    // Step 5: Merge splits with Avro state
    // ========================================
    println("\n🔀 Step 5: Merging splits...")
    val mergeResult = spark.sql(s"MERGE SPLITS '$path' TARGET SIZE 100M").collect()
    println(s"  ➤ Merge result: ${mergeResult.map(_.toString).mkString(", ")}")

    // Verify data integrity after merge
    val read3 = spark.read.format(provider).options(getReadOptions()).load(path)
    read3.count() shouldBe 8

    val allNames = read3.collect().map(_.getAs[String]("name")).toSet
    allNames shouldBe Set("Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry")

    println("✅ Step 5 complete: Merged splits, data integrity verified")

    // ========================================
    // Step 6: Create Avro checkpoint after merge
    // ========================================
    println("\n💾 Step 6: Creating Avro checkpoint after merge...")
    val checkpointResult2 = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
    println(s"  ➤ Checkpoint result: ${checkpointResult2.map(_.toString).mkString(", ")}")

    // Verify still avro-state format
    val stateResult2 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
    stateResult2(0).getAs[String]("format") shouldBe "avro-state"

    println(s"✅ Step 6 complete: Avro checkpoint after merge")

    // ========================================
    // Step 7: Drop partition with Avro state
    // ========================================
    println("\n🗑️ Step 7: Dropping partition (date = '2024-01-02')...")
    val dropResult = spark.sql(s"DROP INDEXTABLES PARTITIONS FROM '$path' WHERE date = '2024-01-02'").collect()
    println(s"  ➤ Drop result: ${dropResult.map(_.toString).mkString(", ")}")

    // Verify data after drop
    val read4 = spark.read.format(provider).options(getReadOptions()).load(path)
    read4.count() shouldBe 5 // 8 - 3 (Charlie, Diana, Henry)

    val jan2Count = read4.filter(col("date") === "2024-01-02").count()
    jan2Count shouldBe 0

    println("✅ Step 7 complete: Dropped partition, 5 records remaining")

    // ========================================
    // Step 8: Truncate time travel with Avro state
    // ========================================
    println("\n✂️ Step 8: Truncating time travel...")
    val truncateResult = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$path'").collect()
    println(s"  ➤ Truncate result: ${truncateResult.map(_.toString).mkString(", ")}")

    // Verify data still accessible after truncate
    val read5 = spark.read.format(provider).options(getReadOptions()).load(path)
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
      .options(getWriteOptions())
      .partitionBy("date")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("append")
      .save(path)

    println("✅ Step 9 complete: Appended 2 more records")

    // ========================================
    // Step 10: Final Avro checkpoint
    // ========================================
    println("\n💾 Step 10: Creating final Avro checkpoint...")
    val checkpointResult3 = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
    println(s"  ➤ Checkpoint result: ${checkpointResult3.map(_.toString).mkString(", ")}")

    // Verify final state
    val stateResultFinal = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
    stateResultFinal(0).getAs[String]("format") shouldBe "avro-state"

    println(s"✅ Step 10 complete: Final Avro checkpoint created")

    // ========================================
    // Step 11: Final verification
    // ========================================
    println("\n📖 Step 11: Final verification...")
    val finalRead = spark.read.format(provider).options(getReadOptions()).load(path)

    val finalCount = finalRead.count()
    finalCount shouldBe 7 // 5 + 2

    val finalNames = finalRead.collect().map(_.getAs[String]("name")).toSet
    finalNames shouldBe Set("Alice", "Bob", "Grace", "Eve", "Frank", "Ivan", "Julia")

    // Verify aggregations
    val finalSum = finalRead.agg(sum("score")).collect()(0).getLong(0)
    // Alice(100) + Bob(200) + Grace(180) + Eve(175) + Frank(225) + Ivan(300) + Julia(350) = 1530
    finalSum shouldBe 1530L

    println(s"✅ Step 11 complete: Final count=$finalCount, sum(score)=$finalSum")

    println(s"""
    |============================================
    |🎉 AVRO STATE LIFECYCLE TEST COMPLETED
    |============================================
    |Path: $path
    |Format: avro-state (verified at each checkpoint)
    |Final records: $finalCount
    |Final sum(score): $finalSum
    |============================================
    """.stripMargin)
  }

  test("Avro state: schema registry preserves docMappingJson across checkpoint") {
    val testId = generateTestId()
    val path = s"s3a://$S3_BUCKET/avro-test-schema-registry-$testId"

    println(s"🚀 Testing schema registry preservation at: $path")

    // Write data with text field (requires docMappingJson)
    val data = Seq(
      (1, "machine learning with python"),
      (2, "deep learning and neural networks"),
      (3, "python programming language")
    )

    val df = spark.createDataFrame(data).toDF("id", "content")

    df.write
      .format(provider)
      .options(getWriteOptions())
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(path)

    println("✅ Wrote data with text field")

    // Create Avro checkpoint
    spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

    // Verify state format
    val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
    stateResult(0).getAs[String]("format") shouldBe "avro-state"

    println("✅ Created Avro checkpoint")

    // Read and verify IndexQuery works (proves docMappingJson was preserved)
    val readDf = spark.read.format(provider).options(getReadOptions()).load(path)

    val pythonDocs = readDf.filter("content indexquery 'python'").collect()
    pythonDocs.length shouldBe 2

    println(s"✅ IndexQuery works after Avro checkpoint (found ${pythonDocs.length} docs)")
    println("✅ Schema registry correctly preserves docMappingJson")
  }

  test("Avro state: partition pruning with manifest bounds") {
    val testId = generateTestId()
    val path = s"s3a://$S3_BUCKET/avro-test-partition-pruning-$testId"

    println(s"🚀 Testing partition pruning at: $path")

    // Create data with multiple partitions
    val data = (1 to 100).map { i =>
      val region = s"region_${i % 10}"
      (i, s"name_$i", region, i * 10)
    }

    val df = spark.createDataFrame(data).toDF("id", "name", "region", "score")

    df.write
      .format(provider)
      .options(getWriteOptions())
      .partitionBy("region")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(path)

    println("✅ Wrote 100 records across 10 partitions")

    // Create Avro checkpoint
    spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

    // Verify state
    val stateResult = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
    stateResult(0).getAs[String]("format") shouldBe "avro-state"
    val numManifests = stateResult(0).getAs[Int]("num_manifests")

    println(s"✅ Created Avro checkpoint with $numManifests manifests")

    // Test partition pruning
    val readDf = spark.read.format(provider).options(getReadOptions()).load(path)

    // Single partition query
    val region0 = readDf.filter(col("region") === "region_0").count()
    region0 shouldBe 10

    // Multiple partitions with IN
    val multiRegion = readDf.filter(col("region").isin("region_0", "region_1", "region_2")).count()
    multiRegion shouldBe 30

    println("✅ Partition pruning works correctly with Avro state")
  }

  test("Avro state: streaming-aware version tracking") {
    val testId = generateTestId()
    val path = s"s3a://$S3_BUCKET/avro-test-streaming-$testId"

    println(s"🚀 Testing streaming version tracking at: $path")

    // Write batch 1
    val batch1 = Seq((1, "doc1"), (2, "doc2"))
    spark.createDataFrame(batch1).toDF("id", "content")
      .write.format(provider).options(getWriteOptions()).mode("overwrite").save(path)

    // Checkpoint
    spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

    // Get version after first checkpoint
    val state1 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
    val version1 = state1(0).getAs[Long]("version")

    println(s"✅ Version after batch 1: $version1")

    // Write batch 2
    val batch2 = Seq((3, "doc3"), (4, "doc4"))
    spark.createDataFrame(batch2).toDF("id", "content")
      .write.format(provider).options(getWriteOptions()).mode("append").save(path)

    // Checkpoint again
    spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

    // Get version after second checkpoint
    val state2 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
    val version2 = state2(0).getAs[Long]("version")

    println(s"✅ Version after batch 2: $version2")

    version2 should be > version1

    // Verify all data
    val allData = spark.read.format(provider).options(getReadOptions()).load(path)
    allData.count() shouldBe 4

    println("✅ Version tracking works correctly for streaming scenarios")
  }
}
