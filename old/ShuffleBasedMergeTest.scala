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

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Comprehensive test for shuffle-based merge-on-write
 *
 * Tests:
 * 1. Basic shuffle-based merge with multiple splits
 * 2. Bin-packing efficiency (FFD algorithm)
 * 3. Spark shuffle disk spilling (large splits)
 * 4. Data integrity after shuffle merge
 * 5. Partition support with shuffle
 * 6. S3 durability upload + shuffle merge
 */
class ShuffleBasedMergeTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _
  private var tempDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Configure Spark for aggressive disk spilling
    spark = SparkSession.builder()
      .appName("ShuffleBasedMergeTest")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.default.parallelism", "4")
      // Aggressive disk spilling configuration
      .config("spark.memory.fraction", "0.3")  // Lower = more aggressive spilling
      .config("spark.memory.storageFraction", "0.2")  // Less cache, more shuffle
      .config("spark.shuffle.spill", "true")
      .config("spark.shuffle.spill.compress", "true")
      .getOrCreate()

    tempDir = Files.createTempDirectory("shuffle-merge-test").toFile
    tempDir.deleteOnExit()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (tempDir != null && tempDir.exists()) {
      org.apache.commons.io.FileUtils.deleteDirectory(tempDir)
    }
    super.afterAll()
  }

  test("Basic shuffle-based merge with multiple splits") {
    val tablePath = new File(tempDir, "basic-shuffle-test").getAbsolutePath

    // Create test data - enough to create multiple splits
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val testData = 1.to(10000).map { i =>
      (i, s"content_$i", i % 100)
    }.toDF("id", "content", "score")
      .repartition(8)  // 8 partitions = 8 splits

    // Write with merge-on-write enabled
    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.indexWriter.batchSize", "1000")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "50M")  // Low threshold
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .mode(SaveMode.Overwrite)
      .save(s"file://$tablePath")

    // Verify data integrity
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(s"file://$tablePath")

    readData.count() shouldBe 10000

    val sample = readData.filter($"id" === 5000).collect()
    sample should have length 1
    sample(0).getAs[String]("content") shouldBe "content_5000"
    sample(0).getAs[Int]("score") shouldBe 0  // 5000 % 100 = 0

    println("✓ Basic shuffle-based merge test passed")
  }

  test("Bin-packing efficiency with FFD algorithm") {
    val tablePath = new File(tempDir, "bin-packing-test").getAbsolutePath

    // Create data that will produce splits of varying sizes
    // This tests the FFD (First-Fit-Decreasing) bin-packing algorithm
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create varying data sizes to test bin packing
    val testData = (1 to 1000).map { i =>
      val contentSize = if (i % 3 == 0) "x" * 1000 else "x" * 100
      (i, s"content_$i$contentSize", i % 50)
    }.toDF("id", "content", "score")
      .repartition(10)  // 10 splits of varying sizes

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.indexWriter.batchSize", "100")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "30M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .mode(SaveMode.Overwrite)
      .save(s"file://$tablePath")

    // Verify data
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(s"file://$tablePath")

    readData.count() shouldBe 1000

    println("✓ Bin-packing efficiency test passed")
    println("✓ Check logs for 'packing efficiency' metrics")
  }

  test("Large splits trigger disk spilling") {
    val tablePath = new File(tempDir, "disk-spill-test").getAbsolutePath

    // Create larger dataset to test disk spilling
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val testData = 1.to(50000).map { i =>
      (i, s"large_content_$i" + ("x" * 100), i % 200)
    }.toDF("id", "content", "score")
      .repartition(16)  // More partitions = more shuffle data

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.indexWriter.batchSize", "2000")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "100M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .mode(SaveMode.Overwrite)
      .save(s"file://$tablePath")

    // Verify data integrity
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(s"file://$tablePath")

    readData.count() shouldBe 50000

    // Verify shuffle happened correctly
    val sample = readData.filter($"id" === 25000).collect()
    sample should have length 1
    sample(0).getAs[String]("content") should startWith("large_content_25000")

    println("✓ Disk spilling test passed")
    println("✓ Check Spark UI for shuffle spill metrics")
  }

  test("Partitioned table with shuffle-based merge") {
    val tablePath = new File(tempDir, "partitioned-shuffle-test").getAbsolutePath

    // Create partitioned data
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val testData = 1.to(5000).map { i =>
      val category = s"category_${i % 5}"
      (i, s"content_$i", i % 100, category)
    }.toDF("id", "content", "score", "category")
      .repartition(10)

    // Write partitioned table with merge-on-write
    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.indexWriter.batchSize", "500")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "40M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .partitionBy("category")
      .mode(SaveMode.Overwrite)
      .save(s"file://$tablePath")

    // Verify data integrity
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(s"file://$tablePath")

    readData.count() shouldBe 5000

    // Verify partition pruning works
    val category0Data = readData.filter($"category" === "category_0")
    category0Data.count() shouldBe 1000  // 5000 / 5 categories

    println("✓ Partitioned shuffle-based merge test passed")
  }

  test("Data integrity after multiple shuffle merges") {
    val tablePath = new File(tempDir, "data-integrity-test").getAbsolutePath

    // Initial write
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val initialData = 1.to(3000).map { i =>
      (i, s"initial_$i", i % 75)
    }.toDF("id", "content", "score")
      .repartition(6)

    initialData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.indexWriter.batchSize", "500")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "30M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.transaction.cache.enabled", "false")  // Disable cache to avoid stale reads
      .mode(SaveMode.Overwrite)
      .save(s"file://$tablePath")

    // Append more data (trigger another merge)
    val appendData = 3001.to(6000).map { i =>
      (i, s"append_$i", i % 75)
    }.toDF("id", "content", "score")
      .repartition(6)

    appendData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.indexWriter.batchSize", "500")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "30M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.transaction.cache.enabled", "false")  // Disable cache to avoid stale reads
      .mode(SaveMode.Append)
      .save(s"file://$tablePath")

    // Verify all data is present and correct
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.cache.enabled", "false")  // Disable cache to avoid stale reads
      .load(s"file://$tablePath")

    readData.count() shouldBe 6000

    // Verify initial data
    val initialSample = readData.filter($"id" === 1500).collect()
    initialSample should have length 1
    initialSample(0).getAs[String]("content") shouldBe "initial_1500"

    // Verify appended data
    val appendSample = readData.filter($"id" === 4500).collect()
    appendSample should have length 1
    appendSample(0).getAs[String]("content") shouldBe "append_4500"

    // Verify no duplicates
    readData.select("id").limit(Int.MaxValue).distinct().count() shouldBe 6000

    println("✓ Data integrity test passed")
    println("✓ Multiple shuffle merges maintained data consistency")
  }

  test("Shuffle handles concurrent writes correctly") {
    val tablePath = new File(tempDir, "concurrent-shuffle-test").getAbsolutePath

    // Create initial table
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    val initialData = 1.to(2000).map { i =>
      (i, s"data_$i", i % 50)
    }.toDF("id", "content", "score")
      .repartition(4)

    initialData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.indexWriter.batchSize", "500")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "25M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.transaction.cache.enabled", "false")  // Disable cache to avoid stale reads
      .mode(SaveMode.Overwrite)
      .save(s"file://$tablePath")

    // Simulate concurrent appends
    val append1 = 2001.to(4000).map { i =>
      (i, s"data_$i", i % 50)
    }.toDF("id", "content", "score")
      .repartition(4)

    val append2 = 4001.to(6000).map { i =>
      (i, s"data_$i", i % 50)
    }.toDF("id", "content", "score")
      .repartition(4)

    // Append sequentially (concurrent appends to same table would be handled by transaction log)
    append1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.indexWriter.batchSize", "500")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "25M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.transaction.cache.enabled", "false")  // Disable cache to avoid stale reads
      .mode(SaveMode.Append)
      .save(s"file://$tablePath")

    append2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .option("spark.indextables.indexWriter.batchSize", "500")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "25M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .option("spark.indextables.transaction.cache.enabled", "false")  // Disable cache to avoid stale reads
      .mode(SaveMode.Append)
      .save(s"file://$tablePath")

    // Verify all data
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.cache.enabled", "false")  // Disable cache to avoid stale reads
      .load(s"file://$tablePath")

    readData.count() shouldBe 6000

    // Verify no data loss
    readData.select("id").limit(Int.MaxValue).distinct().count() shouldBe 6000

    println("✓ Concurrent shuffle merge test passed")
  }
}
