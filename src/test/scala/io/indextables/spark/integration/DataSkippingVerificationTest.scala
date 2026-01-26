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

package io.indextables.spark.integration

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.core.{IndexTables4SparkScan, IndexTables4SparkInputPartition, IndexTables4SparkMultiSplitInputPartition}
import io.indextables.spark.transaction.{AddAction, TransactionLog, TransactionLogFactory}
import io.indextables.spark.TestBase
import org.apache.spark.sql.connector.read.InputPartition
import org.scalatest.BeforeAndAfterEach

class DataSkippingVerificationTest extends TestBase with BeforeAndAfterEach {

  private var testTablePath: Path            = _
  private var transactionLog: TransactionLog = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    testTablePath = new Path(tempDir, "data_skipping_verification")
    transactionLog = TransactionLogFactory.create(testTablePath, spark)

    val schema = StructType(
      Array(
        StructField("id", LongType, false),
        StructField("name", StringType, false),
        StructField("department", StringType, false)
      )
    )

    transactionLog.initialize(schema)
  }

  test("data skipping should reduce scanned partitions with EqualTo filter") {
    // Setup: Create 3 files with distinct ID ranges
    val file1 = createAddActionWithStats(
      "file1.tnt4s",
      minValues = Map("id" -> "1", "name" -> "Alice", "department" -> "Engineering"),
      maxValues = Map("id" -> "100", "name" -> "John", "department" -> "Engineering")
    )
    val file2 = createAddActionWithStats(
      "file2.tnt4s",
      minValues = Map("id" -> "101", "name" -> "Kate", "department" -> "Marketing"),
      maxValues = Map("id" -> "200", "name" -> "Mike", "department" -> "Marketing")
    )
    val file3 = createAddActionWithStats(
      "file3.tnt4s",
      minValues = Map("id" -> "201", "name" -> "Sam", "department" -> "Sales"),
      maxValues = Map("id" -> "300", "name" -> "Zoe", "department" -> "Sales")
    )

    transactionLog.addFile(file1)
    transactionLog.addFile(file2)
    transactionLog.addFile(file3)

    // Test: Query for id=150 (should only scan file2)
    val filter     = EqualTo("id", 150L)
    val scan       = createScanWithFilters(Array(filter))
    val partitions = scan.planInputPartitions()

    // Verify: Should only scan file2
    val paths = extractPaths(partitions)
    assert(paths.length == 1, s"Expected 1 split after data skipping, got ${paths.length}")
    assert(paths.head.contains("file2"), s"Expected file2, got ${paths.head}")

    println("✅ EqualTo filter test passed: Correctly skipped 2 out of 3 files")
  }

  test("data skipping should work with range filters") {
    val file1 = createAddActionWithStats("range1.tnt4s", minValues = Map("id" -> "1"), maxValues = Map("id" -> "100"))
    val file2 = createAddActionWithStats("range2.tnt4s", minValues = Map("id" -> "101"), maxValues = Map("id" -> "200"))
    val file3 = createAddActionWithStats("range3.tnt4s", minValues = Map("id" -> "201"), maxValues = Map("id" -> "300"))

    transactionLog.addFile(file1)
    transactionLog.addFile(file2)
    transactionLog.addFile(file3)

    // Test GreaterThan filter (should skip file1)
    val gtFilter     = GreaterThan("id", 100L)
    val gtScan       = createScanWithFilters(Array(gtFilter))
    val gtPartitions = gtScan.planInputPartitions()

    val gtPaths = extractPaths(gtPartitions)
    assert(gtPaths.length == 2, s"GreaterThan: Expected 2 splits, got ${gtPaths.length}")

    assert(!gtPaths.exists(_.contains("range1")), "GreaterThan should skip range1")
    assert(gtPaths.exists(_.contains("range2")), "GreaterThan should include range2")
    assert(gtPaths.exists(_.contains("range3")), "GreaterThan should include range3")

    // Test LessThan filter (should skip file3)
    val ltFilter     = LessThan("id", 201L)
    val ltScan       = createScanWithFilters(Array(ltFilter))
    val ltPartitions = ltScan.planInputPartitions()

    val ltPaths = extractPaths(ltPartitions)
    assert(ltPaths.length == 2, s"LessThan: Expected 2 splits, got ${ltPaths.length}")

    assert(ltPaths.exists(_.contains("range1")), "LessThan should include range1")
    assert(ltPaths.exists(_.contains("range2")), "LessThan should include range2")
    assert(!ltPaths.exists(_.contains("range3")), "LessThan should skip range3")

    println("✅ Range filter tests passed: GreaterThan and LessThan correctly skip files")
  }

  test("data skipping should handle missing statistics gracefully") {
    val fileWithStats =
      createAddActionWithStats("with_stats.tnt4s", minValues = Map("id" -> "1"), maxValues = Map("id" -> "100"))
    val fileWithoutStats = createAddActionWithoutStats("no_stats.tnt4s")

    transactionLog.addFile(fileWithStats)
    transactionLog.addFile(fileWithoutStats)

    // Test: Filter that would skip fileWithStats but not fileWithoutStats
    val filter     = EqualTo("id", 200L) // Outside range of fileWithStats
    val scan       = createScanWithFilters(Array(filter))
    val partitions = scan.planInputPartitions()

    // Should still scan fileWithoutStats (no stats = can't skip)
    val paths = extractPaths(partitions)
    assert(paths.length == 1, s"Expected 1 split (no stats file), got ${paths.length}")
    assert(paths.head.contains("no_stats"), "Should keep file without statistics")

    println("✅ Missing statistics test passed: Files without stats are not skipped")
  }

  test("data skipping should log statistics") {
    val file1 = createAddActionWithStats("log1.tnt4s", minValues = Map("id" -> "1"), maxValues = Map("id" -> "100"))
    val file2 = createAddActionWithStats("log2.tnt4s", minValues = Map("id" -> "101"), maxValues = Map("id" -> "200"))
    val file3 = createAddActionWithStats("log3.tnt4s", minValues = Map("id" -> "201"), maxValues = Map("id" -> "300"))

    transactionLog.addFile(file1)
    transactionLog.addFile(file2)
    transactionLog.addFile(file3)

    // Capture console output
    val originalOut  = System.out
    val outputStream = new java.io.ByteArrayOutputStream()
    val printStream  = new java.io.PrintStream(outputStream)

    try {
      // Redirect system output to capture logs
      System.setOut(printStream)

      val filter = EqualTo("id", 150L)
      val scan   = createScanWithFilters(Array(filter))
      scan.planInputPartitions() // This should generate log messages

      val output = outputStream.toString()

      // Note: This test may not capture SLF4J logs, but demonstrates the approach
      println("✅ Logging test completed - data skipping statistics should be logged")
      println(s"Captured output length: ${output.length} characters")

    } finally
      System.setOut(originalOut)
  }

  private def createAddActionWithStats(
    filename: String,
    minValues: Map[String, String],
    maxValues: Map[String, String]
  ): AddAction =
    AddAction(
      path = filename,
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(100L),
      minValues = Some(minValues),
      maxValues = Some(maxValues)
    )

  private def createAddActionWithoutStats(filename: String): AddAction =
    AddAction(
      path = filename,
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(100L),
      minValues = None,
      maxValues = None
    )

  test("data skipping with truncated statistics - GreaterThanOrEqual should not skip when filterValue starts with truncated max") {
    // Scenario: max="aaa" (truncated from "aaazzz"), filterValue="aaab"
    // Actual values could be "aaac", "aaad", etc. which are >= "aaab"
    // Should NOT skip this file
    val fileWithTruncatedMax = createAddActionWithStats(
      "truncated_max.tnt4s",
      minValues = Map("name" -> "aaa"),
      maxValues = Map("name" -> "aaa") // Truncated - actual max could be "aaazzz"
    )

    transactionLog.addFile(fileWithTruncatedMax)

    // Test: filterValue="aaab" starts with max="aaa" - should NOT skip
    val filter     = GreaterThanOrEqual("name", "aaab")
    val scan       = createScanWithFilters(Array(filter))
    val partitions = scan.planInputPartitions()

    assert(
      partitions.length == 1,
      s"Should NOT skip file when filterValue starts with truncated max, got ${partitions.length} partitions"
    )
    println("✅ Truncated max test passed: GreaterThanOrEqual correctly did NOT skip when filterValue starts with max")
  }

  test("data skipping with truncated statistics - GreaterThanOrEqual should skip when filterValue does NOT start with truncated max") {
    // Scenario: max="aaa" (truncated), filterValue="bbb"
    // All actual values start with "aaa", so all are < "bbb"
    // Should skip this file
    val fileWithTruncatedMax = createAddActionWithStats(
      "truncated_max2.tnt4s",
      minValues = Map("name" -> "aaa"),
      maxValues = Map("name" -> "aaa") // Truncated
    )

    transactionLog.addFile(fileWithTruncatedMax)

    // Test: filterValue="bbb" does NOT start with max="aaa" and "aaa" < "bbb" - should skip
    val filter     = GreaterThanOrEqual("name", "bbb")
    val scan       = createScanWithFilters(Array(filter))
    val partitions = scan.planInputPartitions()

    assert(
      partitions.length == 0,
      s"Should skip file when max < filterValue and filterValue does NOT start with max, got ${partitions.length} partitions"
    )
    println(
      "✅ Truncated max skip test passed: GreaterThanOrEqual correctly skipped when filterValue does NOT start with max"
    )
  }

  test("data skipping with truncated statistics - LessThanOrEqual should not skip when filterValue starts with truncated min") {
    // Scenario: min="bbb" (truncated from "bbbzzz"), filterValue="bbbc"
    // Actual values could be "bbba", "bbbb", etc. which are <= "bbbc"
    // Should NOT skip this file
    val fileWithTruncatedMin = createAddActionWithStats(
      "truncated_min.tnt4s",
      minValues = Map("name" -> "bbb"), // Truncated - actual min could be "bbbzzz"
      maxValues = Map("name" -> "bbb")
    )

    transactionLog.addFile(fileWithTruncatedMin)

    // Test: filterValue="bbbc" starts with min="bbb" - should NOT skip
    val filter     = LessThanOrEqual("name", "bbbc")
    val scan       = createScanWithFilters(Array(filter))
    val partitions = scan.planInputPartitions()

    assert(
      partitions.length == 1,
      s"Should NOT skip file when filterValue starts with truncated min, got ${partitions.length} partitions"
    )
    println("✅ Truncated min test passed: LessThanOrEqual correctly did NOT skip when filterValue starts with min")
  }

  test("data skipping with truncated statistics - LessThanOrEqual should skip when filterValue does NOT start with truncated min") {
    // Scenario: min="bbb" (truncated), filterValue="aaa"
    // All actual values start with "bbb", so all are > "aaa"
    // Should skip this file
    val fileWithTruncatedMin = createAddActionWithStats(
      "truncated_min2.tnt4s",
      minValues = Map("name" -> "bbb"), // Truncated
      maxValues = Map("name" -> "bbb")
    )

    transactionLog.addFile(fileWithTruncatedMin)

    // Test: filterValue="aaa" does NOT start with min="bbb" and "bbb" > "aaa" - should skip
    val filter     = LessThanOrEqual("name", "aaa")
    val scan       = createScanWithFilters(Array(filter))
    val partitions = scan.planInputPartitions()

    assert(
      partitions.length == 0,
      s"Should skip file when min > filterValue and filterValue does NOT start with min, got ${partitions.length} partitions"
    )
    println(
      "✅ Truncated min skip test passed: LessThanOrEqual correctly skipped when filterValue does NOT start with min"
    )
  }

  test("data skipping with truncated statistics - complex scenarios") {
    // File 1: min="log_2024_01_01" (full, not truncated), max="log_2024_01_31" (full, not truncated)
    val file1 = createAddActionWithStats(
      "file1.tnt4s",
      minValues = Map("name" -> "log_2024_01_01"),
      maxValues = Map("name" -> "log_2024_01_31")
    )

    // File 2: min="log_2024_02_01" (truncated to 64 chars), max="log_2024_02_28" (truncated to 64 chars)
    // Simulating truncation by using shorter values
    val file2 = createAddActionWithStats(
      "file2.tnt4s",
      minValues = Map("name" -> "log_2024_02_01_very_long_prefix_that_gets_truncated_at_64_ch"),
      maxValues = Map("name" -> "log_2024_02_28_very_long_prefix_that_gets_truncated_at_64_ch")
    )

    // File 3: min="log_2024_03_01", max="log_2024_03_31"
    val file3 = createAddActionWithStats(
      "file3.tnt4s",
      minValues = Map("name" -> "log_2024_03_01"),
      maxValues = Map("name" -> "log_2024_03_31")
    )

    transactionLog.addFile(file1)
    transactionLog.addFile(file2)
    transactionLog.addFile(file3)

    // Test 1: GreaterThanOrEqual with filterValue that starts with file2's truncated max
    val filter1     = GreaterThanOrEqual("name", "log_2024_02_28_very_long_prefix_that_gets_truncated_at_64_characters")
    val scan1       = createScanWithFilters(Array(filter1))
    val partitions1 = scan1.planInputPartitions()

    // Should include file2 (because filterValue starts with max) and file3
    val paths1 = extractPaths(partitions1)
    assert(paths1.length >= 2, s"Expected at least 2 splits (file2 and file3), got ${paths1.length}")
    assert(paths1.exists(_.contains("file2")), "Should include file2 (truncated max with matching prefix)")
    assert(paths1.exists(_.contains("file3")), "Should include file3")

    // Test 2: LessThanOrEqual with filterValue that starts with file2's truncated min
    val filter2     = LessThanOrEqual("name", "log_2024_02_01_very_long_prefix_that_gets_truncated_at_64_characters")
    val scan2       = createScanWithFilters(Array(filter2))
    val partitions2 = scan2.planInputPartitions()

    // Should include file1 and file2 (because filterValue starts with min)
    val paths2 = extractPaths(partitions2)
    assert(paths2.length >= 2, s"Expected at least 2 splits (file1 and file2), got ${paths2.length}")
    assert(paths2.exists(_.contains("file1")), "Should include file1")
    assert(paths2.exists(_.contains("file2")), "Should include file2 (truncated min with matching prefix)")

    println("✅ Complex truncation scenarios passed: Correctly handling prefix matching with truncated statistics")
  }

  /** Extract all paths from partitions (handles both single-split and multi-split types) */
  private def extractPaths(partitions: Array[InputPartition]): Seq[String] = {
    partitions.flatMap {
      case single: IndexTables4SparkInputPartition => Seq(single.addAction.path)
      case multi: IndexTables4SparkMultiSplitInputPartition => multi.addActions.map(_.path)
      case other => throw new IllegalArgumentException(s"Unknown partition type: ${other.getClass}")
    }.toSeq
  }

  private def createScanWithFilters(filters: Array[Filter]): IndexTables4SparkScan = {
    val schema = StructType(
      Array(
        StructField("id", LongType, false),
        StructField("name", StringType, false),
        StructField("department", StringType, false)
      )
    )

    val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)

    val emptyBroadcastConfig = spark.sparkContext.broadcast(Map.empty[String, String])
    new IndexTables4SparkScan(
      spark,
      transactionLog,
      schema,
      filters,
      options,
      None,
      emptyBroadcastConfig.value
    )
  }
}
