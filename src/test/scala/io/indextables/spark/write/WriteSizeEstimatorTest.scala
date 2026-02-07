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

package io.indextables.spark.write

import io.indextables.spark.transaction._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class WriteSizeEstimatorTest extends AnyFunSuite with Matchers {

  private val GB = 1024L * 1024 * 1024
  private val MB = 1024L * 1024

  private def makeAddAction(
    size: Long,
    numRecords: Option[Long] = None
  ): AddAction =
    AddAction(
      path = s"test-${System.nanoTime()}.split",
      partitionValues = Map.empty,
      size = size,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = numRecords
    )

  /** Minimal stub of TransactionLogInterface that returns canned AddActions. */
  private class StubTransactionLog(files: Seq[AddAction] = Seq.empty, throwOnList: Boolean = false)
      extends TransactionLogInterface {

    override def getTablePath(): Path = new Path("file:///tmp/stub")
    override def initialize(schema: StructType): Unit = ()
    override def initialize(schema: StructType, partitionColumns: Seq[String]): Unit = ()
    override def addFiles(addActions: Seq[AddAction]): Long = 0L
    override def overwriteFiles(addActions: Seq[AddAction]): Long = 0L
    override def removeFile(path: String, deletionTimestamp: Long): Long = 0L
    override def listFiles(): Seq[AddAction] =
      if (throwOnList) throw new RuntimeException("Simulated failure") else files
    override def getTotalRowCount(): Long = files.flatMap(_.numRecords).sum
    override def getSchema(): Option[StructType] = None
    override def getPartitionColumns(): Seq[String] = Seq.empty
    override def isPartitioned(): Boolean = false
    override def getMetadata(): MetadataAction =
      MetadataAction("stub", None, None, FileFormat("tantivy", Map.empty), "", Seq.empty, Map.empty, None)
    override def getProtocol(): ProtocolAction = ProtocolAction(1, 1)
    override def assertTableReadable(): Unit = ()
    override def assertTableWritable(): Unit = ()
    override def upgradeProtocol(newMinReaderVersion: Int, newMinWriterVersion: Int): Unit = ()
    override def close(): Unit = ()
  }

  test("sampling mode: empty transaction log") {
    val config = OptimizedWriteConfig(targetSplitSizeBytes = GB, samplingRatio = 1.1)
    val estimator = new WriteSizeEstimator(new StubTransactionLog(), config)

    val advisory = estimator.calculateAdvisoryPartitionSize()
    advisory shouldBe (GB / 1.1).toLong
  }

  test("sampling mode: splits with too few rows") {
    val files = Seq(
      makeAddAction(size = 100 * MB, numRecords = Some(500)),   // Below minRowsForEstimation
      makeAddAction(size = 50 * MB, numRecords = Some(1000))    // Below minRowsForEstimation
    )
    val config = OptimizedWriteConfig(
      targetSplitSizeBytes = GB,
      samplingRatio = 1.1,
      minRowsForEstimation = 10000
    )
    val estimator = new WriteSizeEstimator(new StubTransactionLog(files), config)

    val advisory = estimator.calculateAdvisoryPartitionSize()
    advisory shouldBe (GB / 1.1).toLong
  }

  test("sampling mode: splits with no numRecords") {
    val files = Seq(
      makeAddAction(size = 500 * MB, numRecords = None),
      makeAddAction(size = 300 * MB, numRecords = None)
    )
    val config = OptimizedWriteConfig(targetSplitSizeBytes = GB, samplingRatio = 1.1)
    val estimator = new WriteSizeEstimator(new StubTransactionLog(files), config)

    val advisory = estimator.calculateAdvisoryPartitionSize()
    advisory shouldBe (GB / 1.1).toLong
  }

  test("history mode: qualifying splits exist") {
    val files = Seq(
      makeAddAction(size = 500 * MB, numRecords = Some(50000)),
      makeAddAction(size = 700 * MB, numRecords = Some(70000))
    )
    val config = OptimizedWriteConfig(
      targetSplitSizeBytes = GB,
      samplingRatio = 1.1,
      minRowsForEstimation = 10000
    )
    val estimator = new WriteSizeEstimator(new StubTransactionLog(files), config)

    val advisory = estimator.calculateAdvisoryPartitionSize()
    // History mode returns targetSplitSize directly
    advisory shouldBe GB
  }

  test("history mode: mixed qualifying and non-qualifying splits") {
    val files = Seq(
      makeAddAction(size = 500 * MB, numRecords = Some(50000)),   // Qualifying
      makeAddAction(size = 10 * MB, numRecords = Some(500)),       // Too few rows
      makeAddAction(size = 800 * MB, numRecords = Some(80000))    // Qualifying
    )
    val config = OptimizedWriteConfig(
      targetSplitSizeBytes = 2 * GB,
      samplingRatio = 1.1,
      minRowsForEstimation = 10000
    )
    val estimator = new WriteSizeEstimator(new StubTransactionLog(files), config)

    val advisory = estimator.calculateAdvisoryPartitionSize()
    // Only qualifying splits used, returns targetSplitSize
    advisory shouldBe (2 * GB)
  }

  test("sampling mode with custom ratio") {
    val config = OptimizedWriteConfig(
      targetSplitSizeBytes = 2 * GB,
      samplingRatio = 2.0
    )
    val estimator = new WriteSizeEstimator(new StubTransactionLog(), config)

    val advisory = estimator.calculateAdvisoryPartitionSize()
    advisory shouldBe GB // 2GB / 2.0 = 1GB
  }

  test("falls back to sampling on listFiles exception") {
    val config = OptimizedWriteConfig(targetSplitSizeBytes = GB, samplingRatio = 1.1)
    val estimator = new WriteSizeEstimator(new StubTransactionLog(throwOnList = true), config)

    val advisory = estimator.calculateAdvisoryPartitionSize()
    advisory shouldBe (GB / 1.1).toLong
  }

  test("history mode with single qualifying split") {
    val files = Seq(
      makeAddAction(size = 200 * MB, numRecords = Some(20000))
    )
    val config = OptimizedWriteConfig(
      targetSplitSizeBytes = GB,
      minRowsForEstimation = 10000
    )
    val estimator = new WriteSizeEstimator(new StubTransactionLog(files), config)

    val advisory = estimator.calculateAdvisoryPartitionSize()
    advisory shouldBe GB
  }

  test("minRowsForEstimation boundary: exactly at threshold qualifies for history mode") {
    val files = Seq(
      makeAddAction(size = 500 * MB, numRecords = Some(10000)) // Exactly at threshold
    )
    val config = OptimizedWriteConfig(
      targetSplitSizeBytes = GB,
      samplingRatio = 1.1,
      minRowsForEstimation = 10000
    )
    val estimator = new WriteSizeEstimator(new StubTransactionLog(files), config)

    // Should use history mode (returns targetSplitSize)
    estimator.calculateAdvisoryPartitionSize() shouldBe GB
  }

  test("minRowsForEstimation boundary: one below threshold falls back to sampling mode") {
    val files = Seq(
      makeAddAction(size = 500 * MB, numRecords = Some(9999)) // Just below threshold
    )
    val config = OptimizedWriteConfig(
      targetSplitSizeBytes = GB,
      samplingRatio = 1.1,
      minRowsForEstimation = 10000
    )
    val estimator = new WriteSizeEstimator(new StubTransactionLog(files), config)

    // Should use sampling mode (returns targetSplitSize / samplingRatio)
    estimator.calculateAdvisoryPartitionSize() shouldBe (GB / 1.1).toLong
  }

  test("history mode vs sampling mode produce different values") {
    val files = Seq(
      makeAddAction(size = 500 * MB, numRecords = Some(50000))
    )
    val config = OptimizedWriteConfig(
      targetSplitSizeBytes = GB,
      samplingRatio = 1.1,
      minRowsForEstimation = 10000
    )

    // History mode (with qualifying splits)
    val historyAdvisory = new WriteSizeEstimator(new StubTransactionLog(files), config)
      .calculateAdvisoryPartitionSize()

    // Sampling mode (empty tx log)
    val samplingAdvisory = new WriteSizeEstimator(new StubTransactionLog(), config)
      .calculateAdvisoryPartitionSize()

    // History mode = targetSplitSize = 1GB
    historyAdvisory shouldBe GB
    // Sampling mode = targetSplitSize / ratio ≈ 930MB
    samplingAdvisory shouldBe (GB / 1.1).toLong

    // They should differ
    historyAdvisory should not equal samplingAdvisory
    // History mode should be larger (since ratio > 1.0)
    historyAdvisory should be > samplingAdvisory
  }

  test("different targetSplitSizeBytes produce proportionally different advisories") {
    // Use ratio=2.0 to avoid integer division rounding differences
    val config1G = OptimizedWriteConfig(targetSplitSizeBytes = GB, samplingRatio = 2.0)
    val config2G = OptimizedWriteConfig(targetSplitSizeBytes = 2 * GB, samplingRatio = 2.0)

    val advisory1G = new WriteSizeEstimator(new StubTransactionLog(), config1G)
      .calculateAdvisoryPartitionSize()
    val advisory2G = new WriteSizeEstimator(new StubTransactionLog(), config2G)
      .calculateAdvisoryPartitionSize()

    // 1G/2.0 = 512M, 2G/2.0 = 1G — exactly 2x
    advisory1G shouldBe (GB / 2)
    advisory2G shouldBe GB
    advisory2G shouldBe (2 * advisory1G)
  }

  // ===== calculateBytesPerRow tests =====

  test("calculateBytesPerRow returns None for empty transaction log") {
    val config = OptimizedWriteConfig(targetSplitSizeBytes = GB, minRowsForEstimation = 10000)
    val estimator = new WriteSizeEstimator(new StubTransactionLog(), config)

    estimator.calculateBytesPerRow() shouldBe None
  }

  test("calculateBytesPerRow returns None when splits below threshold") {
    val files = Seq(
      makeAddAction(size = 100 * MB, numRecords = Some(500))
    )
    val config = OptimizedWriteConfig(targetSplitSizeBytes = GB, minRowsForEstimation = 10000)
    val estimator = new WriteSizeEstimator(new StubTransactionLog(files), config)

    estimator.calculateBytesPerRow() shouldBe None
  }

  test("calculateBytesPerRow returns correct value from history") {
    val files = Seq(
      makeAddAction(size = 500 * MB, numRecords = Some(50000)),
      makeAddAction(size = 700 * MB, numRecords = Some(70000))
    )
    val config = OptimizedWriteConfig(targetSplitSizeBytes = GB, minRowsForEstimation = 10000)
    val estimator = new WriteSizeEstimator(new StubTransactionLog(files), config)

    val result = estimator.calculateBytesPerRow()
    result shouldBe defined
    // (500MB + 700MB) / (50000 + 70000) = 1200MB / 120000 = 10485.76 bytes/row
    val expected = (500.0 * MB + 700.0 * MB) / (50000 + 70000)
    result.get shouldBe expected +- 0.01
  }

  test("calculateBytesPerRow returns None on listFiles exception") {
    val config = OptimizedWriteConfig(targetSplitSizeBytes = GB)
    val estimator = new WriteSizeEstimator(new StubTransactionLog(throwOnList = true), config)

    estimator.calculateBytesPerRow() shouldBe None
  }

  // ===== calculateMaxRowsPerSplit tests =====

  test("calculateMaxRowsPerSplit returns None without history") {
    val config = OptimizedWriteConfig(
      targetSplitSizeBytes = GB,
      maxSplitSizeBytes = 4 * GB,
      minRowsForEstimation = 10000
    )
    val estimator = new WriteSizeEstimator(new StubTransactionLog(), config)

    estimator.calculateMaxRowsPerSplit() shouldBe None
  }

  test("calculateMaxRowsPerSplit returns correct value with history") {
    // 10KB per row (100MB / 10000 rows)
    val files = Seq(
      makeAddAction(size = 100 * MB, numRecords = Some(10000))
    )
    val config = OptimizedWriteConfig(
      targetSplitSizeBytes = GB,
      maxSplitSizeBytes = 4 * GB,
      minRowsForEstimation = 10000
    )
    val estimator = new WriteSizeEstimator(new StubTransactionLog(files), config)

    val result = estimator.calculateMaxRowsPerSplit()
    result shouldBe defined
    // bytesPerRow = 100MB / 10000 = 10485.76
    // maxRows = 4GB / 10485.76 = 409600
    val bytesPerRow = (100.0 * MB) / 10000
    val expected = (4.0 * GB / bytesPerRow).toLong
    result.get shouldBe expected
  }

  test("calculateMaxRowsPerSplit returns at least 1") {
    // Very large bytes per row (1GB per row) with small max split size
    val files = Seq(
      makeAddAction(size = 10 * GB, numRecords = Some(10000)) // 1MB per row
    )
    val config = OptimizedWriteConfig(
      targetSplitSizeBytes = GB,
      maxSplitSizeBytes = 100, // Very small max split size (100 bytes)
      minRowsForEstimation = 10000
    )
    val estimator = new WriteSizeEstimator(new StubTransactionLog(files), config)

    val result = estimator.calculateMaxRowsPerSplit()
    result shouldBe defined
    result.get shouldBe 1L // Clamped to minimum of 1
  }

  test("calculateMaxRowsPerSplit returns None on listFiles exception") {
    val config = OptimizedWriteConfig(
      targetSplitSizeBytes = GB,
      maxSplitSizeBytes = 4 * GB
    )
    val estimator = new WriteSizeEstimator(new StubTransactionLog(throwOnList = true), config)

    estimator.calculateMaxRowsPerSplit() shouldBe None
  }

  test("calculateAdvisoryPartitionSize unchanged behavior after refactor") {
    // Verify the refactored method produces identical results
    val files = Seq(
      makeAddAction(size = 500 * MB, numRecords = Some(50000))
    )
    val config = OptimizedWriteConfig(
      targetSplitSizeBytes = GB,
      samplingRatio = 1.1,
      minRowsForEstimation = 10000
    )

    // History mode should still return targetSplitSize
    val historyEstimator = new WriteSizeEstimator(new StubTransactionLog(files), config)
    historyEstimator.calculateAdvisoryPartitionSize() shouldBe GB

    // Sampling mode should still return targetSplitSize / ratio
    val samplingEstimator = new WriteSizeEstimator(new StubTransactionLog(), config)
    samplingEstimator.calculateAdvisoryPartitionSize() shouldBe (GB / 1.1).toLong
  }
}
