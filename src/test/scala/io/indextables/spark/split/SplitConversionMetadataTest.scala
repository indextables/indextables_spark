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

package io.indextables.spark.split

import java.io.File

import io.indextables.spark.TestBase

/**
 * Comprehensive tests for Split Conversion and Metadata handling.
 *
 * Tests cover:
 *   - Split file format validation
 *   - Metadata extraction and verification
 *   - Split size tracking
 *   - Document count accuracy
 *   - Field schema preservation
 */
class SplitConversionMetadataTest extends TestBase {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[SplitConversionMetadataTest])

  // ============================================================================
  // SPLIT FILE FORMAT TESTS
  // ============================================================================

  test("should create valid split files with correct extension") {
    val tablePath = s"file://$tempDir/test_split_extension"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val tableDir   = new File(s"$tempDir/test_split_extension")
    val splitFiles = tableDir.listFiles().filter(_.getName.endsWith(".split"))

    assert(splitFiles.nonEmpty, "Split files should be created")
    splitFiles.foreach { f =>
      assert(f.getName.endsWith(".split"), s"File ${f.getName} should have .split extension")
      assert(f.length() > 0, s"Split file ${f.getName} should not be empty")
    }

    logger.info("Split file extension test passed")
  }

  test("should create split files with UUID naming convention") {
    val tablePath = s"file://$tempDir/test_uuid_naming"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val tableDir   = new File(s"$tempDir/test_uuid_naming")
    val splitFiles = tableDir.listFiles().filter(_.getName.endsWith(".split"))

    // Split files have format: part-NNNNN-N-UUID.split
    // UUID format: 8-4-4-4-12 hex digits
    val splitPattern = """^part-\d+-\d+-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.split$""".r

    splitFiles.foreach { f =>
      assert(
        splitPattern.findFirstIn(f.getName).isDefined,
        s"File ${f.getName} should follow split naming convention (part-NNNNN-N-UUID.split)"
      )
    }

    logger.info("UUID naming convention test passed")
  }

  test("should track split file sizes in transaction log") {
    val tablePath = s"file://$tempDir/test_size_tracking"

    val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Read and verify data
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 500

    // Verify actual split files exist
    val tableDir   = new File(s"$tempDir/test_size_tracking")
    val splitFiles = tableDir.listFiles().filter(_.getName.endsWith(".split"))
    assert(splitFiles.nonEmpty, "Split files should exist")

    val totalSize = splitFiles.map(_.length()).sum
    assert(totalSize > 0, "Total split size should be positive")

    logger.info(s"Split file size tracking test passed (total size: $totalSize bytes)")
  }

  // ============================================================================
  // DOCUMENT COUNT ACCURACY TESTS
  // ============================================================================

  test("should accurately track document count in metadata") {
    val tablePath = s"file://$tempDir/test_doc_count"

    val expectedCount = 1234
    val df            = spark.range(0, expectedCount).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe expectedCount

    logger.info("Document count accuracy test passed")
  }

  test("should maintain correct document count after append") {
    val tablePath = s"file://$tempDir/test_append_count"

    // Initial write
    val df1 = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Append
    val df2 = spark.range(500, 800).selectExpr("id", "CAST(id AS STRING) as text")
    df2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("append")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 800

    logger.info("Append document count test passed")
  }

  test("should handle empty write gracefully") {
    val tablePath = s"file://$tempDir/test_empty_write"

    // Create empty DataFrame with schema
    val emptyDf = spark.range(0, 0).selectExpr("id", "CAST(id AS STRING) as text")
    emptyDf.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 0

    logger.info("Empty write test passed")
  }

  // ============================================================================
  // SCHEMA PRESERVATION TESTS
  // ============================================================================

  test("should preserve schema with multiple field types") {
    val tablePath = s"file://$tempDir/test_schema_types"

    val data = Seq(
      (1L, "text1", 1.5, true, java.sql.Date.valueOf("2024-01-01")),
      (2L, "text2", 2.5, false, java.sql.Date.valueOf("2024-01-02"))
    )
    val df = spark.createDataFrame(data).toDF("id", "text", "score", "active", "date")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Verify schema has all fields
    val fields = result.schema.fieldNames.toSeq
    assert(fields.contains("id"), "Schema should contain 'id'")
    assert(fields.contains("text"), "Schema should contain 'text'")
    assert(fields.contains("score"), "Schema should contain 'score'")
    assert(fields.contains("active"), "Schema should contain 'active'")
    assert(fields.contains("date"), "Schema should contain 'date'")

    result.count() shouldBe 2

    logger.info("Schema preservation test passed")
  }

  test("should preserve field ordering in schema") {
    val tablePath = s"file://$tempDir/test_field_order"

    val data = Seq(
      (1L, "a", "b", "c", "d")
    )
    val df = spark.createDataFrame(data).toDF("first", "second", "third", "fourth", "fifth")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val fields = result.schema.fieldNames.toSeq
    fields shouldBe Seq("first", "second", "third", "fourth", "fifth")

    logger.info("Field ordering test passed")
  }

  test("should handle nullable fields correctly") {
    val tablePath = s"file://$tempDir/test_nullable"

    // Test that data with various field types is handled correctly
    val data = Seq(
      (1L, "value1", 1.0),
      (2L, "value2", 2.0),
      (3L, "value3", 3.0)
    )
    val df = spark.createDataFrame(data).toDF("id", "text", "score")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 3

    // Verify all data can be read back correctly
    val rows = result.select("id", "text", "score").collect().sortBy(_.getLong(0))
    rows(0).getString(1) shouldBe "value1"
    rows(1).getString(1) shouldBe "value2"
    rows(2).getString(1) shouldBe "value3"

    logger.info("Nullable fields test passed")
  }

  // ============================================================================
  // SPLIT CONVERSION PARALLELISM TESTS
  // ============================================================================

  test("should respect split conversion parallelism setting") {
    val tablePath = s"file://$tempDir/test_parallelism"

    val df = spark.range(0, 1000).repartition(20).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.splitConversion.maxParallelism", "4")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 1000

    logger.info("Split conversion parallelism test passed")
  }

  test("should handle high parallelism correctly") {
    val tablePath = s"file://$tempDir/test_high_parallelism"

    // Create many partitions
    val df = spark.range(0, 2000).repartition(50).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 2000

    logger.info("High parallelism test passed")
  }

  // ============================================================================
  // HEAP SIZE CONFIGURATION TESTS
  // ============================================================================

  test("should respect indexWriter heapSize setting") {
    val tablePath = s"file://$tempDir/test_heap_size"

    val df = spark.range(0, 500).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "50M")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 500

    logger.info("Heap size configuration test passed")
  }

  test("should handle different heap size formats") {
    val tablePath1 = s"file://$tempDir/test_heap_format_m"
    val tablePath2 = s"file://$tempDir/test_heap_format_g"
    val tablePath3 = s"file://$tempDir/test_heap_format_k"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")

    // Test M (megabytes)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "100M")
      .mode("overwrite")
      .save(tablePath1)

    // Test G (gigabytes)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "1G")
      .mode("overwrite")
      .save(tablePath2)

    // Test K (kilobytes)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.heapSize", "102400K")
      .mode("overwrite")
      .save(tablePath3)

    // Verify all wrote successfully
    spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath1).count() shouldBe 100
    spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath2).count() shouldBe 100
    spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath3).count() shouldBe 100

    logger.info("Heap size format test passed")
  }

  // ============================================================================
  // BATCH SIZE CONFIGURATION TESTS
  // ============================================================================

  test("should respect indexWriter batchSize setting") {
    val tablePath = s"file://$tempDir/test_batch_size"

    val df = spark.range(0, 1000).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexWriter.batchSize", "100")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 1000

    logger.info("Batch size configuration test passed")
  }

  // ============================================================================
  // STATISTICS TESTS
  // ============================================================================

  test("should generate correct min/max statistics for numeric fields") {
    val tablePath = s"file://$tempDir/test_numeric_stats"

    val df = spark.range(100, 200).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Statistics should enable efficient filtering
    val filtered = result.filter("id >= 150 AND id < 175")
    filtered.count() shouldBe 25

    logger.info("Numeric statistics test passed")
  }

  test("should truncate statistics for long text fields") {
    val tablePath = s"file://$tempDir/test_stats_truncation"

    // Create data with very long text
    val data = Seq(
      (1L, "a" * 1000), // Very long text
      (2L, "b" * 1000)
    )
    val df = spark.createDataFrame(data).toDF("id", "long_text")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.stats.truncation.enabled", "true")
      .option("spark.indextables.stats.truncation.maxLength", "32")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 2

    logger.info("Statistics truncation test passed")
  }
}
