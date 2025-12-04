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

package io.indextables.spark.filter

import io.indextables.spark.TestBase

/**
 * Comprehensive tests for Filter Pushdown edge cases.
 *
 * Tests cover:
 * - NULL handling in filters
 * - Unicode and special characters
 * - Boundary values for numeric types
 * - Complex filter combinations
 * - Non-pushable filter fallback
 */
class FilterPushdownEdgeCaseTest extends TestBase {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[FilterPushdownEdgeCaseTest])

  // ============================================================================
  // NULL HANDLING TESTS
  // Note: IS NULL / IS NOT NULL filters have limited support due to how
  // tantivy stores null values. These tests verify basic non-null filtering.
  // ============================================================================

  test("should handle non-null value filtering") {
    val tablePath = s"file://$tempDir/test_non_null"

    val data = Seq(
      (1L, "value1"),
      (2L, "value2"),
      (3L, "value1"),
      (4L, "value3")
    )
    val df = spark.createDataFrame(data).toDF("id", "text")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val filtered = result.filter("text = 'value1'")
    filtered.count() shouldBe 2

    logger.info("Non-null filtering test passed")
  }

  // ============================================================================
  // UNICODE AND SPECIAL CHARACTER TESTS
  // ============================================================================

  test("should handle unicode characters in filter") {
    val tablePath = s"file://$tempDir/test_unicode"

    val data = Seq(
      (1L, "日本語テキスト"),
      (2L, "中文文本"),
      (3L, "한국어 텍스트"),
      (4L, "Ελληνικά")
    )
    val df = spark.createDataFrame(data).toDF("id", "text")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val japanese = result.filter("text = '日本語テキスト'")
    japanese.count() shouldBe 1

    val chinese = result.filter("text = '中文文本'")
    chinese.count() shouldBe 1

    logger.info("Unicode filter test passed")
  }

  test("should handle special characters in filter") {
    val tablePath = s"file://$tempDir/test_special_chars"

    val data = Seq(
      (1L, "test@email.com"),
      (2L, "path/to/file"),
      (3L, "value with spaces"),
      (4L, "quote'test"),
      (5L, "backslash\\test")
    )
    val df = spark.createDataFrame(data).toDF("id", "text")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.filter("text = 'test@email.com'").count() shouldBe 1
    result.filter("text = 'path/to/file'").count() shouldBe 1
    result.filter("text = 'value with spaces'").count() shouldBe 1

    logger.info("Special characters filter test passed")
  }

  test("should handle empty string in filter") {
    val tablePath = s"file://$tempDir/test_empty_string"

    val data = Seq(
      (1L, ""),
      (2L, "non-empty"),
      (3L, "")
    )
    val df = spark.createDataFrame(data).toDF("id", "text")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val emptyRows = result.filter("text = ''")
    emptyRows.count() shouldBe 2

    logger.info("Empty string filter test passed")
  }

  // ============================================================================
  // NUMERIC BOUNDARY VALUE TESTS
  // ============================================================================

  test("should handle Long boundary values") {
    val tablePath = s"file://$tempDir/test_long_boundaries"

    val data = Seq(
      (Long.MinValue, "min"),
      (Long.MaxValue, "max"),
      (0L, "zero"),
      (-1L, "negative")
    )
    val df = spark.createDataFrame(data).toDF("id", "text")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.filter(s"id = ${Long.MinValue}").count() shouldBe 1
    result.filter(s"id = ${Long.MaxValue}").count() shouldBe 1
    result.filter("id = 0").count() shouldBe 1
    result.filter("id < 0").count() shouldBe 2 // MinValue and -1

    logger.info("Long boundary values test passed")
  }

  test("should handle Double boundary values") {
    val tablePath = s"file://$tempDir/test_double_boundaries"

    val data = Seq(
      (1L, Double.MinValue),
      (2L, Double.MaxValue),
      (3L, 0.0),
      (4L, Double.NaN),
      (5L, Double.PositiveInfinity),
      (6L, Double.NegativeInfinity)
    )
    val df = spark.createDataFrame(data).toDF("id", "value")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.filter("value = 0.0").count() shouldBe 1
    result.filter("value > 0").limit(10).count() should be >= 1L

    logger.info("Double boundary values test passed")
  }

  // ============================================================================
  // COMPLEX FILTER COMBINATION TESTS
  // ============================================================================

  test("should handle AND filters correctly") {
    val tablePath = s"file://$tempDir/test_and_filter"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id % 10 AS STRING) as category")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // id >= 50 AND id < 60 should give 10 rows
    val filtered = result.filter("id >= 50 AND id < 60")
    filtered.count() shouldBe 10

    logger.info("AND filter test passed")
  }

  test("should handle OR filters correctly") {
    val tablePath = s"file://$tempDir/test_or_filter"

    val data = Seq(
      (1L, "A"),
      (2L, "B"),
      (3L, "C"),
      (4L, "A"),
      (5L, "B")
    )
    val df = spark.createDataFrame(data).toDF("id", "category")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val filtered = result.filter("category = 'A' OR category = 'B'")
    filtered.count() shouldBe 4

    logger.info("OR filter test passed")
  }

  test("should handle NOT filter correctly") {
    val tablePath = s"file://$tempDir/test_not_filter"

    val data = Seq(
      (1L, "include"),
      (2L, "exclude"),
      (3L, "include"),
      (4L, "exclude")
    )
    val df = spark.createDataFrame(data).toDF("id", "status")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val filtered = result.filter("NOT status = 'exclude'")
    filtered.count() shouldBe 2

    logger.info("NOT filter test passed")
  }

  test("should handle nested boolean logic") {
    val tablePath = s"file://$tempDir/test_nested_logic"

    val data = Seq(
      (1L, "A", 100),
      (2L, "B", 200),
      (3L, "A", 300),
      (4L, "B", 100),
      (5L, "C", 150)
    )
    val df = spark.createDataFrame(data).toDF("id", "category", "value")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // (category = 'A' AND value > 150) OR (category = 'B' AND value < 150)
    val filtered = result.filter("(category = 'A' AND value > 150) OR (category = 'B' AND value < 150)")
    filtered.count() shouldBe 2 // id=3 and id=4

    logger.info("Nested boolean logic test passed")
  }

  // ============================================================================
  // RANGE FILTER TESTS
  // ============================================================================

  test("should handle inclusive range filters") {
    val tablePath = s"file://$tempDir/test_inclusive_range"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // id >= 10 AND id <= 20 should give 11 rows (inclusive both ends)
    val filtered = result.filter("id >= 10 AND id <= 20")
    filtered.count() shouldBe 11

    logger.info("Inclusive range filter test passed")
  }

  test("should handle exclusive range filters") {
    val tablePath = s"file://$tempDir/test_exclusive_range"

    val df = spark.range(0, 100).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // id > 10 AND id < 20 should give 9 rows (exclusive both ends)
    val filtered = result.filter("id > 10 AND id < 20")
    filtered.count() shouldBe 9

    logger.info("Exclusive range filter test passed")
  }

  // ============================================================================
  // IN FILTER TESTS
  // ============================================================================

  test("should handle IN filter with multiple values") {
    val tablePath = s"file://$tempDir/test_in_filter"

    val data = Seq(
      (1L, "red"),
      (2L, "blue"),
      (3L, "green"),
      (4L, "yellow"),
      (5L, "red")
    )
    val df = spark.createDataFrame(data).toDF("id", "color")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val filtered = result.filter("color IN ('red', 'blue')")
    filtered.count() shouldBe 3

    logger.info("IN filter test passed")
  }

  test("should handle NOT IN filter") {
    val tablePath = s"file://$tempDir/test_not_in_filter"

    val data = Seq(
      (1L, "red"),
      (2L, "blue"),
      (3L, "green"),
      (4L, "yellow"),
      (5L, "red")
    )
    val df = spark.createDataFrame(data).toDF("id", "color")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val filtered = result.filter("color NOT IN ('red', 'blue')")
    filtered.count() shouldBe 2 // green and yellow

    logger.info("NOT IN filter test passed")
  }

  // ============================================================================
  // DATE AND TIMESTAMP FILTER TESTS
  // ============================================================================

  test("should handle date equality filter") {
    val tablePath = s"file://$tempDir/test_date_equality"

    val data = Seq(
      (1L, java.sql.Date.valueOf("2024-01-15")),
      (2L, java.sql.Date.valueOf("2024-01-16")),
      (3L, java.sql.Date.valueOf("2024-01-15"))
    )
    val df = spark.createDataFrame(data).toDF("id", "event_date")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val filtered = result.filter("event_date = '2024-01-15'")
    filtered.count() shouldBe 2

    logger.info("Date equality filter test passed")
  }

  test("should handle date range filter") {
    val tablePath = s"file://$tempDir/test_date_range"

    val data = Seq(
      (1L, java.sql.Date.valueOf("2024-01-10")),
      (2L, java.sql.Date.valueOf("2024-01-15")),
      (3L, java.sql.Date.valueOf("2024-01-20")),
      (4L, java.sql.Date.valueOf("2024-01-25"))
    )
    val df = spark.createDataFrame(data).toDF("id", "event_date")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val filtered = result.filter("event_date >= '2024-01-15' AND event_date < '2024-01-25'")
    filtered.count() shouldBe 2 // Jan 15 and Jan 20

    logger.info("Date range filter test passed")
  }

  // ============================================================================
  // NON-PUSHABLE FILTER FALLBACK TESTS
  // ============================================================================

  test("should fall back to Spark for LIKE filter") {
    val tablePath = s"file://$tempDir/test_like_fallback"

    val data = Seq(
      (1L, "hello world"),
      (2L, "hello there"),
      (3L, "goodbye world")
    )
    val df = spark.createDataFrame(data).toDF("id", "text")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // LIKE may not be pushed down, but should still work
    val filtered = result.filter("text LIKE 'hello%'")
    filtered.count() shouldBe 2

    logger.info("LIKE filter fallback test passed")
  }

  test("should handle mixed pushable and non-pushable filters") {
    val tablePath = s"file://$tempDir/test_mixed_filters"

    val data = Seq(
      (1L, "hello world", 100),
      (2L, "hello there", 200),
      (3L, "goodbye world", 300)
    )
    val df = spark.createDataFrame(data).toDF("id", "text", "value")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Combine pushable (value > 150) with non-pushable (LIKE)
    // Use limit().collect().length instead of count() since StringStartsWith is unsupported
    val filtered = result.filter("value > 150 AND text LIKE 'hello%'")
    filtered.limit(1000).collect().length shouldBe 1 // id=2

    logger.info("Mixed filters test passed")
  }
}
