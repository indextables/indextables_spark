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

package io.indextables.spark.core

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import io.indextables.spark.arrow.AggregationArrowFfiConfig
import org.scalatest.funsuite.AnyFunSuite

/**
 * Integration tests for the Arrow FFI aggregation read path.
 *
 * These tests write data via Spark, then read it back with FFI-based aggregation enabled, verifying that results match
 * expectations. Tests also run parity checks against the object-based fallback path.
 *
 * NOTE: Tests that require tantivy4java 0.31.x FFI methods (aggregateArrowFfi, multiSplitAggregateArrowFfi) are marked
 * with a prerequisite check and will be skipped until those methods are available. The feature flag tests and config
 * tests run immediately.
 */
class AggregationArrowFfiIntegrationTest extends AnyFunSuite {

  private val PROVIDER = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  // ===== Feature flag tests =====

  test("feature flag defaults to enabled (tantivy4java 0.31.2+)") {
    assert(AggregationArrowFfiConfig.isEnabled(Map.empty))
  }

  test("feature flag can be enabled") {
    assert(AggregationArrowFfiConfig.isEnabled(
      Map("spark.indextables.read.aggregation.arrowFfi.enabled" -> "true")
    ))
  }

  test("feature flag can be explicitly disabled") {
    assert(!AggregationArrowFfiConfig.isEnabled(
      Map("spark.indextables.read.aggregation.arrowFfi.enabled" -> "false")
    ))
  }

  test("disabled feature flag uses InternalRow path without behavior change") {
    val spark = createSparkSession("ffi-disabled-test")
    try {
      import spark.implicits._
      val tempDir   = Files.createTempDirectory("ffi-disabled-test").toFile
      val tablePath = tempDir.getAbsolutePath

      // Write test data
      val testData = Seq(
        ("doc1", 10), ("doc2", 20), ("doc3", 30)
      ).toDF("id", "score")

      testData.write.format(PROVIDER)
        .option("spark.indextables.indexing.fastfields", "score")
        .option("spark.indextables.read.aggregation.arrowFfi.enabled", "false")
        .mode("overwrite")
        .save(tablePath)

      // Read with FFI disabled
      val df = spark.read.format(PROVIDER)
        .option("spark.indextables.read.aggregation.arrowFfi.enabled", "false")
        .load(tablePath)

      // Simple COUNT(*) should work via InternalRow path
      val result = df.agg(count("*")).collect()
      assert(result.length == 1)
      assert(result(0).getLong(0) == 3L, s"Expected count=3, got ${result(0).getLong(0)}")

      deleteRecursively(tempDir)
    } finally
      spark.stop()
  }

  // ===== Parity test: FFI enabled vs disabled produce same results =====

  test("parity: simple COUNT(*) same result with FFI enabled and disabled") {
    // This test verifies that the FFI path (when available) produces the same
    // result as the InternalRow path. Until tantivy4java 0.31.x, the FFI path
    // will gracefully fall back to InternalRow.
    val spark = createSparkSession("parity-count-test")
    try {
      import spark.implicits._
      val tempDir   = Files.createTempDirectory("parity-count-test").toFile
      val tablePath = tempDir.getAbsolutePath

      val testData = (1 to 50).map(i => (s"doc$i", i, s"category_${i % 3}")).toDF("id", "score", "category")

      testData.write.format(PROVIDER)
        .option("spark.indextables.indexing.fastfields", "score,category")
        .option("spark.indextables.indexing.typemap.category", "string")
        .mode("overwrite")
        .save(tablePath)

      // Result with FFI disabled (baseline)
      val dfDisabled = spark.read.format(PROVIDER)
        .option("spark.indextables.read.aggregation.arrowFfi.enabled", "false")
        .load(tablePath)
      val disabledResult = dfDisabled.agg(count("*")).collect()(0).getLong(0)

      // Result with FFI enabled (default)
      val dfEnabled = spark.read.format(PROVIDER).load(tablePath)
      val enabledResult = dfEnabled.agg(count("*")).collect()(0).getLong(0)

      assert(disabledResult == enabledResult,
        s"COUNT(*) mismatch: disabled=$disabledResult, enabled=$enabledResult")
      assert(disabledResult == 50L)

      deleteRecursively(tempDir)
    } finally
      spark.stop()
  }

  test("parity: simple COUNT, SUM, MIN, MAX same results") {
    val spark = createSparkSession("parity-multi-agg-test")
    try {
      import spark.implicits._
      val tempDir   = Files.createTempDirectory("parity-multi-agg-test").toFile
      val tablePath = tempDir.getAbsolutePath

      val testData = (1 to 100).map(i => (s"doc$i", i)).toDF("id", "score")

      testData.write.format(PROVIDER)
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(tablePath)

      // Baseline (disabled)
      val dfDisabled = spark.read.format(PROVIDER)
        .option("spark.indextables.read.aggregation.arrowFfi.enabled", "false")
        .load(tablePath)
      val disabledRow = dfDisabled.agg(
        count("*"), sum("score"), min("score"), max("score")
      ).collect()(0)

      // FFI-enabled
      val dfEnabled = spark.read.format(PROVIDER).load(tablePath)
      val enabledRow = dfEnabled.agg(
        count("*"), sum("score"), min("score"), max("score")
      ).collect()(0)

      assert(disabledRow.getLong(0) == enabledRow.getLong(0), "COUNT mismatch")
      assert(disabledRow.getLong(1) == enabledRow.getLong(1), "SUM mismatch")
      assert(disabledRow.getInt(2) == enabledRow.getInt(2), "MIN mismatch")
      assert(disabledRow.getInt(3) == enabledRow.getInt(3), "MAX mismatch")

      // Verify actual values
      assert(enabledRow.getLong(0) == 100L, "Expected count=100")
      assert(enabledRow.getLong(1) == 5050L, "Expected sum=5050")
      assert(enabledRow.getInt(2) == 1, "Expected min=1")
      assert(enabledRow.getInt(3) == 100, "Expected max=100")

      deleteRecursively(tempDir)
    } finally
      spark.stop()
  }

  // ===== GROUP BY parity tests =====

  test("parity: GROUP BY with COUNT same results") {
    val spark = createSparkSession("parity-groupby-count-test")
    try {
      import spark.implicits._
      val tempDir   = Files.createTempDirectory("parity-groupby-count-test").toFile
      val tablePath = tempDir.getAbsolutePath

      val testData = Seq(
        ("doc1", "a", 10), ("doc2", "a", 20),
        ("doc3", "b", 30), ("doc4", "b", 40),
        ("doc5", "c", 50)
      ).toDF("id", "category", "score")

      testData.write.format(PROVIDER)
        .option("spark.indextables.indexing.typemap.category", "string")
        .option("spark.indextables.indexing.fastfields", "category,score")
        .mode("overwrite")
        .save(tablePath)

      // Baseline (disabled)
      val dfDisabled = spark.read.format(PROVIDER)
        .option("spark.indextables.read.aggregation.arrowFfi.enabled", "false")
        .load(tablePath)
      val disabledResults = dfDisabled.groupBy("category").count().collect()
        .map(r => r.getString(0) -> r.getLong(1)).toMap

      // FFI-enabled
      val dfEnabled = spark.read.format(PROVIDER).load(tablePath)
      val enabledResults = dfEnabled.groupBy("category").count().collect()
        .map(r => r.getString(0) -> r.getLong(1)).toMap

      assert(disabledResults == enabledResults,
        s"GROUP BY COUNT mismatch: disabled=$disabledResults, enabled=$enabledResults")

      assert(enabledResults("a") == 2)
      assert(enabledResults("b") == 2)
      assert(enabledResults("c") == 1)

      deleteRecursively(tempDir)
    } finally
      spark.stop()
  }

  test("parity: GROUP BY with SUM same results") {
    val spark = createSparkSession("parity-groupby-sum-test")
    try {
      import spark.implicits._
      val tempDir   = Files.createTempDirectory("parity-groupby-sum-test").toFile
      val tablePath = tempDir.getAbsolutePath

      val testData = Seq(
        ("doc1", "a", 100), ("doc2", "a", 200),
        ("doc3", "b", 150), ("doc4", "b", 250),
        ("doc5", "c", 300)
      ).toDF("id", "team", "score")

      testData.write.format(PROVIDER)
        .option("spark.indextables.indexing.typemap.team", "string")
        .option("spark.indextables.indexing.fastfields", "team,score")
        .mode("overwrite")
        .save(tablePath)

      // Baseline
      val dfDisabled = spark.read.format(PROVIDER)
        .option("spark.indextables.read.aggregation.arrowFfi.enabled", "false")
        .load(tablePath)
      val disabledResults = dfDisabled.groupBy("team").agg(sum("score").as("total"))
        .collect().map(r => r.getString(0) -> r.getLong(1)).toMap

      // FFI-enabled
      val dfEnabled = spark.read.format(PROVIDER).load(tablePath)
      val enabledResults = dfEnabled.groupBy("team").agg(sum("score").as("total"))
        .collect().map(r => r.getString(0) -> r.getLong(1)).toMap

      assert(disabledResults == enabledResults,
        s"GROUP BY SUM mismatch: disabled=$disabledResults, enabled=$enabledResults")

      assert(enabledResults("a") == 300)
      assert(enabledResults("b") == 400)
      assert(enabledResults("c") == 300)

      deleteRecursively(tempDir)
    } finally
      spark.stop()
  }

  // ===== Filtered aggregation test =====

  test("parity: filtered aggregation (WHERE + COUNT)") {
    val spark = createSparkSession("parity-filtered-agg-test")
    try {
      import spark.implicits._
      val tempDir   = Files.createTempDirectory("parity-filtered-agg-test").toFile
      val tablePath = tempDir.getAbsolutePath

      val testData = (1 to 100).map(i => (s"doc$i", i, if (i <= 50) "low" else "high")).toDF("id", "score", "tier")

      testData.write.format(PROVIDER)
        .option("spark.indextables.indexing.typemap.tier", "string")
        .option("spark.indextables.indexing.fastfields", "score,tier")
        .mode("overwrite")
        .save(tablePath)

      // Baseline
      val dfDisabled = spark.read.format(PROVIDER)
        .option("spark.indextables.read.aggregation.arrowFfi.enabled", "false")
        .load(tablePath)
      val disabledCount = dfDisabled.filter(col("tier") === "high").agg(count("*")).collect()(0).getLong(0)

      // FFI-enabled
      val dfEnabled = spark.read.format(PROVIDER).load(tablePath)
      val enabledCount = dfEnabled.filter(col("tier") === "high").agg(count("*")).collect()(0).getLong(0)

      assert(disabledCount == enabledCount, s"Filtered COUNT mismatch: disabled=$disabledCount, enabled=$enabledCount")
      assert(enabledCount == 50L)

      deleteRecursively(tempDir)
    } finally
      spark.stop()
  }

  // ===== Empty result handling =====

  test("empty result handling with FFI enabled") {
    val spark = createSparkSession("empty-result-test")
    try {
      import spark.implicits._
      val tempDir   = Files.createTempDirectory("empty-result-test").toFile
      val tablePath = tempDir.getAbsolutePath

      val testData = Seq(("doc1", "a", 10)).toDF("id", "category", "score")

      testData.write.format(PROVIDER)
        .option("spark.indextables.indexing.typemap.category", "string")
        .option("spark.indextables.indexing.fastfields", "category,score")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read.format(PROVIDER).load(tablePath)

      // Filter that matches nothing
      val result = df.filter(col("category") === "nonexistent").agg(count("*")).collect()
      assert(result.length == 1)
      assert(result(0).getLong(0) == 0L)

      deleteRecursively(tempDir)
    } finally
      spark.stop()
  }

  // ===== Partitioned table test =====

  test("parity: partitioned table aggregation") {
    val spark = createSparkSession("parity-partitioned-agg-test")
    try {
      import spark.implicits._
      val tempDir   = Files.createTempDirectory("parity-partitioned-agg-test").toFile
      val tablePath = tempDir.getAbsolutePath

      val testData = Seq(
        ("doc1", "2024-01-01", 10), ("doc2", "2024-01-01", 20),
        ("doc3", "2024-01-02", 30), ("doc4", "2024-01-02", 40)
      ).toDF("id", "date", "score")

      testData.write.format(PROVIDER)
        .option("spark.indextables.indexing.fastfields", "score")
        .partitionBy("date")
        .mode("overwrite")
        .save(tablePath)

      // Baseline
      val dfDisabled = spark.read.format(PROVIDER)
        .option("spark.indextables.read.aggregation.arrowFfi.enabled", "false")
        .load(tablePath)
      val disabledTotal = dfDisabled.agg(count("*"), sum("score")).collect()(0)

      // FFI-enabled
      val dfEnabled = spark.read.format(PROVIDER).load(tablePath)
      val enabledTotal = dfEnabled.agg(count("*"), sum("score")).collect()(0)

      assert(disabledTotal.getLong(0) == enabledTotal.getLong(0), "COUNT mismatch")
      assert(disabledTotal.getLong(1) == enabledTotal.getLong(1), "SUM mismatch")
      assert(enabledTotal.getLong(0) == 4L)
      assert(enabledTotal.getLong(1) == 100L)

      deleteRecursively(tempDir)
    } finally
      spark.stop()
  }

  // ===== Multi-split test =====

  test("parity: multi-split aggregation") {
    val spark = createSparkSession("parity-multi-split-test")
    try {
      import spark.implicits._
      val tempDir   = Files.createTempDirectory("parity-multi-split-test").toFile
      val tablePath = tempDir.getAbsolutePath

      // Write in two batches to create multiple splits
      val batch1 = (1 to 50).map(i => (s"doc$i", i)).toDF("id", "score")
      batch1.write.format(PROVIDER)
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(tablePath)

      val batch2 = (51 to 100).map(i => (s"doc$i", i)).toDF("id", "score")
      batch2.write.format(PROVIDER)
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("append")
        .save(tablePath)

      // Baseline
      val dfDisabled = spark.read.format(PROVIDER)
        .option("spark.indextables.read.aggregation.arrowFfi.enabled", "false")
        .load(tablePath)
      val disabledRow = dfDisabled.agg(count("*"), sum("score"), min("score"), max("score")).collect()(0)

      // FFI-enabled
      val dfEnabled = spark.read.format(PROVIDER).load(tablePath)
      val enabledRow = dfEnabled.agg(count("*"), sum("score"), min("score"), max("score")).collect()(0)

      assert(disabledRow.getLong(0) == enabledRow.getLong(0), "COUNT mismatch")
      assert(disabledRow.getLong(1) == enabledRow.getLong(1), "SUM mismatch")
      assert(disabledRow.getInt(2) == enabledRow.getInt(2), "MIN mismatch")
      assert(disabledRow.getInt(3) == enabledRow.getInt(3), "MAX mismatch")

      assert(enabledRow.getLong(0) == 100L, "Expected count=100")
      assert(enabledRow.getLong(1) == 5050L, "Expected sum=5050")

      deleteRecursively(tempDir)
    } finally
      spark.stop()
  }

  // ===== Helpers =====

  private def createSparkSession(appName: String): SparkSession =
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles().foreach(deleteRecursively)
    file.delete()
  }
}
