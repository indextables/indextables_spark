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

package io.indextables.spark.util

import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for data skipping statistics configuration (Delta Lake compatible).
 *
 * Tests verify:
 *   - Default behavior (first 32 eligible columns)
 *   - Explicit column list via dataSkippingStatsColumns
 *   - Custom numIndexedCols (-1 for all, 0 to disable, positive for limit)
 *   - Eligibility filtering (only primitive types with ordering)
 */
class DataSkippingStatsConfigTest extends AnyFunSuite with Matchers {

  // Test schema with various field types
  val testSchema: StructType = StructType(
    Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("score", DoubleType, nullable = true),
      StructField("created_at", TimestampType, nullable = true),
      StructField("is_active", BooleanType, nullable = true),
      StructField("event_date", DateType, nullable = true),
      StructField("count", LongType, nullable = true),
      StructField("rating", FloatType, nullable = true),
      // Non-eligible types
      StructField("metadata", MapType(StringType, StringType), nullable = true),
      StructField("tags", ArrayType(StringType), nullable = true),
      StructField("details", StructType(Seq(StructField("x", IntegerType))), nullable = true),
      StructField("data", BinaryType, nullable = true)
    )
  )

  // Large schema with 50 eligible columns for testing numIndexedCols
  val largeSchema: StructType = StructType(
    (1 to 50).map(i => StructField(s"col_$i", IntegerType, nullable = true))
  )

  test("isEligibleForStats should return true for supported types") {
    StatisticsCalculator.isEligibleForStats(StructField("f", IntegerType)) shouldBe true
    StatisticsCalculator.isEligibleForStats(StructField("f", LongType)) shouldBe true
    StatisticsCalculator.isEligibleForStats(StructField("f", FloatType)) shouldBe true
    StatisticsCalculator.isEligibleForStats(StructField("f", DoubleType)) shouldBe true
    StatisticsCalculator.isEligibleForStats(StructField("f", BooleanType)) shouldBe true
    StatisticsCalculator.isEligibleForStats(StructField("f", StringType)) shouldBe true
    StatisticsCalculator.isEligibleForStats(StructField("f", DateType)) shouldBe true
    StatisticsCalculator.isEligibleForStats(StructField("f", TimestampType)) shouldBe true
  }

  test("isEligibleForStats should return false for unsupported types") {
    StatisticsCalculator.isEligibleForStats(StructField("f", BinaryType)) shouldBe false
    StatisticsCalculator.isEligibleForStats(StructField("f", ArrayType(StringType))) shouldBe false
    StatisticsCalculator.isEligibleForStats(StructField("f", MapType(StringType, StringType))) shouldBe false
    StatisticsCalculator.isEligibleForStats(StructField("f", StructType(Seq()))) shouldBe false
  }

  test("getStatsEligibleFields should use default numIndexedCols (32) when no config") {
    val config   = Map.empty[String, String]
    val eligible = StatisticsCalculator.getStatsEligibleFields(largeSchema, config)

    // Should be limited to first 32 columns
    eligible.length shouldBe 32
    eligible.head._1.name shouldBe "col_1"
    eligible.last._1.name shouldBe "col_32"
  }

  test("getStatsEligibleFields should filter out non-eligible types") {
    val config   = Map("spark.indextables.dataSkippingNumIndexedCols" -> "-1")
    val eligible = StatisticsCalculator.getStatsEligibleFields(testSchema, config)

    // Should only include the 8 eligible types, not the 4 complex types
    eligible.length shouldBe 8
    val eligibleNames = eligible.map(_._1.name).toSet
    eligibleNames should contain allOf ("id", "name", "score", "created_at", "is_active", "event_date", "count", "rating")
    eligibleNames should not contain "metadata"
    eligibleNames should not contain "tags"
    eligibleNames should not contain "details"
    eligibleNames should not contain "data"
  }

  test("getStatsEligibleFields should use explicit dataSkippingStatsColumns when set") {
    val config = Map(
      "spark.indextables.dataSkippingStatsColumns"   -> "id,score,event_date",
      "spark.indextables.dataSkippingNumIndexedCols" -> "2" // Should be ignored
    )
    val eligible = StatisticsCalculator.getStatsEligibleFields(testSchema, config)

    // Should use explicit columns, ignoring numIndexedCols
    eligible.length shouldBe 3
    val eligibleNames = eligible.map(_._1.name).toSet
    eligibleNames shouldBe Set("id", "score", "event_date")
  }

  test("getStatsEligibleFields should handle numIndexedCols = -1 (all columns)") {
    val config   = Map("spark.indextables.dataSkippingNumIndexedCols" -> "-1")
    val eligible = StatisticsCalculator.getStatsEligibleFields(largeSchema, config)

    // Should include all 50 columns
    eligible.length shouldBe 50
  }

  test("getStatsEligibleFields should handle numIndexedCols = 0 (disabled)") {
    val config   = Map("spark.indextables.dataSkippingNumIndexedCols" -> "0")
    val eligible = StatisticsCalculator.getStatsEligibleFields(largeSchema, config)

    // Should be empty
    eligible.length shouldBe 0
  }

  test("getStatsEligibleFields should handle custom numIndexedCols") {
    val config   = Map("spark.indextables.dataSkippingNumIndexedCols" -> "10")
    val eligible = StatisticsCalculator.getStatsEligibleFields(largeSchema, config)

    // Should be limited to first 10 columns
    eligible.length shouldBe 10
    eligible.head._1.name shouldBe "col_1"
    eligible.last._1.name shouldBe "col_10"
  }

  test("getStatsEligibleFields should filter explicit columns by eligibility") {
    // Request a mix of eligible and non-eligible columns
    val config = Map(
      "spark.indextables.dataSkippingStatsColumns" -> "id,metadata,score,tags"
    )
    val eligible = StatisticsCalculator.getStatsEligibleFields(testSchema, config)

    // Should only include the eligible columns from the list
    eligible.length shouldBe 2
    val eligibleNames = eligible.map(_._1.name).toSet
    eligibleNames shouldBe Set("id", "score")
  }

  test("getStatsEligibleFields should preserve original field indices") {
    val config   = Map("spark.indextables.dataSkippingStatsColumns" -> "score,event_date")
    val eligible = StatisticsCalculator.getStatsEligibleFields(testSchema, config)

    // Verify the indices match the original schema positions
    val scoreField = eligible.find(_._1.name == "score").get
    scoreField._2 shouldBe 2 // score is at index 2 in testSchema

    val eventDateField = eligible.find(_._1.name == "event_date").get
    eventDateField._2 shouldBe 5 // event_date is at index 5 in testSchema
  }

  test("DatasetStatistics should only track configured columns") {
    val config = Map("spark.indextables.dataSkippingStatsColumns" -> "id,score")
    val stats  = new StatisticsCalculator.DatasetStatistics(testSchema, config)

    stats.getTrackedColumns shouldBe Set("id", "score")
  }

  test("DatasetStatistics should use default of 32 columns when schema is small") {
    val config = Map.empty[String, String]
    val stats  = new StatisticsCalculator.DatasetStatistics(testSchema, config)

    // All 8 eligible columns should be tracked (less than 32)
    stats.getTrackedColumns.size shouldBe 8
  }

  test("ConfigUtils.getDataSkippingStatsColumns should parse comma-separated list") {
    val config = Map("spark.indextables.dataSkippingStatsColumns" -> "col1, col2, col3")
    val result = ConfigUtils.getDataSkippingStatsColumns(config)

    result shouldBe defined
    result.get shouldBe Set("col1", "col2", "col3")
  }

  test("ConfigUtils.getDataSkippingStatsColumns should handle whitespace") {
    val config = Map("spark.indextables.dataSkippingStatsColumns" -> "  col1  ,  col2  ,  col3  ")
    val result = ConfigUtils.getDataSkippingStatsColumns(config)

    result shouldBe defined
    result.get shouldBe Set("col1", "col2", "col3")
  }

  test("ConfigUtils.getDataSkippingStatsColumns should return None for empty value") {
    val config = Map("spark.indextables.dataSkippingStatsColumns" -> "")
    val result = ConfigUtils.getDataSkippingStatsColumns(config)

    result shouldBe None
  }

  test("ConfigUtils.getDataSkippingStatsColumns should return None when not set") {
    val config = Map.empty[String, String]
    val result = ConfigUtils.getDataSkippingStatsColumns(config)

    result shouldBe None
  }

  test("ConfigUtils.getDataSkippingNumIndexedCols should return configured value") {
    val config = Map("spark.indextables.dataSkippingNumIndexedCols" -> "10")
    ConfigUtils.getDataSkippingNumIndexedCols(config) shouldBe 10
  }

  test("ConfigUtils.getDataSkippingNumIndexedCols should return default (32) when not set") {
    val config = Map.empty[String, String]
    ConfigUtils.getDataSkippingNumIndexedCols(config) shouldBe 32
  }

  test("ConfigUtils.getDataSkippingNumIndexedCols should handle -1 (all columns)") {
    val config = Map("spark.indextables.dataSkippingNumIndexedCols" -> "-1")
    ConfigUtils.getDataSkippingNumIndexedCols(config) shouldBe -1
  }

  test("config keys should be case-insensitive") {
    val config = Map("spark.indextables.dataskippingstatscolumns" -> "id,name")
    val result = ConfigUtils.getDataSkippingStatsColumns(config)

    result shouldBe defined
    result.get shouldBe Set("id", "name")
  }
}
