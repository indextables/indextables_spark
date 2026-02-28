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

package io.indextables.spark.sync

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import io.indextables.tantivy4java.filter.PartitionFilter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Unit tests for SparkPredicateToPartitionFilter — verifies conversion of Spark SQL WHERE
 * predicate strings into tantivy4java PartitionFilter objects.
 */
class SparkPredicateToPartitionFilterTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("SparkPredicateToPartitionFilterTest")
      .master("local[1]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  private val stringPartCols = Seq("date", "region")
  private val stringSchema = StructType(Seq(
    StructField("id", LongType),
    StructField("date", StringType),
    StructField("region", StringType)
  ))

  private val intPartCols = Seq("year", "month")
  private val intSchema = StructType(Seq(
    StructField("id", LongType),
    StructField("year", IntegerType),
    StructField("month", IntegerType)
  ))

  private val longPartCols = Seq("event_ts")
  private val longSchema = StructType(Seq(
    StructField("id", LongType),
    StructField("event_ts", LongType)
  ))

  private val datePartCols = Seq("dt")
  private val dateSchema = StructType(Seq(
    StructField("id", LongType),
    StructField("dt", DateType)
  ))

  // ─── Basic Equality ───

  test("simple equality on string partition column") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("date = '2024-01-01'"), stringPartCols, spark, Some(stringSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"eq\"")
    json should include("\"column\":\"date\"")
    json should include("\"value\":\"2024-01-01\"")
  }

  test("equality with reversed operands") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("'us-east' = region"), stringPartCols, spark, Some(stringSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"column\":\"region\"")
    json should include("\"value\":\"us-east\"")
  }

  // ─── Range Comparisons ───

  test("greater than on integer partition column should include withType long") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("year > 2024"), intPartCols, spark, Some(intSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"gt\"")
    json should include("\"column\":\"year\"")
    json should include("\"type\":\"long\"")
  }

  test("greater than or equal") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("year >= 2024"), intPartCols, spark, Some(intSchema))
    result shouldBe defined
    result.get.toJson should include("\"op\":\"gte\"")
  }

  test("less than") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("year < 2025"), intPartCols, spark, Some(intSchema))
    result shouldBe defined
    result.get.toJson should include("\"op\":\"lt\"")
  }

  test("less than or equal") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("year <= 2025"), intPartCols, spark, Some(intSchema))
    result shouldBe defined
    result.get.toJson should include("\"op\":\"lte\"")
  }

  test("reversed comparison: literal > column → lt on column") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("2025 > year"), intPartCols, spark, Some(intSchema))
    result shouldBe defined
    result.get.toJson should include("\"op\":\"lt\"")
    result.get.toJson should include("\"column\":\"year\"")
  }

  test("range on string partition column should NOT include withType") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("date >= '2024-01-01'"), stringPartCols, spark, Some(stringSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"gte\"")
    json should not include "\"type\":"
  }

  // ─── IN ───

  test("IN with multiple values") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("region IN ('us-east', 'us-west', 'eu-west')"), stringPartCols, spark, Some(stringSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"in\"")
    json should include("us-east")
    json should include("us-west")
    json should include("eu-west")
  }

  // ─── IS NULL / IS NOT NULL ───

  test("IS NULL") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("region IS NULL"), stringPartCols, spark, Some(stringSchema))
    result shouldBe defined
    result.get.toJson should include("\"op\":\"is_null\"")
  }

  test("IS NOT NULL") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("region IS NOT NULL"), stringPartCols, spark, Some(stringSchema))
    result shouldBe defined
    result.get.toJson should include("\"op\":\"is_not_null\"")
  }

  // ─── Compound: AND / OR / NOT ───

  test("AND of two predicates") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("year >= 2024 AND month <= 6"), intPartCols, spark, Some(intSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"and\"")
  }

  test("OR of two predicates") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("region = 'us-east' OR region = 'eu-west'"), stringPartCols, spark, Some(stringSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"or\"")
  }

  test("NOT on non-equality expression uses general NOT handler") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("NOT (year >= 2024)"), intPartCols, spark, Some(intSchema))
    result shouldBe defined
    result.get.toJson should include("\"op\":\"not\"")
  }

  test("multiple predicate strings are AND-combined") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("date = '2024-01-01'", "region = 'us-east'"), stringPartCols, spark, Some(stringSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"and\"")
  }

  // ─── Not Equal (!=) ───

  test("NOT equality should produce neq filter") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("NOT region = 'us-east'"), stringPartCols, spark, Some(stringSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"neq\"")
    json should include("\"column\":\"region\"")
    json should include("\"value\":\"us-east\"")
  }

  test("!= operator should produce neq filter") {
    // Spark parses != as Not(EqualTo(...))
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("region != 'us-east'"), stringPartCols, spark, Some(stringSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"neq\"")
    json should include("\"column\":\"region\"")
  }

  test("<> operator should produce neq filter") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("region <> 'us-east'"), stringPartCols, spark, Some(stringSchema))
    result shouldBe defined
    result.get.toJson should include("\"op\":\"neq\"")
  }

  test("NOT equality with reversed operands") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("NOT 'us-east' = region"), stringPartCols, spark, Some(stringSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"neq\"")
    json should include("\"column\":\"region\"")
  }

  // ─── LongType Partition Columns ───

  test("range on LongType partition column should include withType long") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("event_ts > 1700000000"), longPartCols, spark, Some(longSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"gt\"")
    json should include("\"column\":\"event_ts\"")
    json should include("\"type\":\"long\"")
  }

  test("equality on LongType partition column") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("event_ts = 1700000000"), longPartCols, spark, Some(longSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"eq\"")
    json should include("\"value\":\"1700000000\"")
  }

  // ─── DateType Partition Columns ───

  test("equality on DateType partition column") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("dt = '2024-01-15'"), datePartCols, spark, Some(dateSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"eq\"")
    json should include("\"column\":\"dt\"")
  }

  test("range on DateType partition column should not include withType long") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("dt >= '2024-01-01'"), datePartCols, spark, Some(dateSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"gte\"")
    json should not include "\"type\":"
  }

  // ─── Date-Format String Partition Columns ───

  test("equality on date-format string like '2025/01/01'") {
    val dtPartCols = Seq("dt")
    val dtSchema = StructType(Seq(
      StructField("id", LongType),
      StructField("dt", StringType)
    ))
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("dt = '2025/01/01'"), dtPartCols, spark, Some(dtSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"eq\"")
    json should include("\"value\":\"2025/01/01\"")
  }

  test("range on date-format string like dt >= '2025/01/01'") {
    val dtPartCols = Seq("dt")
    val dtSchema = StructType(Seq(
      StructField("id", LongType),
      StructField("dt", StringType)
    ))
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("dt >= '2025/01/01'"), dtPartCols, spark, Some(dtSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"gte\"")
    json should include("\"value\":\"2025/01/01\"")
    json should not include "\"type\":"  // string, not long
  }

  // ─── BETWEEN (AND composition) ───

  test("BETWEEN-equivalent filter via AND should produce and(gte, lte)") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("year >= 2023 AND year <= 2025"), intPartCols, spark, Some(intSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"and\"")
    json should include("\"op\":\"gte\"")
    json should include("\"op\":\"lte\"")
    json should include("\"value\":\"2023\"")
    json should include("\"value\":\"2025\"")
    json should include("\"type\":\"long\"")
  }

  test("BETWEEN-equivalent as separate predicates") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("year >= 2023", "year <= 2025"), intPartCols, spark, Some(intSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"and\"")
    json should include("\"op\":\"gte\"")
    json should include("\"op\":\"lte\"")
  }

  // ─── Graceful Degradation ───

  test("empty predicates returns None") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq.empty, stringPartCols, spark, Some(stringSchema))
    result shouldBe None
  }

  test("empty partition columns returns None") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("date = '2024-01-01'"), Seq.empty, spark, Some(stringSchema))
    result shouldBe None
  }

  test("predicate on non-partition column returns None") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("id = 42"), stringPartCols, spark, Some(stringSchema))
    result shouldBe None
  }

  test("OR with one non-convertible side returns None") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("region = 'us-east' OR id > 10"), stringPartCols, spark, Some(stringSchema))
    result shouldBe None
  }

  test("AND with one non-convertible side keeps the convertible part") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("region = 'us-east' AND id > 10"), stringPartCols, spark, Some(stringSchema))
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"column\":\"region\"")
    json should not include "\"column\":\"id\""
  }

  test("unsupported expression type returns None") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("region LIKE 'us-%'"), stringPartCols, spark, Some(stringSchema))
    result shouldBe None
  }

  // ─── Without Schema (defaults to StringType) ───

  test("range on partition column without schema does not include withType") {
    val result = SparkPredicateToPartitionFilter.convert(
      Seq("year >= 2024"), Seq("year"), spark, None)
    result shouldBe defined
    val json = result.get.toJson
    json should include("\"op\":\"gte\"")
    json should not include "\"type\":"
  }
}
