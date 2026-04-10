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

import java.time.LocalDate

import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for the Iceberg DATE partition normalization helpers in DistributedSourceScanner. Covers:
 * normalizeIcebergDatePartitions, extractDateColumns, resolvePartitionValues.
 */
class IcebergDatePartitionNormalizationTest extends AnyFunSuite with Matchers {

  // ── normalizeIcebergDatePartitions ──────────────────────────────────────

  test("normalizeIcebergDatePartitions should convert epoch-day integer to ISO string") {
    val result = DistributedSourceScanner.normalizeIcebergDatePartitions(
      Map("dt" -> "20527"),
      Set("dt")
    )
    result("dt") shouldBe LocalDate.ofEpochDay(20527).toString
  }

  test("normalizeIcebergDatePartitions should pass through non-date columns unchanged") {
    val result = DistributedSourceScanner.normalizeIcebergDatePartitions(
      Map("dt" -> "20527", "region" -> "us-east"),
      Set("dt")
    )
    result("dt") shouldBe LocalDate.ofEpochDay(20527).toString
    result("region") shouldBe "us-east"
  }

  test("normalizeIcebergDatePartitions should pass through ISO string that is not numeric") {
    val result = DistributedSourceScanner.normalizeIcebergDatePartitions(
      Map("dt" -> "2026-03-22"),
      Set("dt")
    )
    result("dt") shouldBe "2026-03-22"
  }

  test("normalizeIcebergDatePartitions should return unchanged when dateColumns is empty") {
    val input  = Map("dt" -> "20527")
    val result = DistributedSourceScanner.normalizeIcebergDatePartitions(input, Set.empty)
    result shouldBe input
  }

  test("normalizeIcebergDatePartitions should handle empty partition values") {
    val result = DistributedSourceScanner.normalizeIcebergDatePartitions(
      Map.empty[String, String],
      Set("dt")
    )
    result shouldBe empty
  }

  test("normalizeIcebergDatePartitions should handle epoch day 0 (Unix epoch)") {
    val result = DistributedSourceScanner.normalizeIcebergDatePartitions(
      Map("dt" -> "0"),
      Set("dt")
    )
    result("dt") shouldBe "1970-01-01"
  }

  test("normalizeIcebergDatePartitions should handle negative epoch days (pre-1970)") {
    val result = DistributedSourceScanner.normalizeIcebergDatePartitions(
      Map("dt" -> "-365"),
      Set("dt")
    )
    result("dt") shouldBe LocalDate.ofEpochDay(-365).toString
  }

  test("normalizeIcebergDatePartitions should handle multiple date columns") {
    val result = DistributedSourceScanner.normalizeIcebergDatePartitions(
      Map("start_date" -> "20527", "end_date" -> "20530"),
      Set("start_date", "end_date")
    )
    result("start_date") shouldBe LocalDate.ofEpochDay(20527).toString
    result("end_date") shouldBe LocalDate.ofEpochDay(20530).toString
  }

  test("normalizeIcebergDatePartitions should handle large epoch day at boundary (100000)") {
    val result = DistributedSourceScanner.normalizeIcebergDatePartitions(
      Map("dt" -> "100000"),
      Set("dt")
    )
    result("dt") shouldBe LocalDate.ofEpochDay(100000).toString
  }

  test("normalizeIcebergDatePartitions should pass through compact ISO date 20260322 (implausible epoch day)") {
    val result = DistributedSourceScanner.normalizeIcebergDatePartitions(
      Map("dt" -> "20260322"),
      Set("dt")
    )
    result("dt") shouldBe "20260322"
  }

  test("normalizeIcebergDatePartitions should pass through compact ISO date 20240115") {
    val result = DistributedSourceScanner.normalizeIcebergDatePartitions(
      Map("dt" -> "20240115"),
      Set("dt")
    )
    result("dt") shouldBe "20240115"
  }

  test("normalizeIcebergDatePartitions should pass through epoch day just above boundary (100001)") {
    val result = DistributedSourceScanner.normalizeIcebergDatePartitions(
      Map("dt" -> "100001"),
      Set("dt")
    )
    result("dt") shouldBe "100001"
  }

  test("normalizeIcebergDatePartitions should convert negative boundary epoch day -100000") {
    val result = DistributedSourceScanner.normalizeIcebergDatePartitions(
      Map("dt" -> "-100000"),
      Set("dt")
    )
    result("dt") shouldBe LocalDate.ofEpochDay(-100000).toString
  }

  // ── isPlausibleEpochDay ───────────────────────────────────────────────

  test("isPlausibleEpochDay should return true for values in plausible range") {
    DistributedSourceScanner.isPlausibleEpochDay(0) shouldBe true
    DistributedSourceScanner.isPlausibleEpochDay(20527) shouldBe true
    DistributedSourceScanner.isPlausibleEpochDay(-365) shouldBe true
    DistributedSourceScanner.isPlausibleEpochDay(100000) shouldBe true
    DistributedSourceScanner.isPlausibleEpochDay(-100000) shouldBe true
  }

  test("isPlausibleEpochDay should return false for compact ISO dates and large values") {
    DistributedSourceScanner.isPlausibleEpochDay(20260322) shouldBe false
    DistributedSourceScanner.isPlausibleEpochDay(20240115) shouldBe false
    DistributedSourceScanner.isPlausibleEpochDay(100001) shouldBe false
    DistributedSourceScanner.isPlausibleEpochDay(-100001) shouldBe false
  }

  // ── extractDateColumns ─────────────────────────────────────────────────

  test("extractDateColumns should extract DATE columns from mixed schema") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("dt", DateType),
        StructField("name", StringType)
      )
    )
    DistributedSourceScanner.extractDateColumns(Some(schema)) shouldBe Set("dt")
  }

  test("extractDateColumns should return empty set for None schema") {
    DistributedSourceScanner.extractDateColumns(None) shouldBe Set.empty
  }

  test("extractDateColumns should return empty set when no DATE columns exist") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("name", StringType)
      )
    )
    DistributedSourceScanner.extractDateColumns(Some(schema)) shouldBe Set.empty
  }

  test("extractDateColumns should find multiple DATE columns") {
    val schema = StructType(
      Seq(
        StructField("start_date", DateType),
        StructField("end_date", DateType),
        StructField("name", StringType)
      )
    )
    DistributedSourceScanner.extractDateColumns(Some(schema)) shouldBe Set("start_date", "end_date")
  }

  test("extractDateColumns should not include TimestampType columns") {
    val schema = StructType(
      Seq(
        StructField("created_at", TimestampType),
        StructField("name", StringType)
      )
    )
    DistributedSourceScanner.extractDateColumns(Some(schema)) shouldBe Set.empty
  }

  // ── resolvePartitionValues ─────────────────────────────────────────────

  test("resolvePartitionValues should normalize dates when partition values are present") {
    val result = DistributedSourceScanner.resolvePartitionValues(
      Map("dt" -> "20527"),
      "s3://bucket/data/dt=20527/file.parquet",
      Some("s3://bucket/data"),
      Set("dt")
    )
    result("dt") shouldBe LocalDate.ofEpochDay(20527).toString
  }

  test("resolvePartitionValues should extract from Hive-style path when values empty and storageRoot defined") {
    val result = DistributedSourceScanner.resolvePartitionValues(
      Map.empty[String, String],
      "s3://bucket/data/dt=2024-01-01/file.parquet",
      Some("s3://bucket/data"),
      Set("dt")
    )
    result("dt") shouldBe "2024-01-01"
  }

  test("resolvePartitionValues should normalize dates even without storageRoot when values non-empty") {
    val result = DistributedSourceScanner.resolvePartitionValues(
      Map("dt" -> "20527"),
      "s3://bucket/data/dt=20527/file.parquet",
      None,
      Set("dt")
    )
    result("dt") shouldBe LocalDate.ofEpochDay(20527).toString
  }
}
