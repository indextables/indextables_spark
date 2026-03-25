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

import java.lang.reflect.InvocationTargetException
import java.sql.Date
import java.time.LocalDate

import org.apache.spark.sql.types._

import io.indextables.spark.TestBase

/**
 * Tests for DateType handling in TransactionLogGroupByCountPartitionReader.convertPartitionValue. Covers both the ISO
 * date string path and the epoch-day integer fallback (Commit 2).
 */
class TransactionLogCountDatePartitionTest extends TestBase {

  // ── Integration tests via GROUP BY COUNT pushdown ──────────────────────

  test("GROUP BY date partition COUNT should return correct counts") {
    withTempPath { tempPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      val testData = Seq(
        ("a", Date.valueOf("2024-01-15"), "content1"),
        ("b", Date.valueOf("2024-01-15"), "content2"),
        ("c", Date.valueOf("2024-02-20"), "content3"),
        ("d", Date.valueOf("2024-03-10"), "content4"),
        ("e", Date.valueOf("2024-03-10"), "content5"),
        ("f", Date.valueOf("2024-03-10"), "content6")
      ).toDF("id", "event_date", "description")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.fastfields", "event_date")
        .partitionBy("event_date")
        .save(tempPath)

      val df     = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tempPath)
      val result = df.groupBy("event_date").count().collect()

      val countMap = result.map(r => (r.getAs[Date]("event_date").toString, r.getAs[Long]("count"))).toMap
      countMap("2024-01-15") shouldBe 2
      countMap("2024-02-20") shouldBe 1
      countMap("2024-03-10") shouldBe 3
    }
  }

  test("GROUP BY date partition with date range filter") {
    withTempPath { tempPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      val testData = Seq(
        ("a", Date.valueOf("2024-01-15"), "c1"),
        ("b", Date.valueOf("2024-02-20"), "c2"),
        ("c", Date.valueOf("2024-03-10"), "c3"),
        ("d", Date.valueOf("2024-03-10"), "c4")
      ).toDF("id", "event_date", "description")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.fastfields", "event_date")
        .partitionBy("event_date")
        .save(tempPath)

      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tempPath)
      df.createOrReplaceTempView("date_group_test")

      val result = spark
        .sql(
          "SELECT event_date, count(*) as cnt FROM date_group_test WHERE event_date >= '2024-02-01' GROUP BY event_date"
        )
        .collect()

      result.length shouldBe 2
      val countMap = result.map(r => (r.getAs[Date]("event_date").toString, r.getAs[Long]("cnt"))).toMap
      countMap("2024-02-20") shouldBe 1
      countMap("2024-03-10") shouldBe 2
    }
  }

  // ── Reflection-based unit tests for convertPartitionValue ──────────────

  private lazy val convertPartitionValueMethod = {
    val m = classOf[TransactionLogGroupByCountPartitionReader].getDeclaredMethod(
      "convertPartitionValue",
      classOf[String],
      classOf[DataType],
      classOf[String]
    )
    m.setAccessible(true)
    m
  }

  private def invokeConvertPartitionValue(
    value: String,
    dataType: DataType,
    columnName: String
  ): Any = {
    val partition = TransactionLogGroupByCountPartition(
      groupedCounts = Seq.empty,
      groupByColumns = Array(columnName),
      tableSchema = Some(StructType(Seq(StructField(columnName, dataType))))
    )
    val reader = new TransactionLogGroupByCountPartitionReader(partition, Map.empty)
    convertPartitionValueMethod.invoke(reader, value, dataType, columnName)
  }

  test("convertPartitionValue should convert ISO date string to epoch days") {
    val expected = LocalDate.parse("2026-03-22").toEpochDay.toInt
    val result   = invokeConvertPartitionValue("2026-03-22", DateType, "event_date")
    result shouldBe expected
  }

  test("convertPartitionValue should convert epoch-day number string to int") {
    val result = invokeConvertPartitionValue("20527", DateType, "event_date")
    result shouldBe 20527
  }

  test("convertPartitionValue should handle ISO datetime with T by extracting date part") {
    val expected = LocalDate.parse("2026-03-22").toEpochDay.toInt
    val result   = invokeConvertPartitionValue("2026-03-22T10:00:00", DateType, "event_date")
    result shouldBe expected
  }

  test("convertPartitionValue should throw for non-date non-numeric string") {
    val ex = intercept[InvocationTargetException] {
      invokeConvertPartitionValue("not-a-date", DateType, "event_date")
    }
    ex.getCause shouldBe a[IllegalArgumentException]
  }

  test("convertPartitionValue should return null for null value") {
    val result = invokeConvertPartitionValue(null, DateType, "event_date")
    assert(result == null)
  }

  test("convertPartitionValue should return null for empty string") {
    val result = invokeConvertPartitionValue("", DateType, "event_date")
    assert(result == null)
  }

  test("convertPartitionValue should handle negative epoch day") {
    // "-1" contains a '-' so ISO parse fails; falls back to toInt
    val result = invokeConvertPartitionValue("-1", DateType, "event_date")
    result shouldBe -1
  }

  test("convertPartitionValue should handle epoch day 0") {
    // "0" fails ISO parse; falls back to toInt = 0 (1970-01-01)
    val result = invokeConvertPartitionValue("0", DateType, "event_date")
    result shouldBe 0
  }
}
