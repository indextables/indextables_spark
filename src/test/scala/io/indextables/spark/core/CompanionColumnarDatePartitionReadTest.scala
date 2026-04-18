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

import java.sql.Date

import org.apache.spark.sql.types._

import io.indextables.spark.TestBase

/**
 * Integration tests for DateType handling in CompanionColumnarPartitionReader.createConstantColumnVector. Validates
 * that date-partitioned data round-trips correctly through write and read paths.
 */
class CompanionColumnarDatePartitionReadTest extends TestBase {

  test("round-trip date-partitioned data through companion reader") {
    withTempPath { tempPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      val testData = Seq(
        ("a", Date.valueOf("2024-01-15"), "content1"),
        ("b", Date.valueOf("2024-02-20"), "content2"),
        ("c", Date.valueOf("2024-03-10"), "content3")
      ).toDF("id", "event_date", "description")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.fastfields", "event_date")
        .partitionBy("event_date")
        .save(tempPath)

      val df   = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tempPath)
      val rows = df.orderBy("id").collect()

      rows.length shouldBe 3
      rows(0).getAs[Date]("event_date") shouldBe Date.valueOf("2024-01-15")
      rows(1).getAs[Date]("event_date") shouldBe Date.valueOf("2024-02-20")
      rows(2).getAs[Date]("event_date") shouldBe Date.valueOf("2024-03-10")
    }
  }

  test("companion read with date filter should return only matching partition") {
    withTempPath { tempPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      val testData = Seq(
        ("a", Date.valueOf("2024-01-15"), "jan"),
        ("b", Date.valueOf("2024-01-15"), "jan2"),
        ("c", Date.valueOf("2024-02-20"), "feb"),
        ("d", Date.valueOf("2024-03-10"), "mar")
      ).toDF("id", "event_date", "description")

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.fastfields", "event_date")
        .partitionBy("event_date")
        .save(tempPath)

      val df     = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tempPath)
      val result = df.filter(df("event_date") === "2024-01-15").collect()

      result.length shouldBe 2
      result.foreach(row => row.getAs[Date]("event_date") shouldBe Date.valueOf("2024-01-15"))
      val ids = result.map(_.getAs[String]("id")).sorted
      ids shouldBe Array("a", "b")
    }
  }
}
