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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import io.indextables.spark.TestBase
import org.scalatest.matchers.should.Matchers._

/**
 * Comprehensive test suite for GROUP BY queries on partition columns of all supported types.
 * Tests both partition-only GROUP BY and mixed partition + non-partition GROUP BY scenarios.
 *
 * Supported partition column types:
 * - StringType
 * - IntegerType
 * - LongType
 * - DateType
 * - TimestampType
 */
class PartitionColumnGroupByTest extends TestBase {

  // ===================================================================================
  // StringType partition column tests
  // ===================================================================================

  test("GROUP BY String partition column and non-partition column") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/string_partition_groupby"

      val schema = StructType(Seq(
        StructField("region", StringType, nullable = false),
        StructField("category", StringType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row("us-east", "electronics", "item1"),
        Row("us-east", "electronics", "item2"),
        Row("us-east", "clothing", "item3"),
        Row("us-west", "electronics", "item4"),
        Row("us-west", "clothing", "item5"),
        Row("us-west", "clothing", "item6"),
        Row("eu-west", "electronics", "item7")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("region")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      readDf.createOrReplaceTempView("string_table")

      val result = spark.sql(
        """
          |SELECT region, category, COUNT(*) as cnt
          |FROM string_table
          |GROUP BY 1, 2
          |ORDER BY 1, 2
        """.stripMargin
      ).collect()

      result.length should be(5)

      // eu-west, electronics: 1
      result(0).getString(0) should be("eu-west")
      result(0).getString(1) should be("electronics")
      result(0).getLong(2) should be(1)

      // us-east, clothing: 1
      result(1).getString(0) should be("us-east")
      result(1).getString(1) should be("clothing")
      result(1).getLong(2) should be(1)

      // us-east, electronics: 2
      result(2).getString(0) should be("us-east")
      result(2).getString(1) should be("electronics")
      result(2).getLong(2) should be(2)

      // us-west, clothing: 2
      result(3).getString(0) should be("us-west")
      result(3).getString(1) should be("clothing")
      result(3).getLong(2) should be(2)

      // us-west, electronics: 1
      result(4).getString(0) should be("us-west")
      result(4).getString(1) should be("electronics")
      result(4).getLong(2) should be(1)
    }
  }

  test("GROUP BY String partition column only") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/string_partition_only"

      val schema = StructType(Seq(
        StructField("region", StringType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row("us-east", "item1"),
        Row("us-east", "item2"),
        Row("us-west", "item3"),
        Row("eu-west", "item4"),
        Row("eu-west", "item5"),
        Row("eu-west", "item6")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("region")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      val result = readDf
        .groupBy("region")
        .agg(count("*").as("cnt"))
        .orderBy("region")
        .collect()

      result.length should be(3)
      result(0).getString(0) should be("eu-west")
      result(0).getLong(1) should be(3)
      result(1).getString(0) should be("us-east")
      result(1).getLong(1) should be(2)
      result(2).getString(0) should be("us-west")
      result(2).getLong(1) should be(1)
    }
  }

  // ===================================================================================
  // IntegerType partition column tests
  // ===================================================================================

  test("GROUP BY Integer partition column and non-partition column") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/int_partition_groupby"

      val schema = StructType(Seq(
        StructField("year", IntegerType, nullable = false),
        StructField("category", StringType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(2022, "A", "item1"),
        Row(2022, "A", "item2"),
        Row(2022, "B", "item3"),
        Row(2023, "A", "item4"),
        Row(2023, "B", "item5"),
        Row(2023, "B", "item6"),
        Row(2024, "A", "item7")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("year")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      readDf.createOrReplaceTempView("int_table")

      val result = spark.sql(
        """
          |SELECT year, category, COUNT(*) as cnt
          |FROM int_table
          |GROUP BY 1, 2
          |ORDER BY 1, 2
        """.stripMargin
      ).collect()

      result.length should be(5)

      // 2022, A: 2
      result(0).getInt(0) should be(2022)
      result(0).getString(1) should be("A")
      result(0).getLong(2) should be(2)

      // 2022, B: 1
      result(1).getInt(0) should be(2022)
      result(1).getString(1) should be("B")
      result(1).getLong(2) should be(1)

      // 2023, A: 1
      result(2).getInt(0) should be(2023)
      result(2).getString(1) should be("A")
      result(2).getLong(2) should be(1)

      // 2023, B: 2
      result(3).getInt(0) should be(2023)
      result(3).getString(1) should be("B")
      result(3).getLong(2) should be(2)

      // 2024, A: 1
      result(4).getInt(0) should be(2024)
      result(4).getString(1) should be("A")
      result(4).getLong(2) should be(1)
    }
  }

  test("GROUP BY Integer partition column only") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/int_partition_only"

      val schema = StructType(Seq(
        StructField("year", IntegerType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(2022, "item1"),
        Row(2022, "item2"),
        Row(2023, "item3"),
        Row(2024, "item4"),
        Row(2024, "item5"),
        Row(2024, "item6")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("year")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      val result = readDf
        .groupBy("year")
        .agg(count("*").as("cnt"))
        .orderBy("year")
        .collect()

      result.length should be(3)
      result(0).getInt(0) should be(2022)
      result(0).getLong(1) should be(2)
      result(1).getInt(0) should be(2023)
      result(1).getLong(1) should be(1)
      result(2).getInt(0) should be(2024)
      result(2).getLong(1) should be(3)
    }
  }

  // ===================================================================================
  // LongType partition column tests
  // ===================================================================================

  test("GROUP BY Long partition column and non-partition column") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/long_partition_groupby"

      val schema = StructType(Seq(
        StructField("epoch_day", LongType, nullable = false),
        StructField("category", StringType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(19723L, "A", "item1"),  // 2024-01-01
        Row(19723L, "A", "item2"),
        Row(19723L, "B", "item3"),
        Row(19724L, "A", "item4"),  // 2024-01-02
        Row(19724L, "B", "item5"),
        Row(19725L, "A", "item6")   // 2024-01-03
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("epoch_day")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      readDf.createOrReplaceTempView("long_table")

      val result = spark.sql(
        """
          |SELECT epoch_day, category, COUNT(*) as cnt
          |FROM long_table
          |GROUP BY 1, 2
          |ORDER BY 1, 2
        """.stripMargin
      ).collect()

      result.length should be(5)

      // 19723, A: 2
      result(0).getLong(0) should be(19723L)
      result(0).getString(1) should be("A")
      result(0).getLong(2) should be(2)

      // 19723, B: 1
      result(1).getLong(0) should be(19723L)
      result(1).getString(1) should be("B")
      result(1).getLong(2) should be(1)

      // 19724, A: 1
      result(2).getLong(0) should be(19724L)
      result(2).getString(1) should be("A")
      result(2).getLong(2) should be(1)

      // 19724, B: 1
      result(3).getLong(0) should be(19724L)
      result(3).getString(1) should be("B")
      result(3).getLong(2) should be(1)

      // 19725, A: 1
      result(4).getLong(0) should be(19725L)
      result(4).getString(1) should be("A")
      result(4).getLong(2) should be(1)
    }
  }

  test("GROUP BY Long partition column only") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/long_partition_only"

      val schema = StructType(Seq(
        StructField("epoch_day", LongType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(19723L, "item1"),
        Row(19723L, "item2"),
        Row(19724L, "item3"),
        Row(19725L, "item4"),
        Row(19725L, "item5"),
        Row(19725L, "item6")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("epoch_day")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      val result = readDf
        .groupBy("epoch_day")
        .agg(count("*").as("cnt"))
        .orderBy("epoch_day")
        .collect()

      result.length should be(3)
      result(0).getLong(0) should be(19723L)
      result(0).getLong(1) should be(2)
      result(1).getLong(0) should be(19724L)
      result(1).getLong(1) should be(1)
      result(2).getLong(0) should be(19725L)
      result(2).getLong(1) should be(3)
    }
  }

  // ===================================================================================
  // DateType partition column tests
  // ===================================================================================

  test("GROUP BY Date partition column and non-partition column") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/date_partition_groupby"

      val schema = StructType(Seq(
        StructField("part_date", DateType, nullable = false),
        StructField("category", StringType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(Date.valueOf("2024-01-01"), "A", "item1"),
        Row(Date.valueOf("2024-01-01"), "A", "item2"),
        Row(Date.valueOf("2024-01-01"), "B", "item3"),
        Row(Date.valueOf("2024-01-02"), "A", "item4"),
        Row(Date.valueOf("2024-01-02"), "B", "item5"),
        Row(Date.valueOf("2024-01-02"), "B", "item6"),
        Row(Date.valueOf("2024-01-03"), "A", "item7")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("part_date")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      readDf.createOrReplaceTempView("date_table")

      val result = spark.sql(
        """
          |SELECT part_date, category, COUNT(*) as cnt
          |FROM date_table
          |GROUP BY 1, 2
          |ORDER BY 1, 2
        """.stripMargin
      ).collect()

      result.length should be(5)

      // 2024-01-01, A: 2
      result(0).getDate(0) should be(Date.valueOf("2024-01-01"))
      result(0).getString(1) should be("A")
      result(0).getLong(2) should be(2)

      // 2024-01-01, B: 1
      result(1).getDate(0) should be(Date.valueOf("2024-01-01"))
      result(1).getString(1) should be("B")
      result(1).getLong(2) should be(1)

      // 2024-01-02, A: 1
      result(2).getDate(0) should be(Date.valueOf("2024-01-02"))
      result(2).getString(1) should be("A")
      result(2).getLong(2) should be(1)

      // 2024-01-02, B: 2
      result(3).getDate(0) should be(Date.valueOf("2024-01-02"))
      result(3).getString(1) should be("B")
      result(3).getLong(2) should be(2)

      // 2024-01-03, A: 1
      result(4).getDate(0) should be(Date.valueOf("2024-01-03"))
      result(4).getString(1) should be("A")
      result(4).getLong(2) should be(1)
    }
  }

  test("GROUP BY Date partition column only") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/date_partition_only"

      val schema = StructType(Seq(
        StructField("part_date", DateType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(Date.valueOf("2024-01-01"), "item1"),
        Row(Date.valueOf("2024-01-01"), "item2"),
        Row(Date.valueOf("2024-01-02"), "item3"),
        Row(Date.valueOf("2024-01-03"), "item4"),
        Row(Date.valueOf("2024-01-03"), "item5"),
        Row(Date.valueOf("2024-01-03"), "item6")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("part_date")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      val result = readDf
        .groupBy("part_date")
        .agg(count("*").as("cnt"))
        .orderBy("part_date")
        .collect()

      result.length should be(3)
      result(0).getDate(0) should be(Date.valueOf("2024-01-01"))
      result(0).getLong(1) should be(2)
      result(1).getDate(0) should be(Date.valueOf("2024-01-02"))
      result(1).getLong(1) should be(1)
      result(2).getDate(0) should be(Date.valueOf("2024-01-03"))
      result(2).getLong(1) should be(3)
    }
  }

  // ===================================================================================
  // TimestampType partition column tests
  // ===================================================================================

  test("GROUP BY Timestamp partition column and non-partition column") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/timestamp_partition_groupby"

      val schema = StructType(Seq(
        StructField("event_time", TimestampType, nullable = false),
        StructField("category", StringType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(Timestamp.valueOf("2024-01-01 10:00:00"), "A", "item1"),
        Row(Timestamp.valueOf("2024-01-01 10:00:00"), "A", "item2"),
        Row(Timestamp.valueOf("2024-01-01 10:00:00"), "B", "item3"),
        Row(Timestamp.valueOf("2024-01-01 11:00:00"), "A", "item4"),
        Row(Timestamp.valueOf("2024-01-01 11:00:00"), "B", "item5"),
        Row(Timestamp.valueOf("2024-01-01 12:00:00"), "A", "item6")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("event_time")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      readDf.createOrReplaceTempView("timestamp_table")

      val result = spark.sql(
        """
          |SELECT event_time, category, COUNT(*) as cnt
          |FROM timestamp_table
          |GROUP BY 1, 2
          |ORDER BY 1, 2
        """.stripMargin
      ).collect()

      result.length should be(5)

      // 10:00:00, A: 2
      result(0).getTimestamp(0) should be(Timestamp.valueOf("2024-01-01 10:00:00"))
      result(0).getString(1) should be("A")
      result(0).getLong(2) should be(2)

      // 10:00:00, B: 1
      result(1).getTimestamp(0) should be(Timestamp.valueOf("2024-01-01 10:00:00"))
      result(1).getString(1) should be("B")
      result(1).getLong(2) should be(1)

      // 11:00:00, A: 1
      result(2).getTimestamp(0) should be(Timestamp.valueOf("2024-01-01 11:00:00"))
      result(2).getString(1) should be("A")
      result(2).getLong(2) should be(1)

      // 11:00:00, B: 1
      result(3).getTimestamp(0) should be(Timestamp.valueOf("2024-01-01 11:00:00"))
      result(3).getString(1) should be("B")
      result(3).getLong(2) should be(1)

      // 12:00:00, A: 1
      result(4).getTimestamp(0) should be(Timestamp.valueOf("2024-01-01 12:00:00"))
      result(4).getString(1) should be("A")
      result(4).getLong(2) should be(1)
    }
  }

  test("GROUP BY Timestamp partition column only") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/timestamp_partition_only"

      val schema = StructType(Seq(
        StructField("event_time", TimestampType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(Timestamp.valueOf("2024-01-01 10:00:00"), "item1"),
        Row(Timestamp.valueOf("2024-01-01 10:00:00"), "item2"),
        Row(Timestamp.valueOf("2024-01-01 11:00:00"), "item3"),
        Row(Timestamp.valueOf("2024-01-01 12:00:00"), "item4"),
        Row(Timestamp.valueOf("2024-01-01 12:00:00"), "item5"),
        Row(Timestamp.valueOf("2024-01-01 12:00:00"), "item6")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("event_time")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      val result = readDf
        .groupBy("event_time")
        .agg(count("*").as("cnt"))
        .orderBy("event_time")
        .collect()

      result.length should be(3)
      result(0).getTimestamp(0) should be(Timestamp.valueOf("2024-01-01 10:00:00"))
      result(0).getLong(1) should be(2)
      result(1).getTimestamp(0) should be(Timestamp.valueOf("2024-01-01 11:00:00"))
      result(1).getLong(1) should be(1)
      result(2).getTimestamp(0) should be(Timestamp.valueOf("2024-01-01 12:00:00"))
      result(2).getLong(1) should be(3)
    }
  }

  // ===================================================================================
  // Multiple partition columns tests
  // ===================================================================================

  test("GROUP BY multiple partition columns (Date + String)") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/multi_partition_groupby"

      val schema = StructType(Seq(
        StructField("part_date", DateType, nullable = false),
        StructField("region", StringType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(Date.valueOf("2024-01-01"), "us-east", "item1"),
        Row(Date.valueOf("2024-01-01"), "us-east", "item2"),
        Row(Date.valueOf("2024-01-01"), "us-west", "item3"),
        Row(Date.valueOf("2024-01-02"), "us-east", "item4"),
        Row(Date.valueOf("2024-01-02"), "us-west", "item5"),
        Row(Date.valueOf("2024-01-02"), "us-west", "item6")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("part_date", "region")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      readDf.createOrReplaceTempView("multi_partition_table")

      val result = spark.sql(
        """
          |SELECT part_date, region, COUNT(*) as cnt
          |FROM multi_partition_table
          |GROUP BY 1, 2
          |ORDER BY 1, 2
        """.stripMargin
      ).collect()

      result.length should be(4)

      // 2024-01-01, us-east: 2
      result(0).getDate(0) should be(Date.valueOf("2024-01-01"))
      result(0).getString(1) should be("us-east")
      result(0).getLong(2) should be(2)

      // 2024-01-01, us-west: 1
      result(1).getDate(0) should be(Date.valueOf("2024-01-01"))
      result(1).getString(1) should be("us-west")
      result(1).getLong(2) should be(1)

      // 2024-01-02, us-east: 1
      result(2).getDate(0) should be(Date.valueOf("2024-01-02"))
      result(2).getString(1) should be("us-east")
      result(2).getLong(2) should be(1)

      // 2024-01-02, us-west: 2
      result(3).getDate(0) should be(Date.valueOf("2024-01-02"))
      result(3).getString(1) should be("us-west")
      result(3).getLong(2) should be(2)
    }
  }

  test("GROUP BY multiple partition columns (Integer + String)") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/multi_partition_int_string"

      val schema = StructType(Seq(
        StructField("year", IntegerType, nullable = false),
        StructField("region", StringType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(2023, "us-east", "item1"),
        Row(2023, "us-east", "item2"),
        Row(2023, "us-west", "item3"),
        Row(2024, "us-east", "item4"),
        Row(2024, "us-west", "item5"),
        Row(2024, "us-west", "item6")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("year", "region")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      readDf.createOrReplaceTempView("multi_partition_int_table")

      val result = spark.sql(
        """
          |SELECT year, region, COUNT(*) as cnt
          |FROM multi_partition_int_table
          |GROUP BY 1, 2
          |ORDER BY 1, 2
        """.stripMargin
      ).collect()

      result.length should be(4)

      // 2023, us-east: 2
      result(0).getInt(0) should be(2023)
      result(0).getString(1) should be("us-east")
      result(0).getLong(2) should be(2)

      // 2023, us-west: 1
      result(1).getInt(0) should be(2023)
      result(1).getString(1) should be("us-west")
      result(1).getLong(2) should be(1)

      // 2024, us-east: 1
      result(2).getInt(0) should be(2024)
      result(2).getString(1) should be("us-east")
      result(2).getLong(2) should be(1)

      // 2024, us-west: 2
      result(3).getInt(0) should be(2024)
      result(3).getString(1) should be("us-west")
      result(3).getLong(2) should be(2)
    }
  }

  // ===================================================================================
  // Simple COUNT(*) tests for each type
  // ===================================================================================

  test("simple COUNT(*) on String partitioned table") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/string_count"

      val schema = StructType(Seq(
        StructField("region", StringType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row("us-east", "item1"),
        Row("us-east", "item2"),
        Row("us-west", "item3")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("region")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      readDf.count() should be(3)
    }
  }

  test("simple COUNT(*) on Integer partitioned table") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/int_count"

      val schema = StructType(Seq(
        StructField("year", IntegerType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(2022, "item1"),
        Row(2023, "item2"),
        Row(2024, "item3")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("year")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      readDf.count() should be(3)
    }
  }

  test("simple COUNT(*) on Long partitioned table") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/long_count"

      val schema = StructType(Seq(
        StructField("epoch_day", LongType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(19723L, "item1"),
        Row(19724L, "item2"),
        Row(19725L, "item3")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("epoch_day")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      readDf.count() should be(3)
    }
  }

  test("simple COUNT(*) on Date partitioned table") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/date_count"

      val schema = StructType(Seq(
        StructField("part_date", DateType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(Date.valueOf("2024-01-01"), "item1"),
        Row(Date.valueOf("2024-01-02"), "item2"),
        Row(Date.valueOf("2024-01-03"), "item3")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("part_date")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      readDf.count() should be(3)
    }
  }

  test("simple COUNT(*) on Timestamp partitioned table") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/timestamp_count"

      val schema = StructType(Seq(
        StructField("event_time", TimestampType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(Timestamp.valueOf("2024-01-01 10:00:00"), "item1"),
        Row(Timestamp.valueOf("2024-01-01 11:00:00"), "item2"),
        Row(Timestamp.valueOf("2024-01-01 12:00:00"), "item3")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("event_time")
        .mode("overwrite")
        .save(tablePath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      readDf.count() should be(3)
    }
  }
}
