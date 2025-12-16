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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import io.indextables.spark.TestBase
import org.scalatest.matchers.should.Matchers._

/**
 * Test case to replicate ClassCastException when running GROUP BY on both a Date-type
 * partition column and a non-partition column.
 *
 * Schema: [part_date (Date, partition key), part_hour (String), value (String)]
 *
 * Query: SELECT part_date, part_hour, COUNT(*) FROM table GROUP BY 1, 2 ORDER BY 1, 2
 */
class DatePartitionGroupByBugTest extends TestBase {

  test("GROUP BY Date partition column and non-partition column should not throw ClassCastException") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/date_partition_groupby_bug"

      // Create schema with Date type for partition column, String for part_hour
      val schema = StructType(Seq(
        StructField("part_date", DateType, nullable = false),
        StructField("part_hour", StringType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      // Create test data with multiple dates and hours (part_hour as String)
      val testData = Seq(
        Row(Date.valueOf("2024-01-01"), "10", "value1"),
        Row(Date.valueOf("2024-01-01"), "10", "value2"),
        Row(Date.valueOf("2024-01-01"), "11", "value3"),
        Row(Date.valueOf("2024-01-01"), "11", "value4"),
        Row(Date.valueOf("2024-01-01"), "11", "value5"),
        Row(Date.valueOf("2024-01-02"), "10", "value6"),
        Row(Date.valueOf("2024-01-02"), "10", "value7"),
        Row(Date.valueOf("2024-01-02"), "12", "value8"),
        Row(Date.valueOf("2024-01-03"), "10", "value9"),
        Row(Date.valueOf("2024-01-03"), "11", "value10")
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      println(s"Test data schema: ${df.schema}")
      println(s"Test data count: ${df.count()}")
      df.show()

      // Write with part_date as the only partition key
      // Using default fast fields config (all columns become fast fields)
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("part_date")
        .mode("overwrite")
        .save(tablePath)

      // Read the data back
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // Register as temp view for SQL query
      readDf.createOrReplaceTempView("my_table")

      println("\n=== Running the problematic query ===")
      println("Query: SELECT part_date, part_hour, COUNT(*) FROM my_table GROUP BY 1, 2 ORDER BY 1, 2")

      // This is the query that causes ClassCastException in production
      // GROUP BY both a Date-type partition column and a non-partition fast field
      val result = spark.sql(
        """
          |SELECT part_date, part_hour, COUNT(*) as cnt
          |FROM my_table
          |GROUP BY 1, 2
          |ORDER BY 1, 2
        """.stripMargin
      )

      // Collect results - this is where the ClassCastException would occur
      val rows = result.collect()

      println("\n=== Results ===")
      rows.foreach { row =>
        println(s"  ${row.getDate(0)}, ${row.getString(1)}, ${row.getLong(2)}")
      }

      // Verify expected results
      rows.length should be(6)

      // 2024-01-01, hour 10: 2 records
      rows(0).getDate(0) should be(Date.valueOf("2024-01-01"))
      rows(0).getString(1) should be("10")
      rows(0).getLong(2) should be(2)

      // 2024-01-01, hour 11: 3 records
      rows(1).getDate(0) should be(Date.valueOf("2024-01-01"))
      rows(1).getString(1) should be("11")
      rows(1).getLong(2) should be(3)

      // 2024-01-02, hour 10: 2 records
      rows(2).getDate(0) should be(Date.valueOf("2024-01-02"))
      rows(2).getString(1) should be("10")
      rows(2).getLong(2) should be(2)

      // 2024-01-02, hour 12: 1 record
      rows(3).getDate(0) should be(Date.valueOf("2024-01-02"))
      rows(3).getString(1) should be("12")
      rows(3).getLong(2) should be(1)

      // 2024-01-03, hour 10: 1 record
      rows(4).getDate(0) should be(Date.valueOf("2024-01-03"))
      rows(4).getString(1) should be("10")
      rows(4).getLong(2) should be(1)

      // 2024-01-03, hour 11: 1 record
      rows(5).getDate(0) should be(Date.valueOf("2024-01-03"))
      rows(5).getString(1) should be("11")
      rows(5).getLong(2) should be(1)
    }
  }

  test("GROUP BY Date partition column and non-partition column using DataFrame API") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/date_partition_groupby_bug_df"

      val schema = StructType(Seq(
        StructField("part_date", DateType, nullable = false),
        StructField("part_hour", StringType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(Date.valueOf("2024-01-01"), "10", "value1"),
        Row(Date.valueOf("2024-01-01"), "10", "value2"),
        Row(Date.valueOf("2024-01-01"), "11", "value3"),
        Row(Date.valueOf("2024-01-02"), "10", "value4"),
        Row(Date.valueOf("2024-01-02"), "11", "value5")
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

      println("\n=== Running GROUP BY via DataFrame API ===")

      // Same query using DataFrame API
      val result = readDf
        .groupBy("part_date", "part_hour")
        .agg(count("*").as("cnt"))
        .orderBy("part_date", "part_hour")

      val rows = result.collect()

      println("\n=== Results ===")
      rows.foreach { row =>
        println(s"  ${row.getDate(0)}, ${row.getString(1)}, ${row.getLong(2)}")
      }

      rows.length should be(4)

      // 2024-01-01, hour 10: 2 records
      rows(0).getDate(0) should be(Date.valueOf("2024-01-01"))
      rows(0).getString(1) should be("10")
      rows(0).getLong(2) should be(2)

      // 2024-01-01, hour 11: 1 record
      rows(1).getDate(0) should be(Date.valueOf("2024-01-01"))
      rows(1).getString(1) should be("11")
      rows(1).getLong(2) should be(1)

      // 2024-01-02, hour 10: 1 record
      rows(2).getDate(0) should be(Date.valueOf("2024-01-02"))
      rows(2).getString(1) should be("10")
      rows(2).getLong(2) should be(1)

      // 2024-01-02, hour 11: 1 record
      rows(3).getDate(0) should be(Date.valueOf("2024-01-02"))
      rows(3).getString(1) should be("11")
      rows(3).getLong(2) should be(1)
    }
  }

  test("GROUP BY Date partition column only should work") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/date_partition_only"

      val schema = StructType(Seq(
        StructField("part_date", DateType, nullable = false),
        StructField("part_hour", StringType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(Date.valueOf("2024-01-01"), "10", "value1"),
        Row(Date.valueOf("2024-01-01"), "11", "value2"),
        Row(Date.valueOf("2024-01-02"), "10", "value3")
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

      println("\n=== GROUP BY part_date only ===")

      val result = readDf
        .groupBy("part_date")
        .agg(count("*").as("cnt"))
        .orderBy("part_date")
        .collect()

      result.foreach { row =>
        println(s"  ${row.getDate(0)}, ${row.getLong(1)}")
      }

      result.length should be(2)
      result(0).getDate(0) should be(Date.valueOf("2024-01-01"))
      result(0).getLong(1) should be(2)
      result(1).getDate(0) should be(Date.valueOf("2024-01-02"))
      result(1).getLong(1) should be(1)
    }
  }

  test("GROUP BY non-partition column only with Date partition should work") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/non_partition_only"

      val schema = StructType(Seq(
        StructField("part_date", DateType, nullable = false),
        StructField("part_hour", StringType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(Date.valueOf("2024-01-01"), "10", "value1"),
        Row(Date.valueOf("2024-01-01"), "11", "value2"),
        Row(Date.valueOf("2024-01-02"), "10", "value3"),
        Row(Date.valueOf("2024-01-02"), "10", "value4")
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

      println("\n=== GROUP BY part_hour only (non-partition column) ===")

      val result = readDf
        .groupBy("part_hour")
        .agg(count("*").as("cnt"))
        .orderBy("part_hour")
        .collect()

      result.foreach { row =>
        println(s"  hour ${row.getString(0)}, count ${row.getLong(1)}")
      }

      result.length should be(2)
      result(0).getString(0) should be("10")
      result(0).getLong(1) should be(3)
      result(1).getString(0) should be("11")
      result(1).getLong(1) should be(1)
    }
  }

  test("simple COUNT without GROUP BY on Date partitioned table should work") {
    withTempPath { tempDir =>
      val tablePath = s"$tempDir/simple_count"

      val schema = StructType(Seq(
        StructField("part_date", DateType, nullable = false),
        StructField("part_hour", StringType, nullable = false),
        StructField("value", StringType, nullable = false)
      ))

      val testData = Seq(
        Row(Date.valueOf("2024-01-01"), "10", "value1"),
        Row(Date.valueOf("2024-01-01"), "11", "value2"),
        Row(Date.valueOf("2024-01-02"), "10", "value3")
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

      println("\n=== Simple COUNT(*) ===")

      val count = readDf.count()
      println(s"  Total count: $count")
      count should be(3)
    }
  }
}
