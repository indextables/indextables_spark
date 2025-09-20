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

package com.tantivy4spark.core

import com.tantivy4spark.TestBase
import org.apache.spark.sql.functions._
import java.sql.Date

/**
 * Test to validate that date filtering works correctly when dates are stored as Date type
 * in the transaction log but WHERE clause specifies them as string literals.
 *
 * This tests partition pruning and filter pushdown for date-partitioned tables.
 */
class DateStringFilterValidationTest extends TestBase {

  test("Date filtering should work with string literals in WHERE clause") {
    withTempPath { tempPath =>
      val tablePath = tempPath.toString

      // Create test data with actual Date types
      val sparkImplicits = spark.implicits
      import sparkImplicits._
      val testData = Seq(
        ("event1", Date.valueOf("2023-01-15"), "content1"),
        ("event2", Date.valueOf("2023-02-20"), "content2"),
        ("event3", Date.valueOf("2023-03-10"), "content3"),
        ("event4", Date.valueOf("2024-01-05"), "content4"),
        ("event5", Date.valueOf("2024-02-14"), "content5")
      ).toDF("id", "event_date", "description")

      // Write partitioned by date
      testData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .partitionBy("event_date")
        .save(tablePath)

      // Read back and verify schema
      val df = spark.read.format("tantivy4spark").load(tablePath)

      // Verify the data is there
      assert(df.count() == 5, "Should have 5 records")

      // Test 1: Exact match with string literal
      val result1 = df.filter($"event_date" === "2023-02-20")
      val rows1 = result1.collect()
      assert(rows1.length == 1, "Should find exactly 1 record for 2023-02-20")
      assert(rows1(0).getAs[String]("id") == "event2", "Should find event2")

      // Test 2: Date range with string literals
      val result2 = df.filter($"event_date" >= "2023-01-01" && $"event_date" < "2024-01-01")
      val rows2 = result2.collect()
      assert(rows2.length == 3, "Should find 3 records for 2023")
      val ids2 = rows2.map(_.getAs[String]("id")).sorted
      assert(ids2.sameElements(Array("event1", "event2", "event3")), "Should find events 1, 2, 3")

      // Test 3: Another exact match
      val result3 = df.filter($"event_date" === "2024-01-05")
      val rows3 = result3.collect()
      assert(rows3.length == 1, "Should find exactly 1 record for 2024-01-05")
      assert(rows3(0).getAs[String]("id") == "event4", "Should find event4")

      // Test 4: IN clause with string dates
      val result4 = df.filter($"event_date".isin("2023-01-15", "2024-02-14"))
      val rows4 = result4.collect()
      assert(rows4.length == 2, "Should find 2 records with IN clause")
      val ids4 = rows4.map(_.getAs[String]("id")).sorted
      assert(ids4.sameElements(Array("event1", "event5")), "Should find events 1 and 5")

      // Test 5: SQL syntax
      df.createOrReplaceTempView("date_test_table")
      val sqlResult = spark.sql("SELECT * FROM date_test_table WHERE event_date = '2023-02-20'")
      val sqlRows = sqlResult.collect()
      assert(sqlRows.length == 1, "SQL query should find exactly 1 record")
      assert(sqlRows(0).getAs[String]("id") == "event2", "SQL should find event2")

      // Test 6: Greater than comparison
      val result6 = df.filter($"event_date" > "2023-12-31")
      val rows6 = result6.collect()
      assert(rows6.length == 2, "Should find 2 records after 2023")
      val ids6 = rows6.map(_.getAs[String]("id")).sorted
      assert(ids6.sameElements(Array("event4", "event5")), "Should find events 4 and 5")

      // Test 7: Non-existent date
      val result7 = df.filter($"event_date" === "2025-01-01")
      assert(result7.count() == 0, "Should find no records for non-existent date")
    }
  }

  test("Date filtering should handle edge cases correctly") {
    withTempPath { tempPath =>
      val tablePath = tempPath.toString

      val sparkImplicits = spark.implicits
      import sparkImplicits._
      val testData = Seq(
        ("edge1", Date.valueOf("2023-12-31"), "last day of year"),
        ("edge2", Date.valueOf("2024-01-01"), "first day of year"),
        ("edge3", Date.valueOf("2024-02-29"), "leap year day"),
        ("edge4", Date.valueOf("2024-12-31"), "end of leap year")
      ).toDF("id", "event_date", "description")

      testData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .partitionBy("event_date")
        .save(tablePath)

      val df = spark.read.format("tantivy4spark").load(tablePath)

      // Test year boundary
      val yearBoundary = df.filter($"event_date" >= "2024-01-01" && $"event_date" <= "2024-01-01")
      assert(yearBoundary.count() == 1, "Should find new year's day")

      // Test leap year day
      val leapDay = df.filter($"event_date" === "2024-02-29")
      assert(leapDay.count() == 1, "Should find leap year day")

      // Test range across year boundary
      val crossYear = df.filter($"event_date" >= "2023-12-30" && $"event_date" <= "2024-01-02")
      assert(crossYear.count() == 2, "Should find records across year boundary")
    }
  }

  test("Date partition pruning should work with string predicates") {
    withTempPath { tempPath =>
      val tablePath = tempPath.toString

      val sparkImplicits = spark.implicits
      import sparkImplicits._
      val testData = Seq(
        ("jan1", Date.valueOf("2023-01-15"), "January data"),
        ("jan2", Date.valueOf("2023-01-20"), "More January data"),
        ("feb1", Date.valueOf("2023-02-10"), "February data"),
        ("mar1", Date.valueOf("2023-03-05"), "March data")
      ).toDF("id", "event_date", "description")

      testData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .partitionBy("event_date")
        .save(tablePath)

      val df = spark.read.format("tantivy4spark").load(tablePath)

      // Filter that should only touch specific partitions
      val januaryData = df.filter($"event_date" >= "2023-01-01" && $"event_date" < "2023-02-01")
      val januaryCount = januaryData.count()
      assert(januaryCount == 2, "Should find 2 January records")

      // Verify the correct records were returned
      val januaryIds = januaryData.collect().map(_.getAs[String]("id")).sorted
      assert(januaryIds.sameElements(Array("jan1", "jan2")), "Should find correct January records")

      // Test single partition access
      val singleDate = df.filter($"event_date" === "2023-02-10")
      assert(singleDate.count() == 1, "Should find single February record")
    }
  }
}