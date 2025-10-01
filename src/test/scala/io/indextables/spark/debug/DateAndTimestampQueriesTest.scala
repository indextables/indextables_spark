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

package io.indextables.spark.debug

import io.indextables.spark.TestBase
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}
import scala.util.Random

/**
 * Isolated test for the failing "should handle date and timestamp queries" test. This helps debug date/timestamp type
 * conversions and query handling.
 */
class DateAndTimestampQueriesTest extends TestBase {

  private def isNativeLibraryAvailable(): Boolean =
    try {
      import io.indextables.spark.search.TantivyNative
      TantivyNative.ensureLibraryLoaded()
    } catch {
      case _: Exception => false
    }

  private def createDateTimeTestDataFrame(): DataFrame = {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    val _random = new Random(42)
    val events  = Array("user_signup", "purchase", "login", "logout", "page_view", "order_placed")
    val users   = Array("alice", "bob", "charlie", "diana", "eve", "frank")

    val dateTimeData = 1.to(200).map { i =>
      val baseDate     = LocalDate.of(2023, 1, 1).plusDays(_random.nextInt(365))
      val baseDateTime = baseDate.atTime(_random.nextInt(24), _random.nextInt(60))

      (
        i.toLong,
        users(_random.nextInt(users.length)),
        events(_random.nextInt(events.length)),
        Date.valueOf(baseDate),
        Timestamp.valueOf(baseDateTime),
        baseDate.getYear,
        baseDate.getMonthValue,
        baseDate.getDayOfMonth
      )
    }

    spark
      .createDataFrame(dateTimeData)
      .toDF(
        "id",
        "user_name",
        "event_type",
        "created_date",
        "created_timestamp",
        "year",
        "month",
        "day"
      )
  }

  test("should handle date and timestamp queries") {
    assume(isNativeLibraryAvailable(), "Native Tantivy library not available - skipping integration test")

    withTempPath { tempPath =>
      println(s"🔧 Using temp path: $tempPath")

      val testData = createDateTimeTestDataFrame()
      println(s"📊 Created test data with ${testData.count()} rows")
      println(s"📋 Schema: ${testData.schema.fields.map(f => s"${f.name}:${f.dataType}").mkString(", ")}")

      // Show sample data
      println("📝 Sample data:")
      testData.show(5, false)

      // Write data
      println("💾 Writing data...")
      testData.write
        .format("tantivy4spark")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      println("✅ Data written successfully")

      // Read the data back
      println("📖 Reading data back...")
      val readData = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      println(s"📊 Read back ${readData.count()} rows")
      println(s"📋 Read schema: ${readData.schema.fields.map(f => s"${f.name}:${f.dataType}").mkString(", ")}")

      // Show sample read data
      println("📝 Sample read data:")
      readData.show(5, false)

      // Test date and timestamp query operations
      println("\n🔍 Testing date and timestamp queries...")

      val dateQueries = Seq(
        // Basic date comparisons
        readData.filter(col("created_date") > "2023-06-01"),
        readData.filter(col("created_date") < "2023-12-31"),
        readData.filter(col("created_date").between("2023-03-01", "2023-09-30")),

        // Timestamp comparisons
        readData.filter(col("created_timestamp") > "2023-06-01 00:00:00"),
        readData.filter(col("created_timestamp") < "2023-12-31 23:59:59"),

        // Date functions (these may not push down but should not crash)
        readData.filter(year(col("created_date")) === 2023),
        readData.filter(month(col("created_date")) === 6),
        readData.filter(dayofweek(col("created_date")) === 2) // Monday
      )

      // Execute queries to verify they don't crash (results may be empty if filtering not implemented)
      dateQueries.zipWithIndex.foreach {
        case (query, index) =>
          println(s"  🔎 Executing date query ${index + 1}...")
          noException should be thrownBy {
            val count = query.count()
            println(s"    Found $count results")
          }
      }

      println("✅ All date and timestamp queries completed successfully")
    }
  }
}
