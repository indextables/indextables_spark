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

package com.tantivy4spark.debug

import com.tantivy4spark.TestBase
import org.apache.spark.sql.SaveMode
import java.sql.Timestamp

/**
 * Test to validate timestamp round-trip accuracy for V2 table provider.
 * This test specifically checks for the bug where 2025 timestamps come back as 1970.
 */
class TimestampRoundTripTest extends TestBase {

  private def isNativeLibraryAvailable(): Boolean = {
    try {
      import com.tantivy4spark.search.TantivyNative
      TantivyNative.ensureLibraryLoaded()
    } catch {
      case _: Exception => false
    }
  }

  test("should correctly round-trip timestamp values using V2 table provider") {
    assume(isNativeLibraryAvailable(), "Native Tantivy library not available - skipping integration test")

    withTempPath { tempPath =>
      println(s"🔧 Using temp path: $tempPath")

      // Create timestamp for 2025-01-01 00:00:00
      val timestamp2025 = Timestamp.valueOf("2025-01-01 00:00:00")
      println(s"Original 2025 timestamp: $timestamp2025")
      println(s"Original 2025 timestamp millis: ${timestamp2025.getTime}")

      // Create test data with known timestamps
      val sparkSession = spark
      import sparkSession.implicits._
      val testData = Seq(
        (1, "test_record", timestamp2025)
      ).toDF("id", "name", "timestamp_field")

      println("📋 Schema:")
      testData.printSchema()

      println("📝 Original data:")
      testData.show(false)

      // Write using V2 table provider
      println("💾 Writing data using V2 table provider...")
      testData.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      println("✅ Data written successfully")

      // Read back using V2 table provider
      println("📖 Reading data back using V2 table provider...")
      val readData = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tempPath)

      println("📋 Read schema:")
      readData.printSchema()

      println("📝 Read data:")
      readData.show(false)

      // Collect and verify timestamp values
      val rows = readData.collect()
      assert(rows.length == 1, "Should have exactly one row")

      val readTimestamp = rows(0).getTimestamp(2)
      println(s"Read back timestamp: $readTimestamp")
      println(s"Read back timestamp millis: ${readTimestamp.getTime}")
      println(s"Difference in millis: ${readTimestamp.getTime - timestamp2025.getTime}")

      // Verify timestamp round-trip accuracy
      if (readTimestamp.getTime == timestamp2025.getTime) {
        println("✅ Timestamp round-trip successful!")
      } else {
        println("❌ Timestamp round-trip FAILED!")
        println(s"Expected: ${timestamp2025.getTime} (${timestamp2025})")
        println(s"Got: ${readTimestamp.getTime} (${readTimestamp})")

        // Check if it's the 1970 bug (values near epoch)
        if (readTimestamp.getTime < 100000000000L) { // Less than ~1973
          println("🐛 This appears to be the 1970 timestamp bug!")
        }
      }

      // Test assertion
      assert(readTimestamp.getTime == timestamp2025.getTime,
        s"Timestamp round-trip failed: expected ${timestamp2025.getTime}, got ${readTimestamp.getTime}")
    }
  }

  test("should correctly round-trip multiple timestamp values") {
    assume(isNativeLibraryAvailable(), "Native Tantivy library not available - skipping integration test")

    withTempPath { tempPath =>
      println(s"🔧 Using temp path: $tempPath")

      // Test various timestamps to ensure the bug isn't year-specific
      val timestamps = Seq(
        ("1970-01-01 00:00:00", Timestamp.valueOf("1970-01-01 00:00:00")),
        ("2000-01-01 00:00:00", Timestamp.valueOf("2000-01-01 00:00:00")),
        ("2024-06-15 12:30:45", Timestamp.valueOf("2024-06-15 12:30:45")),
        ("2025-01-01 00:00:00", Timestamp.valueOf("2025-01-01 00:00:00")),
        ("2030-12-31 23:59:59", Timestamp.valueOf("2030-12-31 23:59:59"))
      )

      val sparkSession = spark
      import sparkSession.implicits._
      val testData = timestamps.zipWithIndex.map { case ((label, ts), idx) =>
        (idx + 1, label, ts)
      }.toDF("id", "label", "timestamp_field")

      println("📝 Original data:")
      testData.show(false)

      // Write and read back
      testData.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      val readData = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(tempPath)

      println("📝 Read data:")
      readData.show(false)

      // Verify all timestamps
      val readRows = readData.collect().toSeq.sortBy(_.getInt(0))
      val originalRows = testData.collect().toSeq.sortBy(_.getInt(0))

      assert(readRows.length == originalRows.length, "Row count mismatch")

      readRows.zip(originalRows).foreach { case (readRow, originalRow) =>
        val readTs = readRow.getTimestamp(2)
        val originalTs = originalRow.getTimestamp(2)
        val label = originalRow.getString(1)

        println(s"Verifying $label: original=${originalTs.getTime}, read=${readTs.getTime}")

        assert(readTs.getTime == originalTs.getTime,
          s"Timestamp mismatch for $label: expected ${originalTs.getTime}, got ${readTs.getTime}")
      }

      println("✅ All timestamp round-trips successful!")
    }
  }
}