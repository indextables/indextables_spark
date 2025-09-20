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

package com.tantivy4spark.transaction

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.sources._
import java.sql.Date

/**
 * Unit tests for date filtering in partition pruning that don't require Spark.
 * Tests the core logic of comparing partition values (stored as strings) with filter values.
 */
class PartitionPruningDateFilterTest extends AnyFunSuite with Matchers {

  test("String date partition values should match string filter values") {
    // Simulate how partition values are stored in transaction log (as strings)
    val partitionValues = Map("event_date" -> "2023-02-20")

    // Test exact string match
    val stringFilter = EqualTo("event_date", "2023-02-20")
    val result = PartitionPruning.evaluateFilter(partitionValues, stringFilter)

    result shouldBe true
  }

  test("String date partition values should work with Date object filters") {
    val partitionValues = Map("event_date" -> "2023-02-20")

    // Test with Date object (Spark may convert string literals to Date objects)
    val dateFilter = EqualTo("event_date", Date.valueOf("2023-02-20"))
    val result = PartitionPruning.evaluateFilter(partitionValues, dateFilter)

    result shouldBe true
  }

  test("Date range filtering should work with string comparisons") {
    val addActions = Seq(
      AddAction(
        path = "s3://bucket/path/event_date=2023-01-15/file1.split",
        partitionValues = Map("event_date" -> "2023-01-15"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/event_date=2023-02-20/file2.split",
        partitionValues = Map("event_date" -> "2023-02-20"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/event_date=2023-03-10/file3.split",
        partitionValues = Map("event_date" -> "2023-03-10"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/event_date=2024-01-05/file4.split",
        partitionValues = Map("event_date" -> "2024-01-05"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val partitionColumns = Seq("event_date")

    // Test range filter: 2023 data only
    val rangeFilters: Array[Filter] = Array(
      GreaterThanOrEqual("event_date", "2023-01-01"),
      LessThan("event_date", "2024-01-01")
    )

    val prunedActions = PartitionPruning.prunePartitions(addActions, partitionColumns, rangeFilters)

    prunedActions should have length 3
    prunedActions.map(_.partitionValues("event_date")) should contain allOf (
      "2023-01-15", "2023-02-20", "2023-03-10"
    )
  }

  test("Exact date filtering should return single partition") {
    val addActions = Seq(
      AddAction(
        path = "s3://bucket/path/event_date=2023-01-15/file1.split",
        partitionValues = Map("event_date" -> "2023-01-15"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/event_date=2023-02-20/file2.split",
        partitionValues = Map("event_date" -> "2023-02-20"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/event_date=2023-03-10/file3.split",
        partitionValues = Map("event_date" -> "2023-03-10"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val partitionColumns = Seq("event_date")
    val exactFilter: Array[Filter] = Array(EqualTo("event_date", "2023-02-20"))

    val prunedActions = PartitionPruning.prunePartitions(addActions, partitionColumns, exactFilter)

    prunedActions should have length 1
    prunedActions.head.partitionValues("event_date") shouldBe "2023-02-20"
  }

  test("Non-existent date should return no partitions") {
    val addActions = Seq(
      AddAction(
        path = "s3://bucket/path/event_date=2023-01-15/file1.split",
        partitionValues = Map("event_date" -> "2023-01-15"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/event_date=2023-02-20/file2.split",
        partitionValues = Map("event_date" -> "2023-02-20"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val partitionColumns = Seq("event_date")
    val nonExistentFilter: Array[Filter] = Array(EqualTo("event_date", "2025-01-01"))

    val prunedActions = PartitionPruning.prunePartitions(addActions, partitionColumns, nonExistentFilter)

    prunedActions should have length 0
  }

  test("ISO 8601 date string ordering should preserve chronological order") {
    val dates = List(
      "2022-12-31",
      "2023-01-01",
      "2023-01-15",
      "2023-02-20",
      "2023-12-31",
      "2024-01-01",
      "2024-02-29"  // leap year
    )

    val sortedDates = dates.sorted

    // Verify that string sorting maintains chronological order for ISO 8601 dates
    sortedDates shouldBe dates
  }

  test("IN clause filtering should work with multiple date values") {
    val addActions = Seq(
      AddAction(
        path = "s3://bucket/path/event_date=2023-01-15/file1.split",
        partitionValues = Map("event_date" -> "2023-01-15"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/event_date=2023-02-20/file2.split",
        partitionValues = Map("event_date" -> "2023-02-20"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/event_date=2023-03-10/file3.split",
        partitionValues = Map("event_date" -> "2023-03-10"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/event_date=2024-01-05/file4.split",
        partitionValues = Map("event_date" -> "2024-01-05"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val partitionColumns = Seq("event_date")
    val inFilter: Array[Filter] = Array(In("event_date", Array("2023-01-15", "2024-01-05")))

    val prunedActions = PartitionPruning.prunePartitions(addActions, partitionColumns, inFilter)

    prunedActions should have length 2
    prunedActions.map(_.partitionValues("event_date")) should contain allOf (
      "2023-01-15", "2024-01-05"
    )
  }

  test("Date objects should work with partition filtering") {
    val partitionValues = Map("event_date" -> "2023-02-20")

    // Test with java.sql.Date object
    val dateFilter = EqualTo("event_date", java.sql.Date.valueOf("2023-02-20"))
    val result = PartitionPruning.evaluateFilter(partitionValues, dateFilter)
    result shouldBe true

    // Test with different date
    val differentDateFilter = EqualTo("event_date", java.sql.Date.valueOf("2023-02-21"))
    val differentResult = PartitionPruning.evaluateFilter(partitionValues, differentDateFilter)
    differentResult shouldBe false

    // Test with range using Date objects
    val rangeStartFilter = GreaterThanOrEqual("event_date", java.sql.Date.valueOf("2023-02-01"))
    val rangeEndFilter = LessThan("event_date", java.sql.Date.valueOf("2023-03-01"))

    val rangeStartResult = PartitionPruning.evaluateFilter(partitionValues, rangeStartFilter)
    val rangeEndResult = PartitionPruning.evaluateFilter(partitionValues, rangeEndFilter)

    rangeStartResult shouldBe true
    rangeEndResult shouldBe true
  }

  test("Timestamp objects should work with partition filtering") {
    // Test with ISO timestamp string partition value
    val timestampPartitionValues = Map("event_time" -> "2023-02-20 14:30:00")

    val timestamp = java.sql.Timestamp.valueOf("2023-02-20 14:30:00")
    val timestampFilter = EqualTo("event_time", timestamp)
    val result = PartitionPruning.evaluateFilter(timestampPartitionValues, timestampFilter)
    result shouldBe true

    // Test with different timestamp
    val differentTimestamp = java.sql.Timestamp.valueOf("2023-02-20 14:30:01")
    val differentFilter = EqualTo("event_time", differentTimestamp)
    val differentResult = PartitionPruning.evaluateFilter(timestampPartitionValues, differentFilter)
    differentResult shouldBe false

    // Test with epoch millis partition value
    val epochPartitionValues = Map("event_time" -> "1676902200000") // 2023-02-20 14:30:00 UTC in millis
    val epochTimestamp = new java.sql.Timestamp(1676902200000L)
    val epochFilter = EqualTo("event_time", epochTimestamp)
    val epochResult = PartitionPruning.evaluateFilter(epochPartitionValues, epochFilter)
    epochResult shouldBe true
  }

  test("BigDecimal objects should work with partition filtering") {
    val decimalPartitionValues = Map("price" -> "123.456")

    // Test with java.math.BigDecimal
    val javaBigDecimal = new java.math.BigDecimal("123.456")
    val javaFilter = EqualTo("price", javaBigDecimal)
    val javaResult = PartitionPruning.evaluateFilter(decimalPartitionValues, javaFilter)
    javaResult shouldBe true

    // Test with scala.math.BigDecimal
    val scalaBigDecimal = scala.math.BigDecimal("123.456")
    val scalaFilter = EqualTo("price", scalaBigDecimal)
    val scalaResult = PartitionPruning.evaluateFilter(decimalPartitionValues, scalaFilter)
    scalaResult shouldBe true

    // Test with different decimal value
    val differentDecimal = new java.math.BigDecimal("123.457")
    val differentFilter = EqualTo("price", differentDecimal)
    val differentResult = PartitionPruning.evaluateFilter(decimalPartitionValues, differentFilter)
    differentResult shouldBe false

    // Test range comparison
    val lowerBound = new java.math.BigDecimal("123.000")
    val upperBound = new java.math.BigDecimal("124.000")

    val lowerFilter = GreaterThan("price", lowerBound)
    val upperFilter = LessThan("price", upperBound)

    val lowerResult = PartitionPruning.evaluateFilter(decimalPartitionValues, lowerFilter)
    val upperResult = PartitionPruning.evaluateFilter(decimalPartitionValues, upperFilter)

    lowerResult shouldBe true
    upperResult shouldBe true
  }

  test("Mixed data type partition pruning should work end-to-end") {
    val addActions = Seq(
      AddAction(
        path = "s3://bucket/path/event_date=2023-01-15/price=100.50/file1.split",
        partitionValues = Map("event_date" -> "2023-01-15", "price" -> "100.50"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/event_date=2023-02-20/price=200.75/file2.split",
        partitionValues = Map("event_date" -> "2023-02-20", "price" -> "200.75"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/event_date=2023-03-10/price=150.25/file3.split",
        partitionValues = Map("event_date" -> "2023-03-10", "price" -> "150.25"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val partitionColumns = Seq("event_date", "price")

    // Test with mixed Date and BigDecimal filters
    val filters: Array[Filter] = Array(
      GreaterThanOrEqual("event_date", java.sql.Date.valueOf("2023-02-01")),
      LessThan("price", new java.math.BigDecimal("180.00"))
    )

    val prunedActions = PartitionPruning.prunePartitions(addActions, partitionColumns, filters)

    // Should return only the March record (2023-03-10, price 150.25)
    prunedActions should have length 1
    prunedActions.head.partitionValues("event_date") shouldBe "2023-03-10"
    prunedActions.head.partitionValues("price") shouldBe "150.25"
  }

  test("AND conditions should work correctly") {
    val addActions = Seq(
      AddAction(
        path = "s3://bucket/path/date=2023-01-15/status=active/file1.split",
        partitionValues = Map("date" -> "2023-01-15", "status" -> "active"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-01-15/status=inactive/file2.split",
        partitionValues = Map("date" -> "2023-01-15", "status" -> "inactive"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-02-20/status=active/file3.split",
        partitionValues = Map("date" -> "2023-02-20", "status" -> "active"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-02-20/status=inactive/file4.split",
        partitionValues = Map("date" -> "2023-02-20", "status" -> "inactive"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val partitionColumns = Seq("date", "status")

    // Test AND condition: date = 2023-01-15 AND status = active
    val andFilters: Array[Filter] = Array(
      And(
        EqualTo("date", "2023-01-15"),
        EqualTo("status", "active")
      )
    )

    val andResult = PartitionPruning.prunePartitions(addActions, partitionColumns, andFilters)

    andResult should have length 1
    andResult.head.partitionValues("date") shouldBe "2023-01-15"
    andResult.head.partitionValues("status") shouldBe "active"
  }

  test("OR conditions should work correctly") {
    val addActions = Seq(
      AddAction(
        path = "s3://bucket/path/date=2023-01-15/status=active/file1.split",
        partitionValues = Map("date" -> "2023-01-15", "status" -> "active"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-01-15/status=inactive/file2.split",
        partitionValues = Map("date" -> "2023-01-15", "status" -> "inactive"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-02-20/status=active/file3.split",
        partitionValues = Map("date" -> "2023-02-20", "status" -> "active"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-02-20/status=inactive/file4.split",
        partitionValues = Map("date" -> "2023-02-20", "status" -> "inactive"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val partitionColumns = Seq("date", "status")

    // Test OR condition: date = 2023-01-15 OR status = active
    val orFilters: Array[Filter] = Array(
      Or(
        EqualTo("date", "2023-01-15"),
        EqualTo("status", "active")
      )
    )

    val orResult = PartitionPruning.prunePartitions(addActions, partitionColumns, orFilters)

    // Should return 3 partitions: 2023-01-15/active, 2023-01-15/inactive, 2023-02-20/active
    orResult should have length 3
    val resultKeys = orResult.map(a => s"${a.partitionValues("date")}/${a.partitionValues("status")}").sorted
    resultKeys shouldBe Array("2023-01-15/active", "2023-01-15/inactive", "2023-02-20/active")
  }

  test("Complex nested AND/OR conditions should work correctly") {
    val addActions = Seq(
      AddAction(
        path = "s3://bucket/path/date=2023-01-15/status=active/region=us/file1.split",
        partitionValues = Map("date" -> "2023-01-15", "status" -> "active", "region" -> "us"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-01-15/status=active/region=eu/file2.split",
        partitionValues = Map("date" -> "2023-01-15", "status" -> "active", "region" -> "eu"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-02-20/status=inactive/region=us/file3.split",
        partitionValues = Map("date" -> "2023-02-20", "status" -> "inactive", "region" -> "us"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-02-20/status=active/region=asia/file4.split",
        partitionValues = Map("date" -> "2023-02-20", "status" -> "active", "region" -> "asia"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val partitionColumns = Seq("date", "status", "region")

    // Test complex condition: (date = 2023-01-15 AND status = active) OR (date = 2023-02-20 AND region = us)
    val complexFilters: Array[Filter] = Array(
      Or(
        And(
          EqualTo("date", "2023-01-15"),
          EqualTo("status", "active")
        ),
        And(
          EqualTo("date", "2023-02-20"),
          EqualTo("region", "us")
        )
      )
    )

    val complexResult = PartitionPruning.prunePartitions(addActions, partitionColumns, complexFilters)

    // Should return 3 partitions:
    // - 2023-01-15/active/us (matches first AND)
    // - 2023-01-15/active/eu (matches first AND)
    // - 2023-02-20/inactive/us (matches second AND)
    complexResult should have length 3
    val resultKeys = complexResult.map(a =>
      s"${a.partitionValues("date")}/${a.partitionValues("status")}/${a.partitionValues("region")}"
    ).sorted
    resultKeys shouldBe Array("2023-01-15/active/eu", "2023-01-15/active/us", "2023-02-20/inactive/us")
  }

  test("Mixed AND/OR with different data types should work correctly") {
    val addActions = Seq(
      AddAction(
        path = "s3://bucket/path/date=2023-01-15/price=100.50/file1.split",
        partitionValues = Map("date" -> "2023-01-15", "price" -> "100.50"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-02-20/price=200.75/file2.split",
        partitionValues = Map("date" -> "2023-02-20", "price" -> "200.75"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-03-10/price=50.25/file3.split",
        partitionValues = Map("date" -> "2023-03-10", "price" -> "50.25"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val partitionColumns = Seq("date", "price")

    // Test: (date >= 2023-02-01 AND price < 150.00) OR date = 2023-01-15
    val mixedFilters: Array[Filter] = Array(
      Or(
        And(
          GreaterThanOrEqual("date", java.sql.Date.valueOf("2023-02-01")),
          LessThan("price", new java.math.BigDecimal("150.00"))
        ),
        EqualTo("date", "2023-01-15")
      )
    )

    val mixedResult = PartitionPruning.prunePartitions(addActions, partitionColumns, mixedFilters)

    // Should return 2 partitions:
    // - 2023-01-15/100.50 (matches OR condition: date = 2023-01-15)
    // - 2023-03-10/50.25 (matches AND condition: date >= 2023-02-01 AND price < 150.00)
    // Note: 2023-02-20/200.75 doesn't match because price >= 150.00
    mixedResult should have length 2
    val resultKeys = mixedResult.map(a => s"${a.partitionValues("date")}/${a.partitionValues("price")}").sorted
    resultKeys shouldBe Array("2023-01-15/100.50", "2023-03-10/50.25")
  }

  test("Should properly honor partition keys vs non-partition columns") {
    val addActions = Seq(
      AddAction(
        path = "s3://bucket/path/date=2023-01-15/status=active/file1.split",
        partitionValues = Map("date" -> "2023-01-15", "status" -> "active"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-02-20/status=inactive/file2.split",
        partitionValues = Map("date" -> "2023-02-20", "status" -> "inactive"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    // Only 'date' is a partition column, 'status' and 'content' are not
    val partitionColumns = Seq("date")

    // Test 1: Filter only on partition columns - should be applied
    val partitionOnlyFilters: Array[Filter] = Array(
      EqualTo("date", "2023-01-15")
    )
    val partitionResult = PartitionPruning.prunePartitions(addActions, partitionColumns, partitionOnlyFilters)
    partitionResult should have length 1
    partitionResult.head.partitionValues("date") shouldBe "2023-01-15"

    // Test 2: Filter only on non-partition columns - should be ignored (all partitions returned)
    val nonPartitionFilters: Array[Filter] = Array(
      EqualTo("content", "some text")  // 'content' is not a partition column
    )
    val nonPartitionResult = PartitionPruning.prunePartitions(addActions, partitionColumns, nonPartitionFilters)
    nonPartitionResult should have length 2  // Should return all partitions since filter can't be applied

    // Test 3: Mixed filter (partition + non-partition columns) - needs special handling
    val mixedFilters: Array[Filter] = Array(
      And(
        EqualTo("date", "2023-01-15"),     // partition column
        EqualTo("content", "some text")    // non-partition column
      )
    )
    val mixedResult = PartitionPruning.prunePartitions(addActions, partitionColumns, mixedFilters)
    // Current implementation might incorrectly apply this filter
    println(s"Mixed filter result count: ${mixedResult.length}")
  }

  test("Should handle complex filters with mixed partition and non-partition columns") {
    val addActions = Seq(
      AddAction(
        path = "s3://bucket/path/date=2023-01-15/region=us/file1.split",
        partitionValues = Map("date" -> "2023-01-15", "region" -> "us"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-02-20/region=eu/file2.split",
        partitionValues = Map("date" -> "2023-02-20", "region" -> "eu"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-03-10/region=us/file3.split",
        partitionValues = Map("date" -> "2023-03-10", "region" -> "us"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    // Both 'date' and 'region' are partition columns, but 'content' and 'user_id' are not
    val partitionColumns = Seq("date", "region")

    // Test complex filter: (date >= 2023-02-01 AND region = us) OR (user_id = 123 AND content contains 'test')
    val complexFilters: Array[Filter] = Array(
      Or(
        And(
          GreaterThanOrEqual("date", "2023-02-01"),  // partition column
          EqualTo("region", "us")                    // partition column
        ),
        And(
          EqualTo("user_id", "123"),                 // non-partition column
          StringContains("content", "test")          // non-partition column
        )
      )
    )

    val complexResult = PartitionPruning.prunePartitions(addActions, partitionColumns, complexFilters)

    // The first part of the OR should be applied: (date >= 2023-02-01 AND region = us)
    // This should match only: 2023-03-10/us
    // The second part should be ignored since it only references non-partition columns
    println(s"Complex filter result count: ${complexResult.length}")
    if (complexResult.nonEmpty) {
      complexResult.foreach(a => println(s"  ${a.partitionValues("date")}/${a.partitionValues("region")}"))
    }
  }

  test("Enhanced partition filtering should properly extract partition-relevant parts") {
    val addActions = Seq(
      AddAction(
        path = "s3://bucket/path/date=2023-01-15/region=us/file1.split",
        partitionValues = Map("date" -> "2023-01-15", "region" -> "us"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-02-20/region=eu/file2.split",
        partitionValues = Map("date" -> "2023-02-20", "region" -> "eu"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/date=2023-03-10/region=us/file3.split",
        partitionValues = Map("date" -> "2023-03-10", "region" -> "us"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val partitionColumns = Seq("date", "region")

    // Test 1: Pure partition filter should work normally
    val purePartitionFilter: Array[Filter] = Array(
      And(
        EqualTo("date", "2023-01-15"),
        EqualTo("region", "us")
      )
    )
    val pureResult = PartitionPruning.prunePartitions(addActions, partitionColumns, purePartitionFilter)
    pureResult should have length 1
    pureResult.head.partitionValues("date") shouldBe "2023-01-15"

    // Test 2: Mixed filter should extract only partition parts
    val mixedFilter: Array[Filter] = Array(
      And(
        EqualTo("date", "2023-01-15"),        // partition column - should be applied
        EqualTo("content", "some text")       // non-partition column - should be ignored
      )
    )
    val mixedResult = PartitionPruning.prunePartitions(addActions, partitionColumns, mixedFilter)
    // Should apply only the date filter, returning the 2023-01-15 partition
    mixedResult should have length 1
    mixedResult.head.partitionValues("date") shouldBe "2023-01-15"

    // Test 3: Complex mixed OR filter
    val complexMixedFilter: Array[Filter] = Array(
      Or(
        And(
          GreaterThanOrEqual("date", "2023-02-01"),  // partition column
          EqualTo("region", "us")                    // partition column
        ),
        And(
          EqualTo("user_id", "123"),                 // non-partition column - should be ignored
          StringContains("content", "test")          // non-partition column - should be ignored
        )
      )
    )
    val complexResult = PartitionPruning.prunePartitions(addActions, partitionColumns, complexMixedFilter)
    // Should extract and apply: (date >= 2023-02-01 AND region = us)
    // This matches only: 2023-03-10/us
    complexResult should have length 1
    complexResult.head.partitionValues("date") shouldBe "2023-03-10"
    complexResult.head.partitionValues("region") shouldBe "us"
  }

  test("Multiple partition columns should work with date filtering") {
    val addActions = Seq(
      AddAction(
        path = "s3://bucket/path/event_date=2023-01-15/load_hour=10/file1.split",
        partitionValues = Map("event_date" -> "2023-01-15", "load_hour" -> "10"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/event_date=2023-01-15/load_hour=11/file2.split",
        partitionValues = Map("event_date" -> "2023-01-15", "load_hour" -> "11"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      ),
      AddAction(
        path = "s3://bucket/path/event_date=2023-02-20/load_hour=10/file3.split",
        partitionValues = Map("event_date" -> "2023-02-20", "load_hour" -> "10"),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    )

    val partitionColumns = Seq("event_date", "load_hour")
    val filters: Array[Filter] = Array(
      EqualTo("event_date", "2023-01-15"),
      EqualTo("load_hour", "10")
    )

    val prunedActions = PartitionPruning.prunePartitions(addActions, partitionColumns, filters)

    prunedActions should have length 1
    val action = prunedActions.head
    action.partitionValues("event_date") shouldBe "2023-01-15"
    action.partitionValues("load_hour") shouldBe "10"
  }
}