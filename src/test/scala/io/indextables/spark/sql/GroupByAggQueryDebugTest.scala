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

package io.indextables.spark.sql

import java.io.File
import java.nio.file.Files
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Debug test to show Tantivy queries for GROUP BY aggregations.
 *
 * Schema: message_timestamp (timestamp), id(string), logGroup (string), logStream (string), message(text), messageType
 * (string), owner (int), subscriptionFilter (array[string]), message_date (date - partition-key), message_hour
 * (string), asv (string)
 */
class GroupByAggQueryDebugTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tempDir: File       = _
  private var tablePath: String   = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Set debug logging to see Tantivy queries
    org.apache.log4j.Logger
      .getLogger("io.indextables.spark.core.IndexTables4SparkGroupByAggregateScan")
      .setLevel(org.apache.log4j.Level.DEBUG)
    org.apache.log4j.Logger
      .getLogger("io.indextables.spark.core.IndexTables4SparkScanBuilder")
      .setLevel(org.apache.log4j.Level.DEBUG)
    org.apache.log4j.Logger.getLogger("io.indextables.tantivy4java.aggregation").setLevel(org.apache.log4j.Level.DEBUG)
    org.apache.log4j.Logger.getLogger("io.indextables.spark.search").setLevel(org.apache.log4j.Level.DEBUG)

    spark = SparkSession
      .builder()
      .appName("GroupByAggQueryDebugTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    tempDir = Files.createTempDirectory("groupby_agg_debug_test").toFile
    tablePath = new File(tempDir, "my_index").getAbsolutePath

    // Create and write test data
    createTestData()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir)
    }
    super.afterAll()
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  private def createTestData(): Unit = {
    val schema = StructType(
      Seq(
        StructField("message_timestamp", TimestampType, nullable = false),
        StructField("id", StringType, nullable = false),
        StructField("logGroup", StringType, nullable = true),
        StructField("logStream", StringType, nullable = true),
        StructField("message", StringType, nullable = true),
        StructField("messageType", StringType, nullable = true),
        StructField("owner", IntegerType, nullable = true),
        StructField("subscriptionFilter", ArrayType(StringType), nullable = true),
        StructField("message_date", DateType, nullable = false),
        StructField("message_hour", StringType, nullable = true),
        StructField("asv", StringType, nullable = false)
      )
    )

    val data = Seq(
      // message_date = 2024-01-01, asv = "v1" - 3 records
      Row(
        Timestamp.valueOf("2024-01-01 10:00:00"),
        "id1",
        "group1",
        "stream1",
        "hello world",
        "INFO",
        100,
        Seq("filter1"),
        Date.valueOf("2024-01-01"),
        "10",
        "v1"
      ),
      Row(
        Timestamp.valueOf("2024-01-01 10:30:00"),
        "id2",
        "group1",
        "stream1",
        "test message",
        "INFO",
        100,
        Seq("filter1"),
        Date.valueOf("2024-01-01"),
        "10",
        "v1"
      ),
      Row(
        Timestamp.valueOf("2024-01-01 11:00:00"),
        "id3",
        "group1",
        "stream2",
        "another msg",
        "WARN",
        101,
        Seq("filter2"),
        Date.valueOf("2024-01-01"),
        "11",
        "v1"
      ),

      // message_date = 2024-01-01, asv = "v2" - 2 records
      Row(
        Timestamp.valueOf("2024-01-01 12:00:00"),
        "id4",
        "group2",
        "stream1",
        "error occurred",
        "ERROR",
        102,
        Seq("filter1", "filter2"),
        Date.valueOf("2024-01-01"),
        "12",
        "v2"
      ),
      Row(
        Timestamp.valueOf("2024-01-01 13:00:00"),
        "id5",
        "group2",
        "stream2",
        "debug info",
        "DEBUG",
        102,
        Seq("filter3"),
        Date.valueOf("2024-01-01"),
        "13",
        "v2"
      ),

      // message_date = 2024-01-02, asv = "v1" - 2 records
      Row(
        Timestamp.valueOf("2024-01-02 09:00:00"),
        "id6",
        "group1",
        "stream1",
        "morning log",
        "INFO",
        100,
        Seq("filter1"),
        Date.valueOf("2024-01-02"),
        "09",
        "v1"
      ),
      Row(
        Timestamp.valueOf("2024-01-02 10:00:00"),
        "id7",
        "group1",
        "stream1",
        "mid morning",
        "INFO",
        100,
        Seq("filter1"),
        Date.valueOf("2024-01-02"),
        "10",
        "v1"
      ),

      // message_date = 2024-01-02, asv = "v2" - 1 record
      Row(
        Timestamp.valueOf("2024-01-02 14:00:00"),
        "id8",
        "group2",
        "stream1",
        "afternoon log",
        "INFO",
        103,
        Seq("filter2"),
        Date.valueOf("2024-01-02"),
        "14",
        "v2"
      ),

      // message_date = 2024-01-03, asv = "v1" - 1 record
      Row(
        Timestamp.valueOf("2024-01-03 08:00:00"),
        "id9",
        "group1",
        "stream1",
        "early log",
        "INFO",
        100,
        Seq("filter1"),
        Date.valueOf("2024-01-03"),
        "08",
        "v1"
      )
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    println("\n" + "=" * 80)
    println("=== WRITING TEST DATA ===")
    println("=" * 80)
    df.show(false)

    // Write with partitioning by message_date
    // Using DEFAULT fast fields configuration (auto-configured)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.message", "text")
      .partitionBy("message_date")
      .mode("overwrite")
      .save(tablePath)

    println(s"Data written to: $tablePath")
  }

  test("GROUP BY message_date, asv with COUNT should show Tantivy aggregation queries") {
    println("\n" + "=" * 80)
    println("=== EXECUTING GROUP BY QUERY ===")
    println("=" * 80)

    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    readDf.createOrReplaceTempView("my_index")

    println("\n=== Schema ===")
    readDf.printSchema()

    println("\n=== Query: SELECT message_date, asv, count(asv) FROM my_index GROUP BY 1,2 ORDER BY 1,2 ===\n")

    val result = spark.sql("""
      SELECT message_date, asv, count(asv) as cnt
      FROM my_index
      GROUP BY 1, 2
      ORDER BY 1, 2
    """)

    // Show explain plan
    println("\n=== EXPLAIN PLAN ===")
    result.explain(true)

    println("\n=== RESULTS ===")
    result.show(false)

    // Verify results
    val rows = result.collect()
    rows.length shouldBe 5

    // Expected:
    // 2024-01-01, v1 -> 3
    // 2024-01-01, v2 -> 2
    // 2024-01-02, v1 -> 2
    // 2024-01-02, v2 -> 1
    // 2024-01-03, v1 -> 1

    println("\n=== EXPECTED COUNTS ===")
    println("2024-01-01, v1 -> 3")
    println("2024-01-01, v2 -> 2")
    println("2024-01-02, v1 -> 2")
    println("2024-01-02, v2 -> 1")
    println("2024-01-03, v1 -> 1")

    // The key thing to look for in the logs:
    // 1. "GROUP BY EXECUTION: Creating TermsAggregation for 2 column(s): message_date, asv"
    // 2. "GROUP BY EXECUTION: Multi-dimensional GROUP BY on 2 fields: message_date, asv"
    // 3. "GROUP BY EXECUTION: Using native MultiTermsAggregation"
    // 4. "GROUP BY EXECUTION: COUNT aggregation at index 0 will use bucket doc count"
    println("\n" + "=" * 80)
    println("=== LOOK FOR THESE LOG LINES ABOVE ===")
    println("=" * 80)
    println("1. 'Creating TermsAggregation for 2 column(s): message_date, asv'")
    println("2. 'Multi-dimensional GROUP BY on 2 fields: message_date, asv'")
    println("3. 'Using native MultiTermsAggregation'")
    println("4. 'COUNT aggregation at index 0 will use bucket doc count'")
    println("=" * 80)
  }

  test("GROUP BY on partition-only column should use optimized aggregation path") {
    println("\n" + "=" * 80)
    println("=== PARTITION-ONLY GROUP BY OPTIMIZATION TEST ===")
    println("=== This tests GROUP BY on just the partition key ===")
    println("=" * 80)

    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    readDf.createOrReplaceTempView("my_index_partition_test")

    // Query that groups by partition column only - should use optimized path
    println("\n=== Query: SELECT message_date, count(*) FROM my_index_partition_test GROUP BY 1 ORDER BY 1 ===\n")

    val result = spark.sql("""
      SELECT message_date, count(*) as cnt
      FROM my_index_partition_test
      GROUP BY 1
      ORDER BY 1
    """)

    println("=== RESULTS ===")
    result.show(false)

    // Verify results - should see counts per date
    val rows = result.collect()
    rows.length shouldBe 3 // 3 distinct dates

    // Expected:
    // 2024-01-01 -> 5
    // 2024-01-02 -> 3
    // 2024-01-03 -> 1

    println("\n=== Expected counts ===")
    println("2024-01-01 -> 5")
    println("2024-01-02 -> 3")
    println("2024-01-03 -> 1")

    // Look for optimization log lines:
    // "GROUP BY OPTIMIZATION: All GROUP BY columns are partition columns - using simplified aggregation"
    println("\n=== LOOK FOR LOG LINE: 'GROUP BY OPTIMIZATION: All GROUP BY columns are partition columns' ===")
  }

  test("GROUP BY on mixed partition and data columns should use optimized path") {
    println("\n" + "=" * 80)
    println("=== MIXED PARTITION/DATA COLUMN OPTIMIZATION TEST ===")
    println("=== Tests GROUP BY on partition_key + regular_field ===")
    println("=" * 80)

    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    readDf.createOrReplaceTempView("my_index_mixed_test")

    // Query that groups by partition column + data column
    // Should optimize by only running Tantivy aggregation on 'asv' and injecting partition values
    println("\n=== Query: SELECT message_date, asv, count(asv) FROM my_index_mixed_test GROUP BY 1,2 ORDER BY 1,2 ===\n")

    val result = spark.sql("""
      SELECT message_date, asv, count(asv) as cnt
      FROM my_index_mixed_test
      GROUP BY 1, 2
      ORDER BY 1, 2
    """)

    println("=== RESULTS ===")
    result.show(false)

    // Verify results
    val rows = result.collect()
    rows.length shouldBe 5

    // Expected:
    // 2024-01-01, v1 -> 3
    // 2024-01-01, v2 -> 2
    // 2024-01-02, v1 -> 2
    // 2024-01-02, v2 -> 1
    // 2024-01-03, v1 -> 1

    println("\n=== Expected counts ===")
    println("2024-01-01, v1 -> 3")
    println("2024-01-01, v2 -> 2")
    println("2024-01-02, v1 -> 2")
    println("2024-01-02, v2 -> 1")
    println("2024-01-03, v1 -> 1")

    // Look for optimization log lines:
    // "GROUP BY OPTIMIZATION: Partition columns in GROUP BY: message_date"
    // "GROUP BY OPTIMIZATION: Data columns requiring Tantivy aggregation: asv"
    println("\n=== LOOK FOR LOG LINES ===")
    println("1. 'GROUP BY OPTIMIZATION: Partition columns in GROUP BY: message_date'")
    println("2. 'GROUP BY OPTIMIZATION: Data columns requiring Tantivy aggregation: asv'")
  }
}
