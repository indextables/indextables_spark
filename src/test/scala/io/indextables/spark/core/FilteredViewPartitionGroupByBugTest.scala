package io.indextables.spark.core

import java.sql.Timestamp

import io.indextables.spark.TestBase

/**
 * Test to reproduce the "unexpected number of columns" bug that occurs when:
 * 1. A view is created with a filter
 * 2. A bucket aggregation (date_histogram) is run on the view
 * 3. Then a partition GROUP BY count(*) query is run on the view
 *
 * The bucket aggregation corrupts state that affects the subsequent partition count.
 */
class FilteredViewPartitionGroupByBugTest extends TestBase {

  test("REPRODUCE BUG: partition count fails after date_histogram on filtered view") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = s"$tempDir/bucket_then_partition_test"

      // Create test data with timestamp and partition column
      val data = Seq(
        ("doc1", Timestamp.valueOf("2024-01-01 10:00:00"), "2024-01-01"),
        ("doc2", Timestamp.valueOf("2024-01-01 10:30:00"), "2024-01-01"),
        ("doc3", Timestamp.valueOf("2024-01-01 11:00:00"), "2024-01-01"),
        ("doc4", Timestamp.valueOf("2024-01-02 10:00:00"), "2024-01-02"),
        ("doc5", Timestamp.valueOf("2024-01-02 10:30:00"), "2024-01-02"),
        ("doc6", Timestamp.valueOf("2024-01-03 10:00:00"), "2024-01-03")
      ).toDF("id", "event_time", "load_date")

      // Write with partition and fast fields for date_histogram
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "event_time,load_date")
        .partitionBy("load_date")
        .mode("overwrite")
        .save(tablePath)

      // Step 1: Read and create filtered view
      val rdf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      val filteredDf = rdf.filter($"load_date" =!= "2024-01-03")
      filteredDf.createOrReplaceTempView("my_view")

      // Step 2: Run date_histogram on the view
      println("Step 2: Running date_histogram on filtered view...")
      val histResult = spark.sql("""
        SELECT indextables_date_histogram(event_time, '1h') as hour_bucket, count(*) as cnt
        FROM my_view
        GROUP BY indextables_date_histogram(event_time, '1h')
        ORDER BY hour_bucket
      """).collect()

      println("DateHistogram results:")
      histResult.foreach(row => println(s"  ${row.getTimestamp(0)} => ${row.getLong(1)}"))

      // Step 3: Run partition GROUP BY count(*) - THIS SHOULD FAIL
      println("\nStep 3: Running partition GROUP BY count(*)...")
      val partitionResult = spark.sql("""
        SELECT load_date, count(*) as cnt
        FROM my_view
        GROUP BY load_date
        ORDER BY load_date
      """).collect()

      println("Partition count results:")
      partitionResult.foreach(row => println(s"  ${row.getString(0)} => ${row.getLong(1)}"))

      // Expected: 2024-01-01 -> 3, 2024-01-02 -> 2 (2024-01-03 filtered out)
      partitionResult.length should be(2)
      partitionResult(0).getString(0) should be("2024-01-01")
      partitionResult(0).getLong(1) should be(3)

      partitionResult(1).getString(0) should be("2024-01-02")
      partitionResult(1).getLong(1) should be(2)
    }
  }
}
