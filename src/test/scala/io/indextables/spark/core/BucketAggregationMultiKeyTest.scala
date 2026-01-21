package io.indextables.spark.core

import java.io.File
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite

/**
 * Test for bucket aggregations with multiple GROUP BY keys.
 *
 * Multi-key bucket aggregations are supported using nested TermsAggregation:
 * - The bucket aggregation (DateHistogram/Histogram) is the outer aggregation
 * - Additional GROUP BY columns use nested TermsAggregation as sub-aggregations
 * - Results are flattened: [bucket_key, term_key, aggregation_values]
 */
class BucketAggregationMultiKeyTest extends AnyFunSuite {

  test("DateHistogram with single GROUP BY key should work") {
    val spark = SparkSession
      .builder()
      .appName("BucketAggregationMultiKeyTest")
      .master("local[*]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

    try {
      import spark.implicits._

      // Test data with timestamps and hostnames
      val testData = Seq(
        (Timestamp.valueOf("2024-01-01 10:00:00"), "host1", 100),
        (Timestamp.valueOf("2024-01-01 10:05:00"), "host1", 200),
        (Timestamp.valueOf("2024-01-01 10:10:00"), "host2", 300),
        (Timestamp.valueOf("2024-01-01 10:20:00"), "host1", 400),
        (Timestamp.valueOf("2024-01-01 10:25:00"), "host2", 500),
        (Timestamp.valueOf("2024-01-01 10:35:00"), "host2", 600)
      ).toDF("timestamp", "hostname", "value")

      val tempDir   = java.nio.file.Files.createTempDirectory("bucket-multikey-test").toFile
      val tablePath = tempDir.getAbsolutePath

      println("=" * 80)
      println("Test: DateHistogram with single GROUP BY key")
      println("=" * 80)

      // Write data with timestamp and hostname as fast fields
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "timestamp,hostname,value")
        .mode("overwrite")
        .save(tablePath)

      println(s"Data written to $tablePath")

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      df.createOrReplaceTempView("test_table")

      // Test: DateHistogram alone (should work)
      println("\nTesting DateHistogram with single GROUP BY key...")
      val singleKeyQuery = spark.sql("""
        SELECT indextables_date_histogram(timestamp, '15m') as slice, count(*) as cnt
        FROM test_table
        GROUP BY indextables_date_histogram(timestamp, '15m')
      """)
      val singleKeyResult = singleKeyQuery.collect()
      println(s"   Single key query succeeded with ${singleKeyResult.length} rows")
      singleKeyQuery.show()

      // Verify we got buckets
      assert(singleKeyResult.nonEmpty, "Expected at least one bucket")

      println("\n" + "=" * 80)
      println("SUCCESS: DateHistogram with single GROUP BY key works!")
      println("=" * 80)

      // Clean up
      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }

  test("DateHistogram with additional GROUP BY column should work with nested TermsAggregation") {
    val spark = SparkSession
      .builder()
      .appName("BucketAggregationMultiKeyTest-MultiKey")
      .master("local[*]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

    try {
      import spark.implicits._

      // Test data with timestamps and hostnames
      val testData = Seq(
        (Timestamp.valueOf("2024-01-01 10:00:00"), "host1", 100),
        (Timestamp.valueOf("2024-01-01 10:05:00"), "host1", 200),
        (Timestamp.valueOf("2024-01-01 10:10:00"), "host2", 300),
        (Timestamp.valueOf("2024-01-01 10:20:00"), "host1", 400),
        (Timestamp.valueOf("2024-01-01 10:25:00"), "host2", 500),
        (Timestamp.valueOf("2024-01-01 10:35:00"), "host2", 600)
      ).toDF("timestamp", "hostname", "value")

      val tempDir   = java.nio.file.Files.createTempDirectory("bucket-multikey-test2").toFile
      val tablePath = tempDir.getAbsolutePath

      println("=" * 80)
      println("Test: DateHistogram with multiple GROUP BY keys (nested TermsAggregation)")
      println("=" * 80)

      // Write data with timestamp and hostname as fast fields
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "timestamp,hostname,value")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      df.createOrReplaceTempView("test_table")

      // Test: DateHistogram with additional GROUP BY column
      println("\nTesting DateHistogram with additional GROUP BY column...")
      val multiKeyQuery = spark.sql("""
        SELECT indextables_date_histogram(timestamp, '15m') as slice, hostname, count(*) as cnt
        FROM test_table
        GROUP BY indextables_date_histogram(timestamp, '15m'), hostname
      """)

      val multiKeyResult = multiKeyQuery.collect()
      println(s"   Multi-key query succeeded with ${multiKeyResult.length} rows")
      multiKeyQuery.show()

      // Verify results
      // Expected buckets with 15-minute intervals:
      // 10:00-10:15 -> host1: 2 (10:00, 10:05), host2: 1 (10:10)
      // 10:15-10:30 -> host1: 1 (10:20), host2: 1 (10:25)
      // 10:30-10:45 -> host2: 1 (10:35)
      assert(multiKeyResult.length >= 4, s"Expected at least 4 rows, got ${multiKeyResult.length}")

      println("\n" + "=" * 80)
      println("SUCCESS: DateHistogram with multiple GROUP BY keys works!")
      println("=" * 80)

      // Clean up
      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }

  test("Histogram with single GROUP BY key should work") {
    val spark = SparkSession
      .builder()
      .appName("BucketAggregationMultiKeyTest-Histogram")
      .master("local[*]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

    try {
      import spark.implicits._

      val testData = Seq(
        ("category_a", "host1", 10.0),
        ("category_a", "host1", 25.0),
        ("category_a", "host2", 55.0),
        ("category_b", "host1", 75.0),
        ("category_b", "host2", 120.0)
      ).toDF("category", "hostname", "price")

      val tempDir   = java.nio.file.Files.createTempDirectory("histogram-singlekey-test").toFile
      val tablePath = tempDir.getAbsolutePath

      println("=" * 80)
      println("Test: Histogram with single GROUP BY key")
      println("=" * 80)

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "category,hostname,price")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      df.createOrReplaceTempView("price_table")

      // Test: Histogram with single GROUP BY key
      println("\nTesting Histogram with single GROUP BY key...")
      val query = spark.sql("""
        SELECT indextables_histogram(price, 50.0) as price_bucket, count(*) as cnt
        FROM price_table
        GROUP BY indextables_histogram(price, 50.0)
      """)

      val result = query.collect()
      println(s"   Query succeeded with ${result.length} rows")
      query.show()

      assert(result.nonEmpty, "Expected at least one bucket")

      println("\n" + "=" * 80)
      println("SUCCESS: Histogram with single GROUP BY key works!")
      println("=" * 80)

      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }

  test("Histogram with additional GROUP BY column should work with nested TermsAggregation") {
    val spark = SparkSession
      .builder()
      .appName("BucketAggregationMultiKeyTest-Histogram-MultiKey")
      .master("local[*]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

    try {
      import spark.implicits._

      val testData = Seq(
        ("category_a", "host1", 10.0),
        ("category_a", "host1", 25.0),
        ("category_a", "host2", 55.0),
        ("category_b", "host1", 75.0),
        ("category_b", "host2", 120.0)
      ).toDF("category", "hostname", "price")

      val tempDir   = java.nio.file.Files.createTempDirectory("histogram-multikey-test").toFile
      val tablePath = tempDir.getAbsolutePath

      println("=" * 80)
      println("Test: Histogram with multiple GROUP BY keys (nested TermsAggregation)")
      println("=" * 80)

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "category,hostname,price")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      df.createOrReplaceTempView("price_table")

      // Test: Histogram with additional GROUP BY column
      println("\nTesting Histogram with additional GROUP BY column...")
      val query = spark.sql("""
        SELECT indextables_histogram(price, 50.0) as price_bucket, hostname, count(*) as cnt
        FROM price_table
        GROUP BY indextables_histogram(price, 50.0), hostname
      """)

      val result = query.collect()
      println(s"   Query succeeded with ${result.length} rows")
      query.show()

      // Expected results:
      // 0.0 bucket: host1 (2 items at 10.0 and 25.0)
      // 50.0 bucket: host1 (1 item at 75.0), host2 (1 item at 55.0)
      // 100.0 bucket: host2 (1 item at 120.0)
      assert(result.length >= 3, s"Expected at least 3 rows, got ${result.length}")

      println("\n" + "=" * 80)
      println("SUCCESS: Histogram with multiple GROUP BY keys works!")
      println("=" * 80)

      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }

  /** Recursively delete a directory and all its contents. */
  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}
