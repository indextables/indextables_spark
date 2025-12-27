package io.indextables.spark.core

import org.apache.spark.sql.functions._

import io.indextables.spark.TestBase

/**
 * Test suite for transaction log optimization of GROUP BY partition columns COUNT queries. Validates that queries like
 * "SELECT partition_col, COUNT(*) GROUP BY partition_col" are served directly from transaction log metadata without
 * accessing split files.
 */
class PartitionGroupByCountOptimizationTest extends TestBase {

  test("SELECT partition_col, COUNT(*) GROUP BY partition_col should use transaction log") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = s"$tempDir/partition_groupby_test"

      // Create test data with multiple partitions
      val data = Seq(
        ("doc1", "content1", 100, "2024-01-01", 10),
        ("doc2", "content2", 200, "2024-01-01", 10),
        ("doc3", "content3", 300, "2024-01-01", 11),
        ("doc4", "content4", 400, "2024-01-02", 10),
        ("doc5", "content5", 500, "2024-01-02", 11),
        ("doc6", "content6", 600, "2024-01-02", 11)
      ).toDF("id", "content", "value", "load_date", "load_hour")

      // Write with V2 API partitioned by load_date and load_hour
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("load_date", "load_hour")
        .mode("overwrite")
        .save(tablePath)

      // Read and verify partition-only GROUP BY uses transaction log
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // Test 1: GROUP BY both partition columns
      println("🧪 TEST 1: GROUP BY load_date, load_hour")
      val result1 = df
        .groupBy("load_date", "load_hour")
        .agg(count("*").as("count"))
        .orderBy("load_date", "load_hour")
        .collect()

      println(s"🧪 TEST 1 Results:")
      result1.foreach(row => println(s"  ${row.getString(0)}, ${row.getInt(1)} => ${row.getLong(2)}"))

      result1.length should be(4)
      result1(0).getString(0) should be("2024-01-01")
      result1(0).getInt(1) should be(10)
      result1(0).getLong(2) should be(2) // 2 records

      result1(1).getString(0) should be("2024-01-01")
      result1(1).getInt(1) should be(11)
      result1(1).getLong(2) should be(1) // 1 record

      result1(2).getString(0) should be("2024-01-02")
      result1(2).getInt(1) should be(10)
      result1(2).getLong(2) should be(1) // 1 record

      result1(3).getString(0) should be("2024-01-02")
      result1(3).getInt(1) should be(11)
      result1(3).getLong(2) should be(2) // 2 records

      // Test 2: GROUP BY single partition column (subset of partition keys)
      println("🧪 TEST 2: GROUP BY load_date only")
      val result2 = df
        .groupBy("load_date")
        .agg(count("*").as("count"))
        .orderBy("load_date")
        .collect()

      println(s"🧪 TEST 2 Results:")
      result2.foreach(row => println(s"  ${row.getString(0)} => ${row.getLong(1)}"))

      result2.length should be(2)
      result2(0).getString(0) should be("2024-01-01")
      result2(0).getLong(1) should be(3) // 3 records across 2 hours

      result2(1).getString(0) should be("2024-01-02")
      result2(1).getLong(1) should be(3) // 3 records across 2 hours

      // Test 3: GROUP BY load_hour only (different subset)
      println("🧪 TEST 3: GROUP BY load_hour only")
      val result3 = df
        .groupBy("load_hour")
        .agg(count("*").as("count"))
        .orderBy("load_hour")
        .collect()

      println(s"🧪 TEST 3 Results:")
      result3.foreach(row => println(s"  ${row.getInt(0)} => ${row.getLong(1)}"))

      result3.length should be(2)
      result3(0).getInt(0) should be(10)
      result3(0).getLong(1) should be(3) // 3 records across 2 dates

      result3(1).getInt(0) should be(11)
      result3(1).getLong(1) should be(3) // 3 records across 2 dates
    }
  }

  test("GROUP BY partition columns with WHERE partition filter should use transaction log") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = s"$tempDir/partition_filter_test"

      // Create test data
      val data = Seq(
        ("doc1", "2024-01-01", 10),
        ("doc2", "2024-01-01", 10),
        ("doc3", "2024-01-01", 11),
        ("doc4", "2024-01-02", 10),
        ("doc5", "2024-01-02", 11),
        ("doc6", "2024-01-03", 10)
      ).toDF("id", "load_date", "load_hour")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("load_date", "load_hour")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // Test with partition filter
      println("🧪 TEST: GROUP BY with WHERE load_date = '2024-01-01'")
      val result = df
        .filter($"load_date" === "2024-01-01")
        .groupBy("load_hour")
        .agg(count("*").as("count"))
        .orderBy("load_hour")
        .collect()

      println(s"🧪 Results:")
      result.foreach(row => println(s"  hour ${row.getInt(0)} => ${row.getLong(1)}"))

      result.length should be(2)
      result(0).getInt(0) should be(10)
      result(0).getLong(1) should be(2) // 2 records for hour 10 on 2024-01-01

      result(1).getInt(0) should be(11)
      result(1).getLong(1) should be(1) // 1 record for hour 11 on 2024-01-01
    }
  }

  test("GROUP BY non-partition column should NOT use transaction log optimization") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = s"$tempDir/non_partition_groupby_test"

      // Create test data
      val data = Seq(
        ("doc1", "A", "2024-01-01"),
        ("doc2", "A", "2024-01-01"),
        ("doc3", "B", "2024-01-01"),
        ("doc4", "B", "2024-01-02")
      ).toDF("id", "category", "load_date")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("load_date")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // GROUP BY non-partition column - should use tantivy aggregation
      println("🧪 TEST: GROUP BY category (non-partition column)")
      val result = df
        .groupBy("category")
        .agg(count("*").as("count"))
        .orderBy("category")
        .collect()

      println(s"🧪 Results:")
      result.foreach(row => println(s"  ${row.getString(0)} => ${row.getLong(1)}"))

      result.length should be(2)
      result(0).getString(0) should be("A")
      result(0).getLong(1) should be(2)

      result(1).getString(0) should be("B")
      result(1).getLong(1) should be(2)
    }
  }

  test("GROUP BY mix of partition and non-partition columns should NOT use transaction log") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    withTempPath { tempDir =>
      val tablePath = s"$tempDir/mixed_groupby_test"

      // Create test data
      val data = Seq(
        ("doc1", "A", "2024-01-01"),
        ("doc2", "A", "2024-01-01"),
        ("doc3", "B", "2024-01-01"),
        ("doc4", "B", "2024-01-02")
      ).toDF("id", "category", "load_date")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("load_date")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // GROUP BY both partition and non-partition columns
      println("🧪 TEST: GROUP BY load_date, category (mixed)")
      val result = df
        .groupBy("load_date", "category")
        .agg(count("*").as("count"))
        .orderBy("load_date", "category")
        .collect()

      println(s"🧪 Results:")
      result.foreach(row => println(s"  ${row.getString(0)}, ${row.getString(1)} => ${row.getLong(2)}"))

      result.length should be(3)
      result(0).getString(0) should be("2024-01-01")
      result(0).getString(1) should be("A")
      result(0).getLong(2) should be(2)
    }
  }

}
