package io.indextables.spark.validation

import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite

/**
 * Test that MIN/MAX aggregations work correctly on string fast fields.
 *
 * This validates the fix for the issue where MIN/MAX on string fast fields would incorrectly fail with "Missing fast
 * fields for aggregation columns" even though the field was correctly configured as a fast field.
 *
 * The root cause was that `isNumericFastField` was being used for MIN/MAX validation, which required fields to be both
 * fast AND numeric. The fix uses `isFastField` instead, which allows MIN/MAX on any fast field type.
 */
class TestGroupByMinMaxStringValidation extends AnyFunSuite {

  test("GROUP BY with MIN/MAX on string fast field should work") {
    val spark = SparkSession
      .builder()
      .appName("TestGroupByMinMaxStringValidation")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._

      // Test data with string fields
      val testData = Seq(
        ("2024-01-01", "code_a", "host1"),
        ("2024-01-01", "code_b", "host2"),
        ("2024-01-01", "code_c", "host1"),
        ("2024-01-02", "code_a", "host2"),
        ("2024-01-02", "code_d", "host1")
      ).toDF("load_date", "code", "hostname")

      val tempDir   = java.nio.file.Files.createTempDirectory("groupby-minmax-string-test").toFile
      val tablePath = tempDir.getAbsolutePath

      println("=" * 80)
      println("🧪 Test: GROUP BY with MIN/MAX on string fast fields")
      println("=" * 80)

      // Write data with string fields as fast fields
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.load_date", "string")
        .option("spark.indextables.indexing.typemap.code", "string")
        .option("spark.indextables.indexing.typemap.hostname", "string")
        .option("spark.indextables.indexing.fastfields", "load_date,code,hostname")
        .mode("overwrite")
        .save(tablePath)

      println(s"✅ Data written to $tablePath")

      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // First verify that GROUP BY with just COUNT works (baseline)
      println("\n1. Testing GROUP BY with COUNT(*) only (baseline)...")
      val countQuery  = df.groupBy("load_date", "code", "hostname").agg(count("*").as("cnt"))
      val countResult = countQuery.collect()
      println(s"   ✅ COUNT(*) query succeeded with ${countResult.length} rows")

      // Now test GROUP BY with MIN/MAX on string fields
      println("\n2. Testing GROUP BY with MIN/MAX on string fast fields...")
      val minMaxQuery = df
        .groupBy("load_date")
        .agg(
          count("*").as("cnt"),
          min("code").as("min_code"),
          max("code").as("max_code")
        )

      val minMaxResult = minMaxQuery.collect()
      println(s"   ✅ MIN/MAX query succeeded with ${minMaxResult.length} rows")

      // Print results
      println("\n   Results:")
      minMaxQuery.show()

      // Verify the results are correct
      val day1 = minMaxResult.find(_.getString(0) == "2024-01-01").get
      val day2 = minMaxResult.find(_.getString(0) == "2024-01-02").get

      assert(
        day1.getString(2) == "code_a",
        s"Expected min_code for 2024-01-01 to be 'code_a', got '${day1.getString(2)}'"
      )
      assert(
        day1.getString(3) == "code_c",
        s"Expected max_code for 2024-01-01 to be 'code_c', got '${day1.getString(3)}'"
      )
      assert(
        day2.getString(2) == "code_a",
        s"Expected min_code for 2024-01-02 to be 'code_a', got '${day2.getString(2)}'"
      )
      assert(
        day2.getString(3) == "code_d",
        s"Expected max_code for 2024-01-02 to be 'code_d', got '${day2.getString(3)}'"
      )

      println("\n   ✅ Result values verified correctly")

      println("\n" + "=" * 80)
      println("✅ SUCCESS: MIN/MAX on string fast fields works correctly!")
      println("=" * 80)

      // Clean up
      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }

  test("GROUP BY with MIN/MAX on non-fast string field should be rejected") {
    val spark = SparkSession
      .builder()
      .appName("TestGroupByMinMaxStringValidation-Rejection")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._

      val testData = Seq(
        ("2024-01-01", "code_a", "host1"),
        ("2024-01-01", "code_b", "host2")
      ).toDF("load_date", "code", "hostname")

      val tempDir   = java.nio.file.Files.createTempDirectory("groupby-minmax-reject-test").toFile
      val tablePath = tempDir.getAbsolutePath

      println("=" * 80)
      println("🧪 Test: GROUP BY with MIN/MAX on non-fast field (should be rejected)")
      println("=" * 80)

      // Write data with load_date as fast field but NOT code
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.load_date", "string")
        .option("spark.indextables.indexing.typemap.code", "string")
        .option("spark.indextables.indexing.fastfields", "load_date") // Only load_date, NOT code
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // This should throw an exception because code is not a fast field
      val query = df
        .groupBy("load_date")
        .agg(
          count("*").as("cnt"),
          min("code").as("min_code")
        )

      try {
        val physicalPlan = query.queryExecution.executedPlan.toString
        println("❌ UNEXPECTED: No exception was thrown - MIN on non-fast field should be rejected")
        assert(false, "Expected exception for missing fast field configuration")
      } catch {
        case e: IllegalArgumentException if e.getMessage.contains("Missing fast fields") =>
          println("✅ EXPECTED: Exception thrown for MIN on non-fast field")
          println(s"✅ EXCEPTION MESSAGE: ${e.getMessage}")
          assert(e.getMessage.contains("code"))
      }

      println("\n" + "=" * 80)
      println("✅ SUCCESS: MIN on non-fast field correctly rejected!")
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
