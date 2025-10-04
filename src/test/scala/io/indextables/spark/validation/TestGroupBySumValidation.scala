package io.indextables.spark.validation

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite

class TestGroupBySumValidation extends AnyFunSuite {

  test("GROUP BY with SUM where SUM field is NOT fast (should be rejected)") {
    val spark = SparkSession
      .builder()
      .appName("TestGroupBySumValidation")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._

      // Test data
      val testData = Seq(
        ("doc1", "category_a", 100),
        ("doc2", "category_a", 200),
        ("doc3", "category_b", 300)
      ).toDF("id", "category", "amount")

      val tempDir   = java.nio.file.Files.createTempDirectory("groupby-sum-validation-test").toFile
      val tablePath = tempDir.getAbsolutePath

      println("🧪 Test: GROUP BY with SUM where SUM field is NOT fast (should be rejected)")

      // Write data with category as fast field but NOT amount
      testData.write
        .format("tantivy4spark")
        .option("spark.indextables.indexing.typemap.category", "string")
        .option("spark.indextables.indexing.fastfields", "category") // Only category, NOT amount
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // This should throw an exception because neither "category" nor "amount" are properly configured as fast fields
      val query = df.groupBy("category").agg(org.apache.spark.sql.functions.sum("amount"))

      // Attempt to execute the query - this should throw an exception
      try {
        val physicalPlan = query.queryExecution.executedPlan.toString
        println("❌ UNEXPECTED: No exception was thrown - GROUP BY should require fast fields")
        assert(false, "Expected exception for missing fast field configuration")
      } catch {
        case e: IllegalArgumentException if e.getMessage.contains("Missing fast fields for GROUP BY columns") =>
          println("✅ EXPECTED: Exception thrown for missing fast field on GROUP BY column")
          println(s"✅ EXCEPTION MESSAGE: ${e.getMessage}")
          assert(e.getMessage.contains("category") || e.getMessage.contains("amount"))
          assert(e.getMessage.contains("fast field"))
          assert(e.getMessage.contains("spark.indexfiles.indexing.fastfields"))
        case e: IllegalArgumentException if e.getMessage.contains("Missing fast fields for aggregation columns") =>
          println("✅ EXPECTED: Exception thrown for missing fast field on aggregation column")
          println(s"✅ EXCEPTION MESSAGE: ${e.getMessage}")
          assert(e.getMessage.contains("amount"))
          assert(e.getMessage.contains("fast field"))
          assert(e.getMessage.contains("spark.indexfiles.indexing.fastfields"))
        case e: Exception =>
          println(s"❌ UNEXPECTED EXCEPTION: ${e.getClass.getSimpleName}: ${e.getMessage}")
          throw e
      }

      // Clean up
      def deleteRecursively(file: java.io.File): Unit = {
        if (file.isDirectory) {
          file.listFiles().foreach(deleteRecursively)
        }
        file.delete()
      }
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
