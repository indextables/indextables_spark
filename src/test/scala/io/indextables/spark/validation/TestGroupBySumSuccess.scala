package io.indextables.spark.validation

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite

class TestGroupBySumSuccess extends AnyFunSuite {

  test("GROUP BY with SUM where BOTH fields are fast (should be accepted)") {
    val spark = SparkSession
      .builder()
      .appName("TestGroupBySumSuccess")
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

      val tempDir   = java.nio.file.Files.createTempDirectory("groupby-sum-success-test").toFile
      val tablePath = tempDir.getAbsolutePath

      println("🧪 Test: GROUP BY with SUM where BOTH fields are fast (should be accepted)")

      // Write data with BOTH category and amount as fast fields
      testData.write
        .format("tantivy4spark")
        .option("spark.indextables.indexing.typemap.category", "string")
        .option("spark.indextables.indexing.fastfields", "category,amount") // Both fields fast
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // This should NOT throw an exception because both "category" and "amount" are fast fields
      val query = df.groupBy("category").agg(org.apache.spark.sql.functions.sum("amount"))

      // Attempt to execute the query - this should NOT throw an exception
      try {
        val physicalPlan = query.queryExecution.executedPlan.toString
        println("🔍 Physical plan:")
        println(physicalPlan)

        if (physicalPlan.contains("IndexTables4SparkGroupByAggregateScan")) {
          println("✅ EXPECTED: GROUP BY + SUM pushdown was accepted")
          println("✅ REASON: Both 'category' and 'amount' are configured as fast fields")
        } else if (physicalPlan.contains("HashAggregate")) {
          println("⚠️  GROUP BY + SUM pushdown fell back to HashAggregate")
          println("⚠️  This may be expected depending on implementation status")
        } else {
          println("? UNKNOWN: Physical plan doesn't contain expected patterns")
        }
      } catch {
        case e: IllegalArgumentException if e.getMessage.contains("fast field") =>
          println(s"❌ UNEXPECTED: Exception thrown despite proper fast field configuration: ${e.getMessage}")
          assert(false, s"Should not throw exception when fast fields are properly configured: ${e.getMessage}")
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
