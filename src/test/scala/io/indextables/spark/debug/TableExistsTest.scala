package io.indextables.spark.debug

import org.apache.spark.sql.functions._

import io.indextables.spark.TestBase

class TableExistsTest extends TestBase {

  test("should throw exception when trying to read non-existent table") {
    val nonExistentPath = "/tmp/non-existent-table"

    // Attempting to read from non-existent table should throw exception, not return empty results
    val exception = intercept[RuntimeException] {
      spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(nonExistentPath)
        .collect() // Trigger the read operation
    }

    exception.getMessage should include("Path does not exist")
    exception.getMessage should include(nonExistentPath)
    exception.getMessage should include("No transaction log found")
    exception.getMessage should include("Use spark.write to create the table first")

    println(s"✅ Exception correctly thrown: ${exception.getMessage}")
  }

  test("should return empty results for existing empty table") {
    // Create a table path for this test
    val tablePath = s"/tmp/empty-table-test-${System.currentTimeMillis()}"

    try {
      // First, create an empty table by writing and then overwriting with empty data
      val initialData = spark
        .range(5)
        .select(
          col("id"),
          lit("test").as("name")
        )

      // Write initial data
      initialData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(tablePath)

      // Overwrite with empty dataset (this should create a legitimate empty table)
      val emptyData = spark
        .range(0)
        .select(
          col("id"),
          lit("test").as("name")
        )

      emptyData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(tablePath)

      // Reading from empty table should return 0 rows, not throw exception
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)
        .collect()

      result.length shouldBe 0
      println(s"✅ Empty table correctly returned ${result.length} rows")

    } finally
      // Clean up test directory
      try {
        import java.io.File
        def deleteRecursively(file: File): Unit = {
          if (file.isDirectory) {
            file.listFiles().foreach(deleteRecursively)
          }
          file.delete()
        }
        deleteRecursively(new File(tablePath))
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
  }
}
