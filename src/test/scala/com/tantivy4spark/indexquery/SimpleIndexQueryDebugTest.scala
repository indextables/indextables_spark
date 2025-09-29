package com.tantivy4spark.indexquery

import com.tantivy4spark.TestBase

class SimpleIndexQueryDebugTest extends TestBase {

  test("Simple IndexQuery debugging") {
    val spark = this.spark
    import spark.implicits._

    // Create simple test data
    val testData = Seq(
      (1, "machine learning", "tech"),
      (2, "data engineering", "tech")
    ).toDF("id", "title", "category")

    val testPath = s"$tempDir/simple_indexquery_debug"

    // Write with string fields for exact matching
    testData.write
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "string")
      .option("spark.indextables.indexing.typemap.category", "string")
      .mode("overwrite")
      .save(testPath)

    // Read and create temp view
    val df = spark.read
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("test_docs")

    // Show all data
    println("=== All Data ===")
    spark.sql("SELECT * FROM test_docs").show()

    // Test exact match IndexQuery
    println("=== IndexQuery Test (exact match) ===")
    val indexQueryResults = spark.sql("SELECT * FROM test_docs WHERE title indexquery 'machine learning'")
    println(s"Query plan:")
    indexQueryResults.explain(true)
    println(s"Results:")
    indexQueryResults.show()

    // Test function call syntax for comparison
    println("=== Function Call Test ===")
    val functionResults = spark.sql("SELECT * FROM test_docs WHERE tantivy4spark_indexquery(title, 'machine learning')")
    functionResults.show()
  }
}