package io.indextables.spark.debug

import io.indextables.spark.TestBase

class SimpleIndexQueryTest extends TestBase {

  test("Minimal IndexQuery test to verify functionality") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/simple_indexquery_test"

    // Create minimal test data with same pattern as tantivy4java standalone test
    val testData = Seq(
      (1, "machine learning algorithms", "tech"),
      (2, "data engineering pipeline", "tech"),
      (3, "web development", "tech")
    ).toDF("id", "title", "category")

    // Write with text fields for title (same as working tantivy4java test)
    testData.write
      .format("io.indextables.spark.core.Tantivy4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")      // tokenized search
      .option("spark.indextables.indexing.typemap.category", "string") // exact matching
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.Tantivy4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("simple_test")

    println("=== Minimal IndexQuery Test ===")

    // First verify data exists
    println("\n🔍 All data:")
    val allData = spark.sql("SELECT * FROM simple_test ORDER BY id").collect()
    allData.foreach(row => println(s"  ID=${row.getInt(0)}: title='${row.getString(1)}', category='${row.getString(2)}'"))

    // Test basic IndexQuery - should find document 1 which contains "machine"
    println("\n🔍 IndexQuery for 'machine':")
    val machineResults = spark
      .sql("""
      SELECT id, title FROM simple_test
      WHERE title indexquery 'machine'
    """)
      .collect()

    println(s"Results: ${machineResults.length}")
    machineResults.foreach(row => println(s"  ID=${row.getInt(0)}: title='${row.getString(1)}'"))

    // Expectation: Should find ID=1 ("machine learning algorithms")
    if (machineResults.length == 1 && machineResults(0).getInt(0) == 1) {
      println("✅ IndexQuery is working correctly!")
    } else {
      println("❌ IndexQuery is NOT working - this confirms the bug")
      println("Expected: 1 result with ID=1")
      println(s"Actual: ${machineResults.length} results")
    }
  }
}
