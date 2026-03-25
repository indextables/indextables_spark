package io.indextables.spark.debug

import io.indextables.spark.TestBase

class ComplexIndexQueryDebugTest extends TestBase {

  test("Debug IndexQuery combined with regular filters") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/complex_debug_test"

    // Use same test data as failing test
    val testData = Seq(
      (1, "tech", "machine learning", "Alice", "2023-01-15", 95, true, "AI machine deep learning"),
      (2, "tech", "data engineering", "Bob", "2023-02-20", 88, true, "data big analytics"),
      (3, "tech", "web development", "Carol", "2023-03-10", 92, false, "web frontend javascript"),
      (8, "tech", "machine vision", "Henry", "2023-08-01", 85, false, "computer vision AI")
    ).toDF("id", "category", "title", "author", "publish_date", "score", "featured", "tags")

    // Same configuration as failing test
    testData.write
      .format(INDEXTABLES_FORMAT)
      .option("spark.indextables.indexing.typemap.category", "string") // exact matching
      .option("spark.indextables.indexing.typemap.title", "text")      // tokenized search
      .option("spark.indextables.indexing.typemap.author", "string")   // exact matching
      .option("spark.indextables.indexing.typemap.tags", "text")       // tokenized search
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format(INDEXTABLES_FORMAT)
      .load(testPath)

    df.createOrReplaceTempView("debug_test")

    println("=== Complex IndexQuery Debug Test ===")

    // Step 1: Show all data
    println("\n🔍 All data:")
    val allData = spark.sql("SELECT id, category, title FROM debug_test ORDER BY id").collect()
    allData.foreach(row => println(s"  ID=${row.getInt(0)}: category='${row.getString(1)}', title='${row.getString(2)}'"))

    // Step 2: Test individual components
    println("\n🔍 Test IndexQuery only: title indexquery 'machine'")
    val indexQueryOnly = spark
      .sql("""
      SELECT id, title FROM debug_test
      WHERE title indexquery 'machine'
      ORDER BY id
    """)
      .collect()
    println(s"Results: ${indexQueryOnly.length}")
    indexQueryOnly.foreach(row => println(s"  ID=${row.getInt(0)}: title='${row.getString(1)}'"))

    println("\n🔍 Test regular filter only: category = 'tech'")
    val regularFilterOnly = spark
      .sql("""
      SELECT id, category FROM debug_test
      WHERE category = 'tech'
      ORDER BY id
    """)
      .collect()
    println(s"Results: ${regularFilterOnly.length}")
    regularFilterOnly.foreach(row => println(s"  ID=${row.getInt(0)}: category='${row.getString(1)}'"))

    // Step 3: Test combined query (the failing case)
    println("\n🔍 Test combined: title indexquery 'machine' AND category = 'tech'")
    val combinedResults = spark
      .sql("""
      SELECT id, title, category FROM debug_test
      WHERE title indexquery 'machine' AND category = 'tech'
      ORDER BY id
    """)
      .collect()
    println(s"Results: ${combinedResults.length}")
    combinedResults.foreach(row =>
      println(s"  ID=${row.getInt(0)}: title='${row.getString(1)}', category='${row.getString(2)}'")
    )

    // Analysis
    println("\n=== Analysis ===")
    val expectedIds = Set(1, 8)
    val actualIds   = combinedResults.map(_.getInt(0)).toSet

    if (actualIds == expectedIds) {
      println("✅ Combined query works correctly!")
    } else {
      println(s"❌ Combined query fails - Expected: $expectedIds, Got: $actualIds")

      // Further debugging
      if (indexQueryOnly.length > 0) {
        println("✅ IndexQuery component works independently")
      } else {
        println("❌ IndexQuery component fails independently")
      }

      if (regularFilterOnly.length > 0) {
        println("✅ Regular filter component works independently")
      } else {
        println("❌ Regular filter component fails independently")
      }
    }
  }
}
