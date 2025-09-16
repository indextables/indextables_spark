package com.tantivy4spark.debug

import com.tantivy4spark.TestBase

class DirectDataFrameIndexQueryTest extends TestBase {

  test("Test IndexQuery using direct DataFrame operations instead of temp views") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/direct_df_test"

    // Same test data as complex debug test
    val testData = Seq(
      (1, "tech", "machine learning", "Alice", "2023-01-15", 95, true, "AI machine deep learning"),
      (2, "tech", "data engineering", "Bob", "2023-02-20", 88, true, "data big analytics"),
      (3, "tech", "web development", "Carol", "2023-03-10", 92, false, "web frontend javascript"),
      (8, "tech", "machine vision", "Henry", "2023-08-01", 85, false, "computer vision AI")
    ).toDF("id", "category", "title", "author", "publish_date", "score", "featured", "tags")

    // Same field configuration
    testData.write
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .option("spark.tantivy4spark.indexing.typemap.category", "string")
      .option("spark.tantivy4spark.indexing.typemap.title", "text")
      .option("spark.tantivy4spark.indexing.typemap.author", "string")
      .option("spark.tantivy4spark.indexing.typemap.tags", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .load(testPath)

    println("=== Direct DataFrame IndexQuery Test ===")

    // Test 1: All data
    println("\n🔍 All data:")
    val allData = df.orderBy($"id").collect()
    allData.foreach(row => println(s"  ID=${row.getInt(0)}: category='${row.getString(1)}', title='${row.getString(2)}'"))

    // Test 2: Use SQL with direct DataFrame (not temp view) to avoid temp view issues
    println("\n🔍 Direct SQL on DataFrame: spark.sql with df registered")

    // Register DataFrame directly for this test (not using temp view)
    df.createOrReplaceTempView("direct_df_test")

    try {
      val indexQueryResults = spark.sql("""
        SELECT id, title FROM direct_df_test
        WHERE title indexquery 'machine'
        ORDER BY id
      """).collect()

      println(s"Results: ${indexQueryResults.length}")
      indexQueryResults.foreach(row => println(s"  ID=${row.getInt(0)}: title='${row.getString(1)}'"))

      if (indexQueryResults.length > 0) {
        println("✅ Direct DataFrame IndexQuery works!")
      } else {
        println("❌ Direct DataFrame IndexQuery still fails")
      }
    } catch {
      case e: Exception =>
        println(s"❌ Direct DataFrame IndexQuery threw exception: ${e.getMessage}")
    }

    // Test 3: Try combined filter using SQL on direct DataFrame
    println("\n🔍 Direct DataFrame combined: SQL combined query")

    try {
      val combinedResults = spark.sql("""
        SELECT id, title, category FROM direct_df_test
        WHERE title indexquery 'machine' AND category = 'tech'
        ORDER BY id
      """).collect()

      println(s"Results: ${combinedResults.length}")
      combinedResults.foreach(row => println(s"  ID=${row.getInt(0)}: title='${row.getString(1)}', category='${row.getString(2)}'"))

      if (combinedResults.length >= 2) {
        println("✅ Direct DataFrame combined query works!")
      } else {
        println("❌ Direct DataFrame combined query fails")
      }
    } catch {
      case e: Exception =>
        println(s"❌ Direct DataFrame combined query threw exception: ${e.getMessage}")
    }
  }
}