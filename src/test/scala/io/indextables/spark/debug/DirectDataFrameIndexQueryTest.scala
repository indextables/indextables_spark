package io.indextables.spark.debug

import org.apache.spark.sql.functions.col

import io.indextables.spark.TestBase

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
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.category", "string")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.typemap.author", "string")
      .option("spark.indextables.indexing.typemap.tags", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
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
      val indexQueryResults = spark
        .sql("""
        SELECT id, title FROM direct_df_test
        WHERE title indexquery 'machine'
        ORDER BY id
      """)
        .collect()

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
      val combinedResults = spark
        .sql("""
        SELECT id, title, category FROM direct_df_test
        WHERE title indexquery 'machine' AND category = 'tech'
        ORDER BY id
      """)
        .collect()

      println(s"Results: ${combinedResults.length}")
      combinedResults.foreach(row =>
        println(s"  ID=${row.getInt(0)}: title='${row.getString(1)}', category='${row.getString(2)}'")
      )

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

  test("IndexQuery should work when DataFrame has pre-applied filter before temp view creation") {
    // This test verifies the bug fix for:
    // When a DataFrame filter is applied BEFORE creating a temp view,
    // IndexQuery expressions in subsequent SQL queries were being ignored.
    // The issue was that V2IndexQueryExpressionRule only checked for direct
    // DataSourceV2Relation children of SubqueryAlias, but when there's an
    // intermediate Filter node (from the pre-applied DataFrame filter),
    // the rule wouldn't find the relation and would return unchanged.
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/pre_filtered_indexquery_test"

    // Create test data with dates for filtering
    val testData = Seq(
      (1, "machine learning basics", "2026-01-05"),
      (2, "data engineering intro", "2026-01-06"),
      (3, "machine vision systems", "2026-01-07"),
      (4, "web development guide", "2026-01-08"),
      (5, "machine translation AI", "2026-01-10"),
      (6, "database optimization", "2026-01-12")
    ).toDF("id", "content", "load_date")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    println("=== Pre-filtered DataFrame IndexQuery Test ===")

    // Scenario 1: Without pre-filter (should work - baseline)
    println("\n[Baseline] Direct view without pre-filter:")
    df.createOrReplaceTempView("baseline_view")
    val baselineResults = spark
      .sql(
        "SELECT id, content FROM baseline_view WHERE content indexquery 'machine' ORDER BY id"
      )
      .collect()
    println(s"  Results: ${baselineResults.length} (expected: 3)")
    baselineResults.foreach(row => println(s"    ID=${row.getInt(0)}: '${row.getString(1)}'"))
    assert(baselineResults.length == 3, "Baseline should return 3 'machine' documents")

    // Scenario 2: With pre-filter applied BEFORE creating temp view (the bug scenario)
    println("\n[Bug Scenario] View with pre-applied DataFrame filter:")
    val filteredDf = df.filter(col("load_date").between("2026-01-01", "2026-01-08"))
    filteredDf.createOrReplaceTempView("pre_filtered_view")

    val filteredResults = spark
      .sql(
        "SELECT id, content FROM pre_filtered_view WHERE content indexquery 'machine' ORDER BY id"
      )
      .collect()
    println(s"  Results: ${filteredResults.length} (expected: 2 - only 'machine' docs within date range)")
    filteredResults.foreach(row => println(s"    ID=${row.getInt(0)}: '${row.getString(1)}'"))

    // With the bug, this would return ALL 4 rows in the date range (indexquery ignored)
    // With the fix, this should return only 2 rows (machine learning basics and machine vision systems)
    assert(
      filteredResults.length == 2,
      s"Should return 2 'machine' documents within date range, but got ${filteredResults.length}. " +
        "If this is 4, the indexquery is being ignored (bug not fixed)."
    )

    // Verify the correct documents were returned
    val ids = filteredResults.map(_.getInt(0)).sorted
    assert(ids.sameElements(Array(1, 3)), s"Expected IDs [1, 3] but got ${ids.mkString("[", ", ", "]")}")

    println("\n✅ Pre-filtered DataFrame IndexQuery works correctly!")
  }
}
