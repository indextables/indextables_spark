package io.indextables.spark.debug

import org.apache.spark.sql.functions._

import io.indextables.spark.TestBase

class SqlVsDataFrameLikeTest extends TestBase {

  private def isNativeLibraryAvailable(): Boolean =
    try {
      import io.indextables.spark.search.TantivyNative
      TantivyNative.ensureLibraryLoaded()
      true
    } catch {
      case _: Exception => false
    }

  private def analyzePushdown(queryName: String, queryExecution: org.apache.spark.sql.execution.QueryExecution)
    : Unit = {
    val physicalPlan = queryExecution.executedPlan
    val planString   = physicalPlan.toString

    println(s"\n=== $queryName ===")
    println("Physical Plan:")
    physicalPlan.foreach(println)

    // Look for evidence of pushdown
    val hasPushedFilters   = planString.contains("PushedFilters:")
    val pushedFiltersMatch = """PushedFilters: \[(.*?)\]""".r.findFirstMatchIn(planString)

    pushedFiltersMatch match {
      case Some(m) =>
        val filtersStr  = m.group(1)
        val filterCount = if (filtersStr.trim.isEmpty) 0 else filtersStr.split(",").length
        println(s"✓ PUSHDOWN DETECTED: [$filtersStr] ($filterCount filters)")
      case None =>
        println(s"✗ NO PUSHDOWN DETECTED - filters will be applied by Spark after reading all data")
    }

    // Check that DataSource V2 API is being used
    if (planString.contains("Scan")) {
      println(s"Using DataSource V2 API (TableProvider)")
    }

    println(s"Full plan string:\n$planString")
  }

  test("compare SQL LIKE vs DataFrame operations pushdown") {
    assume(isNativeLibraryAvailable(), "Native Tantivy library not available - skipping test")

    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data with text that contains patterns suitable for LIKE operations
      val testData = Seq(
        (1, "The dog ran quickly", "animal_story"),
        (2, "A cat sat on the mat", "animal_story"),
        (3, "Dogs are loyal pets", "pet_facts"),
        (4, "Birds can fly high", "nature"),
        (5, "The doghouse is red", "objects"),
        (6, "Fishing is relaxing", "hobby"),
        (7, "Doggone it!", "expression")
      ).toDF("id", "review_text", "category")

      // Save the data
      testData.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(testPath)

      // Read back the data
      val readDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(testPath)
      readDf.createOrReplaceTempView("review_table")

      println(s"Total rows in test table: ${readDf.count()}")

      // Test 1: DataFrame LIKE operation (contains)
      println(s"\n" + "=" * 60)
      println("TEST 1: DataFrame .filter() with contains()")
      val dfContains = readDf.filter($"review_text".contains("dog"))
      analyzePushdown("DataFrame contains('dog')", dfContains.queryExecution)
      val dfResults = dfContains.collect()
      println(s"Results: ${dfResults.length} rows found")
      dfResults.foreach(row => println(s"  - ${row.getAs[String]("review_text")}"))

      // Test 2: SQL LIKE operation
      println(s"\n" + "=" * 60)
      println("TEST 2: SQL LIKE operation")
      val sqlLike = spark.sql("SELECT * FROM review_table WHERE review_text LIKE '%dog%'")
      analyzePushdown("SQL LIKE '%dog%'", sqlLike.queryExecution)
      val sqlResults = sqlLike.collect()
      println(s"Results: ${sqlResults.length} rows found")
      sqlResults.foreach(row => println(s"  - ${row.getAs[String]("review_text")}"))

      // Test 3: DataFrame startsWith
      println(s"\n" + "=" * 60)
      println("TEST 3: DataFrame startsWith()")
      val dfStartsWith = readDf.filter($"review_text".startsWith("The"))
      analyzePushdown("DataFrame startsWith('The')", dfStartsWith.queryExecution)
      val startsWithResults = dfStartsWith.collect()
      println(s"Results: ${startsWithResults.length} rows found")
      startsWithResults.foreach(row => println(s"  - ${row.getAs[String]("review_text")}"))

      // Test 4: SQL LIKE with prefix pattern
      println(s"\n" + "=" * 60)
      println("TEST 4: SQL LIKE with prefix pattern")
      val sqlPrefix = spark.sql("SELECT * FROM review_table WHERE review_text LIKE 'The%'")
      analyzePushdown("SQL LIKE 'The%'", sqlPrefix.queryExecution)
      val prefixResults = sqlPrefix.collect()
      println(s"Results: ${prefixResults.length} rows found")
      prefixResults.foreach(row => println(s"  - ${row.getAs[String]("review_text")}"))

      // Test 5: DataFrame endsWith
      println(s"\n" + "=" * 60)
      println("TEST 5: DataFrame endsWith()")
      val dfEndsWith = readDf.filter($"review_text".endsWith("pets"))
      analyzePushdown("DataFrame endsWith('pets')", dfEndsWith.queryExecution)
      val endsWithResults = dfEndsWith.collect()
      println(s"Results: ${endsWithResults.length} rows found")
      endsWithResults.foreach(row => println(s"  - ${row.getAs[String]("review_text")}"))

      // Test 6: SQL LIKE with suffix pattern
      println(s"\n" + "=" * 60)
      println("TEST 6: SQL LIKE with suffix pattern")
      val sqlSuffix = spark.sql("SELECT * FROM review_table WHERE review_text LIKE '%pets'")
      analyzePushdown("SQL LIKE '%pets'", sqlSuffix.queryExecution)
      val suffixResults = sqlSuffix.collect()
      println(s"Results: ${suffixResults.length} rows found")
      suffixResults.foreach(row => println(s"  - ${row.getAs[String]("review_text")}"))

      // Test 7: Complex SQL query with multiple conditions
      println(s"\n" + "=" * 60)
      println("TEST 7: Complex SQL with LIKE + equality")
      val complexSql =
        spark.sql("SELECT * FROM review_table WHERE review_text LIKE '%dog%' AND category = 'animal_story'")
      analyzePushdown("Complex SQL LIKE + equality", complexSql.queryExecution)
      val complexResults = complexSql.collect()
      println(s"Results: ${complexResults.length} rows found")
      complexResults.foreach(row =>
        println(s"  - ${row.getAs[String]("review_text")} [${row.getAs[String]("category")}]")
      )

      // Test 8: Equivalent DataFrame complex query
      println(s"\n" + "=" * 60)
      println("TEST 8: Equivalent DataFrame complex query")
      val complexDf = readDf.filter($"review_text".contains("dog") && $"category" === "animal_story")
      analyzePushdown("DataFrame contains + equality", complexDf.queryExecution)
      val complexDfResults = complexDf.collect()
      println(s"Results: ${complexDfResults.length} rows found")
      complexDfResults.foreach(row =>
        println(s"  - ${row.getAs[String]("review_text")} [${row.getAs[String]("category")}]")
      )

      println(s"\n" + "=" * 80)
      println("SUMMARY ANALYSIS:")
      println(s"SQL LIKE '%dog%' found: ${sqlResults.length} rows")
      println(s"DataFrame contains('dog') found: ${dfResults.length} rows")
      println(s"Results should be identical: ${sqlResults.length == dfResults.length}")

      if (sqlResults.length == dfResults.length) {
        println("✅ Both approaches return same number of results")
      } else {
        println("❌ Different result counts - potential pushdown issue")
      }

      // The key insight: Check if both get the same pushdown behavior
      println("\nKey Question: Do both SQL and DataFrame operations show pushdown in their physical plans?")
      println("If SQL shows 'NO PUSHDOWN DETECTED' but DataFrame shows 'PUSHDOWN DETECTED',")
      println("then we have identified the root cause of the performance difference.")
    }
  }
}
