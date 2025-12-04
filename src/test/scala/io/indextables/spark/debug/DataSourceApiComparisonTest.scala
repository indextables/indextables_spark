package io.indextables.spark.debug

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import io.indextables.spark.TestBase

class DataSourceApiComparisonTest extends TestBase {

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
    if (planString.contains("BatchScanExec") || planString.contains("Scan")) {
      println(s"Using DataSource V2 API (TableProvider)")
    }

    // Show the execution strategy being used
    val executionPlan = queryExecution.toString
    if (executionPlan.contains("DataSourceV2Strategy")) {
      println(s"Execution Strategy: DataSourceV2Strategy")
    }

    println(s"Physical plan: $planString")
  }

  test("compare DataSource V1 vs V2 API usage for SQL vs DataFrame") {
    assume(isNativeLibraryAvailable(), "Native Tantivy library not available - skipping test")

    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data
      val testData = Seq(
        (1, "The dog ran quickly", "animal_story"),
        (2, "A cat sat on the mat", "animal_story"),
        (3, "Dogs are loyal pets", "pet_facts"),
        (4, "Birds can fly high", "nature"),
        (5, "The doghouse is red", "objects")
      ).toDF("id", "review_text", "category")

      // Save the data
      testData.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(testPath)

      // Test with current default configuration
      println("=== TESTING WITH DEFAULT SPARK CONFIGURATION ===")

      // Read back the data for DataFrame operations
      val readDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(testPath)
      readDf.createOrReplaceTempView("api_test_table")

      // Test 1: DataFrame API
      val dfQuery = readDf.filter($"review_text".contains("dog"))
      analyzePushdown("DataFrame API (.filter)", dfQuery.queryExecution)
      val dfResults = dfQuery.collect()
      println(s"Results: ${dfResults.length} rows")

      // Test 2: SQL API
      val sqlQuery = spark.sql("SELECT * FROM api_test_table WHERE review_text LIKE '%dog%'")
      analyzePushdown("SQL API (LIKE)", sqlQuery.queryExecution)
      val sqlResults = sqlQuery.collect()
      println(s"Results: ${sqlResults.length} rows")

      // Test with V1 source list exclusion (force V2 for tantivy4spark)
      println("\n=== TESTING WITH FORCED V2 CONFIGURATION ===")

      // Force use of V2 by excluding tantivy4spark from V1 source list
      val originalV1Sources = spark.conf.getOption("spark.sql.sources.useV1SourceList")
      println(s"Original V1 source list: $originalV1Sources")

      // Set empty V1 source list to force V2 for all sources
      spark.conf.set("spark.sql.sources.useV1SourceList", "")

      // Re-create the temp view to pick up new configuration
      readDf.createOrReplaceTempView("api_test_table_v2")

      // Test 3: SQL with forced V2
      val sqlQueryV2 = spark.sql("SELECT * FROM api_test_table_v2 WHERE review_text LIKE '%dog%'")
      analyzePushdown("SQL API (LIKE) - Forced V2", sqlQueryV2.queryExecution)
      val sqlResultsV2 = sqlQueryV2.collect()
      println(s"Results: ${sqlResultsV2.length} rows")

      // Test 4: DataFrame with forced V2
      val dfQueryV2 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)
        .filter($"review_text".contains("dog"))
      analyzePushdown("DataFrame API (.filter) - Forced V2", dfQueryV2.queryExecution)
      val dfResultsV2 = dfQueryV2.collect()
      println(s"Results: ${dfResultsV2.length} rows")

      // Restore original configuration
      originalV1Sources match {
        case Some(original) => spark.conf.set("spark.sql.sources.useV1SourceList", original)
        case None           => spark.conf.unset("spark.sql.sources.useV1SourceList")
      }

      println(s"\n" + "=" * 80)
      println("ANALYSIS SUMMARY:")
      println(s"Default config - DataFrame results: ${dfResults.length}")
      println(s"Default config - SQL results: ${sqlResults.length}")
      println(s"Forced V2 config - DataFrame results: ${dfResultsV2.length}")
      println(s"Forced V2 config - SQL results: ${sqlResultsV2.length}")

      if (
        dfResults.length == sqlResults.length && sqlResults.length == dfResultsV2.length && dfResultsV2.length == sqlResultsV2.length
      ) {
        println("✅ All configurations return identical results")
      } else {
        println("❌ Different result counts across configurations - API difference detected")
      }

      println("\nKey Question: Do we see different DataSource API versions being used?")
      println("If SQL uses V1 but DataFrame uses V2 (or vice versa), that could explain performance differences.")
    }
  }

  test("investigate specific LIKE operation performance") {
    assume(isNativeLibraryAvailable(), "Native Tantivy library not available - skipping test")

    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create larger dataset for performance testing
      val testData = (1 to 1000)
        .map { i =>
          val text =
            if (i % 10 == 0) s"Document $i contains dog information"
            else if (i % 7 == 0) s"Document $i has cat details"
            else s"Document $i generic content"
          val category = if (i % 3 == 0) "animal" else "general"
          (i, text, category)
        }
        .toDF("id", "content", "category")

      testData.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(testPath)

      val readDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(testPath)
      readDf.createOrReplaceTempView("performance_test")

      println(s"Created test dataset with ${testData.count()} rows")

      // Test performance of SQL LIKE vs DataFrame contains
      println("\n=== PERFORMANCE COMPARISON ===")

      // DataFrame approach
      val startDf  = System.currentTimeMillis()
      val dfResult = readDf.filter($"content".contains("dog"))
      // Use limit().collect().length since StringContains is unsupported for pushdown
      val dfCount = dfResult.limit(2000).collect().length
      val endDf   = System.currentTimeMillis()

      analyzePushdown("DataFrame .contains('dog')", dfResult.queryExecution)
      println(s"DataFrame: Found $dfCount rows in ${endDf - startDf}ms")

      // SQL approach
      val startSql  = System.currentTimeMillis()
      val sqlResult = spark.sql("SELECT * FROM performance_test WHERE content LIKE '%dog%'")
      // Use limit().collect().length since LIKE is unsupported for pushdown
      val sqlCount = sqlResult.limit(2000).collect().length
      val endSql   = System.currentTimeMillis()

      analyzePushdown("SQL LIKE '%dog%'", sqlResult.queryExecution)
      println(s"SQL LIKE: Found $sqlCount rows in ${endSql - startSql}ms")

      // Verify results match
      if (dfCount == sqlCount) {
        println(s"✅ Both approaches found identical results ($dfCount rows)")
        val timeDiff = Math.abs(endDf - startDf) - Math.abs(endSql - startSql)
        if (Math.abs(timeDiff) < 100) {
          println(s"✅ Performance is comparable (difference: ${timeDiff}ms)")
        } else {
          println(s"⚠️  Performance difference: ${timeDiff}ms")
        }
      } else {
        println(s"❌ Result mismatch: DataFrame=$dfCount, SQL=$sqlCount")
      }
    }
  }
}
