package io.indextables.spark.sql

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import io.indextables.spark.TestBase

class FlushIndexTablesCacheCommandTest extends TestBase {

  test("FlushIndexTablesCacheCommand should execute successfully") {
    val command = FlushIndexTablesCacheCommand()
    val results = command.run(spark)

    // Should return at least 3 results (split_cache, location_registry, tantivy_java_cache)
    assert(results.length >= 3)

    // Check that all operations report some status
    // Access by index since Row doesn't have schema info in this context
    results.foreach { row =>
      val cacheType = row.getString(0) // cache_type
      val status    = row.getString(1) // status
      val _         = row.getLong(2)   // cleared_entries
      val message   = row.getString(3) // message

      assert(cacheType != null)
      assert(status != null)
      assert(message != null)

      // Status should be either "success" or "failed"
      assert(status == "success" || status == "failed")
    }
  }

  test("should flush split cache and location registry") {
    // First, create some cache entries by writing and reading data
    withTempPath { tempDir =>
      val schema = StructType(
        Array(
          StructField("id", IntegerType, nullable = false),
          StructField("name", StringType, nullable = false)
        )
      )

      val data = Seq(
        Row(1, "Alice"),
        Row(2, "Bob")
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write data using IndexTables4Spark - this will create cache managers
      val tablePath = tempDir.toString
      df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

      // Read data back - this will create location registry entries
      val readDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      readDf.collect() // Force execution

      // Now flush the cache
      val command = FlushIndexTablesCacheCommand()
      val results = command.run(spark)

      // Verify results - access by index instead of field name
      val splitCacheResult = results.find(_.getString(0) == "split_cache")
      val localityResult   = results.find(_.getString(0) == "locality_manager")
      val tantivyResult    = results.find(_.getString(0) == "tantivy_java_cache")

      assert(splitCacheResult.isDefined)
      assert(localityResult.isDefined)
      assert(tantivyResult.isDefined)

      // All should report success
      assert(splitCacheResult.get.getString(1) == "success") // status
      assert(localityResult.get.getString(1) == "success")   // status
      assert(tantivyResult.get.getString(1) == "success")    // status
    }
  }

  test("SQL command parsing should work via extension") {
    // Test that our SQL extension can parse the command
    val sqlText = "FLUSH TANTIVY4SPARK SEARCHER CACHE"

    // This test verifies that the parser can handle the command
    val parser     = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)
    val parsedPlan = parser.parsePlan(sqlText)

    assert(parsedPlan.isInstanceOf[FlushIndexTablesCacheCommand])

    // Also test parseQuery method
    val parsedQuery = parser.parseQuery(sqlText)
    assert(parsedQuery.isInstanceOf[FlushIndexTablesCacheCommand])
  }

  test("SQL command should be case insensitive") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val variations = Seq(
      "flush tantivy4spark searcher cache",
      "FLUSH tantivy4spark SEARCHER cache",
      "Flush TANTIVY4SPARK Searcher Cache"
    )

    variations.foreach { sql =>
      val parsedPlan = parser.parsePlan(sql)
      assert(parsedPlan.isInstanceOf[FlushIndexTablesCacheCommand])
    }
  }

  test("non-matching SQL should delegate to default parser") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    // This should be handled by the default Spark SQL parser
    val parsedPlan = parser.parsePlan("SELECT 1 as test")
    assert(!parsedPlan.isInstanceOf[FlushIndexTablesCacheCommand])
  }
}
