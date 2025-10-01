package io.indextables.spark.debug

import io.indextables.spark.TestBase
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, lit}

class V2IndexQueryAllValidationTest extends TestBase {

  test("should validate cross-field search with _indexall indexquery vs single field searches") {
    withTempPath { testDataPath =>
      // Create comprehensive test data where "spark" appears in different fields for different documents
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      val testData = Seq(
        ("doc1", "apache spark framework", "big data processing tool", "technology", "distributed computing"),
        (
          "doc2",
          "machine learning",
          "data processing with apache spark",
          "artificial intelligence",
          "algorithms and models"
        ),
        ("doc3", "data science tools", "python programming", "analytics with spark", "statistical analysis"),
        ("doc4", "database systems", "sql queries", "relational data", "spark optimization techniques"),
        ("doc5", "web development", "javascript frameworks", "frontend tools", "no matching content here")
      ).toDF("id", "title", "description", "category", "tags")

      // Write test data using V2 DataSource
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(SaveMode.Overwrite)
        .save(testDataPath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testDataPath)

      // Create temp view for SQL testing
      spark.sql(s"CREATE OR REPLACE TEMPORARY VIEW tantivy_table USING io.indextables.spark.core.IndexTables4SparkTableProvider OPTIONS (path '$testDataPath')")

      val searchTerm = "spark" // This term appears in different fields across documents

      // Test individual field searches to establish baseline
      println(s"=== Testing individual field searches for '$searchTerm' ===")

      val titleMatches = spark
        .sql(s"SELECT id FROM tantivy_table WHERE title indexquery '$searchTerm'")
        .collect()
        .map(_.getString(0))
        .sorted
      val descMatches = spark
        .sql(s"SELECT id FROM tantivy_table WHERE description indexquery '$searchTerm'")
        .collect()
        .map(_.getString(0))
        .sorted
      val categoryMatches = spark
        .sql(s"SELECT id FROM tantivy_table WHERE category indexquery '$searchTerm'")
        .collect()
        .map(_.getString(0))
        .sorted
      val tagsMatches = spark
        .sql(s"SELECT id FROM tantivy_table WHERE tags indexquery '$searchTerm'")
        .collect()
        .map(_.getString(0))
        .sorted

      println(s"title indexquery '$searchTerm': ${titleMatches.mkString(", ")}")
      println(s"description indexquery '$searchTerm': ${descMatches.mkString(", ")}")
      println(s"category indexquery '$searchTerm': ${categoryMatches.mkString(", ")}")
      println(s"tags indexquery '$searchTerm': ${tagsMatches.mkString(", ")}")

      // Calculate expected union of all field matches
      val allFieldMatches = (titleMatches ++ descMatches ++ categoryMatches ++ tagsMatches).distinct.sorted
      println(s"Union of all field matches: ${allFieldMatches.mkString(", ")} (total: ${allFieldMatches.length})")

      // Test _indexall indexquery - this should search across ALL fields
      println(s"=== Testing _indexall indexquery for '$searchTerm' ===")

      val indexAllQuery = s"SELECT id FROM tantivy_table WHERE _indexall indexquery '$searchTerm'"
      println(s"Executing: $indexAllQuery")

      val indexAllResult = spark.sql(indexAllQuery)
      println("Query plan for _indexall indexquery:")
      indexAllResult.explain(true)

      val indexAllMatches = indexAllResult.collect().map(_.getString(0)).sorted
      println(
        s"_indexall indexquery '$searchTerm': ${indexAllMatches.mkString(", ")} (total: ${indexAllMatches.length})"
      )

      // Validation: _indexall should find at least as many matches as union of all fields
      assert(
        indexAllMatches.length >= allFieldMatches.length,
        s"_indexall should find at least as many matches as individual fields. Expected >= ${allFieldMatches.length}, got ${indexAllMatches.length}"
      )

      // Check that _indexall found all expected matches
      allFieldMatches.foreach { expectedId =>
        assert(
          indexAllMatches.contains(expectedId),
          s"_indexall should include document '$expectedId' found by individual field search"
        )
      }

      println(s"✅ _indexall indexquery correctly searched across all fields and found ${indexAllMatches.length} matches")

      // Additional verification: ensure _indexall actually uses different query execution than single field
      val singleFieldQuery  = s"SELECT id FROM tantivy_table WHERE title indexquery '$searchTerm'"
      val singleFieldResult = spark.sql(singleFieldQuery)
      println("Query plan for single-field indexquery:")
      singleFieldResult.explain(true)

      val singleFieldMatches = singleFieldResult.collect().map(_.getString(0)).sorted
      println(
        s"Single field (title) matches: ${singleFieldMatches.mkString(", ")} (total: ${singleFieldMatches.length})"
      )

      // _indexall should typically find more matches than any single field
      if (indexAllMatches.length > singleFieldMatches.length) {
        println(s"✅ _indexall found more matches (${indexAllMatches.length}) than single field (${singleFieldMatches.length}) - demonstrates cross-field search")
      } else {
        println(s"ℹ️  _indexall found same matches as single field - this is okay if all matches are in that field")
      }
    }
  }
}
