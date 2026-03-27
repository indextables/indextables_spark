package io.indextables.spark.core

import io.indextables.spark.TestBase

/**
 * Minimal reproduction for tantivy4java 0.31.1 wildcard query regression.
 *
 * On tantivy4java 0.31.0, leading wildcard queries (e.g., `*suffix`) work correctly on STRING fields (raw tokenizer).
 * On 0.31.1, they return 0 results.
 *
 * This test proves the issue is in the native wildcard query execution, not in the Spark integration layer.
 */
class WildcardQueryRegressionTest extends TestBase {

  private val format = INDEXTABLES_FORMAT

  test("SplitWildcardQuery with leading wildcard should match on STRING fields") {
    withTempPath { tempPath =>
      val tablePath      = tempPath.toString
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Write test data with STRING field type (raw tokenizer)
      Seq(
        ("doc1", "report.json"),
        ("doc2", "data.csv"),
        ("doc3", "config.json"),
        ("doc4", "readme.txt"),
        ("doc5", "schema.json")
      ).toDF("id", "filename")
        .write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .mode("overwrite")
        .save(tablePath)

      // Open the split and test wildcard queries directly
      val df      = spark.read.format(format).load(tablePath)
      val allRows = df.collect()
      assert(allRows.length == 5, s"Should have 5 rows, got ${allRows.length}")

      // Test 1: Trailing wildcard (prefix query) — should always work
      val prefixResult = df.filter($"filename".startsWith("report")).collect()
      assert(prefixResult.length == 1, s"StartsWith 'report' should match 1 row, got ${prefixResult.length}")

      // Test 2: Leading wildcard (suffix query) — THIS IS THE REGRESSION
      // Use the pushdown-enabled read to test the native wildcard query
      val suffixResult = spark.read
        .format(format)
        .option("spark.indextables.filter.stringEndsWith.pushdown", "true")
        .load(tablePath)
        .filter($"filename".endsWith(".json"))
        .collect()
      assert(
        suffixResult.length == 3,
        s"EndsWith '.json' should match 3 rows (report.json, config.json, schema.json), got ${suffixResult.length}. " +
          "This is a tantivy4java regression: SplitWildcardQuery with leading wildcard (*pattern) returns 0 results on STRING fields."
      )

      // Test 3: Contains wildcard — also uses leading wildcard
      val containsResult = spark.read
        .format(format)
        .option("spark.indextables.filter.stringContains.pushdown", "true")
        .load(tablePath)
        .filter($"filename".contains("json"))
        .collect()
      assert(
        containsResult.length == 3,
        s"Contains 'json' should match 3 rows, got ${containsResult.length}. " +
          "This is a tantivy4java regression: SplitWildcardQuery with *pattern* returns 0 results on STRING fields."
      )
    }
  }
}
