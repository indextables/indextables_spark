package io.indextables.spark.core

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.search.SplitSearchEngine
import io.indextables.spark.transaction.{AddAction, TransactionLogFactory}
import io.indextables.spark.util.SplitMetadataFactory
import io.indextables.spark.TestBase
import io.indextables.tantivy4java.split.{SplitMatchAllQuery, SplitWildcardQuery}

/**
 * Validates Arrow FFI split behavior:
 *   1. Wildcard queries (leading, trailing, contains) on splits 2. Partition column presence/absence in split schemas 3.
 *      Partition filters don't leak to tantivy on FFI splits (which don't index partition cols)
 */
class TantVsFfiSplitComparisonTest extends TestBase {

  private val format = INDEXTABLES_FORMAT

  private def getTransactionLog(tablePath: String) =
    TransactionLogFactory.create(new Path(tablePath), spark, new CaseInsensitiveStringMap(java.util.Collections.emptyMap()))

  test("End-to-end: FFI-written table supports EndsWith/Contains pushdown") {
    withTempPath { tempPath =>
      val tablePath      = tempPath.toString
      val sparkImplicits = spark.implicits
      import sparkImplicits._

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

      val suffixResult = spark.read
        .format(format)
        .option("spark.indextables.filter.stringEndsWith.pushdown", "true")
        .load(tablePath)
        .filter($"filename".endsWith(".json"))
        .collect()

      println(s"FFI EndsWith '.json': ${suffixResult.length} results")
      assert(suffixResult.length == 3, s"FFI: EndsWith '.json' should match 3, got ${suffixResult.length}")

      val containsResult = spark.read
        .format(format)
        .option("spark.indextables.filter.stringContains.pushdown", "true")
        .load(tablePath)
        .filter($"filename".contains("json"))
        .collect()

      println(s"FFI Contains 'json': ${containsResult.length} results")
      assert(containsResult.length == 3, s"FFI: Contains 'json' should match 3, got ${containsResult.length}")
    }
  }

  test("Partitioned end-to-end: partition filters must not leak to tantivy on FFI splits") {
    withTempPath { tempPath =>
      val tablePath      = tempPath.toString
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Write partitioned table with FFI
      Seq(
        ("doc1", "report.json", "A"),
        ("doc2", "data.csv", "B"),
        ("doc3", "config.json", "A"),
        ("doc4", "readme.txt", "B"),
        ("doc5", "schema.json", "A")
      ).toDF("id", "filename", "category")
        .write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.typemap.id", "string")
        .option("spark.indextables.indexing.typemap.category", "string")
        .partitionBy("category")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read.format(format).load(tablePath)

      // Test 1: Simple partition filter — should be handled by partition pruning
      val catA = df.filter($"category" === "A").collect()
      println(s"FFI partitioned: category='A' -> ${catA.length} results")
      assert(catA.length == 3, s"category='A' should match 3, got ${catA.length}")

      // Test 2: Data field filter only
      val allDocs = df.collect()
      println(s"FFI partitioned: all docs -> ${allDocs.length} results")
      assert(allDocs.length == 5, s"All docs should be 5, got ${allDocs.length}")

      // Test 3: Mixed filter — partition + data field in OR
      // The partition reference must be resolved BEFORE sending to tantivy
      // because FFI splits do NOT have partition columns in the split schema
      val mixedResult = df.filter($"filename".startsWith("report") || $"category" === "B").collect()
      println(s"FFI partitioned: startsWith('report') OR category='B' -> ${mixedResult.length} results")
      assert(
        mixedResult.length == 3,
        s"startsWith('report') OR category='B' should match 3 (1 report + 2 in B), got ${mixedResult.length}"
      )

      // Test 4: Partition filter with data filter in AND
      val andResult = df.filter($"category" === "A" && $"filename".startsWith("report")).collect()
      println(s"FFI partitioned: category='A' AND startsWith('report') -> ${andResult.length} results")
      assert(andResult.length == 1, s"category='A' AND startsWith('report') should match 1, got ${andResult.length}")
    }
  }
}
