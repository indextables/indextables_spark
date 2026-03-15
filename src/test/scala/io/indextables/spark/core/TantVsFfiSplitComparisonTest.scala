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
 * Comparative test: TANT batch path vs Arrow FFI path.
 *
 * Validates:
 *   1. Wildcard queries (leading, trailing, contains) on both split types 2. Partition column presence/absence in split
 *      schemas 3. Metadata comparison (field names, doc count, statistics)
 *
 * This test determines whether the leading wildcard regression is FFI-specific or a general tantivy4java 0.31.1 issue,
 * and validates that partition column filters don't leak to tantivy on FFI splits (which don't index partition cols).
 */
class TantVsFfiSplitComparisonTest extends TestBase {

  private val format = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  private def getTransactionLog(tablePath: String) =
    TransactionLogFactory.create(new Path(tablePath), spark, new CaseInsensitiveStringMap(java.util.Collections.emptyMap()))

  /** Write test data using the specified write path, then open the split directly and return diagnostic info. */
  private def writeAndInspect(
    tablePath: String,
    useArrowFfi: Boolean,
    partitioned: Boolean = false
  ): SplitDiagnostics = {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    val data = Seq(
      ("doc1", "report.json", "A"),
      ("doc2", "data.csv", "B"),
      ("doc3", "config.json", "A"),
      ("doc4", "readme.txt", "B"),
      ("doc5", "schema.json", "A")
    )

    val writer = data
      .toDF("id", "filename", "category")
      .write
      .format(format)
      .option("spark.indextables.indexing.typemap.filename", "string")
      .option("spark.indextables.indexing.typemap.id", "string")
      .option("spark.indextables.indexing.typemap.category", "string")
      .option("spark.indextables.write.arrowFfi.enabled", useArrowFfi.toString)
      .mode("overwrite")

    if (partitioned) {
      writer.partitionBy("category").save(tablePath)
    } else {
      writer.save(tablePath)
    }

    // Read the transaction log to find the split files
    val txLog      = getTransactionLog(tablePath)
    val addActions = txLog.listFiles()

    val diagnostics = addActions.map(addAction => inspectSplit(tablePath, addAction))

    // Aggregate: for partitioned tables there may be multiple splits
    val allFieldNames = diagnostics.flatMap(_.splitFieldNames).toSet
    val totalDocs     = diagnostics.map(_.docCount).sum

    // Use the split with the most docs for wildcard testing
    val primarySplit = diagnostics.maxBy(_.docCount)

    SplitDiagnostics(
      writePath = if (useArrowFfi) "Arrow FFI" else "TANT Batch",
      splitFieldNames = allFieldNames,
      docCount = totalDocs,
      trailingWildcardCount = primarySplit.trailingWildcardCount,
      leadingWildcardCount = primarySplit.leadingWildcardCount,
      containsWildcardCount = primarySplit.containsWildcardCount,
      partitionValues = diagnostics.flatMap(_.partitionValues).toMap,
      splitCount = diagnostics.length,
      addActions = addActions
    )
  }

  /** Open a single split and run diagnostics. */
  private def inspectSplit(tablePath: String, addAction: AddAction): SplitDiagnostics = {
    val splitMetadata = SplitMetadataFactory.fromAddAction(addAction, tablePath)
    val splitPath = if (addAction.path.startsWith("/")) {
      addAction.path
    } else {
      s"$tablePath/${addAction.path}"
    }

    val sparkSchema = spark.read.format(format).load(tablePath).schema
    val engine      = SplitSearchEngine.fromSplitFileWithMetadata(sparkSchema, splitPath, splitMetadata)

    try {
      // Get split field names
      val splitSchema = engine.getSchema()
      val fieldNames =
        try
          splitSchema.getFieldNames().asScala.toSet
        finally
          splitSchema.close()

      // Count all docs
      val allResults = engine.search(new SplitMatchAllQuery(), 1000)
      val docCount   = allResults.length

      // Test trailing wildcard: filename:report*
      val trailingCount =
        try
          engine.search(new SplitWildcardQuery("filename", "report*"), 1000).length
        catch {
          case e: Exception =>
            System.err.println(s"  Trailing wildcard query failed: ${e.getMessage}")
            -1
        }

      // Test leading wildcard: filename:*.json
      val leadingCount =
        try
          engine.search(new SplitWildcardQuery("filename", "*.json"), 1000).length
        catch {
          case e: Exception =>
            System.err.println(s"  Leading wildcard query failed: ${e.getMessage}")
            -1
        }

      // Test contains wildcard: filename:*json*
      val containsCount =
        try
          engine.search(new SplitWildcardQuery("filename", "*json*"), 1000).length
        catch {
          case e: Exception =>
            System.err.println(s"  Contains wildcard query failed: ${e.getMessage}")
            -1
        }

      SplitDiagnostics(
        writePath = "",
        splitFieldNames = fieldNames,
        docCount = docCount,
        trailingWildcardCount = trailingCount,
        leadingWildcardCount = leadingCount,
        containsWildcardCount = containsCount,
        partitionValues = addAction.partitionValues,
        splitCount = 1,
        addActions = Seq(addAction)
      )
    } finally
      engine.close()
  }

  case class SplitDiagnostics(
    writePath: String,
    splitFieldNames: Set[String],
    docCount: Int,
    trailingWildcardCount: Int,
    leadingWildcardCount: Int,
    containsWildcardCount: Int,
    partitionValues: Map[String, String],
    splitCount: Int,
    addActions: Seq[AddAction])

  // ---- Non-partitioned tests ----

  test("Non-partitioned: compare TANT vs FFI split schemas and wildcard queries") {
    withTempPath { tantPath =>
      withTempPath { ffiPath =>
        val tant = writeAndInspect(tantPath.toString, useArrowFfi = false)
        val ffi  = writeAndInspect(ffiPath.toString, useArrowFfi = true)

        println("=" * 80)
        println("NON-PARTITIONED SPLIT COMPARISON")
        println("=" * 80)

        println(s"\n--- TANT Batch Path ---")
        println(s"  Split field names: ${tant.splitFieldNames.toSeq.sorted.mkString(", ")}")
        println(s"  Doc count: ${tant.docCount}")
        println(s"  Split count: ${tant.splitCount}")
        println(s"  Trailing wildcard (report*): ${tant.trailingWildcardCount}")
        println(s"  Leading wildcard (*.json):   ${tant.leadingWildcardCount}")
        println(s"  Contains wildcard (*json*):  ${tant.containsWildcardCount}")

        println(s"\n--- Arrow FFI Path ---")
        println(s"  Split field names: ${ffi.splitFieldNames.toSeq.sorted.mkString(", ")}")
        println(s"  Doc count: ${ffi.docCount}")
        println(s"  Split count: ${ffi.splitCount}")
        println(s"  Trailing wildcard (report*): ${ffi.trailingWildcardCount}")
        println(s"  Leading wildcard (*.json):   ${ffi.leadingWildcardCount}")
        println(s"  Contains wildcard (*json*):  ${ffi.containsWildcardCount}")

        println(s"\n--- Comparison ---")
        val fieldDiff = tant.splitFieldNames -- ffi.splitFieldNames
        val ffiOnly   = ffi.splitFieldNames -- tant.splitFieldNames
        if (fieldDiff.nonEmpty) println(s"  Fields in TANT but NOT in FFI: ${fieldDiff.mkString(", ")}")
        if (ffiOnly.nonEmpty) println(s"  Fields in FFI but NOT in TANT: ${ffiOnly.mkString(", ")}")
        if (fieldDiff.isEmpty && ffiOnly.isEmpty) println(s"  Field names: IDENTICAL")
        println(s"  Trailing wildcard: TANT=${tant.trailingWildcardCount} vs FFI=${ffi.trailingWildcardCount}")
        println(s"  Leading wildcard:  TANT=${tant.leadingWildcardCount} vs FFI=${ffi.leadingWildcardCount}")
        println(s"  Contains wildcard: TANT=${tant.containsWildcardCount} vs FFI=${ffi.containsWildcardCount}")

        // Print AddAction metadata comparison
        println(s"\n--- AddAction Metadata ---")
        tant.addActions.headOption.foreach { a =>
          println(s"  TANT: numRecords=${a.numRecords}, size=${a.size}")
          println(s"        minValues=${a.minValues}")
          println(s"        maxValues=${a.maxValues}")
          println(s"        docMappingJson=${a.docMappingJson.map(_.take(200))}")
        }
        ffi.addActions.headOption.foreach { a =>
          println(s"  FFI:  numRecords=${a.numRecords}, size=${a.size}")
          println(s"        minValues=${a.minValues}")
          println(s"        maxValues=${a.maxValues}")
          println(s"        docMappingJson=${a.docMappingJson.map(_.take(200))}")
        }
        println("=" * 80)

        // Assertions
        assert(tant.docCount == 5, s"TANT should have 5 docs, got ${tant.docCount}")
        assert(ffi.docCount == 5, s"FFI should have 5 docs, got ${ffi.docCount}")

        // Record wildcard results for diagnosis — don't fail yet, just report
        if (tant.leadingWildcardCount != ffi.leadingWildcardCount) {
          println(s"\n*** DIVERGENCE: Leading wildcard returns different results: TANT=${tant.leadingWildcardCount} vs FFI=${ffi.leadingWildcardCount}")
        }
      }
    }
  }

  // ---- Partitioned tests ----

  test("Partitioned: compare TANT vs FFI split schemas — partition columns indexed?") {
    withTempPath { tantPath =>
      withTempPath { ffiPath =>
        val tant = writeAndInspect(tantPath.toString, useArrowFfi = false, partitioned = true)
        val ffi  = writeAndInspect(ffiPath.toString, useArrowFfi = true, partitioned = true)

        println("=" * 80)
        println("PARTITIONED SPLIT COMPARISON (partitionBy=category)")
        println("=" * 80)

        println(s"\n--- TANT Batch Path ---")
        println(s"  Split field names: ${tant.splitFieldNames.toSeq.sorted.mkString(", ")}")
        println(s"  Doc count: ${tant.docCount}")
        println(s"  Split count: ${tant.splitCount}")

        println(s"\n--- Arrow FFI Path ---")
        println(s"  Split field names: ${ffi.splitFieldNames.toSeq.sorted.mkString(", ")}")
        println(s"  Doc count: ${ffi.docCount}")
        println(s"  Split count: ${ffi.splitCount}")

        println(s"\n--- Partition Column Analysis ---")
        val tantHasCategory = tant.splitFieldNames.contains("category")
        val ffiHasCategory  = ffi.splitFieldNames.contains("category")
        println(s"  TANT split contains 'category' field: $tantHasCategory")
        println(s"  FFI split contains 'category' field:  $ffiHasCategory")

        val fieldDiff = tant.splitFieldNames -- ffi.splitFieldNames
        val ffiOnly   = ffi.splitFieldNames -- tant.splitFieldNames
        if (fieldDiff.nonEmpty) println(s"  Fields in TANT but NOT in FFI: ${fieldDiff.mkString(", ")}")
        if (ffiOnly.nonEmpty) println(s"  Fields in FFI but NOT in TANT: ${ffiOnly.mkString(", ")}")

        if (tantHasCategory && !ffiHasCategory) {
          println(s"\n  *** CONFIRMED: TANT indexes partition columns, FFI does NOT")
          println(s"  *** Queries on partition columns will succeed on TANT splits but FAIL on FFI splits")
          println(s"  *** The read path MUST strip partition column filters before sending to tantivy")
        }

        // Print AddAction metadata comparison
        println(s"\n--- AddAction Metadata ---")
        tant.addActions.foreach { a =>
          println(s"  TANT: path=${a.path}, partitionValues=${a.partitionValues}, numRecords=${a.numRecords}")
          println(s"        minValues=${a.minValues}")
          println(s"        maxValues=${a.maxValues}")
        }
        ffi.addActions.foreach { a =>
          println(s"  FFI:  path=${a.path}, partitionValues=${a.partitionValues}, numRecords=${a.numRecords}")
          println(s"        minValues=${a.minValues}")
          println(s"        maxValues=${a.maxValues}")
        }
        println("=" * 80)

        // Assertions
        assert(tant.docCount == 5, s"TANT should have 5 docs, got ${tant.docCount}")
        assert(ffi.docCount == 5, s"FFI should have 5 docs, got ${ffi.docCount}")
      }
    }
  }

  test("Partitioned: wildcard queries on data fields — per-split comparison") {
    withTempPath { tantPath =>
      withTempPath { ffiPath =>
        // Write with both paths
        val sparkImplicits = spark.implicits
        import sparkImplicits._

        val data = Seq(
          ("doc1", "report.json", "A"),
          ("doc2", "data.csv", "B"),
          ("doc3", "config.json", "A"),
          ("doc4", "readme.txt", "B"),
          ("doc5", "schema.json", "A")
        )

        Seq((tantPath.toString, false), (ffiPath.toString, true)).foreach {
          case (path, arrowFfi) =>
            data
              .toDF("id", "filename", "category")
              .write
              .format(format)
              .option("spark.indextables.indexing.typemap.filename", "string")
              .option("spark.indextables.indexing.typemap.id", "string")
              .option("spark.indextables.indexing.typemap.category", "string")
              .option("spark.indextables.write.arrowFfi.enabled", arrowFfi.toString)
              .partitionBy("category")
              .mode("overwrite")
              .save(path)
        }

        println("=" * 80)
        println("PARTITIONED WILDCARD QUERY COMPARISON (per-split)")
        println("=" * 80)

        Seq((tantPath.toString, "TANT"), (ffiPath.toString, "FFI")).foreach {
          case (path, label) =>
            val txLog       = getTransactionLog(path)
            val actions     = txLog.listFiles()
            val sparkSchema = spark.read.format(format).load(path).schema

            println(s"\n--- $label Path: ${actions.length} splits ---")
            actions.foreach { addAction =>
              val splitPath =
                if (addAction.path.startsWith("/")) addAction.path
                else s"$path/${addAction.path}"
              val splitMetadata = SplitMetadataFactory.fromAddAction(addAction, path)
              val engine        = SplitSearchEngine.fromSplitFileWithMetadata(sparkSchema, splitPath, splitMetadata)

              try {
                val splitSchema = engine.getSchema()
                val fieldNames =
                  try splitSchema.getFieldNames().asScala.toSet
                  finally splitSchema.close()

                val allCount = engine.search(new SplitMatchAllQuery(), 1000).length

                val trailing =
                  try engine.search(new SplitWildcardQuery("filename", "report*"), 1000).length
                  catch { case e: Exception => println(s"    ERROR trailing: ${e.getMessage}"); -1 }

                val leading =
                  try engine.search(new SplitWildcardQuery("filename", "*.json"), 1000).length
                  catch { case e: Exception => println(s"    ERROR leading: ${e.getMessage}"); -1 }

                val contains =
                  try engine.search(new SplitWildcardQuery("filename", "*json*"), 1000).length
                  catch { case e: Exception => println(s"    ERROR contains: ${e.getMessage}"); -1 }

                println(s"  Split: ${addAction.path}")
                println(s"    Fields: ${fieldNames.toSeq.sorted.mkString(", ")}")
                println(s"    Partition: ${addAction.partitionValues}")
                println(s"    Docs: $allCount, trailing=$trailing, leading=$leading, contains=$contains")
              } finally
                engine.close()
            }
        }
        println("=" * 80)
      }
    }
  }

  // ---- Spark-level read tests (end-to-end) ----

  test("End-to-end: TANT-written table supports EndsWith/Contains pushdown") {
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
        .option("spark.indextables.write.arrowFfi.enabled", "false")
        .mode("overwrite")
        .save(tablePath)

      val suffixResult = spark.read
        .format(format)
        .option("spark.indextables.filter.stringEndsWith.pushdown", "true")
        .load(tablePath)
        .filter($"filename".endsWith(".json"))
        .collect()

      println(s"TANT EndsWith '.json': ${suffixResult.length} results")
      assert(suffixResult.length == 3, s"TANT: EndsWith '.json' should match 3, got ${suffixResult.length}")

      val containsResult = spark.read
        .format(format)
        .option("spark.indextables.filter.stringContains.pushdown", "true")
        .load(tablePath)
        .filter($"filename".contains("json"))
        .collect()

      println(s"TANT Contains 'json': ${containsResult.length} results")
      assert(containsResult.length == 3, s"TANT: Contains 'json' should match 3, got ${containsResult.length}")
    }
  }

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
        .option("spark.indextables.write.arrowFfi.enabled", "true")
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
        .option("spark.indextables.write.arrowFfi.enabled", "true")
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

  test("Partitioned end-to-end: same tests with TANT path for comparison") {
    withTempPath { tempPath =>
      val tablePath      = tempPath.toString
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Write partitioned table with TANT batch
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
        .option("spark.indextables.write.arrowFfi.enabled", "false")
        .partitionBy("category")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read.format(format).load(tablePath)

      val catA = df.filter($"category" === "A").collect()
      println(s"TANT partitioned: category='A' -> ${catA.length} results")
      assert(catA.length == 3, s"category='A' should match 3, got ${catA.length}")

      val allDocs = df.collect()
      println(s"TANT partitioned: all docs -> ${allDocs.length} results")
      assert(allDocs.length == 5, s"All docs should be 5, got ${allDocs.length}")

      val mixedResult = df.filter($"filename".startsWith("report") || $"category" === "B").collect()
      println(s"TANT partitioned: startsWith('report') OR category='B' -> ${mixedResult.length} results")
      assert(
        mixedResult.length == 3,
        s"startsWith('report') OR category='B' should match 3 (1 report + 2 in B), got ${mixedResult.length}"
      )

      val andResult = df.filter($"category" === "A" && $"filename".startsWith("report")).collect()
      println(s"TANT partitioned: category='A' AND startsWith('report') -> ${andResult.length} results")
      assert(andResult.length == 1, s"category='A' AND startsWith('report') should match 1, got ${andResult.length}")
    }
  }
}
