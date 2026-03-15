package io.indextables.spark.core

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Diagnostic test: Range aggregation works at tantivy4java level but returns NULL|6 via Spark SQL. This test isolates
 * where the bug is.
 */
class RangeAggregationDiagnosticTest extends AnyFunSuite with Matchers {

  test("Range aggregation directly via tantivy4java on Spark-written split") {
    val spark = SparkSession
      .builder()
      .appName("RangeAggDiag")
      .master("local[*]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

    try {
      import spark.implicits._

      val testData = Seq(
        ("item1", 10.0),
        ("item2", 25.0),
        ("item3", 35.0),
        ("item4", 75.0),
        ("item5", 150.0),
        ("item6", 250.0)
      ).toDF("name", "price")

      val tempDir   = Files.createTempDirectory("range-diag").toFile
      val tablePath = tempDir.getAbsolutePath

      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "price")
        .option("spark.indextables.write.optimizeWrite.enabled", "true")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Find the split file
      val txLog = new io.indextables.spark.transaction.TransactionLog(
        new org.apache.hadoop.fs.Path(tablePath),
        spark,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(
          java.util.Map.of("spark.indextables.transaction.allowDirectUsage", "true")
        )
      )
      val splits = txLog.listFiles()
      println(s"DIAG: Found ${splits.size} splits")

      val split     = splits.head
      val splitPath = new java.io.File(tablePath, split.path).getAbsolutePath
      println(s"DIAG: Split path: $splitPath")

      // Create cache config (matching how Spark does it)
      val splitCacheConfig = io.indextables.spark.util.ConfigUtils.createSplitCacheConfig(
        Map.empty[String, String],
        Some(tablePath)
      )
      val cacheManager = io.indextables.spark.storage.GlobalSplitCacheManager.getInstance(splitCacheConfig)

      // Create SplitMetadata from AddAction
      val splitMetadata = io.indextables.spark.util.SplitMetadataFactory.fromAddAction(split, tablePath)

      // Create SplitSearcher
      val searcher = cacheManager.createSplitSearcher(splitPath, splitMetadata)
      println(s"DIAG: SplitSearcher created")

      // Check schema
      val schema = searcher.getSchema()
      import scala.jdk.CollectionConverters._
      println(s"DIAG: Schema field names: ${schema.getFieldNames().asScala.mkString(", ")}")
      schema.close()

      // Create Range aggregation (same as executeRangeAggregationInternal)
      val rangeAgg = new io.indextables.tantivy4java.aggregation.RangeAggregation("bucket_agg", "price")
      rangeAgg.addRange("cheap", null, java.lang.Double.valueOf(50.0))
      rangeAgg.addRange("mid", java.lang.Double.valueOf(50.0), java.lang.Double.valueOf(100.0))
      rangeAgg.addRange("expensive", java.lang.Double.valueOf(100.0), null)

      println(s"DIAG: Range agg JSON: ${rangeAgg.toAggregationJson()}")

      // Execute search
      val query  = new io.indextables.tantivy4java.split.SplitMatchAllQuery()
      val result = searcher.search(query, 0, "bucket_agg", rangeAgg)

      println(s"DIAG: hasAggregations = ${result.hasAggregations()}")

      if (result.hasAggregations()) {
        val aggResult = result.getAggregation("bucket_agg")
        println(
          s"DIAG: getAggregation('bucket_agg') type = ${if (aggResult != null) aggResult.getClass.getName else "null"}"
        )

        if (aggResult != null) {
          val rangeResult = aggResult.asInstanceOf[io.indextables.tantivy4java.aggregation.RangeResult]
          val buckets     = rangeResult.getBuckets
          println(s"DIAG: RangeResult has ${if (buckets != null) buckets.size() else 0} buckets")
          if (buckets != null) {
            buckets.asScala.foreach { b =>
              println(s"DIAG:   key=${b.getKey} from=${b.getFrom} to=${b.getTo} docCount=${b.getDocCount}")
            }
          }

          // Note: querying only splits.head, so counts are for that split only
          buckets.size() shouldBe 3
        }
      }

      result.close()

      // Now test via Spark SQL — first with FFI DISABLED to test InternalRow path
      println(s"\nDIAG: === Test via Spark SQL with FFI DISABLED (InternalRow path) ===")
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.aggregation.arrowFfi.enabled", "false")
        .load(tablePath)
      df.createOrReplaceTempView("items")

      val rangeResult = spark.sql(
        """
          |SELECT
          |  indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL) as price_tier,
          |  COUNT(*) as cnt
          |FROM items
          |GROUP BY indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL)
          |ORDER BY price_tier
          |""".stripMargin
      )

      println("DIAG: Spark SQL results:")
      rangeResult.show(false)

      val rows = rangeResult.collect()
      println(s"DIAG: Spark SQL (InternalRow) returned ${rows.length} rows")
      rows.foreach(r => println(s"DIAG:   $r"))

      // Now test with FFI ENABLED (columnar path)
      println(s"\nDIAG: === Test via Spark SQL with FFI ENABLED (columnar path) ===")
      val df2 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.read.aggregation.arrowFfi.enabled", "true")
        .load(tablePath)
      df2.createOrReplaceTempView("items2")

      val rangeResult2 = spark.sql(
        """
          |SELECT
          |  indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL) as price_tier,
          |  COUNT(*) as cnt
          |FROM items2
          |GROUP BY indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL)
          |ORDER BY price_tier
          |""".stripMargin
      )

      println("DIAG: Spark SQL (FFI) results:")
      rangeResult2.show(false)

      val rows2 = rangeResult2.collect()
      println(s"DIAG: Spark SQL (FFI) returned ${rows2.length} rows")
      rows2.foreach(r => println(s"DIAG:   $r"))

      // Cleanup
      searcher.close()
      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }

  private def deleteRecursively(f: java.io.File): Unit = {
    if (f.isDirectory) {
      val children = f.listFiles()
      if (children != null) children.foreach(deleteRecursively)
    }
    f.delete()
  }
}
