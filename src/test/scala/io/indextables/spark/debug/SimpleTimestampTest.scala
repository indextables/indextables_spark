package io.indextables.spark.debug

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class SimpleTimestampTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("SimpleTimestampTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  test("Simple timestamp write and read") {
    val sparkSession = spark
    import sparkSession.implicits._

    val tempPath = java.nio.file.Files.createTempDirectory("timestamp_test").toString

    try {
      // Create simple test data
      val data = Seq(
        (1, Timestamp.valueOf("2025-11-07 05:00:00"))
      ).toDF("id", "ts")

      println("Writing data...")
      data.show(false)

      // Write using V2 API
      data
        .coalesce(1)
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(tempPath)

      println("Reading data back...")
      val readData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      println("Read schema:")
      readData.printSchema()

      println("All data:")
      readData.show(false)

      // DEBUG: Check transaction log for footer offsets using DESCRIBE syntax
      println("\n=== DEBUG: Checking transaction log ===")
      val txLogInfo = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tempPath'")
      println("Transaction log entries:")
      txLogInfo.show(false)

      // Check for footerStartOffset in the output
      val txLogRows = txLogInfo.collect()
      val hasFooterOffsets = txLogRows.exists { row =>
        val actionType = row.getAs[String]("action_type")
        val hasFooter  = row.getAs[Boolean]("has_footer_offsets")
        actionType == "add" && hasFooter
      }

      if (hasFooterOffsets) {
        println("✅ Found footerStartOffset in transaction log")
      } else {
        println("❌ No footerStartOffset found in transaction log")
      }

      println("\nTesting equality filter...")
      val filtered = readData.filter($"ts" === "2025-11-07 05:00:00")
      val count    = filtered.count()
      println(s"Filtered count: $count")
      filtered.show(false)

      assert(count == 1, s"Expected 1 row, got $count")
      println("✅ Test passed!")

    } finally {
      // Cleanup
      import java.nio.file.{Files, Paths}
      import scala.util.Try
      Try {
        val path = Paths.get(tempPath)
        Files
          .walk(path)
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(p => Files.deleteIfExists(p))
      }
    }
  }
}
