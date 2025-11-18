package io.indextables.spark.debug

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class DebugTimestampSchemaTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("DebugTimestampSchemaTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  test("Debug timestamp schema and query") {
    val sparkSession = spark
    import sparkSession.implicits._

    val tempPath = java.nio.file.Files.createTempDirectory("timestamp_debug").toString

    try {
      // Create test data
      val data = Seq(
        (1, Timestamp.valueOf("2025-11-07 05:00:00"))
      ).toDF("id", "ts")

      println("=" * 80)
      println("WRITING DATA")
      println("=" * 80)
      data.show(false)

      // Write
      data.coalesce(1).write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(tempPath)

      println("\n" + "=" * 80)
      println("READING DATA BACK")
      println("=" * 80)

      val readData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      println("All data:")
      readData.show(false)

      println("\n" + "=" * 80)
      println("TESTING EQUALITY FILTER")
      println("=" * 80)

      // Enable DEBUG logging to see query details
      spark.sparkContext.setLogLevel("DEBUG")

      val filtered = readData.filter($"ts" === "2025-11-07 05:00:00")
      println(s"Filter: ts === '2025-11-07 05:00:00'")
      println(s"Result count: ${filtered.count()}")
      filtered.show(false)

      if (filtered.count() > 0) {
        println("✅ Timestamp equality filter works!")
      } else {
        println("❌ Timestamp equality filter returns 0 rows")
      }

      println("\n" + "=" * 80)
      println("TEST COMPLETE")
      println("=" * 80)

    } finally {
      // Cleanup
      import java.nio.file.{Files, Paths}
      import scala.util.Try
      Try {
        val path = Paths.get(tempPath)
        Files.walk(path)
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(p => Files.deleteIfExists(p))
      }
    }
  }
}
