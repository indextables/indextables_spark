package io.indextables.spark.debug

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class TimestampMetadataDebugTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("TimestampMetadataDebugTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  test("Debug timestamp metadata flow") {
    val sparkSession = spark
    import sparkSession.implicits._

    // Use a fixed temp directory that won't be cleaned up
    val tempPath = "/tmp/timestamp_metadata_debug"
    val tempDir = new java.io.File(tempPath)

    // Clean up first if it exists
    if (tempDir.exists()) {
      import java.nio.file.{Files, Paths}
      val path = Paths.get(tempPath)
      Files.walk(path)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(p => Files.deleteIfExists(p))
    }

    tempDir.mkdirs()

    // Create simple test data
    val data = Seq(
      (1, Timestamp.valueOf("2025-11-07 05:00:00"))
    ).toDF("id", "ts")

    println("=== WRITING DATA ===")
    data.show(false)

    // Write using V2 API
    data.coalesce(1).write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tempPath)

    println(s"\n=== DATA WRITTEN TO: $tempPath ===")
    println("Files created:")
    tempDir.listFiles().foreach(f => println(s"  - ${f.getName}"))

    // Check transaction log with proper GZIP handling
    println("\n=== CHECKING TRANSACTION LOG ===")
    val txLogDir = new java.io.File(tempPath, "_transaction_log")
    if (txLogDir.exists()) {
      val txLogFiles = txLogDir.listFiles().filter(_.getName.endsWith(".json")).sortBy(_.getName)
      txLogFiles.foreach { file =>
        println(s"\nTransaction log file: ${file.getName}")

        // Read GZIP-compressed JSON
        import java.io.{FileInputStream, InputStreamReader, BufferedReader}
        import java.util.zip.GZIPInputStream

        val reader = new BufferedReader(
          new InputStreamReader(new GZIPInputStream(new FileInputStream(file)))
        )

        try {
          var line = reader.readLine()
          var lineNum = 0
          while (line != null) {
            lineNum += 1
            if (line.contains("footerStartOffset") || line.contains("footerEndOffset")) {
              println(s"  Line $lineNum: $line")
            }
            if (line.contains("add") && line.contains("path")) {
              // This is an AddAction - check if it has footer offsets
              if (line.contains("footerStartOffset")) {
                println(s"  ✅ AddAction has footerStartOffset")
              } else {
                println(s"  ❌ AddAction missing footerStartOffset")
              }
            }
            line = reader.readLine()
          }
        } finally {
          reader.close()
        }
      }
    }

    println("\n=== READING DATA BACK ===")
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempPath)

    println("Read schema:")
    readData.printSchema()

    println("\nAll data (no filter):")
    readData.show(false)

    println("\n=== TESTING TIMESTAMP FILTER ===")
    val filtered = readData.filter($"ts" === "2025-11-07 05:00:00")
    val count = filtered.count()
    println(s"Filtered count: $count")

    if (count == 0) {
      println("❌ Filter returned 0 rows")
    } else {
      println("✅ Filter works!")
      filtered.show(false)
    }

    println(s"\n=== TEST DATA AVAILABLE AT: $tempPath ===")
    println("(Directory not cleaned up for inspection)")
  }
}
