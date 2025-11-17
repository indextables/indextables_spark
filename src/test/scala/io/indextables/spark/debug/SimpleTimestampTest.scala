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
      data.coalesce(1).write
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

      // DEBUG: Check transaction log for footer offsets
      println("\n=== DEBUG: Checking transaction log ===")
      val txLogDir = new java.io.File(tempPath, "_transaction_log")
      if (txLogDir.exists()) {
        val txLogFiles = txLogDir.listFiles().filter(_.getName.endsWith(".json")).sortBy(_.getName)
        txLogFiles.foreach { file =>
          println(s"Transaction log file: ${file.getName}")
          val content = scala.io.Source.fromFile(file).getLines().mkString("\n")
          if (content.contains("footerStartOffset")) {
            println("✅ Found footerStartOffset in transaction log")
            // Print the specific line with footer offsets
            content.split("\n").filter(line => line.contains("footerStartOffset") || line.contains("footerEndOffset")).foreach(println)
          } else {
            println("❌ No footerStartOffset found in transaction log")
          }
        }
      }

      println("\nTesting equality filter...")
      val filtered = readData.filter($"ts" === "2025-11-07 05:00:00")
      val count = filtered.count()
      println(s"Filtered count: $count")
      filtered.show(false)

      // DEBUG: Try querying the split file directly with tantivy4java to isolate the issue
      println("\n=== DEBUG: Querying split directly with tantivy4java ===")
      val splitFiles = new java.io.File(tempPath).listFiles().filter(_.getName.endsWith(".split"))
      if (splitFiles.nonEmpty) {
        val splitPath = splitFiles(0).getAbsolutePath
        println(s"Split file: $splitPath")

        import io.indextables.tantivy4java.split._
        import io.indextables.tantivy4java.split.merge._

        val metadata = QuickwitSplit.readSplitMetadata(splitPath)
        println(s"Split metadata: ${metadata.getNumDocs} documents")

        val cacheConfig = new SplitCacheManager.CacheConfig("debug-cache")
        val cacheManager = SplitCacheManager.getInstance(cacheConfig)
        val searcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata)

        val queryString = "ts:[2025-11-07T05:00:00Z TO 2025-11-07T05:00:01Z}"
        println(s"Direct query: $queryString")

        val query = searcher.parseQuery(queryString)
        val result = searcher.search(query, 10)
        val directHits = result.getHits.size()

        println(s"Direct query hits: $directHits")

        result.close()
        searcher.close()

        if (directHits != 1) {
          println(s"❌ Direct query also returns 0 rows - issue is in how data is written!")
        } else {
          println(s"✅ Direct query works - issue is in Spark query layer!")
        }
      }

      assert(count == 1, s"Expected 1 row, got $count")
      println("✅ Test passed!")

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
