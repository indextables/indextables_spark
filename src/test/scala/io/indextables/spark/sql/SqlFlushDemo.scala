package io.indextables.spark.sql

/** Demo to test SQL FLUSH command functionality */
object SqlFlushDemo extends App {

  // Create minimal Spark session for testing
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Tantivy4Spark Flush Demo")
    .master("local[2]")
    .config("spark.sql.extensions", "io.indextables.spark.sql.Tantivy4SparkSessionExtension")
    .getOrCreate()

  try {
    println("=== Testing Tantivy4Spark FLUSH command ===")

    // Test 1: Direct command execution
    println("\n1. Testing FlushTantivyCacheCommand directly:")
    val command = FlushTantivyCacheCommand()
    val results = command.run(spark)

    results.foreach { row =>
      val cacheType      = row.getString(0) // cache_type
      val status         = row.getString(1) // status
      val clearedEntries = row.getLong(2)   // cleared_entries
      val message        = row.getString(3) // message

      println(s"  $cacheType: $status ($clearedEntries entries) - $message")
    }

    // Test 2: SQL Parser test
    println("\n2. Testing SQL parser:")
    val parser        = new Tantivy4SparkSqlParser(spark.sessionState.sqlParser)
    val parsedCommand = parser.parsePlan("FLUSH TANTIVY4SPARK SEARCHER CACHE")
    println(s"  Parsed command type: ${parsedCommand.getClass.getSimpleName}")

    // Test 3: Create some data first, then flush
    println("\n3. Testing with actual data:")

    import spark.implicits._
    val testData = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")

    // Try to write to tantivy4spark (this may fail but that's ok for the demo)
    try {
      val tempPath = java.nio.file.Files.createTempDirectory("tantivy-test").toString
      testData.write.format("tantivy4spark").save(tempPath)

      // Read it back to create some cache entries
      val readData = spark.read.format("tantivy4spark").load(tempPath)
      val count    = readData.count()
      println(s"  Successfully wrote and read $count rows to/from Tantivy4Spark")

      // Now flush again
      val flushResults = command.run(spark)
      flushResults.foreach { row =>
        val cacheType      = row.getString(0) // cache_type
        val clearedEntries = row.getLong(2)   // cleared_entries
        println(s"  Post-data flush - $cacheType: $clearedEntries entries cleared")
      }

    } catch {
      case e: Exception =>
        println(s"  (Data test skipped - Tantivy4Spark may not be fully configured: ${e.getMessage})")
    }

    println("\n=== FLUSH command demo completed successfully! ===")

  } finally
    spark.stop()
}
