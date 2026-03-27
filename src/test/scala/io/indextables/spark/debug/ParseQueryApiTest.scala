package io.indextables.spark.debug

import java.util.Arrays

import io.indextables.spark.TestBase
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import io.indextables.tantivy4java.split.SplitCacheManager

class ParseQueryApiTest extends TestBase {

  test("Test if enhanced parseQuery API methods are available") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/parsequery_api_test"

    // Create a simple split with text fields
    val testData = Seq(
      (1, "machine learning", "tech"),
      (2, "data engineering", "tech")
    ).toDF("id", "title", "category")

    // Write with text fields
    testData.write
      .format(INDEXTABLES_FORMAT)
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.typemap.category", "text")
      .mode("overwrite")
      .save(testPath)

    // Read the data to get splits
    val df = spark.read
      .format(INDEXTABLES_FORMAT)
      .load(testPath)

    // Force materialization to create splits
    df.count()

    // Now test the parseQuery API directly
    println("🔍 Testing parseQuery API availability...")

    try {
      // Get the first split file
      val splitDir   = new java.io.File(testPath)
      val splitFiles = splitDir.listFiles().filter(_.getName.endsWith(".split"))

      if (splitFiles.nonEmpty) {
        val splitPath = splitFiles(0).getAbsolutePath
        println(s"Using split: $splitPath")

        // Test SplitSearcher parseQuery methods
        val cacheConfig  = new SplitCacheManager.CacheConfig("test-cache")
        val cacheManager = SplitCacheManager.getInstance(cacheConfig)
        val metadata     = QuickwitSplit.readSplitMetadata(splitPath)

        val searcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata)
        try {

          println("✅ SplitSearcher created successfully")

          // Test method 1: parseQuery(String)
          try {
            val _query1 = searcher.parseQuery("machine")
            println("✅ parseQuery(String) method available and working")
          } catch {
            case e: Exception =>
              println(s"❌ parseQuery(String) failed: ${e.getMessage}")
          }

          // Test method 2: parseQuery(String, String)
          try {
            val _query2 = searcher.parseQuery("machine", "title")
            println("✅ parseQuery(String, String) method available and working")
          } catch {
            case e: NoSuchMethodError =>
              println("❌ parseQuery(String, String) method not available - need newer tantivy4java")
            case e: Exception =>
              println(s"❌ parseQuery(String, String) failed: ${e.getMessage}")
          }

          // Test method 3: parseQuery(String, List<String>)
          try {
            val _query3 = searcher.parseQuery("machine", Arrays.asList("title", "category"))
            println("✅ parseQuery(String, List<String>) method available and working")
          } catch {
            case e: NoSuchMethodError =>
              println("❌ parseQuery(String, List<String>) method not available - need newer tantivy4java")
            case e: Exception =>
              println(s"❌ parseQuery(String, List<String>) failed: ${e.getMessage}")
          }

        } finally
          if (searcher != null) searcher.close()
      } else {
        println("❌ No split files found")
      }

    } catch {
      case e: Exception =>
        println(s"❌ Test failed: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
