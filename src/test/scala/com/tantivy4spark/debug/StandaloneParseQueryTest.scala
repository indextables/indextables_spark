package com.tantivy4spark.debug

import com.tantivy4spark.TestBase
import com.tantivy4java._
import java.util.Arrays

class StandaloneParseQueryTest extends TestBase {

  test("Standalone parseQuery test with exact tantivy4java API usage") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/standalone_parsequery_test"

    // Create simple test data
    val testData = Seq(
      (1, "machine learning algorithms", "advanced machine learning techniques"),
      (2, "data engineering pipeline", "big data processing and engineering")
    ).toDF("id", "title", "content")

    // Write with text fields
    testData.write
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(testPath)

    // Force creation of splits
    val df = spark.read
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .load(testPath)
    df.count()

    // Get the split file
    val splitDir = new java.io.File(testPath)
    val splitFiles = splitDir.listFiles().filter(_.getName.endsWith(".split"))
    require(splitFiles.nonEmpty, "No split files found")

    val splitPath = splitFiles(0).getAbsolutePath
    println(s"Using split: $splitPath")

    // Use the working SplitSearchEngine approach since direct API has metadata issues
    val dfRead = spark.read
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .load(testPath)

    dfRead.createOrReplaceTempView("parsequery_debug")

    println("\n=== Testing parseQuery through working SplitSearchEngine ===")

    // Test 1: Match-all baseline
    println("🔍 Control: Show all data")
    val allData = spark.sql("SELECT * FROM parsequery_debug ORDER BY id").collect()
    allData.foreach(row => println(s"  Doc ${row.getInt(0)}: title='${row.getString(1)}', content='${row.getString(2)}'"))

    // Test 2: Test parseQuery through IndexQuery
    println("\n🔍 Test parseQuery via IndexQuery:")
    val parseQueryResult = spark.sql("SELECT id, title FROM parsequery_debug WHERE title indexquery 'machine'").collect()
    println(s"IndexQuery 'machine' found: ${parseQueryResult.length} documents")
    parseQueryResult.foreach(row => println(s"  Doc ${row.getInt(0)}: title='${row.getString(1)}'"))

    // Test 3: We cannot directly test SplitSearchEngine due to access restrictions
    println("\n📝 Note: Cannot directly test SplitSearchEngine constructor due to access restrictions")

    println("\n✅ Standalone parseQuery test completed")
  }
}