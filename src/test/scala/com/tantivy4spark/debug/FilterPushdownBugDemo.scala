package com.tantivy4spark.debug

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.nio.file.Files

object FilterPushdownBugDemo {
  def main(args: Array[String]): Unit = {
    // Workaround for Java security issue
    System.setProperty("user.name", System.getProperty("user.name", "testuser"))

    val spark = SparkSession
      .builder()
      .appName("FilterPushdownBugDemo")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.indextables.aws.accessKey", "test-access")
      .config("spark.indextables.aws.secretKey", "test-secret")
      .config("spark.indextables.aws.region", "us-east-1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val tempDir  = Files.createTempDirectory("tantivy4spark_bug_demo")
    val testPath = tempDir.resolve("test_data").toString

    println("\n" + "=" * 80)
    println("FILTER PUSHDOWN BUG DEMONSTRATION")
    println("=" * 80 + "\n")

    // Create sample data with the exact schema described
    val schema = StructType(
      Seq(
        StructField("review_text", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true),
        StructField("signup_date", StringType, nullable = true)
      )
    )

    val testData = Seq(
      Row("I love this product with my dog", 25, "2023-01-15"),
      Row("Great service and fast delivery", 32, "2023-02-20"),
      Row("The quality is excellent", 28, "2023-01-15"),
      Row("Not satisfied with the purchase", 45, "2023-03-10"),
      Row("Amazing experience overall", 25, "2023-04-05"),
      Row("Product arrived damaged", 38, "2023-01-15"),
      Row("Would definitely recommend", 25, "2023-05-12"),
      Row("Terrible customer support", 52, "2023-06-01"),
      Row("My dog enjoys the treats", 30, "2023-07-10"),
      Row("Walking the dog is fun", 28, "2023-08-15")
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), schema)

    // Save using tantivy4spark format
    df.write.format("tantivy4spark").mode("overwrite").save(testPath)

    // Read back the data
    val readDf = spark.read.format("tantivy4spark").load(testPath)
    readDf.createOrReplaceTempView("reviews")

    println("Sample data created and saved in tantivy4spark format")
    println(s"Total rows: ${readDf.count()}\n")

    // Bug #1: Non-text field filtering
    println("=" * 80)
    println("BUG #1: NON-TEXT FIELD FILTER PUSHDOWN")
    println("=" * 80 + "\n")

    println("1. Testing filter on 'age' field (IntegerType) - age = 25:")
    val ageFilter = readDf.filter("age = 25")
    println("\nPhysical Plan:")
    ageFilter.explain(true)
    val ageResults = ageFilter.collect()
    println(s"\nResults: Found ${ageResults.length} rows (expected 3)")
    ageResults.foreach(row => println(s"  - $row"))

    println("\n2. Testing filter on 'signup_date' field (StringType) - signup_date = '2023-01-15':")
    val dateFilter = readDf.filter("signup_date = '2023-01-15'")
    println("\nPhysical Plan:")
    dateFilter.explain(true)
    val dateResults = dateFilter.collect()
    println(s"\nResults: Found ${dateResults.length} rows (expected 3)")
    dateResults.foreach(row => println(s"  - $row"))

    println("\n3. Testing filter on 'review_text' field (should work) - contains 'dog':")
    val textFilter = readDf.filter($"review_text".contains("dog"))
    println("\nPhysical Plan:")
    textFilter.explain(true)
    val textResults = textFilter.collect()
    println(s"\nResults: Found ${textResults.length} rows (expected 3)")
    textResults.foreach(row => println(s"  - $row"))

    // Bug #2: SQL LIKE pushdown
    println("\n" + "=" * 80)
    println("BUG #2: SQL LIKE OPERATION PUSHDOWN")
    println("=" * 80 + "\n")

    println("1. DataFrame API with contains (PUSHDOWN WORKS):")
    val dfApiFilter = readDf.filter($"review_text".contains("dog"))
    println("\nPhysical Plan:")
    dfApiFilter.explain(true)
    println(
      s"\nPlan contains PushedFilters: ${dfApiFilter.queryExecution.executedPlan.toString.contains("PushedFilters")}"
    )

    println("\n2. SQL with LIKE (PUSHDOWN FAILS):")
    val sqlLikeQuery = spark.sql("SELECT * FROM reviews WHERE review_text LIKE '%dog%'")
    println("\nPhysical Plan:")
    sqlLikeQuery.explain(true)
    println(
      s"\nPlan contains PushedFilters: ${sqlLikeQuery.queryExecution.executedPlan.toString.contains("PushedFilters")}"
    )
    val sqlResults = sqlLikeQuery.collect()
    println(s"Results: Found ${sqlResults.length} rows (expected 3)")

    println("\n3. SQL with LIKE prefix pattern:")
    val sqlPrefixQuery = spark.sql("SELECT * FROM reviews WHERE review_text LIKE 'My dog%'")
    println("\nPhysical Plan:")
    sqlPrefixQuery.explain(true)
    val sqlPrefixResults = sqlPrefixQuery.collect()
    println(s"Results: Found ${sqlPrefixResults.length} rows (expected 1)")

    println("\n" + "=" * 80)
    println("ANALYSIS SUMMARY")
    println("=" * 80 + "\n")

    println("Bug #1: Non-text fields (age, signup_date) may not filter correctly")
    println("  - IntegerType fields need proper type conversion for tantivy4java")
    println("  - StringType fields are being treated as 'text' which uses phrase queries")
    println("")
    println("Bug #2: SQL LIKE operations don't get pushed down")
    println("  - DataFrame API .contains() -> StringContains filter -> Pushed down ✓")
    println("  - SQL LIKE '%pattern%' -> Not recognized as pushable filter -> Full scan ✗")
    println("  - This causes significant performance degradation for SQL queries")

    spark.stop()

    // Clean up
    import org.apache.commons.io.FileUtils
    FileUtils.deleteDirectory(tempDir.toFile)
  }
}
