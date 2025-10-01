package com.tantivy4spark.debug

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.nio.file.Files

object NumericFieldFilteringDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("user.name", System.getProperty("user.name", "testuser"))

    val spark = SparkSession
      .builder()
      .appName("NumericFieldFilteringDemo")
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

    val tempDir  = Files.createTempDirectory("tantivy4spark_numeric_demo")
    val testPath = tempDir.resolve("numeric_test").toString

    println("\n" + "=" * 80)
    println("NUMERIC FIELD FILTERING DEMO")
    println("=" * 80 + "\n")

    // Create test data with numeric fields
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("age", IntegerType, nullable = true),
        StructField("salary", LongType, nullable = true),
        StructField("score", DoubleType, nullable = true),
        StructField("active", BooleanType, nullable = true),
        StructField("name", StringType, nullable = true)
      )
    )

    val testData = Seq(
      Row(1, 25, 50000L, 85.5, true, "Alice"),
      Row(2, 30, 60000L, 90.0, true, "Bob"),
      Row(3, 25, 45000L, 78.2, false, "Carol"),
      Row(4, 35, 70000L, 92.1, true, "David"),
      Row(5, 28, 55000L, 88.0, false, "Eve"),
      Row(6, 25, 48000L, 82.3, true, "Grace")
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), schema)

    // Save the data
    df.write.format("tantivy4spark").mode("overwrite").save(testPath)

    // Read back the data
    val readDf = spark.read.format("tantivy4spark").load(testPath)
    readDf.createOrReplaceTempView("employees")

    println("Sample data:")
    readDf.show()
    println(s"Total rows: ${readDf.count()}\n")

    // Test 1: Integer Equality
    println("=" * 60)
    println("TEST 1: INTEGER EQUALITY (age = 25)")
    println("=" * 60)

    val ageTest = readDf.filter("age = 25")
    println("Physical Plan:")
    ageTest.explain(extended = false)
    val ageResults = ageTest.collect()
    println(s"\nExpected: 3 rows, Got: ${ageResults.length}")
    ageResults.foreach(row => println(s"  - ${row.getAs[String]("name")}: age=${row.getAs[Int]("age")}"))

    // Test 2: Integer Greater Than
    println("\n" + "=" * 60)
    println("TEST 2: INTEGER GREATER THAN (age > 28)")
    println("=" * 60)

    val ageGtTest = readDf.filter("age > 28")
    println("Physical Plan:")
    ageGtTest.explain(extended = false)
    val ageGtResults = ageGtTest.collect()
    println(s"\nExpected: 2 rows, Got: ${ageGtResults.length}")
    ageGtResults.foreach(row => println(s"  - ${row.getAs[String]("name")}: age=${row.getAs[Int]("age")}"))

    // Test 3: Long Range
    println("\n" + "=" * 60)
    println("TEST 3: LONG RANGE (salary >= 50000 AND salary <= 60000)")
    println("=" * 60)

    val salaryRangeTest = readDf.filter("salary >= 50000 AND salary <= 60000")
    println("Physical Plan:")
    salaryRangeTest.explain(extended = false)
    val salaryResults = salaryRangeTest.collect()
    println(s"\nExpected: 3 rows, Got: ${salaryResults.length}")
    salaryResults.foreach(row => println(s"  - ${row.getAs[String]("name")}: salary=${row.getAs[Long]("salary")}"))

    // Test 4: Double Comparison
    println("\n" + "=" * 60)
    println("TEST 4: DOUBLE COMPARISON (score > 85.0)")
    println("=" * 60)

    val scoreTest = readDf.filter("score > 85.0")
    println("Physical Plan:")
    scoreTest.explain(extended = false)
    val scoreResults = scoreTest.collect()
    println(s"\nExpected: 4 rows, Got: ${scoreResults.length}")
    scoreResults.foreach(row => println(s"  - ${row.getAs[String]("name")}: score=${row.getAs[Double]("score")}"))

    // Test 5: Boolean
    println("\n" + "=" * 60)
    println("TEST 5: BOOLEAN (active = true)")
    println("=" * 60)

    val booleanTest = readDf.filter("active = true")
    println("Physical Plan:")
    booleanTest.explain(extended = false)
    val booleanResults = booleanTest.collect()
    println(s"\nExpected: 4 rows, Got: ${booleanResults.length}")
    booleanResults.foreach(row => println(s"  - ${row.getAs[String]("name")}: active=${row.getAs[Boolean]("active")}"))

    // Test 6: IN Query
    println("\n" + "=" * 60)
    println("TEST 6: IN QUERY (age IN (25, 30))")
    println("=" * 60)

    val inTest = readDf.filter("age IN (25, 30)")
    println("Physical Plan:")
    inTest.explain(extended = false)
    val inResults = inTest.collect()
    println(s"\nExpected: 4 rows, Got: ${inResults.length}")
    inResults.foreach(row => println(s"  - ${row.getAs[String]("name")}: age=${row.getAs[Int]("age")}"))

    // Test 7: Compare SQL vs DataFrame API
    println("\n" + "=" * 60)
    println("TEST 7: SQL vs DataFrame API")
    println("=" * 60)

    val dfApiResult = readDf.filter($"age" === 25).count()
    val sqlResult   = spark.sql("SELECT * FROM employees WHERE age = 25").count()

    println(s"DataFrame API (age === 25): $dfApiResult rows")
    println(s"SQL (age = 25): $sqlResult rows")
    println(s"Match: ${dfApiResult == sqlResult}")

    // Test 8: Check for pushdown in plans
    println("\n" + "=" * 60)
    println("TEST 8: PUSHDOWN ANALYSIS")
    println("=" * 60)

    val testQuery  = readDf.filter("age = 25")
    val planString = testQuery.queryExecution.executedPlan.toString()

    println("Executed Plan Analysis:")
    println(s"Contains 'PushedFilters': ${planString.contains("PushedFilters")}")
    println(s"Contains 'EqualTo': ${planString.contains("EqualTo")}")
    println(s"Contains 'tantivy4spark': ${planString.toLowerCase.contains("tantivy")}")

    if (planString.contains("PushedFilters")) {
      println("✓ Filters are being pushed down!")
    } else {
      println("✗ Filters may not be pushed down properly")
    }

    println("\nFull Executed Plan:")
    testQuery.explain(true)

    spark.stop()

    // Clean up
    import org.apache.commons.io.FileUtils
    FileUtils.deleteDirectory(tempDir.toFile)

    println("\n" + "=" * 80)
    println("DEMO COMPLETED")
    println("=" * 80)
  }
}
