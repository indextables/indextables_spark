package com.tantivy4spark.debug

import com.tantivy4spark.TestBase
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

class NumericFieldFilteringTest extends TestBase {
  
  private def isNativeLibraryAvailable(): Boolean = {
    try {
      // Use the new ensureLibraryLoaded method
      import com.tantivy4spark.search.TantivyNative
      TantivyNative.ensureLibraryLoaded()
    } catch {
      case _: Exception => false
    }
  }

  test("Comprehensive numeric field filtering test") {
    assume(isNativeLibraryAvailable(), "Native Tantivy library not available - skipping integration test")
    
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data with various numeric types
      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("age", IntegerType, nullable = true),
        StructField("salary", LongType, nullable = true),
        StructField("score", DoubleType, nullable = true),
        StructField("rating", FloatType, nullable = true),
        StructField("active", BooleanType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("department", StringType, nullable = true)
      ))

      val testData = Seq(
        Row(1, 25, 50000L, 85.5, 4.2f, true, "Alice", "Engineering"),
        Row(2, 30, 60000L, 90.0, 4.5f, true, "Bob", "Engineering"), 
        Row(3, 25, 45000L, 78.2, 3.8f, false, "Carol", "Marketing"),
        Row(4, 35, 70000L, 92.1, 4.7f, true, "David", "Engineering"),
        Row(5, 28, 55000L, 88.0, 4.3f, false, "Eve", "Sales"),
        Row(6, 32, 65000L, 91.5, 4.6f, true, "Frank", "Marketing"),
        Row(7, 25, 48000L, 82.3, 4.0f, true, "Grace", "Sales"),
        Row(8, 40, 80000L, 95.0, 4.8f, false, "Henry", "Engineering"),
        Row(9, 29, 58000L, 89.2, 4.4f, true, "Ivy", "Marketing"),
        Row(10, 33, 72000L, 93.8, 4.9f, false, "Jack", "Sales")
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), schema)
      
      // Save the data
      df.write.format("tantivy4spark").mode("overwrite").save(testPath)
      
      // Read back the data
      val readDf = spark.read.format("tantivy4spark").load(testPath)
      readDf.createOrReplaceTempView("employees")
      
      println("=== COMPREHENSIVE NUMERIC FIELD FILTERING TEST ===\n")
      println(s"Test data saved and loaded. Total rows: ${readDf.count()}")
      
      // Show sample data
      println("\nSample data:")
      readDf.show(3, false)

      // Test 1: Integer Equality
      println("\n" + "="*80)
      println("TEST 1: INTEGER EQUALITY (age = 25)")
      println("="*80)
    
      val ageEqualityTest = readDf.filter("age = 25")
      println("Physical Plan:")
      ageEqualityTest.explain(true)
      val ageResults = ageEqualityTest.collect()
      println(s"\nResults: Expected 3 rows with age = 25, got ${ageResults.length}")
      ageResults.toList.foreach(row => println(s"  - ID: ${row.getAs[Int]("id")}, Age: ${row.getAs[Int]("age")}, Name: ${row.getAs[String]("name")}"))
      assert(ageResults.length == 3, s"Expected 3 rows with age = 25, but got ${ageResults.length}")

    // Test 2: Integer Greater Than
    println("\n" + "="*80)
    println("TEST 2: INTEGER GREATER THAN (age > 30)")
    println("="*80)
    
    val ageGreaterTest = readDf.filter("age > 30")
    println("Physical Plan:")
    ageGreaterTest.explain(true)
    val ageGreaterResults = ageGreaterTest.collect()
    println(s"\nResults: Expected 4 rows with age > 30, got ${ageGreaterResults.length}")
    ageGreaterResults.toList.foreach(row => println(s"  - ID: ${row.getAs[Int]("id")}, Age: ${row.getAs[Int]("age")}, Name: ${row.getAs[String]("name")}"))
    assert(ageGreaterResults.length == 4, s"Expected 4 rows with age > 30, but got ${ageGreaterResults.length}")

    // Test 3: Long Equality
    println("\n" + "="*80)
    println("TEST 3: LONG EQUALITY (salary = 60000)")
    println("="*80)
    
    val salaryTest = readDf.filter("salary = 60000")
    println("Physical Plan:")
    salaryTest.explain(true)
    val salaryResults = salaryTest.collect()
    println(s"\nResults: Expected 1 row with salary = 60000, got ${salaryResults.length}")
    salaryResults.toList.foreach(row => println(s"  - ID: ${row.getAs[Int]("id")}, Salary: ${row.getAs[Long]("salary")}, Name: ${row.getAs[String]("name")}"))
    assert(salaryResults.length == 1, s"Expected 1 row with salary = 60000, but got ${salaryResults.length}")

    // Test 4: Long Range Query
    println("\n" + "="*80)
    println("TEST 4: LONG RANGE (salary >= 60000 AND salary <= 70000)")
    println("="*80)
    
    val salaryRangeTest = readDf.filter("salary >= 60000 AND salary <= 70000")
    println("Physical Plan:")
    salaryRangeTest.explain(true)
    val salaryRangeResults = salaryRangeTest.collect()
    println(s"\nResults: Expected 3 rows with salary in range, got ${salaryRangeResults.length}")
    salaryRangeResults.toList.foreach(row => println(s"  - ID: ${row.getAs[Int]("id")}, Salary: ${row.getAs[Long]("salary")}, Name: ${row.getAs[String]("name")}"))
    assert(salaryRangeResults.length == 3, s"Expected 3 rows in salary range, but got ${salaryRangeResults.length}")

    // Test 5: Double Comparison
    println("\n" + "="*80)
    println("TEST 5: DOUBLE COMPARISON (score > 90.0)")
    println("="*80)
    
    val scoreTest = readDf.filter("score > 90.0")
    println("Physical Plan:")
    scoreTest.explain(true)
    val scoreResults = scoreTest.collect()
    println(s"\nResults: Expected 4 rows with score > 90.0, got ${scoreResults.length}")
    scoreResults.toList.foreach(row => println(s"  - ID: ${row.getAs[Int]("id")}, Score: ${row.getAs[Double]("score")}, Name: ${row.getAs[String]("name")}"))
    assert(scoreResults.length == 4, s"Expected 4 rows with score > 90.0, but got ${scoreResults.length}")

    // Test 6: Float Equality
    println("\n" + "="*80)
    println("TEST 6: FLOAT EQUALITY (rating = 4.5)")
    println("="*80)
    
    val ratingTest = readDf.filter("rating = 4.5")
    println("Physical Plan:")
    ratingTest.explain(true)
    val ratingResults = ratingTest.collect()
    println(s"\nResults: Expected 1 row with rating = 4.5, got ${ratingResults.length}")
    ratingResults.toList.foreach(row => println(s"  - ID: ${row.getAs[Int]("id")}, Rating: ${row.getAs[Float]("rating")}, Name: ${row.getAs[String]("name")}"))
    assert(ratingResults.length == 1, s"Expected 1 row with rating = 4.5, but got ${ratingResults.length}")

    // Test 7: Boolean Filtering
    println("\n" + "="*80)
    println("TEST 7: BOOLEAN FILTERING (active = true)")
    println("="*80)
    
    val activeTest = readDf.filter("active = true")
    println("Physical Plan:")
    activeTest.explain(true)
    val activeResults = activeTest.collect()
    println(s"\nResults: Expected 6 rows with active = true, got ${activeResults.length}")
    activeResults.toList.foreach(row => println(s"  - ID: ${row.getAs[Int]("id")}, Active: ${row.getAs[Boolean]("active")}, Name: ${row.getAs[String]("name")}"))
    assert(activeResults.length == 6, s"Expected 6 rows with active = true, but got ${activeResults.length}")

    // Test 8: IN Query
    println("\n" + "="*80)
    println("TEST 8: IN QUERY (age IN (25, 30, 35))")
    println("="*80)
    
    val ageInTest = readDf.filter("age IN (25, 30, 35)")
    println("Physical Plan:")
    ageInTest.explain(true)
    val ageInResults = ageInTest.collect()
    println(s"\nResults: Expected 5 rows with age IN (25, 30, 35), got ${ageInResults.length}")
    ageInResults.toList.foreach(row => println(s"  - ID: ${row.getAs[Int]("id")}, Age: ${row.getAs[Int]("age")}, Name: ${row.getAs[String]("name")}"))
    assert(ageInResults.length == 5, s"Expected 5 rows with age IN (25, 30, 35), but got ${ageInResults.length}")

    // Test 9: Complex AND Query
    println("\n" + "="*80)
    println("TEST 9: COMPLEX AND (age = 25 AND active = true)")
    println("="*80)
    
    val complexAndTest = readDf.filter("age = 25 AND active = true")
    println("Physical Plan:")
    complexAndTest.explain(true)
    val complexAndResults = complexAndTest.collect()
    println(s"\nResults: Expected 2 rows with age = 25 AND active = true, got ${complexAndResults.length}")
    complexAndResults.toList.foreach(row => println(s"  - ID: ${row.getAs[Int]("id")}, Age: ${row.getAs[Int]("age")}, Active: ${row.getAs[Boolean]("active")}, Name: ${row.getAs[String]("name")}"))
    assert(complexAndResults.length == 2, s"Expected 2 rows with complex AND, but got ${complexAndResults.length}")

    // Test 10: String field for comparison (should work)
    println("\n" + "="*80)
    println("TEST 10: STRING EXACT MATCH (department = 'Engineering')")
    println("="*80)
    
    val deptTest = readDf.filter("department = 'Engineering'")
    println("Physical Plan:")
    deptTest.explain(true)
    val deptResults = deptTest.collect()
    println(s"\nResults: Expected 4 rows with department = 'Engineering', got ${deptResults.length}")
    deptResults.toList.foreach(row => println(s"  - ID: ${row.getAs[Int]("id")}, Department: ${row.getAs[String]("department")}, Name: ${row.getAs[String]("name")}"))
    assert(deptResults.length == 4, s"Expected 4 rows with department = 'Engineering', but got ${deptResults.length}")

    // Test 11: SQL vs DataFrame API comparison
    println("\n" + "="*80)
    println("TEST 11: SQL vs DataFrame API (age = 25)")
    println("="*80)
    
    println("DataFrame API:")
    val dfApiTest = readDf.filter($"age" === 25)
    dfApiTest.explain(true)
    val dfApiResults = dfApiTest.count()
    println(s"DataFrame API count: $dfApiResults")
    
    println("\nSQL API:")
    val sqlTest = spark.sql("SELECT * FROM employees WHERE age = 25")
    sqlTest.explain(true)
    val sqlResults = sqlTest.count()
    println(s"SQL API count: $sqlResults")
    
    assert(dfApiResults == sqlResults, s"DataFrame API ($dfApiResults) and SQL API ($sqlResults) should return same count")

      println(s"\n${"="*80}")
      println("NUMERIC FILTERING TEST SUMMARY")
      println(s"${"="*80}")
      println("✓ All numeric field filtering tests completed")
      println("Check the physical plans above to see if filters are being pushed down correctly")
    }
  }
}