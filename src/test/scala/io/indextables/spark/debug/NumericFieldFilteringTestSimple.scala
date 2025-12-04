package io.indextables.spark.debug

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import io.indextables.spark.TestBase

class NumericFieldFilteringTestSimple extends TestBase {

  private def isNativeLibraryAvailable(): Boolean =
    try {
      import io.indextables.spark.search.TantivyNative
      TantivyNative.ensureLibraryLoaded()
      true
    } catch {
      case _: Exception => false
    }

  test("numeric field filtering test") {
    assume(isNativeLibraryAvailable(), "Native Tantivy library not available - skipping test")

    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data with numeric fields
      val testData = Seq(
        (1, 25, 50000L, 85.5, true, "Alice"),
        (2, 30, 60000L, 90.0, true, "Bob"),
        (3, 25, 45000L, 78.2, false, "Carol"),
        (4, 35, 70000L, 92.1, true, "David"),
        (5, 25, 55000L, 88.0, false, "Eve")
      ).toDF("id", "age", "salary", "score", "active", "name")

      // Save the data
      testData.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(testPath)

      // Read back the data
      val readDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(testPath)
      readDf.createOrReplaceTempView("employees")

      println(s"Total rows: ${readDf.count()}")

      // Test 1: Integer equality
      println("\n=== TEST 1: INTEGER EQUALITY (age = 25) ===")
      val ageTest = readDf.filter("age = 25")
      println("Physical Plan:")
      ageTest.explain(true)
      val ageResults = ageTest.collect()
      println(s"Expected: 3 rows, Got: ${ageResults.length}")
      ageResults.foreach(row => println(s"  - ${row.getAs[String]("name")}: age=${row.getAs[Int]("age")}"))
      assert(ageResults.length == 3, s"Expected 3 rows with age = 25, got ${ageResults.length}")

      // Test 2: Greater than
      println("\n=== TEST 2: GREATER THAN (age > 28) ===")
      val gtTest = readDf.filter("age > 28")
      println("Physical Plan:")
      gtTest.explain(true)
      val gtResults = gtTest.collect()
      println(s"Expected: 2 rows, Got: ${gtResults.length}")
      gtResults.foreach(row => println(s"  - ${row.getAs[String]("name")}: age=${row.getAs[Int]("age")}"))
      assert(gtResults.length == 2, s"Expected 2 rows with age > 28, got ${gtResults.length}")

      // Test 3: Long field
      println("\n=== TEST 3: LONG EQUALITY (salary = 60000) ===")
      val salaryTest = readDf.filter("salary = 60000")
      println("Physical Plan:")
      salaryTest.explain(true)
      val salaryResults = salaryTest.collect()
      println(s"Expected: 1 row, Got: ${salaryResults.length}")
      salaryResults.foreach(row => println(s"  - ${row.getAs[String]("name")}: salary=${row.getAs[Long]("salary")}"))
      assert(salaryResults.length == 1, s"Expected 1 row with salary = 60000, got ${salaryResults.length}")

      // Test 4: Boolean field
      println("\n=== TEST 4: BOOLEAN (active = true) ===")
      val boolTest = readDf.filter("active = true")
      println("Physical Plan:")
      boolTest.explain(true)
      val boolResults = boolTest.collect()
      println(s"Expected: 3 rows, Got: ${boolResults.length}")
      boolResults.foreach(row => println(s"  - ${row.getAs[String]("name")}: active=${row.getAs[Boolean]("active")}"))
      assert(boolResults.length == 3, s"Expected 3 rows with active = true, got ${boolResults.length}")

      // Test 5: SQL vs DataFrame API
      println("\n=== TEST 5: SQL vs DataFrame API ===")
      val dfCount  = readDf.filter($"age" === 25).collect().length
      val sqlCount = spark.sql("SELECT * FROM employees WHERE age = 25").collect().length
      println(s"DataFrame API: $dfCount rows")
      println(s"SQL API: $sqlCount rows")
      assert(dfCount == sqlCount, s"DataFrame ($dfCount) and SQL ($sqlCount) should match")

      println("\n=== ALL TESTS PASSED ===")
    }
  }
}
