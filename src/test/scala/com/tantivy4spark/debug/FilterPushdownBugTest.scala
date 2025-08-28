/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tantivy4spark.debug

import com.tantivy4spark.TestBase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.nio.file.{Files, Path}
import org.apache.commons.io.FileUtils
import java.io.File

class FilterPushdownBugTest extends TestBase {

  var testTempDir: Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testTempDir = Files.createTempDirectory("tantivy4spark_bug_test")
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (testTempDir != null) {
      FileUtils.deleteDirectory(testTempDir.toFile)
    }
    super.afterAll()
  }

  test("Bug #1: Filter pushdown should work correctly for non-text fields (age, signup_date)") {
    val spark = this.spark
    import spark.implicits._

    val testPath = testTempDir.resolve("test_non_text_fields").toString

    // Create sample data with the exact schema described
    val schema = StructType(Seq(
      StructField("review_text", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("signup_date", StringType, nullable = true)
    ))

    val testData = Seq(
      Row("I love this product with my dog", 25, "2023-01-15"),
      Row("Great service and fast delivery", 32, "2023-02-20"),
      Row("The quality is excellent", 28, "2023-01-15"),
      Row("Not satisfied with the purchase", 45, "2023-03-10"),
      Row("Amazing experience overall", 25, "2023-04-05"),
      Row("Product arrived damaged", 38, "2023-01-15"),
      Row("Would definitely recommend", 25, "2023-05-12"),
      Row("Terrible customer support", 52, "2023-06-01")
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), schema)
    
    // Save using tantivy4spark format
    df.write.format("tantivy4spark").mode("overwrite").save(testPath)

    // Read back the data
    val readDf = spark.read.format("tantivy4spark").load(testPath)
    readDf.createOrReplaceTempView("reviews")

    println("\n=== Testing Non-Text Field Filters ===\n")

    // Test 1: Filter on age field (IntegerType)
    println("Test 1: Filter on age = 25")
    val ageFilter = readDf.filter("age = 25")
    ageFilter.explain(true)
    val ageResults = ageFilter.collect()
    println(s"Expected 3 rows with age = 25, got: ${ageResults.length}")
    ageResults.foreach(row => println(s"  - ${row}"))
    assert(ageResults.length == 3, s"Expected 3 rows with age = 25, but got ${ageResults.length}")

    // Test 2: Filter on signup_date field (StringType but not text-indexed)
    println("\nTest 2: Filter on signup_date = '2023-01-15'")
    val dateFilter = readDf.filter("signup_date = '2023-01-15'")
    dateFilter.explain(true)
    val dateResults = dateFilter.collect()
    println(s"Expected 3 rows with signup_date = '2023-01-15', got: ${dateResults.length}")
    dateResults.foreach(row => println(s"  - ${row}"))
    assert(dateResults.length == 3, s"Expected 3 rows with signup_date = '2023-01-15', but got ${dateResults.length}")

    // Test 3: Combined filter
    println("\nTest 3: Combined filter - age = 25 AND signup_date = '2023-01-15'")
    val combinedFilter = readDf.filter("age = 25 AND signup_date = '2023-01-15'")
    combinedFilter.explain(true)
    val combinedResults = combinedFilter.collect()
    println(s"Expected 1 row with age = 25 AND signup_date = '2023-01-15', got: ${combinedResults.length}")
    combinedResults.foreach(row => println(s"  - ${row}"))
    assert(combinedResults.length == 1, s"Expected 1 row with combined filter, but got ${combinedResults.length}")

    // Test 4: Text field for comparison (this should work)
    println("\nTest 4: Filter on review_text containing 'dog' (should work)")
    val textFilter = readDf.filter($"review_text".contains("dog"))
    textFilter.explain(true)
    val textResults = textFilter.collect()
    println(s"Expected 1 row with 'dog' in review_text, got: ${textResults.length}")
    textResults.foreach(row => println(s"  - ${row}"))
    assert(textResults.length == 1, s"Expected 1 row with 'dog' in review_text, but got ${textResults.length}")
  }

  test("Bug #2: SQL LIKE operations should get pushed down for better performance") {
    val spark = this.spark
    import spark.implicits._

    val testPath = testTempDir.resolve("test_sql_like").toString

    // Create sample data
    val testData = Seq(
      ("I love this product with my dog", 25, "2023-01-15"),
      ("Great service and fast delivery", 32, "2023-02-20"),
      ("The dog park is nearby", 28, "2023-03-15"),
      ("My dog enjoys the treats", 45, "2023-04-10"),
      ("Amazing experience overall", 30, "2023-05-05"),
      ("Walking the dog is fun", 38, "2023-06-15"),
      ("Cats and dogs living together", 25, "2023-07-12"),
      ("Terrible customer support", 52, "2023-08-01")
    ).toDF("review_text", "age", "signup_date")

    // Save using tantivy4spark format
    testData.write.format("tantivy4spark").mode("overwrite").save(testPath)

    // Read back and create temp view
    val readDf = spark.read.format("tantivy4spark").load(testPath)
    readDf.createOrReplaceTempView("reviews_like")

    println("\n=== Testing SQL LIKE Pushdown ===\n")

    // Test 1: DataFrame API with contains (should push down)
    println("Test 1: DataFrame API with contains('dog')")
    val dfApiFilter = readDf.filter($"review_text".contains("dog"))
    println("DataFrame API Physical Plan:")
    dfApiFilter.explain(true)
    val dfApiResults = dfApiFilter.collect()
    println(s"DataFrame API found ${dfApiResults.length} rows with 'dog'")
    assert(dfApiResults.length == 5, s"Expected 5 rows with 'dog', but got ${dfApiResults.length}")

    // Test 2: SQL with LIKE (should also push down but currently doesn't)
    println("\nTest 2: SQL with LIKE '%dog%'")
    val sqlLikeQuery = spark.sql("SELECT * FROM reviews_like WHERE review_text LIKE '%dog%'")
    println("SQL LIKE Physical Plan:")
    sqlLikeQuery.explain(true)
    val sqlLikeResults = sqlLikeQuery.collect()
    println(s"SQL LIKE found ${sqlLikeResults.length} rows with 'dog'")
    assert(sqlLikeResults.length == 5, s"Expected 5 rows with 'dog', but got ${sqlLikeResults.length}")

    // Check if pushdown occurred by examining the physical plan
    val dfApiPlan = dfApiFilter.queryExecution.executedPlan.toString
    val sqlLikePlan = sqlLikeQuery.queryExecution.executedPlan.toString
    
    println("\n=== Pushdown Analysis ===")
    println(s"DataFrame API uses pushdown: ${dfApiPlan.contains("PushedFilters")}")
    println(s"SQL LIKE uses pushdown: ${sqlLikePlan.contains("PushedFilters")}")
    
    // The SQL LIKE should have similar performance characteristics as DataFrame API
    // Currently it doesn't push down, which is the bug
    if (!sqlLikePlan.contains("PushedFilters") || !sqlLikePlan.contains("StringContains")) {
      println("WARNING: SQL LIKE is not being pushed down to the data source!")
      println("This results in poor performance as Spark has to scan all data.")
    }

    // Test 3: SQL with prefix pattern
    println("\nTest 3: SQL with LIKE 'dog%' (prefix)")
    val sqlPrefixQuery = spark.sql("SELECT * FROM reviews_like WHERE review_text LIKE 'My dog%'")
    println("SQL Prefix LIKE Physical Plan:")
    sqlPrefixQuery.explain(true)
    val sqlPrefixResults = sqlPrefixQuery.collect()
    println(s"SQL Prefix LIKE found ${sqlPrefixResults.length} rows")
    assert(sqlPrefixResults.length == 1, s"Expected 1 row starting with 'My dog', but got ${sqlPrefixResults.length}")

    // Test 4: SQL with suffix pattern
    println("\nTest 4: SQL with LIKE '%dog' (suffix)")
    val sqlSuffixQuery = spark.sql("SELECT * FROM reviews_like WHERE review_text LIKE '%dog'")
    println("SQL Suffix LIKE Physical Plan:")
    sqlSuffixQuery.explain(true)
    val sqlSuffixResults = sqlSuffixQuery.collect()
    println(s"SQL Suffix LIKE found ${sqlSuffixResults.length} rows")
    assert(sqlSuffixResults.length == 1, s"Expected 1 row ending with 'dog', but got ${sqlSuffixResults.length}")
  }

  test("Verify exact field type handling in schema") {
    val spark = this.spark
    import spark.implicits._
    
    val testPath = testTempDir.resolve("test_field_types").toString
    
    // Create data with explicit types
    val schema = StructType(Seq(
      StructField("text_field", StringType),
      StructField("int_field", IntegerType),
      StructField("string_field", StringType) // This should NOT be treated as text
    ))
    
    val testData = Seq(
      Row("searchable text content", 100, "exact-match-string"),
      Row("another text document", 200, "different-string"),
      Row("searchable content again", 100, "exact-match-string")
    )
    
    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), schema)
    df.write.format("tantivy4spark").mode("overwrite").save(testPath)
    
    val readDf = spark.read.format("tantivy4spark").load(testPath)
    
    // Test exact string matching (not text search)
    println("\n=== Testing String vs Text Field Handling ===")
    val stringExactMatch = readDf.filter("string_field = 'exact-match-string'")
    stringExactMatch.explain(true)
    val stringResults = stringExactMatch.collect()
    println(s"String exact match found ${stringResults.length} rows")
    assert(stringResults.length == 2, s"Expected 2 exact matches, got ${stringResults.length}")
    
    // Test integer field
    val intMatch = readDf.filter("int_field = 100")
    intMatch.explain(true)
    val intResults = intMatch.collect()
    println(s"Integer match found ${intResults.length} rows")
    assert(intResults.length == 2, s"Expected 2 integer matches, got ${intResults.length}")
  }
}