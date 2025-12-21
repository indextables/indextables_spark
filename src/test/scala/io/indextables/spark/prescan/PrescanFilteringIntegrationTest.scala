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

package io.indextables.spark.prescan

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import io.indextables.spark.TestBase
import org.scalatest.BeforeAndAfterAll

/**
 * Comprehensive integration tests for prescan filtering.
 *
 * These tests verify that prescan filtering produces IDENTICAL results to
 * non-prescan queries across all data types and query patterns.
 *
 * Test strategy:
 * 1. Create multiple splits with diverse data types
 * 2. Execute identical queries with prescan enabled and disabled
 * 3. Compare result sets to ensure prescan never incorrectly excludes valid data
 */
class PrescanFilteringIntegrationTest extends TestBase with BeforeAndAfterAll {

  // Provider format string
  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Clear any session state
    PrescanSessionState.clearAll()
  }

  override def afterAll(): Unit = {
    PrescanSessionState.clearAll()
    super.afterAll()
  }

  // ===========================================
  // Helper to compare results with/without prescan
  // ===========================================

  /**
   * Execute query with and without prescan and verify identical results.
   *
   * @param tablePath Path to the table
   * @param queryFn Function that takes a DataFrame and returns filtered DataFrame
   * @param description Test description for logging
   */
  private def verifyPrescanEquivalence(
    tablePath: String,
    queryFn: DataFrame => DataFrame,
    description: String
  ): Unit = {
    // Query WITHOUT prescan
    val dfWithoutPrescan = spark.read
      .format(provider)
      .option("spark.indextables.read.prescan.enabled", "false")
      .load(tablePath)
    val resultsWithoutPrescan = queryFn(dfWithoutPrescan).collect().sortBy(_.toString)

    // Query WITH prescan (force low threshold to trigger prescan)
    val dfWithPrescan = spark.read
      .format(provider)
      .option("spark.indextables.read.prescan.enabled", "true")
      .option("spark.indextables.read.prescan.minSplitThreshold", "1")
      .load(tablePath)
    val resultsWithPrescan = queryFn(dfWithPrescan).collect().sortBy(_.toString)

    // Compare results
    val countWithout = resultsWithoutPrescan.length
    val countWith = resultsWithPrescan.length

    assert(
      countWithout == countWith,
      s"$description: Row count mismatch! Without prescan: $countWithout, With prescan: $countWith"
    )

    // Compare actual data
    resultsWithoutPrescan.zip(resultsWithPrescan).zipWithIndex.foreach { case ((rowWithout, rowWith), idx) =>
      assert(
        rowWithout.toString == rowWith.toString,
        s"$description: Row $idx mismatch!\n  Without prescan: $rowWithout\n  With prescan: $rowWith"
      )
    }

    println(s"✅ $description: $countWithout rows match")
  }

  // ===========================================
  // Multi-split data creation helpers
  // ===========================================

  /**
   * Create test table with multiple splits containing all data types.
   * Each split contains unique data to test prescan filtering effectiveness.
   */
  private def createComprehensiveTestTable(path: String): Unit = {
    // Split 1: Users A-M with dates in 2023
    val split1Data = Seq(
      Row(1, "alice", "Alice Smith", 25, 75000.50, Date.valueOf("2023-01-15"), Timestamp.valueOf("2023-01-15 10:30:00"), true, Row("NYC", "10001"), Seq("scala", "java")),
      Row(2, "bob", "Bob Johnson", 30, 85000.00, Date.valueOf("2023-03-20"), Timestamp.valueOf("2023-03-20 14:45:00"), false, Row("LA", "90001"), Seq("python")),
      Row(3, "carol", "Carol Williams", 28, 72000.25, Date.valueOf("2023-06-10"), Timestamp.valueOf("2023-06-10 09:15:00"), true, Row("NYC", "10002"), Seq("java", "go")),
      Row(4, "david", "David Brown", 35, 95000.75, Date.valueOf("2023-08-05"), Timestamp.valueOf("2023-08-05 16:00:00"), true, Row("SF", "94102"), Seq("rust", "c++")),
      Row(5, "emma", "Emma Davis", 27, 68000.00, Date.valueOf("2023-11-22"), Timestamp.valueOf("2023-11-22 11:30:00"), false, Row("LA", "90002"), Seq("javascript"))
    )

    // Split 2: Users N-Z with dates in 2024
    val split2Data = Seq(
      Row(6, "nate", "Nate Miller", 32, 88000.00, Date.valueOf("2024-01-10"), Timestamp.valueOf("2024-01-10 08:00:00"), true, Row("Chicago", "60601"), Seq("python", "sql")),
      Row(7, "olivia", "Olivia Wilson", 29, 79000.50, Date.valueOf("2024-02-14"), Timestamp.valueOf("2024-02-14 12:00:00"), true, Row("NYC", "10003"), Seq("scala")),
      Row(8, "peter", "Peter Taylor", 40, 120000.00, Date.valueOf("2024-03-25"), Timestamp.valueOf("2024-03-25 17:30:00"), false, Row("SF", "94103"), Seq("java", "kotlin")),
      Row(9, "quinn", "Quinn Anderson", 26, 65000.25, Date.valueOf("2024-05-08"), Timestamp.valueOf("2024-05-08 10:00:00"), true, Row("Boston", "02101"), Seq("go")),
      Row(10, "rachel", "Rachel Thomas", 33, 92000.00, Date.valueOf("2024-07-19"), Timestamp.valueOf("2024-07-19 14:15:00"), false, Row("Chicago", "60602"), Seq("python", "r"))
    )

    // Split 3: Special cases - nulls, edge values
    val split3Data = Seq(
      Row(11, "sam", "Sam Jackson", 45, 150000.00, Date.valueOf("2024-09-01"), Timestamp.valueOf("2024-09-01 09:00:00"), true, Row("NYC", "10004"), Seq("all", "languages")),
      Row(12, "tina", "Tina White", 22, 55000.00, Date.valueOf("2024-10-15"), Timestamp.valueOf("2024-10-15 15:45:00"), false, Row("LA", "90003"), Seq.empty[String]),
      Row(13, "uma", "Uma Harris", 38, 105000.50, Date.valueOf("2024-11-30"), Timestamp.valueOf("2024-11-30 11:00:00"), true, Row("SF", "94104"), Seq("java")),
      Row(14, "victor", "Victor Martin", 31, 82000.00, Date.valueOf("2024-12-20"), Timestamp.valueOf("2024-12-20 16:30:00"), true, Row("Boston", "02102"), Seq("python", "java", "scala")),
      Row(15, "wendy", "Wendy Clark", 24, 60000.00, Date.valueOf("2024-12-31"), Timestamp.valueOf("2024-12-31 23:59:59"), false, Row("Chicago", "60603"), Seq("javascript", "typescript"))
    )

    val addressSchema = StructType(Seq(
      StructField("city", StringType, nullable = false),
      StructField("zip", StringType, nullable = false)
    ))

    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("username", StringType, nullable = false),     // String field (raw tokenizer)
      StructField("fullname", StringType, nullable = false),     // Text field
      StructField("age", IntegerType, nullable = false),
      StructField("salary", DoubleType, nullable = false),
      StructField("hire_date", DateType, nullable = false),
      StructField("last_login", TimestampType, nullable = false),
      StructField("is_active", BooleanType, nullable = false),
      StructField("address", addressSchema, nullable = false),   // Struct field
      StructField("skills", ArrayType(StringType), nullable = false)  // Array field
    ))

    // Write splits separately to ensure multiple split files
    // Only set field options on first write; appends inherit from existing table
    Seq(split1Data, split2Data, split3Data).zipWithIndex.foreach { case (data, idx) =>
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      val writer = df.write
        .format(provider)
        .mode(if (idx == 0) "overwrite" else "append")

      // Only set type options on first write (overwrite)
      val configuredWriter = if (idx == 0) {
        writer
          .option("spark.indextables.indexing.typemap.username", "string")
          .option("spark.indextables.indexing.typemap.fullname", "text")
          .option("spark.indextables.indexing.fastfields", "salary,age,is_active")
      } else {
        writer
      }
      configuredWriter.save(path)
    }

    println(s"✅ Created test table with 3 splits at $path")
  }

  // ===========================================
  // STRING FIELD TESTS (exact matching)
  // ===========================================

  test("prescan should produce same results for string equality filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("username") === "alice"),
        "String equality (username = 'alice')"
      )
    }
  }

  test("prescan should produce same results for string IN filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("username").isin("alice", "bob", "nate", "olivia")),
        "String IN filter"
      )
    }
  }

  // ===========================================
  // NUMERIC FIELD TESTS (range queries)
  // ===========================================

  test("prescan should produce same results for integer equality filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("age") === 30),
        "Integer equality (age = 30)"
      )
    }
  }

  test("prescan should produce same results for integer range filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("age") >= 25 && col("age") <= 35),
        "Integer range (25 <= age <= 35)"
      )
    }
  }

  test("prescan should produce same results for double range filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("salary") > 80000.0),
        "Double range (salary > 80000)"
      )
    }
  }

  test("prescan should produce same results for integer less-than filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("age") < 28),
        "Integer less-than (age < 28)"
      )
    }
  }

  // ===========================================
  // DATE/TIMESTAMP FIELD TESTS
  // ===========================================

  test("prescan should produce same results for date equality filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("hire_date") === Date.valueOf("2024-01-10")),
        "Date equality"
      )
    }
  }

  test("prescan should produce same results for date range filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(
          col("hire_date") >= Date.valueOf("2024-01-01") &&
          col("hire_date") <= Date.valueOf("2024-06-30")
        ),
        "Date range (2024 H1)"
      )
    }
  }

  test("prescan should produce same results for timestamp range filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(
          col("last_login") >= Timestamp.valueOf("2024-01-01 00:00:00") &&
          col("last_login") < Timestamp.valueOf("2024-07-01 00:00:00")
        ),
        "Timestamp range"
      )
    }
  }

  // ===========================================
  // BOOLEAN FIELD TESTS
  // ===========================================

  test("prescan should produce same results for boolean filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("is_active") === true),
        "Boolean filter (is_active = true)"
      )
    }
  }

  // ===========================================
  // STRUCT (JSON) FIELD TESTS
  // ===========================================

  test("prescan should produce same results for struct field filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("address.city") === "NYC"),
        "Struct field filter (address.city = 'NYC')"
      )
    }
  }

  test("prescan should produce same results for nested struct range filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("address.zip") >= "10000" && col("address.zip") < "20000"),
        "Nested struct range filter"
      )
    }
  }

  // ===========================================
  // COMPOUND FILTER TESTS (AND/OR/NOT)
  // ===========================================

  test("prescan should produce same results for AND compound filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("age") > 25 && col("is_active") === true),
        "AND compound filter (age > 25 AND is_active)"
      )
    }
  }

  test("prescan should produce same results for OR compound filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("username") === "alice" || col("username") === "nate"),
        "OR compound filter"
      )
    }
  }

  test("prescan should produce same results for NOT filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(!(col("is_active") === true)),
        "NOT filter (!is_active)"
      )
    }
  }

  test("prescan should produce same results for complex compound filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(
          (col("age") >= 25 && col("age") <= 35) &&
          (col("address.city") === "NYC" || col("address.city") === "SF") &&
          col("salary") > 70000.0
        ),
        "Complex compound filter"
      )
    }
  }

  // ===========================================
  // AGGREGATE QUERY TESTS
  // ===========================================

  test("prescan should produce same results for COUNT aggregate with filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("age") > 30).agg(count("*").as("cnt")),
        "COUNT with filter"
      )
    }
  }

  test("prescan should produce same results for SUM aggregate with filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("is_active") === true).agg(sum("salary").as("total")),
        "SUM with filter"
      )
    }
  }

  test("prescan should produce same results for AVG aggregate with filter") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("address.city") === "NYC").agg(avg("age").as("avg_age")),
        "AVG with struct filter"
      )
    }
  }

  test("prescan should produce same results for GROUP BY aggregate") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("salary") > 70000.0)
          .groupBy("is_active")
          .agg(count("*").as("cnt"), avg("salary").as("avg_salary")),
        "GROUP BY with filter"
      )
    }
  }

  // ===========================================
  // EDGE CASE TESTS
  // ===========================================

  test("prescan should produce same results for filter matching no data") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("username") === "nonexistent_user"),
        "Filter matching no data"
      )
    }
  }

  test("prescan should produce same results for filter matching all data") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      verifyPrescanEquivalence(
        path,
        df => df.filter(col("age") >= 0),
        "Filter matching all data (age >= 0)"
      )
    }
  }

  test("prescan should produce same results for filter matching single split") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      // Only split 1 has users with id <= 5
      verifyPrescanEquivalence(
        path,
        df => df.filter(col("id") <= 5),
        "Filter matching single split (id <= 5)"
      )
    }
  }

  // ===========================================
  // SQL COMMAND INTEGRATION TESTS
  // ===========================================

  test("ENABLE/DISABLE PRESCAN FILTERING commands should work") {
    withTempPath { path =>
      createComprehensiveTestTable(path)

      // Enable prescan via SQL
      spark.sql("ENABLE INDEXTABLES PRESCAN FILTERING")

      val df1 = spark.read.format(provider).load(path)
      val result1 = df1.filter(col("age") > 30).count()

      // Disable prescan via SQL
      spark.sql("DISABLE INDEXTABLES PRESCAN FILTERING")

      val df2 = spark.read.format(provider).load(path)
      val result2 = df2.filter(col("age") > 30).count()

      assert(result1 == result2, s"Results should match: enabled=$result1, disabled=$result2")
      println(s"✅ SQL commands work: count=$result1")

      // Clear state
      PrescanSessionState.clear(spark)
    }
  }
}
