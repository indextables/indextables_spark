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

package io.indextables.spark.json

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import io.indextables.spark.TestBase

/**
 * Test suite for aggregate operations on JSON/Struct fields with filter pushdown. Validates that SUM, AVG, MIN, MAX,
 * COUNT work correctly on nested field values and that WHERE clauses on JSON fields are properly pushed down with
 * aggregates.
 */
class JsonFieldAggregateTest extends TestBase {

  test("should count rows with JSON fields") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false),
          StructField("city", StringType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30, "NYC")),
        Row(2, Row("Bob", 25, "SF")),
        Row(3, Row("Charlie", 35, "NYC")),
        Row(4, Row("David", 28, "SF")),
        Row(5, Row("Eve", 32, "NYC"))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write with fast fields enabled for age (required for aggregations)
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "user.age")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows with fast fields on user.age")

      // Read back
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: Count all rows (transaction log optimization)
      val totalCount = result.count()
      totalCount shouldBe 5
      println(s"✅ COUNT(*): $totalCount rows")
    }
  }

  test("should perform SUM aggregate on nested JSON field") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false),
          StructField("salary", IntegerType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30, 80000)),
        Row(2, Row("Bob", 25, 60000)),
        Row(3, Row("Charlie", 35, 90000)),
        Row(4, Row("David", 28, 70000)),
        Row(5, Row("Eve", 32, 85000))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write with fast fields enabled for salary
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "user.salary")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows with fast fields on user.salary")

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: SUM on nested field
      val totalSalary = result.agg(sum(col("user.salary"))).collect()(0).getLong(0)
      totalSalary shouldBe 385000L // 80000 + 60000 + 90000 + 70000 + 85000
      println(s"✅ SUM(user.salary): $totalSalary")
    }
  }

  test("should perform AVG aggregate on nested JSON field") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30)),
        Row(2, Row("Bob", 25)),
        Row(3, Row("Charlie", 35)),
        Row(4, Row("David", 28)),
        Row(5, Row("Eve", 32))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "user.age")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: AVG on nested field
      val avgAge = result.agg(avg(col("user.age"))).collect()(0).getDouble(0)
      avgAge shouldBe 30.0 // (30 + 25 + 35 + 28 + 32) / 5
      println(s"✅ AVG(user.age): $avgAge")
    }
  }

  test("should perform MIN/MAX aggregates on nested JSON field") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30)),
        Row(2, Row("Bob", 25)),
        Row(3, Row("Charlie", 35)),
        Row(4, Row("David", 28)),
        Row(5, Row("Eve", 32))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "user.age")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: MIN/MAX on nested field
      val minMax = result.agg(min(col("user.age")), max(col("user.age"))).collect()(0)
      val minAge = minMax.getInt(0)
      val maxAge = minMax.getInt(1)

      minAge shouldBe 25
      maxAge shouldBe 35
      println(s"✅ MIN(user.age): $minAge, MAX(user.age): $maxAge")
    }
  }

  test("should perform multiple aggregates on nested JSON fields") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false),
          StructField("salary", IntegerType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30, 80000)),
        Row(2, Row("Bob", 25, 60000)),
        Row(3, Row("Charlie", 35, 90000)),
        Row(4, Row("David", 28, 70000)),
        Row(5, Row("Eve", 32, 85000))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "user.age,user.salary")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: Multiple aggregates on nested fields
      val stats = result
        .agg(
          count("*").as("total"),
          sum(col("user.salary")).as("sum_salary"),
          avg(col("user.age")).as("avg_age"),
          min(col("user.age")).as("min_age"),
          max(col("user.age")).as("max_age")
        )
        .collect()(0)

      stats.getLong(0) shouldBe 5L
      stats.getLong(1) shouldBe 385000L
      stats.getDouble(2) shouldBe 30.0
      stats.getInt(3) shouldBe 25
      stats.getInt(4) shouldBe 35

      println(
        s"✅ Multiple aggregates: count=${stats.getLong(0)}, sum=${stats.getLong(1)}, " +
          s"avg=${stats.getDouble(2)}, min=${stats.getInt(3)}, max=${stats.getInt(4)}"
      )
    }
  }

  test("should perform aggregates with equality filter on nested JSON field") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false),
          StructField("city", StringType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30, "NYC")),
        Row(2, Row("Bob", 25, "SF")),
        Row(3, Row("Charlie", 35, "NYC")),
        Row(4, Row("David", 28, "SF")),
        Row(5, Row("Eve", 32, "NYC"))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "user.age")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: Aggregate with WHERE clause on nested string field
      val nycStats = result
        .filter(col("user.city") === "NYC")
        .agg(
          count("*").as("total"),
          avg(col("user.age")).as("avg_age")
        )
        .collect()(0)

      nycStats.getLong(0) shouldBe 3L                          // Alice, Charlie, Eve
      math.abs(nycStats.getDouble(1) - 32.33) should be < 0.01 // (30 + 35 + 32) / 3 = 32.33

      println(s"✅ Aggregate with WHERE user.city='NYC': count=${nycStats.getLong(0)}, avg_age=${nycStats.getDouble(1)}")
    }
  }

  test("should perform aggregates with range filter on nested JSON field") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false),
          StructField("salary", IntegerType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30, 80000)),
        Row(2, Row("Bob", 25, 60000)),
        Row(3, Row("Charlie", 35, 90000)),
        Row(4, Row("David", 28, 70000)),
        Row(5, Row("Eve", 32, 85000))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "user.age,user.salary")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: Aggregate with WHERE clause on range (age > 30)
      val seniorStats = result
        .filter(col("user.age") > 30)
        .agg(
          count("*").as("total"),
          sum(col("user.salary")).as("sum_salary"),
          avg(col("user.salary")).as("avg_salary")
        )
        .collect()(0)

      seniorStats.getLong(0) shouldBe 2L        // Charlie (35), Eve (32)
      seniorStats.getLong(1) shouldBe 175000L   // 90000 + 85000
      seniorStats.getDouble(2) shouldBe 87500.0 // 175000 / 2

      println(
        s"✅ Aggregate with WHERE user.age>30: count=${seniorStats.getLong(0)}, " +
          s"sum_salary=${seniorStats.getLong(1)}, avg_salary=${seniorStats.getDouble(2)}"
      )
    }
  }

  test("should perform aggregates with combined filters on nested JSON fields") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false),
          StructField("city", StringType, nullable = false),
          StructField("salary", IntegerType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30, "NYC", 80000)),
        Row(2, Row("Bob", 25, "SF", 60000)),
        Row(3, Row("Charlie", 35, "NYC", 90000)),
        Row(4, Row("David", 28, "SF", 70000)),
        Row(5, Row("Eve", 32, "NYC", 85000)),
        Row(6, Row("Frank", 29, "NYC", 75000))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "user.age,user.salary")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: Aggregate with combined WHERE clause (city='NYC' AND age > 30)
      val nycSeniorStats = result
        .filter(col("user.city") === "NYC" && col("user.age") > 30)
        .agg(
          count("*").as("total"),
          sum(col("user.salary")).as("sum_salary"),
          avg(col("user.age")).as("avg_age"),
          min(col("user.salary")).as("min_salary"),
          max(col("user.salary")).as("max_salary")
        )
        .collect()(0)

      nycSeniorStats.getLong(0) shouldBe 2L      // Charlie (35), Eve (32)
      nycSeniorStats.getLong(1) shouldBe 175000L // 90000 + 85000
      nycSeniorStats.getDouble(2) shouldBe 33.5  // (35 + 32) / 2
      nycSeniorStats.getInt(3) shouldBe 85000    // Eve's salary
      nycSeniorStats.getInt(4) shouldBe 90000    // Charlie's salary

      println(
        s"✅ Aggregate with WHERE user.city='NYC' AND user.age>30: " +
          s"count=${nycSeniorStats.getLong(0)}, sum=${nycSeniorStats.getLong(1)}, " +
          s"avg_age=${nycSeniorStats.getDouble(2)}, min_sal=${nycSeniorStats.getInt(3)}, " +
          s"max_sal=${nycSeniorStats.getInt(4)}"
      )
    }
  }

  test("should perform aggregates with complex boolean filters on nested JSON fields") {
    withTempPath { path =>
      val userSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false),
          StructField("city", StringType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user", userSchema, nullable = false)
        )
      )

      val data = Seq(
        Row(1, Row("Alice", 30, "NYC")),
        Row(2, Row("Bob", 25, "SF")),
        Row(3, Row("Charlie", 35, "NYC")),
        Row(4, Row("David", 28, "SF")),
        Row(5, Row("Eve", 32, "NYC")),
        Row(6, Row("Frank", 40, "LA"))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "user.age")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test: Aggregate with complex OR condition ((city='NYC' AND age>30) OR city='LA')
      val complexStats = result
        .filter((col("user.city") === "NYC" && col("user.age") > 30) || col("user.city") === "LA")
        .agg(
          count("*").as("total"),
          avg(col("user.age")).as("avg_age")
        )
        .collect()(0)

      complexStats.getLong(0) shouldBe 3L                          // Charlie (35, NYC), Eve (32, NYC), Frank (40, LA)
      math.abs(complexStats.getDouble(1) - 35.67) should be < 0.01 // (35 + 32 + 40) / 3 = 35.67

      println(
        s"✅ Aggregate with complex WHERE ((city='NYC' AND age>30) OR city='LA'): " +
          s"count=${complexStats.getLong(0)}, avg_age=${complexStats.getDouble(1)}"
      )
    }
  }

  test("should perform aggregates on large dataset with 5000 rows with non-JSON fields") {
    withTempPath { path =>
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("timestamp", LongType, nullable = false),
          StructField("value", IntegerType, nullable = false),
          StructField("name", StringType, nullable = false)
        )
      )

      // Generate 5000 rows
      val data = (1 to 5000).map(i => Row(i, System.currentTimeMillis() + i, i * 10, s"name_$i"))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      println(s"📊 Writing ${df.count()} rows with non-JSON fields (timestamp, value)")

      // Write with fast fields enabled for timestamp and value
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "timestamp,value")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows with fast fields on timestamp, value")

      // Read back
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test 1: Count all rows - should be 5000, NOT 250 (default search limit)
      val totalCount = result.count()
      totalCount shouldBe 5000L
      println(s"✅ COUNT(*): $totalCount rows (validates pushdown is working)")

      // Test 2: Multiple aggregates on regular fields (reproduces the user's issue)
      val stats = result
        .agg(
          count("*").as("total"),
          min(col("timestamp")).as("min_timestamp"),
          max(col("timestamp")).as("max_timestamp"),
          sum(col("value")).as("sum_value"),
          avg(col("value")).as("avg_value")
        )
        .collect()(0)

      stats.getLong(0) shouldBe 5000L
      println(
        s"✅ Multiple aggregates: count=${stats.getLong(0)}, " +
          s"min_timestamp=${stats.getLong(1)}, max_timestamp=${stats.getLong(2)}, " +
          s"sum_value=${stats.getLong(3)}, avg_value=${stats.getDouble(4)}"
      )

      // Verify the actual values
      val expectedSumValue = (1 to 5000).map(_ * 10).sum.toLong
      val expectedAvgValue = (1 to 5000).map(_ * 10).sum.toDouble / 5000

      stats.getLong(3) shouldBe expectedSumValue
      stats.getDouble(4) shouldBe expectedAvgValue

      // Test 3: Aggregates with filter on regular field
      val filteredStats = result
        .filter(col("value") > 40000)
        .agg(
          count("*").as("total"),
          min(col("value")).as("min_value"),
          max(col("value")).as("max_value")
        )
        .collect()(0)

      filteredStats.getLong(0) shouldBe 1000L // 4001 to 5000
      filteredStats.getInt(1) shouldBe 40010  // 4001 * 10
      filteredStats.getInt(2) shouldBe 50000  // 5000 * 10

      println(
        s"✅ Filtered aggregates (value > 40000): count=${filteredStats.getLong(0)}, " +
          s"min=${filteredStats.getInt(1)}, max=${filteredStats.getInt(2)}"
      )
    }
  }

  test("should perform aggregates on large dataset with 5000 rows with JSON struct fields") {
    withTempPath { path =>
      val metadataSchema = StructType(
        Seq(
          StructField("field1", IntegerType, nullable = false),
          StructField("field2", IntegerType, nullable = false),
          StructField("name", StringType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("value", IntegerType, nullable = false),
          StructField("metadata", metadataSchema, nullable = false)
        )
      )

      // Generate 5000 rows
      val data = (1 to 5000).map(i => Row(i, i * 10, Row(i, i * 2, s"name_$i")))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      println(s"📊 Writing ${df.count()} rows with JSON struct field (metadata)")

      // Write with fast fields enabled for metadata.field1 and metadata.field2
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "metadata.field1,metadata.field2,value")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows with fast fields on metadata.field1, metadata.field2, value")

      // Read back
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test 1: Count all rows - should be 5000, NOT 250 (default search limit)
      val totalCount = result.count()
      totalCount shouldBe 5000L
      println(s"✅ COUNT(*): $totalCount rows (validates pushdown is working)")

      // Test 2: Multiple aggregates on nested JSON fields
      val stats = result
        .agg(
          count("*").as("total"),
          min(col("metadata.field1")).as("min_field1"),
          max(col("metadata.field1")).as("max_field1"),
          sum(col("metadata.field2")).as("sum_field2"),
          avg(col("value")).as("avg_value")
        )
        .collect()(0)

      stats.getLong(0) shouldBe 5000L
      stats.getInt(1) shouldBe 1                                              // min(field1) = 1
      stats.getInt(2) shouldBe 5000                                           // max(field1) = 5000
      stats.getLong(3) shouldBe (1 to 5000).map(_ * 2).sum.toLong             // sum(field2)
      stats.getDouble(4) shouldBe (1 to 5000).map(_ * 10).sum.toDouble / 5000 // avg(value)

      println(
        s"✅ Multiple aggregates: count=${stats.getLong(0)}, " +
          s"min_field1=${stats.getInt(1)}, max_field1=${stats.getInt(2)}, " +
          s"sum_field2=${stats.getLong(3)}, avg_value=${stats.getDouble(4)}"
      )

      // Test 3: Aggregates with filter on nested JSON field
      val filteredStats = result
        .filter(col("metadata.field1") > 4000)
        .agg(
          count("*").as("total"),
          min(col("metadata.field1")).as("min_field1"),
          max(col("metadata.field1")).as("max_field1")
        )
        .collect()(0)

      filteredStats.getLong(0) shouldBe 1000L // 4001 to 5000
      filteredStats.getInt(1) shouldBe 4001
      filteredStats.getInt(2) shouldBe 5000

      println(
        s"✅ Filtered aggregates (metadata.field1 > 4000): count=${filteredStats.getLong(0)}, " +
          s"min=${filteredStats.getInt(1)}, max=${filteredStats.getInt(2)}"
      )

      // Test 4: Aggregates with string filter on nested JSON field
      val stringFilterStats = result
        .filter(col("metadata.name") === "name_2500")
        .agg(
          count("*").as("total"),
          sum(col("metadata.field2")).as("sum_field2")
        )
        .collect()(0)

      stringFilterStats.getLong(0) shouldBe 1L
      stringFilterStats.getLong(1) shouldBe 5000L // 2500 * 2

      println(
        s"✅ String filtered aggregates (metadata.name = 'name_2500'): count=${stringFilterStats.getLong(0)}, " +
          s"sum_field2=${stringFilterStats.getLong(1)}"
      )
    }
  }

  test("should perform GROUP BY with SUM and AVG on large dataset with 5000 rows and JSON aggregations") {
    withTempPath { path =>
      val metadataSchema = StructType(
        Seq(
          StructField("amount", IntegerType, nullable = false),
          StructField("quantity", IntegerType, nullable = false),
          StructField("description", StringType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("category", StringType, nullable = false), // Top-level for GROUP BY
          StructField("value", IntegerType, nullable = false),
          StructField("metadata", metadataSchema, nullable = false)
        )
      )

      // Generate 5000 rows with 10 categories (500 rows per category)
      // For category_0: ids 0, 10, 20, ..., 4990 (500 values)
      // For category_1: ids 1, 11, 21, ..., 4991 (500 values)
      // etc.
      val data = (0 until 5000).map { i =>
        val categoryNum = i % 10
        Row(
          i,
          s"category_$categoryNum", // Top-level category for GROUP BY
          i * 10,
          Row(
            i * 2, // amount
            i + 1, // quantity
            s"desc_$i"
          )
        )
      }

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      println(s"📊 Writing 5000 rows with GROUP BY test data (10 categories, 500 rows each)")

      // Write with fast fields enabled for category (for GROUP BY) and metadata fields (for aggregations)
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "category,metadata.amount,metadata.quantity,value")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote 5000 rows with fast fields on category, metadata.amount, metadata.quantity, value")

      // Read back
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test 1: GROUP BY category with COUNT, SUM, and AVG on JSON fields
      val groupedStats = result
        .groupBy(col("category"))
        .agg(
          count("*").as("total"),
          sum(col("metadata.amount")).as("sum_amount"),
          avg(col("metadata.amount")).as("avg_amount"),
          sum(col("metadata.quantity")).as("sum_quantity"),
          avg(col("metadata.quantity")).as("avg_quantity"),
          avg(col("value")).as("avg_value")
        )
        .orderBy(col("category"))
        .collect()

      // Verify we have 10 groups
      groupedStats.length shouldBe 10

      println(s"✅ GROUP BY returned ${groupedStats.length} groups")

      // Verify each group
      groupedStats.zipWithIndex.foreach {
        case (row, idx) =>
          val category    = row.getString(0)
          val count       = row.getLong(1)
          val sumAmount   = row.getLong(2)
          val avgAmount   = row.getDouble(3)
          val sumQuantity = row.getLong(4)
          val avgQuantity = row.getDouble(5)
          val avgValue    = row.getDouble(6)

          // Each category should have 500 rows
          count shouldBe 500L

          // Calculate expected values for category_idx
          // IDs: idx, idx+10, idx+20, ..., idx+4990
          // metadata.amount: idx*2, (idx+10)*2, (idx+20)*2, ..., (idx+4990)*2
          val expectedSumAmount = (0 until 500).map(j => (idx + j * 10) * 2).sum.toLong
          val expectedAvgAmount = expectedSumAmount.toDouble / 500

          // metadata.quantity: idx+1, (idx+10)+1, (idx+20)+1, ..., (idx+4990)+1
          val expectedSumQuantity = (0 until 500).map(j => (idx + j * 10) + 1).sum.toLong
          val expectedAvgQuantity = expectedSumQuantity.toDouble / 500

          // value: idx*10, (idx+10)*10, (idx+20)*10, ..., (idx+4990)*10
          val expectedAvgValue = (0 until 500).map(j => (idx + j * 10) * 10).sum.toDouble / 500

          sumAmount shouldBe expectedSumAmount
          Math.abs(avgAmount - expectedAvgAmount) should be < 0.01
          sumQuantity shouldBe expectedSumQuantity
          Math.abs(avgQuantity - expectedAvgQuantity) should be < 0.01
          Math.abs(avgValue - expectedAvgValue) should be < 0.01

          println(
            s"✅ Category '$category': count=$count, sum_amount=$sumAmount, avg_amount=$avgAmount, " +
              s"sum_quantity=$sumQuantity, avg_quantity=$avgQuantity, avg_value=$avgValue"
          )
      }

      // Test 2: GROUP BY with filter on JSON field
      val filteredGroupedStats = result
        .filter(col("metadata.amount") > 5000)
        .groupBy(col("category"))
        .agg(
          count("*").as("total"),
          sum(col("metadata.amount")).as("sum_amount"),
          avg(col("value")).as("avg_value")
        )
        .collect()

      // All 10 categories should have some rows with amount > 5000
      filteredGroupedStats.length should be > 0

      println(s"✅ Filtered GROUP BY (metadata.amount > 5000) returned ${filteredGroupedStats.length} groups")

      filteredGroupedStats.foreach { row =>
        val category  = row.getString(0)
        val count     = row.getLong(1)
        val sumAmount = row.getLong(2)
        val avgValue  = row.getDouble(3)

        println(s"✅ Filtered category '$category': count=$count, sum_amount=$sumAmount, avg_value=$avgValue")
      }
    }
  }

  test("should perform aggregates on very large split with 50000 rows to verify COUNT(*) correctness") {
    withTempPath { path =>
      val metadataSchema = StructType(
        Seq(
          StructField("score", IntegerType, nullable = false),
          StructField("rating", DoubleType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("value", IntegerType, nullable = false),
          StructField("metadata", metadataSchema, nullable = false)
        )
      )

      // Generate 50,000 rows - large enough to verify we're not hitting any limits
      val data = (1 to 50000).map(i => Row(i, i * 10, Row(i % 1000, i / 100.0)))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 4), schema) // 4 partitions

      println(s"📊 Writing 50,000 rows to test large split aggregation")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "metadata.score,metadata.rating,value")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote 50,000 rows with fast fields")

      // Read back
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Test 1: COUNT(*) on full dataset - should be exactly 50000, not limited
      val totalCount = result.count()
      totalCount shouldBe 50000L
      println(s"✅ COUNT(*): $totalCount rows (validates no limits)")

      // Test 2: Multiple aggregates on large dataset
      val stats = result
        .agg(
          count("*").as("total"),
          sum(col("metadata.score")).as("sum_score"),
          avg(col("metadata.rating")).as("avg_rating"),
          min(col("value")).as("min_value"),
          max(col("value")).as("max_value")
        )
        .collect()(0)

      stats.getLong(0) shouldBe 50000L

      // Expected sum of scores: sum of (i % 1000) for i=1 to 50000
      // = 50 * sum(0 to 999) = 50 * (999 * 1000 / 2) = 50 * 499500 = 24975000
      val expectedSumScore = 24975000L
      stats.getLong(1) shouldBe expectedSumScore

      // Expected avg rating: sum of (i / 100.0) for i=1 to 50000 divided by 50000
      val expectedAvgRating = (1 to 50000).map(_ / 100.0).sum / 50000
      Math.abs(stats.getDouble(2) - expectedAvgRating) should be < 0.01

      println(
        s"✅ Aggregates on 50K rows: count=${stats.getLong(0)}, sum_score=${stats.getLong(1)}, " +
          s"avg_rating=${stats.getDouble(2)}, min_value=${stats.getInt(3)}, max_value=${stats.getInt(4)}"
      )

      // Test 3: Filtered aggregation on large dataset
      val filteredStats = result
        .filter(col("metadata.score") > 500)
        .agg(
          count("*").as("total"),
          sum(col("value")).as("sum_value"),
          avg(col("metadata.rating")).as("avg_rating")
        )
        .collect()(0)

      // metadata.score > 500 means score in 501..999 (499 values per 1000 block)
      // With 50 blocks: 50 * 499 = 24950 rows
      val expectedFilteredCount = 24950L
      filteredStats.getLong(0) shouldBe expectedFilteredCount

      println(
        s"✅ Filtered aggregation (metadata.score > 500): count=${filteredStats.getLong(0)}, " +
          s"sum_value=${filteredStats.getLong(1)}, avg_rating=${filteredStats.getDouble(2)}"
      )
      println(s"✅ All large split aggregations passed - no limits encountered!")
    }
  }
}
