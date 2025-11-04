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

import io.indextables.spark.TestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Test suite for aggregate operations on JSON/Struct fields with filter pushdown.
 * Validates that SUM, AVG, MIN, MAX, COUNT work correctly on nested field values
 * and that WHERE clauses on JSON fields are properly pushed down with aggregates.
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

      println(s"✅ Multiple aggregates: count=${stats.getLong(0)}, sum=${stats.getLong(1)}, " +
             s"avg=${stats.getDouble(2)}, min=${stats.getInt(3)}, max=${stats.getInt(4)}")
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

      nycStats.getLong(0) shouldBe 3L // Alice, Charlie, Eve
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

      seniorStats.getLong(0) shouldBe 2L // Charlie (35), Eve (32)
      seniorStats.getLong(1) shouldBe 175000L // 90000 + 85000
      seniorStats.getDouble(2) shouldBe 87500.0 // 175000 / 2

      println(s"✅ Aggregate with WHERE user.age>30: count=${seniorStats.getLong(0)}, " +
             s"sum_salary=${seniorStats.getLong(1)}, avg_salary=${seniorStats.getDouble(2)}")
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

      nycSeniorStats.getLong(0) shouldBe 2L // Charlie (35), Eve (32)
      nycSeniorStats.getLong(1) shouldBe 175000L // 90000 + 85000
      nycSeniorStats.getDouble(2) shouldBe 33.5 // (35 + 32) / 2
      nycSeniorStats.getInt(3) shouldBe 85000 // Eve's salary
      nycSeniorStats.getInt(4) shouldBe 90000 // Charlie's salary

      println(s"✅ Aggregate with WHERE user.city='NYC' AND user.age>30: " +
             s"count=${nycSeniorStats.getLong(0)}, sum=${nycSeniorStats.getLong(1)}, " +
             s"avg_age=${nycSeniorStats.getDouble(2)}, min_sal=${nycSeniorStats.getInt(3)}, " +
             s"max_sal=${nycSeniorStats.getInt(4)}")
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

      complexStats.getLong(0) shouldBe 3L // Charlie (35, NYC), Eve (32, NYC), Frank (40, LA)
      math.abs(complexStats.getDouble(1) - 35.67) should be < 0.01 // (35 + 32 + 40) / 3 = 35.67

      println(s"✅ Aggregate with complex WHERE ((city='NYC' AND age>30) OR city='LA'): " +
             s"count=${complexStats.getLong(0)}, avg_age=${complexStats.getDouble(1)}")
    }
  }
}
