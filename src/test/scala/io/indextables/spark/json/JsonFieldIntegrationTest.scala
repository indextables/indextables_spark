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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import io.indextables.spark.TestBase
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * Integration tests for JSON field support (Struct, Array, JSON strings).
 *
 * Tests end-to-end write/read functionality with automatic JSON field detection
 * and conversion.
 */
class JsonFieldIntegrationTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  test("should write and read simple Struct field") {
    withTempPath { path =>
      // Define schema with nested Struct
      val userSchema = StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false)
      ))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("user", userSchema, nullable = false)
      ))

      // Create test data with nested structure
      val data = Seq(
        Row(1, Row("Alice", 30)),
        Row(2, Row("Bob", 25)),
        Row(3, Row("Charlie", 35))
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )

      // Write using V2 API (JSON fields should be automatically detected)
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows with Struct field to $path")

      // Read back
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      println(s"✅ Read back ${result.count()} rows")
      result.show(false)

      // Verify count
      result.count() shouldBe 3

      // Verify schema
      result.schema.fields.map(_.name) should contain theSameElementsAs Array("id", "user")
      val userFieldType = result.schema.fields.find(_.name == "user").get.dataType
      userFieldType shouldBe a[StructType]

      // Verify data integrity
      val rows = result.collect()
      rows.length shouldBe 3

      // Find rows by ID (order may not be preserved)
      val row1 = rows.find(_.getInt(0) == 1).get
      val row2 = rows.find(_.getInt(0) == 2).get

      // Check first row
      val user1 = row1.getStruct(1)
      user1.getString(0) shouldBe "Alice"
      user1.getInt(1) shouldBe 30

      // Check second row
      val user2 = row2.getStruct(1)
      user2.getString(0) shouldBe "Bob"
      user2.getInt(1) shouldBe 25

      println("✅ All data integrity checks passed!")
    }
  }

  test("should write and read Array field") {
    withTempPath { path =>
      // Define schema with Array
      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("scores", ArrayType(IntegerType, containsNull = false), nullable = false)
      ))

      // Create test data with arrays
      val data = Seq(
        Row(1, Array(90, 85, 92)),
        Row(2, Array(78, 88, 95)),
        Row(3, Array(88, 90, 87))
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )

      // Write using V2 API
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows with Array field to $path")

      // Read back
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      println(s"✅ Read back ${result.count()} rows")
      result.show(false)

      // Verify count
      result.count() shouldBe 3

      // Verify schema
      result.schema.fields.map(_.name) should contain theSameElementsAs Array("id", "scores")
      val scoresFieldType = result.schema.fields.find(_.name == "scores").get.dataType
      scoresFieldType shouldBe a[ArrayType]

      // Verify data integrity
      val rows = result.collect()
      rows.length shouldBe 3

      // Find rows by ID (order may not be preserved)
      val row1 = rows.find(_.getInt(0) == 1).get
      val row2 = rows.find(_.getInt(0) == 2).get

      // Check first row
      val scores1 = row1.getSeq[Int](1)
      scores1 should contain theSameElementsInOrderAs Seq(90, 85, 92)

      // Check second row
      val scores2 = row2.getSeq[Int](1)
      scores2 should contain theSameElementsInOrderAs Seq(78, 88, 95)

      println("✅ All array data integrity checks passed!")
    }
  }

  test("should write and read nested Struct with multiple fields") {
    withTempPath { path =>
      // Define complex nested schema
      val addressSchema = StructType(Seq(
        StructField("street", StringType, nullable = false),
        StructField("city", StringType, nullable = false),
        StructField("zipcode", StringType, nullable = false)
      ))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("address", addressSchema, nullable = false)
      ))

      // Create test data
      val data = Seq(
        Row(1, "Alice", Row("123 Main St", "Seattle", "98101")),
        Row(2, "Bob", Row("456 Oak Ave", "Portland", "97201"))
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )

      // Write
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows with nested Struct to $path")

      // Read back
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      println(s"✅ Read back ${result.count()} rows")
      result.show(false)

      // Verify data
      result.count() shouldBe 2
      val rows = result.collect()

      // Find row by ID (order may not be preserved)
      val row1 = rows.find(_.getInt(0) == 1).get

      // Verify first row
      row1.getString(1) shouldBe "Alice"
      val address1 = row1.getStruct(2)
      address1.getString(0) shouldBe "123 Main St"
      address1.getString(1) shouldBe "Seattle"
      address1.getString(2) shouldBe "98101"

      println("✅ Nested struct data integrity verified!")
    }
  }

  test("should handle null values in Struct fields") {
    withTempPath { path =>
      // Define schema with nullable Struct
      val userSchema = StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true)
      ))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("user", userSchema, nullable = true)
      ))

      // Create test data with some nulls
      val data = Seq(
        Row(1, Row("Alice", 30)),
        Row(2, null), // Null struct
        Row(3, Row(null, 25)) // Null field within struct
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )

      // Write
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows with nullable Struct fields")

      // Read back
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 3
      val rows = result.collect()

      // Find rows by ID (order may not be preserved)
      val row1 = rows.find(_.getInt(0) == 1).get
      val row2 = rows.find(_.getInt(0) == 2).get
      val row3 = rows.find(_.getInt(0) == 3).get

      // Verify row 1 has normal data
      row1.isNullAt(1) shouldBe false
      val user1 = row1.getStruct(1)
      user1.getString(0) shouldBe "Alice"
      user1.getInt(1) shouldBe 30

      // Verify row 2 has null struct
      row2.isNullAt(1) shouldBe true

      // Verify row 3 has null field within struct
      row3.isNullAt(1) shouldBe false
      val user3 = row3.getStruct(1)
      user3.isNullAt(0) shouldBe true
      user3.getInt(1) shouldBe 25

      println("✅ Null handling verified!")
    }
  }

  // ========== Filter Pushdown Integration Tests ==========

  test("should push down equality filter on nested struct field") {
    withTempPath { path =>
      // Define schema with nested Struct
      val userSchema = StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false),
        StructField("city", StringType, nullable = false)
      ))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("user", userSchema, nullable = false)
      ))

      // Create test data
      val data = Seq(
        Row(1, Row("Alice", 30, "NYC")),
        Row(2, Row("Bob", 25, "SF")),
        Row(3, Row("Charlie", 35, "LA")),
        Row(4, Row("Diana", 28, "NYC"))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write data
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} rows for filter pushdown test")

      // Read with filter on nested field
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("user.name") === "Alice")

      result.show(false)
      result.count() shouldBe 1

      val row = result.collect()(0)
      row.getInt(0) shouldBe 1
      val user = row.getStruct(1)
      user.getString(0) shouldBe "Alice"

      println("✅ Equality filter pushdown verified!")
    }
  }

  test("should push down range filter on nested struct field") {
    withTempPath { path =>
      // Define schema with nested Struct
      val userSchema = StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false)
      ))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("user", userSchema, nullable = false)
      ))

      // Create test data
      val data = Seq(
        Row(1, Row("Alice", 30)),
        Row(2, Row("Bob", 25)),
        Row(3, Row("Charlie", 35)),
        Row(4, Row("Diana", 28))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write data
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Test GreaterThan filter
      val result1 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("user.age") > 28)

      result1.show(false)
      result1.count() shouldBe 2  // Alice (30) and Charlie (35)

      // Test LessThan filter
      val result2 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("user.age") < 30)

      result2.count() shouldBe 2  // Bob (25) and Diana (28)

      println("✅ Range filter pushdown verified!")
    }
  }

  test("should push down combined filters on nested fields") {
    withTempPath { path =>
      // Define schema with nested Struct
      val userSchema = StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false),
        StructField("city", StringType, nullable = false)
      ))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("user", userSchema, nullable = false)
      ))

      // Create test data
      val data = Seq(
        Row(1, Row("Alice", 30, "NYC")),
        Row(2, Row("Bob", 25, "SF")),
        Row(3, Row("Charlie", 35, "NYC")),
        Row(4, Row("Diana", 28, "NYC")),
        Row(5, Row("Eve", 32, "LA"))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write data
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Test AND filter: city = NYC AND age > 28
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("user.city") === "NYC" && col("user.age") > 28)

      result.show(false)
      result.count() shouldBe 2  // Alice (30, NYC) and Charlie (35, NYC)

      val rows = result.collect()
      val ids = rows.map(_.getInt(0)).sorted
      ids shouldBe Array(1, 3)

      println("✅ Combined filter pushdown verified!")
    }
  }

  test("should push down IsNotNull filter on nested field") {
    withTempPath { path =>
      // Define schema with nullable nested field
      val userSchema = StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = false)
      ))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("user", userSchema, nullable = false)
      ))

      // Create test data with some null names
      val data = Seq(
        Row(1, Row("Alice", 30)),
        Row(2, Row(null, 25)),
        Row(3, Row("Charlie", 35))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write data
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Filter for non-null names
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("user.name").isNotNull)

      result.show(false)
      result.count() shouldBe 2  // Alice and Charlie

      println("✅ IsNotNull filter pushdown verified!")
    }
  }

  test("should push down complex boolean combinations") {
    withTempPath { path =>
      // Define schema with nested Struct
      val userSchema = StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false),
        StructField("city", StringType, nullable = false)
      ))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("user", userSchema, nullable = false)
      ))

      // Create test data
      val data = Seq(
        Row(1, Row("Alice", 30, "NYC")),
        Row(2, Row("Bob", 25, "SF")),
        Row(3, Row("Charlie", 35, "NYC")),
        Row(4, Row("Diana", 28, "LA")),
        Row(5, Row("Eve", 32, "NYC"))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write data
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Test OR filter: (city = NYC AND age > 30) OR name = Bob
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter((col("user.city") === "NYC" && col("user.age") > 30) || col("user.name") === "Bob")

      result.show(false)
      result.count() shouldBe 3  // Charlie (35, NYC), Eve (32, NYC), Bob

      val rows = result.collect()
      val ids = rows.map(_.getInt(0)).sorted
      ids shouldBe Array(2, 3, 5)

      println("✅ Complex boolean filter pushdown verified!")
    }
  }

  test("should push down deep nested path filters") {
    withTempPath { path =>
      // Define deep nested schema
      val addressSchema = StructType(Seq(
        StructField("street", StringType, nullable = false),
        StructField("city", StringType, nullable = false)
      ))

      val userSchema = StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("address", addressSchema, nullable = false)
      ))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("user", userSchema, nullable = false)
      ))

      // Create test data
      val data = Seq(
        Row(1, Row("Alice", Row("123 Main St", "NYC"))),
        Row(2, Row("Bob", Row("456 Oak Ave", "SF"))),
        Row(3, Row("Charlie", Row("789 Pine Rd", "NYC")))
      )

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Write data
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Filter on deep nested path
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)
        .filter(col("user.address.city") === "NYC")

      result.show(false)
      result.count() shouldBe 2  // Alice and Charlie

      val rows = result.collect()
      val ids = rows.map(_.getInt(0)).sorted
      ids shouldBe Array(1, 3)

      println("✅ Deep nested path filter pushdown verified!")
    }
  }
}
