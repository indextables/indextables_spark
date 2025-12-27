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

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import io.indextables.spark.TestBase

/**
 * Tests for using IndexQuery with Struct types (which are automatically indexed as JSON).
 *
 * This demonstrates the WORKING approach: using Spark Struct types that are automatically converted to JSON fields and
 * support IndexQuery on nested properties.
 *
 * Example: case class Data(one: String, three: String) val df = Seq((1, Data("two", "four"))).toDF("id", "val_field")
 * Query: _indexall indexquery 'val_field.one:two'
 */
class JsonStringTypeIndexQueryTest extends TestBase {

  test("should use IndexQuery with Struct fields (val_field.one:two example)") {
    withTempPath { path =>
      // Define schema with Struct (will be automatically indexed as JSON)
      val valFieldSchema = StructType(
        Seq(
          StructField("one", StringType, nullable = false),
          StructField("three", StringType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("val_field", valFieldSchema, nullable = false)
        )
      )

      // Create test data
      val testData = Seq(
        Row(1, Row("two", "four")),
        Row(2, Row("alpha", "beta")),
        Row(3, Row("two", "gamma")),
        Row(4, Row("delta", "four"))
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      // Write - Struct types are automatically indexed as JSON
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} documents with Struct field (auto-indexed as JSON)")

      // Read back
      val resultDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      resultDf.createOrReplaceTempView("test_data")

      // Test: Query val_field.one:two (exact example from user request!)
      println("\n🔍 Test: Query _indexall indexquery 'val_field.one:two'")
      val query = spark.sql("""
        SELECT id, val_field
        FROM test_data
        WHERE _indexall indexquery 'val_field.one:two'
        ORDER BY id
      """)

      val results = query.collect()
      println(s"   Found ${results.length} documents where val_field.one = 'two':")
      results.foreach(row => println(s"   - ID ${row.getInt(0)}: ${row.getStruct(1)}"))

      results.length should be >= 1
      results.foreach { row =>
        val valField = row.getStruct(1)
        valField.getString(0) shouldBe "two"
      }

      println("\n🎉 IndexQuery with Struct fields works perfectly!")
    }
  }

  test("should query nested Struct properties with AND") {
    withTempPath { path =>
      // Schema with multiple properties
      val userDataSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("role", StringType, nullable = false),
          StructField("team", StringType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("user_data", userDataSchema, nullable = false)
        )
      )

      // Test data
      val testData = Seq(
        Row(1, Row("alice", "engineer", "search")),
        Row(2, Row("bob", "manager", "analytics")),
        Row(3, Row("charlie", "engineer", "platform")),
        Row(4, Row("diana", "designer", "search")),
        Row(5, Row("eve", "engineer", "analytics"))
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      val resultDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      resultDf.createOrReplaceTempView("users")

      // Test: Query with AND
      println("\n🔍 Test: user_data.role:engineer AND user_data.team:analytics")
      val query = spark.sql("""
        SELECT id, user_data
        FROM users
        WHERE _indexall indexquery 'user_data.role:engineer AND user_data.team:analytics'
        ORDER BY id
      """)

      val results = query.collect()
      println(s"   Found ${results.length} engineers in analytics:")
      results.foreach { row =>
        val userData = row.getStruct(1)
        println(s"   - ID ${row.getInt(0)}: role=${userData.getString(1)}, team=${userData.getString(2)}")
      }

      results.length should be >= 1
      results.foreach { row =>
        val userData = row.getStruct(1)
        userData.getString(1) shouldBe "engineer"  // role
        userData.getString(2) shouldBe "analytics" // team
      }

      println("\n🎉 Complex IndexQuery with AND works!")
    }
  }

  test("should query nested Struct properties with OR") {
    withTempPath { path =>
      val productSchema = StructType(
        Seq(
          StructField("name", StringType, nullable = false),
          StructField("category", StringType, nullable = false),
          StructField("brand", StringType, nullable = false)
        )
      )

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("product", productSchema, nullable = false)
        )
      )

      val testData = Seq(
        Row(1, Row("laptop", "electronics", "dell")),
        Row(2, Row("mouse", "electronics", "logitech")),
        Row(3, Row("desk", "furniture", "ikea")),
        Row(4, Row("chair", "furniture", "ikea")),
        Row(5, Row("monitor", "electronics", "dell"))
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      val resultDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      resultDf.createOrReplaceTempView("products")

      // Test: Query with OR
      println("\n🔍 Test: product.brand:dell OR product.brand:ikea")
      val query = spark.sql("""
        SELECT id, product
        FROM products
        WHERE _indexall indexquery 'product.brand:dell OR product.brand:ikea'
        ORDER BY id
      """)

      val results = query.collect()
      println(s"   Found ${results.length} products from Dell or IKEA:")
      results.foreach { row =>
        val product = row.getStruct(1)
        println(s"   - ID ${row.getInt(0)}: ${product.getString(0)} (${product.getString(2)})")
      }

      results.length should be >= 2
      results.foreach { row =>
        val product = row.getStruct(1)
        val brand   = product.getString(2)
        assert(brand == "dell" || brand == "ikea", s"Expected dell or ikea, got $brand")
      }

      println("\n🎉 IndexQuery with OR works!")
    }
  }
}
