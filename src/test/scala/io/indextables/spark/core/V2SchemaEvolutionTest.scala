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

package io.indextables.spark.core

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import io.indextables.spark.TestBase
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * Test schema evolution in V2 DataSource API including:
 *   - Column additions and removals
 *   - Data type changes and compatibility
 *   - Schema inference and validation
 *   - Backward compatibility handling
 *   - Column ordering variations
 */
class V2SchemaEvolutionTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  ignore("should handle column additions in schema evolution") {
    withTempPath { path =>
      // Initial schema: id, name
      val initialData = spark
        .range(0, 10)
        .select(
          col("id"),
          concat(lit("User "), col("id")).as("name")
        )

      initialData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Verify initial read
      val initialResult = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      initialResult.count() shouldBe 10
      initialResult.schema.fieldNames should contain theSameElementsAs Array("id", "name")

      // Evolved schema: id, name, email (new column)
      val evolvedData = spark
        .range(10, 20)
        .select(
          col("id"),
          concat(lit("User "), col("id")).as("name"),
          concat(lit("user"), col("id"), lit("@example.com")).as("email")
        )

      // Append data with new schema
      evolvedData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(path)

      // Read evolved table
      val evolvedResult = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      evolvedResult.count() shouldBe 20
      evolvedResult.schema.fieldNames should contain theSameElementsAs Array("id", "name", "email")

      // Verify data integrity
      val oldDataCount = evolvedResult.filter(col("email").isNull).count()
      val newDataCount = evolvedResult.filter(col("email").isNotNull).count()

      oldDataCount shouldBe 10 // Original data should have null emails
      newDataCount shouldBe 10 // New data should have email values
    }
  }

  ignore("should handle column reordering gracefully") {
    withTempPath { path =>
      // Original order: id, name, age
      val originalData = spark
        .range(0, 5)
        .select(
          col("id"),
          concat(lit("Person "), col("id")).as("name"),
          (col("id") + 20).cast(IntegerType).as("age")
        )

      originalData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Different order: age, id, name
      val reorderedData = spark
        .range(5, 10)
        .select(
          (col("id") + 25).cast(IntegerType).as("age"),
          col("id"),
          concat(lit("Person "), col("id")).as("name")
        )

      reorderedData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(path)

      // Read combined data
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 10

      // Verify schema contains all columns (order may vary)
      result.schema.fieldNames should contain theSameElementsAs Array("id", "name", "age")

      // Verify data correctness regardless of write order
      val ages = result.select("age").collect().map(_.getInt(0)).sorted
      ages.toSeq should contain theSameElementsAs (20 until 30)
    }
  }

  ignore("should handle data type compatibility") {
    withTempPath { path =>
      // Original data with Integer
      val intData = spark
        .range(0, 5)
        .select(
          col("id"),
          col("id").cast(IntegerType).as("value"),
          lit("integer").as("data_type")
        )

      intData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // New data with Long (compatible with Integer)
      val longData = spark
        .range(5, 10)
        .select(
          col("id"),
          col("id").cast(LongType).as("value"),
          lit("long").as("data_type")
        )

      longData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(path)

      // Read combined data
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 10

      // Schema should handle type promotion
      val valueField = result.schema.fields.find(_.name == "value").get
      valueField.dataType should be(LongType) // Should be promoted to Long

      // Verify data from both sources
      val intCount  = result.filter(col("data_type") === "integer").count()
      val longCount = result.filter(col("data_type") === "long").count()

      intCount shouldBe 5
      longCount shouldBe 5
    }
  }

  ignore("should handle backward compatibility with missing columns") {
    withTempPath { path =>
      // Full schema: id, name, category, active
      val fullData = spark
        .range(0, 8)
        .select(
          col("id"),
          concat(lit("Item "), col("id")).as("name"),
          when(col("id") % 2 === 0, "even").otherwise("odd").as("category"),
          (col("id") % 3 === 0).as("active")
        )

      fullData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Partial schema: id, name (missing category, active)
      val partialData = spark
        .range(8, 15)
        .select(
          col("id"),
          concat(lit("Item "), col("id")).as("name")
        )

      partialData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(path)

      // Read all data
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 15
      result.schema.fieldNames should contain theSameElementsAs Array("id", "name", "category", "active")

      // Verify null handling for missing columns
      val fullRowsCount    = result.filter(col("category").isNotNull && col("active").isNotNull).count()
      val partialRowsCount = result.filter(col("category").isNull && col("active").isNull).count()

      fullRowsCount shouldBe 8    // Original full schema data
      partialRowsCount shouldBe 7 // New partial schema data
    }
  }

  ignore("should handle schema inference correctly") {
    withTempPath { path =>
      // Create data with various types for schema inference
      val diverseData = spark
        .range(0, 10)
        .select(
          col("id"),
          concat(lit("Record "), col("id")).as("description"),
          (col("id") * 1.5).as("score"),
          (col("id") % 2 === 0).as("is_even"),
          current_timestamp().as("created_at")
        )

      diverseData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Read without explicit schema
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      // Verify inferred schema types
      val schema = result.schema
      schema.fields.find(_.name == "id").get.dataType shouldBe LongType
      schema.fields.find(_.name == "description").get.dataType shouldBe StringType
      schema.fields.find(_.name == "score").get.dataType shouldBe DoubleType
      schema.fields.find(_.name == "is_even").get.dataType shouldBe BooleanType
      schema.fields.find(_.name == "created_at").get.dataType shouldBe TimestampType

      result.count() shouldBe 10
    }
  }

  ignore("should handle string to numeric evolution") {
    withTempPath { path =>
      // Original data: numeric values stored as strings
      val stringData = spark
        .range(0, 6)
        .select(
          col("id"),
          col("id").cast(StringType).as("numeric_field"), // Numbers as strings
          lit("string").as("source_type")
        )

      stringData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Evolution: actual numeric values
      val numericData = spark
        .range(6, 12)
        .select(
          col("id"),
          (col("id") * 100).cast(StringType).as("numeric_field"), // Keep as string for compatibility
          lit("numeric").as("source_type")
        )

      numericData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(path)

      // Read and verify
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 12

      // Schema should remain string (most compatible)
      val numericField = result.schema.fields.find(_.name == "numeric_field").get
      numericField.dataType shouldBe StringType

      // Verify both data types are present
      val stringCount  = result.filter(col("source_type") === "string").count()
      val numericCount = result.filter(col("source_type") === "numeric").count()

      stringCount shouldBe 6
      numericCount shouldBe 6
    }
  }

  ignore("should handle nullable to non-nullable evolution") {
    withTempPath { path =>
      // Original data with nullable columns
      val nullableData = spark
        .range(0, 5)
        .select(
          col("id"),
          when(col("id") % 2 === 0, concat(lit("Name "), col("id"))).otherwise(lit(null)).as("name"),
          when(col("id") > 2, col("id") * 10).otherwise(lit(null)).cast(IntegerType).as("value")
        )

      nullableData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // New data without nulls
      val nonNullableData = spark
        .range(5, 10)
        .select(
          col("id"),
          concat(lit("Name "), col("id")).as("name"),    // Always has value
          (col("id") * 10).cast(IntegerType).as("value") // Always has value
        )

      nonNullableData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(path)

      // Read combined data
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 10

      // Verify null handling
      val nullNameCount    = result.filter(col("name").isNull).count()
      val nonNullNameCount = result.filter(col("name").isNotNull).count()

      nullNameCount shouldBe 3    // From original nullable data
      nonNullNameCount shouldBe 7 // From both datasets combined
    }
  }

  ignore("should handle complex schema merging") {
    withTempPath { path =>
      // Dataset 1: Basic user info
      val userData = spark
        .range(0, 4)
        .select(
          col("id"),
          concat(lit("User "), col("id")).as("username"),
          (col("id") + 25).cast(IntegerType).as("age")
        )

      userData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Dataset 2: Add contact info
      val contactData = spark
        .range(4, 7)
        .select(
          col("id"),
          concat(lit("User "), col("id")).as("username"),
          (col("id") + 25).cast(IntegerType).as("age"),
          concat(lit("user"), col("id"), lit("@email.com")).as("email"),
          concat(lit("555-000"), col("id")).as("phone")
        )

      contactData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(path)

      // Dataset 3: Add preference info
      val prefData = spark
        .range(7, 10)
        .select(
          col("id"),
          concat(lit("User "), col("id")).as("username"),
          (col("id") + 25).cast(IntegerType).as("age"),
          concat(lit("user"), col("id"), lit("@email.com")).as("email"),
          concat(lit("555-000"), col("id")).as("phone"),
          (col("id") % 2 === 0).as("newsletter_subscribed"),
          when(col("id") % 3 === 0, "premium").otherwise("basic").as("account_type")
        )

      prefData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(path)

      // Read final merged schema
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 10
      result.schema.fieldNames should contain theSameElementsAs Array(
        "id",
        "username",
        "age",
        "email",
        "phone",
        "newsletter_subscribed",
        "account_type"
      )

      // Verify progressive data availability
      val basicCount   = result.filter(col("email").isNull && col("phone").isNull).count()
      val contactCount = result.filter(col("email").isNotNull && col("newsletter_subscribed").isNull).count()
      val fullCount    = result.filter(col("newsletter_subscribed").isNotNull).count()

      basicCount shouldBe 4   // First dataset
      contactCount shouldBe 3 // Second dataset
      fullCount shouldBe 3    // Third dataset
    }
  }
}
