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

import io.indextables.spark.TestBase
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

/**
 * Tests to verify that unsupported data types are explicitly rejected rather than being silently converted or causing
 * runtime errors.
 */
class UnsupportedTypesTest extends TestBase with Matchers {

  test("should reject array types when writing to IndexTables4Spark") {
    withTempPath { tempPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create DataFrame with array column
      val dataWithArray = Seq(
        (1, "item1", Array("tag1", "tag2", "tag3")),
        (2, "item2", Array("tagA", "tagB"))
      ).toDF("id", "name", "tags")

      // Should throw UnsupportedOperationException when trying to write
      val exception = intercept[org.apache.spark.SparkException] {
        dataWithArray.write
          .format("tantivy4spark")
          .mode("overwrite")
          .save(tempPath)
      }

      // Verify the exception contains appropriate message about arrays
      exception.getMessage should include("UnsupportedOperationException")
      println(s"✅ Array type correctly rejected: ${exception.getMessage}")
    }
  }

  test("should reject map types when writing to IndexTables4Spark") {
    withTempPath { tempPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create DataFrame with map column using map function
      val dataWithMap = Seq(
        (1, "item1", "key1:val1,key2:val2"),
        (2, "item2", "keyA:valA,keyB:valB")
      ).toDF("id", "name", "data_str")
        .withColumn("metadata", map(lit("type"), col("name"), lit("count"), lit(1)))

      // Should throw UnsupportedOperationException when trying to write
      val exception = intercept[org.apache.spark.SparkException] {
        dataWithMap.write
          .format("tantivy4spark")
          .mode("overwrite")
          .save(tempPath)
      }

      // Verify the exception contains appropriate message about maps
      exception.getMessage should include("UnsupportedOperationException")
      println(s"✅ Map type correctly rejected: ${exception.getMessage}")
    }
  }

  test("should reject struct types when writing to IndexTables4Spark") {
    withTempPath { tempPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create DataFrame with struct column
      val dataWithStruct = Seq(
        (1, "item1", "some data"),
        (2, "item2", "more data")
      ).toDF("id", "name", "raw_data")
        .withColumn("details", struct(col("name"), col("raw_data").alias("data")))

      // Should throw UnsupportedOperationException when trying to write
      val exception = intercept[org.apache.spark.SparkException] {
        dataWithStruct.write
          .format("tantivy4spark")
          .mode("overwrite")
          .save(tempPath)
      }

      // Verify the exception contains appropriate message about structs
      exception.getMessage should include("UnsupportedOperationException")
      println(s"✅ Struct type correctly rejected: ${exception.getMessage}")
    }
  }

  test("should support all basic data types") {
    withTempPath { tempPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create DataFrame with basic supported types (avoiding problematic float/double mix)
      val supportedData = Seq(
        (1L, "text", 42, 2.718d, true)
      ).toDF("long_col", "string_col", "int_col", "double_col", "bool_col")
        .withColumn("timestamp_col", current_timestamp())
        .withColumn("date_col", current_date())

      // Should succeed without any exceptions
      noException should be thrownBy {
        supportedData.write
          .format("tantivy4spark")
          .mode("overwrite")
          .save(tempPath)
      }

      // Verify we can read back the data
      val readData = spark.read
        .format("tantivy4spark")
        .load(tempPath)

      readData.count() shouldBe 1
      readData.columns should contain allOf ("long_col", "string_col", "int_col", "double_col", "bool_col", "timestamp_col", "date_col")

      println(s"✅ All supported basic types handled correctly")
    }
  }

  test("should support numeric data types specifically") {
    withTempPath { tempPath =>
      // Test both Float and Double type support through schema verification
      import io.indextables.spark.util.TypeConversionUtil
      import org.apache.spark.sql.types.{FloatType, DoubleType, BinaryType}

      // Verify that both Float and Double types convert correctly to "f64"
      val floatTypeResult = TypeConversionUtil.sparkTypeToTantivyType(FloatType)
      floatTypeResult shouldBe "f64"

      val doubleTypeResult = TypeConversionUtil.sparkTypeToTantivyType(DoubleType)
      doubleTypeResult shouldBe "f64"

      // Verify that BinaryType converts correctly to "bytes"
      val binaryTypeResult = TypeConversionUtil.sparkTypeToTantivyType(BinaryType)
      binaryTypeResult shouldBe "bytes"

      println(s"✅ Numeric data types correctly supported: FloatType -> $floatTypeResult, DoubleType -> $doubleTypeResult, BinaryType -> $binaryTypeResult")
    }
  }

  test("should provide clear error messages for complex nested types") {
    withTempPath { tempPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create DataFrame with nested array of structs (very complex)
      val complexData = Seq(
        (1, "item1"),
        (2, "item2")
      ).toDF("id", "name")
        .withColumn(
          "complex_data",
          array(
            struct(lit("key1"), lit("value1")),
            struct(lit("key2"), lit("value2"))
          )
        )

      // Should throw UnsupportedOperationException
      val exception = intercept[org.apache.spark.SparkException] {
        complexData.write
          .format("tantivy4spark")
          .mode("overwrite")
          .save(tempPath)
      }

      // Should mention unsupported operation (complex nested types)
      exception.getMessage should include("UnsupportedOperationException")
      println(s"✅ Complex nested type correctly rejected: ${exception.getMessage}")
    }
  }

  test("should handle schema validation during DataFrame creation") {
    // Test that the type checking happens early in the process
    val arraySchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("tags", ArrayType(StringType), nullable = true)
      )
    )

    val mapSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("metadata", MapType(StringType, StringType), nullable = true)
      )
    )

    val structSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField(
          "details",
          StructType(
            Seq(
              StructField("name", StringType, nullable = true),
              StructField("value", IntegerType, nullable = true)
            )
          ),
          nullable = true
        )
      )
    )

    // Test type conversion directly
    import io.indextables.spark.util.TypeConversionUtil

    intercept[UnsupportedOperationException] {
      TypeConversionUtil.sparkTypeToTantivyType(ArrayType(StringType))
    }.getMessage should include("Array types are not supported")

    intercept[UnsupportedOperationException] {
      TypeConversionUtil.sparkTypeToTantivyType(MapType(StringType, StringType))
    }.getMessage should include("Map types are not supported")

    intercept[UnsupportedOperationException] {
      TypeConversionUtil.sparkTypeToTantivyType(StructType(Seq.empty))
    }.getMessage should include("Struct types are not supported")

    println(s"✅ Schema validation correctly rejects unsupported types at conversion level")
  }
}
