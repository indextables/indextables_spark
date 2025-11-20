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
import org.scalatest.matchers.should.Matchers

/**
 * Tests to verify that complex data types (Struct, Array, Map) are now supported via JSON fields, and that other
 * unsupported types are explicitly rejected.
 */
class UnsupportedTypesTest extends TestBase with Matchers {

  test("should support array types via JSON fields when writing to IndexTables4Spark") {
    withTempPath { tempPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create DataFrame with array column
      val dataWithArray = Seq(
        (1, "item1", Array("tag1", "tag2", "tag3")),
        (2, "item2", Array("tagA", "tagB"))
      ).toDF("id", "name", "tags")

      // Should succeed - arrays are now supported via JSON fields
      noException should be thrownBy {
        dataWithArray.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("overwrite")
          .save(tempPath)
      }

      // Verify we can read back the data
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      result.count() shouldBe 2
      println(s"✅ Array type correctly supported via JSON fields")
    }
  }

  test("should support map types via JSON fields when writing to IndexTables4Spark") {
    withTempPath { tempPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create DataFrame with map column using map function
      val dataWithMap = Seq(
        (1, "item1", "key1:val1,key2:val2"),
        (2, "item2", "keyA:valA,keyB:valB")
      ).toDF("id", "name", "data_str")
        .withColumn("metadata", map(lit("type"), col("name"), lit("count"), lit(1)))

      // Should succeed - maps are now supported via JSON fields
      noException should be thrownBy {
        dataWithMap.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("overwrite")
          .save(tempPath)
      }

      // Verify we can read back the data
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      result.count() shouldBe 2
      println(s"✅ Map type correctly supported via JSON fields")
    }
  }

  test("should support struct types via JSON fields when writing to IndexTables4Spark") {
    withTempPath { tempPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create DataFrame with struct column
      val dataWithStruct = Seq(
        (1, "item1", "some data"),
        (2, "item2", "more data")
      ).toDF("id", "name", "raw_data")
        .withColumn("details", struct(col("name"), col("raw_data").alias("data")))

      // Should succeed - structs are now supported via JSON fields
      noException should be thrownBy {
        dataWithStruct.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("overwrite")
          .save(tempPath)
      }

      // Verify we can read back the data
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      result.count() shouldBe 2
      println(s"✅ Struct type correctly supported via JSON fields")
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

  test("should support complex nested types via JSON fields") {
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

      // Should succeed - complex nested types are now supported via JSON fields
      noException should be thrownBy {
        complexData.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("overwrite")
          .save(tempPath)
      }

      // Verify we can read back the data
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      result.count() shouldBe 2
      println(s"✅ Complex nested type correctly supported via JSON fields")
    }
  }

  test("should handle schema validation during DataFrame creation") {
    // TypeConversionUtil handles basic field types only
    // Complex types (Array, Map, Struct) are handled separately via JSON fields at a higher level
    import io.indextables.spark.util.TypeConversionUtil

    // TypeConversionUtil still rejects complex types (these are handled by JSON field logic)
    intercept[UnsupportedOperationException] {
      TypeConversionUtil.sparkTypeToTantivyType(ArrayType(StringType))
    }.getMessage should include("Array types are not supported")

    intercept[UnsupportedOperationException] {
      TypeConversionUtil.sparkTypeToTantivyType(MapType(StringType, StringType))
    }.getMessage should include("Map types are not supported")

    intercept[UnsupportedOperationException] {
      TypeConversionUtil.sparkTypeToTantivyType(StructType(Seq.empty))
    }.getMessage should include("Struct types are not supported")

    println(s"✅ TypeConversionUtil correctly rejects complex types (handled separately by JSON fields)")
  }
}
