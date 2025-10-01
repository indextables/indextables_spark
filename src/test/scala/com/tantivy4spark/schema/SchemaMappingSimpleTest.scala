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

package com.tantivy4spark.schema

import com.tantivy4spark.TestBase
import com.tantivy4spark.schema.SchemaMapping._
import org.apache.spark.sql.types._

/** Simple tests for the schema mapping functionality that don't require tantivy4java mocking */
class SchemaMappingSimpleTest extends TestBase {

  test("SchemaMapping.Write should handle new dataset with valid schema") {
    val sparkSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("salary", DoubleType, nullable = true),
        StructField("active", BooleanType, nullable = false)
      )
    )

    val result = SchemaMapping.Write.handleNewDataset(sparkSchema)
    result should equal(sparkSchema)
  }

  test("SchemaMapping.Write should reject new dataset with unsupported types") {
    val sparkSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("metadata", MapType(StringType, StringType), nullable = true)
      )
    )

    an[UnsupportedOperationException] should be thrownBy {
      SchemaMapping.Write.handleNewDataset(sparkSchema)
    }
  }

  test("SchemaMapping.Write should reject new dataset with array types") {
    val sparkSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("tags", ArrayType(StringType), nullable = true)
      )
    )

    an[UnsupportedOperationException] should be thrownBy {
      SchemaMapping.Write.handleNewDataset(sparkSchema)
    }
  }

  test("SchemaMapping.Write should handle existing dataset with matching schema") {
    val sparkSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true)
      )
    )

    val transactionLogSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true)
      )
    )

    val result = SchemaMapping.Write.handleExistingDataset(sparkSchema, transactionLogSchema)
    result should equal(transactionLogSchema)
  }

  test("SchemaMapping.Write should reject existing dataset with different field types") {
    val sparkSchema = StructType(
      Seq(
        StructField("id", LongType, nullable = false), // Different type
        StructField("name", StringType, nullable = true)
      )
    )

    val transactionLogSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false), // Original type
        StructField("name", StringType, nullable = true)
      )
    )

    val exception = the[SchemaConflictException] thrownBy {
      SchemaMapping.Write.handleExistingDataset(sparkSchema, transactionLogSchema)
    }

    exception.message should include("Schema conflict detected")
    exception.message should include("Field 'id' type mismatch")
  }

  test("SchemaMapping utility methods should correctly identify supported types") {
    SchemaMapping.isSupportedSparkType(StringType) should be(true)
    SchemaMapping.isSupportedSparkType(IntegerType) should be(true)
    SchemaMapping.isSupportedSparkType(LongType) should be(true)
    SchemaMapping.isSupportedSparkType(DoubleType) should be(true)
    SchemaMapping.isSupportedSparkType(FloatType) should be(true)
    SchemaMapping.isSupportedSparkType(BooleanType) should be(true)
    SchemaMapping.isSupportedSparkType(DateType) should be(true)
    SchemaMapping.isSupportedSparkType(TimestampType) should be(true)
    SchemaMapping.isSupportedSparkType(BinaryType) should be(true)

    // Unsupported types
    SchemaMapping.isSupportedSparkType(ArrayType(StringType)) should be(false)
    SchemaMapping.isSupportedSparkType(MapType(StringType, StringType)) should be(false)
    SchemaMapping.isSupportedSparkType(StructType(Seq.empty)) should be(false)
  }

  test("SchemaMapping utility methods should convert Spark types to Tantivy field types") {
    import io.indextables.tantivy4java.core.FieldType

    SchemaMapping.sparkTypeToTantivyFieldType(StringType) should be(FieldType.TEXT)
    SchemaMapping.sparkTypeToTantivyFieldType(IntegerType) should be(FieldType.INTEGER)
    SchemaMapping.sparkTypeToTantivyFieldType(LongType) should be(FieldType.INTEGER)
    SchemaMapping.sparkTypeToTantivyFieldType(DoubleType) should be(FieldType.FLOAT)
    SchemaMapping.sparkTypeToTantivyFieldType(FloatType) should be(FieldType.FLOAT)
    SchemaMapping.sparkTypeToTantivyFieldType(BooleanType) should be(FieldType.BOOLEAN)
    SchemaMapping.sparkTypeToTantivyFieldType(DateType) should be(FieldType.DATE)
    SchemaMapping.sparkTypeToTantivyFieldType(TimestampType) should be(FieldType.DATE)
    SchemaMapping.sparkTypeToTantivyFieldType(BinaryType) should be(FieldType.BYTES)
  }

  test("SchemaMapping should provide detailed error message for schema conflicts") {
    val sparkSchema = StructType(
      Seq(
        StructField("id", LongType, nullable = true),
        StructField("name", IntegerType, nullable = false), // Type conflict
        StructField("extra", StringType, nullable = true)   // Extra field
      )
    )

    val transactionLogSchema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),      // Nullability conflict
        StructField("name", StringType, nullable = false),  // Type conflict
        StructField("missing", StringType, nullable = true) // Missing field in spark schema
      )
    )

    val exception = the[SchemaConflictException] thrownBy {
      SchemaMapping.Write.handleExistingDataset(sparkSchema, transactionLogSchema)
    }

    exception.message should include("Field 'id' nullability mismatch")
    exception.message should include("Field 'name' type mismatch")
    exception.message should include("Field 'extra' exists in first schema but not second")
    exception.message should include("Field 'missing' exists in second schema but not first")
  }

  test("SchemaMapping should handle empty schemas") {
    val emptySparkSchema = StructType(Seq.empty)
    val result           = SchemaMapping.Write.handleNewDataset(emptySparkSchema)
    result should equal(emptySparkSchema)
  }

  test("SchemaMapping should handle schema with all supported types") {
    val allTypesSchema = StructType(
      Seq(
        StructField("str", StringType, nullable = true),
        StructField("int", IntegerType, nullable = false),
        StructField("long", LongType, nullable = true),
        StructField("double", DoubleType, nullable = false),
        StructField("float", FloatType, nullable = true),
        StructField("bool", BooleanType, nullable = false),
        StructField("date", DateType, nullable = true),
        StructField("timestamp", TimestampType, nullable = false),
        StructField("binary", BinaryType, nullable = true)
      )
    )

    val result = SchemaMapping.Write.handleNewDataset(allTypesSchema)
    result should equal(allTypesSchema)
  }
}
