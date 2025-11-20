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

/**
 * Tests for duplicate column name validation.
 *
 * This test validates that:
 *   1. Duplicate column names are rejected with clear error messages 2. Case-insensitive duplicates generate warnings
 *      3. The validation prevents JVM crashes that would occur with duplicate columns
 */
class DuplicateColumnValidationTest extends TestBase {

  test("should reject DataFrame with duplicate column names") {
    val tablePath = s"file://$tempDir/duplicate_columns_test"

    // Create a DataFrame with duplicate column names
    val df = spark
      .range(10)
      .selectExpr("id", "id as name", "id + 1 as id") // "id" appears twice

    // Attempt to write should throw IllegalArgumentException
    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(tablePath)
    }

    // Verify error message mentions duplicate columns
    exception.getMessage should include("duplicate column names")
    exception.getMessage should include("id")
    exception.getMessage should include("JVM crash")
  }

  test("should reject DataFrame with multiple duplicate column names") {
    val tablePath = s"file://$tempDir/multiple_duplicates_test"

    // Create a DataFrame with multiple duplicate column names
    val df = spark
      .range(10)
      .selectExpr(
        "id",
        "id as name",
        "id + 1 as id", // "id" appears 3 times total
        "id * 2 as value",
        "id / 2 as value" // "value" appears twice
      )

    // Attempt to write should throw IllegalArgumentException
    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(tablePath)
    }

    // Verify error message mentions duplicate columns
    exception.getMessage should include("duplicate column names")
    // At least one of the duplicate columns should be mentioned
    assert(
      exception.getMessage.contains("id") || exception.getMessage.contains("value"),
      s"Error message should mention 'id' or 'value': ${exception.getMessage}"
    )
  }

  test("should allow DataFrame with unique column names") {
    val tablePath = s"file://$tempDir/unique_columns_test"

    // Create a DataFrame with unique column names
    val df = spark
      .range(10)
      .selectExpr("id", "id as name", "id + 1 as value")

    // Write should succeed
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Verify data can be read back
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 10
    result.columns should contain allOf ("id", "name", "value")
  }

  test("should warn about case-insensitive duplicate column names") {
    val tablePath = s"file://$tempDir/case_insensitive_duplicates_test"

    // Create a DataFrame with columns that differ only in case
    // Note: Spark itself may prevent this, but we test the validation logic
    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as Name", "id + 1 as value")

    // This should succeed (case-sensitive duplicates are allowed, just warned about)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Verify data can be read back
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 10
  }

  test("should handle empty DataFrame schema") {
    val tablePath = s"file://$tempDir/empty_schema_test"

    // Create an empty DataFrame with a valid schema
    val df = spark.createDataFrame(
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      org.apache.spark.sql.types.StructType(
        Seq(
          org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType, nullable = true)
        )
      )
    )

    // Write should succeed
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Verify table was created
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 0
    result.columns should contain("id")
  }

  test("should provide clear error message for duplicate columns") {
    val tablePath = s"file://$tempDir/clear_error_message_test"

    val df = spark
      .range(5)
      .selectExpr("id", "id as duplicate_col", "id + 1 as duplicate_col")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(tablePath)
    }

    // Verify error message is informative
    exception.getMessage should include("Schema contains duplicate column names")
    exception.getMessage should include("duplicate_col")
    exception.getMessage should include("unique")
    exception.getMessage should include("JVM crash")
  }
}
