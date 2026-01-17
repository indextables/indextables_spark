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
 * Tests for validating field names in indexing configuration options.
 *
 * This test validates that:
 *   1. typemap field names that don't exist in schema are rejected
 *   2. fastfields field names that don't exist in schema are rejected
 *   3. indexrecordoption field names that don't exist in schema are rejected
 *   4. Error messages include all invalid fields and available schema fields
 *   5. Valid configurations continue to work
 */
class IndexingConfigFieldValidationTest extends TestBase {

  test("should reject typemap configuration with non-existent field") {
    val tablePath = s"file://$tempDir/typemap_invalid_field_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as title", "CAST(id AS STRING) as content")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.title", "text")
        .option("spark.indextables.indexing.typemap.descriptin", "text") // Typo: should be "description"
        .mode("overwrite")
        .save(tablePath)
    }

    exception.getMessage should include("descriptin")
    exception.getMessage should include("does not exist")
  }

  test("should reject typemap configuration with multiple non-existent fields") {
    val tablePath = s"file://$tempDir/typemap_multiple_invalid_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as title")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.titl", "text")      // Typo
        .option("spark.indextables.indexing.typemap.contnt", "text")    // Typo
        .option("spark.indextables.indexing.typemap.descriptin", "text") // Typo
        .mode("overwrite")
        .save(tablePath)
    }

    // All invalid fields should be reported
    exception.getMessage should include("titl")
    exception.getMessage should include("contnt")
    exception.getMessage should include("descriptin")
  }

  test("should reject fastfields configuration with non-existent field") {
    val tablePath = s"file://$tempDir/fastfields_invalid_test"

    val df = spark
      .range(10)
      .selectExpr("id", "id * 2 as score", "id * 3 as value")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "score,val,ratng") // "val" and "ratng" are typos
        .mode("overwrite")
        .save(tablePath)
    }

    exception.getMessage should include("val")
    exception.getMessage should include("ratng")
    exception.getMessage should include("does not exist")
  }

  test("should reject indexrecordoption configuration with non-existent field") {
    val tablePath = s"file://$tempDir/indexrecordoption_invalid_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as content")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.indexrecordoption.contnt", "position") // Typo
        .mode("overwrite")
        .save(tablePath)
    }

    exception.getMessage should include("contnt")
    exception.getMessage should include("does not exist")
  }

  test("should report all configuration errors together") {
    val tablePath = s"file://$tempDir/all_config_errors_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as title", "id * 2 as score")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.titl", "text")           // Typo in typemap
        .option("spark.indextables.indexing.fastfields", "scor,ratng")       // Typos in fastfields
        .option("spark.indextables.indexing.indexrecordoption.contnt", "position") // Typo in indexrecordoption
        .mode("overwrite")
        .save(tablePath)
    }

    // All invalid fields should be in the error message
    exception.getMessage should include("titl")
    exception.getMessage should include("scor")
    exception.getMessage should include("ratng")
    exception.getMessage should include("contnt")
  }

  test("should include available schema fields in error message") {
    val tablePath = s"file://$tempDir/schema_fields_in_error_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as title", "CAST(id AS STRING) as content")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.descriptin", "text")
        .mode("overwrite")
        .save(tablePath)
    }

    // Error should help user by showing available fields
    exception.getMessage should (include("id") or include("title") or include("content") or include("Available"))
  }

  test("should allow valid typemap configuration") {
    val tablePath = s"file://$tempDir/valid_typemap_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as title", "CAST(id AS STRING) as content")

    // Valid configuration should succeed
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "string")
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 10
  }

  test("should allow valid fastfields configuration") {
    val tablePath = s"file://$tempDir/valid_fastfields_test"

    val df = spark
      .range(10)
      .selectExpr("id", "id * 2 as score", "id * 3 as value")

    // Valid configuration should succeed
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "score,value")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 10
  }

  test("should allow valid indexrecordoption configuration") {
    val tablePath = s"file://$tempDir/valid_indexrecordoption_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as content")

    // Valid configuration should succeed
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.indexrecordoption.content", "position")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 10
  }

  test("should handle case-insensitive field matching for typemap") {
    val tablePath = s"file://$tempDir/case_insensitive_typemap_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as Title", "CAST(id AS STRING) as CONTENT")

    // Should accept case-insensitive match
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "string")   // lowercase matches Title
      .option("spark.indextables.indexing.typemap.content", "text")   // lowercase matches CONTENT
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 10
  }

  test("should reject nonfastfields configuration with non-existent field") {
    val tablePath = s"file://$tempDir/nonfastfields_invalid_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as content")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.nonfastfields", "contnt,descriptin") // Typos
        .mode("overwrite")
        .save(tablePath)
    }

    exception.getMessage should include("contnt")
    exception.getMessage should include("descriptin")
  }

  test("should reject storeonlyfields configuration with non-existent field") {
    val tablePath = s"file://$tempDir/storeonlyfields_invalid_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as metadata")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.storeonlyfields", "metadta") // Typo
        .mode("overwrite")
        .save(tablePath)
    }

    exception.getMessage should include("metadta")
  }

  test("should reject indexonlyfields configuration with non-existent field") {
    val tablePath = s"file://$tempDir/indexonlyfields_invalid_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as searchable")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.indexonlyfields", "searchbl") // Typo
        .mode("overwrite")
        .save(tablePath)
    }

    exception.getMessage should include("searchbl")
  }

  test("should reject tokenizer configuration with non-existent field") {
    val tablePath = s"file://$tempDir/tokenizer_invalid_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as content")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.tokenizer.contnt", "en_stem") // Typo
        .mode("overwrite")
        .save(tablePath)
    }

    exception.getMessage should include("contnt")
  }

  test("should allow empty fastfields configuration") {
    val tablePath = s"file://$tempDir/empty_fastfields_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as title")

    // Empty fastfields should be allowed (no fields explicitly set as fast)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 10
  }

  test("should allow nested field paths when root field exists") {
    val tablePath = s"file://$tempDir/nested_field_paths_test"

    // Create a DataFrame with a struct field using SQL
    val df = spark.sql("""
      SELECT 1 as id, named_struct('name', 'Alice', 'age', 30) as user
      UNION ALL
      SELECT 2 as id, named_struct('name', 'Bob', 'age', 25) as user
    """)

    // Nested path like "user.age" should be allowed because "user" exists in schema
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "user.age,user.name")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 2
  }

  test("should reject nested field paths when root field does not exist") {
    val tablePath = s"file://$tempDir/invalid_nested_field_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as title")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "user.age,profile.name") // "user" and "profile" don't exist
        .mode("overwrite")
        .save(tablePath)
    }

    // Should report both missing root fields
    exception.getMessage should include("user")
    exception.getMessage should include("profile")
  }
}
