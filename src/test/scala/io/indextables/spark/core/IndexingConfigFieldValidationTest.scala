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
 *   1. typemap field names that don't exist in schema are rejected 2. fastfields field names that don't exist in schema
 *      are rejected 3. indexrecordoption field names that don't exist in schema are rejected 4. Error messages include
 *      all invalid fields and available schema fields 5. Valid configurations continue to work
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
        .option("spark.indextables.indexing.typemap.titl", "text")       // Typo
        .option("spark.indextables.indexing.typemap.contnt", "text")     // Typo
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
        .option("spark.indextables.indexing.typemap.titl", "text")                 // Typo in typemap
        .option("spark.indextables.indexing.fastfields", "scor,ratng")             // Typos in fastfields
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
      .option("spark.indextables.indexing.typemap.title", "string") // lowercase matches Title
      .option("spark.indextables.indexing.typemap.content", "text") // lowercase matches CONTENT
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
        .option("spark.indextables.indexing.tokenizer.en_stem", "contnt") // Typo in field name
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

  // ===== Tests for new list-based syntax =====

  test("should support list-based typemap syntax (typemap.text = field1,field2)") {
    val tablePath = s"file://$tempDir/typemap_list_syntax_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as title", "CAST(id AS STRING) as content", "CAST(id AS STRING) as body")

    // New syntax: typemap.<type> = "field1,field2,..."
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "title,content,body")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 10
  }

  test("should support list-based indexrecordoption syntax (indexrecordoption.position = field1,field2)") {
    val tablePath = s"file://$tempDir/indexrecordoption_list_syntax_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as title", "CAST(id AS STRING) as content")

    // New syntax: indexrecordoption.<option> = "field1,field2,..."
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "title,content")
      .option("spark.indextables.indexing.indexrecordoption.position", "title,content")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 10
  }

  test("should support list-based tokenizer syntax (tokenizer.en_stem = field1,field2)") {
    val tablePath = s"file://$tempDir/tokenizer_list_syntax_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as title", "CAST(id AS STRING) as content")

    // New syntax: tokenizer.<tokenizer_name> = "field1,field2,..."
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "title,content")
      .option("spark.indextables.indexing.tokenizer.en_stem", "title,content")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 10
  }

  test("should reject list-based typemap with invalid field names") {
    val tablePath = s"file://$tempDir/typemap_list_invalid_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as title")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.text", "title,contnt,descriptin") // typos
        .mode("overwrite")
        .save(tablePath)
    }

    exception.getMessage should include("contnt")
    exception.getMessage should include("descriptin")
  }

  test("should reject list-based indexrecordoption with invalid field names") {
    val tablePath = s"file://$tempDir/indexrecordoption_list_invalid_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as content")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.text", "content")
        .option("spark.indextables.indexing.indexrecordoption.position", "content,bdy") // typo
        .mode("overwrite")
        .save(tablePath)
    }

    exception.getMessage should include("bdy")
  }

  test("should reject list-based tokenizer with invalid field names") {
    val tablePath = s"file://$tempDir/tokenizer_list_invalid_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as content")

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.text", "content")
        .option("spark.indextables.indexing.tokenizer.en_stem", "content,titl") // typo
        .mode("overwrite")
        .save(tablePath)
    }

    exception.getMessage should include("titl")
  }

  test("should support mixed old and new typemap syntax") {
    val tablePath = s"file://$tempDir/typemap_mixed_syntax_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as title", "CAST(id AS STRING) as content", "CAST(id AS STRING) as tags")

    // Mixed syntax: some per-field, some per-type
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "title,content") // new syntax
      .option("spark.indextables.indexing.typemap.tags", "string")        // old syntax
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 10
  }

  test("should support single field in list-based syntax") {
    val tablePath = s"file://$tempDir/single_field_list_syntax_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as content")

    // List syntax with single field (no comma)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "content")
      .option("spark.indextables.indexing.tokenizer.en_stem", "content")
      .option("spark.indextables.indexing.indexrecordoption.basic", "content")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 10
  }

  test("should handle whitespace in list-based syntax") {
    val tablePath = s"file://$tempDir/whitespace_list_syntax_test"

    val df = spark
      .range(10)
      .selectExpr("id", "CAST(id AS STRING) as title", "CAST(id AS STRING) as content")

    // List with extra whitespace should be trimmed
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "  title , content  ")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 10
  }

  // ===== Functional tests to verify configuration is actually applied =====

  test("typemap.text should enable full-text search with tokenization") {
    val tablePath = s"file://$tempDir/typemap_text_functional_test"

    // Create data with multi-word content using SQL
    val df = spark.sql("""
      SELECT 1 as id, 'The quick brown fox jumps' as content
      UNION ALL SELECT 2 as id, 'A lazy dog sleeps' as content
      UNION ALL SELECT 3 as id, 'Running is good exercise' as content
    """)

    // Configure content as text field (tokenized)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "content")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result.createOrReplaceTempView("typemap_text_test")

    // Text fields should support single-word queries (tokenized search)
    val quickMatches = spark.sql("SELECT * FROM typemap_text_test WHERE content indexquery 'quick'").collect()
    quickMatches.length shouldBe 1
    quickMatches(0).getAs[Int]("id") shouldBe 1

    // Should also find partial word matches
    val dogMatches = spark.sql("SELECT * FROM typemap_text_test WHERE content indexquery 'dog'").collect()
    dogMatches.length shouldBe 1
    dogMatches(0).getAs[Int]("id") shouldBe 2
  }

  test("typemap.string should only match exact values") {
    val tablePath = s"file://$tempDir/typemap_string_functional_test"

    // Create data with various content using SQL
    val df = spark.sql("""
      SELECT 1 as id, 'active' as status
      UNION ALL SELECT 2 as id, 'inactive' as status
      UNION ALL SELECT 3 as id, 'active' as status
    """)

    // Configure status as string field (exact match, which is default)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.string", "status")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result.createOrReplaceTempView("typemap_string_test")

    // String fields should match exact values
    val activeMatches = spark.sql("SELECT * FROM typemap_string_test WHERE status = 'active'").collect()
    activeMatches.length shouldBe 2

    // Partial match should not work for string fields
    val partialMatches = spark.sql("SELECT * FROM typemap_string_test WHERE status = 'act'").collect()
    partialMatches.length shouldBe 0
  }

  test("tokenizer.en_stem should enable stemming (running matches run)") {
    val tablePath = s"file://$tempDir/tokenizer_stem_functional_test"

    // Create data with words that can be stemmed (regular verb forms)
    val df = spark.sql("""
      SELECT 1 as id, 'I am running fast' as content
      UNION ALL SELECT 2 as id, 'She runs daily' as content
      UNION ALL SELECT 3 as id, 'They run every morning' as content
    """)

    // Configure content as text with en_stem tokenizer
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "content")
      .option("spark.indextables.indexing.tokenizer.en_stem", "content")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result.createOrReplaceTempView("tokenizer_stem_test")

    // With stemming, "run" should match "running" and "runs" (Porter stemmer)
    val runMatches = spark.sql("SELECT * FROM tokenizer_stem_test WHERE content indexquery 'run'").collect()
    // running->run, runs->run, run->run, so all three should match
    runMatches.length should be >= 2 // At minimum running and runs should match
  }

  test("tokenizer without stemming should not match stemmed forms") {
    val tablePath = s"file://$tempDir/tokenizer_no_stem_functional_test"

    // Create data with words that can be stemmed
    val df = spark.sql("""
      SELECT 1 as id, 'I am running fast' as content
      UNION ALL SELECT 2 as id, 'She runs daily' as content
      UNION ALL SELECT 3 as id, 'They run daily' as content
    """)

    // Configure content as text with default tokenizer (no stemming)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "content")
      .option("spark.indextables.indexing.tokenizer.default", "content")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    result.createOrReplaceTempView("tokenizer_no_stem_test")

    // Without stemming, "run" should only match exact "run"
    val runMatches = spark.sql("SELECT * FROM tokenizer_no_stem_test WHERE content indexquery 'run'").collect()
    runMatches.length shouldBe 1 // Only record 3 with "run" should match

    // "running" should match exactly "running"
    val runningMatches = spark.sql("SELECT * FROM tokenizer_no_stem_test WHERE content indexquery 'running'").collect()
    runningMatches.length shouldBe 1
    runningMatches(0).getAs[Int]("id") shouldBe 1
  }
}
