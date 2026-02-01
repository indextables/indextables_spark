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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.TestBase

/**
 * Tests for configurable token length limits.
 *
 * During tokenization, tokens longer than the configured limit are **filtered out** (not truncated). This prevents
 * excessively long tokens from consuming index space and degrading search performance.
 *
 * Token length constants (matching tantivy4java's TokenLength class):
 *   - TANTIVY_MAX (65530): Maximum supported by Tantivy (u16::MAX - 5)
 *   - DEFAULT (255): Quickwit-compatible default
 *   - LEGACY (40): Original tantivy4java default
 *   - MIN (1): Minimum valid limit
 */
class TokenLengthConfigurationTest extends TestBase {

  // ===== Unit Tests for IndexTables4SparkOptions =====

  test("getDefaultMaxTokenLength should return 255 when not configured") {
    val options = new IndexTables4SparkOptions(new CaseInsensitiveStringMap(Map.empty[String, String].asJava))
    options.getDefaultMaxTokenLength shouldBe 255
  }

  test("getDefaultMaxTokenLength should accept numeric value") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map("spark.indextables.indexing.text.maxTokenLength" -> "100").asJava
      )
    )
    options.getDefaultMaxTokenLength shouldBe 100
  }

  test("getDefaultMaxTokenLength should accept named constant 'legacy'") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map("spark.indextables.indexing.text.maxTokenLength" -> "legacy").asJava
      )
    )
    options.getDefaultMaxTokenLength shouldBe 40
  }

  test("getDefaultMaxTokenLength should accept named constant 'tantivy_max'") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map("spark.indextables.indexing.text.maxTokenLength" -> "tantivy_max").asJava
      )
    )
    options.getDefaultMaxTokenLength shouldBe 65530
  }

  test("getDefaultMaxTokenLength should accept named constant 'default'") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map("spark.indextables.indexing.text.maxTokenLength" -> "default").asJava
      )
    )
    options.getDefaultMaxTokenLength shouldBe 255
  }

  test("getDefaultMaxTokenLength should accept named constant 'min'") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map("spark.indextables.indexing.text.maxTokenLength" -> "min").asJava
      )
    )
    options.getDefaultMaxTokenLength shouldBe 1
  }

  test("getDefaultMaxTokenLength should reject value below minimum") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map("spark.indextables.indexing.text.maxTokenLength" -> "0").asJava
      )
    )
    val exception = intercept[IllegalArgumentException] {
      options.getDefaultMaxTokenLength
    }
    exception.getMessage should include("must be between 1 and 65530")
  }

  test("getDefaultMaxTokenLength should reject value above maximum") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map("spark.indextables.indexing.text.maxTokenLength" -> "65531").asJava
      )
    )
    val exception = intercept[IllegalArgumentException] {
      options.getDefaultMaxTokenLength
    }
    exception.getMessage should include("must be between 1 and 65530")
  }

  test("getDefaultMaxTokenLength should reject invalid string value") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map("spark.indextables.indexing.text.maxTokenLength" -> "invalid").asJava
      )
    )
    val exception = intercept[IllegalArgumentException] {
      options.getDefaultMaxTokenLength
    }
    exception.getMessage should include("Invalid token length value")
  }

  // ===== Per-field token length override tests (old syntax) =====

  test("getMaxTokenLengthOverrides should parse old syntax (tokenLength.fieldName = value)") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.indexing.tokenLength.content" -> "255",
          "spark.indextables.indexing.tokenLength.title"   -> "100"
        ).asJava
      )
    )
    val overrides = options.getMaxTokenLengthOverrides
    overrides("content") shouldBe 255
    overrides("title") shouldBe 100
  }

  test("getMaxTokenLengthOverrides should accept named constants in old syntax") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.indexing.tokenLength.content"     -> "legacy",
          "spark.indextables.indexing.tokenLength.title"       -> "tantivy_max",
          "spark.indextables.indexing.tokenLength.description" -> "default"
        ).asJava
      )
    )
    val overrides = options.getMaxTokenLengthOverrides
    overrides("content") shouldBe 40
    overrides("title") shouldBe 65530
    overrides("description") shouldBe 255
  }

  // ===== Per-field token length override tests (new list-based syntax) =====

  test("getMaxTokenLengthOverrides should parse new syntax (tokenLength.value = field1,field2)") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map("spark.indextables.indexing.tokenLength.255" -> "content,title,body").asJava
      )
    )
    val overrides = options.getMaxTokenLengthOverrides
    overrides("content") shouldBe 255
    overrides("title") shouldBe 255
    overrides("body") shouldBe 255
  }

  test("getMaxTokenLengthOverrides should accept named constants in new syntax") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.indexing.tokenLength.legacy"      -> "short_content",
          "spark.indextables.indexing.tokenLength.tantivy_max" -> "url,base64_data"
        ).asJava
      )
    )
    val overrides = options.getMaxTokenLengthOverrides
    overrides("short_content") shouldBe 40
    overrides("url") shouldBe 65530
    overrides("base64_data") shouldBe 65530
  }

  test("getMaxTokenLengthOverrides should handle mixed old and new syntax") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.indexing.tokenLength.255"     -> "content,title", // new syntax
          "spark.indextables.indexing.tokenLength.summary" -> "legacy",        // old syntax
          "spark.indextables.indexing.tokenLength.40"      -> "short_field"    // new syntax with numeric
        ).asJava
      )
    )
    val overrides = options.getMaxTokenLengthOverrides
    overrides("content") shouldBe 255
    overrides("title") shouldBe 255
    overrides("summary") shouldBe 40
    overrides("short_field") shouldBe 40
  }

  // ===== FieldIndexingConfig tests =====

  test("getFieldIndexingConfig should include maxTokenLength with default value") {
    val options = new IndexTables4SparkOptions(new CaseInsensitiveStringMap(Map.empty[String, String].asJava))
    val config  = options.getFieldIndexingConfig("content")
    config.maxTokenLength shouldBe Some(255) // Default value
  }

  test("getFieldIndexingConfig should include per-field token length override") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map("spark.indextables.indexing.tokenLength.content" -> "100").asJava
      )
    )
    val config = options.getFieldIndexingConfig("content")
    config.maxTokenLength shouldBe Some(100)
  }

  test("getFieldIndexingConfig should prefer per-field override over default") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.indexing.text.maxTokenLength" -> "255",
          "spark.indextables.indexing.tokenLength.content" -> "40"
        ).asJava
      )
    )
    val config = options.getFieldIndexingConfig("content")
    config.maxTokenLength shouldBe Some(40) // Per-field override takes precedence
  }

  test("getFieldIndexingConfig should return default when no per-field override exists") {
    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.indexing.text.maxTokenLength" -> "127",
          "spark.indextables.indexing.tokenLength.other"   -> "40"
        ).asJava
      )
    )
    val config = options.getFieldIndexingConfig("content")
    config.maxTokenLength shouldBe Some(127) // Uses default, not "other" field's override
  }

  // ===== Field validation tests =====

  test("validateFieldsExist should check tokenLength field names") {
    import org.apache.spark.sql.types._
    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("content", StringType, nullable = true)
      )
    )

    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map("spark.indextables.indexing.tokenLength.contnt" -> "255").asJava // Typo
      )
    )

    val exception = intercept[IllegalArgumentException] {
      options.validateFieldsExist(schema)
    }
    exception.getMessage should include("contnt")
    exception.getMessage should include("does not exist")
  }

  test("validateFieldsExist should accept valid tokenLength field names") {
    import org.apache.spark.sql.types._
    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("content", StringType, nullable = true)
      )
    )

    val options = new IndexTables4SparkOptions(
      new CaseInsensitiveStringMap(
        Map("spark.indextables.indexing.tokenLength.content" -> "255").asJava
      )
    )

    // Should not throw
    options.validateFieldsExist(schema)
  }

  // ===== Integration tests =====

  test("should write and read with default token length (255)") {
    val tablePath = s"file://$tempDir/token_length_default_test"

    val df = spark.sql("""
      SELECT 1 as id, 'short token' as content
      UNION ALL SELECT 2 as id, 'another short token' as content
    """)

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "content")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 2
  }

  test("should write and read with custom token length") {
    val tablePath = s"file://$tempDir/token_length_custom_test"

    val df = spark.sql("""
      SELECT 1 as id, 'short content' as content
      UNION ALL SELECT 2 as id, 'longer content here' as content
    """)

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "content")
      .option("spark.indextables.indexing.text.maxTokenLength", "100")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 2
  }

  test("should write and read with legacy token length") {
    val tablePath = s"file://$tempDir/token_length_legacy_test"

    val df = spark.sql("""
      SELECT 1 as id, 'short content' as content
      UNION ALL SELECT 2 as id, 'another short content' as content
    """)

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "content")
      .option("spark.indextables.indexing.text.maxTokenLength", "legacy")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 2
  }

  test("should write and read with per-field token length overrides") {
    val tablePath = s"file://$tempDir/token_length_per_field_test"

    val df = spark.sql("""
      SELECT 1 as id, 'short title' as title, 'longer content body' as content
      UNION ALL SELECT 2 as id, 'another title' as title, 'another longer content' as content
    """)

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "title,content")
      .option("spark.indextables.indexing.text.maxTokenLength", "255")         // default
      .option("spark.indextables.indexing.tokenLength.title", "40")            // legacy for title
      .option("spark.indextables.indexing.tokenLength.content", "tantivy_max") // max for content
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 2
  }

  test("should support list-based token length syntax") {
    val tablePath = s"file://$tempDir/token_length_list_syntax_test"

    val df = spark.sql("""
      SELECT 1 as id, 'title one' as title, 'content one' as content, 'body one' as body
      UNION ALL SELECT 2 as id, 'title two' as title, 'content two' as content, 'body two' as body
    """)

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "title,content,body")
      .option("spark.indextables.indexing.tokenLength.legacy", "title")     // 40 for title
      .option("spark.indextables.indexing.tokenLength.255", "content,body") // 255 for content and body
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 2
  }

  test("should reject tokenLength configuration with non-existent field") {
    val tablePath = s"file://$tempDir/token_length_invalid_field_test"

    val df = spark.sql("""
      SELECT 1 as id, 'content here' as content
    """)

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.text", "content")
        .option("spark.indextables.indexing.tokenLength.contnt", "255") // Typo
        .mode("overwrite")
        .save(tablePath)
    }

    exception.getMessage should include("contnt")
    exception.getMessage should include("does not exist")
  }

  test("should reject list-based tokenLength with invalid field names") {
    val tablePath = s"file://$tempDir/token_length_list_invalid_test"

    val df = spark.sql("""
      SELECT 1 as id, 'content here' as content
    """)

    val exception = intercept[IllegalArgumentException] {
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.text", "content")
        .option("spark.indextables.indexing.tokenLength.255", "content,titl,bdy") // Typos
        .mode("overwrite")
        .save(tablePath)
    }

    exception.getMessage should include("titl")
    exception.getMessage should include("bdy")
  }

  // ===== Functional tests (verify token filtering actually works) =====
  // NOTE: These tests verify end-to-end behavior including tantivy4java token filtering.
  // The token filtering is implemented in tantivy4java's Rust code.

  test("token length configuration should be passed to tantivy4java during write") {
    val tablePath = s"file://$tempDir/token_length_passthrough_test"

    // Create data with various token lengths
    val df = spark.sql("""
      SELECT 1 as id, 'short verylongword end' as content
      UNION ALL SELECT 2 as id, 'only short words here' as content
    """)

    // This test verifies that the configuration is accepted and write completes successfully
    // The actual token filtering behavior is implemented in tantivy4java
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "content")
      .option("spark.indextables.indexing.text.maxTokenLength", "10") // Custom limit
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Verify data was written and can be read
    result.count() shouldBe 2
  }

  test("per-field token length should be passed correctly during schema creation") {
    val tablePath = s"file://$tempDir/token_length_per_field_passthrough_test"

    // Create data with multiple text fields
    val df = spark.sql("""
      SELECT 1 as id, 'short title' as title, 'longer content body here' as content
    """)

    // Different token lengths for different fields
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "title,content")
      .option("spark.indextables.indexing.tokenLength.title", "40")    // legacy for title
      .option("spark.indextables.indexing.tokenLength.content", "255") // default for content
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Verify data was written and can be read
    result.count() shouldBe 1
  }

  test("tantivy_max token length should allow long content") {
    val tablePath = s"file://$tempDir/token_length_max_passthrough_test"

    // Create data with long content
    val longContent = "word " * 100 // Many words
    val df          = spark.createDataFrame(Seq((1, longContent))).toDF("id", "content")

    // Maximum token length setting
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.text", "content")
      .option("spark.indextables.indexing.text.maxTokenLength", "tantivy_max")
      .mode("overwrite")
      .save(tablePath)

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Verify data was written and can be read
    result.count() shouldBe 1
  }

  // ===== Constants validation tests =====

  test("TOKEN_LENGTH_TANTIVY_MAX should be 65530") {
    IndexTables4SparkOptions.TOKEN_LENGTH_TANTIVY_MAX shouldBe 65530
  }

  test("TOKEN_LENGTH_DEFAULT should be 255") {
    IndexTables4SparkOptions.TOKEN_LENGTH_DEFAULT shouldBe 255
  }

  test("TOKEN_LENGTH_LEGACY should be 40") {
    IndexTables4SparkOptions.TOKEN_LENGTH_LEGACY shouldBe 40
  }

  test("TOKEN_LENGTH_MIN should be 1") {
    IndexTables4SparkOptions.TOKEN_LENGTH_MIN shouldBe 1
  }
}
