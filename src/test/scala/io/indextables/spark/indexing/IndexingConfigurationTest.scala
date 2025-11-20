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

package io.indextables.spark.indexing

import org.apache.spark.sql.Row

import io.indextables.spark.TestBase
import org.scalatest.matchers.should.Matchers

class IndexingConfigurationTest extends TestBase with Matchers {

  test("default field types should be string instead of text") {
    withTempPath { tablePath =>
      // Create DataFrame with string fields
      val data = spark
        .createDataFrame(
          Seq(
            ("doc1", "content one"),
            ("doc2", "content two")
          )
        )
        .toDF("id", "content")

      // Write without any field type configuration - should default to string
      data.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

      // Read back and verify we can query with exact matching (string behavior)
      val df      = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val results = df.filter(df("content") === "content one").collect()

      results should have length 1
      results(0).getString(0) should be("doc1")
    }
  }

  test("text field type configuration with tokenized queries") {
    withTempPath { tablePath =>
      val data = spark
        .createDataFrame(
          Seq(
            ("doc1", "machine learning algorithms"),
            ("doc2", "deep learning networks"),
            ("doc3", "machine vision systems")
          )
        )
        .toDF("id", "content")

      // Configure content field as text type
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.typemap.content", "text")
        .save(tablePath)

      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // Text fields with === should still use exact matching (not tokenized)
      // For exact phrase search, the full phrase must match exactly
      val exactResults = df.filter(df("content") === "machine learning algorithms").collect()
      exactResults should have length 1
      exactResults(0).getString(0) should be("doc1")

      // Partial phrase search with === should return no results (exact matching)
      val partialResults = df.filter(df("content") === "machine learning").collect()
      println(s"🔍 DEBUG: Partial results for 'machine learning': ${partialResults.length}")
      partialResults.foreach(row => println(s"  - Found: ${row.getString(0)} -> '${row.getString(1)}'"))
      partialResults should have length 0
    }
  }

  test("fast fields configuration") {
    withTempPath { tablePath =>
      val data = spark
        .createDataFrame(
          Seq(
            ("doc1", "content1", 100),
            ("doc2", "content2", 200)
          )
        )
        .toDF("id", "content", "score")

      // Configure score as fast field
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.fastfields", "score")
        .save(tablePath)

      val df      = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val results = df.filter(df("score") > 150).collect()

      results should have length 1
      results(0).getString(0) should be("doc2")
    }
  }

  test("store-only fields configuration") {
    withTempPath { tablePath =>
      val data = spark
        .createDataFrame(
          Seq(
            ("doc1", "searchable content", "metadata only"),
            ("doc2", "more content", "more metadata")
          )
        )
        .toDF("id", "content", "metadata")

      // Configure metadata as store-only (not indexed)
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.storeonlyfields", "metadata")
        .save(tablePath)

      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // Should be able to filter on indexed content field
      val contentResults = df.filter(df("content") === "searchable content").collect()
      contentResults should have length 1

      // Store-only fields should still be readable in results
      contentResults(0).getString(2) should be("metadata only")
    }
  }

  test("index-only fields configuration") {
    withTempPath { tablePath =>
      val data = spark
        .createDataFrame(
          Seq(
            ("doc1", "searchable", "visible content"),
            ("doc2", "findable", "more visible content")
          )
        )
        .toDF("id", "searchField", "displayField")

      // Configure searchField as index-only (not stored)
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.indexonlyfields", "searchField")
        .save(tablePath)

      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // Should be able to filter on indexed field
      val results = df.filter(df("searchField") === "searchable").collect()
      results should have length 1

      // Display field should be available
      results(0).getString(2) should be("visible content")
    }
  }

  test("custom tokenizer configuration") {
    withTempPath { tablePath =>
      val data = spark
        .createDataFrame(
          Seq(
            ("doc1", "test@example.com"),
            ("doc2", "user@domain.org")
          )
        )
        .toDF("id", "email")

      // Configure email field with text type and custom tokenizer
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.typemap.email", "text")
        .option("spark.indextables.indexing.tokenizer.email", "whitespace")
        .save(tablePath)

      val df      = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      val results = df.filter(df("email") === "test@example.com").collect()

      results should have length 1
      results(0).getString(0) should be("doc1")
    }
  }

  test("json field type configuration") {
    withTempPath { tablePath =>
      val data = spark
        .createDataFrame(
          Seq(
            ("doc1", """{"name": "john", "age": 30}"""),
            ("doc2", """{"name": "jane", "age": 25}""")
          )
        )
        .toDF("id", "jsonData")

      // Configure jsonData as json type - this parses and indexes the JSON structure
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.typemap.jsonData", "json")
        .save(tablePath)

      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // JSON fields cannot be queried with exact string matching - they are indexed as structured data
      // Instead, we verify that the data can be read back correctly
      val results = df.collect()
      results should have length 2

      // Verify the JSON field can be read back with correct data
      // Note: JSON field order may change during serialization/deserialization
      val doc1     = results.find(_.getString(0) == "doc1").get
      val doc1Json = doc1.getString(1)
      doc1Json should (include("john") and include("30"))

      val doc2     = results.find(_.getString(0) == "doc2").get
      val doc2Json = doc2.getString(1)
      doc2Json should (include("jane") and include("25"))

      // Note: To query nested fields within JSON, use IndexQuery like:
      // df.filter("_indexall indexquery 'jsonData.name:john'")
    }
  }

  test("configuration validation against existing table") {
    withTempPath { tablePath =>
      val data = spark
        .createDataFrame(
          Seq(
            ("doc1", "content1")
          )
        )
        .toDF("id", "content")

      // First write with text configuration
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.typemap.content", "text")
        .save(tablePath)

      // Second write with conflicting string configuration should fail
      val moreData = spark
        .createDataFrame(
          Seq(
            ("doc2", "content2")
          )
        )
        .toDF("id", "content")

      assertThrows[IllegalArgumentException] {
        moreData.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("append")
          .mode("append")
          .option("spark.indextables.indexing.typemap.content", "string")
          .save(tablePath)
      }
    }
  }

  test("mixed field type configuration") {
    withTempPath { tablePath =>
      val data = spark
        .createDataFrame(
          Seq(
            ("doc1", "exact match", "tokenized content here", 100),
            ("doc2", "another exact", "more tokenized words", 200)
          )
        )
        .toDF("id", "exactField", "textField", "numField")

      // Configure mixed field types
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.typemap.exactField", "string")
        .option("spark.indextables.indexing.typemap.textField", "text")
        .option("spark.indextables.indexing.fastfields", "numField")
        .save(tablePath)

      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // String field should match exactly
      val exactResults = df.filter(df("exactField") === "exact match").collect()
      exactResults should have length 1

      // Text field with === should also use exact matching (not tokenized)
      val textResults = df.filter(df("textField") === "tokenized content here").collect()
      textResults should have length 1

      // Fast field should work for range queries
      val numResults = df.filter(df("numField") > 150).collect()
      numResults should have length 1
    }
  }
}
