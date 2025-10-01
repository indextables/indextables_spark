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

import io.indextables.spark.TestBase
import org.scalatest.matchers.should.Matchers

class TokenizationTest extends TestBase with Matchers {

  test("string fields should use exact matching (default behavior)") {
    withTempPath { tablePath =>
      val data = spark
        .createDataFrame(
          Seq(
            ("doc1", "machine learning"),
            ("doc2", "deep learning"),
            ("doc3", "machine")
          )
        )
        .toDF("id", "content")

      // Write with default string type (no explicit configuration)
      data.write
        .format("io.indextables.spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read.format("io.indextables.spark.core.Tantivy4SparkTableProvider").load(tablePath)

      // String fields should do exact matching
      val exactMatch = df.filter(df("content") === "machine learning").collect()
      exactMatch should have length 1
      exactMatch(0).getString(0) should be("doc1")

      // Partial matches should not work with string fields
      val partialMatch = df.filter(df("content") === "machine").collect()
      partialMatch should have length 1
      partialMatch(0).getString(0) should be("doc3")
    }
  }

  test("text fields should support indexquery for tokenized matching") {
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

      // Write with explicit text type
      data.write
        .format("io.indextables.spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.typemap.content", "text")
        .save(tablePath)

      val df = spark.read.format("io.indextables.spark.core.Tantivy4SparkTableProvider").load(tablePath)

      // Text fields with === should still do exact matching
      val exactMatch = df.filter(df("content") === "machine learning algorithms").collect()
      exactMatch should have length 1
      exactMatch(0).getString(0) should be("doc1")

      // For tokenized search, users should use indexquery operator
      // Note: This would require indexquery implementation which is separate from field type configuration
    }
  }

  test("mixed field types should work together") {
    withTempPath { tablePath =>
      val data = spark
        .createDataFrame(
          Seq(
            ("doc1", "exact_title", "machine learning algorithms"),
            ("doc2", "another_title", "deep learning networks")
          )
        )
        .toDF("id", "title", "content")

      // Configure title as string (exact) and content as text (tokenized)
      data.write
        .format("io.indextables.spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.typemap.title", "string")
        .option("spark.indextables.indexing.typemap.content", "text")
        .save(tablePath)

      val df = spark.read.format("io.indextables.spark.core.Tantivy4SparkTableProvider").load(tablePath)

      // String field should match exactly
      val exactTitleMatch = df.filter(df("title") === "exact_title").collect()
      exactTitleMatch should have length 1
      exactTitleMatch(0).getString(0) should be("doc1")

      // Text field with === should still do exact matching
      val exactContentMatch = df.filter(df("content") === "machine learning algorithms").collect()
      exactContentMatch should have length 1
      exactContentMatch(0).getString(0) should be("doc1")
    }
  }
}
