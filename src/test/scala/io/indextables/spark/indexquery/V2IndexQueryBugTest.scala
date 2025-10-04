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

package io.indextables.spark.indexquery

import java.util.UUID

import io.indextables.spark.RealS3TestBase

/**
 * Test to demonstrate and validate the IndexQuery bug in V2 DataSource.
 *
 * Bug: The V2IndexQueryExpressionRule checks for "tantivy4spark" in the table class name, but the package was renamed
 * to "indextables", causing IndexQuery expressions to be ignored.
 *
 * Uses real S3 to validate production scenario.
 */
class V2IndexQueryBugTest extends RealS3TestBase {

  // S3 bucket for testing - matches other real S3 tests
  private val S3_BUCKET = "test-tantivy4sparkbucket"
  private val s3Prefix  = s"test-indexquery-bug-${UUID.randomUUID()}"

  test("V2 DataSource should process IndexQuery expressions") {
    val s3Path = s"s3a://$S3_BUCKET/$s3Prefix/test1"

    // Write test data with text field
    val data = Seq(
      ("doc1", "apache spark is awesome"),
      ("doc2", "machine learning with spark"),
      ("doc3", "data engineering pipelines"),
      ("doc4", "spark streaming applications")
    )
    val df = spark.createDataFrame(data).toDF("id", "content")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(s3Path)

    // Read back using V2 DataSource
    val readDF = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(s3Path)

    // Create temp view for SQL testing
    readDF.createOrReplaceTempView("test_docs")

    // Test IndexQuery expression using SQL - should find documents containing "spark"
    val result = spark.sql("SELECT * FROM test_docs WHERE content indexquery 'spark'").collect()

    // Should find 3 documents containing "spark"
    assert(result.length == 3, s"Expected 3 documents with 'spark', got ${result.length}")

    val ids = result.map(_.getString(0)).toSet
    assert(ids == Set("doc1", "doc2", "doc4"), s"Expected doc1, doc2, doc4, got: $ids")
  }

  test("V2 DataSource should process IndexQueryAll expressions") {
    val s3Path = s"s3a://$S3_BUCKET/$s3Prefix/test2"

    // Write test data
    val data = Seq(
      ("doc1", "apache spark", "distributed"),
      ("doc2", "machine learning", "spark"),
      ("doc3", "data engineering", "pipelines"),
      ("doc4", "streaming", "spark applications")
    )
    val df = spark.createDataFrame(data).toDF("id", "content", "tags")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.typemap.tags", "text")
      .mode("overwrite")
      .save(s3Path)

    // Read back using V2 DataSource
    val readDF = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(s3Path)

    // Create temp view for SQL testing
    readDF.createOrReplaceTempView("test_docs_all")

    // Test IndexQueryAll - should search across all fields
    val result = spark.sql("SELECT * FROM test_docs_all WHERE _indexall indexquery 'spark'").collect()

    // Should find 3 documents containing "spark" in any field
    assert(result.length == 3, s"Expected 3 documents with 'spark', got ${result.length}")

    val ids = result.map(_.getString(0)).toSet
    assert(ids == Set("doc1", "doc2", "doc4"), s"Expected doc1, doc2, doc4, got: $ids")
  }

  test("V2 DataSource IndexQuery should work with complex boolean queries") {
    val s3Path = s"s3a://$S3_BUCKET/$s3Prefix/test3"

    // Write test data
    val data = Seq(
      ("doc1", "apache spark and hadoop"),
      ("doc2", "spark streaming with kafka"),
      ("doc3", "machine learning pipelines"),
      ("doc4", "spark sql optimization"),
      ("doc5", "data processing with flink")
    )
    val df = spark.createDataFrame(data).toDF("id", "content")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(s3Path)

    // Read back using V2 DataSource
    val readDF = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(s3Path)

    // Create temp view for SQL testing
    readDF.createOrReplaceTempView("test_docs_complex")

    // Test complex boolean query
    val result =
      spark.sql("SELECT * FROM test_docs_complex WHERE content indexquery 'spark AND (streaming OR sql)'").collect()

    // Should find doc2 (spark AND streaming) and doc4 (spark AND sql)
    assert(result.length == 2, s"Expected 2 documents, got ${result.length}")

    val ids = result.map(_.getString(0)).toSet
    assert(ids == Set("doc2", "doc4"), s"Expected doc2, doc4, got: $ids")
  }
}
