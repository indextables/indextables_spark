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

import io.indextables.spark.TestBase

/**
 * Test that IndexQuery with non-existent field throws a descriptive error.
 *
 * Previously, the system would silently ignore the predicate and return all documents, which led to incorrect results
 * without any indication of the problem.
 */
class IndexQueryNonExistentFieldTest extends TestBase {

  test("IndexQuery with non-existent field should throw descriptive error") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/indexquery_nonexistent_field_test"

    // Create test data with known fields
    val testData = Seq(
      (1, "machine learning algorithms", "tech"),
      (2, "data engineering pipeline", "tech"),
      (3, "web development", "tech")
    ).toDF("id", "title", "category")

    // Write with text field for title
    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.typemap.category", "string")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("test_table")

    // Test: IndexQuery with a field that doesn't exist should throw an error
    // Validation happens on driver before tasks are dispatched (PR #114 + #122 pattern)
    val exception = intercept[IllegalArgumentException] {
      spark
        .sql("""
        SELECT id, title FROM test_table
        WHERE fake_field indexquery 'something'
      """)
        .collect()
    }

    // Verify the error message is descriptive
    val errorMessage = exception.getMessage

    assert(
      errorMessage.contains("fake_field") && errorMessage.contains("non-existent"),
      s"Error message should mention the non-existent field. Actual message: $errorMessage"
    )

    // Verify available fields are listed in the error
    assert(
      errorMessage.contains("Available fields"),
      s"Error message should list available fields. Actual message: $errorMessage"
    )
  }

  test("IndexQuery with existing field should work normally") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/indexquery_existing_field_test"

    val testData = Seq(
      (1, "machine learning algorithms", "tech"),
      (2, "data engineering pipeline", "tech"),
      (3, "web development", "tech")
    ).toDF("id", "title", "category")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.typemap.category", "string")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("test_table_existing")

    // This should work fine - "title" is a valid field
    val results = spark
      .sql("""
        SELECT id, title FROM test_table_existing
        WHERE title indexquery 'machine'
      """)
      .collect()

    assert(results.length == 1, s"Expected 1 result, got ${results.length}")
    assert(results(0).getInt(0) == 1, s"Expected id=1, got ${results(0).getInt(0)}")
  }

  test("IndexQuery error message should include available fields") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/indexquery_error_message_test"

    val testData = Seq(
      (1, "test content", "category1", 100.0)
    ).toDF("id", "content", "category", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("test_table_error_msg")

    // Validation happens on driver before tasks are dispatched (PR #114 + #122 pattern)
    val exception = intercept[IllegalArgumentException] {
      spark
        .sql("""
        SELECT * FROM test_table_error_msg
        WHERE nonexistent_column indexquery 'test'
      """)
        .collect()
    }

    val errorMessage = exception.getMessage

    // The error should mention available fields to help the user
    assert(
      errorMessage.contains("Available fields"),
      s"Error should mention available fields. Got: $errorMessage"
    )
  }

  test("Simple SELECT with IndexQuery on non-existent field should throw error") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/indexquery_simple_select_test"

    val testData = Seq(
      (1, "machine learning", 100.0),
      (2, "data science", 200.0),
      (3, "deep learning", 150.0)
    ).toDF("id", "title", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("simple_select_test")

    // Simple SELECT with non-existent field should fail
    // Validation happens on driver before tasks are dispatched (PR #114 + #122 pattern)
    val exception = intercept[IllegalArgumentException] {
      spark
        .sql("""
        SELECT id, title FROM simple_select_test
        WHERE bogus_field indexquery 'learning'
      """)
        .collect()
    }

    val errorMessage = exception.getMessage
    assert(
      errorMessage.contains("bogus_field") && errorMessage.contains("non-existent"),
      s"Error should mention the non-existent field 'bogus_field'. Got: $errorMessage"
    )
  }

  test("COUNT aggregation with IndexQuery on non-existent field should throw error") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/indexquery_count_agg_test"

    val testData = Seq(
      (1, "machine learning", 100.0),
      (2, "data science", 200.0),
      (3, "deep learning", 150.0)
    ).toDF("id", "title", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("count_agg_test")

    // COUNT with IndexQuery on non-existent field should fail
    // Can throw either SparkException or IllegalArgumentException depending on execution path
    val exception = intercept[Exception] {
      spark
        .sql("""
        SELECT COUNT(*) FROM count_agg_test
        WHERE invalid_column indexquery 'learning'
      """)
        .collect()
    }

    val fullErrorMessage = getFullErrorMessage(exception)
    assert(
      fullErrorMessage.contains("invalid_column") && fullErrorMessage.contains("non-existent"),
      s"Error should mention the non-existent field 'invalid_column'. Got: $fullErrorMessage"
    )
  }

  test("SUM aggregation with IndexQuery on non-existent field should throw error") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/indexquery_sum_agg_test"

    val testData = Seq(
      (1, "machine learning", 100.0),
      (2, "data science", 200.0),
      (3, "deep learning", 150.0)
    ).toDF("id", "title", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("sum_agg_test")

    // SUM with IndexQuery on non-existent field should fail
    // Can throw either SparkException or IllegalArgumentException depending on execution path
    val exception = intercept[Exception] {
      spark
        .sql("""
        SELECT SUM(score) FROM sum_agg_test
        WHERE does_not_exist indexquery 'machine'
      """)
        .collect()
    }

    val fullErrorMessage = getFullErrorMessage(exception)
    assert(
      fullErrorMessage.contains("does_not_exist") && fullErrorMessage.contains("non-existent"),
      s"Error should mention the non-existent field 'does_not_exist'. Got: $fullErrorMessage"
    )
  }

  test("COUNT aggregation with IndexQuery on existing field should work") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/indexquery_count_existing_test"

    val testData = Seq(
      (1, "machine learning algorithms", 100.0),
      (2, "data science pipeline", 200.0),
      (3, "deep learning neural networks", 150.0)
    ).toDF("id", "title", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("count_existing_test")

    // COUNT with IndexQuery on valid field should work
    val results = spark
      .sql("""
        SELECT COUNT(*) FROM count_existing_test
        WHERE title indexquery 'learning'
      """)
      .collect()

    // "learning" appears in "machine learning algorithms" and "deep learning neural networks"
    assert(results.length == 1, s"Expected 1 row, got ${results.length}")
    assert(results(0).getLong(0) == 2, s"Expected count of 2, got ${results(0).getLong(0)}")
  }

  test("SUM aggregation with IndexQuery on existing field should work") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/indexquery_sum_existing_test"

    val testData = Seq(
      (1, "machine learning algorithms", 100.0),
      (2, "data science pipeline", 200.0),
      (3, "deep learning neural networks", 150.0)
    ).toDF("id", "title", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("sum_existing_test")

    // SUM with IndexQuery on valid field should work
    val results = spark
      .sql("""
        SELECT SUM(score) FROM sum_existing_test
        WHERE title indexquery 'learning'
      """)
      .collect()

    // "learning" appears in rows with score 100.0 and 150.0
    assert(results.length == 1, s"Expected 1 row, got ${results.length}")
    assert(results(0).getDouble(0) == 250.0, s"Expected sum of 250.0, got ${results(0).getDouble(0)}")
  }

  test("GROUP BY with IndexQuery on non-existent field should throw error") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/indexquery_groupby_nonexistent_test"

    val testData = Seq(
      (1, "machine learning", "tech", 100.0),
      (2, "data science", "tech", 200.0),
      (3, "deep learning", "ai", 150.0),
      (4, "neural networks", "ai", 175.0)
    ).toDF("id", "title", "category", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.typemap.category", "string")
      .option("spark.indextables.indexing.fastfields", "score,category") // category needed for GROUP BY
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("groupby_nonexistent_test")

    // GROUP BY with IndexQuery on non-existent field should fail
    // Can throw either SparkException or IllegalArgumentException depending on execution path
    val exception = intercept[Exception] {
      spark
        .sql("""
        SELECT category, COUNT(*), SUM(score)
        FROM groupby_nonexistent_test
        WHERE nonexistent_field indexquery 'learning'
        GROUP BY category
      """)
        .collect()
    }

    val fullErrorMessage = getFullErrorMessage(exception)
    assert(
      fullErrorMessage.contains("nonexistent_field") && fullErrorMessage.contains("non-existent"),
      s"Error should mention the non-existent field 'nonexistent_field'. Got: $fullErrorMessage"
    )
  }

  test("GROUP BY with IndexQuery on existing field should work") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/indexquery_groupby_existing_test"

    val testData = Seq(
      (1, "machine learning algorithms", "tech", 100.0),
      (2, "data science pipeline", "tech", 200.0),
      (3, "deep learning neural networks", "ai", 150.0),
      (4, "neural networks training", "ai", 175.0)
    ).toDF("id", "title", "category", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.typemap.category", "string")
      .option("spark.indextables.indexing.fastfields", "score,category") // category needed for GROUP BY
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("groupby_existing_test")

    // GROUP BY with IndexQuery on valid field should work
    // "learning" appears in "machine learning algorithms" (tech) and "deep learning neural networks" (ai)
    val results = spark
      .sql("""
        SELECT category, COUNT(*) as cnt, SUM(score) as total
        FROM groupby_existing_test
        WHERE title indexquery 'learning'
        GROUP BY category
        ORDER BY category
      """)
      .collect()

    assert(results.length == 2, s"Expected 2 categories, got ${results.length}")

    // ai: 1 doc (deep learning neural networks), score 150.0
    // tech: 1 doc (machine learning algorithms), score 100.0
    val aiRow   = results.find(_.getString(0) == "ai").get
    val techRow = results.find(_.getString(0) == "tech").get

    assert(aiRow.getLong(1) == 1, s"Expected ai count 1, got ${aiRow.getLong(1)}")
    assert(aiRow.getDouble(2) == 150.0, s"Expected ai sum 150.0, got ${aiRow.getDouble(2)}")
    assert(techRow.getLong(1) == 1, s"Expected tech count 1, got ${techRow.getLong(1)}")
    assert(techRow.getDouble(2) == 100.0, s"Expected tech sum 100.0, got ${techRow.getDouble(2)}")
  }

  test("Complex aggregation (AVG, MAX, MIN) with IndexQuery on non-existent field should throw error") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/indexquery_complex_agg_nonexistent_test"

    val testData = Seq(
      (1, "machine learning", 100.0),
      (2, "data science", 200.0),
      (3, "deep learning", 150.0)
    ).toDF("id", "title", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("complex_agg_nonexistent_test")

    // Complex aggregation with IndexQuery on non-existent field should fail
    // Can throw either SparkException or IllegalArgumentException depending on execution path
    val exception = intercept[Exception] {
      spark
        .sql("""
        SELECT AVG(score), MAX(score), MIN(score)
        FROM complex_agg_nonexistent_test
        WHERE missing_column indexquery 'learning'
      """)
        .collect()
    }

    val fullErrorMessage = getFullErrorMessage(exception)
    assert(
      fullErrorMessage.contains("missing_column") && fullErrorMessage.contains("non-existent"),
      s"Error should mention the non-existent field 'missing_column'. Got: $fullErrorMessage"
    )
  }

  test("Complex aggregation with IndexQuery on existing field should work") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/indexquery_complex_agg_existing_test"

    val testData = Seq(
      (1, "machine learning algorithms", 100.0),
      (2, "data science pipeline", 200.0),
      (3, "deep learning neural networks", 150.0)
    ).toDF("id", "title", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("complex_agg_existing_test")

    // Complex aggregation with IndexQuery on valid field should work
    // "learning" matches rows with scores 100.0 and 150.0
    val results = spark
      .sql("""
        SELECT AVG(score), MAX(score), MIN(score)
        FROM complex_agg_existing_test
        WHERE title indexquery 'learning'
      """)
      .collect()

    assert(results.length == 1, s"Expected 1 row, got ${results.length}")
    assert(results(0).getDouble(0) == 125.0, s"Expected avg 125.0, got ${results(0).getDouble(0)}")
    assert(results(0).getDouble(1) == 150.0, s"Expected max 150.0, got ${results(0).getDouble(1)}")
    assert(results(0).getDouble(2) == 100.0, s"Expected min 100.0, got ${results(0).getDouble(2)}")
  }

  private def getFullErrorMessage(e: Throwable): String = {
    val messages           = scala.collection.mutable.ArrayBuffer[String]()
    var current: Throwable = e
    while (current != null) {
      if (current.getMessage != null) {
        messages += current.getMessage
      }
      current = current.getCause
    }
    messages.mkString(" -> ")
  }
}
