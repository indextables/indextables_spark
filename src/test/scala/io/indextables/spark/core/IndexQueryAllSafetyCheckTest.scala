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

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import io.indextables.spark.config.IndexTables4SparkSQLConf

/**
 * Tests for the _indexall safety check that rejects unqualified queries on wide tables.
 */
class IndexQueryAllSafetyCheckTest extends AnyFunSuite with BeforeAndAfterAll {

  @transient private var _spark: SparkSession = _
  private var tempDir: java.nio.file.Path = _

  def spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    super.beforeAll()

    _spark = SparkSession.builder()
      .master("local[2]")
      .appName("IndexQueryAllSafetyCheckTest")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

    tempDir = Files.createTempDirectory("indexall_safety_test")
  }

  override def afterAll(): Unit = {
    if (_spark != null) {
      _spark.stop()
    }
    if (tempDir != null) {
      deleteRecursively(tempDir.toFile)
    }
    super.afterAll()
  }

  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  // ==================== Unit Tests for containsUnqualifiedTerms ====================

  test("containsUnqualifiedTerms: detects simple unqualified term") {
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("spark") === true)
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("machine") === true)
  }

  test("containsUnqualifiedTerms: allows fully qualified query") {
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("url:spark") === false)
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("title:machine") === false)
  }

  test("containsUnqualifiedTerms: detects mixed qualified and unqualified") {
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("url:spark AND machine") === true)
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("title:test OR content:demo AND unqualified") === true)
  }

  test("containsUnqualifiedTerms: allows multiple qualified terms with OR") {
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("url:spark OR content:spark") === false)
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("field1:term1 AND field2:term2") === false)
  }

  test("containsUnqualifiedTerms: detects unqualified phrase") {
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("\"apache spark\"") === true)
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("\"machine learning\"") === true)
  }

  test("containsUnqualifiedTerms: allows qualified phrase") {
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("title:\"apache spark\"") === false)
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("content:\"machine learning\"") === false)
  }

  test("containsUnqualifiedTerms: handles complex boolean expressions") {
    // All qualified
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("(url:a OR url:b) AND title:c") === false)
    // One unqualified
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("(url:a OR url:b) AND term") === true)
  }

  test("containsUnqualifiedTerms: handles nested field references") {
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("metadata.name:value") === false)
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("user.profile.name:test") === false)
  }

  test("containsUnqualifiedTerms: handles range queries") {
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("timestamp:[2024-01-01 TO 2024-12-31]") === false)
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("score:[10 TO 100]") === false)
  }

  test("containsUnqualifiedTerms: handles grouped terms") {
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("field:(term1 term2 term3)") === false)
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("title:(apache spark tantivy)") === false)
  }

  test("containsUnqualifiedTerms: boolean keywords alone are not unqualified") {
    // Just keywords should not trigger as unqualified
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("field1:a AND field2:b") === false)
    assert(FiltersToQueryConverter.containsUnqualifiedTerms("field1:a OR field2:b NOT field3:c") === false)
  }

  // ==================== Integration Tests ====================

  test("reject unqualified _indexall query when fields > limit") {
    val spark = this.spark
    import spark.implicits._

    // Create a table with 15 fields (more than default limit of 10)
    val data = Seq((1, "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"))
    val df = data.toDF("id", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12", "f13", "f14", "f15")

    val tablePath = tempDir.resolve("wide_table_reject").toString
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    val readDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

    // Should throw IllegalArgumentException with helpful message
    val exception = intercept[Exception] {
      readDf.filter("_indexall indexquery 'test'").collect()
    }

    assert(exception.getMessage.contains("Unqualified _indexall query"))
    assert(exception.getMessage.contains("16 fields"))
    assert(exception.getMessage.contains("limit: 10"))
    assert(exception.getMessage.contains("To fix, qualify your search"))
  }

  test("allow qualified _indexall query on wide table") {
    val spark = this.spark
    import spark.implicits._

    // Create a table with 15 fields
    val data = Seq((1, "test", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"))
    val df = data.toDF("id", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12", "f13", "f14", "f15")

    val tablePath = tempDir.resolve("wide_table_qualified").toString
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    val readDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

    // Should succeed - query is fully qualified
    val result = readDf.filter("_indexall indexquery 'f1:test'").collect()
    assert(result.length === 1)
  }

  test("allow unqualified _indexall query when fields <= limit") {
    val spark = this.spark
    import spark.implicits._

    // Create a table with 5 fields (below default limit of 10)
    val data = Seq((1, "test", "value", "data", "info"))
    val df = data.toDF("id", "f1", "f2", "f3", "f4")

    val tablePath = tempDir.resolve("narrow_table").toString
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    val readDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

    // Should succeed - table has only 5 fields, below limit of 10
    val result = readDf.filter("_indexall indexquery 'test'").collect()
    assert(result.length === 1)
  }

  test("respect custom maxUnqualifiedFields config") {
    val spark = this.spark
    import spark.implicits._

    // Create a table with 15 fields
    val data = Seq((1, "test", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"))
    val df = data.toDF("id", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12", "f13", "f14", "f15")

    val tablePath = tempDir.resolve("wide_table_custom_config").toString
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Set higher limit via read option
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option(IndexTables4SparkSQLConf.TANTIVY4SPARK_INDEXALL_MAX_UNQUALIFIED_FIELDS, "20")
      .load(tablePath)

    // Should succeed - custom limit of 20 is above 16 fields
    val result = readDf.filter("_indexall indexquery 'test'").collect()
    assert(result.length === 1)
  }

  test("allow when maxUnqualifiedFields is 0 (disabled)") {
    val spark = this.spark
    import spark.implicits._

    // Create a table with 15 fields
    val data = Seq((1, "test", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"))
    val df = data.toDF("id", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12", "f13", "f14", "f15")

    val tablePath = tempDir.resolve("wide_table_disabled").toString
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    // Disable the check by setting limit to 0
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option(IndexTables4SparkSQLConf.TANTIVY4SPARK_INDEXALL_MAX_UNQUALIFIED_FIELDS, "0")
      .load(tablePath)

    // Should succeed - check is disabled
    val result = readDf.filter("_indexall indexquery 'test'").collect()
    assert(result.length === 1)
  }

  test("error message includes field names and suggestions") {
    val spark = this.spark
    import spark.implicits._

    // Create a table with specific field names
    val data = Seq((1, "data", "data", "data"))
    val df = data.toDF("id", "title", "content", "description")
      .withColumn("author", org.apache.spark.sql.functions.lit("writer"))
      .withColumn("category", org.apache.spark.sql.functions.lit("blog"))
      .withColumn("tags", org.apache.spark.sql.functions.lit("tag1"))
      .withColumn("status", org.apache.spark.sql.functions.lit("active"))
      .withColumn("priority", org.apache.spark.sql.functions.lit("high"))
      .withColumn("created", org.apache.spark.sql.functions.lit("2024-01-01"))
      .withColumn("updated", org.apache.spark.sql.functions.lit("2024-01-02"))
      .withColumn("extra1", org.apache.spark.sql.functions.lit("x"))

    val tablePath = tempDir.resolve("table_with_named_fields").toString
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    val readDf = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

    val exception = intercept[Exception] {
      readDf.filter("_indexall indexquery 'search'").collect()
    }

    val msg = exception.getMessage
    // Should include helpful suggestions
    assert(msg.contains("spark.conf.set"))
    assert(msg.contains("spark.indextables.indexquery.indexall.maxUnqualifiedFields"))
    assert(msg.contains("Available fields:"))
  }
}
