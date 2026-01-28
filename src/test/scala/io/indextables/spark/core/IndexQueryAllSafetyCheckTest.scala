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
import io.indextables.tantivy4java.core.Schema

/**
 * Tests for the _indexall safety check that rejects queries searching too many fields.
 * Uses tantivy4java's SplitQuery.countQueryFields() for accurate field counting.
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

  /**
   * Helper to create a tantivy schema for testing countQueryFields.
   * Creates a schema with text fields: title, content, body, author, tags.
   *
   * Note: Schema.fromDocMappingJson() expects a direct array format, not a wrapped object.
   */
  private def createTestSchema(): Schema = {
    // Direct array format as expected by tantivy4java
    val docMappingJson = """[
      {"name": "title", "type": "text", "tokenizer": "default", "record": "position", "stored": true, "fast": false},
      {"name": "content", "type": "text", "tokenizer": "default", "record": "position", "stored": true, "fast": false},
      {"name": "body", "type": "text", "tokenizer": "default", "record": "position", "stored": true, "fast": false},
      {"name": "author", "type": "text", "tokenizer": "default", "record": "position", "stored": true, "fast": false},
      {"name": "tags", "type": "text", "tokenizer": "default", "record": "position", "stored": true, "fast": false}
    ]"""
    Schema.fromDocMappingJson(docMappingJson)
  }

  // ==================== Unit Tests for countQueryFields ====================

  test("countQueryFields: single qualified field returns 1") {
    val schema = createTestSchema()
    try {
      assert(FiltersToQueryConverter.countQueryFields("title:spark", schema) === 1)
      assert(FiltersToQueryConverter.countQueryFields("content:machine", schema) === 1)
    } finally {
      schema.close()
    }
  }

  test("countQueryFields: multiple qualified fields returns correct count") {
    val schema = createTestSchema()
    try {
      assert(FiltersToQueryConverter.countQueryFields("title:spark OR content:spark", schema) === 2)
      assert(FiltersToQueryConverter.countQueryFields("title:a AND body:b AND author:c", schema) === 3)
    } finally {
      schema.close()
    }
  }

  test("countQueryFields: same field multiple times counts as 1") {
    val schema = createTestSchema()
    try {
      assert(FiltersToQueryConverter.countQueryFields("title:spark AND title:lucene", schema) === 1)
      assert(FiltersToQueryConverter.countQueryFields("content:a OR content:b OR content:c", schema) === 1)
    } finally {
      schema.close()
    }
  }

  test("countQueryFields: unqualified term uses all default text fields") {
    val schema = createTestSchema()
    try {
      // Unqualified search term should search all text fields (5 in our test schema)
      val count = FiltersToQueryConverter.countQueryFields("searchterm", schema)
      assert(count === 5, s"Expected 5 fields for unqualified search, got $count")
    } finally {
      schema.close()
    }
  }

  test("countQueryFields: mixed qualified and unqualified") {
    val schema = createTestSchema()
    try {
      // title:spark is 1 field, but "machine" unqualified adds all 5 text fields
      // The union should be 5 (all text fields include title)
      val count = FiltersToQueryConverter.countQueryFields("title:spark AND machine", schema)
      assert(count === 5, s"Expected 5 fields for mixed query, got $count")
    } finally {
      schema.close()
    }
  }

  test("countQueryFields: qualified phrase returns 1") {
    val schema = createTestSchema()
    try {
      assert(FiltersToQueryConverter.countQueryFields("title:\"apache spark\"", schema) === 1)
      assert(FiltersToQueryConverter.countQueryFields("content:\"machine learning\"", schema) === 1)
    } finally {
      schema.close()
    }
  }

  test("countQueryFields: unqualified phrase uses all default text fields") {
    val schema = createTestSchema()
    try {
      val count = FiltersToQueryConverter.countQueryFields("\"apache spark\"", schema)
      assert(count === 5, s"Expected 5 fields for unqualified phrase, got $count")
    } finally {
      schema.close()
    }
  }

  test("countQueryFields: complex boolean expression") {
    val schema = createTestSchema()
    try {
      // All qualified: (title OR content) AND body = 3 fields
      val count1 = FiltersToQueryConverter.countQueryFields("(title:a OR content:b) AND body:c", schema)
      assert(count1 === 3, s"Expected 3 fields for qualified boolean, got $count1")
    } finally {
      schema.close()
    }
  }

  // ==================== Integration Tests ====================

  test("reject _indexall query when searched fields > limit") {
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

    assert(exception.getMessage.contains("_indexall query would search"))
    assert(exception.getMessage.contains("fields"))
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

    // Should succeed - query is fully qualified (searches only 1 field)
    val result = readDf.filter("_indexall indexquery 'f1:test'").collect()
    assert(result.length === 1)
  }

  test("allow _indexall query when searched fields <= limit") {
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

  // ==================== Mixed Boolean Expression Tests (PR #120 compatibility) ====================

  test("reject mixed boolean with one unqualified _indexall query") {
    val spark = this.spark
    import spark.implicits._

    // Create a table with 15 fields (more than default limit of 10)
    val data = Seq(
      (1, "1234", "community content", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"),
      (2, "5678", "curl content", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n")
    )
    val df = data.toDF("id", "uid", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12", "f13", "f14")

    val tablePath = tempDir.resolve("mixed_boolean_unqualified").toString
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .createOrReplaceTempView("mixed_test")

    // Mixed boolean: one qualified, one unqualified - should reject because 'curl' is unqualified
    // WHERE uid='1234' AND (_indexall indexquery 'f1:community' OR _indexall indexquery 'curl')
    val exception = intercept[Exception] {
      spark.sql("""
        SELECT * FROM mixed_test
        WHERE uid = '1234'
          AND ((_indexall indexquery 'f1:community') OR (_indexall indexquery 'curl'))
      """).collect()
    }

    assert(exception.getMessage.contains("_indexall query would search"))
    assert(exception.getMessage.contains("limit: 10"))
  }

  test("allow mixed boolean with all qualified _indexall queries") {
    val spark = this.spark
    import spark.implicits._

    // Create a table with 15 fields (more than default limit of 10)
    val data = Seq(
      (1, "1234", "community content", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"),
      (2, "5678", "curl content", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n")
    )
    val df = data.toDF("id", "uid", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12", "f13", "f14")

    val tablePath = tempDir.resolve("mixed_boolean_qualified").toString
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.f1", "text")  // text field for tokenized search
      .mode("overwrite")
      .save(tablePath)

    spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .createOrReplaceTempView("mixed_qualified_test")

    // Mixed boolean: all qualified - should succeed
    // WHERE uid='1234' AND (_indexall indexquery 'f1:community' OR _indexall indexquery 'f1:curl')
    val result = spark.sql("""
      SELECT * FROM mixed_qualified_test
      WHERE uid = '1234'
        AND ((_indexall indexquery 'f1:community') OR (_indexall indexquery 'f1:curl'))
    """).collect()

    assert(result.length === 1)
  }

  test("reject complex nested mixed boolean with unqualified _indexall") {
    val spark = this.spark
    import spark.implicits._

    // Create a table with 15 fields (more than default limit of 10)
    val data = Seq(
      (1, "1234", "CRITICAL", "one content", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"),
      (2, "1234", "CLASS2", "two content", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"),
      (3, "5678", "CRITICAL", "one content", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m")
    )
    val df = data.toDF("id", "uid", "urgency", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12", "f13")

    val tablePath = tempDir.resolve("complex_nested_unqualified").toString
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").mode("overwrite").save(tablePath)

    spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .createOrReplaceTempView("complex_nested_test")

    // Complex nested: one qualified in first branch, unqualified in second
    // WHERE uid='1234' AND ((urgency = 'CRITICAL' AND _indexall indexquery 'f1:one')
    //                       OR (urgency <> 'CLASS2' AND _indexall indexquery 'two'))
    val exception = intercept[Exception] {
      spark.sql("""
        SELECT * FROM complex_nested_test
        WHERE uid = '1234'
          AND ((urgency = 'CRITICAL' AND _indexall indexquery 'f1:one')
               OR (urgency <> 'CLASS2' AND _indexall indexquery 'two'))
      """).collect()
    }

    assert(exception.getMessage.contains("_indexall query would search"))
    assert(exception.getMessage.contains("limit: 10"))
  }

  test("allow complex nested mixed boolean with all qualified _indexall") {
    val spark = this.spark
    import spark.implicits._

    // Create a table with 15 fields (more than default limit of 10)
    val data = Seq(
      (1, "1234", "CRITICAL", "one content", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"),
      (2, "1234", "CLASS2", "two content", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"),
      (3, "5678", "CRITICAL", "one content", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"),
      (4, "1234", "LOW", "two content", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m")
    )
    val df = data.toDF("id", "uid", "urgency", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12", "f13")

    val tablePath = tempDir.resolve("complex_nested_qualified").toString
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.f1", "text")  // text field for tokenized search
      .mode("overwrite")
      .save(tablePath)

    spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .createOrReplaceTempView("complex_qualified_test")

    // Complex nested: all _indexall queries are qualified
    // WHERE uid='1234' AND ((urgency = 'CRITICAL' AND _indexall indexquery 'f1:one')
    //                       OR (urgency <> 'CLASS2' AND _indexall indexquery 'f1:two'))
    val result = spark.sql("""
      SELECT * FROM complex_qualified_test
      WHERE uid = '1234'
        AND ((urgency = 'CRITICAL' AND _indexall indexquery 'f1:one')
             OR (urgency <> 'CLASS2' AND _indexall indexquery 'f1:two'))
    """).collect()

    // id=1: uid='1234', urgency='CRITICAL', matches 'f1:one' -> included
    // id=2: uid='1234', urgency='CLASS2', excluded by urgency <> 'CLASS2'
    // id=3: uid='5678', excluded by uid filter
    // id=4: uid='1234', urgency='LOW', matches 'f1:two' -> included
    assert(result.length === 2)
  }
}
