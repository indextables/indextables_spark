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

package io.indextables.spark.sql

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/** Tests for CHECKPOINT INDEXTABLES command. */
class CheckpointCommandTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tempDir: File       = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .appName("CheckpointCommandTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    tempDir = Files.createTempDirectory("checkpoint-cmd-test").toFile
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir)
    }
    super.afterAll()
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  private def createTestTable(tablePath: String): Unit = {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("value", StringType, nullable = false)
      )
    )
    val data = Seq(Row(1, "one"), Row(2, "two"), Row(3, "three"))
    val df   = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)
  }

  // ===== SQL Parsing Tests =====

  test("CHECKPOINT INDEXTABLES should parse with path string") {
    val tablePath = new File(tempDir, "parse_test_table").getAbsolutePath
    createTestTable(tablePath)

    val result = spark.sql(s"CHECKPOINT INDEXTABLES '$tablePath'")

    // Verify schema
    val columns = result.columns.toSet
    columns should contain("table_path")
    columns should contain("status")
    columns should contain("checkpoint_version")
    columns should contain("num_actions")
    columns should contain("num_files")
    columns should contain("protocol_version")
    columns should contain("is_multi_part")

    // Should return exactly one row
    result.count() shouldBe 1
  }

  test("CHECKPOINT TANTIVY4SPARK should parse with alternate keyword") {
    val tablePath = new File(tempDir, "tantivy_keyword_test").getAbsolutePath
    createTestTable(tablePath)

    val result = spark.sql(s"CHECKPOINT TANTIVY4SPARK '$tablePath'")

    result.columns should contain("table_path")
    result.count() shouldBe 1
  }

  test("checkpoint indextables should be case insensitive") {
    val tablePath = new File(tempDir, "case_insensitive_test").getAbsolutePath
    createTestTable(tablePath)

    val result = spark.sql(s"checkpoint indextables '$tablePath'")

    result.columns should contain("status")
    result.count() shouldBe 1
  }

  test("COMPACT INDEXTABLES should work as alias for CHECKPOINT") {
    val tablePath = new File(tempDir, "compact_alias_test").getAbsolutePath
    createTestTable(tablePath)

    val result = spark.sql(s"COMPACT INDEXTABLES '$tablePath'")

    result.columns should contain("table_path")
    result.columns should contain("status")
    result.count() shouldBe 1

    val row = result.collect().head
    row.getString(1) shouldBe "SUCCESS"
    row.getLong(5) shouldBe 4L // protocol_version (V4 with Avro state)
  }

  test("COMPACT TANTIVY4SPARK should work as alias for CHECKPOINT") {
    val tablePath = new File(tempDir, "compact_tantivy_alias_test").getAbsolutePath
    createTestTable(tablePath)

    val result = spark.sql(s"COMPACT TANTIVY4SPARK '$tablePath'")

    result.columns should contain("status")
    result.count() shouldBe 1
    result.collect().head.getString(1) shouldBe "SUCCESS"
  }

  // ===== Functional Tests =====

  test("CHECKPOINT INDEXTABLES should create checkpoint and return SUCCESS") {
    val tablePath = new File(tempDir, "success_test_table").getAbsolutePath
    createTestTable(tablePath)

    val result = spark.sql(s"CHECKPOINT INDEXTABLES '$tablePath'")
    val rows   = result.collect()

    rows.length shouldBe 1
    val row = rows.head

    row.getString(1) shouldBe "SUCCESS" // status column
    row.getLong(2) should be >= 0L      // checkpoint_version
    row.getLong(3) should be >= 1L      // num_actions (at least protocol + metadata + 1 file)
    row.getLong(4) should be >= 1L      // num_files
    row.getLong(5) shouldBe 4L          // protocol_version (V4 with Avro state)
  }

  test("CHECKPOINT INDEXTABLES should upgrade protocol to V4") {
    val tablePath = new File(tempDir, "v4_upgrade_test").getAbsolutePath
    createTestTable(tablePath)

    val result = spark.sql(s"CHECKPOINT INDEXTABLES '$tablePath'")
    val row    = result.collect().head

    // Protocol version should be 4 (V4 with Avro state)
    row.getLong(5) shouldBe 4L
  }

  test("CHECKPOINT INDEXTABLES should return ERROR for non-existent table") {
    val fakePath = new File(tempDir, "does_not_exist").getAbsolutePath

    val result = spark.sql(s"CHECKPOINT INDEXTABLES '$fakePath'")
    val rows   = result.collect()

    rows.length shouldBe 1
    val status = rows.head.getString(1)
    status should startWith("ERROR")
  }

  test("CHECKPOINT INDEXTABLES can be run multiple times on same table") {
    val tablePath = new File(tempDir, "multiple_checkpoint_test").getAbsolutePath
    createTestTable(tablePath)

    // First checkpoint
    val result1 = spark.sql(s"CHECKPOINT INDEXTABLES '$tablePath'")
    result1.collect().head.getString(1) shouldBe "SUCCESS"

    // Second checkpoint
    val result2 = spark.sql(s"CHECKPOINT INDEXTABLES '$tablePath'")
    result2.collect().head.getString(1) shouldBe "SUCCESS"
  }

  // ===== Schema Tests =====

  test("CHECKPOINT INDEXTABLES should have correct column types") {
    val tablePath = new File(tempDir, "schema_test_table").getAbsolutePath
    createTestTable(tablePath)

    val result = spark.sql(s"CHECKPOINT INDEXTABLES '$tablePath'")
    val schema = result.schema

    schema("table_path").dataType.typeName shouldBe "string"
    schema("status").dataType.typeName shouldBe "string"
    schema("checkpoint_version").dataType.typeName shouldBe "long"
    schema("num_actions").dataType.typeName shouldBe "long"
    schema("num_files").dataType.typeName shouldBe "long"
    schema("protocol_version").dataType.typeName shouldBe "long"
    schema("is_multi_part").dataType.typeName shouldBe "boolean"
  }

  test("CHECKPOINT INDEXTABLES result should be queryable") {
    val tablePath = new File(tempDir, "queryable_test").getAbsolutePath
    createTestTable(tablePath)

    val result = spark.sql(s"CHECKPOINT INDEXTABLES '$tablePath'")

    // Register as temp view and query
    result.createOrReplaceTempView("checkpoint_result")

    val successCount = spark.sql("SELECT * FROM checkpoint_result WHERE status = 'SUCCESS'").count()
    successCount shouldBe 1

    val v4Count = spark.sql("SELECT * FROM checkpoint_result WHERE protocol_version = 4").count()
    v4Count shouldBe 1
  }
}
