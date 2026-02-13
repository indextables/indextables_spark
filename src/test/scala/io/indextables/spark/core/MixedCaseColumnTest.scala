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

import org.apache.spark.sql.functions._

import io.indextables.spark.TestBase
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * Test that mixed-case column names work correctly in SQL query predicates.
 * Reproduces issue where field names get lowercased and tantivy can't find them.
 */
class MixedCaseColumnTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  test("Mixed-case columns: SQL WHERE clause on regular write path") {
    withTempPath { path =>
      val ss = spark; import ss.implicits._

      // Write data with mixed-case column names
      Seq(
        (1, "alice", "Engineering"),
        (2, "bob", "Marketing"),
        (3, "charlie", "Engineering")
      ).toDF("userId", "userName", "deptName")
        .write
        .format(provider)
        .mode("overwrite")
        .save(path)

      // Read and register as temp view
      val df = spark.read.format(provider)
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(path)
      df.createOrReplaceTempView("mixed_case_regular")

      // Verify schema preserves original case
      val fieldNames = df.schema.fieldNames
      fieldNames should contain("userId")
      fieldNames should contain("userName")
      fieldNames should contain("deptName")

      // SQL filter on mixed-case column
      val result = spark.sql("SELECT * FROM mixed_case_regular WHERE userName = 'bob'").collect()
      result.length shouldBe 1
      result.head.getAs[String]("userName") shouldBe "bob"

      // Another mixed-case filter
      val deptResult = spark.sql("SELECT * FROM mixed_case_regular WHERE deptName = 'Engineering'").collect()
      deptResult.length shouldBe 2
    }
  }

  test("Mixed-case columns: SQL WHERE with typemap text configuration") {
    withTempPath { path =>
      val ss = spark; import ss.implicits._

      // Write with mixed-case column configured as text
      Seq(
        (1, "the quick brown fox"),
        (2, "lazy dog sleeps"),
        (3, "quick fox jumps high")
      ).toDF("docId", "contentBody")
        .write
        .format(provider)
        .option("spark.indextables.indexing.typemap.contentBody", "text")
        .mode("overwrite")
        .save(path)

      // Read with same typemap config
      val df = spark.read.format(provider)
        .option("spark.indextables.read.defaultLimit", "1000")
        .option("spark.indextables.indexing.typemap.contentBody", "text")
        .load(path)
      df.createOrReplaceTempView("mixed_case_text")

      // Verify schema preserves case
      df.schema.fieldNames should contain("contentBody")

      // SQL indexquery on mixed-case text field
      val iqResult = spark.sql(
        "SELECT * FROM mixed_case_text WHERE contentBody indexquery 'quick fox'"
      ).collect()
      iqResult.length should be >= 1

      // Filter on non-text mixed-case column
      val idResult = spark.sql("SELECT * FROM mixed_case_text WHERE docId = 1").collect()
      idResult.length shouldBe 1
    }
  }
}
