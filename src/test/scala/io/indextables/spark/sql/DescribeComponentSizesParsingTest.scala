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

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

/**
 * Parsing tests for DESCRIBE INDEXTABLES COMPONENT SIZES command.
 *
 * Tests SQL syntax variations and parameter extraction.
 */
class DescribeComponentSizesParsingTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _

  override def beforeEach(): Unit =
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("DescribeComponentSizesParsingTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

  override def afterEach(): Unit =
    if (spark != null) {
      spark.stop()
      spark = null
    }

  // === Basic Syntax Tests ===

  test("DESCRIBE INDEXTABLES COMPONENT SIZES should parse with path only") {
    val sql = "DESCRIBE INDEXTABLES COMPONENT SIZES '/tmp/test_table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])

    val cmd = plan.asInstanceOf[DescribeComponentSizesCommand]
    assert(cmd.tablePath == "/tmp/test_table")
    assert(cmd.wherePredicates.isEmpty)
  }

  test("DESCRIBE INDEXTABLES COMPONENT SIZES should parse with S3 path") {
    val sql = "DESCRIBE INDEXTABLES COMPONENT SIZES 's3://bucket/table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])

    val cmd = plan.asInstanceOf[DescribeComponentSizesCommand]
    assert(cmd.tablePath == "s3://bucket/table")
  }

  test("DESCRIBE INDEXTABLES COMPONENT SIZES should parse with Azure path") {
    val sql = "DESCRIBE INDEXTABLES COMPONENT SIZES 'abfss://container@account.dfs.core.windows.net/path'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])

    val cmd = plan.asInstanceOf[DescribeComponentSizesCommand]
    assert(cmd.tablePath == "abfss://container@account.dfs.core.windows.net/path")
  }

  test("DESCRIBE TANTIVY4SPARK COMPONENT SIZES should also work") {
    val sql = "DESCRIBE TANTIVY4SPARK COMPONENT SIZES '/tmp/test_table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])
  }

  test("DESCRIBE COMPONENT SIZES should parse case-insensitive keywords") {
    val sql = "describe indextables component sizes '/tmp/test_table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])
  }

  test("DESCRIBE INDEXTABLES COMPONENT SIZES should parse with table identifier") {
    val sql = "DESCRIBE INDEXTABLES COMPONENT SIZES my_table"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])

    val cmd = plan.asInstanceOf[DescribeComponentSizesCommand]
    assert(cmd.tablePath == "my_table")
  }

  test("DESCRIBE INDEXTABLES COMPONENT SIZES should parse with qualified table identifier") {
    val sql = "DESCRIBE INDEXTABLES COMPONENT SIZES my_db.my_table"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])

    val cmd = plan.asInstanceOf[DescribeComponentSizesCommand]
    assert(cmd.tablePath == "my_db.my_table")
  }

  // === WHERE Clause Tests ===

  test("DESCRIBE COMPONENT SIZES should parse with simple WHERE clause") {
    val sql = "DESCRIBE INDEXTABLES COMPONENT SIZES '/tmp/test' WHERE year = '2024'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])

    val cmd = plan.asInstanceOf[DescribeComponentSizesCommand]
    assert(cmd.wherePredicates.nonEmpty)
    assert(cmd.wherePredicates.head.contains("year"))
    assert(cmd.wherePredicates.head.contains("2024"))
  }

  test("DESCRIBE COMPONENT SIZES should parse with compound WHERE clause (AND)") {
    val sql = "DESCRIBE INDEXTABLES COMPONENT SIZES '/tmp/test' WHERE year = '2024' AND region = 'us-east'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])

    val cmd = plan.asInstanceOf[DescribeComponentSizesCommand]
    assert(cmd.wherePredicates.nonEmpty)
    assert(cmd.wherePredicates.head.contains("year"))
    assert(cmd.wherePredicates.head.contains("region"))
  }

  test("DESCRIBE COMPONENT SIZES should parse with compound WHERE clause (OR)") {
    val sql = "DESCRIBE INDEXTABLES COMPONENT SIZES '/tmp/test' WHERE year = '2023' OR year = '2024'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])

    val cmd = plan.asInstanceOf[DescribeComponentSizesCommand]
    assert(cmd.wherePredicates.nonEmpty)
    assert(cmd.wherePredicates.head.contains("OR") || cmd.wherePredicates.head.contains("or"))
  }

  test("DESCRIBE COMPONENT SIZES should parse with IN clause") {
    val sql = "DESCRIBE INDEXTABLES COMPONENT SIZES '/tmp/test' WHERE region IN ('us-east', 'us-west')"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])

    val cmd = plan.asInstanceOf[DescribeComponentSizesCommand]
    assert(cmd.wherePredicates.nonEmpty)
    assert(cmd.wherePredicates.head.contains("IN") || cmd.wherePredicates.head.contains("in"))
  }

  test("DESCRIBE COMPONENT SIZES should parse with BETWEEN clause") {
    val sql = "DESCRIBE INDEXTABLES COMPONENT SIZES '/tmp/test' WHERE month BETWEEN 1 AND 6"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])

    val cmd = plan.asInstanceOf[DescribeComponentSizesCommand]
    assert(cmd.wherePredicates.nonEmpty)
    assert(cmd.wherePredicates.head.contains("BETWEEN") || cmd.wherePredicates.head.contains("between"))
  }

  test("DESCRIBE COMPONENT SIZES should parse with comparison operators") {
    val sql = "DESCRIBE INDEXTABLES COMPONENT SIZES '/tmp/test' WHERE year >= '2020' AND year < '2025'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])

    val cmd = plan.asInstanceOf[DescribeComponentSizesCommand]
    assert(cmd.wherePredicates.nonEmpty)
    assert(cmd.wherePredicates.head.contains(">="))
    assert(cmd.wherePredicates.head.contains("<"))
  }

  // === Mixed Case and Whitespace Tests ===

  test("DESCRIBE COMPONENT SIZES should handle mixed case in WHERE clause") {
    val sql = "DESCRIBE IndExtables COMPONENT Sizes '/tmp/test' Where year = '2024'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])

    val cmd = plan.asInstanceOf[DescribeComponentSizesCommand]
    assert(cmd.tablePath == "/tmp/test")
    assert(cmd.wherePredicates.nonEmpty)
  }

  test("DESCRIBE COMPONENT SIZES should handle extra whitespace") {
    val sql = "DESCRIBE   INDEXTABLES   COMPONENT   SIZES   '/tmp/test'   WHERE   year = '2024'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DescribeComponentSizesCommand])

    val cmd = plan.asInstanceOf[DescribeComponentSizesCommand]
    assert(cmd.tablePath == "/tmp/test")
    assert(cmd.wherePredicates.nonEmpty)
  }
}
