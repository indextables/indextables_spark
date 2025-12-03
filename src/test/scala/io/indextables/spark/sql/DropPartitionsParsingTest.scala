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

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/** Parsing tests for DROP INDEXTABLES PARTITIONS command. */
class DropPartitionsParsingTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _

  override def beforeEach(): Unit =
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("DropPartitionsParsingTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

  override def afterEach(): Unit =
    if (spark != null) {
      spark.stop()
      spark = null
    }

  test("DROP INDEXTABLES PARTITIONS should parse with path and simple equality predicate") {
    val sql  = "DROP INDEXTABLES PARTITIONS FROM '/tmp/test_table' WHERE year = '2023'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])

    val cmd = plan.asInstanceOf[DropPartitionsCommand]
    assert(cmd.userPartitionPredicates.nonEmpty)
    assert(cmd.userPartitionPredicates.head.contains("year"))
  }

  test("DROP INDEXTABLES PARTITIONS should parse with greater than predicate") {
    val sql  = "DROP INDEXTABLES PARTITIONS FROM '/tmp/test_table' WHERE year > '2020'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])
  }

  test("DROP INDEXTABLES PARTITIONS should parse with less than predicate") {
    val sql  = "DROP INDEXTABLES PARTITIONS FROM '/tmp/test_table' WHERE month < 6"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])
  }

  test("DROP INDEXTABLES PARTITIONS should parse with BETWEEN predicate") {
    val sql  = "DROP INDEXTABLES PARTITIONS FROM '/tmp/test_table' WHERE month BETWEEN 1 AND 6"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])

    val cmd = plan.asInstanceOf[DropPartitionsCommand]
    assert(cmd.userPartitionPredicates.head.contains("BETWEEN") || cmd.userPartitionPredicates.head.contains("between"))
  }

  test("DROP INDEXTABLES PARTITIONS should parse with AND compound predicate") {
    val sql  = "DROP INDEXTABLES PARTITIONS FROM '/tmp/test_table' WHERE year = '2023' AND month = '01'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])

    val cmd = plan.asInstanceOf[DropPartitionsCommand]
    assert(cmd.userPartitionPredicates.head.contains("AND") || cmd.userPartitionPredicates.head.contains("and"))
  }

  test("DROP INDEXTABLES PARTITIONS should parse with OR compound predicate") {
    val sql  = "DROP INDEXTABLES PARTITIONS FROM '/tmp/test_table' WHERE year = '2023' OR year = '2024'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])

    val cmd = plan.asInstanceOf[DropPartitionsCommand]
    assert(cmd.userPartitionPredicates.head.contains("OR") || cmd.userPartitionPredicates.head.contains("or"))
  }

  test("DROP INDEXTABLES PARTITIONS should parse with complex compound predicate") {
    val sql  = "DROP INDEXTABLES PARTITIONS FROM '/tmp/test_table' WHERE region = 'us-east' AND year > '2020' OR status = 'inactive'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])
  }

  test("DROP INDEXTABLES PARTITIONS should parse with S3 path") {
    val sql  = "DROP INDEXTABLES PARTITIONS FROM 's3://bucket/table' WHERE partition_key = 'value'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])
  }

  test("DROP INDEXTABLES PARTITIONS should parse case-insensitive keywords") {
    val sql  = "drop indextables partitions from '/tmp/test_table' where year = '2023'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])
  }

  test("DROP TANTIVY4SPARK PARTITIONS should also work") {
    val sql  = "DROP TANTIVY4SPARK PARTITIONS FROM '/tmp/test_table' WHERE year = '2023'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])
  }

  test("DROP INDEXTABLES PARTITIONS should parse with table identifier") {
    val sql  = "DROP INDEXTABLES PARTITIONS FROM my_table WHERE year = '2023'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])
  }

  test("DROP INDEXTABLES PARTITIONS should parse with qualified table identifier") {
    val sql  = "DROP INDEXTABLES PARTITIONS FROM my_db.my_table WHERE year = '2023'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])
  }

  test("DROP INDEXTABLES PARTITIONS should fail without WHERE clause") {
    val sql = "DROP INDEXTABLES PARTITIONS FROM '/tmp/test_table'"
    val ex  = intercept[Exception] {
      spark.sessionState.sqlParser.parsePlan(sql)
    }
    // The parsing should fail because WHERE is required - Spark's parser gives a syntax error
    assert(
      ex.getMessage.contains("WHERE") ||
        ex.getMessage.contains("mismatched input") ||
        ex.getMessage.contains("PARSE_SYNTAX_ERROR") ||
        ex.getMessage.contains("Syntax error")
    )
  }

  test("DROP INDEXTABLES PARTITIONS should parse with IN predicate") {
    val sql  = "DROP INDEXTABLES PARTITIONS FROM '/tmp/test_table' WHERE region IN ('us-east', 'us-west', 'eu-west')"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])
  }

  test("DROP INDEXTABLES PARTITIONS should parse with greater than or equal predicate") {
    val sql  = "DROP INDEXTABLES PARTITIONS FROM '/tmp/test_table' WHERE year >= '2020'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])
  }

  test("DROP INDEXTABLES PARTITIONS should parse with less than or equal predicate") {
    val sql  = "DROP INDEXTABLES PARTITIONS FROM '/tmp/test_table' WHERE month <= 6"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DropPartitionsCommand])
  }
}
