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
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/** Tests for TRUNCATE INDEXTABLES TIME TRAVEL SQL parsing. */
class TruncateTimeTravelParsingTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("TruncateTimeTravelParsingTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  test("TRUNCATE INDEXTABLES TIME TRAVEL should parse with path string") {
    val sql  = "TRUNCATE INDEXTABLES TIME TRAVEL '/tmp/test_table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[TruncateTimeTravelCommand])

    val cmd = plan.asInstanceOf[TruncateTimeTravelCommand]
    assert(cmd.tablePath == "/tmp/test_table")
    assert(cmd.dryRun == false)
  }

  test("TRUNCATE INDEXTABLES TIME TRAVEL should parse with S3 path") {
    val sql  = "TRUNCATE INDEXTABLES TIME TRAVEL 's3://bucket/path/to/table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[TruncateTimeTravelCommand])

    val cmd = plan.asInstanceOf[TruncateTimeTravelCommand]
    assert(cmd.tablePath == "s3://bucket/path/to/table")
    assert(cmd.dryRun == false)
  }

  test("TRUNCATE INDEXTABLES TIME TRAVEL should parse with Azure path") {
    val sql  = "TRUNCATE INDEXTABLES TIME TRAVEL 'abfss://container@account.dfs.core.windows.net/path'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[TruncateTimeTravelCommand])

    val cmd = plan.asInstanceOf[TruncateTimeTravelCommand]
    assert(cmd.tablePath == "abfss://container@account.dfs.core.windows.net/path")
    assert(cmd.dryRun == false)
  }

  test("TRUNCATE INDEXTABLES TIME TRAVEL should parse with DRY RUN flag") {
    val sql  = "TRUNCATE INDEXTABLES TIME TRAVEL '/tmp/test_table' DRY RUN"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[TruncateTimeTravelCommand])

    val cmd = plan.asInstanceOf[TruncateTimeTravelCommand]
    assert(cmd.tablePath == "/tmp/test_table")
    assert(cmd.dryRun == true)
  }

  test("TRUNCATE INDEXTABLES TIME TRAVEL should parse with table identifier") {
    val sql  = "TRUNCATE INDEXTABLES TIME TRAVEL my_table"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[TruncateTimeTravelCommand])

    val cmd = plan.asInstanceOf[TruncateTimeTravelCommand]
    assert(cmd.tablePath == "my_table")
    assert(cmd.dryRun == false)
  }

  test("TRUNCATE INDEXTABLES TIME TRAVEL should parse with qualified table identifier") {
    val sql  = "TRUNCATE INDEXTABLES TIME TRAVEL my_database.my_table"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[TruncateTimeTravelCommand])

    val cmd = plan.asInstanceOf[TruncateTimeTravelCommand]
    assert(cmd.tablePath == "my_database.my_table")
    assert(cmd.dryRun == false)
  }

  test("TRUNCATE TANTIVY4SPARK TIME TRAVEL should parse (alternate keyword)") {
    val sql  = "TRUNCATE TANTIVY4SPARK TIME TRAVEL '/tmp/test_table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[TruncateTimeTravelCommand])

    val cmd = plan.asInstanceOf[TruncateTimeTravelCommand]
    assert(cmd.tablePath == "/tmp/test_table")
    assert(cmd.dryRun == false)
  }

  test("TRUNCATE TANTIVY4SPARK TIME TRAVEL with DRY RUN should parse") {
    val sql  = "TRUNCATE TANTIVY4SPARK TIME TRAVEL 's3://bucket/table' DRY RUN"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[TruncateTimeTravelCommand])

    val cmd = plan.asInstanceOf[TruncateTimeTravelCommand]
    assert(cmd.tablePath == "s3://bucket/table")
    assert(cmd.dryRun == true)
  }

  test("TRUNCATE INDEXTABLES TIME TRAVEL should be case-insensitive") {
    val sql  = "truncate indextables time travel '/tmp/test'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[TruncateTimeTravelCommand])

    val cmd = plan.asInstanceOf[TruncateTimeTravelCommand]
    assert(cmd.tablePath == "/tmp/test")
  }

  test("TRUNCATE INDEXTABLES TIME TRAVEL with mixed case DRY RUN") {
    val sql  = "TRUNCATE INDEXTABLES TIME TRAVEL '/tmp/test' Dry Run"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[TruncateTimeTravelCommand])

    val cmd = plan.asInstanceOf[TruncateTimeTravelCommand]
    assert(cmd.tablePath == "/tmp/test")
    assert(cmd.dryRun == true)
  }

  test("TRUNCATE INDEXTABLES TIME TRAVEL should parse with double-quoted path") {
    val sql  = """TRUNCATE INDEXTABLES TIME TRAVEL "/tmp/test_table""""
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[TruncateTimeTravelCommand])

    val cmd = plan.asInstanceOf[TruncateTimeTravelCommand]
    assert(cmd.tablePath == "/tmp/test_table")
  }
}
