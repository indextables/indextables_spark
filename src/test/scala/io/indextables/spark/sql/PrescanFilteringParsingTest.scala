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

/** Parsing tests for prescan filtering SQL commands. */
class PrescanFilteringParsingTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _

  override def beforeEach(): Unit =
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("PrescanFilteringParsingTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

  override def afterEach(): Unit =
    if (spark != null) {
      spark.stop()
      spark = null
    }

  // ======================
  // ENABLE INDEXTABLES PRESCAN FILTERING tests
  // ======================

  test("ENABLE INDEXTABLES PRESCAN FILTERING should parse without path or table") {
    val sql = "ENABLE INDEXTABLES PRESCAN FILTERING"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[EnablePrescanFilteringCommand])

    val cmd = plan.asInstanceOf[EnablePrescanFilteringCommand]
    assert(cmd.pathOption.isEmpty)
    assert(cmd.tableOption.isEmpty)
  }

  test("ENABLE INDEXTABLES PRESCAN FILTERING should parse with path string") {
    val sql = "ENABLE INDEXTABLES PRESCAN FILTERING FOR 's3://bucket/table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[EnablePrescanFilteringCommand])

    val cmd = plan.asInstanceOf[EnablePrescanFilteringCommand]
    assert(cmd.pathOption.contains("s3://bucket/table"))
    assert(cmd.tableOption.isEmpty)
  }

  test("ENABLE INDEXTABLES PRESCAN FILTERING should parse with table identifier") {
    val sql = "ENABLE INDEXTABLES PRESCAN FILTERING FOR my_database.my_table"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[EnablePrescanFilteringCommand])

    val cmd = plan.asInstanceOf[EnablePrescanFilteringCommand]
    assert(cmd.pathOption.isEmpty)
    assert(cmd.tableOption.isDefined)
    assert(cmd.tableOption.get.table == "my_table")
  }

  test("ENABLE TANTIVY4SPARK PRESCAN FILTERING should parse (alias keyword)") {
    val sql = "ENABLE TANTIVY4SPARK PRESCAN FILTERING"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[EnablePrescanFilteringCommand])
  }

  // ======================
  // DISABLE INDEXTABLES PRESCAN FILTERING tests
  // ======================

  test("DISABLE INDEXTABLES PRESCAN FILTERING should parse without path or table") {
    val sql = "DISABLE INDEXTABLES PRESCAN FILTERING"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DisablePrescanFilteringCommand])

    val cmd = plan.asInstanceOf[DisablePrescanFilteringCommand]
    assert(cmd.pathOption.isEmpty)
    assert(cmd.tableOption.isEmpty)
  }

  test("DISABLE INDEXTABLES PRESCAN FILTERING should parse with path string") {
    val sql = "DISABLE INDEXTABLES PRESCAN FILTERING FOR 's3://bucket/table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DisablePrescanFilteringCommand])

    val cmd = plan.asInstanceOf[DisablePrescanFilteringCommand]
    assert(cmd.pathOption.contains("s3://bucket/table"))
    assert(cmd.tableOption.isEmpty)
  }

  test("DISABLE INDEXTABLES PRESCAN FILTERING should parse with table identifier") {
    val sql = "DISABLE INDEXTABLES PRESCAN FILTERING FOR my_table"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DisablePrescanFilteringCommand])

    val cmd = plan.asInstanceOf[DisablePrescanFilteringCommand]
    assert(cmd.pathOption.isEmpty)
    assert(cmd.tableOption.isDefined)
    assert(cmd.tableOption.get.table == "my_table")
  }

  test("DISABLE TANTIVY4SPARK PRESCAN FILTERING should parse (alias keyword)") {
    val sql = "DISABLE TANTIVY4SPARK PRESCAN FILTERING"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[DisablePrescanFilteringCommand])
  }

  // ======================
  // PREWARM INDEXTABLES PRESCAN FILTERS tests
  // ======================

  test("PREWARM INDEXTABLES PRESCAN FILTERS should parse with path only") {
    val sql = "PREWARM INDEXTABLES PRESCAN FILTERS FOR 's3://bucket/table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmPrescanFiltersCommand])

    val cmd = plan.asInstanceOf[PrewarmPrescanFiltersCommand]
    assert(cmd.pathOption.contains("s3://bucket/table"))
    assert(cmd.tableOption.isEmpty)
    assert(cmd.fields.isEmpty)
    assert(cmd.wherePredicates.isEmpty)
  }

  test("PREWARM INDEXTABLES PRESCAN FILTERS should parse with table identifier") {
    val sql = "PREWARM INDEXTABLES PRESCAN FILTERS FOR my_catalog.my_table"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmPrescanFiltersCommand])

    val cmd = plan.asInstanceOf[PrewarmPrescanFiltersCommand]
    assert(cmd.pathOption.isEmpty)
    assert(cmd.tableOption.isDefined)
    assert(cmd.tableOption.get.table == "my_table")
  }

  test("PREWARM INDEXTABLES PRESCAN FILTERS should parse with ON FIELDS clause") {
    val sql = "PREWARM INDEXTABLES PRESCAN FILTERS FOR 's3://bucket/table' ON FIELDS(title, author, date)"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmPrescanFiltersCommand])

    val cmd = plan.asInstanceOf[PrewarmPrescanFiltersCommand]
    assert(cmd.pathOption.contains("s3://bucket/table"))
    assert(cmd.fields.length == 3)
    assert(cmd.fields.contains("title"))
    assert(cmd.fields.contains("author"))
    assert(cmd.fields.contains("date"))
  }

  test("PREWARM INDEXTABLES PRESCAN FILTERS should parse with WHERE clause") {
    val sql = "PREWARM INDEXTABLES PRESCAN FILTERS FOR 's3://bucket/table' WHERE date = '2024-01-01'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmPrescanFiltersCommand])

    val cmd = plan.asInstanceOf[PrewarmPrescanFiltersCommand]
    assert(cmd.pathOption.contains("s3://bucket/table"))
    assert(cmd.wherePredicates.nonEmpty)
    assert(cmd.wherePredicates.head.contains("date"))
  }

  test("PREWARM INDEXTABLES PRESCAN FILTERS should parse with ON FIELDS and WHERE clause") {
    val sql = "PREWARM INDEXTABLES PRESCAN FILTERS FOR 's3://bucket/table' ON FIELDS(title, content) WHERE region = 'us-east'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmPrescanFiltersCommand])

    val cmd = plan.asInstanceOf[PrewarmPrescanFiltersCommand]
    assert(cmd.pathOption.contains("s3://bucket/table"))
    assert(cmd.fields.length == 2)
    assert(cmd.fields.contains("title"))
    assert(cmd.fields.contains("content"))
    assert(cmd.wherePredicates.nonEmpty)
  }

  test("PREWARM TANTIVY4SPARK PRESCAN FILTERS should parse (alias keyword)") {
    val sql = "PREWARM TANTIVY4SPARK PRESCAN FILTERS FOR 's3://bucket/table'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PrewarmPrescanFiltersCommand])
  }

  // ======================
  // Case insensitivity tests
  // ======================

  test("prescan commands should be case insensitive") {
    val sql1 = "enable indextables prescan filtering"
    val plan1 = spark.sessionState.sqlParser.parsePlan(sql1)
    assert(plan1.isInstanceOf[EnablePrescanFilteringCommand])

    val sql2 = "DISABLE INDEXTABLES PRESCAN FILTERING"
    val plan2 = spark.sessionState.sqlParser.parsePlan(sql2)
    assert(plan2.isInstanceOf[DisablePrescanFilteringCommand])

    val sql3 = "Prewarm IndexTables Prescan Filters For 's3://bucket/table'"
    val plan3 = spark.sessionState.sqlParser.parsePlan(sql3)
    assert(plan3.isInstanceOf[PrewarmPrescanFiltersCommand])
  }
}
