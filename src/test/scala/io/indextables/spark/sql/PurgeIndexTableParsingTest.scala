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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.SparkSession

class PurgeIndexTableParsingTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("PurgeOrphanedSplitsParsingTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  test("PURGE INDEXTABLE should parse with path") {
    val sql = "PURGE INDEXTABLE '/tmp/test_table'"
    // Just verify it parses without error
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PurgeOrphanedSplitsCommand])

    val cmd = plan.asInstanceOf[PurgeOrphanedSplitsCommand]
    assert(cmd.tablePath == "/tmp/test_table")
    assert(cmd.retentionHours.isEmpty)
    assert(!cmd.dryRun)
  }

  test("PURGE INDEXTABLE should parse with OLDER THAN days") {
    val sql = "PURGE INDEXTABLE '/tmp/test_table' OLDER THAN 7 DAYS"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PurgeOrphanedSplitsCommand])

    val cmd = plan.asInstanceOf[PurgeOrphanedSplitsCommand]
    assert(cmd.tablePath == "/tmp/test_table")
    assert(cmd.retentionHours.contains(168)) // 7 days = 168 hours
    assert(!cmd.dryRun)
  }

  test("PURGE INDEXTABLE should parse with OLDER THAN hours") {
    val sql = "PURGE INDEXTABLE '/tmp/test_table' OLDER THAN 24 HOURS"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PurgeOrphanedSplitsCommand])

    val cmd = plan.asInstanceOf[PurgeOrphanedSplitsCommand]
    assert(cmd.tablePath == "/tmp/test_table")
    assert(cmd.retentionHours.contains(24))
    assert(!cmd.dryRun)
  }

  test("PURGE INDEXTABLE should parse with DRY RUN") {
    val sql = "PURGE INDEXTABLE '/tmp/test_table' DRY RUN"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PurgeOrphanedSplitsCommand])

    val cmd = plan.asInstanceOf[PurgeOrphanedSplitsCommand]
    assert(cmd.tablePath == "/tmp/test_table")
    assert(cmd.retentionHours.isEmpty)
    assert(cmd.dryRun)
  }

  test("PURGE INDEXTABLE should parse with all options") {
    val sql = "PURGE INDEXTABLE '/tmp/test_table' OLDER THAN 14 DAYS DRY RUN"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PurgeOrphanedSplitsCommand])

    val cmd = plan.asInstanceOf[PurgeOrphanedSplitsCommand]
    assert(cmd.tablePath == "/tmp/test_table")
    assert(cmd.retentionHours.contains(336)) // 14 days = 336 hours
    assert(cmd.dryRun)
  }

  test("PURGE INDEXTABLE should parse case-insensitive keywords") {
    val sql = "purge indextable '/tmp/test_table' older than 3 days dry run"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[PurgeOrphanedSplitsCommand])

    val cmd = plan.asInstanceOf[PurgeOrphanedSplitsCommand]
    assert(cmd.tablePath == "/tmp/test_table")
    assert(cmd.retentionHours.contains(72)) // 3 days = 72 hours
    assert(cmd.dryRun)
  }
}
