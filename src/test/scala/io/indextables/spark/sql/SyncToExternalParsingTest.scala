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

import io.indextables.spark.TestBase

/**
 * Tests for SYNC INDEXTABLES WITH DELTA SQL parsing.
 *
 * Validates that the ANTLR grammar correctly parses all SYNC command variants
 * and produces the correct SyncToExternalCommand parameters.
 * Both "WITH DELTA" (preferred) and "TO DELTA" (backward-compatible) syntaxes are tested.
 */
class SyncToExternalParsingTest extends TestBase {

  private def parseSync(sql: String): SyncToExternalCommand = {
    val sqlParser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)
    val plan = sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[SyncToExternalCommand], s"Expected SyncToExternalCommand, got ${plan.getClass.getSimpleName}")
    plan.asInstanceOf[SyncToExternalCommand]
  }

  // --- Basic syntax tests (TO DELTA - backward compatibility) ---

  test("parse basic SYNC INDEXTABLES TO DELTA syntax (backward compat)") {
    val cmd = parseSync(
      "SYNC INDEXTABLES TO DELTA 's3://bucket/delta_table' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "delta"
    cmd.sourcePath shouldBe "s3://bucket/delta_table"
    cmd.destPath shouldBe "s3://bucket/index"
    cmd.indexingModes shouldBe empty
    cmd.fastFieldMode shouldBe "HYBRID"
    cmd.targetInputSize shouldBe None
    cmd.dryRun shouldBe false
  }

  // --- Primary syntax: WITH DELTA ---

  test("parse basic SYNC INDEXTABLES WITH DELTA syntax") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA 's3://bucket/delta_table' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "delta"
    cmd.sourcePath shouldBe "s3://bucket/delta_table"
    cmd.destPath shouldBe "s3://bucket/index"
    cmd.indexingModes shouldBe empty
    cmd.fastFieldMode shouldBe "HYBRID"
    cmd.targetInputSize shouldBe None
    cmd.fromVersion shouldBe None
    cmd.wherePredicates shouldBe empty
    cmd.dryRun shouldBe false
  }

  test("parse SYNC with TANTIVY4SPARK keyword") {
    val cmd = parseSync(
      "SYNC TANTIVY4SPARK WITH DELTA 's3://bucket/delta_table' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "delta"
    cmd.sourcePath shouldBe "s3://bucket/delta_table"
    cmd.destPath shouldBe "s3://bucket/index"
  }

  test("parse SYNC with DRY RUN") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA 's3://data/events' AT LOCATION 's3://idx/events' DRY RUN"
    )
    cmd.dryRun shouldBe true
    cmd.sourcePath shouldBe "s3://data/events"
    cmd.destPath shouldBe "s3://idx/events"
  }

  test("parse SYNC with FASTFIELDS MODE HYBRID") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' FASTFIELDS MODE HYBRID AT LOCATION '/tmp/index'"
    )
    cmd.fastFieldMode shouldBe "HYBRID"
  }

  test("parse SYNC with FASTFIELDS MODE DISABLED") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' FASTFIELDS MODE DISABLED AT LOCATION '/tmp/index'"
    )
    cmd.fastFieldMode shouldBe "DISABLED"
  }

  test("parse SYNC with FASTFIELDS MODE PARQUET_ONLY") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' FASTFIELDS MODE PARQUET_ONLY AT LOCATION '/tmp/index'"
    )
    cmd.fastFieldMode shouldBe "PARQUET_ONLY"
  }

  test("parse SYNC with TARGET INPUT SIZE") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' TARGET INPUT SIZE 2G AT LOCATION '/tmp/index'"
    )
    cmd.targetInputSize shouldBe defined
    cmd.targetInputSize.get shouldBe (2L * 1024L * 1024L * 1024L)
  }

  test("parse SYNC with TARGET INPUT SIZE in megabytes") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' TARGET INPUT SIZE 500M AT LOCATION '/tmp/index'"
    )
    cmd.targetInputSize shouldBe defined
    cmd.targetInputSize.get shouldBe (500L * 1024L * 1024L)
  }

  test("parse SYNC with INDEXING MODES") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' INDEXING MODES ('title':'text', 'ip_addr':'ipaddress') AT LOCATION '/tmp/index'"
    )
    cmd.indexingModes should contain key "title"
    cmd.indexingModes("title") shouldBe "text"
    cmd.indexingModes should contain key "ip_addr"
    cmd.indexingModes("ip_addr") shouldBe "ipaddress"
  }

  test("parse SYNC with all options combined") {
    val cmd = parseSync(
      """SYNC INDEXTABLES WITH DELTA 's3://bucket/delta'
        |  INDEXING MODES ('content':'text', 'status':'string')
        |  FASTFIELDS MODE PARQUET_ONLY
        |  TARGET INPUT SIZE 1G
        |  AT LOCATION 's3://bucket/index'
        |  DRY RUN""".stripMargin
    )
    cmd.sourceFormat shouldBe "delta"
    cmd.sourcePath shouldBe "s3://bucket/delta"
    cmd.destPath shouldBe "s3://bucket/index"
    cmd.fastFieldMode shouldBe "PARQUET_ONLY"
    cmd.targetInputSize shouldBe Some(1L * 1024L * 1024L * 1024L)
    cmd.dryRun shouldBe true
    cmd.indexingModes should have size 2
    cmd.indexingModes("content") shouldBe "text"
    cmd.indexingModes("status") shouldBe "string"
  }

  test("parse SYNC with local filesystem paths") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/data/my_delta_table' AT LOCATION '/data/my_index'"
    )
    cmd.sourcePath shouldBe "/data/my_delta_table"
    cmd.destPath shouldBe "/data/my_index"
  }

  test("parse SYNC with Azure paths") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA 'abfss://container@account.dfs.core.windows.net/delta' AT LOCATION 'abfss://container@account.dfs.core.windows.net/index'"
    )
    cmd.sourcePath shouldBe "abfss://container@account.dfs.core.windows.net/delta"
    cmd.destPath shouldBe "abfss://container@account.dfs.core.windows.net/index"
  }

  test("default fastFieldMode should be HYBRID") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' AT LOCATION '/tmp/index'"
    )
    cmd.fastFieldMode shouldBe "HYBRID"
  }

  test("default indexingModes should be empty") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' AT LOCATION '/tmp/index'"
    )
    cmd.indexingModes shouldBe empty
  }

  test("default targetInputSize should be None") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' AT LOCATION '/tmp/index'"
    )
    cmd.targetInputSize shouldBe None
  }

  test("default dryRun should be false") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' AT LOCATION '/tmp/index'"
    )
    cmd.dryRun shouldBe false
  }

  // --- WHERE clause tests ---

  test("parse SYNC with WHERE clause") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' WHERE year >= 2024 AT LOCATION '/tmp/index'"
    )
    cmd.wherePredicates should have size 1
    cmd.wherePredicates.head should include("year")
    cmd.wherePredicates.head should include("2024")
  }

  test("parse SYNC with compound WHERE clause") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' WHERE year >= 2024 AND region = 'us-east' AT LOCATION '/tmp/index'"
    )
    cmd.wherePredicates should have size 1
    cmd.wherePredicates.head should include("year")
    cmd.wherePredicates.head should include("region")
  }

  test("parse SYNC with WHERE and DRY RUN") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' WHERE status = 'active' AT LOCATION '/tmp/index' DRY RUN"
    )
    cmd.wherePredicates should have size 1
    cmd.wherePredicates.head should include("status")
    cmd.dryRun shouldBe true
  }

  test("default wherePredicates should be empty") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' AT LOCATION '/tmp/index'"
    )
    cmd.wherePredicates shouldBe empty
  }

  // --- FROM VERSION tests ---

  test("parse SYNC with FROM VERSION") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' FROM VERSION 500 AT LOCATION '/tmp/index'"
    )
    cmd.fromVersion shouldBe Some(500L)
  }

  test("parse SYNC with FROM VERSION 0") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' FROM VERSION 0 AT LOCATION '/tmp/index'"
    )
    cmd.fromVersion shouldBe Some(0L)
  }

  test("default fromVersion should be None") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' AT LOCATION '/tmp/index'"
    )
    cmd.fromVersion shouldBe None
  }

  // --- Combined WHERE + FROM VERSION tests ---

  test("parse SYNC with FROM VERSION and WHERE") {
    val cmd = parseSync(
      "SYNC INDEXTABLES WITH DELTA '/tmp/delta' FROM VERSION 100 WHERE region = 'us-east' AT LOCATION '/tmp/index'"
    )
    cmd.fromVersion shouldBe Some(100L)
    cmd.wherePredicates should have size 1
    cmd.wherePredicates.head should include("region")
  }

  test("parse SYNC with all options including WHERE and FROM VERSION") {
    val cmd = parseSync(
      """SYNC INDEXTABLES WITH DELTA 's3://bucket/delta'
        |  INDEXING MODES ('content':'text')
        |  FASTFIELDS MODE HYBRID
        |  TARGET INPUT SIZE 2G
        |  FROM VERSION 42
        |  WHERE year = '2024'
        |  AT LOCATION 's3://bucket/index'
        |  DRY RUN""".stripMargin
    )
    cmd.sourcePath shouldBe "s3://bucket/delta"
    cmd.destPath shouldBe "s3://bucket/index"
    cmd.fastFieldMode shouldBe "HYBRID"
    cmd.targetInputSize shouldBe Some(2L * 1024L * 1024L * 1024L)
    cmd.fromVersion shouldBe Some(42L)
    cmd.wherePredicates should have size 1
    cmd.wherePredicates.head should include("year")
    cmd.dryRun shouldBe true
    cmd.indexingModes("content") shouldBe "text"
  }
}
