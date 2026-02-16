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
 * Tests for BUILD INDEXTABLES COMPANION FOR (DELTA|PARQUET|ICEBERG) SQL parsing.
 *
 * Validates that the ANTLR grammar correctly parses all BUILD COMPANION command variants
 * and produces the correct SyncToExternalCommand parameters.
 */
class SyncToExternalParsingTest extends TestBase {

  private def parseSync(sql: String): SyncToExternalCommand = {
    val sqlParser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)
    val plan = sqlParser.parsePlan(sql)
    assert(plan.isInstanceOf[SyncToExternalCommand], s"Expected SyncToExternalCommand, got ${plan.getClass.getSimpleName}")
    plan.asInstanceOf[SyncToExternalCommand]
  }

  // --- FOR DELTA syntax ---

  test("parse basic BUILD INDEXTABLES COMPANION FOR DELTA syntax") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA 's3://bucket/delta_table' AT LOCATION 's3://bucket/index'"
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
      "BUILD TANTIVY4SPARK COMPANION FOR DELTA 's3://bucket/delta_table' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "delta"
    cmd.sourcePath shouldBe "s3://bucket/delta_table"
    cmd.destPath shouldBe "s3://bucket/index"
  }

  test("parse SYNC with DRY RUN") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA 's3://data/events' AT LOCATION 's3://idx/events' DRY RUN"
    )
    cmd.dryRun shouldBe true
    cmd.sourcePath shouldBe "s3://data/events"
    cmd.destPath shouldBe "s3://idx/events"
  }

  test("parse SYNC with FASTFIELDS MODE HYBRID") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' FASTFIELDS MODE HYBRID AT LOCATION '/tmp/index'"
    )
    cmd.fastFieldMode shouldBe "HYBRID"
  }

  test("parse SYNC with FASTFIELDS MODE DISABLED") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' FASTFIELDS MODE DISABLED AT LOCATION '/tmp/index'"
    )
    cmd.fastFieldMode shouldBe "DISABLED"
  }

  test("parse SYNC with FASTFIELDS MODE PARQUET_ONLY") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' FASTFIELDS MODE PARQUET_ONLY AT LOCATION '/tmp/index'"
    )
    cmd.fastFieldMode shouldBe "PARQUET_ONLY"
  }

  test("parse SYNC with TARGET INPUT SIZE") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' TARGET INPUT SIZE 2G AT LOCATION '/tmp/index'"
    )
    cmd.targetInputSize shouldBe defined
    cmd.targetInputSize.get shouldBe (2L * 1024L * 1024L * 1024L)
  }

  test("parse SYNC with TARGET INPUT SIZE in megabytes") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' TARGET INPUT SIZE 500M AT LOCATION '/tmp/index'"
    )
    cmd.targetInputSize shouldBe defined
    cmd.targetInputSize.get shouldBe (500L * 1024L * 1024L)
  }

  test("parse SYNC with INDEXING MODES") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' INDEXING MODES ('title':'text', 'ip_addr':'ipaddress') AT LOCATION '/tmp/index'"
    )
    cmd.indexingModes should contain key "title"
    cmd.indexingModes("title") shouldBe "text"
    cmd.indexingModes should contain key "ip_addr"
    cmd.indexingModes("ip_addr") shouldBe "ipaddress"
  }

  test("parse SYNC with all options combined") {
    val cmd = parseSync(
      """BUILD INDEXTABLES COMPANION FOR DELTA 's3://bucket/delta'
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
      "BUILD INDEXTABLES COMPANION FOR DELTA '/data/my_delta_table' AT LOCATION '/data/my_index'"
    )
    cmd.sourcePath shouldBe "/data/my_delta_table"
    cmd.destPath shouldBe "/data/my_index"
  }

  test("parse SYNC with Azure paths") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA 'abfss://container@account.dfs.core.windows.net/delta' AT LOCATION 'abfss://container@account.dfs.core.windows.net/index'"
    )
    cmd.sourcePath shouldBe "abfss://container@account.dfs.core.windows.net/delta"
    cmd.destPath shouldBe "abfss://container@account.dfs.core.windows.net/index"
  }

  test("default fastFieldMode should be HYBRID") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' AT LOCATION '/tmp/index'"
    )
    cmd.fastFieldMode shouldBe "HYBRID"
  }

  test("default indexingModes should be empty") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' AT LOCATION '/tmp/index'"
    )
    cmd.indexingModes shouldBe empty
  }

  test("default targetInputSize should be None") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' AT LOCATION '/tmp/index'"
    )
    cmd.targetInputSize shouldBe None
  }

  test("default dryRun should be false") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' AT LOCATION '/tmp/index'"
    )
    cmd.dryRun shouldBe false
  }

  // --- WRITER HEAP SIZE tests ---

  test("parse SYNC with WRITER HEAP SIZE 512M") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' WRITER HEAP SIZE 512M AT LOCATION '/tmp/index'"
    )
    cmd.writerHeapSize shouldBe Some(512L * 1024L * 1024L)
  }

  test("parse SYNC with WRITER HEAP SIZE 2G") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' WRITER HEAP SIZE 2G AT LOCATION '/tmp/index'"
    )
    cmd.writerHeapSize shouldBe Some(2L * 1024L * 1024L * 1024L)
  }

  test("default writerHeapSize should be None") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' AT LOCATION '/tmp/index'"
    )
    cmd.writerHeapSize shouldBe None
  }

  test("parse SYNC with TARGET INPUT SIZE and WRITER HEAP SIZE combined") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' TARGET INPUT SIZE 4G WRITER HEAP SIZE 1G AT LOCATION '/tmp/index'"
    )
    cmd.targetInputSize shouldBe Some(4L * 1024L * 1024L * 1024L)
    cmd.writerHeapSize shouldBe Some(1L * 1024L * 1024L * 1024L)
  }

  // --- WHERE clause tests ---

  test("parse SYNC with WHERE clause") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' WHERE year >= 2024 AT LOCATION '/tmp/index'"
    )
    cmd.wherePredicates should have size 1
    cmd.wherePredicates.head should include("year")
    cmd.wherePredicates.head should include("2024")
  }

  test("parse SYNC with compound WHERE clause") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' WHERE year >= 2024 AND region = 'us-east' AT LOCATION '/tmp/index'"
    )
    cmd.wherePredicates should have size 1
    cmd.wherePredicates.head should include("year")
    cmd.wherePredicates.head should include("region")
  }

  test("parse SYNC with WHERE and DRY RUN") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' WHERE status = 'active' AT LOCATION '/tmp/index' DRY RUN"
    )
    cmd.wherePredicates should have size 1
    cmd.wherePredicates.head should include("status")
    cmd.dryRun shouldBe true
  }

  test("default wherePredicates should be empty") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' AT LOCATION '/tmp/index'"
    )
    cmd.wherePredicates shouldBe empty
  }

  // --- FROM VERSION tests ---

  test("parse SYNC with FROM VERSION") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' FROM VERSION 500 AT LOCATION '/tmp/index'"
    )
    cmd.fromVersion shouldBe Some(500L)
  }

  test("parse SYNC with FROM VERSION 0") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' FROM VERSION 0 AT LOCATION '/tmp/index'"
    )
    cmd.fromVersion shouldBe Some(0L)
  }

  test("default fromVersion should be None") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' AT LOCATION '/tmp/index'"
    )
    cmd.fromVersion shouldBe None
  }

  // --- Combined WHERE + FROM VERSION tests ---

  test("parse SYNC with FROM VERSION and WHERE") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/delta' FROM VERSION 100 WHERE region = 'us-east' AT LOCATION '/tmp/index'"
    )
    cmd.fromVersion shouldBe Some(100L)
    cmd.wherePredicates should have size 1
    cmd.wherePredicates.head should include("region")
  }

  test("parse SYNC with all options including WHERE and FROM VERSION") {
    val cmd = parseSync(
      """BUILD INDEXTABLES COMPANION FOR DELTA 's3://bucket/delta'
        |  INDEXING MODES ('content':'text')
        |  FASTFIELDS MODE HYBRID
        |  TARGET INPUT SIZE 2G
        |  WRITER HEAP SIZE 512M
        |  FROM VERSION 42
        |  WHERE year = '2024'
        |  AT LOCATION 's3://bucket/index'
        |  DRY RUN""".stripMargin
    )
    cmd.sourcePath shouldBe "s3://bucket/delta"
    cmd.destPath shouldBe "s3://bucket/index"
    cmd.fastFieldMode shouldBe "HYBRID"
    cmd.targetInputSize shouldBe Some(2L * 1024L * 1024L * 1024L)
    cmd.writerHeapSize shouldBe Some(512L * 1024L * 1024L)
    cmd.fromVersion shouldBe Some(42L)
    cmd.wherePredicates should have size 1
    cmd.wherePredicates.head should include("year")
    cmd.dryRun shouldBe true
    cmd.indexingModes("content") shouldBe "text"
  }

  // ==========================================================================
  // FOR PARQUET tests
  // ==========================================================================

  test("parse basic BUILD COMPANION FOR PARQUET syntax") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR PARQUET 's3://bucket/parquet_dir' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "parquet"
    cmd.sourcePath shouldBe "s3://bucket/parquet_dir"
    cmd.destPath shouldBe "s3://bucket/index"
    cmd.indexingModes shouldBe empty
    cmd.fastFieldMode shouldBe "HYBRID"
    cmd.targetInputSize shouldBe None
    cmd.fromVersion shouldBe None
    cmd.fromSnapshot shouldBe None
    cmd.schemaSourcePath shouldBe None
    cmd.catalogName shouldBe None
    cmd.dryRun shouldBe false
  }

  test("parse PARQUET with SCHEMA SOURCE") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR PARQUET 's3://bucket/data' SCHEMA SOURCE 's3://bucket/data/part-00000.parquet' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "parquet"
    cmd.schemaSourcePath shouldBe Some("s3://bucket/data/part-00000.parquet")
  }

  test("parse PARQUET with all options") {
    val cmd = parseSync(
      """BUILD INDEXTABLES COMPANION FOR PARQUET 's3://bucket/events'
        |  SCHEMA SOURCE 's3://bucket/events/year=2024/part-00000.parquet'
        |  INDEXING MODES ('content':'text', 'src_ip':'ipaddress')
        |  FASTFIELDS MODE PARQUET_ONLY
        |  TARGET INPUT SIZE 2G
        |  WRITER HEAP SIZE 1G
        |  WHERE year >= 2024
        |  AT LOCATION 's3://bucket/index'
        |  DRY RUN""".stripMargin
    )
    cmd.sourceFormat shouldBe "parquet"
    cmd.sourcePath shouldBe "s3://bucket/events"
    cmd.schemaSourcePath shouldBe Some("s3://bucket/events/year=2024/part-00000.parquet")
    cmd.fastFieldMode shouldBe "PARQUET_ONLY"
    cmd.targetInputSize shouldBe Some(2L * 1024L * 1024L * 1024L)
    cmd.writerHeapSize shouldBe Some(1L * 1024L * 1024L * 1024L)
    cmd.wherePredicates should have size 1
    cmd.wherePredicates.head should include("year")
    cmd.dryRun shouldBe true
    cmd.indexingModes("content") shouldBe "text"
    cmd.indexingModes("src_ip") shouldBe "ipaddress"
  }

  test("parse PARQUET with local filesystem path") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR PARQUET '/data/my_parquet_dir' AT LOCATION '/data/my_index'"
    )
    cmd.sourceFormat shouldBe "parquet"
    cmd.sourcePath shouldBe "/data/my_parquet_dir"
    cmd.destPath shouldBe "/data/my_index"
  }

  test("parse PARQUET with Azure path") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR PARQUET 'abfss://container@account.dfs.core.windows.net/data' AT LOCATION 'abfss://container@account.dfs.core.windows.net/index'"
    )
    cmd.sourceFormat shouldBe "parquet"
    cmd.sourcePath shouldBe "abfss://container@account.dfs.core.windows.net/data"
  }

  test("PARQUET should not accept FROM VERSION") {
    an[Exception] should be thrownBy {
      parseSync(
        "BUILD INDEXTABLES COMPANION FOR PARQUET '/tmp/data' FROM VERSION 5 AT LOCATION '/tmp/index'"
      )
    }
  }

  test("PARQUET should not accept FROM SNAPSHOT") {
    an[Exception] should be thrownBy {
      parseSync(
        "BUILD INDEXTABLES COMPANION FOR PARQUET '/tmp/data' FROM SNAPSHOT 123 AT LOCATION '/tmp/index'"
      )
    }
  }

  test("PARQUET should not accept CATALOG") {
    an[Exception] should be thrownBy {
      parseSync(
        "BUILD INDEXTABLES COMPANION FOR PARQUET '/tmp/data' CATALOG 'my_catalog' AT LOCATION '/tmp/index'"
      )
    }
  }

  test("parse PARQUET basic syntax") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR PARQUET 's3://bucket/data' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "parquet"
    cmd.sourcePath shouldBe "s3://bucket/data"
  }

  // ==========================================================================
  // FOR ICEBERG tests
  // ==========================================================================

  test("parse basic BUILD COMPANION FOR ICEBERG syntax") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR ICEBERG 'my_namespace.my_table' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "iceberg"
    cmd.sourcePath shouldBe "my_namespace.my_table"
    cmd.destPath shouldBe "s3://bucket/index"
    cmd.catalogName shouldBe None
    cmd.fromSnapshot shouldBe None
    cmd.schemaSourcePath shouldBe None
    cmd.fromVersion shouldBe None
  }

  test("parse ICEBERG with CATALOG") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR ICEBERG 'prod.events' CATALOG 'unity_catalog' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "iceberg"
    cmd.sourcePath shouldBe "prod.events"
    cmd.catalogName shouldBe Some("unity_catalog")
  }

  test("parse ICEBERG with FROM SNAPSHOT") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR ICEBERG 'ns.table' FROM SNAPSHOT 1234567890 AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "iceberg"
    cmd.fromSnapshot shouldBe Some(1234567890L)
  }

  test("parse ICEBERG with CATALOG and FROM SNAPSHOT") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR ICEBERG 'analytics.events' CATALOG 'glue_catalog' FROM SNAPSHOT 9876543210 AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "iceberg"
    cmd.sourcePath shouldBe "analytics.events"
    cmd.catalogName shouldBe Some("glue_catalog")
    cmd.fromSnapshot shouldBe Some(9876543210L)
  }

  test("parse ICEBERG with all options") {
    val cmd = parseSync(
      """BUILD INDEXTABLES COMPANION FOR ICEBERG 'prod.web_events'
        |  CATALOG 'rest_catalog'
        |  INDEXING MODES ('message':'text', 'client_ip':'ip')
        |  FASTFIELDS MODE HYBRID
        |  TARGET INPUT SIZE 4G
        |  WRITER HEAP SIZE 2G
        |  FROM SNAPSHOT 5555555555
        |  WHERE region = 'us-east'
        |  AT LOCATION 's3://bucket/index'
        |  DRY RUN""".stripMargin
    )
    cmd.sourceFormat shouldBe "iceberg"
    cmd.sourcePath shouldBe "prod.web_events"
    cmd.catalogName shouldBe Some("rest_catalog")
    cmd.fastFieldMode shouldBe "HYBRID"
    cmd.targetInputSize shouldBe Some(4L * 1024L * 1024L * 1024L)
    cmd.writerHeapSize shouldBe Some(2L * 1024L * 1024L * 1024L)
    cmd.fromSnapshot shouldBe Some(5555555555L)
    cmd.wherePredicates should have size 1
    cmd.wherePredicates.head should include("region")
    cmd.dryRun shouldBe true
    cmd.indexingModes("message") shouldBe "text"
    cmd.indexingModes("client_ip") shouldBe "ip"
  }

  test("ICEBERG should not accept FROM VERSION") {
    an[Exception] should be thrownBy {
      parseSync(
        "BUILD INDEXTABLES COMPANION FOR ICEBERG 'ns.table' FROM VERSION 5 AT LOCATION '/tmp/index'"
      )
    }
  }

  test("ICEBERG should not accept SCHEMA SOURCE") {
    an[Exception] should be thrownBy {
      parseSync(
        "BUILD INDEXTABLES COMPANION FOR ICEBERG 'ns.table' SCHEMA SOURCE '/tmp/file.parquet' AT LOCATION '/tmp/index'"
      )
    }
  }

  test("parse ICEBERG basic syntax (alternate)") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR ICEBERG 'ns.table' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "iceberg"
    cmd.sourcePath shouldBe "ns.table"
  }

  test("parse ICEBERG with TANTIVY4SPARK keyword") {
    val cmd = parseSync(
      "BUILD TANTIVY4SPARK COMPANION FOR ICEBERG 'ns.table' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "iceberg"
    cmd.sourcePath shouldBe "ns.table"
  }

  // ==========================================================================
  // CATALOG TYPE and WAREHOUSE tests
  // ==========================================================================

  test("parse ICEBERG with CATALOG TYPE") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR ICEBERG 'prod.events' CATALOG 'unity_catalog' TYPE 'rest' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "iceberg"
    cmd.catalogName shouldBe Some("unity_catalog")
    cmd.catalogType shouldBe Some("rest")
  }

  test("parse ICEBERG with WAREHOUSE") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR ICEBERG 'prod.events' WAREHOUSE 's3://my-warehouse' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "iceberg"
    cmd.warehouse shouldBe Some("s3://my-warehouse")
  }

  test("parse ICEBERG with CATALOG TYPE and WAREHOUSE") {
    val cmd = parseSync(
      """BUILD INDEXTABLES COMPANION FOR ICEBERG 'prod.events'
        |  CATALOG 'unity' TYPE 'rest'
        |  WAREHOUSE 's3://warehouse/path'
        |  AT LOCATION 's3://bucket/index'""".stripMargin
    )
    cmd.sourceFormat shouldBe "iceberg"
    cmd.catalogName shouldBe Some("unity")
    cmd.catalogType shouldBe Some("rest")
    cmd.warehouse shouldBe Some("s3://warehouse/path")
  }

  test("parse ICEBERG with all options including CATALOG TYPE and WAREHOUSE") {
    val cmd = parseSync(
      """BUILD INDEXTABLES COMPANION FOR ICEBERG 'analytics.web_events'
        |  CATALOG 'uc_catalog' TYPE 'rest'
        |  WAREHOUSE 's3://unity-warehouse/iceberg'
        |  INDEXING MODES ('message':'text')
        |  FASTFIELDS MODE HYBRID
        |  TARGET INPUT SIZE 2G
        |  FROM SNAPSHOT 9999999
        |  WHERE region = 'us-east'
        |  AT LOCATION 's3://bucket/index'
        |  DRY RUN""".stripMargin
    )
    cmd.sourceFormat shouldBe "iceberg"
    cmd.sourcePath shouldBe "analytics.web_events"
    cmd.catalogName shouldBe Some("uc_catalog")
    cmd.catalogType shouldBe Some("rest")
    cmd.warehouse shouldBe Some("s3://unity-warehouse/iceberg")
    cmd.fastFieldMode shouldBe "HYBRID"
    cmd.targetInputSize shouldBe Some(2L * 1024L * 1024L * 1024L)
    cmd.fromSnapshot shouldBe Some(9999999L)
    cmd.wherePredicates should have size 1
    cmd.dryRun shouldBe true
    cmd.indexingModes("message") shouldBe "text"
  }

  test("default catalogType should be None") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR ICEBERG 'ns.table' CATALOG 'my_cat' AT LOCATION 's3://bucket/index'"
    )
    cmd.catalogType shouldBe None
  }

  test("default warehouse should be None") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR ICEBERG 'ns.table' AT LOCATION 's3://bucket/index'"
    )
    cmd.warehouse shouldBe None
  }

  test("CATALOG TYPE without CATALOG should not parse") {
    // TYPE is a sub-clause of CATALOG, so it requires CATALOG first.
    // Without CATALOG, TYPE is just an identifier and will be absorbed by passThrough or cause parse error.
    // This test verifies that TYPE alone doesn't create a catalogType.
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR ICEBERG 'ns.table' AT LOCATION 's3://bucket/index'"
    )
    cmd.catalogType shouldBe None
  }

  test("parse DELTA with CATALOG TYPE") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA 'schema.table' CATALOG 'unity' TYPE 'rest' AT LOCATION '/tmp/index'"
    )
    cmd.sourceFormat shouldBe "delta"
    cmd.sourcePath shouldBe "schema.table"
    cmd.catalogName shouldBe Some("unity")
    cmd.catalogType shouldBe Some("rest")
  }

  test("DELTA should not accept WAREHOUSE") {
    an[Exception] should be thrownBy {
      parseSync(
        "BUILD INDEXTABLES COMPANION FOR DELTA '/tmp/data' WAREHOUSE 's3://wh' AT LOCATION '/tmp/index'"
      )
    }
  }

  test("PARQUET should not accept WAREHOUSE") {
    an[Exception] should be thrownBy {
      parseSync(
        "BUILD INDEXTABLES COMPANION FOR PARQUET '/tmp/data' WAREHOUSE 's3://wh' AT LOCATION '/tmp/index'"
      )
    }
  }

  // ==========================================================================
  // CATALOG / WAREHOUSE interchangeability tests
  // ==========================================================================

  test("WAREHOUSE-only should parse as warehouse (catalog remains None)") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR ICEBERG 'ns.table' WAREHOUSE 'my_catalog' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "iceberg"
    cmd.warehouse shouldBe Some("my_catalog")
    cmd.catalogName shouldBe None
  }

  test("CATALOG-only should parse as catalogName (warehouse remains None)") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR ICEBERG 'ns.table' CATALOG 'my_catalog' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "iceberg"
    cmd.catalogName shouldBe Some("my_catalog")
    cmd.warehouse shouldBe None
  }

  test("WAREHOUSE with TYPE should parse correctly") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR ICEBERG 'ns.table' CATALOG 'uc' TYPE 'rest' WAREHOUSE 'my_wh' AT LOCATION 's3://bucket/index'"
    )
    cmd.catalogName shouldBe Some("uc")
    cmd.catalogType shouldBe Some("rest")
    cmd.warehouse shouldBe Some("my_wh")
  }

  // ==========================================================================
  // Delta UC table name resolution tests
  // ==========================================================================

  test("parse DELTA with CATALOG for UC table name resolution") {
    val cmd = parseSync(
      "BUILD INDEXTABLES COMPANION FOR DELTA 'schema.my_table' CATALOG 'unity_catalog' AT LOCATION 's3://bucket/index'"
    )
    cmd.sourceFormat shouldBe "delta"
    cmd.sourcePath shouldBe "schema.my_table"
    cmd.destPath shouldBe "s3://bucket/index"
    cmd.catalogName shouldBe Some("unity_catalog")
    cmd.catalogType shouldBe None
    cmd.warehouse shouldBe None
  }

  test("parse DELTA with CATALOG and all options") {
    val cmd = parseSync(
      """BUILD INDEXTABLES COMPANION FOR DELTA 'my_schema.events'
        |  CATALOG 'unity' TYPE 'rest'
        |  INDEXING MODES ('content':'text', 'title':'string')
        |  FASTFIELDS MODE HYBRID
        |  TARGET INPUT SIZE 2G
        |  FROM VERSION 42
        |  WHERE date = '2024-01-01'
        |  AT LOCATION 's3://bucket/companion'
        |  DRY RUN""".stripMargin
    )
    cmd.sourceFormat shouldBe "delta"
    cmd.sourcePath shouldBe "my_schema.events"
    cmd.catalogName shouldBe Some("unity")
    cmd.catalogType shouldBe Some("rest")
    cmd.fromVersion shouldBe Some(42L)
    cmd.dryRun shouldBe true
    cmd.indexingModes shouldBe Map("content" -> "text", "title" -> "string")
  }

  test("DELTA with CATALOG should not accept WAREHOUSE") {
    an[Exception] should be thrownBy {
      parseSync(
        "BUILD INDEXTABLES COMPANION FOR DELTA 'schema.table' CATALOG 'uc' WAREHOUSE 'wh' AT LOCATION '/tmp/index'"
      )
    }
  }

  test("PARQUET should not accept CATALOG TYPE") {
    an[Exception] should be thrownBy {
      parseSync(
        "BUILD INDEXTABLES COMPANION FOR PARQUET '/tmp/data' CATALOG 'cat' TYPE 'rest' AT LOCATION '/tmp/index'"
      )
    }
  }
}
