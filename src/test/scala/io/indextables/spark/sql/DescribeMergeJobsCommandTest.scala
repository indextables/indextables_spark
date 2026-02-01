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

import io.indextables.spark.merge.AsyncMergeOnWriteManager
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach

/** Tests for DESCRIBE INDEXTABLES MERGE JOBS command. */
class DescribeMergeJobsCommandTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .appName("DescribeMergeJobsCommandTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    // Reset the manager before each test for isolation
    AsyncMergeOnWriteManager.reset()
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    // Clean up after each test
    AsyncMergeOnWriteManager.reset()
    super.afterEach()
  }

  // ===== SQL Parsing Tests =====

  test("DESCRIBE INDEXTABLES MERGE JOBS should parse and execute") {
    val result = spark.sql("DESCRIBE INDEXTABLES MERGE JOBS")

    // Verify schema
    val columns = result.columns.toSet
    columns should contain("job_id")
    columns should contain("table_path")
    columns should contain("status")
    columns should contain("total_groups")
    columns should contain("completed_groups")
    columns should contain("total_batches")
    columns should contain("completed_batches")
    columns should contain("progress_pct")
    columns should contain("duration_ms")
    columns should contain("error_message")

    // Should return empty (no jobs running)
    result.count() shouldBe 0
  }

  test("DESCRIBE TANTIVY4SPARK MERGE JOBS should parse and execute") {
    val result = spark.sql("DESCRIBE TANTIVY4SPARK MERGE JOBS")

    // Should work with alternate keyword
    result.columns should contain("job_id")
    result.columns should contain("table_path")
    result.columns should contain("status")
    result.count() shouldBe 0
  }

  test("describe indextables merge jobs should be case insensitive") {
    val result = spark.sql("describe indextables merge jobs")

    result.columns should contain("job_id")
    result.count() shouldBe 0
  }

  // ===== Schema Tests =====

  test("DESCRIBE INDEXTABLES MERGE JOBS should have correct column types") {
    val result = spark.sql("DESCRIBE INDEXTABLES MERGE JOBS")
    val schema = result.schema

    schema("job_id").dataType.typeName shouldBe "string"
    schema("table_path").dataType.typeName shouldBe "string"
    schema("status").dataType.typeName shouldBe "string"
    schema("total_groups").dataType.typeName shouldBe "integer"
    schema("completed_groups").dataType.typeName shouldBe "integer"
    schema("total_batches").dataType.typeName shouldBe "integer"
    schema("completed_batches").dataType.typeName shouldBe "integer"
    schema("progress_pct").dataType.typeName shouldBe "double"
    schema("duration_ms").dataType.typeName shouldBe "long"
    schema("error_message").dataType.typeName shouldBe "string"
  }

  test("DESCRIBE INDEXTABLES MERGE JOBS should have nullable error_message") {
    val result = spark.sql("DESCRIBE INDEXTABLES MERGE JOBS")
    val schema = result.schema

    // error_message should be nullable (null when no error)
    schema("error_message").nullable shouldBe true

    // Other columns should not be nullable
    schema("job_id").nullable shouldBe false
    schema("table_path").nullable shouldBe false
    schema("status").nullable shouldBe false
  }

  // ===== Result Content Tests =====

  test("DESCRIBE INDEXTABLES MERGE JOBS returns empty when no jobs") {
    val result = spark.sql("DESCRIBE INDEXTABLES MERGE JOBS")
    result.count() shouldBe 0
  }

  test("DESCRIBE INDEXTABLES MERGE JOBS result should be queryable") {
    val result = spark.sql("DESCRIBE INDEXTABLES MERGE JOBS")

    // Register as temp view and query
    result.createOrReplaceTempView("merge_jobs")

    val runningJobs = spark.sql("SELECT * FROM merge_jobs WHERE status = 'RUNNING'")
    runningJobs.count() shouldBe 0

    val completedJobs = spark.sql("SELECT * FROM merge_jobs WHERE status = 'COMPLETED'")
    completedJobs.count() shouldBe 0
  }

  test("DESCRIBE INDEXTABLES MERGE JOBS should allow filtering by table_path pattern") {
    val result = spark.sql("DESCRIBE INDEXTABLES MERGE JOBS")
    result.createOrReplaceTempView("merge_jobs")

    // Filter for specific table paths
    val s3Jobs = spark.sql("SELECT * FROM merge_jobs WHERE table_path LIKE 's3://%'")
    // No jobs, but query should work
    s3Jobs.count() shouldBe 0

    val azureJobs = spark.sql("SELECT * FROM merge_jobs WHERE table_path LIKE 'abfss://%'")
    azureJobs.count() shouldBe 0
  }

  test("DESCRIBE INDEXTABLES MERGE JOBS should allow aggregations") {
    val result = spark.sql("DESCRIBE INDEXTABLES MERGE JOBS")
    result.createOrReplaceTempView("merge_jobs")

    // Group by status
    val byStatus = spark.sql("SELECT status, COUNT(*) as cnt FROM merge_jobs GROUP BY status")
    byStatus.count() shouldBe 0 // No jobs to group

    // Sum progress
    val totalProgress = spark.sql("SELECT SUM(completed_groups) as total_completed FROM merge_jobs")
    val rows          = totalProgress.collect()
    rows.length shouldBe 1
    // NULL sum when no rows
    rows(0).isNullAt(0) shouldBe true
  }
}
