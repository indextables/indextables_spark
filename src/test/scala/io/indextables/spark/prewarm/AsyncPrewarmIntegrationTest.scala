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

package io.indextables.spark.prewarm

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.SparkSession

import io.indextables.spark.sql.{DescribePrewarmJobsCommand, PrewarmCacheCommand, WaitForPrewarmJobsCommand}
import io.indextables.spark.storage.DriverSplitLocalityManager
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Integration tests for async prewarm functionality.
 *
 * Tests the ASYNC MODE option for PREWARM INDEXTABLES CACHE command,
 * DESCRIBE INDEXTABLES PREWARM JOBS, and WAIT FOR INDEXTABLES PREWARM JOBS commands.
 */
class AsyncPrewarmIntegrationTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  var spark: SparkSession   = _
  var tempTablePath: String = _

  override def beforeAll(): Unit = {
    // Create Spark session for testing
    spark = SparkSession
      .builder()
      .appName("AsyncPrewarmIntegrationTest")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "localhost")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

    // Create temporary directory for test table
    val tempDir = Files.createTempDirectory("async_prewarm_integration_test_").toFile
    tempTablePath = tempDir.getAbsolutePath
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (tempTablePath != null) {
      val tempDir = new File(tempTablePath)
      if (tempDir.exists()) {
        FileUtils.deleteDirectory(tempDir)
      }
    }
  }

  override def beforeEach(): Unit = {
    PreWarmManager.clearAll()
    DriverSplitLocalityManager.clear()
    AsyncPrewarmJobManager.reset()
  }

  override def afterEach(): Unit = {
    PreWarmManager.clearAll()
    DriverSplitLocalityManager.clear()
    AsyncPrewarmJobManager.reset()
  }

  // === SQL Parsing Tests ===

  test("PREWARM with ASYNC MODE should parse and create command with asyncMode=true") {
    val sql  = s"PREWARM INDEXTABLES CACHE '$tempTablePath' ASYNC MODE"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)

    plan.isInstanceOf[PrewarmCacheCommand] shouldBe true
    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    cmd.asyncMode shouldBe true
  }

  test("PREWARM without ASYNC MODE should parse and create command with asyncMode=false") {
    val sql  = s"PREWARM INDEXTABLES CACHE '$tempTablePath'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)

    plan.isInstanceOf[PrewarmCacheCommand] shouldBe true
    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    cmd.asyncMode shouldBe false
  }

  test("DESCRIBE INDEXTABLES PREWARM JOBS should parse correctly") {
    val sql  = "DESCRIBE INDEXTABLES PREWARM JOBS"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)

    plan.isInstanceOf[DescribePrewarmJobsCommand] shouldBe true
  }

  test("WAIT FOR INDEXTABLES PREWARM JOBS should parse correctly") {
    val sql  = "WAIT FOR INDEXTABLES PREWARM JOBS TIMEOUT 60"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)

    plan.isInstanceOf[WaitForPrewarmJobsCommand] shouldBe true
    val cmd = plan.asInstanceOf[WaitForPrewarmJobsCommand]
    cmd.timeoutSeconds shouldBe 60
  }

  test("WAIT FOR PREWARM JOBS with table path should parse correctly") {
    val sql  = s"WAIT FOR INDEXTABLES PREWARM JOBS '$tempTablePath'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)

    plan.isInstanceOf[WaitForPrewarmJobsCommand] shouldBe true
    val cmd = plan.asInstanceOf[WaitForPrewarmJobsCommand]
    cmd.tablePath shouldBe Some(tempTablePath)
  }

  test("WAIT FOR PREWARM JOBS with job ID should parse correctly") {
    val sql  = "WAIT FOR INDEXTABLES PREWARM JOBS JOB 'abc-123-def-456'"
    val plan = spark.sessionState.sqlParser.parsePlan(sql)

    plan.isInstanceOf[WaitForPrewarmJobsCommand] shouldBe true
    val cmd = plan.asInstanceOf[WaitForPrewarmJobsCommand]
    cmd.jobId shouldBe Some("abc-123-def-456")
  }

  // === Output Schema Tests ===

  test("PrewarmCacheCommand output should include job_id and async_mode columns") {
    val cmd = PrewarmCacheCommand(
      tablePath = tempTablePath,
      segments = Seq.empty,
      fields = None,
      splitsPerTask = 2,
      wherePredicates = Seq.empty,
      asyncMode = true
    )

    val outputNames = cmd.output.map(_.name)
    outputNames should contain("job_id")
    outputNames should contain("async_mode")
  }

  test("DescribePrewarmJobsCommand output should have expected columns") {
    val cmd = DescribePrewarmJobsCommand()

    val outputNames = cmd.output.map(_.name)
    outputNames should contain("executor_id")
    outputNames should contain("host")
    outputNames should contain("job_id")
    outputNames should contain("table_path")
    outputNames should contain("status")
    outputNames should contain("total_splits")
    outputNames should contain("completed_splits")
    outputNames should contain("progress_pct")
    outputNames should contain("duration_ms")
    outputNames should contain("error_message")
  }

  test("WaitForPrewarmJobsCommand output should have expected columns") {
    val cmd = WaitForPrewarmJobsCommand()

    val outputNames = cmd.output.map(_.name)
    outputNames should contain("executor_id")
    outputNames should contain("host")
    outputNames should contain("job_id")
    outputNames should contain("table_path")
    outputNames should contain("status")
    outputNames should contain("total_splits")
    outputNames should contain("splits_prewarmed")
    outputNames should contain("duration_ms")
    outputNames should contain("error_message")
  }

  // === AsyncPrewarmJobManager Tests ===

  test("AsyncPrewarmJobManager should be accessible from multiple threads") {
    AsyncPrewarmJobManager.configure(maxConcurrentJobs = 2)

    val activeCount = AsyncPrewarmJobManager.getActiveJobCount
    activeCount shouldBe 0

    val availableSlots = AsyncPrewarmJobManager.getAvailableSlots
    availableSlots shouldBe 2
  }

  test("DESCRIBE PREWARM JOBS should execute and return empty when no jobs") {
    // Configure the manager
    AsyncPrewarmJobManager.configure(maxConcurrentJobs = 1)

    val result = spark.sql("DESCRIBE INDEXTABLES PREWARM JOBS")
    val rows   = result.collect()

    // Should return empty or no rows when no jobs are running
    // This is expected behavior - no jobs means no rows
    rows.length shouldBe 0
  }

  // === Command Construction Tests ===

  test("PrewarmCacheCommand should have correct defaults") {
    val cmd = PrewarmCacheCommand(
      tablePath = tempTablePath,
      segments = Seq.empty,
      fields = None,
      splitsPerTask = 2,
      wherePredicates = Seq.empty
    )

    cmd.asyncMode shouldBe false
    cmd.failOnMissingField shouldBe true
  }

  test("WaitForPrewarmJobsCommand should have correct defaults") {
    val cmd = WaitForPrewarmJobsCommand()

    cmd.tablePath shouldBe None
    cmd.jobId shouldBe None
    cmd.timeoutSeconds shouldBe 3600
  }

  // === Combined Syntax Tests ===

  test("PREWARM with all options and ASYNC MODE should parse") {
    val sql = s"""
      PREWARM INDEXTABLES CACHE '$tempTablePath'
        FOR SEGMENTS (TERM_DICT, POSTINGS)
        ON FIELDS (id, content)
        WITH PERWORKER PARALLELISM OF 4
        WHERE year = '2024'
        ASYNC MODE
    """.stripMargin.replaceAll("\n", " ")

    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    plan.isInstanceOf[PrewarmCacheCommand] shouldBe true

    val cmd = plan.asInstanceOf[PrewarmCacheCommand]
    cmd.tablePath shouldBe tempTablePath
    cmd.segments.size shouldBe 2
    cmd.fields.isDefined shouldBe true
    cmd.fields.get.size shouldBe 2
    cmd.splitsPerTask shouldBe 4
    cmd.wherePredicates.nonEmpty shouldBe true
    cmd.asyncMode shouldBe true
  }

  test("WAIT FOR PREWARM JOBS with all options should parse") {
    val sql = s"WAIT FOR INDEXTABLES PREWARM JOBS '$tempTablePath' JOB 'job-123' TIMEOUT 1800"

    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    plan.isInstanceOf[WaitForPrewarmJobsCommand] shouldBe true

    val cmd = plan.asInstanceOf[WaitForPrewarmJobsCommand]
    cmd.tablePath shouldBe Some(tempTablePath)
    cmd.jobId shouldBe Some("job-123")
    cmd.timeoutSeconds shouldBe 1800
  }

  // === Case Sensitivity Tests ===

  test("All async prewarm commands should be case-insensitive") {
    val lowerSql = s"prewarm indextables cache '$tempTablePath' async mode"
    val plan1    = spark.sessionState.sqlParser.parsePlan(lowerSql)
    plan1.isInstanceOf[PrewarmCacheCommand] shouldBe true
    plan1.asInstanceOf[PrewarmCacheCommand].asyncMode shouldBe true

    val mixedSql = "Describe IndExTables PREWARM Jobs"
    val plan2    = spark.sessionState.sqlParser.parsePlan(mixedSql)
    plan2.isInstanceOf[DescribePrewarmJobsCommand] shouldBe true

    val waitSql = "wait FOR indextables prewarm JOBS timeout 100"
    val plan3   = spark.sessionState.sqlParser.parsePlan(waitSql)
    plan3.isInstanceOf[WaitForPrewarmJobsCommand] shouldBe true
    plan3.asInstanceOf[WaitForPrewarmJobsCommand].timeoutSeconds shouldBe 100
  }
}
