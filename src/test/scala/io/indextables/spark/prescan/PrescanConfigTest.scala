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

package io.indextables.spark.prescan

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

import scala.collection.JavaConverters._

/** Unit tests for PrescanConfig. */
class PrescanConfigTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("PrescanConfigTest")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    PrescanSessionState.clearAll()
  }

  override def afterEach(): Unit = {
    PrescanSessionState.clearAll()
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  // ======================
  // Simple options map tests
  // ======================

  test("resolve from empty options should use defaults") {
    val config = PrescanConfig.resolve(Map.empty, defaultParallelism = 8)

    assert(!config.enabled)
    assert(config.minSplitThreshold == 16) // 2 * 8
    assert(config.maxConcurrency == 4 * Runtime.getRuntime.availableProcessors())
    assert(config.timeoutMs == 30000L)
  }

  test("resolve should parse enabled option") {
    val config = PrescanConfig.resolve(
      Map(PrescanConfig.PRESCAN_ENABLED -> "true"),
      defaultParallelism = 8
    )

    assert(config.enabled)
  }

  test("resolve should parse minSplitThreshold option") {
    val config = PrescanConfig.resolve(
      Map(PrescanConfig.PRESCAN_MIN_SPLIT_THRESHOLD -> "100"),
      defaultParallelism = 8
    )

    assert(config.minSplitThreshold == 100)
  }

  test("resolve should parse maxConcurrency option") {
    val config = PrescanConfig.resolve(
      Map(PrescanConfig.PRESCAN_MAX_CONCURRENCY -> "32"),
      defaultParallelism = 8
    )

    assert(config.maxConcurrency == 32)
  }

  test("resolve should parse timeoutMs option") {
    val config = PrescanConfig.resolve(
      Map(PrescanConfig.PRESCAN_TIMEOUT_MS -> "60000"),
      defaultParallelism = 8
    )

    assert(config.timeoutMs == 60000L)
  }

  test("resolve should parse all options together") {
    val config = PrescanConfig.resolve(
      Map(
        PrescanConfig.PRESCAN_ENABLED -> "true",
        PrescanConfig.PRESCAN_MIN_SPLIT_THRESHOLD -> "50",
        PrescanConfig.PRESCAN_MAX_CONCURRENCY -> "16",
        PrescanConfig.PRESCAN_TIMEOUT_MS -> "45000"
      ),
      defaultParallelism = 8
    )

    assert(config.enabled)
    assert(config.minSplitThreshold == 50)
    assert(config.maxConcurrency == 16)
    assert(config.timeoutMs == 45000L)
  }

  // ======================
  // SparkSession resolution tests
  // ======================

  test("resolve with SparkSession should use default parallelism for minSplitThreshold") {
    val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
    val config = PrescanConfig.resolve(spark, options, "s3://bucket/table")

    // Default is 2 * defaultParallelism
    // In local[2] mode, defaultParallelism should be 2
    assert(config.minSplitThreshold == 2 * spark.sparkContext.defaultParallelism)
  }

  test("resolve with SparkSession should prefer read options over spark config") {
    // Set spark config
    spark.conf.set(PrescanConfig.PRESCAN_ENABLED, "false")

    // Set read option to override
    val options = new CaseInsensitiveStringMap(
      Map(PrescanConfig.PRESCAN_ENABLED -> "true").asJava
    )

    val config = PrescanConfig.resolve(spark, options, "s3://bucket/table")

    assert(config.enabled)
  }

  test("resolve should use session state override first") {
    // Enable via session state
    PrescanSessionState.enable(spark, Some("s3://bucket/table"))

    // Set read option to disabled
    val options = new CaseInsensitiveStringMap(
      Map(PrescanConfig.PRESCAN_ENABLED -> "false").asJava
    )

    val config = PrescanConfig.resolve(spark, options, "s3://bucket/table")

    // Session state should override read options
    assert(config.enabled)
  }

  test("resolve should use global session state when table-specific not set") {
    // Enable globally via session state
    PrescanSessionState.enable(spark, None)

    val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)

    val config = PrescanConfig.resolve(spark, options, "s3://bucket/any_table")

    assert(config.enabled)
  }

  test("resolve should prefer table-specific session state over global") {
    // Enable globally
    PrescanSessionState.enable(spark, None)
    // Disable for specific table
    PrescanSessionState.disable(spark, Some("s3://bucket/table1"))

    val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)

    val config1 = PrescanConfig.resolve(spark, options, "s3://bucket/table1")
    val config2 = PrescanConfig.resolve(spark, options, "s3://bucket/table2")

    assert(!config1.enabled) // table-specific disabled
    assert(config2.enabled)   // falls back to global enabled
  }

  // ======================
  // Configuration key tests
  // ======================

  test("configuration keys should have correct values") {
    assert(PrescanConfig.PRESCAN_ENABLED == "spark.indextables.read.prescan.enabled")
    assert(PrescanConfig.PRESCAN_MIN_SPLIT_THRESHOLD == "spark.indextables.read.prescan.minSplitThreshold")
    assert(PrescanConfig.PRESCAN_MAX_CONCURRENCY == "spark.indextables.read.prescan.maxConcurrency")
    assert(PrescanConfig.PRESCAN_TIMEOUT_MS == "spark.indextables.read.prescan.timeoutMs")
  }

  test("DEFAULT_TIMEOUT_MS should be 30 seconds") {
    assert(PrescanConfig.DEFAULT_TIMEOUT_MS == 30000L)
  }
}
