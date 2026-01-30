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

package io.indextables.spark.indexquery

import io.indextables.spark.TestBase
import io.indextables.spark.core.FiltersToQueryConverter

/**
 * Test cases for wildcard query patterns in IndexQuery.
 *
 * This tests verifies that all wildcard patterns work correctly:
 * - Leading wildcards: `*term` (e.g., `*Configuration`)
 * - Trailing wildcards: `term*` (e.g., `Config*`)
 * - Embedded wildcards: `te*rm` (e.g., `Con*tion`)
 * - Combined wildcards: `*term*` (e.g., `*Broker*`)
 *
 * GitHub Issue #127: Leading wildcards were throwing verbose Java exceptions
 * instead of working. The fix automatically transforms leading wildcard queries
 * to use explicit field syntax which Tantivy supports.
 */
class WildcardQueryTest extends TestBase {

  // ============================================================================
  // Unit tests for the transformation helper
  // ============================================================================

  test("transformLeadingWildcardQuery should add field prefix for leading asterisk") {
    val result = FiltersToQueryConverter.transformLeadingWildcardQuery("*Configuration", "message")
    assert(result == "message:*Configuration")
  }

  test("transformLeadingWildcardQuery should add field prefix for leading question mark") {
    val result = FiltersToQueryConverter.transformLeadingWildcardQuery("?onfig", "message")
    assert(result == "message:?onfig")
  }

  test("transformLeadingWildcardQuery should not modify trailing wildcard") {
    val result = FiltersToQueryConverter.transformLeadingWildcardQuery("Config*", "message")
    assert(result == "Config*")
  }

  test("transformLeadingWildcardQuery should not modify embedded wildcard") {
    val result = FiltersToQueryConverter.transformLeadingWildcardQuery("Con*tion", "message")
    assert(result == "Con*tion")
  }

  test("transformLeadingWildcardQuery should add field prefix for combined wildcards starting with asterisk") {
    val result = FiltersToQueryConverter.transformLeadingWildcardQuery("*Broker*", "message")
    assert(result == "message:*Broker*")
  }

  test("transformLeadingWildcardQuery should not modify query that already has field prefix") {
    val result = FiltersToQueryConverter.transformLeadingWildcardQuery("field:*Configuration", "message")
    assert(result == "field:*Configuration")
  }

  test("transformLeadingWildcardQuery should transform leading wildcard in boolean AND expression") {
    val result = FiltersToQueryConverter.transformLeadingWildcardQuery("*term AND other", "message")
    assert(result == "message:*term AND other")
  }

  test("transformLeadingWildcardQuery should transform leading wildcard after AND") {
    val result = FiltersToQueryConverter.transformLeadingWildcardQuery("bob AND *wildcard*", "msg")
    assert(result == "bob AND msg:*wildcard*")
  }

  test("transformLeadingWildcardQuery should transform multiple leading wildcards in complex expression") {
    val result = FiltersToQueryConverter.transformLeadingWildcardQuery("bob AND *ildcar* OR (sally AND *oogl*)", "msg")
    assert(result == "bob AND msg:*ildcar* OR (sally AND msg:*oogl*)")
  }

  test("transformLeadingWildcardQuery should transform leading wildcard after open paren") {
    val result = FiltersToQueryConverter.transformLeadingWildcardQuery("(sally AND *oogl*)", "msg")
    assert(result == "(sally AND msg:*oogl*)")
  }

  test("transformLeadingWildcardQuery should not double-prefix already prefixed wildcards in complex query") {
    val result = FiltersToQueryConverter.transformLeadingWildcardQuery("bob AND field:*wildcard*", "msg")
    assert(result == "bob AND field:*wildcard*")
  }

  test("transformLeadingWildcardQuery should handle mixed prefixed and unprefixed wildcards") {
    val result = FiltersToQueryConverter.transformLeadingWildcardQuery("field:*prefixed* AND *unprefixed*", "msg")
    assert(result == "field:*prefixed* AND msg:*unprefixed*")
  }

  test("transformLeadingWildcardQuery should not modify trailing wildcards in complex expression") {
    val result = FiltersToQueryConverter.transformLeadingWildcardQuery("bob AND wildcard* OR sally", "msg")
    assert(result == "bob AND wildcard* OR sally")
  }

  test("hasLeadingWildcard should detect leading asterisk") {
    assert(FiltersToQueryConverter.hasLeadingWildcard("*Configuration"))
    assert(FiltersToQueryConverter.hasLeadingWildcard("*Broker*"))
    assert(FiltersToQueryConverter.hasLeadingWildcard("?onfig"))
  }

  test("hasLeadingWildcard should not detect non-leading wildcards") {
    assert(!FiltersToQueryConverter.hasLeadingWildcard("Config*"))
    assert(!FiltersToQueryConverter.hasLeadingWildcard("Con*tion"))
    assert(!FiltersToQueryConverter.hasLeadingWildcard("normal term"))
  }

  // ============================================================================
  // Integration tests for wildcard queries
  // ============================================================================

  test("trailing wildcard query should work (Config*)") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/trailing_wildcard_test"

    val testData = Seq(
      (1, "ConfigurationManager started", "info"),
      (2, "ConfigService initialized", "info"),
      (3, "Database connection failed", "error")
    ).toDF("id", "message", "level")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.message", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("trailing_wildcard_test")

    // Trailing wildcard should match "ConfigurationManager" and "ConfigService"
    val results = spark.sql("""
      SELECT id, message FROM trailing_wildcard_test
      WHERE message indexquery 'Config*'
    """).collect()

    assert(results.length == 2, s"Expected 2 results for 'Config*', got ${results.length}")
  }

  test("embedded wildcard query should work (Con*tion)") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/embedded_wildcard_test"

    val testData = Seq(
      (1, "Configuration loaded", "info"),
      (2, "Connection established", "info"),
      (3, "Database ready", "info")
    ).toDF("id", "message", "level")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.message", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("embedded_wildcard_test")

    // Embedded wildcard should match "Configuration" and "Connection"
    val results = spark.sql("""
      SELECT id, message FROM embedded_wildcard_test
      WHERE message indexquery 'Con*tion'
    """).collect()

    assert(results.length == 2, s"Expected 2 results for 'Con*tion', got ${results.length}")
  }

  test("leading wildcard query should work (*Configuration) - GitHub Issue #127") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/leading_wildcard_test"

    val testData = Seq(
      (1, "AppConfiguration started", "info"),
      (2, "SystemConfiguration loaded", "info"),
      (3, "Database ready", "info")
    ).toDF("id", "message", "level")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.message", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("leading_wildcard_test")

    // Leading wildcard should match "AppConfiguration" and "SystemConfiguration"
    // This was the pattern that previously threw an exception (Issue #127)
    val results = spark.sql("""
      SELECT id, message FROM leading_wildcard_test
      WHERE message indexquery '*Configuration'
    """).collect()

    assert(results.length == 2, s"Expected 2 results for '*Configuration', got ${results.length}")
  }

  test("combined wildcard query should work (*Broker*) - GitHub Issue #127") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/combined_wildcard_test"

    val testData = Seq(
      (1, "MessageBrokerService started", "info"),
      (2, "BrokerConnection established", "info"),
      (3, "Database ready", "info")
    ).toDF("id", "message", "level")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.message", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("combined_wildcard_test")

    // Combined wildcard (*Broker*) should match documents containing "Broker"
    // This was the pattern that previously threw an exception (Issue #127)
    val results = spark.sql("""
      SELECT id, message FROM combined_wildcard_test
      WHERE message indexquery '*Broker*'
    """).collect()

    assert(results.length == 2, s"Expected 2 results for '*Broker*', got ${results.length}")
  }

  test("question mark wildcard should work (Con?ig)") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/question_wildcard_test"

    // Both "Config" and "Konfig" match the pattern "?onfig" (single char wildcard at start)
    // Note: "Con?ig" matches "Config" but not "Confog" (different vowel)
    val testData = Seq(
      (1, "Config loaded", "info"),
      (2, "Database ready", "info")
    ).toDF("id", "message", "level")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.message", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("question_wildcard_test")

    // Question mark matches single character - "Con?ig" matches "Config"
    val results = spark.sql("""
      SELECT id, message FROM question_wildcard_test
      WHERE message indexquery 'Con?ig'
    """).collect()

    assert(results.length == 1, s"Expected 1 result for 'Con?ig', got ${results.length}")
  }

  test("leading question mark wildcard should work (?onfig) - GitHub Issue #127") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/leading_question_wildcard_test"

    val testData = Seq(
      (1, "Config loaded", "info"),
      (2, "Konfig misspelled", "info"),
      (3, "Database ready", "info")
    ).toDF("id", "message", "level")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.message", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("leading_question_wildcard_test")

    // Leading question mark should match single character at start
    val results = spark.sql("""
      SELECT id, message FROM leading_question_wildcard_test
      WHERE message indexquery '?onfig'
    """).collect()

    assert(results.length == 2, s"Expected 2 results for '?onfig', got ${results.length}")
  }

  test("wildcard with aggregation should work - GitHub Issue #127") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/wildcard_agg_test"

    val testData = Seq(
      (1, "AppConfiguration started", 100.0),
      (2, "SystemConfiguration loaded", 200.0),
      (3, "Database ready", 50.0)
    ).toDF("id", "message", "score")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.message", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("wildcard_agg_test")

    // COUNT with leading wildcard
    val countResult = spark.sql("""
      SELECT COUNT(*) FROM wildcard_agg_test
      WHERE message indexquery '*Configuration'
    """).collect()

    assert(countResult.length == 1, s"Expected 1 row, got ${countResult.length}")
    assert(countResult(0).getLong(0) == 2, s"Expected count of 2, got ${countResult(0).getLong(0)}")

    // SUM with leading wildcard
    val sumResult = spark.sql("""
      SELECT SUM(score) FROM wildcard_agg_test
      WHERE message indexquery '*Configuration'
    """).collect()

    assert(sumResult.length == 1, s"Expected 1 row, got ${sumResult.length}")
    assert(sumResult(0).getDouble(0) == 300.0, s"Expected sum of 300.0, got ${sumResult(0).getDouble(0)}")
  }

  test("wildcard combined with other filters should work - GitHub Issue #127") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/wildcard_combined_test"

    val testData = Seq(
      (1, "AppConfiguration started", "info"),
      (2, "SystemConfiguration loaded", "error"),
      (3, "AppConfiguration failed", "error")
    ).toDF("id", "message", "level")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.message", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("wildcard_combined_test")

    // Leading wildcard combined with Spark filter
    val results = spark.sql("""
      SELECT id, message FROM wildcard_combined_test
      WHERE level = 'error' AND message indexquery '*Configuration'
    """).collect()

    assert(results.length == 2, s"Expected 2 results for combined filter, got ${results.length}")
  }

  test("complex boolean expression with multiple leading wildcards should work - GitHub Issue #127") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/complex_boolean_wildcard_test"

    val testData = Seq(
      (1, "bob started the ConfigurationManager", "info"),
      (2, "sally loaded the GoogleService", "info"),
      (3, "bob tested the WildcardHandler", "info"),
      (4, "alice initialized DatabaseConnection", "info"),
      (5, "sally ran the GoogleAnalytics", "info")
    ).toDF("id", "message", "level")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.message", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("complex_boolean_wildcard_test")

    // Complex boolean expression with multiple leading wildcards:
    // Should match documents containing "bob" AND words ending in "ildcar"
    // OR documents containing "sally" AND words starting with "Goo" (ending in "oogl" pattern)
    // This tests the transformation: bob AND *ildcar* -> bob AND message:*ildcar*
    val results = spark.sql("""
      SELECT id, message FROM complex_boolean_wildcard_test
      WHERE message indexquery 'bob AND *ildcar*'
    """).collect()

    // Should match row 3: "bob tested the WildcardHandler" (bob AND *ildcar* matches "Wildcard")
    assert(results.length == 1, s"Expected 1 result for 'bob AND *ildcar*', got ${results.length}")
    assert(results(0).getInt(0) == 3, s"Expected id 3, got ${results(0).getInt(0)}")
  }

  test("leading wildcard after OR in boolean expression should work - GitHub Issue #127") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/or_wildcard_test"

    val testData = Seq(
      (1, "AppConfiguration started", "info"),
      (2, "SystemConfiguration loaded", "info"),
      (3, "BrokerService initialized", "info"),
      (4, "Database ready", "info")
    ).toDF("id", "message", "level")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.message", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("or_wildcard_test")

    // Test leading wildcard after OR: database OR *Configuration
    // Should match "Database" OR anything ending with "Configuration"
    val results = spark.sql("""
      SELECT id, message FROM or_wildcard_test
      WHERE message indexquery 'database OR *Configuration'
    """).collect()

    // Should match rows 1, 2, 4 (AppConfiguration, SystemConfiguration, Database)
    assert(results.length == 3, s"Expected 3 results for 'database OR *Configuration', got ${results.length}")
  }

  test("leading wildcard inside parentheses should work - GitHub Issue #127") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/paren_wildcard_test"

    val testData = Seq(
      (1, "AppConfiguration started", "info"),
      (2, "BrokerService initialized", "info"),
      (3, "Database ready", "info")
    ).toDF("id", "message", "level")

    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.message", "text")
      .mode("overwrite")
      .save(testPath)

    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("paren_wildcard_test")

    // Test leading wildcard inside parentheses: (started OR *Service)
    val results = spark.sql("""
      SELECT id, message FROM paren_wildcard_test
      WHERE message indexquery '(started OR *Service)'
    """).collect()

    // Should match rows 1 (started) and 2 (BrokerService)
    assert(results.length == 2, s"Expected 2 results for '(started OR *Service)', got ${results.length}")
  }
}
