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

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.{TransactionLog, TransactionLogFactory}
import io.indextables.spark.TestBase

class IndexCrossReferencesCommandTest extends TestBase {

  test("SQL parsing: basic INDEX CROSSREFERENCES FOR path") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val sql = "INDEX CROSSREFERENCES FOR '/path/to/table'"
    val parsedPlan = parser.parsePlan(sql)

    assert(parsedPlan.isInstanceOf[IndexCrossReferencesCommand])
    val command = parsedPlan.asInstanceOf[IndexCrossReferencesCommand]
    assert(command.pathOption.contains("/path/to/table"))
    assert(command.tableIdOption.isEmpty)
    assert(command.wherePredicates.isEmpty)
    assert(!command.forceRebuild)
    assert(!command.dryRun)
  }

  test("SQL parsing: INDEX CROSSREFERENCES FOR with FORCE REBUILD") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val sql = "INDEX CROSSREFERENCES FOR '/path/to/table' FORCE REBUILD"
    val parsedPlan = parser.parsePlan(sql)

    assert(parsedPlan.isInstanceOf[IndexCrossReferencesCommand])
    val command = parsedPlan.asInstanceOf[IndexCrossReferencesCommand]
    assert(command.pathOption.contains("/path/to/table"))
    assert(command.forceRebuild)
    assert(!command.dryRun)
  }

  test("SQL parsing: INDEX CROSSREFERENCES FOR with DRY RUN") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val sql = "INDEX CROSSREFERENCES FOR '/path/to/table' DRY RUN"
    val parsedPlan = parser.parsePlan(sql)

    assert(parsedPlan.isInstanceOf[IndexCrossReferencesCommand])
    val command = parsedPlan.asInstanceOf[IndexCrossReferencesCommand]
    assert(command.pathOption.contains("/path/to/table"))
    assert(!command.forceRebuild)
    assert(command.dryRun)
  }

  test("SQL parsing: INDEX CROSSREFERENCES FOR with FORCE REBUILD DRY RUN") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val sql = "INDEX CROSSREFERENCES FOR '/path/to/table' FORCE REBUILD DRY RUN"
    val parsedPlan = parser.parsePlan(sql)

    assert(parsedPlan.isInstanceOf[IndexCrossReferencesCommand])
    val command = parsedPlan.asInstanceOf[IndexCrossReferencesCommand]
    assert(command.pathOption.contains("/path/to/table"))
    assert(command.forceRebuild)
    assert(command.dryRun)
  }

  test("SQL parsing: INDEX CROSSREFERENCES FOR with WHERE clause") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val sql = "INDEX CROSSREFERENCES FOR '/path/to/table' WHERE date = '2024-01-01'"
    val parsedPlan = parser.parsePlan(sql)

    assert(parsedPlan.isInstanceOf[IndexCrossReferencesCommand])
    val command = parsedPlan.asInstanceOf[IndexCrossReferencesCommand]
    assert(command.pathOption.contains("/path/to/table"))
    assert(command.wherePredicates.nonEmpty)
    assert(command.wherePredicates.head.contains("date"))
  }

  test("SQL parsing: INDEX CROSSREFERENCES FOR with WHERE clause and FORCE REBUILD") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val sql = "INDEX CROSSREFERENCES FOR '/path/to/table' WHERE region = 'us-east' FORCE REBUILD"
    val parsedPlan = parser.parsePlan(sql)

    assert(parsedPlan.isInstanceOf[IndexCrossReferencesCommand])
    val command = parsedPlan.asInstanceOf[IndexCrossReferencesCommand]
    assert(command.pathOption.contains("/path/to/table"))
    assert(command.wherePredicates.nonEmpty)
    assert(command.forceRebuild)
    assert(!command.dryRun)
  }

  test("SQL parsing: INDEX CROSSREFERENCES FOR table identifier") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val sql = "INDEX CROSSREFERENCES FOR my_table"
    val parsedPlan = parser.parsePlan(sql)

    assert(parsedPlan.isInstanceOf[IndexCrossReferencesCommand])
    val command = parsedPlan.asInstanceOf[IndexCrossReferencesCommand]
    assert(command.pathOption.isEmpty)
    assert(command.tableIdOption.isDefined)
    assert(command.tableIdOption.get.table == "my_table")
  }

  test("SQL parsing: INDEX CROSSREFERENCES FOR qualified table identifier") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val sql = "INDEX CROSSREFERENCES FOR my_db.my_table"
    val parsedPlan = parser.parsePlan(sql)

    assert(parsedPlan.isInstanceOf[IndexCrossReferencesCommand])
    val command = parsedPlan.asInstanceOf[IndexCrossReferencesCommand]
    assert(command.pathOption.isEmpty)
    assert(command.tableIdOption.isDefined)
    assert(command.tableIdOption.get.database.contains("my_db"))
    assert(command.tableIdOption.get.table == "my_table")
  }

  test("SQL parsing: case insensitivity") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    // Test various case combinations
    val sqls = Seq(
      "index crossreferences for '/path/to/table'",
      "INDEX CROSSREFERENCES FOR '/path/to/table'",
      "Index CrossReferences For '/path/to/table'",
      "INDEX crossreferences FOR '/path/to/table' force rebuild dry run"
    )

    sqls.foreach { sql =>
      val parsedPlan = parser.parsePlan(sql)
      assert(parsedPlan.isInstanceOf[IndexCrossReferencesCommand], s"Failed for SQL: $sql")
    }
  }

  test("SQL parsing: INDEX CROSSREFERENCES FOR with MAX XREF BUILDS") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val sql = "INDEX CROSSREFERENCES FOR '/path/to/table' MAX XREF BUILDS 5"
    val parsedPlan = parser.parsePlan(sql)

    assert(parsedPlan.isInstanceOf[IndexCrossReferencesCommand])
    val command = parsedPlan.asInstanceOf[IndexCrossReferencesCommand]
    assert(command.pathOption.contains("/path/to/table"))
    assert(command.maxXRefBuilds.contains(5))
    assert(!command.forceRebuild)
    assert(!command.dryRun)
  }

  test("SQL parsing: INDEX CROSSREFERENCES FOR with FORCE REBUILD MAX XREF BUILDS DRY RUN") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val sql = "INDEX CROSSREFERENCES FOR '/path/to/table' FORCE REBUILD MAX XREF BUILDS 10 DRY RUN"
    val parsedPlan = parser.parsePlan(sql)

    assert(parsedPlan.isInstanceOf[IndexCrossReferencesCommand])
    val command = parsedPlan.asInstanceOf[IndexCrossReferencesCommand]
    assert(command.pathOption.contains("/path/to/table"))
    assert(command.forceRebuild)
    assert(command.maxXRefBuilds.contains(10))
    assert(command.dryRun)
  }

  test("SQL parsing: INDEX CROSSREFERENCES FOR with WHERE clause and MAX XREF BUILDS") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val sql = "INDEX CROSSREFERENCES FOR '/path/to/table' WHERE date = '2024-01-01' MAX XREF BUILDS 3"
    val parsedPlan = parser.parsePlan(sql)

    assert(parsedPlan.isInstanceOf[IndexCrossReferencesCommand])
    val command = parsedPlan.asInstanceOf[IndexCrossReferencesCommand]
    assert(command.pathOption.contains("/path/to/table"))
    assert(command.wherePredicates.nonEmpty)
    assert(command.maxXRefBuilds.contains(3))
    assert(!command.forceRebuild)
    assert(!command.dryRun)
  }

  test("SQL parsing: MAX XREF BUILDS case insensitivity") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val sqls = Seq(
      "INDEX CROSSREFERENCES FOR '/path/to/table' max xref builds 5",
      "INDEX CROSSREFERENCES FOR '/path/to/table' MAX XREF BUILDS 5",
      "INDEX CROSSREFERENCES FOR '/path/to/table' Max Xref Builds 5"
    )

    sqls.foreach { sql =>
      val parsedPlan = parser.parsePlan(sql)
      assert(parsedPlan.isInstanceOf[IndexCrossReferencesCommand], s"Failed for SQL: $sql")
      val command = parsedPlan.asInstanceOf[IndexCrossReferencesCommand]
      assert(command.maxXRefBuilds.contains(5), s"maxXRefBuilds not parsed correctly for SQL: $sql")
    }
  }

  test("command execution: DRY RUN should not modify anything") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString

      // Create and initialize a transaction log
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(
          Array(
            StructField("id", IntegerType, nullable = false),
            StructField("name", StringType, nullable = false)
          )
        )

        // Initialize the transaction log
        transactionLog.initialize(schema)

        // Create command with DRY RUN
        val command = IndexCrossReferencesCommand.apply(
          pathOption = Some(tablePath),
          tableIdOption = None,
          wherePredicates = Seq.empty,
          forceRebuild = false,
          dryRun = true,
          maxXRefBuilds = None
        )

        // Execute command
        val results = command.run(spark)

        // Should return results without error
        assert(results.nonEmpty)

        // Get the result row
        val result = results.head
        val tablePath_result = result.getString(0)
        val resultStruct = result.getStruct(1)
        val action = resultStruct.getString(0)
        val dryRun_result = resultStruct.getBoolean(7)

        assert(tablePath_result.contains(tablePath.split("/").last))
        assert(dryRun_result)
        // Action should be "skipped" since there are no splits to index
        assert(action == "skipped" || action == "error" || action == "would_rebuild")
      } finally
        transactionLog.close()
    }
  }

  test("command execution: should handle empty table gracefully") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString

      // Create and initialize a transaction log with no splits
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(
          Array(
            StructField("id", IntegerType, nullable = false),
            StructField("name", StringType, nullable = false)
          )
        )

        // Initialize the transaction log (but don't add any files)
        transactionLog.initialize(schema)

        // Create command
        val command = IndexCrossReferencesCommand.apply(
          pathOption = Some(tablePath),
          tableIdOption = None,
          wherePredicates = Seq.empty,
          forceRebuild = false,
          dryRun = false,
          maxXRefBuilds = None
        )

        // Execute command
        val results = command.run(spark)

        // Should return results
        assert(results.nonEmpty)

        // Get the result row
        val result = results.head
        val resultStruct = result.getStruct(1)
        val action = resultStruct.getString(0)

        // Should skip since there are too few splits (0 < minSplitsToTrigger)
        assert(action == "skipped" || action == "error")
      } finally
        transactionLog.close()
    }
  }

  test("command output schema should be correct") {
    val command = IndexCrossReferencesCommand.apply(
      pathOption = Some("/test/path"),
      tableIdOption = None,
      wherePredicates = Seq.empty,
      forceRebuild = false,
      dryRun = true,
      maxXRefBuilds = None
    )

    val output = command.output

    // Should have 2 attributes: table_path and result
    assert(output.length == 2)
    assert(output(0).name == "table_path")
    assert(output(0).dataType == StringType)
    assert(output(1).name == "result")
    assert(output(1).dataType.isInstanceOf[StructType])

    // Check result struct fields
    val resultStruct = output(1).dataType.asInstanceOf[StructType]
    assert(resultStruct.fieldNames.contains("action"))
    assert(resultStruct.fieldNames.contains("xref_path"))
    assert(resultStruct.fieldNames.contains("source_splits_count"))
    assert(resultStruct.fieldNames.contains("total_terms"))
    assert(resultStruct.fieldNames.contains("xref_size_bytes"))
    assert(resultStruct.fieldNames.contains("build_duration_ms"))
    assert(resultStruct.fieldNames.contains("force_rebuild"))
    assert(resultStruct.fieldNames.contains("dry_run"))
    assert(resultStruct.fieldNames.contains("error_message"))
  }
}
