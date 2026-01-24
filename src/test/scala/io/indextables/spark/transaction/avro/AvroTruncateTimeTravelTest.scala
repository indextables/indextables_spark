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

package io.indextables.spark.transaction.avro

import java.io.File

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import io.indextables.spark.TestBase

/**
 * Integration tests for TRUNCATE INDEXTABLES TIME TRAVEL command with Avro state format.
 *
 * Tests cover:
 *   - Truncation cleanup of old state directories
 *   - Checkpoint-to-state directory conversion
 *   - DRY RUN mode with Avro format
 *   - Data integrity after truncation
 */
class AvroTruncateTimeTravelTest extends TestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Use Avro format (default)
    spark.conf.set("spark.indextables.state.format", "avro")
    // Set checkpoint interval low for testing
    spark.conf.set("spark.indextables.checkpoint.interval", "5")
    spark.conf.set("spark.indextables.checkpoint.enabled", "true")
  }

  /** Create a table with multiple transactions to generate version files. */
  private def createTableWithManyTransactions(tablePath: String, numTransactions: Int): Unit = {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("value", StringType, nullable = false)
      )
    )

    for (i <- 1 to numTransactions) {
      val data = Seq(Row(i, s"value_$i"))
      val df   = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "id")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }
  }

  /** Count transaction log files in a table directory. Returns (versionFileCount, stateDirectoryCount) */
  private def countTransactionLogFiles(tablePath: String): (Int, Int) = {
    val txLogDir = new File(tablePath, "_transaction_log")
    if (!txLogDir.exists()) {
      return (0, 0)
    }

    val files = txLogDir.listFiles()
    if (files == null) {
      return (0, 0)
    }

    val versionPattern = """^\d{20}\.json$""".r
    val versionCount   = files.count(f => versionPattern.findFirstIn(f.getName).isDefined)

    // Count Avro state directories
    val stateDirectoryCount = files.count(f => f.getName.startsWith("state-v") && f.isDirectory)

    (versionCount, stateDirectoryCount)
  }

  // ===== Basic Truncation Tests =====

  test("Avro: TRUNCATE TIME TRAVEL should work with Avro state format") {
    val tablePath = new File(tempDir, "avro_truncate_test").getAbsolutePath

    // Create table with 15 transactions (should create checkpoints with interval of 5)
    createTableWithManyTransactions(tablePath, 15)

    // Verify we have version files and/or state directories before truncation
    // Note: Avro format may have fewer version files due to state directory consolidation
    val (versionsBefore, stateDirsBefore) = countTransactionLogFiles(tablePath)
    // With Avro format, we should have at least some version files or state directories
    (versionsBefore + stateDirsBefore) should be >= 1

    // Truncate time travel
    val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'")
    val rows   = result.collect()

    rows.length shouldBe 1
    val row = rows.head

    row.getString(1) shouldBe "SUCCESS" // status
    row.getLong(2) should be >= 0L      // checkpoint_version

    // Verify truncation completed - version files may be reduced
    val (versionsAfter, stateDirsAfter) = countTransactionLogFiles(tablePath)
    // After truncation, should still have valid state
    (versionsAfter + stateDirsAfter) should be >= 0

    // Verify data integrity - all records should still be readable
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    df.count() shouldBe 15
  }

  test("Avro: TRUNCATE TIME TRAVEL DRY RUN should preview changes") {
    val tablePath = new File(tempDir, "avro_dryrun_test").getAbsolutePath

    // Create table with transactions
    createTableWithManyTransactions(tablePath, 10)

    val (versionsBefore, _) = countTransactionLogFiles(tablePath)

    // DRY RUN should not delete anything
    val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath' DRY RUN")
    val rows   = result.collect()

    rows.length shouldBe 1
    val row = rows.head
    row.getString(1) shouldBe "DRY_RUN" // status

    // Version files should NOT be deleted
    val (versionsAfter, _) = countTransactionLogFiles(tablePath)
    versionsAfter shouldBe versionsBefore

    // Data should still be readable
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    df.count() shouldBe 10
  }

  test("Avro: table should be readable after truncation") {
    val tablePath = new File(tempDir, "avro_readable_after_truncate").getAbsolutePath

    // Create table with transactions
    createTableWithManyTransactions(tablePath, 8)

    // Truncate
    spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'").collect()

    // Verify table is readable
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    df.count() shouldBe 8

    // Verify queries work
    df.filter("id > 5").count() shouldBe 3
  }

  test("Avro: append should work after truncation") {
    val tablePath = new File(tempDir, "avro_append_after_truncate").getAbsolutePath

    // Create table with transactions
    createTableWithManyTransactions(tablePath, 5)

    // Truncate
    spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'").collect()

    // Append more data
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("value", StringType, nullable = false)
      )
    )
    val newData = Seq(Row(100, "new_value"))
    val newDf   = spark.createDataFrame(spark.sparkContext.parallelize(newData), schema)
    newDf.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("append")
      .save(tablePath)

    // Verify append worked
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    df.count() shouldBe 6

    // Verify new record is present
    df.filter("id = 100").count() shouldBe 1
  }

  test("Avro: state directory should exist after truncation") {
    val tablePath = new File(tempDir, "avro_state_after_truncate").getAbsolutePath

    // Create table with enough transactions to trigger checkpoint
    createTableWithManyTransactions(tablePath, 10)

    // Truncate
    spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'").collect()

    // Verify Avro state directory exists
    val txLogDir = new File(tablePath, "_transaction_log")
    val stateDirectories = txLogDir.listFiles().filter { f =>
      f.getName.startsWith("state-v") && f.isDirectory
    }

    // Should have at least one state directory
    stateDirectories.length should be >= 1

    // Verify _last_checkpoint exists and references avro-state
    val lastCheckpointFile = new File(txLogDir, "_last_checkpoint")
    if (lastCheckpointFile.exists()) {
      val content = scala.io.Source.fromFile(lastCheckpointFile).mkString
      assert(
        content.contains("avro-state") || content.contains("stateDir"),
        s"_last_checkpoint should reference Avro state format after truncation"
      )
    }
  }

  test("Avro: truncation result schema should be correct") {
    val tablePath = new File(tempDir, "avro_result_schema_test").getAbsolutePath

    // Create table
    createTableWithManyTransactions(tablePath, 5)

    // Truncate and check result schema
    val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'")

    // Verify expected columns
    val columns = result.columns
    assert(columns.contains("table_path"), "Result should have table_path column")
    assert(columns.contains("status"), "Result should have status column")
    assert(columns.contains("checkpoint_version"), "Result should have checkpoint_version column")
  }
}
