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

import java.io.File

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import io.indextables.spark.TestBase

/**
 * Integration tests for TRUNCATE INDEXTABLES TIME TRAVEL command.
 *
 * Tests cover:
 *   - Basic truncation with multiple transactions
 *   - Checkpoint creation before truncation
 *   - DRY RUN mode
 *   - Data integrity after truncation
 *   - Edge cases (empty table, single version, etc.)
 */
class TruncateTimeTravelTest extends TestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Set checkpoint interval low for testing
    spark.conf.set("spark.indextables.checkpoint.interval", "5")
  }

  private def createTestTable(tablePath: String): Unit = {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("value", StringType, nullable = false)
      )
    )
    val data = Seq(Row(1, "one"), Row(2, "two"), Row(3, "three"))
    val df   = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)
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
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }
  }

  /** Count transaction log files in a table directory. Returns (versionFileCount, checkpointFileCount) */
  private def countTransactionLogFiles(tablePath: String): (Int, Int) = {
    val txLogDir = new File(tablePath, "_transaction_log")
    if (!txLogDir.exists()) {
      return (0, 0)
    }

    val files = txLogDir.listFiles()
    if (files == null) {
      return (0, 0)
    }

    val versionPattern    = """^\d{20}\.json$""".r
    val checkpointPattern = """^\d{20}\.checkpoint.*\.json$""".r

    val versionCount    = files.count(f => versionPattern.findFirstIn(f.getName).isDefined)
    val checkpointCount = files.count(f => checkpointPattern.findFirstIn(f.getName).isDefined)

    (versionCount, checkpointCount)
  }

  // ===== Basic Truncation Tests =====

  test("TRUNCATE INDEXTABLES TIME TRAVEL should truncate table with many transactions") {
    val tablePath = new File(tempDir, "many_transactions_test").getAbsolutePath

    // Create table with 15 transactions (should create 2-3 checkpoints with interval of 5)
    createTableWithManyTransactions(tablePath, 15)

    // Verify we have multiple version files before truncation
    val (versionsBefore, _) = countTransactionLogFiles(tablePath)
    versionsBefore should be >= 15

    // Truncate time travel
    val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'")
    val rows   = result.collect()

    rows.length shouldBe 1
    val row = rows.head

    row.getString(1) shouldBe "SUCCESS" // status
    row.getLong(2) should be >= 0L      // checkpoint_version
    row.getLong(3) should be >= 0L      // versions_deleted

    // Verify version files were deleted
    val (versionsAfter, _) = countTransactionLogFiles(tablePath)
    versionsAfter should be < versionsBefore

    // Verify data integrity - all records should still be readable
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    df.count() shouldBe 15
  }

  test("TRUNCATE INDEXTABLES TIME TRAVEL should create checkpoint if none exists") {
    val tablePath = new File(tempDir, "no_checkpoint_test").getAbsolutePath

    // Create table with just 2 transactions (below checkpoint interval)
    createTableWithManyTransactions(tablePath, 2)

    // No checkpoint should exist yet
    val (_, checkpointsBefore) = countTransactionLogFiles(tablePath)

    // Truncate - should create checkpoint first
    val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'")
    val row    = result.collect().head

    row.getString(1) shouldBe "SUCCESS"
    row.getLong(2) should be >= 0L // checkpoint_version created

    // Checkpoint should now exist
    val (_, checkpointsAfter) = countTransactionLogFiles(tablePath)
    checkpointsAfter should be >= 1

    // Data should still be readable
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    df.count() shouldBe 2
  }

  // ===== DRY RUN Tests =====

  test("TRUNCATE INDEXTABLES TIME TRAVEL DRY RUN should preview without deleting") {
    val tablePath = new File(tempDir, "dry_run_test").getAbsolutePath

    // Create table with 12 transactions
    createTableWithManyTransactions(tablePath, 12)

    val (versionsBefore, _) = countTransactionLogFiles(tablePath)

    // Run DRY RUN
    val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath' DRY RUN")
    val row    = result.collect().head

    row.getString(1) shouldBe "DRY_RUN"
    row.getLong(3) should be >= 0L // versions_deleted (would be deleted)

    // Verify NO files were actually deleted
    val (versionsAfter, _) = countTransactionLogFiles(tablePath)
    versionsAfter shouldBe versionsBefore
  }

  test("DRY RUN followed by actual truncate should produce consistent results") {
    val tablePath = new File(tempDir, "dry_run_consistency_test").getAbsolutePath

    // Create table with 10 transactions
    createTableWithManyTransactions(tablePath, 10)

    // First, DRY RUN
    val dryRunResult     = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath' DRY RUN")
    val dryRunRow        = dryRunResult.collect().head
    val versionsToDelete = dryRunRow.getLong(3)

    // Now actually truncate
    val actualResult = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'")
    val actualRow    = actualResult.collect().head

    actualRow.getString(1) shouldBe "SUCCESS"
    actualRow.getLong(3) shouldBe versionsToDelete // Should match DRY RUN
  }

  // ===== Edge Cases =====

  test("TRUNCATE should handle table with single version") {
    val tablePath = new File(tempDir, "single_version_test").getAbsolutePath

    // Create table with just 1 transaction
    createTestTable(tablePath)

    val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'")
    val row    = result.collect().head

    // Should succeed (creates checkpoint, nothing old to delete)
    row.getString(1) shouldBe "SUCCESS"

    // Data should still be readable
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    df.count() shouldBe 3
  }

  test("TRUNCATE should handle table already at minimal state") {
    val tablePath = new File(tempDir, "minimal_state_test").getAbsolutePath

    // Create table and immediately truncate
    createTestTable(tablePath)
    spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'").collect()

    // Try to truncate again
    val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'")
    val row    = result.collect().head

    row.getString(1) shouldBe "SUCCESS"
    // Message should indicate nothing to delete
    row.getString(6) should include("minimal state")
  }

  test("TRUNCATE should return ERROR for non-existent table") {
    val nonExistentPath = new File(tempDir, "non_existent_table").getAbsolutePath

    val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$nonExistentPath'")
    val row    = result.collect().head

    row.getString(1) shouldBe "ERROR"
  }

  // ===== Data Integrity Tests =====

  test("All data should be preserved after truncation") {
    val tablePath = new File(tempDir, "data_integrity_test").getAbsolutePath

    // Create table with 20 transactions, each adding one record
    createTableWithManyTransactions(tablePath, 20)

    // Get record count before truncation
    val countBefore = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()

    // Truncate
    spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'").collect()

    // Verify record count is preserved
    val countAfter = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
      .count()

    countAfter shouldBe countBefore
    countAfter shouldBe 20
  }

  test("Queries should work correctly after truncation") {
    val tablePath = new File(tempDir, "query_after_truncate_test").getAbsolutePath

    // Create table with diverse data
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("score", DoubleType, nullable = false)
      )
    )

    // Write initial data
    val initialData = (1 to 10).map(i => Row(i, s"value_$i", i * 10.0))
    val initialDf   = spark.createDataFrame(spark.sparkContext.parallelize(initialData), schema)
    initialDf.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Add more data in separate transactions
    for (i <- 11 to 15) {
      val data = Seq(Row(i, s"value_$i", i * 10.0))
      val df   = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(tablePath)
    }

    // Truncate
    spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'").collect()

    // Test various queries
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Count query
    result.count() shouldBe 15

    // Filter query
    result.filter("id > 10").count() shouldBe 5

    // Aggregation
    result.agg(Map("score" -> "sum")).collect().head.getDouble(0) shouldBe 1200.0
  }

  // ===== Output Schema Tests =====

  test("TRUNCATE should return correct output schema") {
    val tablePath = new File(tempDir, "schema_test").getAbsolutePath
    createTestTable(tablePath)

    val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'")

    val columns = result.columns.toSet
    columns should contain("table_path")
    columns should contain("status")
    columns should contain("checkpoint_version")
    columns should contain("versions_deleted")
    columns should contain("checkpoints_deleted")
    columns should contain("files_preserved")
    columns should contain("message")

    result.count() shouldBe 1
  }

  // ===== Syntax Variants Tests =====

  test("TRUNCATE TANTIVY4SPARK TIME TRAVEL should work with alternate keyword") {
    val tablePath = new File(tempDir, "tantivy4spark_keyword_test").getAbsolutePath
    createTableWithManyTransactions(tablePath, 8)

    val result = spark.sql(s"TRUNCATE TANTIVY4SPARK TIME TRAVEL '$tablePath'")
    val row    = result.collect().head

    row.getString(1) shouldBe "SUCCESS"
  }

  // ===== Fast Fields Preservation Tests =====

  test("Fast fields configuration should be preserved after truncation") {
    val tablePath = new File(tempDir, "fast_fields_preservation_test").getAbsolutePath

    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("region", StringType, nullable = false), // partition column
        StructField("category", StringType, nullable = false),
        StructField("amount", DoubleType, nullable = false), // fast field (non-partition)
        StructField("count", IntegerType, nullable = false)  // fast field (non-partition)
      )
    )

    // Create initial data with multiple transactions to generate version files
    for (i <- 1 to 12) {
      val data = Seq(
        Row(i * 2 - 1, "us-east", s"cat_${i % 3}", i * 10.0, i * 2),
        Row(i * 2, "us-west", s"cat_${i % 3}", i * 15.0, i * 3)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "amount,count,region")
        .partitionBy("region")
        .mode(if (i == 1) "overwrite" else "append")
        .save(tablePath)
    }

    // Verify aggregations work BEFORE truncation
    val dfBefore = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val countBefore     = dfBefore.count()
    val sumAmountBefore = dfBefore.agg(Map("amount" -> "sum")).collect().head.getDouble(0)
    val sumCountBefore  = dfBefore.agg(Map("count" -> "sum")).collect().head.getLong(0)
    val avgAmountBefore = dfBefore.agg(Map("amount" -> "avg")).collect().head.getDouble(0)
    val maxAmountBefore = dfBefore.agg(Map("amount" -> "max")).collect().head.getDouble(0)
    val minAmountBefore = dfBefore.agg(Map("amount" -> "min")).collect().head.getDouble(0)

    // Aggregation by partition column
    val sumByPartitionBefore = dfBefore
      .groupBy("region")
      .agg(Map("amount" -> "sum"))
      .collect()
      .map(r => (r.getString(0), r.getDouble(1)))
      .toMap

    countBefore shouldBe 24 // 12 transactions * 2 records each

    // Truncate time travel
    val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'")
    val row    = result.collect().head
    row.getString(1) shouldBe "SUCCESS"

    // Verify aggregations work AFTER truncation (proves fast fields preserved)
    val dfAfter = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Basic count should match
    val countAfter = dfAfter.count()
    countAfter shouldBe countBefore

    // Aggregations on fast fields should still work and return same results
    val sumAmountAfter = dfAfter.agg(Map("amount" -> "sum")).collect().head.getDouble(0)
    val sumCountAfter  = dfAfter.agg(Map("count" -> "sum")).collect().head.getLong(0)
    val avgAmountAfter = dfAfter.agg(Map("amount" -> "avg")).collect().head.getDouble(0)
    val maxAmountAfter = dfAfter.agg(Map("amount" -> "max")).collect().head.getDouble(0)
    val minAmountAfter = dfAfter.agg(Map("amount" -> "min")).collect().head.getDouble(0)

    sumAmountAfter shouldBe sumAmountBefore
    sumCountAfter shouldBe sumCountBefore
    avgAmountAfter shouldBe avgAmountBefore
    maxAmountAfter shouldBe maxAmountBefore
    minAmountAfter shouldBe minAmountBefore

    // Aggregation by partition column should still work
    val sumByPartitionAfter = dfAfter
      .groupBy("region")
      .agg(Map("amount" -> "sum"))
      .collect()
      .map(r => (r.getString(0), r.getDouble(1)))
      .toMap

    sumByPartitionAfter("us-east") shouldBe sumByPartitionBefore("us-east")
    sumByPartitionAfter("us-west") shouldBe sumByPartitionBefore("us-west")

    println(s"✅ Fast fields preserved: SUM(amount)=$sumAmountAfter, SUM(count)=$sumCountAfter")
    println(s"✅ Partition aggregation preserved: us-east=${sumByPartitionAfter("us-east")}, us-west=${sumByPartitionAfter("us-west")}")
  }

  // ===== Multiple Checkpoint Tests =====

  test("TRUNCATE should handle multiple existing checkpoints with uncheckpointed version") {
    val tablePath = new File(tempDir, "multi_checkpoint_test").getAbsolutePath

    // Use checkpoint interval of 10 for this test
    val originalInterval = spark.conf.getOption("spark.indextables.checkpoint.interval")
    spark.conf.set("spark.indextables.checkpoint.interval", "10")

    try {
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("value", StringType, nullable = false),
          StructField("amount", DoubleType, nullable = false)
        )
      )

      // Write 21 transactions to create:
      // - Checkpoint at version 10
      // - Checkpoint at version 20
      // - Version 21 (uncheckpointed - the "extra" version after checkpoint)
      for (i <- 1 to 21) {
        val data = Seq(Row(i, s"value_$i", i * 100.0))
        val df   = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        df.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode(if (i == 1) "overwrite" else "append")
          .save(tablePath)
      }

      // Verify we have multiple checkpoints before truncation
      val (versionsBefore, checkpointsBefore) = countTransactionLogFiles(tablePath)
      versionsBefore should be >= 21
      checkpointsBefore should be >= 2 // Should have checkpoints at v10 and v20

      // Get data state before truncation
      val dataBefore = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)
      val countBefore = dataBefore.count()
      val sumBefore   = dataBefore.agg(Map("amount" -> "sum")).collect().head.getDouble(0)
      val idsBefore   = dataBefore.select("id").collect().map(_.getInt(0)).sorted

      countBefore shouldBe 21
      sumBefore shouldBe 23100.0 // sum of 100+200+...+2100 = 100*(1+2+...+21) = 100*231 = 23100

      // Truncate time travel
      val result = spark.sql(s"TRUNCATE INDEXTABLES TIME TRAVEL '$tablePath'")
      val row    = result.collect().head

      row.getString(1) shouldBe "SUCCESS"
      row.getLong(3) should be >= 20L // Should delete versions 0-19 or 0-20
      row.getLong(4) should be >= 1L  // Should delete at least checkpoint at v10

      // Verify transaction log state after truncation
      val (versionsAfter, checkpointsAfter) = countTransactionLogFiles(tablePath)
      versionsAfter should be < versionsBefore
      checkpointsAfter shouldBe 1 // Only latest checkpoint should remain

      // CRITICAL: Verify ALL data is still readable after truncation
      val dataAfter = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      val countAfter = dataAfter.count()
      val sumAfter   = dataAfter.agg(Map("amount" -> "sum")).collect().head.getDouble(0)
      val idsAfter   = dataAfter.select("id").collect().map(_.getInt(0)).sorted

      // Verify exact data integrity
      countAfter shouldBe countBefore
      sumAfter shouldBe sumBefore
      idsAfter shouldBe idsBefore

      // Verify each individual record is readable
      for (expectedId <- 1 to 21) {
        val record = dataAfter.filter(s"id = $expectedId").collect()
        record.length shouldBe 1
        record.head.getString(1) shouldBe s"value_$expectedId"
        record.head.getDouble(2) shouldBe (expectedId * 100.0)
      }

    } finally
      // Restore checkpoint interval
      originalInterval match {
        case Some(v) => spark.conf.set("spark.indextables.checkpoint.interval", v)
        case None    => spark.conf.set("spark.indextables.checkpoint.interval", "5")
      }
  }
}
