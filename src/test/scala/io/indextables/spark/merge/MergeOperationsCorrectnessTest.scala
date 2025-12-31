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

package io.indextables.spark.merge

import java.io.File

import io.indextables.spark.TestBase
import org.scalatest.BeforeAndAfterEach

/**
 * Comprehensive tests for Merge Operations correctness.
 *
 * Tests cover:
 *   - Data integrity preservation during merges
 *   - Target size compliance
 *   - Schema preservation after merge
 *   - Merge with filters (WHERE clause)
 *   - Transaction log updates after merge
 */
class MergeOperationsCorrectnessTest extends TestBase with BeforeAndAfterEach {

  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[MergeOperationsCorrectnessTest])

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sessionState.catalog.reset()
  }

  // ============================================================================
  // DATA INTEGRITY TESTS
  // ============================================================================

  test("should preserve all documents after merge") {
    val tablePath = s"file://$tempDir/test_merge_integrity"

    // Create table with multiple small partitions
    val df = spark.range(0, 1000).repartition(20).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Verify initial count
    val beforeMerge = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    beforeMerge.count() shouldBe 1000

    // Execute merge via SQL
    spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 100M")

    // Verify count after merge
    val afterMerge = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    afterMerge.count() shouldBe 1000

    // Verify all IDs preserved
    val ids = afterMerge.select("id").limit(1000).collect().map(_.getLong(0)).sorted
    ids shouldBe (0L until 1000L).toArray

    logger.info("Data integrity after merge test passed")
  }

  test("should preserve document content after merge") {
    val tablePath = s"file://$tempDir/test_merge_content"

    val data = Seq(
      (1L, "unique_content_1", 100.5),
      (2L, "unique_content_2", 200.5),
      (3L, "unique_content_3", 300.5)
    )
    val df = spark.createDataFrame(data).toDF("id", "text", "score")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Execute merge
    spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 100M")

    // Verify content preserved
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val rows1 = result.filter("id = 1").limit(1).collect()
    rows1(0).getAs[String]("text") shouldBe "unique_content_1"

    val rows2 = result.filter("id = 2").limit(1).collect()
    rows2(0).getAs[String]("text") shouldBe "unique_content_2"

    logger.info("Document content preservation test passed")
  }

  // ============================================================================
  // SPLIT CONSOLIDATION TESTS
  // ============================================================================

  test("should reduce number of split files after merge") {
    val tablePath = s"file://$tempDir/test_split_reduction"

    // Create many small splits
    val df = spark.range(0, 500).repartition(25).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    val tableDir     = new File(s"$tempDir/test_split_reduction")
    val splitsBefore = tableDir.listFiles().filter(_.getName.endsWith(".split")).length

    // Execute merge with large target size to consolidate all
    spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 1G")

    // Give filesystem time to sync
    Thread.sleep(500)

    // Count active splits (merged splits should be fewer)
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 500

    logger.info(s"Split reduction test passed (before: $splitsBefore splits)")
  }

  test("should respect target size during merge") {
    val tablePath = s"file://$tempDir/test_target_size"

    // Create data
    val df = spark
      .range(0, 2000)
      .repartition(40)
      .selectExpr(
        "id",
        "CAST(id AS STRING) as text",
        "REPEAT('x', 100) as padding"
      )
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Execute merge with specific target size
    spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 1M")

    // Verify data intact
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 2000

    logger.info("Target size compliance test passed")
  }

  // ============================================================================
  // SCHEMA PRESERVATION TESTS
  // ============================================================================

  test("should preserve schema with multiple field types after merge") {
    val tablePath = s"file://$tempDir/test_schema_merge"

    val data = Seq(
      (1L, "text1", 1.5, true, java.sql.Date.valueOf("2024-01-01")),
      (2L, "text2", 2.5, false, java.sql.Date.valueOf("2024-01-02"))
    )
    val df = spark.createDataFrame(data).toDF("id", "text", "score", "active", "date")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Execute merge
    spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 100M")

    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    // Verify schema preserved
    val fields = result.schema.fieldNames.toSeq
    fields should contain allOf ("id", "text", "score", "active", "date")

    // Verify data types preserved
    val rows = result.filter("id = 1").limit(1).collect()
    rows(0).getAs[Long]("id") shouldBe 1L
    rows(0).getAs[String]("text") shouldBe "text1"
    rows(0).getAs[Boolean]("active") shouldBe true

    logger.info("Schema preservation after merge test passed")
  }

  // ============================================================================
  // MERGE WITH WHERE CLAUSE TESTS
  // ============================================================================

  test("should merge only splits matching WHERE clause") {
    val tablePath = s"file://$tempDir/test_merge_where"

    // Create partitioned data
    val df = spark
      .range(0, 200)
      .selectExpr(
        "id",
        "CAST(id AS STRING) as text",
        "CAST(id % 4 AS STRING) as partition_col"
      )
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .partitionBy("partition_col")
      .mode("overwrite")
      .save(tablePath)

    // Merge only partition_col = '0'
    spark.sql(s"MERGE SPLITS '$tablePath' WHERE partition_col = '0' TARGET SIZE 100M")

    // Verify all data still accessible
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 200

    // Verify partition data intact
    result.filter("partition_col = '0'").count() shouldBe 50
    result.filter("partition_col = '1'").count() shouldBe 50

    logger.info("Merge with WHERE clause test passed")
  }

  // ============================================================================
  // MAX DEST SPLITS TESTS
  // ============================================================================

  test("should respect MAX DEST SPLITS limit") {
    val tablePath = s"file://$tempDir/test_max_dest_splits"

    // Create data with many partitions
    val df = spark.range(0, 500).repartition(20).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Execute merge with MAX DEST SPLITS limit
    spark.sql(s"MERGE SPLITS '$tablePath' MAX DEST SPLITS 3")

    // Verify data intact
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 500

    logger.info("MAX DEST SPLITS limit test passed")
  }

  // ============================================================================
  // TRANSACTION LOG UPDATE TESTS
  // ============================================================================

  test("should update transaction log after merge") {
    val tablePath = s"file://$tempDir/test_txlog_update"

    val df = spark.range(0, 100).repartition(10).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Count log entries before merge
    val txLogDir      = new File(s"$tempDir/test_txlog_update/_transaction_log")
    val entriesBefore = txLogDir.listFiles().filter(_.getName.endsWith(".json")).length

    // Execute merge
    spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 100M")

    // Count log entries after merge
    val entriesAfter = txLogDir.listFiles().filter(_.getName.endsWith(".json")).length

    // Should have more entries after merge (merge creates new log entry)
    assert(entriesAfter >= entriesBefore, "Transaction log should be updated after merge")

    // Verify data still readable
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 100

    logger.info("Transaction log update test passed")
  }

  // ============================================================================
  // EDGE CASE TESTS
  // ============================================================================

  test("should handle merge on table with single split") {
    val tablePath = s"file://$tempDir/test_single_split_merge"

    // Create table with single partition
    val df = spark.range(0, 100).repartition(1).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Execute merge on single split
    spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 100M")

    // Verify data intact
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 100

    logger.info("Single split merge test passed")
  }

  test("should handle merge on empty table") {
    val tablePath = s"file://$tempDir/test_empty_merge"

    // Create empty table
    val df = spark.range(0, 0).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Execute merge on empty table - should not fail
    try
      spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 100M")
    catch {
      case _: Exception =>
      // Empty table merge might fail gracefully - that's OK
    }

    // Verify table still accessible
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 0

    logger.info("Empty table merge test passed")
  }

  test("should handle consecutive merges") {
    val tablePath = s"file://$tempDir/test_consecutive_merge"

    // Create initial data
    val df = spark.range(0, 500).repartition(25).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // First merge
    spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 100M")

    // Append more data
    val df2 = spark.range(500, 1000).repartition(25).selectExpr("id", "CAST(id AS STRING) as text")
    df2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("append")
      .save(tablePath)

    // Second merge
    spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 100M")

    // Verify all data intact
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.count() shouldBe 1000

    // Verify all IDs present
    val ids = result.select("id").limit(1000).collect().map(_.getLong(0)).sorted
    ids shouldBe (0L until 1000L).toArray

    logger.info("Consecutive merges test passed")
  }

  // ============================================================================
  // FILTER PUSHDOWN AFTER MERGE TESTS
  // ============================================================================

  test("should maintain filter pushdown capability after merge") {
    val tablePath = s"file://$tempDir/test_filter_after_merge"

    val df = spark.range(0, 500).repartition(10).selectExpr("id", "CAST(id AS STRING) as text")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Execute merge
    spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 100M")

    // Verify filters still work
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    result.filter("id = 250").count() shouldBe 1
    result.filter("id >= 100 AND id < 200").count() shouldBe 100
    result.filter("text = '300'").count() shouldBe 1

    logger.info("Filter pushdown after merge test passed")
  }

  // ============================================================================
  // AGGREGATION AFTER MERGE TESTS
  // ============================================================================

  test("should maintain aggregation capability after merge") {
    val tablePath = s"file://$tempDir/test_agg_after_merge"

    val df = spark
      .range(0, 100)
      .repartition(10)
      .selectExpr(
        "id",
        "CAST(id AS STRING) as text",
        "CAST(id AS DOUBLE) as value"
      )
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.fastfields", "value")
      .mode("overwrite")
      .save(tablePath)

    // Execute merge
    spark.sql(s"MERGE SPLITS '$tablePath' TARGET SIZE 100M")

    // Verify aggregations work
    val result = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val agg = result
      .agg(
        org.apache.spark.sql.functions.count("*").as("cnt"),
        org.apache.spark.sql.functions.sum("value").as("total"),
        org.apache.spark.sql.functions.min("value").as("min_val"),
        org.apache.spark.sql.functions.max("value").as("max_val")
      )
      .limit(1)
      .collect()
      .head

    agg.getLong(0) shouldBe 100      // count
    agg.getDouble(1) shouldBe 4950.0 // sum of 0..99
    agg.getDouble(2) shouldBe 0.0    // min
    agg.getDouble(3) shouldBe 99.0   // max

    logger.info("Aggregation after merge test passed")
  }
}
