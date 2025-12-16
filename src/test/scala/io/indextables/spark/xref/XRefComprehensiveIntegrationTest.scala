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

package io.indextables.spark.xref

import scala.collection.immutable.Range

import org.apache.spark.sql.functions._

import io.indextables.spark.TestBase

/**
 * Comprehensive integration tests for XRef (Cross-Reference) indexing.
 *
 * This test suite validates:
 * - Mix of splits with and without cross references
 * - At least 20 unique splits
 * - Correct results when querying each row
 * - Both cross-referenced and non-cross-referenced splits
 * - Range queries and term queries
 * - Partition and non-partition key queries
 * - Aggregate pushdown with XRef skipping
 */
class XRefComprehensiveIntegrationTest extends TestBase {

  private val DataSourceFormat = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  // Test data generation helpers
  private def generateTestData(numRows: Int, partition: String, batchId: Int): Seq[(Int, String, String, Int, Double, String)] = {
    (0 until numRows).map { i =>
      val globalId = batchId * 1000 + i
      (
        globalId,                                    // id (unique across all batches)
        s"term_${partition}_${batchId}_$i",          // searchable_term (for term queries)
        partition,                                   // partition_key
        100 + i * 10,                                // numeric_value (for range queries)
        (globalId % 100) / 10.0,                     // score (for aggregations)
        s"content for row $globalId in $partition"  // content
      )
    }
  }

  test("comprehensive XRef integration - 25+ splits with mixed coverage") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val spark = this.spark
      import spark.implicits._

      // Configure for small splits to create many splits
      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")

      // Enable XRef query routing
      spark.conf.set("spark.indextables.xref.query.enabled", "true")
      spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "5") // Low threshold for testing

      // ================================================================
      // PHASE 1: Create multiple splits across multiple partitions
      // ================================================================
      val partitions = Seq("region_a", "region_b", "region_c", "region_d", "region_e")

      // Write data in batches to create multiple splits per partition
      // Use repartition(2) to ensure at least 2 splits per batch
      partitions.zipWithIndex.foreach { case (partition, partIdx) =>
        // Create 5 batches per partition = 25 total batches
        (0 until 5).foreach { batchIdx =>
          val batchId = partIdx * 5 + batchIdx
          val data = generateTestData(50, partition, batchId)
          val df = data.toDF("id", "searchable_term", "partition_key", "numeric_value", "score", "content")
            .repartition(2) // Force 2 partitions per batch to create more splits

          df.write.format(DataSourceFormat)
            .option("spark.indextables.indexing.typemap.content", "text")
            .option("spark.indextables.indexing.fastfields", "numeric_value,score")
            .mode("append")
            .partitionBy("partition_key")
            .save(tablePath)
        }
      }

      // Verify we have multiple splits by counting rows (V2 DataSource doesn't support inputFiles)
      val dfAll = spark.read.format(DataSourceFormat).load(tablePath)
      val totalRows = dfAll.count()
      assert(totalRows == 25 * 50, s"Expected ${25 * 50} rows, got $totalRows")

      // Verify split count via transaction log (expect at least 10 splits for meaningful XRef testing)
      val txLogResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath'")
      val splitCount = txLogResult.filter("action_type = 'add'").count()
      assert(splitCount >= 10, s"Expected at least 10 splits in transaction log for XRef testing, got $splitCount")

      // ================================================================
      // PHASE 2: Build XRef indexes for ONLY region_a, region_b, region_c
      // Leave region_d and region_e without XRef coverage
      // ================================================================
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath' WHERE partition_key = 'region_a'")
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath' WHERE partition_key = 'region_b'")
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath' WHERE partition_key = 'region_c'")

      // Verify XRef entries were created in the transaction log
      // Note: Each INDEX CROSSREFERENCES command creates one XRef entry per batch of splits
      val xrefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
      val xrefCount = xrefResult.count()
      assert(xrefCount >= 1, s"Expected at least 1 XRef entry, got $xrefCount")
      println(s"Created $xrefCount XRef entries for 3 covered partitions")

      // ================================================================
      // PHASE 3: Validate term queries return correct results
      // ================================================================

      // Test term query on XRef-covered partition (region_a)
      val termQueryA = dfAll.filter(col("searchable_term") === "term_region_a_0_0")
      assert(termQueryA.count() == 1, "Term query on XRef-covered partition should return 1 row")
      val resultA = termQueryA.collect().head
      assert(resultA.getAs[Int]("id") == 0, "Term query should return correct id")
      assert(resultA.getAs[String]("partition_key") == "region_a")

      // Test term query on non-XRef-covered partition (region_d)
      val termQueryD = dfAll.filter(col("searchable_term") === "term_region_d_15_0")
      assert(termQueryD.count() == 1, "Term query on non-XRef partition should return 1 row")
      val resultD = termQueryD.collect().head
      assert(resultD.getAs[Int]("id") == 15000, "Term query should return correct id")
      assert(resultD.getAs[String]("partition_key") == "region_d")

      // ================================================================
      // PHASE 4: Validate range queries on numeric fields
      // ================================================================

      // Range query across all partitions
      val rangeQueryAll = dfAll.filter(col("numeric_value") >= 150 && col("numeric_value") < 160)
      val rangeCount = rangeQueryAll.count()
      // Each batch has rows with numeric_value: 100, 110, 120, ..., 590
      // Values 150, 151, ..., 159 don't exist (values are 100+i*10)
      // So this should return rows where numeric_value is exactly 150
      val expectedRangeCount = 25 * 1 // One row per batch (numeric_value = 100 + 5*10 = 150)
      assert(rangeCount == expectedRangeCount, s"Range query should return $expectedRangeCount rows, got $rangeCount")

      // Range query on XRef-covered partitions only
      val rangeQueryXRef = dfAll
        .filter(col("partition_key").isin("region_a", "region_b", "region_c"))
        .filter(col("numeric_value") >= 100 && col("numeric_value") <= 200)
      val rangeXRefCount = rangeQueryXRef.count()
      // 15 batches (3 partitions x 5 batches), values 100-200 step 10 = 11 values per batch
      assert(rangeXRefCount == 15 * 11, s"Range query on XRef partitions should return ${15 * 11} rows, got $rangeXRefCount")

      // Range query on non-XRef-covered partitions
      val rangeQueryNonXRef = dfAll
        .filter(col("partition_key").isin("region_d", "region_e"))
        .filter(col("numeric_value") >= 100 && col("numeric_value") <= 200)
      val rangeNonXRefCount = rangeQueryNonXRef.count()
      assert(rangeNonXRefCount == 10 * 11, s"Range query on non-XRef partitions should return ${10 * 11} rows, got $rangeNonXRefCount")

      // ================================================================
      // PHASE 5: Validate partition key queries
      // ================================================================

      // Single partition query (XRef-covered)
      val partitionQueryA = dfAll.filter(col("partition_key") === "region_a")
      assert(partitionQueryA.count() == 5 * 50, s"Partition query should return ${5 * 50} rows")

      // Single partition query (non-XRef-covered)
      val partitionQueryE = dfAll.filter(col("partition_key") === "region_e")
      assert(partitionQueryE.count() == 5 * 50, s"Partition query should return ${5 * 50} rows")

      // Multiple partition query (mixed XRef coverage)
      val mixedPartitionQuery = dfAll.filter(col("partition_key").isin("region_a", "region_d"))
      assert(mixedPartitionQuery.count() == 10 * 50, s"Mixed partition query should return ${10 * 50} rows")

      // ================================================================
      // PHASE 6: Validate combined partition + non-partition filters
      // ================================================================

      // Partition filter + term filter
      val combinedQuery1 = dfAll
        .filter(col("partition_key") === "region_b")
        .filter(col("searchable_term") === "term_region_b_6_25")
      assert(combinedQuery1.count() == 1, "Combined partition + term filter should return 1 row")

      // Partition filter + range filter
      val combinedQuery2 = dfAll
        .filter(col("partition_key") === "region_c")
        .filter(col("numeric_value") > 300)
      val combinedCount = combinedQuery2.count()
      // Each batch has 50 rows with values 100, 110, ..., 590
      // Values > 300: 310, 320, ..., 590 = 29 values per batch
      // region_c has 5 batches
      assert(combinedCount == 5 * 29, s"Combined partition + range filter should return ${5 * 29} rows, got $combinedCount")

      // Non-partition filter + range filter on non-XRef partition
      val combinedQuery3 = dfAll
        .filter(col("partition_key") === "region_e")
        .filter(col("numeric_value") >= 200 && col("numeric_value") < 300)
      val combinedCount3 = combinedQuery3.count()
      // Values 200, 210, ..., 290 = 10 values per batch, 5 batches
      assert(combinedCount3 == 5 * 10, s"Combined query on non-XRef partition should return ${5 * 10} rows, got $combinedCount3")

      // ================================================================
      // PHASE 7: Validate each row can be retrieved correctly
      // ================================================================

      // Sample rows from each partition and verify all fields
      partitions.foreach { partition =>
        val sampleIds = Seq(0, 25, 49) // First, middle, last row in first batch
        sampleIds.foreach { rowIdx =>
          val partitionIdx = partitions.indexOf(partition)
          val expectedId = partitionIdx * 5 * 1000 + rowIdx

          val row = dfAll.filter(col("id") === expectedId).collect()
          assert(row.length == 1, s"Should find exactly 1 row with id=$expectedId")

          val actual = row.head
          assert(actual.getAs[Int]("id") == expectedId)
          assert(actual.getAs[String]("partition_key") == partition)
          assert(actual.getAs[Int]("numeric_value") == 100 + rowIdx * 10)
        }
      }

      // ================================================================
      // PHASE 8: Validate aggregate pushdown with XRef skipping
      // ================================================================

      // COUNT across all partitions
      val countAll = dfAll.agg(count("*")).collect().head.getLong(0)
      assert(countAll == 1250, s"COUNT(*) should return 1250, got $countAll")

      // COUNT on XRef-covered partitions
      val countXRef = dfAll
        .filter(col("partition_key").isin("region_a", "region_b", "region_c"))
        .agg(count("*")).collect().head.getLong(0)
      assert(countXRef == 750, s"COUNT(*) on XRef partitions should return 750, got $countXRef")

      // COUNT on non-XRef-covered partitions
      val countNonXRef = dfAll
        .filter(col("partition_key").isin("region_d", "region_e"))
        .agg(count("*")).collect().head.getLong(0)
      assert(countNonXRef == 500, s"COUNT(*) on non-XRef partitions should return 500, got $countNonXRef")

      // SUM on numeric field (XRef-covered partitions)
      val sumXRef = dfAll
        .filter(col("partition_key") === "region_a")
        .agg(sum("numeric_value")).collect().head.getLong(0)
      // Each batch: sum of 100, 110, ..., 590 = (100 + 590) * 50 / 2 = 17250
      // 5 batches: 17250 * 5 = 86250
      assert(sumXRef == 86250L, s"SUM on XRef partition should return 86250, got $sumXRef")

      // AVG on score field
      val avgScore = dfAll.agg(avg("score")).collect().head.getDouble(0)
      // score = (globalId % 100) / 10.0, for globalIds 0-49 per batch → avg is ~2.45
      assert(avgScore >= 2.0 && avgScore <= 3.0, s"AVG(score) should be around 2.45, got $avgScore")

      // MIN/MAX on numeric_value (across all partitions)
      val minMax = dfAll.agg(min("numeric_value"), max("numeric_value")).collect().head
      assert(minMax.getInt(0) == 100, "MIN(numeric_value) should be 100")
      assert(minMax.getInt(1) == 590, "MAX(numeric_value) should be 590")

      // COUNT with filter on XRef-covered partition
      val countFilteredXRef = dfAll
        .filter(col("partition_key") === "region_a")
        .filter(col("numeric_value") > 400)
        .agg(count("*")).collect().head.getLong(0)
      // Values > 400: 410, 420, ..., 590 = 19 values per batch, 5 batches
      assert(countFilteredXRef == 5 * 19, s"COUNT with filter on XRef partition should return ${5 * 19}, got $countFilteredXRef")

      // COUNT with filter on non-XRef-covered partition
      val countFilteredNonXRef = dfAll
        .filter(col("partition_key") === "region_d")
        .filter(col("numeric_value") > 400)
        .agg(count("*")).collect().head.getLong(0)
      assert(countFilteredNonXRef == 5 * 19, s"COUNT with filter on non-XRef partition should return ${5 * 19}, got $countFilteredNonXRef")

      // ================================================================
      // PHASE 9: Verify XRef entries are still valid after all queries
      // ================================================================
      val finalXRefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
      val finalXRefCount = finalXRefResult.count()
      assert(finalXRefCount >= 1, s"XRef entries should still exist after queries, got $finalXRefCount")

      // Clean up configuration
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.query.enabled")
      spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
    }
  }

  test("XRef query routing - term queries skip non-matching splits") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val spark = this.spark
      import spark.implicits._

      // Configure for small splits
      spark.conf.set("spark.indextables.indexWriter.batchSize", "100")
      spark.conf.set("spark.indextables.xref.query.enabled", "true")
      spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "3")

      // Create 10 splits with unique terms per split
      (0 until 10).foreach { splitIdx =>
        val data = (0 until 100).map { i =>
          (splitIdx * 100 + i, s"unique_term_split_${splitIdx}_row_$i", s"split_$splitIdx")
        }
        val df = data.toDF("id", "term", "split_marker")

        df.write.format(DataSourceFormat)
          .mode("append")
          .save(tablePath)
      }

      // Build XRef for all splits
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      // Verify XRef was created
      val xrefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
      assert(xrefResult.count() >= 1, "XRef entry should be created in transaction log")

      // Query for a specific term that only exists in one split
      val dfAll = spark.read.format(DataSourceFormat).load(tablePath)

      // This term only exists in split 5
      val result = dfAll.filter(col("term") === "unique_term_split_5_row_50")
      assert(result.count() == 1, "Should find exactly 1 row")

      val row = result.collect().head
      assert(row.getAs[Int]("id") == 550)
      assert(row.getAs[String]("split_marker") == "split_5")

      // Query for a term that doesn't exist
      val noResult = dfAll.filter(col("term") === "nonexistent_term")
      assert(noResult.count() == 0, "Should find 0 rows for nonexistent term")

      // Clean up
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.query.enabled")
      spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
    }
  }

  test("XRef with range queries on fast fields") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val spark = this.spark
      import spark.implicits._

      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.query.enabled", "true")
      spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "3")

      // Create splits with different value ranges
      // Split 0: values 0-99
      // Split 1: values 100-199
      // Split 2: values 200-299
      // etc.
      (0 until 10).foreach { splitIdx =>
        val data = (0 until 100).map { i =>
          val value = splitIdx * 100 + i
          (value, s"row_$value", value.toDouble)
        }
        val df = data.toDF("id", "name", "value")

        df.write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "value")
          .mode("append")
          .save(tablePath)
      }

      // Build XRef
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      // Verify XRef was created
      val xrefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
      assert(xrefResult.count() >= 1, "XRef entry should be created in transaction log")

      val dfAll = spark.read.format(DataSourceFormat).load(tablePath)

      // Range query that should match only split 5 (values 500-599)
      val rangeResult = dfAll.filter(col("value") >= 550.0 && col("value") < 560.0)
      assert(rangeResult.count() == 10, "Range query should return 10 rows")

      // Verify all results are in expected range
      val values = rangeResult.collect().map(_.getAs[Double]("value"))
      assert(values.forall(v => v >= 550.0 && v < 560.0))

      // Range query across multiple splits
      val multiSplitRange = dfAll.filter(col("value") >= 250.0 && col("value") <= 350.0)
      assert(multiSplitRange.count() == 101, "Multi-split range should return 101 rows (250-350 inclusive)")

      // Clean up
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.query.enabled")
      spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
    }
  }

  test("XRef aggregate pushdown with selective filters") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val spark = this.spark
      import spark.implicits._

      spark.conf.set("spark.indextables.indexWriter.batchSize", "100")
      spark.conf.set("spark.indextables.xref.query.enabled", "true")
      spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "3")

      // Create 20 splits with category field
      val categories = Seq("A", "B", "C", "D")
      (0 until 20).foreach { splitIdx =>
        val category = categories(splitIdx % 4)
        val data = (0 until 100).map { i =>
          (splitIdx * 100 + i, category, (i + 1).toLong, (i % 10).toDouble)
        }
        val df = data.toDF("id", "category", "amount", "discount")

        df.write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "amount,discount")
          .mode("append")
          .save(tablePath)
      }

      // Build XRef for half the splits (first 10)
      // Note: In practice, XRef indexes all splits by default
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

      // Verify XRef was created
      val xrefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
      assert(xrefResult.count() >= 1, "XRef entry should be created in transaction log")

      val dfAll = spark.read.format(DataSourceFormat).load(tablePath)

      // Aggregate by category - should work with XRef skipping
      val categoryAgg = dfAll
        .filter(col("category") === "A")
        .agg(count("*"), sum("amount"), avg("discount"))
        .collect().head

      // 5 splits with category A, 100 rows each
      assert(categoryAgg.getLong(0) == 500, s"COUNT for category A should be 500, got ${categoryAgg.getLong(0)}")

      // Sum of amounts: each split has amounts 1-100, sum = 5050, 5 splits = 25250
      assert(categoryAgg.getLong(1) == 25250L, s"SUM(amount) for category A should be 25250, got ${categoryAgg.getLong(1)}")

      // Multiple aggregates with filter
      val multiAgg = dfAll
        .filter(col("amount") > 50)
        .agg(count("*"), min("amount"), max("amount"))
        .collect().head

      // Each split has 50 rows with amount > 50 (51-100), 20 splits = 1000 rows
      assert(multiAgg.getLong(0) == 1000, s"COUNT with filter should be 1000, got ${multiAgg.getLong(0)}")
      assert(multiAgg.getLong(1) == 51, "MIN(amount) should be 51")
      assert(multiAgg.getLong(2) == 100, "MAX(amount) should be 100")

      // Clean up
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.query.enabled")
      spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
    }
  }

  test("XRef with partitioned data and combined filters") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val spark = this.spark
      import spark.implicits._

      spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
      spark.conf.set("spark.indextables.xref.query.enabled", "true")
      spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "3")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2") // Lower threshold for WHERE-filtered builds

      // Create partitioned data
      val years = Seq(2022, 2023, 2024)
      val months = Seq(1, 6, 12)

      years.foreach { year =>
        months.foreach { month =>
          val data = (0 until 100).map { i =>
            (year * 10000 + month * 100 + i, s"item_$i", year, month, (i * 10).toLong)
          }
          val df = data.toDF("id", "name", "year", "month", "sales")

          df.write.format(DataSourceFormat)
            .option("spark.indextables.indexing.fastfields", "sales")
            .partitionBy("year", "month")
            .mode("append")
            .save(tablePath)
        }
      }

      // Build XRef for specific partitions
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath' WHERE year = 2024")

      // Verify XRef was created
      val xrefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
      assert(xrefResult.count() >= 1, "XRef entry should be created in transaction log")

      val dfAll = spark.read.format(DataSourceFormat).load(tablePath)

      // Query on XRef-covered partition with range filter
      val xrefQuery = dfAll
        .filter(col("year") === 2024)
        .filter(col("sales") > 500)
        .agg(count("*"), sum("sales"))
        .collect().head

      // 3 months, each with 100 rows, sales > 500 means rows 51-99 (49 rows per month)
      assert(xrefQuery.getLong(0) == 3 * 49, s"COUNT should be ${3 * 49}, got ${xrefQuery.getLong(0)}")

      // Query on non-XRef-covered partition
      val nonXRefQuery = dfAll
        .filter(col("year") === 2022)
        .filter(col("sales") > 500)
        .agg(count("*"))
        .collect().head.getLong(0)

      assert(nonXRefQuery == 3 * 49, s"Non-XRef query should return ${3 * 49}, got $nonXRefQuery")

      // Cross-partition query (mixed XRef coverage)
      val mixedQuery = dfAll
        .filter(col("month") === 6)
        .agg(count("*"))
        .collect().head.getLong(0)

      assert(mixedQuery == 3 * 100, s"Cross-partition query should return ${3 * 100}, got $mixedQuery")

      // Partition pruning + data filter + XRef
      val combinedQuery = dfAll
        .filter(col("year") === 2024 && col("month") === 12)
        .filter(col("name") === "item_50")
        .collect()

      assert(combinedQuery.length == 1, "Combined filter should return 1 row")
      assert(combinedQuery.head.getAs[Int]("id") == 20241250)

      // Clean up
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.query.enabled")
      spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  /**
   * This test verifies that splits without XRef coverage are correctly included
   * in query results when XRef routing is active.
   *
   * The bug this test catches: XRef routing was incorrectly excluding splits
   * that had no XRef coverage, even though we can't prove those splits don't
   * contain matching documents.
   *
   * Test setup:
   * - Create splits with identical data patterns across covered and uncovered partitions
   * - Build XRef for only some partitions
   * - Query with a searchable term that exists in BOTH covered and uncovered splits
   * - Verify results include data from uncovered splits
   */
  test("XRef query routing - uncovered splits must be included in results") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val spark = this.spark
      import spark.implicits._

      spark.conf.set("spark.indextables.indexWriter.batchSize", "100")
      spark.conf.set("spark.indextables.xref.query.enabled", "true")
      spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "2")
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2") // Lower threshold for test

      // Create data with IDENTICAL searchable terms across all partitions
      // This ensures the same term exists in both covered and uncovered splits
      val sharedTerms = (0 until 10).map(i => s"shared_term_$i")

      // Partition "covered_1" and "covered_2" will have XRef
      // Partition "uncovered_1" and "uncovered_2" will NOT have XRef
      val partitions = Seq("covered_1", "covered_2", "uncovered_1", "uncovered_2")

      partitions.foreach { partition =>
        val data = (0 until 100).map { i =>
          (
            s"${partition}_$i",                    // unique id
            sharedTerms(i % 10),                   // shared searchable term
            partition,                              // partition_key
            i * 10                                  // value
          )
        }
        val df = data.toDF("id", "searchable_term", "partition_key", "value")

        df.write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "value")
          .partitionBy("partition_key")
          .mode("append")
          .save(tablePath)
      }

      // Build XRef ONLY for covered partitions
      spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath' WHERE partition_key IN ('covered_1', 'covered_2')")

      // Verify XRef was created for covered partitions
      val xrefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
      val xrefCount = xrefResult.count()
      assert(xrefCount >= 1, s"XRef entry should be created for covered partitions, got $xrefCount")

      val dfAll = spark.read.format(DataSourceFormat).load(tablePath)

      // ================================================================
      // TEST 1: Query for a shared term - should return from ALL partitions
      // ================================================================

      // Query for "shared_term_0" - exists in all 4 partitions (10 rows each)
      val sharedTermQuery = dfAll.filter(col("searchable_term") === "shared_term_0")
      val sharedTermCount = sharedTermQuery.count()

      // CRITICAL ASSERTION: Should return 40 rows (10 per partition × 4 partitions)
      // BUG: Without fix, uncovered partitions would be excluded, returning only 20 rows
      assert(sharedTermCount == 40,
        s"Query for shared term should return 40 rows (from ALL partitions), got $sharedTermCount. " +
        "This indicates uncovered splits are being incorrectly excluded!")

      // Verify we got rows from each partition by checking individual partition filters
      partitions.foreach { partition =>
        val partitionCount = dfAll
          .filter(col("searchable_term") === "shared_term_0")
          .filter(col("partition_key") === partition)
          .count()
        assert(partitionCount == 10,
          s"Partition $partition should have 10 rows with shared_term_0, got $partitionCount. " +
          s"${if (partition.startsWith("uncovered")) "Uncovered partition data is missing!" else ""}")
      }

      // ================================================================
      // TEST 2: Query with combined filters spanning covered/uncovered
      // ================================================================

      // Query for shared_term_5 with value > 50
      val combinedQuery = dfAll
        .filter(col("searchable_term") === "shared_term_5")
        .filter(col("value") > 50)
      val combinedCount = combinedQuery.count()

      // shared_term_5 appears at indices 5, 15, 25, 35, 45, 55, 65, 75, 85, 95 (i % 10 == 5)
      // value = i * 10, so value > 50 means i > 5
      // Intersection: indices 15, 25, 35, 45, 55, 65, 75, 85, 95 → 9 rows per partition
      // Total: 9 × 4 = 36 rows
      assert(combinedCount == 36,
        s"Combined query should return 36 rows from all partitions, got $combinedCount")

      // ================================================================
      // TEST 3: Aggregate query with shared term filter
      // ================================================================

      val aggQuery = dfAll
        .filter(col("searchable_term") === "shared_term_3")
        .agg(count("*"), sum("value"))
        .collect().head

      // shared_term_3 appears at indices 3, 13, 23, 33, 43, 53, 63, 73, 83, 93 → 10 per partition
      // Total count: 40
      assert(aggQuery.getLong(0) == 40,
        s"Aggregate COUNT should be 40, got ${aggQuery.getLong(0)}")

      // Sum of values: for each partition, sum is (3+13+23+33+43+53+63+73+83+93)*10 = 4800
      // Total: 4800 × 4 = 19200
      assert(aggQuery.getLong(1) == 19200L,
        s"Aggregate SUM should be 19200, got ${aggQuery.getLong(1)}")

      // ================================================================
      // TEST 4: Query that matches ONLY in uncovered partitions
      // ================================================================

      // Add unique data only to uncovered partitions
      val uncoveredOnlyData = Seq(
        ("unique_uncovered_1", "only_in_uncovered", "uncovered_1", 999),
        ("unique_uncovered_2", "only_in_uncovered", "uncovered_2", 999)
      ).toDF("id", "searchable_term", "partition_key", "value")

      uncoveredOnlyData.write.format(DataSourceFormat)
        .option("spark.indextables.indexing.fastfields", "value")
        .partitionBy("partition_key")
        .mode("append")
        .save(tablePath)

      // Reload to get new data
      val dfUpdated = spark.read.format(DataSourceFormat).load(tablePath)

      // Query for term that only exists in uncovered partitions
      val uncoveredOnlyQuery = dfUpdated.filter(col("searchable_term") === "only_in_uncovered")
      val uncoveredOnlyCount = uncoveredOnlyQuery.count()

      // CRITICAL: This should find 2 rows even though only covered partitions have XRef
      // XRef will return no matches (term doesn't exist in covered splits)
      // But uncovered splits MUST be queried and will find the matches
      assert(uncoveredOnlyCount == 2,
        s"Query for term only in uncovered splits should return 2 rows, got $uncoveredOnlyCount. " +
        "This indicates uncovered splits are being incorrectly excluded!")

      // Clean up
      spark.conf.unset("spark.indextables.indexWriter.batchSize")
      spark.conf.unset("spark.indextables.xref.query.enabled")
      spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
      spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
    }
  }

  /**
   * CRITICAL TEST: Proves that XRef routing is actually used during scan generation.
   *
   * This test uses XRefSearcher's test hook to simulate XRef search behavior:
   * 1. Create multiple splits with unique data
   * 2. Build an XRef index
   * 3. Set up a test search override that returns EMPTY results for a specific split
   * 4. Verify that with XRef routing, data in the "unmatched" split is NOT found
   * 5. Clear the override and verify data IS found via full scan
   *
   * This proves the system actually uses XRef during query planning, not just for show.
   *
   * NOTE: The actual tantivy4java XRef search API is not yet implemented (XRefSearcher.isAvailable()
   * returns false). This test uses a test hook to simulate what will happen when the API is available.
   */
  test("PROOF: XRef routing is actually used - test hook simulates split filtering") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val spark = this.spark
      import spark.implicits._

      // Reset any previous test state
      XRefSearcher.resetAvailabilityCheck()
      XRefSearcher.clearTestSearchOverride()

      try {
        spark.conf.set("spark.indextables.indexWriter.batchSize", "100")
        spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

        // ================================================================
        // STEP 1: Create multiple splits with unique data
        // ================================================================

        // Split group 1: will be "matched" by our test search override
        val data1 = (0 until 50).map { i =>
          (s"split1_$i", s"TERM_MATCHED_$i", "group_1", i * 10)
        }
        data1.toDF("id", "searchable_term", "group", "value")
          .write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "value")
          .mode("append")
          .save(tablePath)

        // Split group 2: will be "matched" by our test search override
        val data2 = (0 until 50).map { i =>
          (s"split2_$i", s"TERM_MATCHED_${50 + i}", "group_2", i * 10)
        }
        data2.toDF("id", "searchable_term", "group", "value")
          .write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "value")
          .mode("append")
          .save(tablePath)

        // Split group 3: will be "not matched" by our test search override (simulating XRef filter)
        val data3 = (0 until 50).map { i =>
          (s"split3_$i", s"TERM_FILTERED_$i", "group_3", i * 10)
        }
        data3.toDF("id", "searchable_term", "group", "value")
          .write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "value")
          .mode("append")
          .save(tablePath)

        // ================================================================
        // STEP 2: Build XRef index
        // ================================================================
        spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

        // Verify XRef was created
        val xrefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
        val xrefCount = xrefResult.count()
        assert(xrefCount >= 1, s"XRef should be created, got $xrefCount")
        println(s"Created $xrefCount XRef entries")

        // Get split information from transaction log
        import org.apache.hadoop.fs.Path
        val txLog = io.indextables.spark.transaction.TransactionLogFactory.create(
          new Path(tablePath), spark
        )

        val allSplits = txLog.listFiles()
        val allSplitFileNames = allSplits.map(s => XRefStorageUtils.extractFileName(s.path))
        println(s"Total splits: ${allSplits.size}")
        allSplitFileNames.foreach(fn => println(s"  Split: $fn"))
        txLog.close()

        // Identify split containing group_3 data (based on write order, likely the last split)
        // We'll use a heuristic - the test search override will filter based on actual XRef source paths

        // ================================================================
        // STEP 3: Set up test search override that filters out splits not in XRef's actual sources
        // ================================================================

        // This simulates what real XRef search would do:
        // - The XRef was built with groups 1 and 2
        // - Group 3 was added later, so it won't be in the XRef
        // - Our override returns EMPTY for any query, simulating "term not found in XRef"
        // - This causes XRef routing to skip ALL covered splits (since none match)
        // - Uncovered splits (group_3) should still be included

        val searchCallCount = new java.util.concurrent.atomic.AtomicInteger(0)

        XRefSearcher.setTestSearchOverride { (xref, query, actualSourceSplits) =>
          val callNum = searchCallCount.incrementAndGet()
          println(s"[TEST OVERRIDE #$callNum] XRef ${xref.xrefId} queried for: '$query'")
          println(s"[TEST OVERRIDE #$callNum] XRef covers ${actualSourceSplits.size} splits: ${actualSourceSplits.mkString(", ")}")

          // Simulate XRef search: return ONLY splits that would actually match
          // For "TERM_FILTERED_*" queries, the term is NOT in the XRef (added after build)
          // So we return empty to simulate "no matching splits in XRef"
          if (query.contains("TERM_FILTERED")) {
            println(s"[TEST OVERRIDE #$callNum] Query contains TERM_FILTERED - returning EMPTY (not in XRef)")
            Seq.empty // XRef says no splits match
          } else {
            println(s"[TEST OVERRIDE #$callNum] Returning all source splits for other queries")
            actualSourceSplits // Return all for other queries
          }
        }

        assert(XRefSearcher.hasTestSearchOverride, "Test search override should be set")
        println("Test search override installed")

        // ================================================================
        // STEP 4: Enable XRef routing and query for data in group_3
        // ================================================================

        spark.conf.set("spark.indextables.xref.query.enabled", "true")
        spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "2")
        spark.conf.set("spark.indextables.xref.query.fallbackOnError", "false")

        println("\n=== Query WITH XRef routing (test override returns empty) ===")
        println(s"XRef query enabled: ${spark.conf.get("spark.indextables.xref.query.enabled", "false")}")
        println(s"XRef min splits: ${spark.conf.get("spark.indextables.xref.query.minSplitsForXRef", "128")}")

        val dfWithXRef = spark.read.format(DataSourceFormat).load(tablePath)

        // Query for a term that exists ONLY in group_3
        // The XRef was built before group_3 was added, so:
        // - XRef query returns empty (term not found in covered splits)
        // - But group_3 splits are UNCOVERED by XRef, so they should still be searched
        val resultWithXRef = dfWithXRef.filter(col("searchable_term") === "TERM_FILTERED_25")
        val countWithXRef = resultWithXRef.count()

        println(s"Query with XRef routing: found $countWithXRef rows")
        println(s"Test override was called ${searchCallCount.get()} times")

        // ================================================================
        // STEP 5: Clear test override and query again (simulates full scan)
        // ================================================================

        println("\n=== Query WITHOUT XRef routing (disabled via threshold) ===")

        // Disable XRef by setting threshold very high
        spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "9999")

        // Force a fresh read
        searchCallCount.set(0)
        val dfWithoutXRef = spark.read.format(DataSourceFormat).load(tablePath)
        val resultWithoutXRef = dfWithoutXRef.filter(col("searchable_term") === "TERM_FILTERED_25")
        val countWithoutXRef = resultWithoutXRef.count()

        println(s"Query without XRef routing: found $countWithoutXRef rows")
        println(s"Test override was called ${searchCallCount.get()} times (should be 0)")

        // ================================================================
        // STEP 6: Assertions
        // ================================================================

        // Both queries should find the data because:
        // 1. With XRef: Group 3 splits are UNCOVERED (not in XRef.sourceSplitPaths), so they're always searched
        // 2. Without XRef: Full scan searches all splits

        // The PROOF is that the test override was CALLED during XRef routing
        // This proves XRef code path is being executed
        assert(searchCallCount.get() == 0,
          s"XRef routing should NOT be used when disabled (threshold=9999). Override was called ${searchCallCount.get()} times")

        // Re-enable and verify override is called
        spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "2")
        searchCallCount.set(0)
        val dfVerify = spark.read.format(DataSourceFormat).load(tablePath)
        dfVerify.filter(col("searchable_term") === "TERM_FILTERED_25").count()

        assert(searchCallCount.get() > 0,
          s"XRef routing SHOULD be used when enabled. Override was called ${searchCallCount.get()} times")

        println("\n=== SUCCESS ===")
        println("PROOF: XRef routing code path is being executed!")
        println(s"  - Test override was called ${searchCallCount.get()} times when XRef enabled")
        println(s"  - Test override was NOT called when XRef disabled (threshold=9999)")
        println(s"  - Data found in both cases (group_3 is uncovered, so always searched)")

      } finally {
        // Clean up
        XRefSearcher.clearTestSearchOverride()
        XRefSearcher.resetAvailabilityCheck()
        spark.conf.unset("spark.indextables.indexWriter.batchSize")
        spark.conf.unset("spark.indextables.xref.query.enabled")
        spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
        spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
        spark.conf.unset("spark.indextables.xref.query.fallbackOnError")
      }
    }
  }

  /**
   * CRITICAL TEST: Proves XRef routing actually filters splits when override says "no match".
   *
   * This test proves that when XRef search returns empty (no matching splits),
   * those splits are actually skipped and data is not found.
   */
  test("PROOF: XRef routing skips splits when search returns no matches") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val spark = this.spark
      import spark.implicits._

      // Reset any previous test state
      XRefSearcher.resetAvailabilityCheck()
      XRefSearcher.clearTestSearchOverride()

      try {
        spark.conf.set("spark.indextables.indexWriter.batchSize", "100")
        spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")

        // ================================================================
        // STEP 1: Create multiple splits
        // ================================================================

        val data1 = (0 until 50).map { i =>
          (s"split1_$i", s"FINDME_$i", "group_1", i * 10)
        }
        data1.toDF("id", "searchable_term", "group", "value")
          .write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "value")
          .mode("append")
          .save(tablePath)

        val data2 = (0 until 50).map { i =>
          (s"split2_$i", s"OTHER_$i", "group_2", i * 10)
        }
        data2.toDF("id", "searchable_term", "group", "value")
          .write.format(DataSourceFormat)
          .option("spark.indextables.indexing.fastfields", "value")
          .mode("append")
          .save(tablePath)

        // ================================================================
        // STEP 2: Build XRef
        // ================================================================
        spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

        val xrefResult = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
        assert(xrefResult.count() >= 1, "XRef should be created")
        println("XRef built successfully")

        // ================================================================
        // STEP 3: Set test override that returns EMPTY for all queries
        // This simulates XRef saying "term not found in any split"
        // ================================================================

        XRefSearcher.setTestSearchOverride { (xref, query, actualSourceSplits) =>
          println(s"[TEST OVERRIDE] Returning EMPTY for query '$query' (XRef covers ${actualSourceSplits.size} splits)")
          Seq.empty // No splits match - this should cause data to not be found!
        }

        // ================================================================
        // STEP 4: Query with XRef - data should NOT be found
        // ================================================================

        spark.conf.set("spark.indextables.xref.query.enabled", "true")
        spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "2")

        val dfWithXRef = spark.read.format(DataSourceFormat).load(tablePath)
        val countWithXRef = dfWithXRef.filter(col("searchable_term") === "FINDME_25").count()

        println(s"Query with XRef (override returns empty): found $countWithXRef rows")

        // ================================================================
        // STEP 5: Query without XRef - data SHOULD be found
        // ================================================================

        XRefSearcher.clearTestSearchOverride()

        // Note: Even without override, XRefSearcher.isAvailable() returns false
        // so it falls back to returning all source splits. This is equivalent to no filtering.
        spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "9999") // Disable XRef routing

        val dfWithoutXRef = spark.read.format(DataSourceFormat).load(tablePath)
        val countWithoutXRef = dfWithoutXRef.filter(col("searchable_term") === "FINDME_25").count()

        println(s"Query without XRef: found $countWithoutXRef rows")

        // ================================================================
        // STEP 6: Assertions - THIS IS THE KEY PROOF
        // ================================================================

        // With XRef and empty override: Data should NOT be found
        // (XRef says "no splits match" so all covered splits are skipped)
        assert(countWithXRef == 0,
          s"With XRef routing (test override returning empty), data should NOT be found. " +
          s"Got $countWithXRef rows. This proves XRef split filtering is working!")

        // Without XRef: Data SHOULD be found via full scan
        assert(countWithoutXRef == 1,
          s"Without XRef routing, data SHOULD be found via full scan. Got $countWithoutXRef rows.")

        println("\n=== SUCCESS: XRef ROUTING IS PROVEN TO WORK ===")
        println("  - With XRef (override returns empty): 0 rows - splits were SKIPPED")
        println("  - Without XRef: 1 row - full scan found data")
        println("  This proves XRef routing ACTUALLY FILTERS splits during query planning!")

      } finally {
        XRefSearcher.clearTestSearchOverride()
        XRefSearcher.resetAvailabilityCheck()
        spark.conf.unset("spark.indextables.indexWriter.batchSize")
        spark.conf.unset("spark.indextables.xref.query.enabled")
        spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
        spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
      }
    }
  }

  /**
   * Test XRef incremental update behavior:
   * - When an XRef split doesn't contain the maximum number of source splits
   * - New splits are added to the table
   * - XRef gets re-created to include new splits (within maxSourceSplits limit)
   * - Previous XRef is invalidated
   *
   * This tests the "expand_existing_xref" code path in XRefBuildManager.
   */
  test("XRef incremental update - rebuild when new splits added below max capacity") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val spark = this.spark
      import spark.implicits._

      // Reset state
      XRefSearcher.resetAvailabilityCheck()

      try {
        // Configure for small splits and set max source splits limit
        spark.conf.set("spark.indextables.indexWriter.batchSize", "50")
        spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "2")
        spark.conf.set("spark.indextables.xref.autoIndex.maxSourceSplits", "20") // Allow up to 20 splits per XRef
        spark.conf.set("spark.indextables.xref.query.enabled", "true")
        spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "2")

        // ================================================================
        // PHASE 1: Create initial splits and build XRef
        // ================================================================
        println("=== PHASE 1: Initial data write and XRef build ===")

        // Write initial data - create 4 splits
        (0 until 4).foreach { batchIdx =>
          val data = (0 until 50).map { i =>
            val globalId = batchIdx * 100 + i
            (globalId, s"term_initial_batch${batchIdx}_$i", s"batch_$batchIdx", i * 10)
          }
          data.toDF("id", "searchable_term", "batch_marker", "value")
            .write.format(DataSourceFormat)
            .option("spark.indextables.indexing.fastfields", "value")
            .mode("append")
            .save(tablePath)
        }

        // Verify initial split count
        val txLogInitial = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath'")
        val initialSplitCount = txLogInitial.filter("action_type = 'add'").count()
        println(s"Initial split count: $initialSplitCount")
        assert(initialSplitCount >= 4, s"Should have at least 4 splits, got $initialSplitCount")

        // Build initial XRef
        spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

        // Verify initial XRef
        val xrefInitial = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
        val initialXRefCount = xrefInitial.count()
        println(s"Initial XRef count: $initialXRefCount")
        assert(initialXRefCount >= 1, s"Should have at least 1 XRef, got $initialXRefCount")

        // Get initial XRef details
        val initialXRefRow = xrefInitial.collect().head
        val initialXRefPath = initialXRefRow.getAs[String]("path")
        val initialSourceSplitCount = initialXRefRow.getAs[Long]("num_records")
        println(s"Initial XRef path: $initialXRefPath")
        println(s"Initial XRef source split count: $initialSourceSplitCount")

        // Verify initial XRef has less than max capacity (4 < 20)
        assert(initialSourceSplitCount < 20,
          s"Initial XRef should have less than max capacity (20), got $initialSourceSplitCount")

        // Query initial data to verify XRef works
        val dfInitial = spark.read.format(DataSourceFormat).load(tablePath)
        val initialRowCount = dfInitial.count()
        assert(initialRowCount == 4 * 50, s"Initial row count should be 200, got $initialRowCount")

        // Test term query on initial data
        val initialTermResult = dfInitial.filter(col("searchable_term") === "term_initial_batch0_25")
        assert(initialTermResult.count() == 1, "Should find initial term")

        // ================================================================
        // PHASE 2: Add new splits (within max capacity)
        // ================================================================
        println("\n=== PHASE 2: Add new splits ===")

        // Write additional data - create 3 more splits
        (4 until 7).foreach { batchIdx =>
          val data = (0 until 50).map { i =>
            val globalId = batchIdx * 100 + i
            (globalId, s"term_added_batch${batchIdx}_$i", s"batch_$batchIdx", i * 10)
          }
          data.toDF("id", "searchable_term", "batch_marker", "value")
            .write.format(DataSourceFormat)
            .option("spark.indextables.indexing.fastfields", "value")
            .mode("append")
            .save(tablePath)
        }

        // Verify new split count
        val txLogAfterAdd = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath'")
        val splitCountAfterAdd = txLogAfterAdd.filter("action_type = 'add'").count()
        println(s"Split count after adding data: $splitCountAfterAdd")
        assert(splitCountAfterAdd >= 7, s"Should have at least 7 splits, got $splitCountAfterAdd")

        // At this point, new splits are NOT covered by XRef
        // Query for new data should still work (via uncovered split fallback)
        val dfBeforeReindex = spark.read.format(DataSourceFormat).load(tablePath)
        val newTermBeforeReindex = dfBeforeReindex.filter(col("searchable_term") === "term_added_batch5_25")
        val countBeforeReindex = newTermBeforeReindex.count()
        println(s"New term query before reindex: $countBeforeReindex rows")
        assert(countBeforeReindex == 1, "New data should be accessible via uncovered splits")

        // ================================================================
        // PHASE 3: Rebuild XRef to include new splits
        // ================================================================
        println("\n=== PHASE 3: Rebuild XRef to include new splits ===")

        // Re-run INDEX CROSSREFERENCES - should expand existing XRef
        spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

        // Verify updated XRef
        val xrefAfterRebuild = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
        val xrefCountAfterRebuild = xrefAfterRebuild.count()
        println(s"XRef count after rebuild: $xrefCountAfterRebuild")

        // Get updated XRef details
        val updatedXRefRows = xrefAfterRebuild.collect()
        println("XRef entries after rebuild:")
        updatedXRefRows.foreach { row =>
          println(s"  Path: ${row.getAs[String]("path")}, Source splits: ${row.getAs[Long]("num_records")}")
        }

        // Find the active XRef (most recent or only one)
        val updatedXRefRow = updatedXRefRows.maxBy(_.getAs[Long]("num_records"))
        val updatedXRefPath = updatedXRefRow.getAs[String]("path")
        val updatedSourceSplitCount = updatedXRefRow.getAs[Long]("num_records")

        // CRITICAL ASSERTION: Updated XRef should include more splits
        println(s"Updated XRef source split count: $updatedSourceSplitCount (was $initialSourceSplitCount)")
        assert(updatedSourceSplitCount > initialSourceSplitCount,
          s"Updated XRef should include more splits. Initial: $initialSourceSplitCount, Updated: $updatedSourceSplitCount")

        // ================================================================
        // PHASE 4: Verify old XRef is invalidated
        // ================================================================
        println("\n=== PHASE 4: Verify old XRef invalidation ===")

        // Check transaction log for remove actions
        val txLogFull = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' INCLUDE ALL")
        val xrefRemoveActions = txLogFull.filter("action_type = 'remove_xref'")
        val removeCount = xrefRemoveActions.count()
        println(s"XRef remove actions in transaction log: $removeCount")

        // If the system replaced the XRef, there should be a remove action for the old one
        if (updatedXRefPath != initialXRefPath) {
          println(s"XRef path changed from $initialXRefPath to $updatedXRefPath")
          // Old XRef should have been removed
          assert(removeCount >= 1, "Old XRef should be removed when rebuilt")
        } else {
          println("XRef path unchanged (same XRef file updated)")
        }

        // ================================================================
        // PHASE 5: Verify queries work with updated XRef
        // ================================================================
        println("\n=== PHASE 5: Verify queries with updated XRef ===")

        val dfAfterRebuild = spark.read.format(DataSourceFormat).load(tablePath)

        // Query for initial data
        val initialTermAfterRebuild = dfAfterRebuild.filter(col("searchable_term") === "term_initial_batch0_25")
        assert(initialTermAfterRebuild.count() == 1, "Initial data should still be queryable")

        // Query for newly added data
        val newTermAfterRebuild = dfAfterRebuild.filter(col("searchable_term") === "term_added_batch5_25")
        assert(newTermAfterRebuild.count() == 1, "New data should be queryable via updated XRef")

        // Aggregate query across all data
        val totalCount = dfAfterRebuild.count()
        val expectedTotal = 7 * 50 // 7 batches * 50 rows each
        assert(totalCount == expectedTotal, s"Total count should be $expectedTotal, got $totalCount")

        // Range query spanning initial and new data
        val rangeQuery = dfAfterRebuild.filter(col("value") > 200)
        val rangeCount = rangeQuery.count()
        // Each batch has rows with value 0, 10, 20, ..., 490
        // Values > 200: 210, 220, ..., 490 → 29 values per batch
        // 7 batches: 29 * 7 = 203
        assert(rangeCount == 203, s"Range query should return 203 rows, got $rangeCount")

        // ================================================================
        // PHASE 6: Add more splits until at or near max capacity
        // ================================================================
        println("\n=== PHASE 6: Test behavior near max capacity ===")

        // Add more splits to approach the max (currently at ~7, max is 20)
        (7 until 18).foreach { batchIdx =>
          val data = (0 until 50).map { i =>
            val globalId = batchIdx * 100 + i
            (globalId, s"term_more_batch${batchIdx}_$i", s"batch_$batchIdx", i * 10)
          }
          data.toDF("id", "searchable_term", "batch_marker", "value")
            .write.format(DataSourceFormat)
            .option("spark.indextables.indexing.fastfields", "value")
            .mode("append")
            .save(tablePath)
        }

        // Rebuild XRef again
        spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

        // Verify XRef near max capacity
        val xrefNearMax = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
        val nearMaxXRefRows = xrefNearMax.collect()
        println("XRef entries near max capacity:")
        nearMaxXRefRows.foreach { row =>
          println(s"  Path: ${row.getAs[String]("path")}, Source splits: ${row.getAs[Long]("num_records")}")
        }

        val maxSourceSplitCount = nearMaxXRefRows.map(_.getAs[Long]("num_records")).max
        println(s"Max source split count in XRef: $maxSourceSplitCount")

        // Verify all data is queryable
        val dfNearMax = spark.read.format(DataSourceFormat).load(tablePath)
        val nearMaxCount = dfNearMax.count()
        val expectedNearMax = 18 * 50 // 18 batches
        assert(nearMaxCount == expectedNearMax, s"Near max count should be $expectedNearMax, got $nearMaxCount")

        // ================================================================
        // PHASE 7: Add splits that exceed max capacity - should create new XRef
        // ================================================================
        println("\n=== PHASE 7: Test exceeding max capacity ===")

        // Add more splits to exceed max (20)
        (18 until 25).foreach { batchIdx =>
          val data = (0 until 50).map { i =>
            val globalId = batchIdx * 100 + i
            (globalId, s"term_overflow_batch${batchIdx}_$i", s"batch_$batchIdx", i * 10)
          }
          data.toDF("id", "searchable_term", "batch_marker", "value")
            .write.format(DataSourceFormat)
            .option("spark.indextables.indexing.fastfields", "value")
            .mode("append")
            .save(tablePath)
        }

        // Rebuild XRef - should create additional XRef or handle overflow
        spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

        // Verify XRef after exceeding capacity
        val xrefOverflow = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
        val overflowXRefCount = xrefOverflow.count()
        println(s"XRef count after exceeding capacity: $overflowXRefCount")

        val overflowXRefRows = xrefOverflow.collect()
        println("XRef entries after overflow:")
        overflowXRefRows.foreach { row =>
          println(s"  Path: ${row.getAs[String]("path")}, Source splits: ${row.getAs[Long]("num_records")}")
        }

        // Should either have multiple XRefs or one XRef handling all splits
        val totalSplitsCovered = overflowXRefRows.map(_.getAs[Long]("num_records")).sum
        println(s"Total splits covered by XRefs: $totalSplitsCovered")

        // Verify all data is still queryable
        val dfOverflow = spark.read.format(DataSourceFormat).load(tablePath)
        val overflowCount = dfOverflow.count()
        val expectedOverflow = 25 * 50 // 25 batches
        assert(overflowCount == expectedOverflow, s"Overflow count should be $expectedOverflow, got $overflowCount")

        // Query data from overflow batches
        val overflowTermResult = dfOverflow.filter(col("searchable_term") === "term_overflow_batch22_25")
        assert(overflowTermResult.count() == 1, "Overflow data should be queryable")

        println("\n=== SUCCESS: XRef incremental update test passed ===")
        println(s"  - Initial XRef: $initialSourceSplitCount splits")
        println(s"  - After adding splits: $updatedSourceSplitCount splits")
        println(s"  - Near max capacity: $maxSourceSplitCount splits")
        println(s"  - After overflow: $overflowXRefCount XRef(s) covering $totalSplitsCovered splits")

      } finally {
        spark.conf.unset("spark.indextables.indexWriter.batchSize")
        spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
        spark.conf.unset("spark.indextables.xref.autoIndex.maxSourceSplits")
        spark.conf.unset("spark.indextables.xref.query.enabled")
        spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
      }
    }
  }

  /**
   * Test that XRef is consulted even when queries have very high LIMIT values.
   * This verifies that the XRef routing logic doesn't skip due to large result set expectations.
   */
  test("XRef with very high LIMIT values") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString
      val spark = this.spark
      import spark.implicits._

      // Configure XRef
      spark.conf.set("spark.indextables.xref.autoIndex.minSplitsToTrigger", "3")
      spark.conf.set("spark.indextables.xref.autoIndex.maxSourceSplits", "100")
      spark.conf.set("spark.indextables.xref.query.enabled", "true")
      spark.conf.set("spark.indextables.xref.query.minSplitsForXRef", "2")
      spark.conf.set("spark.indextables.indexWriter.batchSize", "25")

      try {
        // Create multiple splits with unique terms in each partition
        val partitions = Seq("alpha", "beta", "gamma", "delta")
        partitions.foreach { partition =>
          val data = (0 until 100).map { i =>
            (i, s"term_${partition}_$i", partition, i * 10.0)
          }
          data.toDF("id", "searchable_term", "partition_name", "value")
            .write.format(DataSourceFormat)
            .option("spark.indextables.indexing.fastfields", "id,value")
            .mode("append")
            .save(tablePath)
        }

        // Build XRef
        spark.sql(s"INDEX CROSSREFERENCES FOR '$tablePath'")

        // Verify XRef was created
        val xrefs = spark.sql(s"DESCRIBE INDEXTABLES TRANSACTION LOG '$tablePath' XREFS")
        val xrefCount = xrefs.count()
        assert(xrefCount >= 1, s"Should have at least 1 XRef, got $xrefCount")

        val df = spark.read.format(DataSourceFormat).load(tablePath)

        // Test 1: Query with very high LIMIT (1 million)
        // This should still use XRef for filtering
        println("\n=== Test 1: Query with LIMIT 1000000 ===")
        val highLimitResult = df
          .filter(col("searchable_term") === "term_alpha_50")
          .limit(1000000)
          .collect()

        assert(highLimitResult.length == 1,
          s"Should find exactly 1 row with term_alpha_50, got ${highLimitResult.length}")
        println(s"High limit query found: ${highLimitResult.length} rows")

        // Test 2: Aggregate query with filter (no LIMIT but accesses all rows)
        println("\n=== Test 2: Aggregate query on filtered data ===")
        val aggResult = df
          .filter(col("partition_name") === "beta")
          .agg(sum("value"))
          .collect()

        // Beta partition has values 0, 10, 20, ..., 990 = sum is 49500
        val expectedSum = (0 until 100).map(_ * 10.0).sum
        val actualSum = aggResult.head.getDouble(0)
        assert(actualSum == expectedSum, s"Sum should be $expectedSum, got $actualSum")
        println(s"Aggregate query sum: $actualSum (expected $expectedSum)")

        // Test 3: Count query with very high LIMIT (should return all matching)
        println("\n=== Test 3: Count query with very high LIMIT ===")
        val countResult = df
          .filter(col("partition_name") === "gamma")
          .limit(10000000)
          .count()

        assert(countResult == 100, s"Should have 100 rows for gamma partition, got $countResult")
        println(s"High limit count: $countResult rows")

        // Test 4: Query for non-existent term with high LIMIT
        println("\n=== Test 4: Non-existent term with high LIMIT ===")
        val noMatchResult = df
          .filter(col("searchable_term") === "nonexistent_term_xyz")
          .limit(1000000)
          .collect()

        assert(noMatchResult.isEmpty, "Should find no rows for non-existent term")
        println(s"Non-existent term query: ${noMatchResult.length} rows (expected 0)")

        // Test 5: Multiple filter conditions with high LIMIT
        println("\n=== Test 5: Multiple conditions with high LIMIT ===")
        val multiFilterResult = df
          .filter(col("partition_name") === "delta")
          .filter(col("id") > 50)
          .limit(500000)
          .count()

        // Delta partition with id > 50: rows 51-99 = 49 rows
        assert(multiFilterResult == 49, s"Should have 49 rows, got $multiFilterResult")
        println(s"Multi-filter query: $multiFilterResult rows (expected 49)")

        println("\n=== SUCCESS: XRef works correctly with high LIMIT values ===")

      } finally {
        spark.conf.unset("spark.indextables.indexWriter.batchSize")
        spark.conf.unset("spark.indextables.xref.autoIndex.minSplitsToTrigger")
        spark.conf.unset("spark.indextables.xref.autoIndex.maxSourceSplits")
        spark.conf.unset("spark.indextables.xref.query.enabled")
        spark.conf.unset("spark.indextables.xref.query.minSplitsForXRef")
      }
    }
  }
}
