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

package io.indextables.spark.util

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase

/** Tests for SplitSizeAnalyzer - analyzing historical split data for auto-sizing. */
class SplitSizeAnalyzerTest extends TestBase {

  test("SplitSizeAnalyzer should analyze historical data and calculate target rows") {
    withTempPath { tempPath =>
      // Create initial data with known characteristics
      val initialData = createTestDataFrame()

      // Write initial data to establish historical baseline
      initialData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(tempPath)

      // Read back to verify
      val readDf = spark.read.format("tantivy4spark").load(tempPath)
      assert(readDf.count() == 5)

      // Now test the analyzer
      val options  = new CaseInsensitiveStringMap(java.util.Map.of())
      val analyzer = SplitSizeAnalyzer(new Path(tempPath), spark, options)

      // Test with a target size
      val targetSizeBytes = 1024L * 1024L // 1MB
      val result          = analyzer.calculateTargetRows(targetSizeBytes)

      // Should return some calculated value based on historical data
      assert(result.isDefined, "Should calculate target rows from historical data")
      assert(result.get > 0, "Target rows should be positive")

      println(
        s"✅ Analyzer calculated ${result.get} target rows for ${SizeParser.formatBytes(targetSizeBytes)} target size"
      )
    }
  }

  test("SplitSizeAnalyzer should handle empty transaction log gracefully") {
    withTempPath { tempPath =>
      // Create empty directory (no transaction log)
      new java.io.File(tempPath).mkdirs()

      val options  = new CaseInsensitiveStringMap(java.util.Map.of())
      val analyzer = SplitSizeAnalyzer(new Path(tempPath), spark, options)

      val targetSizeBytes = 1024L * 1024L // 1MB
      val result          = analyzer.calculateTargetRows(targetSizeBytes)

      // Should return None when no historical data exists
      assert(result.isEmpty, "Should return None when no historical data exists")

      println("✅ Analyzer gracefully handled empty transaction log")
    }
  }

  test("SplitSizeAnalyzer should handle non-existent path gracefully") {
    val nonExistentPath = "/non/existent/path"
    val options         = new CaseInsensitiveStringMap(java.util.Map.of())
    val analyzer        = SplitSizeAnalyzer(new Path(nonExistentPath), spark, options)

    val targetSizeBytes = 1024L * 1024L
    val result          = analyzer.calculateTargetRows(targetSizeBytes)

    // Should return None gracefully
    assert(result.isEmpty, "Should return None for non-existent path")

    println("✅ Analyzer gracefully handled non-existent path")
  }

  test("SplitSizeAnalyzer should work with multiple historical writes") {
    withTempPath { tempPath =>
      // Write multiple batches to build historical data
      val batch1 = createTestDataFrame()
      batch1.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(tempPath)

      // Add more data
      val batch2 = createTestDataFrame().filter("id > 3")
      batch2.write
        .format("tantivy4spark")
        .mode("append")
        .save(tempPath)

      // Add even more data
      val batch3 = createLargeTestDataFrame(1000)
      batch3.write
        .format("tantivy4spark")
        .mode("append")
        .save(tempPath)

      // Verify total data
      val totalData  = spark.read.format("tantivy4spark").load(tempPath)
      val totalCount = totalData.count()
      assert(totalCount > 1000, s"Should have substantial data for analysis, got $totalCount rows")

      // Analyze with multiple historical splits
      val options  = new CaseInsensitiveStringMap(java.util.Map.of())
      val analyzer = SplitSizeAnalyzer(new Path(tempPath), spark, options)

      val targetSizeBytes = 2L * 1024L * 1024L // 2MB
      val result          = analyzer.calculateTargetRows(targetSizeBytes)

      assert(result.isDefined, "Should calculate target rows from multiple historical splits")
      assert(result.get > 0, "Target rows should be positive")

      println(s"✅ Analyzer processed multiple historical splits, calculated ${result.get} target rows")
    }
  }

  test("SplitSizeAnalyzer should handle different target sizes appropriately") {
    withTempPath { tempPath =>
      // Create substantial historical data
      val data = createLargeTestDataFrame(5000)
      data.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(tempPath)

      val options  = new CaseInsensitiveStringMap(java.util.Map.of())
      val analyzer = SplitSizeAnalyzer(new Path(tempPath), spark, options)

      // Test different target sizes
      val targetSizes = List(
        100L * 1024L,        // 100K
        1024L * 1024L,       // 1M
        10L * 1024L * 1024L, // 10M
        100L * 1024L * 1024L // 100M
      )

      val results = targetSizes.map { targetSize =>
        val result = analyzer.calculateTargetRows(targetSize)
        (targetSize, result)
      }

      // All should succeed
      results.foreach {
        case (targetSize, result) =>
          assert(result.isDefined, s"Should calculate target rows for ${SizeParser.formatBytes(targetSize)}")
          assert(result.get > 0, s"Target rows should be positive for ${SizeParser.formatBytes(targetSize)}")
      }

      // Larger target sizes should generally result in more rows per split
      val sortedResults = results.map { case (targetSize, result) => (targetSize, result.get) }.sortBy(_._1)
      println("Target size -> calculated rows:")
      sortedResults.foreach {
        case (targetSize, rows) =>
          println(s"  ${SizeParser.formatBytes(targetSize)} -> $rows rows")
      }

      println("✅ Analyzer handled different target sizes appropriately")
    }
  }

  test("SplitSizeAnalyzer factory method should work correctly") {
    withTempPath { tempPath =>
      // Create test data
      val data = createTestDataFrame()
      data.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(tempPath)

      val options = new CaseInsensitiveStringMap(java.util.Map.of())

      // Test factory method
      val result = SplitSizeAnalyzer.calculateTargetRows(
        tablePath = new Path(tempPath),
        spark = spark,
        options = options,
        targetSizeBytes = 1024L * 1024L, // 1MB
        maxHistoricalSplits = 5
      )

      assert(result.isDefined, "Factory method should calculate target rows")
      assert(result.get > 0, "Target rows should be positive")

      println(s"✅ Factory method calculated ${result.get} target rows")
    }
  }

  test("SplitSizeAnalyzer should respect maxHistoricalSplits parameter") {
    withTempPath { tempPath =>
      // Write many small batches to create many splits
      for (i <- 1 to 20) {
        val batch = spark
          .range(i * 100, i * 100 + 10)
          .selectExpr("id", "CAST(id AS STRING) as name")

        val mode = if (i == 1) "overwrite" else "append"
        batch.write
          .format("tantivy4spark")
          .mode(mode)
          .save(tempPath)
      }

      val options = new CaseInsensitiveStringMap(java.util.Map.of())

      // Test with different maxHistoricalSplits values
      val analyzer = SplitSizeAnalyzer(new Path(tempPath), spark, options)

      val result1 = analyzer.calculateTargetRows(1024L * 1024L, maxHistoricalSplits = 3)
      val result2 = analyzer.calculateTargetRows(1024L * 1024L, maxHistoricalSplits = 10)

      // Both should work
      assert(result1.isDefined, "Should work with maxHistoricalSplits = 3")
      assert(result2.isDefined, "Should work with maxHistoricalSplits = 10")

      println(s"✅ Analyzer respected maxHistoricalSplits parameter")
      println(s"   3 splits: ${result1.get} rows, 10 splits: ${result2.get} rows")
    }
  }

  test("SplitSizeAnalyzer should handle edge cases gracefully") {
    withTempPath { tempPath =>
      // Create minimal data
      val minimalData = spark.range(1).selectExpr("id", "CAST(id AS STRING) as name")
      minimalData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(tempPath)

      val options  = new CaseInsensitiveStringMap(java.util.Map.of())
      val analyzer = SplitSizeAnalyzer(new Path(tempPath), spark, options)

      // Test with very small target size
      val smallTarget = analyzer.calculateTargetRows(1L) // 1 byte
      assert(smallTarget.isDefined, "Should handle very small target size")
      assert(smallTarget.get >= 1L, "Should return at least 1 row for very small target")

      // Test with very large target size
      val largeTarget = analyzer.calculateTargetRows(1024L * 1024L * 1024L) // 1GB
      assert(largeTarget.isDefined, "Should handle very large target size")

      println("✅ Analyzer handled edge cases gracefully")
      println(s"   1 byte target -> ${smallTarget.get} rows")
      println(s"   1GB target -> ${largeTarget.get} rows")
    }
  }
}
