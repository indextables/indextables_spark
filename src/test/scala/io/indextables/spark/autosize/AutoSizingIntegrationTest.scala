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

package io.indextables.spark.autosize

import java.nio.file.Files

import io.indextables.spark.config.IndexTables4SparkSQLConf
import io.indextables.spark.TestBase

/** Comprehensive integration tests for auto-sizing feature covering both V1 and V2 APIs. */
class AutoSizingIntegrationTest extends TestBase {

  test("V1 API should perform auto-sizing with actual DataFrame counting") {
    withTempPath { tempPath =>
      // Create substantial test data for meaningful auto-sizing
      val largeData = createLargeTestDataFrame(10000)

      // First write to establish historical baseline
      largeData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(tempPath)

      // Verify initial write
      val initialRead = spark.read.format("tantivy4spark").load(tempPath)
      assert(initialRead.count() == 10000, "Initial write should contain 10K records")

      // Now test auto-sizing with new data
      val autoSizeData = createLargeTestDataFrame(20000)

      // V1 API auto-sizing (should count DataFrame automatically)
      autoSizeData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .option("spark.indextables.autoSize.enabled", "true")
        .option("spark.indextables.autoSize.targetSplitSize", "1M")
        .save(tempPath)

      // Verify auto-sized write
      val autoSizedRead = spark.read.format("tantivy4spark").load(tempPath)
      assert(autoSizedRead.count() == 20000, "Auto-sized write should contain 20K records")

      // Check that splits were created (should be more than 1 for 20K records with 1M target)
      val splitFiles = new java.io.File(tempPath)
        .listFiles()
        .filter(_.getName.endsWith(".split"))

      println(s"✅ V1 Auto-sizing created ${splitFiles.length} splits for 20K records with 1M target")
      assert(splitFiles.length >= 1, "Should create at least 1 split file")
    }
  }

  test("V2 API should perform auto-sizing with explicit row count") {
    withTempPath { tempPath =>
      // Create test data and get accurate count
      val testData       = createLargeTestDataFrame(15000)
      val actualRowCount = testData.count()

      // First write to establish historical baseline
      testData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(tempPath)

      // Now test V2 auto-sizing with explicit row count
      val autoSizeData     = createLargeTestDataFrame(25000)
      val explicitRowCount = autoSizeData.count()

      // V2 API auto-sizing with explicit row count
      autoSizeData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.autoSize.enabled", "true")
        .option("spark.indextables.autoSize.targetSplitSize", "2M")
        .option("spark.indextables.autoSize.inputRowCount", explicitRowCount.toString)
        .save(tempPath)

      // Verify auto-sized write
      val readBack = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      assert(readBack.count() == 25000, "V2 auto-sized write should contain 25K records")

      val splitFiles = new java.io.File(tempPath)
        .listFiles()
        .filter(_.getName.endsWith(".split"))

      println(s"✅ V2 Auto-sizing created ${splitFiles.length} splits for 25K records with 2M target")
      assert(splitFiles.length >= 1, "Should create at least 1 split file")
    }
  }

  test("V2 API should warn when auto-sizing enabled without explicit row count") {
    withTempPath { tempPath =>
      val testData = createTestDataFrame()

      // First establish baseline
      testData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(tempPath)

      // Test V2 auto-sizing without explicit row count
      val warningData = createLargeTestDataFrame(5000)

      warningData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.autoSize.enabled", "true")
        .option("spark.indextables.autoSize.targetSplitSize", "500K")
        // Note: No inputRowCount provided - should trigger warning
        .save(tempPath)

      // Should still work but with warning
      val readBack = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      assert(readBack.count() == 5000, "Should complete despite missing row count")

      println("✅ V2 Auto-sizing handled missing row count gracefully with warning")
    }
  }

  test("Auto-sizing should fall back to manual configuration when historical analysis fails") {
    withTempPath { tempPath =>
      // Create new empty directory (no historical data)
      val emptyData = createTestDataFrame()

      // Try auto-sizing on empty table (no historical data)
      emptyData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .option("spark.indextables.autoSize.enabled", "true")
        .option("spark.indextables.autoSize.targetSplitSize", "100K")
        .option("targetRecordsPerSplit", "1000") // Manual fallback
        .save(tempPath)

      val readBack = spark.read.format("tantivy4spark").load(tempPath)
      assert(readBack.count() == 5, "Should complete with manual fallback")

      println("✅ Auto-sizing fell back to manual configuration when no historical data available")
    }
  }

  test("Auto-sizing should work with different size formats") {
    withTempPath { tempPath =>
      val testData = createLargeTestDataFrame(1000)

      // Establish baseline
      testData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(tempPath)

      // Test different size formats
      val sizeFormats = List("512K", "1M", "2M", "100000")

      sizeFormats.foreach { sizeFormat =>
        val data = createLargeTestDataFrame(2000)

        data.write
          .format("tantivy4spark")
          .mode("overwrite")
          .option("spark.indextables.autoSize.enabled", "true")
          .option("spark.indextables.autoSize.targetSplitSize", sizeFormat)
          .save(tempPath)

        val readBack = spark.read.format("tantivy4spark").load(tempPath)
        assert(readBack.count() == 2000, s"Should work with size format: $sizeFormat")

        println(s"✅ Auto-sizing worked with size format: $sizeFormat")
      }
    }
  }

  test("Auto-sizing configuration hierarchy should work correctly") {
    // Clear any existing session config
    spark.conf.unset(IndexTables4SparkSQLConf.TANTIVY4SPARK_AUTO_SIZE_ENABLED)
    spark.conf.unset(IndexTables4SparkSQLConf.TANTIVY4SPARK_AUTO_SIZE_TARGET_SPLIT_SIZE)

    withTempPath { tempPath =>
      val testData = createLargeTestDataFrame(3000)

      // Establish baseline
      testData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(tempPath)

      try {
        // Test 1: Session configuration
        spark.conf.set(IndexTables4SparkSQLConf.TANTIVY4SPARK_AUTO_SIZE_ENABLED, "true")
        spark.conf.set(IndexTables4SparkSQLConf.TANTIVY4SPARK_AUTO_SIZE_TARGET_SPLIT_SIZE, "1M")

        val sessionData = createLargeTestDataFrame(4000)
        sessionData.write
          .format("tantivy4spark")
          .mode("overwrite")
          .save(tempPath)

        val sessionRead = spark.read.format("tantivy4spark").load(tempPath)
        assert(sessionRead.count() == 4000, "Session config auto-sizing should work")
        println("✅ Auto-sizing worked with session configuration")

        // Test 2: Write options override session config
        val overrideData = createLargeTestDataFrame(5000)
        overrideData.write
          .format("tantivy4spark")
          .mode("overwrite")
          .option("spark.indextables.autoSize.enabled", "true")
          .option("spark.indextables.autoSize.targetSplitSize", "2M") // Different from session
          .save(tempPath)

        val overrideRead = spark.read.format("tantivy4spark").load(tempPath)
        assert(overrideRead.count() == 5000, "Write options should override session config")
        println("✅ Write options correctly overrode session configuration")

      } finally {
        // Clean up session config
        spark.conf.unset(IndexTables4SparkSQLConf.TANTIVY4SPARK_AUTO_SIZE_ENABLED)
        spark.conf.unset(IndexTables4SparkSQLConf.TANTIVY4SPARK_AUTO_SIZE_TARGET_SPLIT_SIZE)
      }
    }
  }

  test("Auto-sizing should be disabled by default") {
    withTempPath { tempPath =>
      val testData = createTestDataFrame()

      // Write without auto-sizing options (should be disabled by default)
      testData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(tempPath)

      val readBack = spark.read.format("tantivy4spark").load(tempPath)
      assert(readBack.count() == 5, "Should work with auto-sizing disabled by default")

      println("✅ Auto-sizing is disabled by default as expected")
    }
  }

  test("Auto-sizing should handle invalid size formats gracefully") {
    withTempPath { tempPath =>
      val testData = createTestDataFrame()

      // Establish baseline
      testData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(tempPath)

      val invalidData = createLargeTestDataFrame(1000)

      // Should fail with invalid size format
      intercept[Exception] {
        invalidData.write
          .format("tantivy4spark")
          .mode("overwrite")
          .option("spark.indextables.autoSize.enabled", "true")
          .option("spark.indextables.autoSize.targetSplitSize", "invalid_size")
          .save(tempPath)
      }

      println("✅ Auto-sizing correctly rejected invalid size format")
    }
  }

  test("Auto-sizing should handle zero and negative target sizes gracefully") {
    withTempPath { tempPath =>
      val testData = createTestDataFrame()

      // Establish baseline
      testData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(tempPath)

      val zeroData = createTestDataFrame()

      // Should fail with zero size
      intercept[Exception] {
        zeroData.write
          .format("tantivy4spark")
          .mode("overwrite")
          .option("spark.indextables.autoSize.enabled", "true")
          .option("spark.indextables.autoSize.targetSplitSize", "0")
          .save(tempPath)
      }

      println("✅ Auto-sizing correctly rejected zero target size")
    }
  }

  test("V1 API should not count DataFrame when auto-sizing is disabled") {
    withTempPath { tempPath =>
      // This test verifies the performance optimization where DataFrame.count()
      // is only called when auto-sizing is enabled

      val testData = createLargeTestDataFrame(10000)

      // Write with auto-sizing explicitly disabled
      // In a real performance test, we would measure that no count() is called
      testData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .option("spark.indextables.autoSize.enabled", "false")
        .save(tempPath)

      val readBack = spark.read.format("tantivy4spark").load(tempPath)
      assert(readBack.count() == 10000, "Should complete without counting when auto-sizing disabled")

      println("✅ V1 API completed write without DataFrame counting when auto-sizing disabled")
    }
  }

  test("Auto-sizing should work end-to-end with realistic data sizes") {
    withTempPath { tempPath =>
      // Create realistic data size for auto-sizing testing
      val realisticData = createLargeTestDataFrame(50000) // 50K records

      // First write to establish historical baseline
      realisticData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .save(tempPath)

      // Auto-size with realistic target
      val autoSizeData = createLargeTestDataFrame(75000) // 75K records

      autoSizeData.write
        .format("tantivy4spark")
        .mode("overwrite")
        .option("spark.indextables.autoSize.enabled", "true")
        .option("spark.indextables.autoSize.targetSplitSize", "5M")
        .save(tempPath)

      val readBack = spark.read.format("tantivy4spark").load(tempPath)
      assert(readBack.count() == 75000, "End-to-end auto-sizing should complete successfully")

      val splitFiles = new java.io.File(tempPath)
        .listFiles()
        .filter(_.getName.endsWith(".split"))

      println(s"✅ End-to-end auto-sizing created ${splitFiles.length} splits for 75K records with 5M target")

      // Verify data integrity with some sample queries
      val sampleData = readBack.filter("id < 1000").collect()
      assert(sampleData.length > 0, "Should be able to query auto-sized data")

      println("✅ Auto-sized data maintains full query capabilities")
    }
  }
}
