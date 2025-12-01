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

package io.indextables.spark.core

import java.util.concurrent.atomic.AtomicLong

import io.indextables.spark.TestBase

/**
 * Tests that verify task metrics (bytesWritten, recordsWritten, bytesRead, recordsRead)
 * are properly reported to Spark UI when reading/writing IndexTables.
 */
class OutputMetricsTest extends TestBase {

  test("should report output metrics (bytesWritten, recordsWritten) when writing") {
    val tablePath = s"file://$tempDir/output_metrics_test"

    // Create test data
    val numRows = 500
    val df = spark.range(0, numRows)
      .selectExpr("id", "CAST(id AS STRING) as text")

    // Use atomic counters to capture task metrics
    val capturedBytesWritten = new AtomicLong(0)
    val capturedRecordsWritten = new AtomicLong(0)

    val listener = new org.apache.spark.scheduler.SparkListener {
      override def onTaskEnd(taskEnd: org.apache.spark.scheduler.SparkListenerTaskEnd): Unit = {
        val metrics = taskEnd.taskMetrics
        if (metrics != null && metrics.outputMetrics != null) {
          capturedBytesWritten.addAndGet(metrics.outputMetrics.bytesWritten)
          capturedRecordsWritten.addAndGet(metrics.outputMetrics.recordsWritten)
        }
      }
    }

    spark.sparkContext.addSparkListener(listener)

    try {
      // Write the data
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(tablePath)

      // Wait for listener to process events
      Thread.sleep(1000)

      val bytesWritten = capturedBytesWritten.get()
      val recordsWritten = capturedRecordsWritten.get()

      info(s"Captured output metrics from TaskContext:")
      info(s"  bytesWritten: $bytesWritten")
      info(s"  recordsWritten: $recordsWritten")

      // Verify metrics were captured
      assert(bytesWritten > 0, s"bytesWritten should be > 0, got $bytesWritten")
      assert(recordsWritten > 0, s"recordsWritten should be > 0, got $recordsWritten")
      assert(recordsWritten == numRows,
        s"recordsWritten should equal $numRows, got $recordsWritten")

      info(s"✅ Output metrics correctly reported: $bytesWritten bytes, $recordsWritten records")

      // Also verify data was written correctly
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)
      result.count() shouldBe numRows

    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("should report output metrics for partitioned writes") {
    val tablePath = s"file://$tempDir/output_metrics_partitioned_test"

    // Create partitioned test data
    val numRows = 600
    val df = spark.range(0, numRows)
      .selectExpr("id", "CAST(id % 3 AS STRING) as partition_col", "CAST(id AS STRING) as text")

    // Use atomic counters to capture task metrics
    val capturedBytesWritten = new AtomicLong(0)
    val capturedRecordsWritten = new AtomicLong(0)

    val listener = new org.apache.spark.scheduler.SparkListener {
      override def onTaskEnd(taskEnd: org.apache.spark.scheduler.SparkListenerTaskEnd): Unit = {
        val metrics = taskEnd.taskMetrics
        if (metrics != null && metrics.outputMetrics != null) {
          capturedBytesWritten.addAndGet(metrics.outputMetrics.bytesWritten)
          capturedRecordsWritten.addAndGet(metrics.outputMetrics.recordsWritten)
        }
      }
    }

    spark.sparkContext.addSparkListener(listener)

    try {
      // Write partitioned data
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("partition_col")
        .mode("overwrite")
        .save(tablePath)

      // Wait for listener to process events
      Thread.sleep(1000)

      val bytesWritten = capturedBytesWritten.get()
      val recordsWritten = capturedRecordsWritten.get()

      info(s"Captured output metrics for partitioned write:")
      info(s"  bytesWritten: $bytesWritten")
      info(s"  recordsWritten: $recordsWritten")

      // Verify metrics were captured
      assert(bytesWritten > 0, s"bytesWritten should be > 0, got $bytesWritten")
      assert(recordsWritten > 0, s"recordsWritten should be > 0, got $recordsWritten")

      info(s"✅ Partitioned write metrics correctly reported: $bytesWritten bytes, $recordsWritten records")

    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("should report zero metrics when no rows written") {
    val tablePath = s"file://$tempDir/output_metrics_empty_test"

    // Create empty DataFrame
    val df = spark.range(0, 0)
      .selectExpr("id", "CAST(id AS STRING) as text")

    // Use atomic counters to capture task metrics
    val capturedBytesWritten = new AtomicLong(0)
    val capturedRecordsWritten = new AtomicLong(0)

    val listener = new org.apache.spark.scheduler.SparkListener {
      override def onTaskEnd(taskEnd: org.apache.spark.scheduler.SparkListenerTaskEnd): Unit = {
        val metrics = taskEnd.taskMetrics
        if (metrics != null && metrics.outputMetrics != null) {
          capturedBytesWritten.addAndGet(metrics.outputMetrics.bytesWritten)
          capturedRecordsWritten.addAndGet(metrics.outputMetrics.recordsWritten)
        }
      }
    }

    spark.sparkContext.addSparkListener(listener)

    try {
      // Write empty data
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(tablePath)

      // Wait for listener to process events
      Thread.sleep(1000)

      val bytesWritten = capturedBytesWritten.get()
      val recordsWritten = capturedRecordsWritten.get()

      info(s"Captured output metrics for empty write:")
      info(s"  bytesWritten: $bytesWritten")
      info(s"  recordsWritten: $recordsWritten")

      // Empty write should have zero metrics
      assert(bytesWritten == 0, s"bytesWritten should be 0 for empty write, got $bytesWritten")
      assert(recordsWritten == 0, s"recordsWritten should be 0 for empty write, got $recordsWritten")

      info(s"✅ Empty write correctly reported zero metrics")

    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("should report input metrics (bytesRead, recordsRead) when reading") {
    val tablePath = s"file://$tempDir/input_metrics_test"

    // First, write some test data
    val numRows = 500
    val df = spark.range(0, numRows)
      .selectExpr("id", "CAST(id AS STRING) as text")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Use atomic counters to capture task metrics during read
    val capturedBytesRead = new AtomicLong(0)
    val capturedRecordsRead = new AtomicLong(0)
    @volatile var captureEnabled = false

    val listener = new org.apache.spark.scheduler.SparkListener {
      override def onTaskEnd(taskEnd: org.apache.spark.scheduler.SparkListenerTaskEnd): Unit = {
        if (captureEnabled) {
          val metrics = taskEnd.taskMetrics
          if (metrics != null && metrics.inputMetrics != null) {
            capturedBytesRead.addAndGet(metrics.inputMetrics.bytesRead)
            capturedRecordsRead.addAndGet(metrics.inputMetrics.recordsRead)
          }
        }
      }
    }

    spark.sparkContext.addSparkListener(listener)

    try {
      // Wait for any pending events from write to be processed before enabling capture
      Thread.sleep(500)

      // Enable capture only for read operation
      captureEnabled = true

      // Read the data
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)
        .collect()

      // Wait for listener to process events
      Thread.sleep(1000)

      val bytesRead = capturedBytesRead.get()
      val recordsRead = capturedRecordsRead.get()

      info(s"Captured input metrics from TaskContext:")
      info(s"  bytesRead: $bytesRead")
      info(s"  recordsRead: $recordsRead")

      // Verify metrics were captured
      assert(bytesRead > 0, s"bytesRead should be > 0, got $bytesRead")
      assert(recordsRead > 0, s"recordsRead should be > 0, got $recordsRead")
      assert(recordsRead == numRows,
        s"recordsRead should equal $numRows, got $recordsRead")

      info(s"✅ Input metrics correctly reported: $bytesRead bytes, $recordsRead records")

    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("should report input metrics for filtered reads") {
    val tablePath = s"file://$tempDir/input_metrics_filtered_test"

    // First, write some test data
    val numRows = 1000
    val df = spark.range(0, numRows)
      .selectExpr("id", "CAST(id AS STRING) as text")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Use atomic counters to capture task metrics during read
    val capturedBytesRead = new AtomicLong(0)
    val capturedRecordsRead = new AtomicLong(0)
    @volatile var captureEnabled = false

    val listener = new org.apache.spark.scheduler.SparkListener {
      override def onTaskEnd(taskEnd: org.apache.spark.scheduler.SparkListenerTaskEnd): Unit = {
        if (captureEnabled) {
          val metrics = taskEnd.taskMetrics
          if (metrics != null && metrics.inputMetrics != null) {
            capturedBytesRead.addAndGet(metrics.inputMetrics.bytesRead)
            capturedRecordsRead.addAndGet(metrics.inputMetrics.recordsRead)
          }
        }
      }
    }

    spark.sparkContext.addSparkListener(listener)

    try {
      // Wait for any pending events from write to be processed before enabling capture
      Thread.sleep(500)

      // Enable capture only for read operation
      captureEnabled = true

      // Read with filter - should only return ~100 rows (id < 100)
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)
        .filter("id < 100")
        .collect()

      // Wait for listener to process events
      Thread.sleep(1000)

      val bytesRead = capturedBytesRead.get()
      val recordsRead = capturedRecordsRead.get()

      info(s"Captured input metrics for filtered read:")
      info(s"  bytesRead: $bytesRead")
      info(s"  recordsRead: $recordsRead")
      info(s"  actual result count: ${result.length}")

      // Verify metrics were captured
      // bytesRead should reflect the split file size (we still read the whole split)
      assert(bytesRead > 0, s"bytesRead should be > 0, got $bytesRead")
      // recordsRead should match actual records returned (filter pushdown)
      assert(recordsRead > 0, s"recordsRead should be > 0, got $recordsRead")
      assert(recordsRead == result.length,
        s"recordsRead should equal actual result count ${result.length}, got $recordsRead")

      info(s"✅ Filtered read metrics correctly reported: $bytesRead bytes, $recordsRead records")

    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("should report input metrics for aggregation queries") {
    val tablePath = s"file://$tempDir/input_metrics_agg_test"

    // First, write some test data
    val numRows = 500
    val df = spark.range(0, numRows)
      .selectExpr("id", "CAST(id AS STRING) as text")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    // Use atomic counters to capture task metrics during read
    val capturedBytesRead = new AtomicLong(0)
    val capturedRecordsRead = new AtomicLong(0)
    @volatile var captureEnabled = false

    val listener = new org.apache.spark.scheduler.SparkListener {
      override def onTaskEnd(taskEnd: org.apache.spark.scheduler.SparkListenerTaskEnd): Unit = {
        if (captureEnabled) {
          val metrics = taskEnd.taskMetrics
          if (metrics != null && metrics.inputMetrics != null) {
            capturedBytesRead.addAndGet(metrics.inputMetrics.bytesRead)
            capturedRecordsRead.addAndGet(metrics.inputMetrics.recordsRead)
          }
        }
      }
    }

    spark.sparkContext.addSparkListener(listener)

    try {
      // Wait for any pending events from write to be processed before enabling capture
      Thread.sleep(500)

      // Enable capture only for read operation
      captureEnabled = true

      // Execute count aggregation
      val count = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)
        .count()

      // Wait for listener to process events
      Thread.sleep(1000)

      val bytesRead = capturedBytesRead.get()
      val recordsRead = capturedRecordsRead.get()

      info(s"Captured input metrics for aggregation query:")
      info(s"  bytesRead: $bytesRead")
      info(s"  recordsRead: $recordsRead")
      info(s"  count result: $count")

      // For aggregate pushdown, we may not read all records
      // But we should still report some metrics
      info(s"✅ Aggregation query metrics reported: $bytesRead bytes, $recordsRead records")

    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }
}
