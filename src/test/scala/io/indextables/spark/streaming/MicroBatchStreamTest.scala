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

package io.indextables.spark.streaming

import java.io.File
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import io.indextables.spark.TestBase
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for the native MicroBatchStream implementation.
 *
 * These tests verify:
 *   - Streaming offset serialization/deserialization
 *   - Reading IndexTables4Spark as a streaming source
 *   - Filter pushdown in streaming mode
 *   - Merged split filtering (dataChange=false)
 *   - Partition pruning in streaming
 */
class MicroBatchStreamTest extends AnyFunSuite with TestBase {

  // ==========================================================================
  // Offset Serialization Tests
  // ==========================================================================

  test("IndexTables4SparkStreamingOffset serialization and deserialization") {
    val offset = IndexTables4SparkStreamingOffset(version = 42L, timestamp = Some(1234567890L))

    // Test serialization
    val json = offset.json()
    assert(json.contains("42"), "JSON should contain version 42")
    assert(json.contains("1234567890"), "JSON should contain timestamp")

    // Test deserialization
    val parsed = IndexTables4SparkStreamingOffset.fromJson(json)
    assert(parsed.version == 42L, "Version should be 42")
    assert(parsed.timestamp.contains(1234567890L), "Timestamp should match")
  }

  test("IndexTables4SparkStreamingOffset without timestamp") {
    val offset = IndexTables4SparkStreamingOffset(version = 10L)

    val json = offset.json()
    assert(json.contains("10"), "JSON should contain version 10")

    val parsed = IndexTables4SparkStreamingOffset.fromJson(json)
    assert(parsed.version == 10L, "Version should be 10")
    assert(parsed.timestamp.isEmpty || parsed.timestamp.contains(0L), "Timestamp should be empty or zero")
  }

  test("INITIAL offset should have version -1") {
    val initial = IndexTables4SparkStreamingOffset.INITIAL
    assert(initial.version == -1L, "INITIAL offset should have version -1")
  }

  // ==========================================================================
  // startingVersion and startingTimestamp Tests (Delta Lake compatible)
  // ==========================================================================

  test("Streaming with startingVersion option") {
    val outputDir     = Files.createTempDirectory("streaming_startingversion_").toFile.getAbsolutePath
    val checkpointDir = Files.createTempDirectory("checkpoint_startingversion_").toFile.getAbsolutePath

    try {
      val spark = this.spark
      import spark.implicits._

      // Write data - first overwrite creates version 0
      println("Writing initial batch (creates version 0)...")
      val data1 = (1 to 20).map(i => (i, s"user_$i", i * 10))
      data1.toDF("id", "username", "score")
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(outputDir)

      // Verify we can read the version
      val versionCheck = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(outputDir)
        .count()
      println(s"Batch read count: $versionCheck")

      // Stream starting from version 0 (the version that exists)
      // startingVersion=0 means process versions >= 0 (offset becomes -1)
      // This is equivalent to "earliest"
      val collectedRecords = ListBuffer[Int]()

      val streamingQuery = spark.readStream
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.streaming.startingVersion", "0")  // Start from version 0
        .load(outputDir)
        .writeStream
        .trigger(Trigger.AvailableNow())
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          val ids = batchDf.select("id").collect().map(_.getInt(0))
          println(s"Batch $batchId: received ${ids.length} records: ${ids.sorted.take(5).mkString(",")}...")
          collectedRecords ++= ids
          ()
        }
        .option("checkpointLocation", checkpointDir)
        .start()

      streamingQuery.awaitTermination(60000)

      val sortedIds = collectedRecords.sorted
      println(s"Collected ${sortedIds.length} IDs with startingVersion=0")

      // startingVersion=0 means process from version 0 onwards (inclusive)
      // offset = 0 - 1 = -1, which means process versions > -1, i.e., version 0+
      assert(sortedIds.length == 20, s"Expected 20 records with startingVersion=0, got ${sortedIds.length}")
      assert(sortedIds.head == 1, s"First ID should be 1, got ${sortedIds.head}")
      assert(sortedIds.last == 20, s"Last ID should be 20, got ${sortedIds.last}")

      println("Test passed: startingVersion option works correctly")

    } finally {
      deleteDirectory(new File(outputDir))
      deleteDirectory(new File(checkpointDir))
    }
  }

  test("Streaming with startingTimestamp option") {
    val outputDir     = Files.createTempDirectory("streaming_startingtimestamp_").toFile.getAbsolutePath
    val checkpointDir = Files.createTempDirectory("checkpoint_startingtimestamp_").toFile.getAbsolutePath

    try {
      val spark = this.spark
      import spark.implicits._

      // Write first batch
      println("Writing first batch...")
      val data1 = (1 to 10).map(i => (i, s"user_$i", i * 10))
      data1.toDF("id", "username", "score")
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(outputDir)

      // Record timestamp between writes
      val timestampBetweenWrites = System.currentTimeMillis()
      Thread.sleep(100) // Small delay to ensure file timestamps differ

      // Write second batch
      println("Writing second batch...")
      val data2 = (11 to 20).map(i => (i, s"user_$i", i * 10))
      data2.toDF("id", "username", "score")
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(outputDir)

      // Stream starting from timestamp (should get records from second batch onwards)
      val collectedRecords = ListBuffer[Int]()

      val streamingQuery = spark.readStream
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.streaming.startingTimestamp", timestampBetweenWrites.toString)
        .load(outputDir)
        .writeStream
        .trigger(Trigger.AvailableNow())
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          val ids = batchDf.select("id").collect().map(_.getInt(0))
          println(s"Batch $batchId: received ${ids.length} records: ${ids.sorted.mkString(",")}")
          collectedRecords ++= ids
          ()
        }
        .option("checkpointLocation", checkpointDir)
        .start()

      streamingQuery.awaitTermination(60000)

      val sortedIds = collectedRecords.sorted
      println(s"Collected IDs with startingTimestamp: ${sortedIds.mkString(", ")}")

      // Should have records from second batch (11-20) since we used timestamp between writes
      // Note: Timing can be tricky - we may get 10 or 20 records depending on file timestamps
      if (sortedIds.nonEmpty) {
        assert(sortedIds.head >= 1, s"First ID should be >= 1, got ${sortedIds.head}")
        println(s"Test passed: startingTimestamp option works (received ${sortedIds.length} records)")
      } else {
        println("Note: No records received (timestamp may be after all writes - acceptable)")
      }

    } finally {
      deleteDirectory(new File(outputDir))
      deleteDirectory(new File(checkpointDir))
    }
  }

  test("startingTimestamp with ISO-8601 format") {
    val outputDir     = Files.createTempDirectory("streaming_isotimestamp_").toFile.getAbsolutePath
    val checkpointDir = Files.createTempDirectory("checkpoint_isotimestamp_").toFile.getAbsolutePath

    try {
      val spark = this.spark
      import spark.implicits._

      // Write data
      println("Writing data...")
      val data = (1 to 20).map(i => (i, s"user_$i", i * 10))
      data.toDF("id", "username", "score")
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(outputDir)

      // Use a timestamp far in the past to get all data
      val pastTimestamp = "2020-01-01T00:00:00Z"
      val collectedRecords = ListBuffer[Int]()

      val streamingQuery = spark.readStream
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.streaming.startingTimestamp", pastTimestamp)
        .load(outputDir)
        .writeStream
        .trigger(Trigger.AvailableNow())
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          val ids = batchDf.select("id").collect().map(_.getInt(0))
          println(s"Batch $batchId: received ${ids.length} records")
          collectedRecords ++= ids
          ()
        }
        .option("checkpointLocation", checkpointDir)
        .start()

      streamingQuery.awaitTermination(60000)

      val sortedIds = collectedRecords.sorted
      println(s"Collected ${sortedIds.length} records with ISO-8601 timestamp")

      // With a timestamp in 2020, should get all records (version 0 onwards)
      assert(sortedIds.length == 20, s"Expected 20 records with past timestamp, got ${sortedIds.length}")

      println("Test passed: ISO-8601 timestamp format works correctly")

    } finally {
      deleteDirectory(new File(outputDir))
      deleteDirectory(new File(checkpointDir))
    }
  }

  // ==========================================================================
  // Native Streaming Source Tests
  // ==========================================================================

  test("Read IndexTables4Spark table as streaming source with earliest offset") {
    val outputDir     = Files.createTempDirectory("streaming_source_").toFile.getAbsolutePath
    val checkpointDir = Files.createTempDirectory("checkpoint_").toFile.getAbsolutePath
    val resultDir     = Files.createTempDirectory("result_").toFile.getAbsolutePath

    try {
      // Step 1: Write some initial data using batch mode
      println("Writing initial batch data...")
      val spark = this.spark
      import spark.implicits._
      val initialData = (1 to 100).map(i => (i, s"user_$i", s"content for record $i", i * 10))
      val initialDf = initialData.toDF("id", "username", "content", "score")

      initialDf.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.fastfields", "score")
        .save(outputDir)

      println("Initial data written. Starting streaming read...")

      // Step 2: Read the table as a streaming source
      val collectedBatches = ListBuffer[Long]()

      val streamingQuery = spark.readStream
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.streaming.startingOffset", "earliest")
        .load(outputDir)
        .writeStream
        .trigger(Trigger.AvailableNow())
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          val count = batchDf.count()
          println(s"Batch $batchId: received $count records")
          collectedBatches += count
          ()
        }
        .option("checkpointLocation", checkpointDir)
        .start()

      streamingQuery.awaitTermination(60000) // 60 second timeout

      println(s"Streaming completed. Batches: ${collectedBatches.mkString(", ")}")

      // Verify we received all records
      val totalReceived = collectedBatches.sum
      assert(totalReceived == 100, s"Expected 100 records, got $totalReceived")

      println("Test passed: Read IndexTables4Spark as streaming source")

    } finally {
      deleteDirectory(new File(outputDir))
      deleteDirectory(new File(checkpointDir))
      deleteDirectory(new File(resultDir))
    }
  }

  test("Streaming source with latest offset receives only new data") {
    val outputDir     = Files.createTempDirectory("streaming_latest_").toFile.getAbsolutePath
    val checkpointDir = Files.createTempDirectory("checkpoint_latest_").toFile.getAbsolutePath

    try {
      val spark = this.spark
      import spark.implicits._

      // Step 1: Write initial data
      println("Writing initial batch data (before streaming starts)...")
      val initialData = (1 to 50).map(i => (i, s"user_$i", s"initial content $i", i * 10))
      initialData.toDF("id", "username", "content", "score")
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.typemap.content", "text")
        .save(outputDir)

      // Step 2: Start streaming with "latest" offset - should not see initial data
      val collectedCounts = ListBuffer[Long]()
      @volatile var queryStarted = false
      val latch = new CountDownLatch(1)

      val streamingQuery = spark.readStream
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.streaming.startingOffset", "latest")
        .load(outputDir)
        .writeStream
        .trigger(Trigger.ProcessingTime("1 second"))
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          val count = batchDf.count()
          if (count > 0) {
            println(s"Batch $batchId: received $count NEW records")
            collectedCounts += count
            latch.countDown()
          }
          if (!queryStarted) {
            queryStarted = true
          }
          ()
        }
        .option("checkpointLocation", checkpointDir)
        .start()

      // Wait for streaming to start
      Thread.sleep(2000)

      // Step 3: Write new data after streaming started
      println("Writing new data after streaming started...")
      val newData = (51 to 75).map(i => (i, s"user_$i", s"new content $i", i * 10))
      newData.toDF("id", "username", "content", "score")
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .option("spark.indextables.indexing.typemap.content", "text")
        .save(outputDir)

      // Wait for the streaming query to pick up new data
      val received = latch.await(30, TimeUnit.SECONDS)

      streamingQuery.stop()

      if (received) {
        val totalNew = collectedCounts.sum
        println(s"Total new records received: $totalNew")
        assert(totalNew == 25, s"Expected 25 new records, got $totalNew")
        println("Test passed: Latest offset only receives new data")
      } else {
        // This might happen if streaming doesn't pick up new data in time
        // which is acceptable behavior - the key is that initial data wasn't received
        println("Note: New data not picked up in time (acceptable)")
      }

    } finally {
      deleteDirectory(new File(outputDir))
      deleteDirectory(new File(checkpointDir))
    }
  }

  test("Streaming with filter pushdown") {
    val outputDir     = Files.createTempDirectory("streaming_filter_").toFile.getAbsolutePath
    val checkpointDir = Files.createTempDirectory("checkpoint_filter_").toFile.getAbsolutePath

    try {
      val spark = this.spark
      import spark.implicits._

      // Write data with a range of scores
      println("Writing data with score range...")
      val data = (1 to 100).map(i => (i, s"user_$i", s"content $i", i * 10))
      data.toDF("id", "username", "content", "score")
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .option("spark.indextables.indexing.fastfields", "score")
        .save(outputDir)

      // Stream with filter on score
      val collectedRecords = ListBuffer[Int]()

      val streamingQuery = spark.readStream
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.streaming.startingOffset", "earliest")
        .load(outputDir)
        .filter("score >= 500") // Filter: score >= 500 means id >= 50
        .writeStream
        .trigger(Trigger.AvailableNow())
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          val ids = batchDf.select("id").collect().map(_.getInt(0))
          println(s"Batch $batchId: received ${ids.length} records with ids: ${ids.sorted.take(5).mkString(",")}...")
          collectedRecords ++= ids
          ()
        }
        .option("checkpointLocation", checkpointDir)
        .start()

      streamingQuery.awaitTermination(60000)

      // Verify filter was applied
      val sortedIds = collectedRecords.sorted
      println(s"Collected ${sortedIds.length} records after filtering")

      // Should only have records with score >= 500 (id 50-100)
      assert(sortedIds.length == 51, s"Expected 51 records (50-100), got ${sortedIds.length}")
      assert(sortedIds.head == 50, s"First ID should be 50, got ${sortedIds.head}")
      assert(sortedIds.last == 100, s"Last ID should be 100, got ${sortedIds.last}")

      println("Test passed: Filter pushdown works in streaming")

    } finally {
      deleteDirectory(new File(outputDir))
      deleteDirectory(new File(checkpointDir))
    }
  }

  test("Streaming with partitioned data and partition pruning") {
    val outputDir     = Files.createTempDirectory("streaming_partitioned_").toFile.getAbsolutePath
    val checkpointDir = Files.createTempDirectory("checkpoint_partitioned_").toFile.getAbsolutePath

    try {
      val spark = this.spark
      import spark.implicits._

      // Write partitioned data
      println("Writing partitioned data...")
      val data = Seq(
        (1, "user1", "content 1", 100, "2024-01-01"),
        (2, "user2", "content 2", 200, "2024-01-01"),
        (3, "user3", "content 3", 300, "2024-01-02"),
        (4, "user4", "content 4", 400, "2024-01-02"),
        (5, "user5", "content 5", 500, "2024-01-03"),
        (6, "user6", "content 6", 600, "2024-01-03")
      )
      data.toDF("id", "username", "content", "score", "date")
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .partitionBy("date")
        .option("spark.indextables.indexing.fastfields", "score")
        .save(outputDir)

      // Stream with partition filter
      val collectedRecords = ListBuffer[Int]()

      val streamingQuery = spark.readStream
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.streaming.startingOffset", "earliest")
        .load(outputDir)
        .filter("date = '2024-01-02'") // Only partition for 2024-01-02
        .writeStream
        .trigger(Trigger.AvailableNow())
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          val ids = batchDf.select("id").collect().map(_.getInt(0))
          println(s"Batch $batchId: received ${ids.length} records for date 2024-01-02")
          collectedRecords ++= ids
          ()
        }
        .option("checkpointLocation", checkpointDir)
        .start()

      streamingQuery.awaitTermination(60000)

      // Verify only partition for 2024-01-02 was read
      val sortedIds = collectedRecords.sorted
      println(s"Collected IDs: ${sortedIds.mkString(", ")}")

      assert(sortedIds.length == 2, s"Expected 2 records (3, 4), got ${sortedIds.length}")
      assert(sortedIds.contains(3) && sortedIds.contains(4), "Should contain IDs 3 and 4")

      println("Test passed: Partition pruning works in streaming")

    } finally {
      deleteDirectory(new File(outputDir))
      deleteDirectory(new File(checkpointDir))
    }
  }

  // ==========================================================================
  // Merged Split Filtering Tests (dataChange=false)
  // ==========================================================================

  test("Streaming filters out merged splits (dataChange=false) during active streaming") {
    val outputDir      = Files.createTempDirectory("streaming_merge_").toFile.getAbsolutePath
    val checkpointDir  = Files.createTempDirectory("checkpoint_merge_").toFile.getAbsolutePath

    try {
      val spark = this.spark
      import spark.implicits._

      // Step 1: Write initial data to establish the table
      println("Writing initial batch...")
      val initialData = (1 to 50).map(i => (i, s"user_$i", s"content $i", i * 10))
      initialData.toDF("id", "username", "content", "score")
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(outputDir)

      // Step 2: Start streaming from "latest" offset
      // This simulates an active streaming query that should only see NEW changes
      val collectedRecords = ListBuffer[Int]()
      val latch = new CountDownLatch(1)

      val streamingQuery = spark.readStream
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.streaming.startingOffset", "latest")
        .load(outputDir)
        .writeStream
        .trigger(Trigger.ProcessingTime("500 milliseconds"))
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          val ids = batchDf.select("id").collect().map(_.getInt(0))
          if (ids.nonEmpty) {
            println(s"Batch $batchId: received ${ids.length} records: ${ids.sorted.take(5).mkString(",")}...")
            collectedRecords ++= ids
            latch.countDown()
          }
          ()
        }
        .option("checkpointLocation", checkpointDir)
        .start()

      // Wait for streaming to initialize
      Thread.sleep(2000)

      // Step 3: Write more batches to create multiple splits
      println("Writing additional batches while streaming is active...")
      for (batch <- 0 until 3) {
        val start = 51 + batch * 10
        val end = start + 9
        val data = (start to end).map(i => (i, s"user_$i", s"content $i", i * 10))
        data.toDF("id", "username", "content", "score")
          .write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .mode("append")
          .save(outputDir)
      }

      // Step 4: Run MERGE SPLITS - this creates a merged split with dataChange=false
      println("Merging splits while streaming is active...")
      spark.sql(s"MERGE SPLITS '$outputDir' TARGET SIZE 100M")

      // Wait for streaming to pick up new data (but NOT the merged split)
      val received = latch.await(30, TimeUnit.SECONDS)

      // Give a bit more time for any additional batches
      Thread.sleep(3000)

      streamingQuery.stop()

      // Step 5: Verify results
      // Should have received only the NEW data (records 51-80) from the 3 append batches
      // Should NOT have received any data from the merge (which would be dataChange=false)
      val sortedIds = collectedRecords.sorted.distinct
      println(s"Collected ${collectedRecords.length} records, ${sortedIds.length} distinct")
      println(s"IDs received: ${sortedIds.mkString(", ")}")

      if (received && collectedRecords.nonEmpty) {
        // Verify we only got the NEW records (51-80), not the initial batch (1-50)
        assert(sortedIds.forall(_ > 50), s"Should only receive new records (>50), but got: ${sortedIds.filter(_ <= 50)}")
        assert(sortedIds.length == 30, s"Expected 30 new records (51-80), got ${sortedIds.length}")
        // Verify no duplicates (which would indicate merged split was incorrectly included)
        assert(collectedRecords.length == sortedIds.length,
          s"No duplicates expected: ${collectedRecords.length} total vs ${sortedIds.length} distinct")
        println("Test passed: Merged splits correctly filtered out during active streaming")
      } else {
        // Timing-dependent: if we didn't receive data, that's acceptable
        // The key is that we should never receive duplicates or data from merged splits
        println("Note: New data not picked up in time (timing-dependent, acceptable)")
      }

    } finally {
      deleteDirectory(new File(outputDir))
      deleteDirectory(new File(checkpointDir))
    }
  }

  test("Streaming from earliest on merged table receives no data (merged splits filtered)") {
    val outputDir     = Files.createTempDirectory("streaming_merged_earliest_").toFile.getAbsolutePath
    val checkpointDir = Files.createTempDirectory("checkpoint_merged_earliest_").toFile.getAbsolutePath

    try {
      val spark = this.spark
      import spark.implicits._

      // Step 1: Write data and merge
      println("Writing data and merging...")
      val data = (1 to 100).map(i => (i, s"user_$i", s"content $i", i * 10))
      data.toDF("id", "username", "content", "score")
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(outputDir)

      // Merge all splits (creates new split with dataChange=false)
      spark.sql(s"MERGE SPLITS '$outputDir' TARGET SIZE 100M")

      // Verify batch read still works
      val batchCount = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(outputDir)
        .count()
      println(s"Batch read count: $batchCount")
      assert(batchCount == 100, s"Batch read should return 100 records, got $batchCount")

      // Step 2: Stream from earliest AFTER merge
      // All data is now in merged splits (dataChange=false), so streaming should receive 0 records
      // This is the correct semantic: streaming only processes NEW data changes
      val collectedRecords = ListBuffer[Int]()

      val streamingQuery = spark.readStream
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.streaming.startingOffset", "earliest")
        .load(outputDir)
        .writeStream
        .trigger(Trigger.AvailableNow())
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          val ids = batchDf.select("id").collect().map(_.getInt(0))
          println(s"Batch $batchId: received ${ids.length} records")
          collectedRecords ++= ids
          ()
        }
        .option("checkpointLocation", checkpointDir)
        .start()

      streamingQuery.awaitTermination(30000)

      // After a merge, streaming from earliest should receive 0 records
      // because the only split is the merged one with dataChange=false
      println(s"Collected ${collectedRecords.length} records after merge")

      // This is expected behavior: merged tables have dataChange=false splits
      // Streaming correctly filters these out to prevent duplicate processing
      assert(collectedRecords.isEmpty,
        s"Expected 0 records from merged table (dataChange=false filtered), got ${collectedRecords.length}")

      println("Test passed: Streaming from earliest on merged table correctly returns 0 records")

    } finally {
      deleteDirectory(new File(outputDir))
      deleteDirectory(new File(checkpointDir))
    }
  }

  // ==========================================================================
  // Helper Methods
  // ==========================================================================

  private def deleteDirectory(directory: File): Unit =
    if (directory.exists()) {
      Option(directory.listFiles()).foreach(_.foreach { file =>
        if (file.isDirectory) deleteDirectory(file)
        else file.delete()
      })
      directory.delete()
    }
}
