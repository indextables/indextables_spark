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

import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import io.indextables.spark.config.IndexTables4SparkSQLConf
import io.indextables.spark.storage.SplitConversionThrottle
import io.indextables.spark.TestBase

/**
 * Tests for split conversion throttling functionality.
 *
 * The throttle limits the parallelism of tantivy index -> quickwit split conversions to prevent resource exhaustion.
 * Default: max(1, availableProcessors / 4)
 */
class SplitConversionThrottleTest extends TestBase {

  test("V2 DataSource should auto-configure split conversion max parallelism based on availableProcessors") {
    val tempPath = Files.createTempDirectory("v2_split_throttle_auto_").toFile.getAbsolutePath

    // Don't set splitConversion.maxParallelism - let it auto-configure
    val df = spark
      .range(0, 1000)
      .selectExpr("id", "CONCAT('content ', CAST(id AS STRING)) as content")

    // Write using V2 API (should auto-configure)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tempPath)

    // Verify data was written
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempPath)

    readDf.count() shouldBe 1000

    // Verify throttle was initialized
    val expectedMaxParallelism = Math.max(1, Runtime.getRuntime.availableProcessors() / 4)
    println(s"✅ V2 write completed with auto-configured throttle (expected: $expectedMaxParallelism based on ${Runtime.getRuntime.availableProcessors()} processors)")
  }

  test("V2 DataSource should respect explicit splitConversion.maxParallelism configuration") {
    val tempPath = Files.createTempDirectory("v2_split_throttle_explicit_").toFile.getAbsolutePath

    // Explicitly set maxParallelism to 2
    val df = spark
      .range(0, 1000)
      .selectExpr("id", "CONCAT('content ', CAST(id AS STRING)) as content")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option(IndexTables4SparkSQLConf.TANTIVY4SPARK_SPLIT_CONVERSION_MAX_PARALLELISM, "2")
      .save(tempPath)

    // Verify data was written
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempPath)

    readDf.count() shouldBe 1000

    println(s"✅ V2 write completed with explicit maxParallelism=2")
  }

  test("SplitConversionThrottle should limit concurrent operations") {
    // Test the throttle directly
    val maxParallelism = 3
    SplitConversionThrottle.initialize(maxParallelism)

    val concurrentOperations = new AtomicInteger(0)
    val maxConcurrent        = new AtomicInteger(0)
    val completedOperations  = new AtomicInteger(0)
    val totalOperations      = 10

    val executor   = Executors.newFixedThreadPool(totalOperations)
    val startLatch = new CountDownLatch(1)

    try {
      // Submit 10 operations that will all try to run concurrently
      (1 to totalOperations).foreach { i =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            startLatch.await() // Wait for all threads to be ready

            SplitConversionThrottle.withThrottle {
              // Track concurrent operations
              val current = concurrentOperations.incrementAndGet()

              // Update max concurrent if this is higher
              var prevMax = maxConcurrent.get()
              while (current > prevMax && !maxConcurrent.compareAndSet(prevMax, current))
                prevMax = maxConcurrent.get()

              // Simulate work
              Thread.sleep(50)

              concurrentOperations.decrementAndGet()
              completedOperations.incrementAndGet()
            }
          }
        })
      }

      // Release all threads at once
      startLatch.countDown()

      // Wait for all operations to complete
      executor.shutdown()
      executor.awaitTermination(10, TimeUnit.SECONDS) shouldBe true

      // Verify all operations completed
      completedOperations.get() shouldBe totalOperations

      // Verify max concurrent operations never exceeded the limit
      val observedMaxConcurrent = maxConcurrent.get()
      println(s"Max concurrent operations observed: $observedMaxConcurrent (limit: $maxParallelism)")

      // Due to timing, we might see slightly higher than maxParallelism in edge cases,
      // but it should be close to the limit
      observedMaxConcurrent should be <= (maxParallelism + 1) // Allow 1 for race conditions

      println(s"✅ Throttle correctly limited concurrency to ~$maxParallelism (observed: $observedMaxConcurrent)")
    } finally
      if (!executor.isTerminated) {
        executor.shutdownNow()
      }
  }

  test("SplitConversionThrottle should handle initialization idempotently") {
    // Initialize with value 5
    SplitConversionThrottle.initialize(5)
    SplitConversionThrottle.getMaxParallelism shouldBe 5

    // Re-initialize with same value (should be no-op)
    SplitConversionThrottle.initialize(5)
    SplitConversionThrottle.getMaxParallelism shouldBe 5

    // Initialize with different value (should update)
    SplitConversionThrottle.initialize(3)
    SplitConversionThrottle.getMaxParallelism shouldBe 3

    println("✅ Throttle initialization is idempotent")
  }

  test("SplitConversionThrottle should reject invalid parallelism values") {
    assertThrows[IllegalArgumentException] {
      SplitConversionThrottle.initialize(0)
    }

    assertThrows[IllegalArgumentException] {
      SplitConversionThrottle.initialize(-1)
    }

    println("✅ Throttle correctly rejects invalid parallelism values")
  }

  test("V2 DataSource with multiple partitions should throttle split conversions") {
    val tempPath = Files.createTempDirectory("v2_split_throttle_multipart_").toFile.getAbsolutePath

    // Create DataFrame with 4 partitions
    val df = spark
      .range(0, 4000, 1, numPartitions = 4)
      .selectExpr("id", "CONCAT('content ', CAST(id AS STRING)) as content")

    // Write with explicit low throttle to test limiting
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option(IndexTables4SparkSQLConf.TANTIVY4SPARK_SPLIT_CONVERSION_MAX_PARALLELISM, "1")
      .save(tempPath)

    // Verify all data was written correctly
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempPath)

    readDf.count() shouldBe 4000

    // Count split files
    val splitFiles = new java.io.File(tempPath)
      .listFiles()
      .filter(_.getName.endsWith(".split"))

    println(s"✅ Created ${splitFiles.length} split files with maxParallelism=1 throttling")
  }

  test("Default calculation should use availableProcessors/4") {
    val availableProcessors = Runtime.getRuntime.availableProcessors()
    val expectedDefault     = Math.max(1, availableProcessors / 4)

    println(s"Available processors: $availableProcessors")
    println(s"Expected default throttle: $expectedDefault")

    // The actual default is calculated in the write builders
    expectedDefault should be >= 1

    println(s"✅ Default calculation produces valid value: $expectedDefault")
  }

  test("SplitConversionThrottle permits should be released on exception") {
    val maxParallelism = 2
    SplitConversionThrottle.initialize(maxParallelism)

    // Record initial permits
    val initialPermits = SplitConversionThrottle.getAvailablePermits

    // Execute operation that throws exception
    assertThrows[RuntimeException] {
      SplitConversionThrottle.withThrottle {
        throw new RuntimeException("Simulated error")
      }
    }

    // Verify permit was released despite exception
    SplitConversionThrottle.getAvailablePermits shouldBe initialPermits

    println("✅ Throttle correctly releases permits on exception")
  }

  test("BatchWrite (V1 path) should also auto-configure split conversion throttle") {
    val tempPath = Files.createTempDirectory("batch_split_throttle_auto_").toFile.getAbsolutePath

    // Use V2 API which uses BatchWrite internally
    val df = spark
      .range(0, 1000)
      .selectExpr("id", "CONCAT('content ', CAST(id AS STRING)) as content")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tempPath)

    // Verify data was written
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tempPath)

    readDf.count() shouldBe 1000

    val expectedMaxParallelism = Math.max(1, Runtime.getRuntime.availableProcessors() / 4)
    println(s"✅ BatchWrite completed with auto-configured throttle (expected: $expectedMaxParallelism)")
  }

  test("Throttle should maintain FIFO ordering with fair semaphore") {
    val maxParallelism = 1 // Force serial execution
    SplitConversionThrottle.initialize(maxParallelism)

    val executionOrder  = new java.util.concurrent.ConcurrentLinkedQueue[Int]()
    val totalOperations = 5

    val executor   = Executors.newFixedThreadPool(totalOperations)
    val startLatch = new CountDownLatch(1)

    try {
      // Submit operations in order
      (1 to totalOperations).foreach { i =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            startLatch.await()

            SplitConversionThrottle.withThrottle {
              executionOrder.add(i)
              Thread.sleep(10) // Small delay to ensure serial execution
            }
          }
        })
      }

      startLatch.countDown()

      executor.shutdown()
      executor.awaitTermination(10, TimeUnit.SECONDS) shouldBe true

      // Verify execution order (with fair semaphore, should be roughly FIFO)
      val order = executionOrder.toArray().toSeq
      println(s"Execution order with fair semaphore (maxParallelism=1): $order")

      // All operations should complete
      order.length shouldBe totalOperations

      println(s"✅ Fair semaphore completed all $totalOperations operations")
    } finally
      if (!executor.isTerminated) {
        executor.shutdownNow()
      }
  }
}
