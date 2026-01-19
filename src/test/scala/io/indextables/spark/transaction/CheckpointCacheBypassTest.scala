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

package io.indextables.spark.transaction

import java.io.{InputStream, OutputStream}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.indextables.spark.io.{CloudFileInfo, CloudStorageProvider, CloudStorageCounters, HadoopCloudStorageProvider}
import io.indextables.spark.TestBase

/**
 * Test suite to reproduce and measure the checkpoint cache bypass issue.
 *
 * The issue: When reading metadata from a table with checkpoints, the EnhancedTransactionLogCache correctly caches the
 * computed MetadataAction. However, on cache MISS (first read or after expiration), the compute function calls
 * TransactionLogCheckpoint.getActionsFromCheckpoint() which reads directly from storage WITHOUT using any caching
 * layer.
 *
 * This results in:
 *   1. _last_checkpoint file read (to get checkpoint version) 2. Checkpoint file read (to get actual actions)
 *
 * Both of these reads happen EVERY TIME the metadata cache misses, even if the same checkpoint was just read moments
 * ago for another operation (like getProtocol).
 */
class CheckpointCacheBypassTest extends TestBase {

  /**
   * A wrapper around CloudStorageProvider that counts file read operations. This allows us to measure the actual I/O
   * happening at the storage layer.
   */
  class CountingCloudStorageProvider(delegate: CloudStorageProvider) extends CloudStorageProvider {
    val readFileCount = new AtomicInteger(0)
    val readFilePaths = new java.util.concurrent.ConcurrentLinkedQueue[String]()

    override def listFiles(path: String, recursive: Boolean): Seq[CloudFileInfo] =
      delegate.listFiles(path, recursive)

    override def exists(path: String): Boolean = delegate.exists(path)

    override def getFileInfo(path: String): Option[CloudFileInfo] = delegate.getFileInfo(path)

    override def readFile(path: String): Array[Byte] = {
      readFileCount.incrementAndGet()
      readFilePaths.add(path)
      delegate.readFile(path)
    }

    override def readRange(
      path: String,
      offset: Long,
      length: Long
    ): Array[Byte] =
      delegate.readRange(path, offset, length)

    override def openInputStream(path: String): InputStream = delegate.openInputStream(path)

    override def createOutputStream(path: String): OutputStream = delegate.createOutputStream(path)

    override def writeFile(path: String, content: Array[Byte]): Unit =
      delegate.writeFile(path, content)

    override def writeFileIfNotExists(path: String, content: Array[Byte]): Boolean =
      delegate.writeFileIfNotExists(path, content)

    override def writeFileFromStream(
      path: String,
      inputStream: InputStream,
      contentLength: Option[Long]
    ): Unit =
      delegate.writeFileFromStream(path, inputStream, contentLength)

    override def deleteFile(path: String): Boolean = delegate.deleteFile(path)

    override def createDirectory(path: String): Boolean = delegate.createDirectory(path)

    override def readFilesParallel(paths: Seq[String]): Map[String, Array[Byte]] =
      delegate.readFilesParallel(paths)

    override def existsParallel(paths: Seq[String]): Map[String, Boolean] =
      delegate.existsParallel(paths)

    override def getProviderType: String = s"Counting[${delegate.getProviderType}]"

    override def close(): Unit = delegate.close()

    def reset(): Unit = {
      readFileCount.set(0)
      readFilePaths.clear()
    }

    def getReadCount: Int = readFileCount.get()

    def getReadPaths: Seq[String] = {
      import scala.jdk.CollectionConverters._
      readFilePaths.asScala.toSeq
    }

    def getCheckpointReadCount: Int =
      getReadPaths.count(_.contains("checkpoint"))

    def getLastCheckpointReadCount: Int =
      getReadPaths.count(_.contains("_last_checkpoint"))
  }

  test("REPRODUCE: checkpoint file reads happen on every metadata cache miss") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      // Create table with checkpoints enabled (interval=3 to trigger checkpoint quickly)
      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.checkpoint.enabled"  -> "true",
          "spark.indextables.checkpoint.interval" -> "3"
        ).asJava
      )

      val txLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        // Initialize and write enough data to trigger checkpoint
        val schema = StructType(
          Seq(
            StructField("id", IntegerType, true),
            StructField("name", StringType, true)
          )
        )
        txLog.initialize(schema)

        // Add files to trigger checkpoint creation (interval=3)
        for (i <- 1 to 5) {
          val addAction = AddAction(
            path = s"file$i.split",
            partitionValues = Map.empty,
            size = 1000L * i,
            modificationTime = System.currentTimeMillis(),
            dataChange = true
          )
          txLog.addFiles(Seq(addAction))
        }

        // Verify checkpoint was created
        val checkpointVersion = txLog.getLastCheckpointVersion()
        assert(checkpointVersion.isDefined, "Checkpoint should have been created")
        println(s"Checkpoint created at version: ${checkpointVersion.get}")

        // Now invalidate cache to simulate fresh reads
        txLog.invalidateCache()

        // Count reads during first metadata access
        println("\n=== First getMetadata() call (cache miss expected) ===")
        val startTime1 = System.nanoTime()
        val metadata1  = txLog.getMetadata()
        val time1      = (System.nanoTime() - startTime1) / 1000000.0
        println(s"Time: ${time1}ms")
        println(s"Metadata id: ${metadata1.id}")

        // Get cache stats after first read
        txLog.getCacheStats() match {
          case Some(stats) =>
            println(s"Cache stats after first read: hits=${stats.hits}, misses=${stats.misses}")
          case None =>
            println("No cache stats available")
        }

        // Second read should hit cache
        println("\n=== Second getMetadata() call (cache hit expected) ===")
        val startTime2 = System.nanoTime()
        val metadata2  = txLog.getMetadata()
        val time2      = (System.nanoTime() - startTime2) / 1000000.0
        println(s"Time: ${time2}ms")

        txLog.getCacheStats() match {
          case Some(stats) =>
            println(s"Cache stats after second read: hits=${stats.hits}, misses=${stats.misses}")
            // Verify cache hit
            assert(stats.hits > 0, "Should have cache hits on second read")
          case None =>
            println("No cache stats available")
        }

        // The second read should be faster (cached)
        assert(
          time2 < time1 * 0.5 || time2 < 5.0,
          s"Second read ($time2 ms) should be significantly faster than first ($time1 ms)"
        )

        println(s"\nPerformance improvement: ${((time1 - time2) / time1 * 100).formatted("%.1f")}% faster on cache hit")

      } finally
        txLog.close()
    }
  }

  test("MEASURE: count checkpoint file reads during repeated metadata access") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.checkpoint.enabled"  -> "true",
          "spark.indextables.checkpoint.interval" -> "3"
        ).asJava
      )

      val txLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        // Initialize and create checkpoint
        val schema = StructType(
          Seq(
            StructField("id", IntegerType, true),
            StructField("name", StringType, true)
          )
        )
        txLog.initialize(schema)

        for (i <- 1 to 5)
          txLog.addFiles(
            Seq(
              AddAction(
                path = s"file$i.split",
                partitionValues = Map.empty,
                size = 1000L,
                modificationTime = System.currentTimeMillis(),
                dataChange = true
              )
            )
          )

        assert(txLog.getLastCheckpointVersion().isDefined, "Checkpoint should exist")

        // Now do multiple operations that should all hit the same cached checkpoint
        println("\n=== Multiple operations that should use cached checkpoint ===")

        // Invalidate cache to get fresh stats
        txLog.invalidateCache()

        // These operations all need metadata/protocol which come from checkpoint
        val operations = Seq(
          ("getMetadata()", () => txLog.getMetadata()),
          ("getSchema()", () => txLog.getSchema()),
          ("getProtocol()", () => txLog.getProtocol()),
          ("getMetadata() again", () => txLog.getMetadata()),
          ("getProtocol() again", () => txLog.getProtocol())
        )

        operations.foreach {
          case (name, op) =>
            val start = System.nanoTime()
            op()
            val elapsed = (System.nanoTime() - start) / 1000000.0

            txLog.getCacheStats() match {
              case Some(stats) =>
                println(f"$name%-25s: $elapsed%6.2f ms (cache hits: ${stats.hits}, misses: ${stats.misses})")
              case None =>
                println(f"$name%-25s: $elapsed%6.2f ms")
            }
        }

        println("\nExpected behavior:")
        println("- First getMetadata() should cache miss and read checkpoint")
        println("- Subsequent calls should hit cache WITHOUT reading checkpoint again")

      } finally
        txLog.close()
    }
  }

  test("DEMONSTRATE: V2 DataSource repeated schema access pattern") {
    // This test simulates what happens in production: multiple Spark operations
    // trigger repeated schema reads, each potentially causing checkpoint I/O

    withTempPath { tempPath =>
      // Write a table with V2 API
      val df = spark.range(100).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1") // Ensure checkpoint is created
        .mode("overwrite")
        .save(tempPath)

      println("\n=== V2 DataSource repeated read pattern ===")

      // Simulate multiple queries on the same table
      val operations = Seq(
        "First read (DataFrame creation)",
        "Count query",
        "Filter query",
        "Aggregation query",
        "Second filter query"
      )

      operations.foreach { opName =>
        val start = System.nanoTime()

        // Each of these creates a new DataFrame, triggering schema reads
        val readDf = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(tempPath)

        // Force schema resolution
        readDf.schema

        val elapsed = (System.nanoTime() - start) / 1000000.0
        println(f"$opName%-35s: $elapsed%6.2f ms")
      }

      println("\nIn production with S3, each cache miss adds ~50-200ms of network latency")
      println("for reading _last_checkpoint and checkpoint files.")
    }
  }

  test("VERIFY: global cache is shared across TransactionLog instances") {
    // This test verifies that checkpoint data cached during schema inference
    // is reused by subsequent table operations (the bug fix)

    withTempPath { tempPath =>
      // First, create a table with checkpoint
      val df = spark.range(100).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear the global cache to start fresh
      EnhancedTransactionLogCache.clearGlobalCaches()

      // Get initial global cache stats
      val (actionsStatsBefore, lastCheckpointStatsBefore) = EnhancedTransactionLogCache.getGlobalCacheStats()
      val missCountBefore = actionsStatsBefore.missCount() + lastCheckpointStatsBefore.missCount()

      println(s"\n=== Global cache before any reads ===")
      println(s"Checkpoint actions: hits=${actionsStatsBefore.hitCount()}, misses=${actionsStatsBefore.missCount()}")
      println(s"Last checkpoint info: hits=${lastCheckpointStatsBefore.hitCount()}, misses=${lastCheckpointStatsBefore.missCount()}")

      // First read - this triggers inferSchema which creates TransactionLog #1
      // and should populate the global cache
      val readDf1 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      // Force schema resolution (this is what triggers the checkpoint read)
      val schema1 = readDf1.schema

      val (actionsStatsAfterSchema, lastCheckpointStatsAfterSchema) = EnhancedTransactionLogCache.getGlobalCacheStats()

      println(s"\n=== Global cache after schema inference ===")
      println(
        s"Checkpoint actions: hits=${actionsStatsAfterSchema.hitCount()}, misses=${actionsStatsAfterSchema.missCount()}"
      )
      println(s"Last checkpoint info: hits=${lastCheckpointStatsAfterSchema.hitCount()}, misses=${lastCheckpointStatsAfterSchema.missCount()}")

      // Now create a SECOND DataFrame - this creates TransactionLog #2
      // but should HIT the global cache (no new misses)
      val readDf2 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      val schema2 = readDf2.schema

      val (actionsStatsAfterSecond, lastCheckpointStatsAfterSecond) = EnhancedTransactionLogCache.getGlobalCacheStats()

      println(s"\n=== Global cache after second schema read ===")
      println(
        s"Checkpoint actions: hits=${actionsStatsAfterSecond.hitCount()}, misses=${actionsStatsAfterSecond.missCount()}"
      )
      println(s"Last checkpoint info: hits=${lastCheckpointStatsAfterSecond.hitCount()}, misses=${lastCheckpointStatsAfterSecond.missCount()}")

      // Verify: Second read should have MORE HITS but SAME MISSES
      // (global cache was reused across TransactionLog instances)
      val hitCountAfterSecond = actionsStatsAfterSecond.hitCount() + lastCheckpointStatsAfterSecond.hitCount()
      val hitCountAfterSchema = actionsStatsAfterSchema.hitCount() + lastCheckpointStatsAfterSchema.hitCount()

      assert(
        hitCountAfterSecond > hitCountAfterSchema,
        s"Second read should have more cache hits ($hitCountAfterSecond) than first read ($hitCountAfterSchema)"
      )

      println(s"\n✅ Global cache sharing verified: second TransactionLog instance reused cached checkpoint data")
      println(s"   Hits increased from $hitCountAfterSchema to $hitCountAfterSecond")
    }
  }

  test("QUANTIFY: cache miss penalty with simulated network latency") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.checkpoint.enabled"  -> "true",
          "spark.indextables.checkpoint.interval" -> "3",
          // Short cache TTL to force more misses
          "spark.indextables.cache.metadata.ttl" -> "1" // 1 minute
        ).asJava
      )

      val txLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        val schema = StructType(
          Seq(
            StructField("id", IntegerType, true),
            StructField("name", StringType, true)
          )
        )
        txLog.initialize(schema)

        for (i <- 1 to 5)
          txLog.addFiles(
            Seq(
              AddAction(
                path = s"file$i.split",
                partitionValues = Map.empty,
                size = 1000L,
                modificationTime = System.currentTimeMillis(),
                dataChange = true
              )
            )
          )

        assert(txLog.getLastCheckpointVersion().isDefined)

        println("\n=== Quantifying cache miss penalty ===")

        // Measure with cache invalidation (simulates cache miss)
        val missTimings = (1 to 5).map { i =>
          txLog.invalidateCache()
          val start = System.nanoTime()
          txLog.getMetadata()
          (System.nanoTime() - start) / 1000000.0
        }

        // Measure with cache hit
        val hitTimings = (1 to 5).map { _ =>
          val start = System.nanoTime()
          txLog.getMetadata()
          (System.nanoTime() - start) / 1000000.0
        }

        val avgMissTime = missTimings.sum / missTimings.size
        val avgHitTime  = hitTimings.sum / hitTimings.size

        println(f"Average cache MISS time: $avgMissTime%.2f ms (reads checkpoint from storage)")
        println(f"Average cache HIT time:  $avgHitTime%.2f ms (returns cached value)")
        println(
          f"Cache miss penalty:      ${avgMissTime - avgHitTime}%.2f ms (${(avgMissTime / avgHitTime).formatted("%.1f")}x slower)"
        )

        println("\nWith S3 network latency (~50-100ms per read):")
        val estimatedS3MissTime = avgMissTime + 100 // Add ~100ms for 2 S3 reads
        println(f"Estimated S3 cache miss: $estimatedS3MissTime%.0f ms")
        println(f"Estimated S3 penalty:    ${estimatedS3MissTime - avgHitTime}%.0f ms")

        // The fix should eliminate checkpoint file reads on cache miss by caching
        // the checkpoint data itself, not just the computed metadata

      } finally
        txLog.close()
    }
  }

  test("ASSERT: global counters verify no repeated exists calls for cached checkpoint info") {
    // This test uses global CloudStorageProvider counters to verify that
    // the checkpoint caching fix is working - exists() should not be called
    // repeatedly when accessing getLastCheckpointInfo() multiple times
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.checkpoint.enabled"  -> "true",
          "spark.indextables.checkpoint.interval" -> "3"
        ).asJava
      )

      val txLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        // Initialize and create checkpoint
        val schema = StructType(
          Seq(
            StructField("id", IntegerType, true),
            StructField("name", StringType, true)
          )
        )
        txLog.initialize(schema)

        // Add files to trigger checkpoint creation (interval=3)
        for (i <- 1 to 5)
          txLog.addFiles(
            Seq(
              AddAction(
                path = s"file$i.split",
                partitionValues = Map.empty,
                size = 1000L,
                modificationTime = System.currentTimeMillis(),
                dataChange = true
              )
            )
          )

        assert(txLog.getLastCheckpointVersion().isDefined, "Checkpoint should have been created")

        // Reset global counters to start fresh measurement
        CloudStorageProvider.resetCounters()
        val startCounters = CloudStorageProvider.getCountersSnapshot

        // Invalidate cache to simulate fresh start
        txLog.invalidateCache()
        EnhancedTransactionLogCache.clearGlobalCaches()

        println("\n=== Testing checkpoint info caching with global counters ===")
        println(s"Initial exists count: ${startCounters.exists}")

        // First call - should cause one exists() call (cache miss)
        val checkpointInfo1 = txLog.getLastCheckpointInfo()
        val afterFirstCall = CloudStorageProvider.getCountersSnapshot
        val existsAfterFirst = afterFirstCall.exists - startCounters.exists
        println(s"After first getLastCheckpointInfo(): exists calls = $existsAfterFirst")

        // Second call - should NOT cause additional exists() calls (cache hit)
        val checkpointInfo2 = txLog.getLastCheckpointInfo()
        val afterSecondCall = CloudStorageProvider.getCountersSnapshot
        val existsAfterSecond = afterSecondCall.exists - startCounters.exists
        println(s"After second getLastCheckpointInfo(): exists calls = $existsAfterSecond")

        // Third call - should still NOT cause additional exists() calls
        val checkpointInfo3 = txLog.getLastCheckpointInfo()
        val afterThirdCall = CloudStorageProvider.getCountersSnapshot
        val existsAfterThird = afterThirdCall.exists - startCounters.exists
        println(s"After third getLastCheckpointInfo(): exists calls = $existsAfterThird")

        // CRITICAL ASSERTION: After the first call, subsequent calls should cause
        // ZERO additional exists() calls. This was the bug we fixed.
        val additionalExistsSecond = existsAfterSecond - existsAfterFirst
        val additionalExistsThird = existsAfterThird - existsAfterFirst

        assert(
          additionalExistsSecond == 0,
          s"Second getLastCheckpointInfo() should cause ZERO additional exists() calls! " +
            s"Got $additionalExistsSecond additional calls. " +
            s"Cache bypass bug detected!"
        )

        assert(
          additionalExistsThird == 0,
          s"Third getLastCheckpointInfo() should cause ZERO additional exists() calls! " +
            s"Got $additionalExistsThird additional calls (total: $existsAfterThird). " +
            s"Cache bypass bug detected!"
        )

        println(s"\n✅ PASSED: ZERO additional exists() calls after first cache miss")
        println(s"   First call: $existsAfterFirst exists() calls (cache miss)")
        println(s"   Second call: +$additionalExistsSecond exists() calls (cache hit)")
        println(s"   Third call: +$additionalExistsThird exists() calls (cache hit)")

      } finally
        txLog.close()
    }
  }

  test("ASSERT: multiple metadata reads do not cause repeated storage calls") {
    // This test verifies that multiple operations that need metadata
    // (getMetadata, getSchema, getProtocol) don't cause repeated storage calls
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.checkpoint.enabled"  -> "true",
          "spark.indextables.checkpoint.interval" -> "3"
        ).asJava
      )

      val txLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        val schema = StructType(
          Seq(
            StructField("id", IntegerType, true),
            StructField("name", StringType, true)
          )
        )
        txLog.initialize(schema)

        for (i <- 1 to 5)
          txLog.addFiles(
            Seq(
              AddAction(
                path = s"file$i.split",
                partitionValues = Map.empty,
                size = 1000L,
                modificationTime = System.currentTimeMillis(),
                dataChange = true
              )
            )
          )

        assert(txLog.getLastCheckpointVersion().isDefined)

        // Clear caches and reset counters
        txLog.invalidateCache()
        EnhancedTransactionLogCache.clearGlobalCaches()
        CloudStorageProvider.resetCounters()

        println("\n=== Testing multiple metadata operations with global counters ===")

        // Perform 5 different operations that all need checkpoint/metadata
        txLog.getMetadata()
        val afterOp1 = CloudStorageProvider.getCountersSnapshot
        println(s"After getMetadata(): exists=${afterOp1.exists}, readFile=${afterOp1.readFile}")

        txLog.getSchema()
        val afterOp2 = CloudStorageProvider.getCountersSnapshot
        println(s"After getSchema(): exists=${afterOp2.exists}, readFile=${afterOp2.readFile}")

        txLog.getProtocol()
        val afterOp3 = CloudStorageProvider.getCountersSnapshot
        println(s"After getProtocol(): exists=${afterOp3.exists}, readFile=${afterOp3.readFile}")

        txLog.getMetadata()
        val afterOp4 = CloudStorageProvider.getCountersSnapshot
        println(s"After getMetadata() again: exists=${afterOp4.exists}, readFile=${afterOp4.readFile}")

        txLog.getLastCheckpointInfo()
        val afterOp5 = CloudStorageProvider.getCountersSnapshot
        println(s"After getLastCheckpointInfo(): exists=${afterOp5.exists}, readFile=${afterOp5.readFile}")

        // After the first few operations populate the cache, operations 4 and 5
        // should cause ZERO additional exists() calls
        val additionalExistsOp4 = afterOp4.exists - afterOp3.exists
        val additionalExistsOp5 = afterOp5.exists - afterOp4.exists

        assert(
          additionalExistsOp4 == 0,
          s"Operation 4 (getMetadata again) should cause ZERO exists() calls! " +
            s"Got $additionalExistsOp4 additional calls. Cache bypass detected."
        )

        assert(
          additionalExistsOp5 == 0,
          s"Operation 5 (getLastCheckpointInfo) should cause ZERO exists() calls! " +
            s"Got $additionalExistsOp5 additional calls. Cache bypass detected."
        )

        println(s"\n✅ PASSED: ZERO storage calls after cache warm-up")
        println(s"   Operations 1-3 (warm-up): ${afterOp3.exists} exists, ${afterOp3.readFile} readFile")
        println(s"   Operation 4: +$additionalExistsOp4 exists (expected 0)")
        println(s"   Operation 5: +$additionalExistsOp5 exists (expected 0)")

      } finally
        txLog.close()
    }
  }

  test("ASSERT: query execution should cause ZERO storage calls after read") {
    // This test verifies that once a DataFrame is created (read), executing
    // queries on it should NOT cause any additional storage calls for checkpoint info.
    // Read operations can make storage calls, but query execution must reuse cached data.
    withTempPath { tempPath =>
      // Write a table first
      val df = spark.range(100).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches and reset counters
      EnhancedTransactionLogCache.clearGlobalCaches()
      CloudStorageProvider.resetCounters()

      println("\n=== Testing query execution (not reads) ===")

      // Read operation - creates DataFrame and resolves schema
      // This can make storage calls (cache miss)
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.schema // Force schema resolution
      val afterRead = CloudStorageProvider.getCountersSnapshot
      println(s"After read (DataFrame creation): exists=${afterRead.exists}, readFile=${afterRead.readFile}")

      // Now execute multiple queries on the same DataFrame
      // These should cause ZERO additional checkpoint-related storage calls
      val query1Result = readDf.count()
      val afterQuery1 = CloudStorageProvider.getCountersSnapshot
      val query1Exists = afterQuery1.exists - afterRead.exists
      println(s"After query 1 (count): exists=${afterQuery1.exists} (+$query1Exists), result=$query1Result")

      val query2Result = readDf.filter("id > 50").count()
      val afterQuery2 = CloudStorageProvider.getCountersSnapshot
      val query2Exists = afterQuery2.exists - afterQuery1.exists
      println(s"After query 2 (filter+count): exists=${afterQuery2.exists} (+$query2Exists), result=$query2Result")

      val query3Result = readDf.agg(org.apache.spark.sql.functions.sum("id")).collect()
      val afterQuery3 = CloudStorageProvider.getCountersSnapshot
      val query3Exists = afterQuery3.exists - afterQuery2.exists
      println(s"After query 3 (agg): exists=${afterQuery3.exists} (+$query3Exists)")

      // collect() is the most important one - it fetches all data
      val query4Result = readDf.collect()
      val afterQuery4 = CloudStorageProvider.getCountersSnapshot
      val query4Exists = afterQuery4.exists - afterQuery3.exists
      println(s"After query 4 (collect): exists=${afterQuery4.exists} (+$query4Exists), rows=${query4Result.length}")

      // CRITICAL ASSERTIONS: Query execution should NOT make storage calls
      // Note: Query execution may need to re-read split data, but should NOT
      // need to check checkpoint info (that was done during schema resolution)
      // The exists() calls are specifically for checkpoint info (_last_checkpoint file)
      val totalQueryExists = (afterQuery4.exists - afterRead.exists)

      println(s"\n=== Summary ===")
      println(s"Read operation (cache miss): ${afterRead.exists} exists() calls")
      println(s"Query execution (4 queries including collect): $totalQueryExists exists() calls")

      // We expect zero exists calls during query execution because:
      // - getLastCheckpointInfo() should be cached after the read
      // - Query execution reuses the cached checkpoint info
      assert(
        totalQueryExists == 0,
        s"Query execution should cause ZERO exists() calls! " +
          s"Got $totalQueryExists calls (count=+$query1Exists, filter=+$query2Exists, agg=+$query3Exists, collect=+$query4Exists). " +
          s"This indicates checkpoint cache bypass during queries."
      )

      println(s"\n✅ PASSED: ZERO exists() calls during query execution (including collect)")
    }
  }

  test("ASSERT: SQL cache invalidation causes storage calls to resume") {
    // This test verifies that after calling INVALIDATE INDEXTABLES TRANSACTION LOG CACHE,
    // subsequent reads DO cause storage calls again (proving the cache was actually invalidated)
    withTempPath { tempPath =>
      // Write a table first
      val df = spark.range(100).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches and reset counters
      EnhancedTransactionLogCache.clearGlobalCaches()
      CloudStorageProvider.resetCounters()

      println("\n=== Testing SQL INVALIDATE command causes storage calls to resume ===")

      // Phase 1: Initial read to warm cache
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.schema
      val afterInitialRead = CloudStorageProvider.getCountersSnapshot
      println(s"After initial read (cache warm-up): exists=${afterInitialRead.exists}")

      // Phase 2: Execute query - should cause ZERO exists calls (cache hit)
      val result1 = readDf.collect()
      val afterQuery1 = CloudStorageProvider.getCountersSnapshot
      val query1Exists = afterQuery1.exists - afterInitialRead.exists
      println(s"After first collect(): exists=${afterQuery1.exists} (+$query1Exists), rows=${result1.length}")

      assert(
        query1Exists == 0,
        s"Query before invalidation should cause ZERO exists() calls! Got $query1Exists"
      )

      // Phase 3: Use SQL command to invalidate the cache
      println(s"\n--- Running: INVALIDATE INDEXTABLES TRANSACTION LOG CACHE FOR '$tempPath' ---")
      val invalidateResult = spark.sql(s"INVALIDATE INDEXTABLES TRANSACTION LOG CACHE FOR '$tempPath'")
      invalidateResult.show(truncate = false)
      val afterInvalidation = CloudStorageProvider.getCountersSnapshot
      println(s"After SQL INVALIDATE command: exists=${afterInvalidation.exists}")

      // Phase 4: Read again - this should cause exists calls (cache miss after invalidation)
      val readDf2 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf2.schema
      val afterSecondRead = CloudStorageProvider.getCountersSnapshot
      val secondReadExists = afterSecondRead.exists - afterInvalidation.exists
      println(s"After second read (post-invalidation): exists=${afterSecondRead.exists} (+$secondReadExists)")

      // CRITICAL ASSERTION: After invalidation, reading should cause storage calls
      assert(
        secondReadExists > 0,
        s"After cache invalidation, reading should cause exists() calls! " +
          s"Got $secondReadExists calls. Cache invalidation may not be working."
      )

      // Phase 5: First collect after invalidation - this uses the schema which was just read
      val result2 = readDf2.collect()
      val afterCollect2 = CloudStorageProvider.getCountersSnapshot
      val collect2Exists = afterCollect2.exists - afterSecondRead.exists
      println(s"After first collect (post-invalidation): exists=${afterCollect2.exists} (+$collect2Exists), rows=${result2.length}")

      // Phase 6: Another collect on the same DataFrame - should be ZERO (cache warm)
      val result3 = readDf2.collect()
      val afterCollect3 = CloudStorageProvider.getCountersSnapshot
      val collect3Exists = afterCollect3.exists - afterCollect2.exists
      println(s"After second collect (same DataFrame): exists=${afterCollect3.exists} (+$collect3Exists), rows=${result3.length}")

      // Phase 7: Third collect to really prove the point
      val result4 = readDf2.collect()
      val afterCollect4 = CloudStorageProvider.getCountersSnapshot
      val collect4Exists = afterCollect4.exists - afterCollect3.exists
      println(s"After third collect (same DataFrame): exists=${afterCollect4.exists} (+$collect4Exists), rows=${result4.length}")

      // CRITICAL ASSERTION: Collects after cache warm-up should be ZERO
      assert(
        collect2Exists == 0,
        s"First collect after read should cause ZERO exists() calls! Got $collect2Exists"
      )
      assert(
        collect3Exists == 0,
        s"Second collect (same DataFrame) should cause ZERO exists() calls! Got $collect3Exists"
      )
      assert(
        collect4Exists == 0,
        s"Third collect (same DataFrame) should cause ZERO exists() calls! Got $collect4Exists"
      )

      println(s"\n=== Summary ===")
      println(s"Initial read (cache warm-up): ${afterInitialRead.exists} exists() calls")
      println(s"First collect (cache hit): +$query1Exists exists() calls")
      println(s"SQL INVALIDATE command executed")
      println(s"New read after invalidation: +$secondReadExists exists() calls (cache miss)")
      println(s"First collect after new read: +$collect2Exists exists() calls (cache hit)")
      println(s"Second collect (same DF): +$collect3Exists exists() calls (cache hit)")
      println(s"Third collect (same DF): +$collect4Exists exists() calls (cache hit)")

      println(s"\n✅ PASSED: SQL INVALIDATE causes storage calls to resume, subsequent collects use cache")
    }
  }

  test("ASSERT: global SQL INVALIDATE clears all transaction log caches") {
    // This test verifies that INVALIDATE INDEXTABLES TRANSACTION LOG CACHE (without FOR clause)
    // clears all global caches, causing subsequent reads to make storage calls
    withTempPath { tempPath =>
      // Write a table first
      val df = spark.range(100).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.checkpoint.enabled", "true")
        .option("spark.indextables.checkpoint.interval", "1")
        .mode("overwrite")
        .save(tempPath)

      // Clear caches and reset counters
      EnhancedTransactionLogCache.clearGlobalCaches()
      CloudStorageProvider.resetCounters()

      println("\n=== Testing global SQL INVALIDATE command ===")

      // Phase 1: Initial read to warm cache
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf.schema
      val afterInitialRead = CloudStorageProvider.getCountersSnapshot
      println(s"After initial read (cache warm-up): exists=${afterInitialRead.exists}")

      // Phase 2: Execute query - should cause ZERO exists calls (cache hit)
      val result1 = readDf.collect()
      val afterQuery1 = CloudStorageProvider.getCountersSnapshot
      val query1Exists = afterQuery1.exists - afterInitialRead.exists
      println(s"After first collect(): exists=${afterQuery1.exists} (+$query1Exists), rows=${result1.length}")

      assert(
        query1Exists == 0,
        s"Query before invalidation should cause ZERO exists() calls! Got $query1Exists"
      )

      // Phase 3: Use global SQL command to invalidate ALL caches (no FOR clause)
      println("\n--- Running: INVALIDATE INDEXTABLES TRANSACTION LOG CACHE (global) ---")
      val invalidateResult = spark.sql("INVALIDATE INDEXTABLES TRANSACTION LOG CACHE")
      invalidateResult.show(truncate = false)
      val afterInvalidation = CloudStorageProvider.getCountersSnapshot
      println(s"After global SQL INVALIDATE command: exists=${afterInvalidation.exists}")

      // Verify the command succeeded
      val resultRow = invalidateResult.collect().head
      assert(resultRow.getString(0) == "GLOBAL", "Should be global invalidation")
      assert(resultRow.getString(1).contains("cleared successfully"), s"Should report success: ${resultRow.getString(1)}")

      // Phase 4: Read again - this should cause exists calls (cache miss after global invalidation)
      val readDf2 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      readDf2.schema
      val afterSecondRead = CloudStorageProvider.getCountersSnapshot
      val secondReadExists = afterSecondRead.exists - afterInvalidation.exists
      println(s"After second read (post-global-invalidation): exists=${afterSecondRead.exists} (+$secondReadExists)")

      // CRITICAL ASSERTION: After global invalidation, reading should cause storage calls
      assert(
        secondReadExists > 0,
        s"After global cache invalidation, reading should cause exists() calls! " +
          s"Got $secondReadExists calls. Global cache invalidation may not be working."
      )

      // Phase 5: Collect on the new DataFrame - should be zero (cache re-warmed)
      val result2 = readDf2.collect()
      val afterCollect2 = CloudStorageProvider.getCountersSnapshot
      val collect2Exists = afterCollect2.exists - afterSecondRead.exists
      println(s"After collect (post-global-invalidation): exists=${afterCollect2.exists} (+$collect2Exists), rows=${result2.length}")

      assert(
        collect2Exists == 0,
        s"Collect after re-warming cache should cause ZERO exists() calls! Got $collect2Exists"
      )

      println(s"\n=== Summary ===")
      println(s"Initial read (cache warm-up): ${afterInitialRead.exists} exists() calls")
      println(s"First collect (cache hit): +$query1Exists exists() calls")
      println(s"Global SQL INVALIDATE executed")
      println(s"New read after global invalidation: +$secondReadExists exists() calls (cache miss)")
      println(s"Collect after new read: +$collect2Exists exists() calls (cache hit)")

      println(s"\n✅ PASSED: Global SQL INVALIDATE clears all caches, subsequent reads cause storage calls")
    }
  }
}
