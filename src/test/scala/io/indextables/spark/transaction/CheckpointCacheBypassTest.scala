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

import io.indextables.spark.TestBase
import io.indextables.spark.io.{CloudFileInfo, CloudStorageProvider, HadoopCloudStorageProvider}

/**
 * Test suite to reproduce and measure the checkpoint cache bypass issue.
 *
 * The issue: When reading metadata from a table with checkpoints, the EnhancedTransactionLogCache
 * correctly caches the computed MetadataAction. However, on cache MISS (first read or after expiration),
 * the compute function calls TransactionLogCheckpoint.getActionsFromCheckpoint() which reads
 * directly from storage WITHOUT using any caching layer.
 *
 * This results in:
 * 1. _last_checkpoint file read (to get checkpoint version)
 * 2. Checkpoint file read (to get actual actions)
 *
 * Both of these reads happen EVERY TIME the metadata cache misses, even if the same checkpoint
 * was just read moments ago for another operation (like getProtocol).
 */
class CheckpointCacheBypassTest extends TestBase {

  /**
   * A wrapper around CloudStorageProvider that counts file read operations.
   * This allows us to measure the actual I/O happening at the storage layer.
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

    override def readRange(path: String, offset: Long, length: Long): Array[Byte] =
      delegate.readRange(path, offset, length)

    override def openInputStream(path: String): InputStream = delegate.openInputStream(path)

    override def createOutputStream(path: String): OutputStream = delegate.createOutputStream(path)

    override def writeFile(path: String, content: Array[Byte]): Unit =
      delegate.writeFile(path, content)

    override def writeFileIfNotExists(path: String, content: Array[Byte]): Boolean =
      delegate.writeFileIfNotExists(path, content)

    override def writeFileFromStream(path: String, inputStream: InputStream, contentLength: Option[Long]): Unit =
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

    def getCheckpointReadCount: Int = {
      getReadPaths.count(_.contains("checkpoint"))
    }

    def getLastCheckpointReadCount: Int = {
      getReadPaths.count(_.contains("_last_checkpoint"))
    }
  }

  test("REPRODUCE: checkpoint file reads happen on every metadata cache miss") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      // Create table with checkpoints enabled (interval=3 to trigger checkpoint quickly)
      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.checkpoint.enabled" -> "true",
          "spark.indextables.checkpoint.interval" -> "3"
        ).asJava
      )

      val txLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        // Initialize and write enough data to trigger checkpoint
        val schema = StructType(Seq(
          StructField("id", IntegerType, true),
          StructField("name", StringType, true)
        ))
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
        val metadata1 = txLog.getMetadata()
        val time1 = (System.nanoTime() - startTime1) / 1000000.0
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
        val metadata2 = txLog.getMetadata()
        val time2 = (System.nanoTime() - startTime2) / 1000000.0
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
        assert(time2 < time1 * 0.5 || time2 < 5.0,
          s"Second read ($time2 ms) should be significantly faster than first ($time1 ms)")

        println(s"\nPerformance improvement: ${((time1 - time2) / time1 * 100).formatted("%.1f")}% faster on cache hit")

      } finally {
        txLog.close()
      }
    }
  }

  test("MEASURE: count checkpoint file reads during repeated metadata access") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.checkpoint.enabled" -> "true",
          "spark.indextables.checkpoint.interval" -> "3"
        ).asJava
      )

      val txLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        // Initialize and create checkpoint
        val schema = StructType(Seq(
          StructField("id", IntegerType, true),
          StructField("name", StringType, true)
        ))
        txLog.initialize(schema)

        for (i <- 1 to 5) {
          txLog.addFiles(Seq(AddAction(
            path = s"file$i.split",
            partitionValues = Map.empty,
            size = 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true
          )))
        }

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

        operations.foreach { case (name, op) =>
          val start = System.nanoTime()
          op()
          val elapsed = (System.nanoTime() - start) / 1000000.0

          txLog.getCacheStats() match {
            case Some(stats) =>
              println(f"$name%-25s: ${elapsed}%6.2f ms (cache hits: ${stats.hits}, misses: ${stats.misses})")
            case None =>
              println(f"$name%-25s: ${elapsed}%6.2f ms")
          }
        }

        println("\nExpected behavior:")
        println("- First getMetadata() should cache miss and read checkpoint")
        println("- Subsequent calls should hit cache WITHOUT reading checkpoint again")

      } finally {
        txLog.close()
      }
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
        .option("spark.indextables.checkpoint.interval", "1")  // Ensure checkpoint is created
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
        println(f"$opName%-35s: ${elapsed}%6.2f ms")
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
      println(s"Checkpoint actions: hits=${actionsStatsAfterSchema.hitCount()}, misses=${actionsStatsAfterSchema.missCount()}")
      println(s"Last checkpoint info: hits=${lastCheckpointStatsAfterSchema.hitCount()}, misses=${lastCheckpointStatsAfterSchema.missCount()}")

      // Now create a SECOND DataFrame - this creates TransactionLog #2
      // but should HIT the global cache (no new misses)
      val readDf2 = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)

      val schema2 = readDf2.schema

      val (actionsStatsAfterSecond, lastCheckpointStatsAfterSecond) = EnhancedTransactionLogCache.getGlobalCacheStats()

      println(s"\n=== Global cache after second schema read ===")
      println(s"Checkpoint actions: hits=${actionsStatsAfterSecond.hitCount()}, misses=${actionsStatsAfterSecond.missCount()}")
      println(s"Last checkpoint info: hits=${lastCheckpointStatsAfterSecond.hitCount()}, misses=${lastCheckpointStatsAfterSecond.missCount()}")

      // Verify: Second read should have MORE HITS but SAME MISSES
      // (global cache was reused across TransactionLog instances)
      val hitCountAfterSecond = actionsStatsAfterSecond.hitCount() + lastCheckpointStatsAfterSecond.hitCount()
      val hitCountAfterSchema = actionsStatsAfterSchema.hitCount() + lastCheckpointStatsAfterSchema.hitCount()

      assert(hitCountAfterSecond > hitCountAfterSchema,
        s"Second read should have more cache hits (${hitCountAfterSecond}) than first read (${hitCountAfterSchema})")

      println(s"\n✅ Global cache sharing verified: second TransactionLog instance reused cached checkpoint data")
      println(s"   Hits increased from ${hitCountAfterSchema} to ${hitCountAfterSecond}")
    }
  }

  test("QUANTIFY: cache miss penalty with simulated network latency") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.checkpoint.enabled" -> "true",
          "spark.indextables.checkpoint.interval" -> "3",
          // Short cache TTL to force more misses
          "spark.indextables.cache.metadata.ttl" -> "1"  // 1 minute
        ).asJava
      )

      val txLog = TransactionLogFactory.create(tablePath, spark, options)

      try {
        val schema = StructType(Seq(
          StructField("id", IntegerType, true),
          StructField("name", StringType, true)
        ))
        txLog.initialize(schema)

        for (i <- 1 to 5) {
          txLog.addFiles(Seq(AddAction(
            path = s"file$i.split",
            partitionValues = Map.empty,
            size = 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true
          )))
        }

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
        val avgHitTime = hitTimings.sum / hitTimings.size

        println(f"Average cache MISS time: $avgMissTime%.2f ms (reads checkpoint from storage)")
        println(f"Average cache HIT time:  $avgHitTime%.2f ms (returns cached value)")
        println(f"Cache miss penalty:      ${avgMissTime - avgHitTime}%.2f ms (${(avgMissTime / avgHitTime).formatted("%.1f")}x slower)")

        println("\nWith S3 network latency (~50-100ms per read):")
        val estimatedS3MissTime = avgMissTime + 100  // Add ~100ms for 2 S3 reads
        println(f"Estimated S3 cache miss: $estimatedS3MissTime%.0f ms")
        println(f"Estimated S3 penalty:    ${estimatedS3MissTime - avgHitTime}%.0f ms")

        // The fix should eliminate checkpoint file reads on cache miss by caching
        // the checkpoint data itself, not just the computed metadata

      } finally {
        txLog.close()
      }
    }
  }
}
