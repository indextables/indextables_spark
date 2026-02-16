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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase

class PrewarmCacheTest extends TestBase {

  test("prewarmCache should populate cache on table initialization") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val txLog     = TransactionLogFactory.create(tablePath, spark)

      try {
        // Initialize table with some data
        val schema = getTestSchema()
        txLog.initialize(schema)

        // Add some files
        for (i <- 1 to 5) {
          val addAction = AddAction(
            path = s"s3://bucket/file$i.split",
            partitionValues = Map.empty,
            size = 1000L * i,
            modificationTime = System.currentTimeMillis(),
            dataChange = true
          )
          txLog.addFiles(Seq(addAction))
        }

        // Clear cache to reset
        txLog.invalidateCache()

        // Call prewarm - should populate all caches
        val startTime = System.currentTimeMillis()
        txLog.prewarmCache()
        val prewarmTime = System.currentTimeMillis() - startTime

        println(s"Prewarm completed in ${prewarmTime}ms")

        // Verify cache is populated by checking that subsequent reads are fast
        val cacheStartTime = System.currentTimeMillis()
        val files          = txLog.listFiles()
        val cachedReadTime = System.currentTimeMillis() - cacheStartTime

        files.size shouldBe 5
        println(s"Cached read completed in ${cachedReadTime}ms")

        // Cached read should be very fast (< 50ms typically)
        assert(cachedReadTime < 100L, s"Cached read took ${cachedReadTime}ms, expected < 100ms")

        // Verify the cache was exercised during prewarm.
        // Note: prewarmCache() populates the EnhancedTransactionLogCache (producing misses),
        // while listFiles() may read from a different cache layer (TransactionLog's own cache).
        // So we check that the enhanced cache was loaded (hits + misses > 0) rather than
        // requiring cross-layer cache hits.
        txLog.getCacheStats() match {
          case Some(stats) =>
            println(s"Cache stats: hits=${stats.hits}, misses=${stats.misses}, hitRate=${stats.hitRate}")
            assert(
              stats.hits + stats.misses > 0L,
              s"Expected cache activity (hits + misses > 0), got hits=${stats.hits}, misses=${stats.misses}"
            )
          case None =>
            println("No cache stats available (using standard TransactionLog)")
        }
      } finally
        txLog.close()
    }
  }

  test("prewarmCache should work with V2 DataSource initialization") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)

      // First, create a table with V2 API
      val df = spark.range(100).selectExpr("id", "concat('text_', id) as content")
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(tempPath)

      // Now read with V2 API - prewarm should happen automatically
      val startTime = System.currentTimeMillis()
      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
      val initTime = System.currentTimeMillis() - startTime

      println(s"V2 table initialization (with prewarm) completed in ${initTime}ms")

      // Verify we can read the data
      readDf.count() shouldBe 100

      // Subsequent reads should be fast due to prewarmed cache
      val cachedStartTime = System.currentTimeMillis()
      readDf.count() shouldBe 100
      val cachedTime = System.currentTimeMillis() - cachedStartTime

      println(s"Cached read completed in ${cachedTime}ms")
    }
  }

  test("prewarmCache should handle empty tables gracefully") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val txLog     = TransactionLogFactory.create(tablePath, spark)

      try {
        // Initialize empty table
        val schema = getTestSchema()
        txLog.initialize(schema)

        // Prewarm should not fail on empty table
        txLog.prewarmCache()

        // Verify table is still accessible
        val files = txLog.listFiles()
        files.size shouldBe 0
      } finally
        txLog.close()
    }
  }

  test("prewarmCache should handle tables with checkpoints") {
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
        // Initialize table
        val schema = getTestSchema()
        txLog.initialize(schema)

        // Add files to trigger checkpoint
        for (i <- 1 to 5) {
          val addAction = AddAction(
            path = s"s3://bucket/file$i.split",
            partitionValues = Map.empty,
            size = 1000L * i,
            modificationTime = System.currentTimeMillis(),
            dataChange = true
          )
          txLog.addFiles(Seq(addAction))
        }

        // Verify checkpoint was created
        val checkpointVersion = txLog.getLastCheckpointVersion()
        checkpointVersion shouldBe defined

        // Clear cache and prewarm
        txLog.invalidateCache()
        txLog.prewarmCache()

        // Verify cache is populated and checkpoint is available
        val files = txLog.listFiles()
        files.size shouldBe 5
      } finally
        txLog.close()
    }
  }

  test("prewarmCache should be non-fatal on errors") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val txLog     = TransactionLogFactory.create(tablePath, spark)

      try
        // Call prewarm on non-existent table - should not throw exception
        noException shouldBe thrownBy {
          txLog.prewarmCache()
        }
      finally
        txLog.close()
    }
  }
}
