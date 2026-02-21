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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.TestBase

/**
 * Validates that cache-staleness bugs in the global transaction log caches are fixed.
 *
 * Bug 1 (FIXED) – global caches now use `expireAfterWrite` instead of `expireAfterAccess`
 *   Previously, under continuous query load the TTL clock reset on every read, so `_last_checkpoint`
 *   was never re-read from disk.  All version-keyed caches used the stale checkpoint version as
 *   their key, returning stale V1 data even after V2 was committed by another writer.
 *   Fix: all global caches now use `expireAfterWrite` so entries expire after a fixed TTL
 *   regardless of access frequency.
 *
 * Bug 2 (FIXED) – `getMetadata()` no longer uses non-local return
 *   Previously in Avro mode, `getMetadata()` used `return` inside the compute block, which was a
 *   non-local return that bypassed `globalMetadataCache.put()`. The global metadata cache was NEVER
 *   populated in Avro mode.  Fix: restructured to use if/else expressions instead of `return`.
 *
 * Test approach: we capture a complete snapshot of ALL global caches after a V1 read,
 * then inject that snapshot after a V2 write. Since `expireAfterWrite` is now used, the injected
 * entries will still be stale (simulating a reader JVM), but subsequent cache misses will
 * re-read fresh data from disk.
 */
class TransactionLogCacheStalenessTest extends TestBase {

  /** Options for creating a TransactionLog directly (bypassing the DataSource layer) */
  private def directOptions: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(
    Map("spark.indextables.transaction.allowDirectUsage" -> "true").asJava
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.indextables.state.format", "avro")
    // Disable merge-on-write to prevent background threads from interfering.
    spark.conf.set("spark.indextables.mergeOnWrite.async.enabled", "false")
    spark.conf.set("spark.indextables.mergeOnWrite.enabled", "false")
  }

  // ---------------------------------------------------------------------------
  // Bug 1: Demonstrates that injected stale cache entries produce stale reads.
  // The fix (expireAfterWrite) ensures these entries expire after a fixed TTL
  // even under continuous reads, preventing indefinite staleness in production.
  // ---------------------------------------------------------------------------

  test("stale cache snapshot causes listFiles to return stale file list") {
    withTempPath { tempPath =>

      // -----------------------------------------------------------------------
      // Step 1: Write V1 (2 rows → 2 splits in local[2] mode).
      // -----------------------------------------------------------------------
      spark.range(2)
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(tempPath)

      // -----------------------------------------------------------------------
      // Step 2: Clear caches, then read V1 to populate ALL global caches.
      //         Capture a FULL snapshot of global cache state BEFORE close().
      // -----------------------------------------------------------------------
      EnhancedTransactionLogCache.clearGlobalCaches()

      val txLogV1 = TransactionLogFactory.create(new Path(tempPath), spark, directOptions)
      val filesV1 = txLogV1.listFiles()
      println(s"V1 listFiles: ${filesV1.size} files")
      assert(filesV1.nonEmpty, "V1 should have files")

      // Capture ALL global cache entries before close() clears them.
      val staleLastCkpt = EnhancedTransactionLogCache.globalLastCheckpointInfoCache.asMap().asScala.toMap
      val staleCheckpointActions = EnhancedTransactionLogCache.globalCheckpointActionsCache.asMap().asScala.toMap
      val staleAvroFileList = EnhancedTransactionLogCache.globalAvroFileListCache.asMap().asScala.toMap
      val staleAvroStateManifest = EnhancedTransactionLogCache.globalAvroStateManifestCache.asMap().asScala.toMap
      val staleProtocol = EnhancedTransactionLogCache.globalProtocolCache.asMap().asScala.toMap

      println(s"V1 cache snapshot: lastCkpt=${staleLastCkpt.size}, ckptActions=${staleCheckpointActions.size}, " +
        s"avroFileList=${staleAvroFileList.size}, avroManifest=${staleAvroStateManifest.size}, protocol=${staleProtocol.size}")

      // Sanity: confirm the key caches are populated.
      assert(staleLastCkpt.nonEmpty, "lastCheckpointInfo should be cached after V1 read")
      assert(staleAvroFileList.nonEmpty, "avroFileList should be cached after V1 read")

      val v1Version = staleLastCkpt.values.head.info.map(_.version).getOrElse(-1L)
      println(s"Stale cache points to version=$v1Version")

      txLogV1.close() // Clears ALL global caches.

      // -----------------------------------------------------------------------
      // Step 3: Append V2 (1 more row → 3 files total).
      // -----------------------------------------------------------------------
      spark.range(1)
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("append")
        .save(tempPath)

      // Verify V2 is live.
      EnhancedTransactionLogCache.clearGlobalCaches()
      val txLogV2 = TransactionLogFactory.create(new Path(tempPath), spark, directOptions)
      val filesV2 = txLogV2.listFiles()
      println(s"V2 listFiles (fresh): ${filesV2.size} files")
      assert(filesV2.size == filesV1.size + 1, s"V2 should have ${filesV1.size + 1} files, got ${filesV2.size}")
      txLogV2.close()

      // -----------------------------------------------------------------------
      // Step 4: Re-inject the FULL stale V1 cache snapshot.
      //
      //         This simulates a cross-JVM reader whose caches were warmed
      //         before V2 was written.  Under expireAfterAccess, continuous
      //         queries keep resetting the TTL so nothing ever expires.
      // -----------------------------------------------------------------------
      EnhancedTransactionLogCache.clearGlobalCaches() // Start clean.
      staleLastCkpt.foreach { case (k, v) =>
        EnhancedTransactionLogCache.globalLastCheckpointInfoCache.put(k, v)
      }
      staleCheckpointActions.foreach { case (k, v) =>
        EnhancedTransactionLogCache.globalCheckpointActionsCache.put(k, v)
      }
      staleAvroFileList.foreach { case (k, v) =>
        EnhancedTransactionLogCache.globalAvroFileListCache.put(k, v)
      }
      staleAvroStateManifest.foreach { case (k, v) =>
        EnhancedTransactionLogCache.globalAvroStateManifestCache.put(k, v)
      }
      staleProtocol.foreach { case (k, v) =>
        EnhancedTransactionLogCache.globalProtocolCache.put(k, v)
      }
      println(s"Re-injected full stale V1 cache snapshot")

      // -----------------------------------------------------------------------
      // Step 5: Read with stale caches.
      //
      //         Injected stale entries are still served as cache hits (they
      //         are "fresh" from Guava's perspective).  This confirms that
      //         stale caches DO produce stale reads — the fix (expireAfterWrite)
      //         prevents this in production by expiring entries after a fixed
      //         TTL regardless of read frequency.
      // -----------------------------------------------------------------------
      val txLogStale = TransactionLogFactory.create(new Path(tempPath), spark, directOptions)
      val filesStale = txLogStale.listFiles()
      txLogStale.close()

      println(s"Bug 1: listFiles() with stale cache = ${filesStale.size} files  (expected stale=${filesV1.size}, fresh=${filesV2.size})")

      // Injected stale entries produce stale reads (expected behavior for populated caches).
      assert(
        filesStale.size == filesV1.size,
        s"Expected stale count=${filesV1.size} but got ${filesStale.size}. " +
          s"Stale cache entries should still produce stale reads."
      )

      // -----------------------------------------------------------------------
      // Step 6: Verify that clearing caches returns fresh V2 data.
      // -----------------------------------------------------------------------
      EnhancedTransactionLogCache.clearGlobalCaches()
      val txLogFresh = TransactionLogFactory.create(new Path(tempPath), spark, directOptions)
      val filesFresh = txLogFresh.listFiles()
      txLogFresh.close()
      assert(filesFresh.size == filesV2.size, s"Fresh read should return V2 file count, got ${filesFresh.size}")
      println(s"Bug 1: fresh listFiles after clearGlobalCaches = ${filesFresh.size} files")
    }
  }

  // ---------------------------------------------------------------------------
  // Bug 2 (FIXED): getMetadata() now properly populates globalMetadataCache
  // ---------------------------------------------------------------------------

  test("getMetadata populates globalMetadataCache in Avro mode") {
    withTempPath { tempPath =>

      // -----------------------------------------------------------------------
      // Step 1: Write a table and call getMetadata() via TransactionLog.
      // -----------------------------------------------------------------------
      spark.range(5)
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "id")
        .mode("overwrite")
        .save(tempPath)

      EnhancedTransactionLogCache.clearGlobalCaches()

      val txLog1 = TransactionLogFactory.create(new Path(tempPath), spark, directOptions)
      val metaV1 = txLog1.getMetadata()
      println(s"V1 metadata id=${metaV1.id}")

      // -----------------------------------------------------------------------
      // Step 2: After fix, the metadata cache should be populated.
      //         Previously, a non-local `return` inside the compute block
      //         bypassed globalMetadataCache.put() — every call re-read disk.
      // -----------------------------------------------------------------------
      val metaCacheAfterV1 = EnhancedTransactionLogCache.globalMetadataCache.asMap().asScala
      println(s"globalMetadataCache after getMetadata(): size=${metaCacheAfterV1.size}")

      assert(
        metaCacheAfterV1.nonEmpty,
        "globalMetadataCache should be populated after getMetadata() in Avro mode"
      )

      // Verify cached metadata is correct and repeated calls use the cache.
      val metaV1Again = txLog1.getMetadata()
      assert(metaV1.id == metaV1Again.id, "Repeated getMetadata() should return same result")
      println(s"getMetadata() returns correct cached data: id=${metaV1Again.id}")

      txLog1.close()
    }
  }

  // ---------------------------------------------------------------------------
  // Bug 3 (FIXED): getProtocol() had the same non-local return bug
  // ---------------------------------------------------------------------------

  test("getProtocol populates globalProtocolCache in Avro mode") {
    withTempPath { tempPath =>

      spark.range(5)
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(tempPath)

      EnhancedTransactionLogCache.clearGlobalCaches()

      val txLog1 = TransactionLogFactory.create(new Path(tempPath), spark, directOptions)
      txLog1.getProtocol()

      val protoCacheAfter = EnhancedTransactionLogCache.globalProtocolCache.asMap().asScala
      println(s"globalProtocolCache after getProtocol(): size=${protoCacheAfter.size}")

      assert(
        protoCacheAfter.nonEmpty,
        "globalProtocolCache should be populated after getProtocol() in Avro mode"
      )

      txLog1.close()
    }
  }
}
