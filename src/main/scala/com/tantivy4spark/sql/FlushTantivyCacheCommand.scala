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

package com.tantivy4spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{StringType, LongType}
import com.tantivy4spark.storage.{GlobalSplitCacheManager, SplitLocationRegistry}
import org.slf4j.LoggerFactory

/**
 * SQL command to flush Tantivy4Spark searcher cache.
 *
 * Syntax: FLUSH TANTIVY4SPARK SEARCHER CACHE
 *
 * This command:
 *   1. Flushes all JVM-wide split cache managers 2. Clears split location registry 3. Calls tantivy4java's cache flush
 *      functionality
 */
case class FlushTantivyCacheCommand() extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[FlushTantivyCacheCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("cache_type", StringType, nullable = false)(),
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("cleared_entries", LongType, nullable = false)(),
    AttributeReference("message", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info("Executing FLUSH TANTIVY4SPARK SEARCHER CACHE command")

    val results = scala.collection.mutable.ArrayBuffer[Row]()

    try {
      // 1. Flush GlobalSplitCacheManager instances
      val splitCacheResult = GlobalSplitCacheManager.flushAllCaches()
      results += Row(
        "split_cache",
        "success",
        splitCacheResult.flushedManagers.toLong,
        s"Flushed ${splitCacheResult.flushedManagers} split cache managers"
      )

      // 2. Clear SplitLocationRegistry
      val locationResult = SplitLocationRegistry.clearAllLocations()
      results += Row(
        "location_registry",
        "success",
        locationResult.clearedEntries.toLong,
        s"Cleared ${locationResult.clearedEntries} split location entries"
      )

      // 3. Flush tantivy4java searcher cache
      val tantivyResult = flushTantivyJavaCache()
      results += Row(
        "tantivy_java_cache",
        if (tantivyResult.success) "success" else "failed",
        tantivyResult.flushedCaches.toLong,
        tantivyResult.message
      )

      logger.info(s"FLUSH TANTIVY4SPARK SEARCHER CACHE completed successfully. Results: ${results.length} operations")

    } catch {
      case ex: Exception =>
        logger.error("Error executing FLUSH TANTIVY4SPARK SEARCHER CACHE", ex)
        results += Row(
          "error",
          "failed",
          0L,
          s"Cache flush failed: ${ex.getMessage}"
        )
    }

    results.toSeq
  }

  private def flushTantivyJavaCache(): TantivyFlushResult =
    // Flush tantivy4java caches by closing our SplitCacheManager instances
    // This is the proper way to flush native resources in tantivy4java
    try {
      val flushedCount = flushViaSplitCacheManagers()
      TantivyFlushResult(
        success = true,
        flushedCaches = flushedCount,
        message = s"Flushed $flushedCount tantivy4java native caches via SplitCacheManager close operations"
      )
    } catch {
      case ex: Exception =>
        logger.warn(s"Failed to flush tantivy4java native caches: ${ex.getMessage}")
        TantivyFlushResult(
          success = false,
          flushedCaches = 0,
          message = s"tantivy4java cache flush failed: ${ex.getMessage}"
        )
    }

  private def flushViaSplitCacheManagers(): Int = {
    // This method flushes tantivy4java caches by going through our SplitCacheManager instances
    // and calling their close() methods, which releases native tantivy4java resources
    var flushedCount = 0

    // The GlobalSplitCacheManager.flushAllCaches() will close all the SplitCacheManager instances
    // When each SplitCacheManager closes, it releases its native tantivy4java resources (searchers, readers, etc.)
    val result = GlobalSplitCacheManager.flushAllCaches()
    flushedCount += result.flushedManagers

    // Additionally, try to call any global tantivy4java flush if available via reflection
    try {
      // Some versions of tantivy4java may have a global flush method
      val clazz  = Class.forName("com.tantivy4java.SplitCacheManager")
      val method = clazz.getDeclaredMethod("clearGlobalCache")
      method.invoke(null)
      flushedCount += 1
    } catch {
      case _: Exception =>
      // Method doesn't exist or failed - that's okay, we already flushed via SplitCacheManager.close()
    }

    flushedCount
  }
}

case class TantivyFlushResult(
  success: Boolean,
  flushedCaches: Int,
  message: String)
