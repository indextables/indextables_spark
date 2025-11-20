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

package io.indextables.spark.purge

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap

import org.slf4j.LoggerFactory

/**
 * Per-session transaction counter for purge-on-write feature.
 *
 * Tracks the number of write transactions completed on each table path within a Spark session. When the count reaches
 * the configured threshold, purge-on-write is triggered and the counter resets.
 *
 * This is a session-scoped singleton - each Spark session maintains its own counter state. Counters are NOT persisted
 * across sessions or JVM restarts.
 *
 * Thread-safe implementation using ConcurrentHashMap and AtomicInteger.
 */
object PurgeOnWriteTransactionCounter {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Per-table transaction counters. Key: table path (normalized) Value: atomic counter for number of transactions
   * completed
   */
  private val counters = new ConcurrentHashMap[String, AtomicInteger]()

  /**
   * Increment the transaction count for a table path.
   *
   * @param tablePath
   *   The table path (will be normalized)
   * @return
   *   The new transaction count for this table
   */
  def incrementAndGet(tablePath: String): Int = {
    val normalizedPath = normalizePath(tablePath)
    val counter        = counters.computeIfAbsent(normalizedPath, _ => new AtomicInteger(0))
    val newCount       = counter.incrementAndGet()
    logger.debug(s"Incremented transaction count for $normalizedPath to $newCount")
    newCount
  }

  /**
   * Get the current transaction count for a table path.
   *
   * @param tablePath
   *   The table path (will be normalized)
   * @return
   *   The current transaction count, or 0 if never written
   */
  def get(tablePath: String): Int = {
    val normalizedPath = normalizePath(tablePath)
    val counter        = counters.get(normalizedPath)
    if (counter != null) counter.get() else 0
  }

  /**
   * Reset the transaction count for a table path to zero. Called after purge-on-write completes.
   *
   * @param tablePath
   *   The table path (will be normalized)
   */
  def reset(tablePath: String): Unit = {
    val normalizedPath = normalizePath(tablePath)
    val counter        = counters.get(normalizedPath)
    if (counter != null) {
      val oldCount = counter.getAndSet(0)
      logger.debug(s"Reset transaction count for $normalizedPath from $oldCount to 0")
    }
  }

  /** Clear all transaction counters. Used for testing or session cleanup. */
  def clearAll(): Unit = {
    logger.debug(s"Clearing all transaction counters (${counters.size()} tables)")
    counters.clear()
  }

  /** Normalize table path for consistent lookup. Converts to canonical form and removes trailing slashes. */
  private def normalizePath(path: String): String = {
    // Remove trailing slashes
    val trimmed = path.replaceAll("/+$", "")

    // For cloud paths (s3://, azure://, etc.), return as-is after trimming
    if (
      trimmed.startsWith("s3://") || trimmed.startsWith("s3a://") ||
      trimmed.startsWith("azure://") || trimmed.startsWith("wasbs://") ||
      trimmed.startsWith("abfss://") || trimmed.startsWith("gs://") ||
      trimmed.startsWith("hdfs://")
    ) {
      trimmed
    } else {
      // For local paths, try to get canonical form
      try
        new java.io.File(trimmed).getCanonicalPath
      catch {
        case _: Exception => trimmed
      }
    }
  }

  /** Get statistics about tracked tables for debugging. */
  def getStats(): Map[String, Int] = {
    import scala.jdk.CollectionConverters._
    counters.asScala.map { case (k, v) => k -> v.get() }.toMap
  }
}
