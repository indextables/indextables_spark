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

import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import com.google.common.cache.{Cache, CacheBuilder}

import org.slf4j.LoggerFactory

/**
 * JVM-wide cache for transaction files on executors.
 * Similar to SplitCacheManager but for transaction log files.
 *
 * This cache is executor-local, meaning each executor maintains its own cache
 * of transaction file contents. This avoids redundant S3 reads when executors
 * process multiple queries or partitions that reference the same transaction files.
 *
 * Thread-safe singleton implementation using Guava Cache.
 */
object TransactionFileCache {

  private val logger = LoggerFactory.getLogger(getClass)

  // Default configuration
  private val DEFAULT_MAX_SIZE = 1000L // Cache up to 1000 transaction files per executor
  private val DEFAULT_TTL_SECONDS = 300L // 5 minutes TTL

  // Guava cache for transaction file actions
  // Key: "tablePath/transactionFile" (e.g., "s3://bucket/table/_transaction_log/000000000000000123.json")
  // Value: Seq[Action] parsed from the transaction file
  private val cache: Cache[String, Seq[Action]] = CacheBuilder
    .newBuilder()
    .maximumSize(DEFAULT_MAX_SIZE)
    .expireAfterWrite(DEFAULT_TTL_SECONDS, TimeUnit.SECONDS)
    .recordStats() // Enable statistics tracking
    .build[String, Seq[Action]]()

  /**
   * Get cached actions for a transaction file.
   *
   * @param key Cache key (typically "tablePath/_transaction_log/version.json")
   * @return Some(actions) if cached, None otherwise
   */
  def get(key: String): Option[Seq[Action]] = {
    val result = Option(cache.getIfPresent(key))
    if (result.isDefined) {
      logger.debug(s"Cache hit for transaction file: $key")
    } else {
      logger.debug(s"Cache miss for transaction file: $key")
    }
    result
  }

  /**
   * Put actions into cache for a transaction file.
   *
   * @param key Cache key
   * @param actions Parsed actions from transaction file
   */
  def put(key: String, actions: Seq[Action]): Unit = {
    logger.debug(s"Caching transaction file: $key (${actions.length} actions)")
    cache.put(key, actions)
  }

  /**
   * Invalidate all cache entries matching a table path pattern.
   * Used when transaction log is updated and caches need to be refreshed.
   *
   * @param tablePathPattern Table path prefix to match (e.g., "s3://bucket/table")
   */
  def invalidate(tablePathPattern: String): Unit = {
    if (tablePathPattern.isEmpty || tablePathPattern == "*") {
      // Invalidate all entries
      val entriesBefore = cache.size()
      cache.invalidateAll()
      logger.info(s"Invalidated all transaction file cache entries (${entriesBefore} entries)")
    } else {
      // Invalidate entries matching the pattern
      val keysToInvalidate = cache.asMap().keySet().asScala
        .filter(_.startsWith(tablePathPattern))
        .toSeq

      keysToInvalidate.foreach(cache.invalidate)

      logger.info(s"Invalidated ${keysToInvalidate.length} transaction file cache entries for pattern: $tablePathPattern")
    }
  }

  /**
   * Get current cache size (number of entries).
   *
   * @return Number of cached transaction files
   */
  def size(): Long = cache.size()

  /**
   * Get cache statistics as a formatted string.
   * Includes hit rate, request count, eviction count, etc.
   *
   * @return Cache statistics string
   */
  def stats(): String = {
    val cacheStats = cache.stats()
    s"TransactionFileCache(size=${size()}, ${cacheStats.toString})"
  }

  /**
   * Get detailed cache statistics.
   * Useful for monitoring and debugging cache effectiveness.
   *
   * @return Cache statistics object
   */
  def getStats(): com.google.common.cache.CacheStats = cache.stats()

  /**
   * Clear all entries from the cache.
   * Used primarily for testing.
   */
  def clear(): Unit = {
    val entriesBefore = cache.size()
    cache.invalidateAll()
    logger.debug(s"Cleared transaction file cache (${entriesBefore} entries)")
  }

  /**
   * Get cache hit rate (0.0 to 1.0).
   *
   * @return Hit rate as a double, or 0.0 if no requests have been made
   */
  def hitRate(): Double = {
    val stats = cache.stats()
    val totalRequests = stats.requestCount()
    if (totalRequests > 0) {
      stats.hitCount().toDouble / totalRequests
    } else {
      0.0
    }
  }
}
