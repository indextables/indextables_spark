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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.sources.Filter

import com.google.common.cache.{Cache, CacheBuilder}
import org.slf4j.LoggerFactory

/**
 * Cache for partition filter evaluation results. Reduces redundant evaluations by caching the result of filter
 * evaluations against partition values.
 *
 * Uses Guava's Cache with LRU eviction for efficient memory management.
 *
 * Key: Combined hash of filter set and partition values Value: Boolean evaluation result
 *
 * This cache dramatically improves performance when:
 *   - Multiple files share the same partition values
 *   - The same filters are evaluated repeatedly
 */
object PartitionFilterCache {

  private val logger = LoggerFactory.getLogger(PartitionFilterCache.getClass)

  /** Typed cache key to avoid hash collisions from Long packing. */
  private case class FilterCacheKey(filterStrings: Seq[String], partitionValues: Map[String, String])

  // Maximum cache size to prevent unbounded memory growth
  private val MAX_CACHE_SIZE = 100000L

  // Guava cache with LRU eviction
  private val cache: Cache[FilterCacheKey, java.lang.Boolean] = CacheBuilder
    .newBuilder()
    .maximumSize(MAX_CACHE_SIZE)
    .recordStats()
    .build[FilterCacheKey, java.lang.Boolean]()

  // Statistics (supplementary to Guava stats)
  private val hitCount  = new AtomicLong(0)
  private val missCount = new AtomicLong(0)

  /**
   * Get cached evaluation result or compute and cache it.
   *
   * @param filters
   *   The filters being evaluated
   * @param partitionValues
   *   The partition values to evaluate against
   * @param compute
   *   Lazy computation of the result if not cached
   * @return
   *   The evaluation result
   */
  def getOrCompute(
    filters: Array[Filter],
    partitionValues: Map[String, String],
    compute: => Boolean
  ): Boolean = {
    val cacheKey = FilterCacheKey(filters.map(_.toString).sorted.toSeq, partitionValues)

    val cached = cache.getIfPresent(cacheKey)
    if (cached != null) {
      hitCount.incrementAndGet()
      cached.booleanValue()
    } else {
      missCount.incrementAndGet()
      val result = compute
      cache.put(cacheKey, java.lang.Boolean.valueOf(result))
      result
    }
  }

  /** Invalidate all cached results. Should be called when the file list changes. */
  def invalidate(): Unit = {
    cache.invalidateAll()
    hitCount.set(0)
    missCount.set(0)
    logger.debug("Invalidated PartitionFilterCache")
  }

  /**
   * Get cache statistics for monitoring.
   *
   * @return
   *   Tuple of (hits, misses, hitRate)
   */
  def getStats(): (Long, Long, Double) = {
    val hits    = hitCount.get()
    val misses  = missCount.get()
    val total   = hits + misses
    val hitRate = if (total > 0) hits.toDouble / total else 0.0
    (hits, misses, hitRate)
  }

  /** Get current cache size. */
  def size(): Int = cache.size().toInt

  /** Reset statistics without clearing cache. */
  def resetStats(): Unit = {
    hitCount.set(0)
    missCount.set(0)
  }

  /** Get detailed cache statistics from Guava. */
  def getDetailedStats(): String = cache.stats().toString
}
