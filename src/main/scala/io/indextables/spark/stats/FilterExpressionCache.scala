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

package io.indextables.spark.stats

import java.util.concurrent.atomic.AtomicLong

import com.google.common.cache.{Cache, CacheBuilder}

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import org.slf4j.LoggerFactory

/**
 * Cache for simplified filter expressions and IN filter range computations.
 *
 * Caches:
 * 1. Simplified filter arrays (expression simplification results)
 * 2. IN filter value ranges (min/max of IN list values)
 *
 * Uses Guava's Cache with LRU eviction for efficient memory management.
 */
object FilterExpressionCache {

  private val logger = LoggerFactory.getLogger(FilterExpressionCache.getClass)

  // Cache for simplified filters: key = (filters hash, schema hash)
  private val MAX_SIMPLIFIED_CACHE_SIZE = 10000L

  private val simplifiedFiltersCache: Cache[java.lang.Long, Array[Filter]] =
    CacheBuilder.newBuilder()
      .maximumSize(MAX_SIMPLIFIED_CACHE_SIZE)
      .recordStats()
      .build[java.lang.Long, Array[Filter]]()

  // Cache for IN filter ranges: key = filter hash, value = (min, max) as strings
  private val MAX_IN_RANGE_CACHE_SIZE = 50000L

  private val inRangeCache: Cache[java.lang.Integer, InFilterRange] =
    CacheBuilder.newBuilder()
      .maximumSize(MAX_IN_RANGE_CACHE_SIZE)
      .recordStats()
      .build[java.lang.Integer, InFilterRange]()

  // Statistics
  private val simplifiedHits = new AtomicLong(0)
  private val simplifiedMisses = new AtomicLong(0)
  private val inRangeHits = new AtomicLong(0)
  private val inRangeMisses = new AtomicLong(0)

  /**
   * Represents the min/max range of an IN filter's value list.
   */
  case class InFilterRange(
    minValue: Comparable[Any],
    maxValue: Comparable[Any],
    valueCount: Int
  )

  /**
   * Get or compute simplified filters.
   *
   * @param filters Original filters
   * @param schema Schema for simplification
   * @return Simplified filters
   */
  def getOrSimplify(filters: Array[Filter], schema: StructType): Array[Filter] = {
    val cacheKey: java.lang.Long = computeSimplifiedCacheKey(filters, schema)

    val cached = simplifiedFiltersCache.getIfPresent(cacheKey)
    if (cached != null) {
      simplifiedHits.incrementAndGet()
      cached
    } else {
      simplifiedMisses.incrementAndGet()
      val simplified = ExpressionSimplifier.simplify(filters, schema)
      simplifiedFiltersCache.put(cacheKey, simplified)
      simplified
    }
  }

  /**
   * Get or compute IN filter range (min/max of values in the IN list).
   *
   * @param attribute Column name
   * @param values Values in the IN list
   * @return InFilterRange with min/max values
   */
  def getOrComputeInRange(attribute: String, values: Array[Any]): Option[InFilterRange] = {
    if (values.isEmpty) return None

    val cacheKey: java.lang.Integer = computeInRangeCacheKey(attribute, values)

    val cached = inRangeCache.getIfPresent(cacheKey)
    if (cached != null) {
      inRangeHits.incrementAndGet()
      Some(cached)
    } else {
      inRangeMisses.incrementAndGet()
      val range = computeInRange(values)
      range.foreach(r => inRangeCache.put(cacheKey, r))
      range
    }
  }

  /**
   * Compute min/max from IN list values.
   */
  private def computeInRange(values: Array[Any]): Option[InFilterRange] = {
    if (values.isEmpty) return None

    // Filter out nulls and convert to comparable values
    val nonNullValues = values.filterNot(_ == null)
    if (nonNullValues.isEmpty) return None

    try {
      // Try to find min/max using natural ordering
      val sortedStrings = nonNullValues.map(_.toString).sorted

      val minValue = sortedStrings.head.asInstanceOf[Comparable[Any]]
      val maxValue = sortedStrings.last.asInstanceOf[Comparable[Any]]

      Some(InFilterRange(minValue, maxValue, nonNullValues.length))
    } catch {
      case _: Exception =>
        logger.debug(s"Could not compute IN range for values: ${values.take(5).mkString(", ")}...")
        None
    }
  }

  /**
   * Compute cache key for simplified filters.
   */
  private def computeSimplifiedCacheKey(filters: Array[Filter], schema: StructType): Long = {
    val filterHash = filters.map(_.hashCode()).sorted.foldLeft(17) { (acc, h) =>
      31 * acc + h
    }
    val schemaHash = schema.hashCode()
    (filterHash.toLong << 32) | (schemaHash.toLong & 0xFFFFFFFFL)
  }

  /**
   * Compute cache key for IN filter range.
   */
  private def computeInRangeCacheKey(attribute: String, values: Array[Any]): Int = {
    var hash = attribute.hashCode()
    values.foreach { v =>
      hash = 31 * hash + (if (v == null) 0 else v.hashCode())
    }
    hash
  }

  /**
   * Get cache statistics.
   *
   * @return (simplifiedHits, simplifiedMisses, inRangeHits, inRangeMisses)
   */
  def getStats(): (Long, Long, Long, Long) = {
    (simplifiedHits.get(), simplifiedMisses.get(), inRangeHits.get(), inRangeMisses.get())
  }

  /**
   * Get hit rates.
   *
   * @return (simplifiedHitRate, inRangeHitRate)
   */
  def getHitRates(): (Double, Double) = {
    val simplifiedTotal = simplifiedHits.get() + simplifiedMisses.get()
    val inRangeTotal = inRangeHits.get() + inRangeMisses.get()

    val simplifiedRate = if (simplifiedTotal > 0) simplifiedHits.get().toDouble / simplifiedTotal else 0.0
    val inRangeRate = if (inRangeTotal > 0) inRangeHits.get().toDouble / inRangeTotal else 0.0

    (simplifiedRate, inRangeRate)
  }

  /**
   * Get detailed Guava cache statistics.
   */
  def getDetailedStats(): String = {
    s"SimplifiedFilters: ${simplifiedFiltersCache.stats()}\n" +
    s"InFilterRanges: ${inRangeCache.stats()}"
  }

  /**
   * Get current cache sizes.
   */
  def getCacheSizes(): (Long, Long) = {
    (simplifiedFiltersCache.size(), inRangeCache.size())
  }

  /**
   * Invalidate all cached data.
   */
  def invalidateAll(): Unit = {
    simplifiedFiltersCache.invalidateAll()
    inRangeCache.invalidateAll()
    simplifiedHits.set(0)
    simplifiedMisses.set(0)
    inRangeHits.set(0)
    inRangeMisses.set(0)
    logger.debug("Invalidated FilterExpressionCache")
  }

  /**
   * Reset statistics without clearing cache.
   */
  def resetStats(): Unit = {
    simplifiedHits.set(0)
    simplifiedMisses.set(0)
    inRangeHits.set(0)
    inRangeMisses.set(0)
  }
}
