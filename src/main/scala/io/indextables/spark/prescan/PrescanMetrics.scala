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

package io.indextables.spark.prescan

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.util.AccumulatorV2

/**
 * Metrics collected during prescan filtering operations.
 *
 * @param splitsBeforePrescan Number of splits before prescan filtering
 * @param splitsAfterPrescan Number of splits after prescan filtering (splits that could have results)
 * @param prescanTimeMs Total time spent in prescan filtering (milliseconds)
 * @param cacheHits Number of splits where prescan data was already cached
 * @param cacheMisses Number of splits where prescan data needed to be fetched
 * @param errors Number of splits that encountered errors during prescan (conservatively included)
 */
case class PrescanMetricsData(
  splitsBeforePrescan: Long = 0,
  splitsAfterPrescan: Long = 0,
  prescanTimeMs: Long = 0,
  cacheHits: Long = 0,
  cacheMisses: Long = 0,
  errors: Long = 0
) {

  /**
   * Merge two metrics instances (for accumulator aggregation).
   */
  def merge(other: PrescanMetricsData): PrescanMetricsData = PrescanMetricsData(
    splitsBeforePrescan + other.splitsBeforePrescan,
    splitsAfterPrescan + other.splitsAfterPrescan,
    prescanTimeMs + other.prescanTimeMs,
    cacheHits + other.cacheHits,
    cacheMisses + other.cacheMisses,
    errors + other.errors
  )

  /**
   * Calculate the split elimination rate (0.0 to 1.0).
   * Higher is better - means more splits were eliminated.
   */
  def eliminationRate: Double =
    if (splitsBeforePrescan > 0)
      (splitsBeforePrescan - splitsAfterPrescan).toDouble / splitsBeforePrescan
    else 0.0

  /**
   * Calculate the cache hit rate (0.0 to 1.0).
   * Higher is better - means more data was already cached.
   */
  def cacheHitRate: Double =
    if (cacheHits + cacheMisses > 0)
      cacheHits.toDouble / (cacheHits + cacheMisses)
    else 0.0

  /**
   * Number of splits eliminated by prescan filtering.
   */
  def splitsEliminated: Long = splitsBeforePrescan - splitsAfterPrescan

  /**
   * Human-readable summary of the metrics.
   */
  def summary: String = {
    val elimPct = (eliminationRate * 100).toInt
    val cacheHitPct = (cacheHitRate * 100).toInt
    s"Prescan: eliminated $splitsEliminated of $splitsBeforePrescan splits ($elimPct%), " +
      s"${prescanTimeMs}ms, cache hit rate $cacheHitPct%, errors $errors"
  }

  override def toString: String = summary
}

object PrescanMetricsData {
  /** Empty metrics instance (identity for merge). */
  val empty: PrescanMetricsData = PrescanMetricsData()
}

/**
 * Spark accumulator for collecting prescan metrics.
 *
 * This accumulator is registered with SparkContext and aggregates metrics
 * from prescan filtering operations. Since prescan runs on the driver,
 * the accumulator is primarily used for tracking and reporting rather
 * than distributed aggregation.
 */
class PrescanMetricsAccumulator extends AccumulatorV2[PrescanMetricsData, PrescanMetricsData] {

  private var _metrics: PrescanMetricsData = PrescanMetricsData.empty

  override def isZero: Boolean = _metrics == PrescanMetricsData.empty

  override def copy(): PrescanMetricsAccumulator = {
    val acc = new PrescanMetricsAccumulator
    acc._metrics = _metrics
    acc
  }

  override def reset(): Unit = {
    _metrics = PrescanMetricsData.empty
  }

  override def add(v: PrescanMetricsData): Unit = {
    _metrics = _metrics.merge(v)
  }

  override def merge(other: AccumulatorV2[PrescanMetricsData, PrescanMetricsData]): Unit = {
    _metrics = _metrics.merge(other.value)
  }

  override def value: PrescanMetricsData = _metrics

  /**
   * Get a snapshot of current metrics without modifying the accumulator.
   */
  def snapshot: PrescanMetricsData = _metrics.copy()
}

/**
 * Global registry for prescan metrics per table path.
 *
 * Allows tracking and retrieving prescan metrics for specific tables.
 * This is useful for:
 * - Monitoring prescan effectiveness per table
 * - Debugging prescan behavior
 * - Performance analysis and tuning
 */
object PrescanMetricsRegistry {

  private val registry = new ConcurrentHashMap[String, PrescanMetricsAccumulator]()

  /**
   * Register a metrics accumulator for a table path.
   *
   * @param tablePath Table path to register
   * @param accumulator Accumulator instance
   */
  def register(tablePath: String, accumulator: PrescanMetricsAccumulator): Unit = {
    registry.put(tablePath, accumulator)
  }

  /**
   * Get the metrics accumulator for a table path.
   *
   * @param tablePath Table path to look up
   * @return Optional accumulator if registered
   */
  def get(tablePath: String): Option[PrescanMetricsAccumulator] =
    Option(registry.get(tablePath))

  /**
   * Get or create a metrics accumulator for a table path.
   *
   * @param tablePath Table path
   * @return Existing or new accumulator
   */
  def getOrCreate(tablePath: String): PrescanMetricsAccumulator =
    registry.computeIfAbsent(tablePath, _ => new PrescanMetricsAccumulator)

  /**
   * Get current metrics for a table path.
   *
   * @param tablePath Table path to look up
   * @return Optional metrics data if accumulator exists
   */
  def getMetrics(tablePath: String): Option[PrescanMetricsData] =
    get(tablePath).map(_.value)

  /**
   * Remove the metrics accumulator for a table path.
   *
   * @param tablePath Table path to remove
   */
  def remove(tablePath: String): Unit = {
    registry.remove(tablePath)
  }

  /**
   * Clear all registered metrics accumulators.
   */
  def clear(): Unit = {
    registry.clear()
  }

  /**
   * Get all registered table paths.
   *
   * @return Set of registered table paths
   */
  def registeredTables: Set[String] = {
    import scala.collection.JavaConverters._
    registry.keySet().asScala.toSet
  }

  /**
   * Get a summary of all registered metrics.
   *
   * @return Map of table path to metrics summary
   */
  def allMetricsSummary: Map[String, String] = {
    import scala.collection.JavaConverters._
    registry.asScala.map { case (path, acc) =>
      path -> acc.value.summary
    }.toMap
  }
}
