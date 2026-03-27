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

package io.indextables.spark.sql

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}

import io.indextables.spark.storage.DriverSplitLocalityManager
import io.indextables.tantivy4java.split.FfiProfiler
import org.slf4j.LoggerFactory

/**
 * SQL command to enable the FFI read-path profiler on all hosts.
 *
 * Syntax: ENABLE INDEXTABLES PROFILER
 */
case class EnableFfiProfilerCommand() extends LeafRunnableCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("host_count", IntegerType, nullable = false)(),
    AttributeReference("message", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    FfiProfiler.enable() // driver host
    val hostCount = FfiProfilerExecutorHelper.broadcastToHosts(sparkSession.sparkContext, enable = true)
    Seq(Row("enabled", hostCount,
      s"FFI profiler enabled on driver + $hostCount executor host(s) (counters auto-reset)"))
  }
}

/**
 * SQL command to disable the FFI read-path profiler on all hosts.
 *
 * Syntax: DISABLE INDEXTABLES PROFILER
 */
case class DisableFfiProfilerCommand() extends LeafRunnableCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("host_count", IntegerType, nullable = false)(),
    AttributeReference("message", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    FfiProfiler.disable() // driver host
    val hostCount = FfiProfilerExecutorHelper.broadcastToHosts(sparkSession.sparkContext, enable = false)
    Seq(Row("disabled", hostCount,
      s"FFI profiler disabled on driver + $hostCount executor host(s) (counters preserved)"))
  }
}

/**
 * SQL command to read FFI profiler counters from all hosts as a DataFrame.
 *
 * Syntax:
 *   DESCRIBE INDEXTABLES PROFILER        -- section timings
 *   DESCRIBE INDEXTABLES PROFILER CACHE  -- cache hit/miss counters
 */
case class DescribeFfiProfilerCommand(cacheOnly: Boolean = false) extends LeafRunnableCommand {

  override val output: Seq[Attribute] =
    if (cacheOnly) FfiProfilerCommands.cacheSchema else FfiProfilerCommands.sectionSchema

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sc = sparkSession.sparkContext
    if (cacheOnly)
      FfiProfilerCommands.buildCacheRows(FfiProfilerExecutorHelper.collectCacheCounters(sc, reset = false))
    else
      FfiProfilerCommands.buildSectionRows(FfiProfilerExecutorHelper.collectSectionCounters(sc, reset = false))
  }
}

/**
 * SQL command to read and reset FFI profiler counters from all hosts.
 *
 * Reset is atomic per-host (native CAS), but not cluster-atomic. Events recorded on executors
 * between their reset and the driver reset are not captured in the returned snapshot.
 *
 * Syntax:
 *   RESET INDEXTABLES PROFILER        -- section timings
 *   RESET INDEXTABLES PROFILER CACHE  -- cache hit/miss counters
 */
case class ResetFfiProfilerCommand(cacheOnly: Boolean = false) extends LeafRunnableCommand {

  override val output: Seq[Attribute] =
    if (cacheOnly) FfiProfilerCommands.cacheSchema else FfiProfilerCommands.sectionSchema

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sc = sparkSession.sparkContext
    if (cacheOnly)
      FfiProfilerCommands.buildCacheRows(FfiProfilerExecutorHelper.collectCacheCounters(sc, reset = true))
    else
      FfiProfilerCommands.buildSectionRows(FfiProfilerExecutorHelper.collectSectionCounters(sc, reset = true))
  }
}

/** Shared schemas and row-building helpers for profiler commands. */
private[sql] object FfiProfilerCommands {

  val sectionSchema: Seq[Attribute] = Seq(
    AttributeReference("section", StringType, nullable = false)(),
    AttributeReference("category", StringType, nullable = false)(),
    AttributeReference("calls", LongType, nullable = false)(),
    AttributeReference("total_ms", DoubleType, nullable = false)(),
    AttributeReference("avg_us", DoubleType, nullable = false)(),
    AttributeReference("min_us", DoubleType, nullable = false)(),
    AttributeReference("max_us", DoubleType, nullable = false)()
  )

  val cacheSchema: Seq[Attribute] = Seq(
    AttributeReference("cache", StringType, nullable = false)(),
    AttributeReference("hits", LongType, nullable = false)(),
    AttributeReference("misses", LongType, nullable = false)(),
    AttributeReference("hit_rate", DoubleType, nullable = true)()
  )

  // Serializable representation: (count, totalNanos, minNanos, maxNanos)
  private[sql] type SectionData = (Long, Long, Long, Long)

  def buildSectionRows(sections: Map[String, SectionData]): Seq[Row] =
    sections.toSeq
      .filter { case (_, (count, _, _, _)) => count > 0 }
      .sortBy { case (name, _) => name }
      .map { case (name, (count, totalNanos, minNanos, maxNanos)) =>
        val totalMs = totalNanos / 1000000.0
        val avgUs   = if (count > 0) totalNanos.toDouble / count / 1000.0 else 0.0
        val minUs   = minNanos / 1000.0
        val maxUs   = maxNanos / 1000.0
        Row(name, categoryFor(name), count, totalMs, avgUs, minUs, maxUs)
      }

  def buildCacheRows(counters: Map[String, Long]): Seq[Row] = {
    // Derive cache names from actual keys (strip _hit/_miss suffixes) so new caches
    // added in tantivy4java are automatically picked up without code changes.
    val cacheNames = counters.keys
      .map(k => k.replaceAll("_(hit|miss)$", ""))
      .toSeq.distinct.sorted

    cacheNames.flatMap { name =>
      val hits   = counters.getOrElse(s"${name}_hit", 0L)
      val misses = counters.getOrElse(s"${name}_miss", 0L)
      if (hits == 0 && misses == 0) None
      else {
        val total = hits + misses
        val hitRate: java.lang.Double = if (total > 0) hits.toDouble / total else null
        Some(Row(name, hits, misses, hitRate))
      }
    }
  }

  private def categoryFor(section: String): String = section match {
    case s if s.startsWith("agg_")        => "aggregation"
    case s if s.startsWith("doc_batch_")  => "doc_retrieval"
    case s if s.startsWith("pq_doc_")     => "pq_companion_doc"
    case s if s.startsWith("single_doc_") => "single_doc"
    case s if s.startsWith("pc_hash_")    => "pq_companion_hash"
    case s if s.startsWith("pc_")         => "pq_companion_transcode"
    case s if s.startsWith("stream_")     => "streaming"
    case s if s.startsWith("arrow_ffi_")  => "arrow_ffi"
    case "prewarm_components"             => "prewarm"
    case _                                => "search"
  }
}

/**
 * Helper for running profiler operations on each unique executor host.
 *
 * Uses `DriverSplitLocalityManager.getAvailableHosts` for host discovery and
 * `sc.makeRDD` with preferred locations — the same pattern as `PrewarmCacheCommand`.
 * Since profiler counters are global per host, one collect per unique host is correct.
 */
private[sql] object FfiProfilerExecutorHelper {

  private val logger = LoggerFactory.getLogger(getClass)

  import FfiProfilerCommands.SectionData

  /**
   * Run a function once on each unique executor host using preferred locations.
   * In local mode (no executors), returns empty — caller handles driver separately.
   *
   * Note: preferred locations are hints, not guarantees. Under heavy load, Spark may
   * schedule a task on a different host. This is best-effort, same as PrewarmCacheCommand.
   */
  private def runOnEachHost[T: ClassTag](
    sc: SparkContext,
    fn: () => T
  ): Seq[T] = {
    if (sc.isLocal) return Seq.empty

    val hosts = DriverSplitLocalityManager.getAvailableHosts(sc)
    if (hosts.isEmpty) return Seq.empty

    val hostsWithLocations: Seq[(String, Seq[String])] =
      hosts.toSeq.map(host => (host, Seq(host)))

    logger.debug(s"Running profiler operation on ${hosts.size} executor host(s): ${hosts.mkString(", ")}")

    sc.makeRDD(hostsWithLocations)
      .map(_ => fn())
      .collect()
      .toSeq
  }

  /** Broadcast enable/disable to all executor hosts. Returns number of hosts reached. */
  def broadcastToHosts(sc: SparkContext, enable: Boolean): Int = {
    val results = runOnEachHost(sc, () => {
      if (enable) FfiProfiler.enable() else FfiProfiler.disable()
    })
    results.size
  }

  /** Collect and merge section counters from all hosts (executors + driver). */
  def collectSectionCounters(sc: SparkContext, reset: Boolean): Map[String, SectionData] = {
    val hostResults = runOnEachHost[Map[String, SectionData]](sc, () => {
      val entries = if (reset) FfiProfiler.reset() else FfiProfiler.snapshot()
      entries.asScala.map { case (name, pe) =>
        name -> (pe.getCount, pe.getTotalNanos, pe.getMinNanos, pe.getMaxNanos)
      }.toMap
    })

    // Driver-host counters
    val driverEntries = if (reset) FfiProfiler.reset() else FfiProfiler.snapshot()
    val driverMap = driverEntries.asScala.map { case (name, pe) =>
      name -> (pe.getCount, pe.getTotalNanos, pe.getMinNanos, pe.getMaxNanos)
    }.toMap

    mergeProfiles(hostResults :+ driverMap)
  }

  /** Collect and merge cache counters from all hosts (executors + driver). */
  def collectCacheCounters(sc: SparkContext, reset: Boolean): Map[String, Long] = {
    val hostResults = runOnEachHost[Map[String, Long]](sc, () => {
      val counters = if (reset) FfiProfiler.resetCacheCounters() else FfiProfiler.cacheCounters()
      counters.asScala.map { case (k, v) => k -> v.longValue() }.toMap
    })

    // Driver-host counters
    val driverCounters = if (reset) FfiProfiler.resetCacheCounters() else FfiProfiler.cacheCounters()
    val driverMap = driverCounters.asScala.map { case (k, v) => k -> v.longValue() }.toMap

    val allMaps = hostResults :+ driverMap
    allMaps.flatten.groupBy(_._1).map { case (key, entries) =>
      key -> entries.map(_._2).sum
    }
  }

  private def mergeProfiles(profiles: Seq[Map[String, SectionData]]): Map[String, SectionData] =
    profiles.flatten.groupBy(_._1).map { case (section, entries) =>
      val values = entries.map(_._2)
      section -> (
        values.map(_._1).sum,       // sum counts
        values.map(_._2).sum,       // sum totalNanos
        values.map(_._3).min,       // min of mins
        values.map(_._4).max        // max of maxes
      )
    }
}
