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

package io.indextables.spark.storage

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkContext

import io.indextables.tantivy4java.split.SplitSearcher.IndexComponent
import org.slf4j.LoggerFactory

/**
 * Driver-based split locality manager that maintains sticky split-to-host assignments with per-query load balancing.
 *
 * This manager replaces the broadcast-based approach (BroadcastSplitLocalityManager) with a simpler, more efficient
 * driver-side assignment strategy:
 *
 *   1. **Sticky assignments**: Once a split is assigned to a host, it stays there unless the host becomes unavailable
 *      2. **Per-query load balancing**: New splits (unassigned or host unavailable) are assigned to hosts with the
 *      fewest splits IN THE CURRENT QUERY 3. **Zero per-query overhead**: No broadcast jobs, no network collection
 *
 * The key insight is that we track historical assignments for cache locality, but balance NEW assignments based on the
 * current query's workload distribution.
 */
object DriverSplitLocalityManager {
  private val logger = LoggerFactory.getLogger(getClass)

  // Primary assignment map: splitPath -> assigned hostname
  // This is PERSISTENT across queries for sticky assignment
  private val splitAssignments = new ConcurrentHashMap[String, String]()

  // Track known hosts to detect when new executors are added
  @volatile private var knownHosts: Set[String] = Set.empty

  // Lock for batch operations
  private val lock = new ReentrantReadWriteLock()

  // Prewarm state tracking: tracks which (splitPath, hostname, components) combinations have been prewarmed
  // Key format: splitPath
  // Value: PrewarmState containing hostname and prewarmed components
  private val prewarmState = new ConcurrentHashMap[String, PrewarmSplitState]()

  // Track which hosts have had new splits assigned since last prewarm
  // Used for catch-up behavior
  private val hostsNeedingCatchUp = new ConcurrentHashMap[String, java.util.Set[String]]() // host -> splitPaths

  /**
   * Assign all splits for a query in a single batch operation. Uses per-query load balancing for new assignments.
   *
   * @param splitPaths
   *   All split paths involved in the current query
   * @param availableHosts
   *   Currently available executor hosts
   * @return
   *   Map of splitPath -> assigned host (for this query)
   */
  def assignSplitsForQuery(
    splitPaths: Seq[String],
    availableHosts: Set[String]
  ): Map[String, String] = {
    if (availableHosts.isEmpty) {
      logger.debug("No available hosts, returning empty assignments")
      return Map.empty
    }

    lock.writeLock().lock()
    try {
      // PHASE 0: Detect new hosts and rebalance if needed
      rebalanceForNewHosts(splitPaths, availableHosts)

      // PHASE 1: Categorize splits into "has valid assignment" vs "needs assignment"
      val (alreadyAssigned, needsAssignment) = splitPaths.partition { splitPath =>
        Option(splitAssignments.get(splitPath))
          .exists(availableHosts.contains)
      }

      logger.debug(
        s"Split assignment: ${alreadyAssigned.size} already assigned, ${needsAssignment.size} need assignment"
      )

      // PHASE 2: Count per-query load from existing assignments
      // This is the count of splits IN THIS QUERY assigned to each host
      val perQueryHostCounts = mutable.Map[String, Int]().withDefaultValue(0)

      // Initialize all available hosts with 0 count
      availableHosts.foreach(host => perQueryHostCounts(host) = 0)

      alreadyAssigned.foreach { splitPath =>
        val host = splitAssignments.get(splitPath)
        perQueryHostCounts(host) += 1
      }

      // PHASE 3: Assign splits that need new assignments
      // Use per-query counts for load balancing
      needsAssignment.foreach { splitPath =>
        // Find host with minimum count IN THIS QUERY
        val leastLoadedHost = availableHosts.minBy(perQueryHostCounts(_))

        // Record assignment (persistent for future queries)
        splitAssignments.put(splitPath, leastLoadedHost)

        // Update per-query count
        perQueryHostCounts(leastLoadedHost) += 1

        logger.debug(s"Assigned split $splitPath to $leastLoadedHost (now has ${perQueryHostCounts(leastLoadedHost)} splits in query)")
      }

      // Log distribution
      if (logger.isDebugEnabled) {
        val distribution = perQueryHostCounts.toSeq.sortBy(-_._2)
        logger.debug(s"Per-query host distribution: ${distribution.map { case (h, c) => s"$h=$c" }.mkString(", ")}")
      }

      // Return all assignments for this query
      splitPaths.flatMap(splitPath => Option(splitAssignments.get(splitPath)).map(splitPath -> _)).toMap

    } finally
      lock.writeLock().unlock()
  }

  /**
   * Get preferred host for a split. Returns None if unassigned or assigned host is unavailable.
   *
   * @param splitPath
   *   The split path to look up
   * @param availableHosts
   *   Set of currently available hosts
   * @return
   *   The assigned host if still available, None otherwise
   */
  def getPreferredHost(splitPath: String, availableHosts: Set[String]): Option[String] =
    Option(splitAssignments.get(splitPath))
      .filter(availableHosts.contains)

  /**
   * Get available executor hosts from SparkContext. Excludes the driver since it doesn't run tasks in distributed mode.
   *
   * @param sc
   *   The SparkContext
   * @return
   *   Set of hostnames where executors are running
   */
  def getAvailableHosts(sc: SparkContext): Set[String] =
    try {
      // Get the driver's block manager ID to exclude it from executor list
      val driverBlockManagerId = org.apache.spark.SparkEnv.get.blockManager.blockManagerId
      val driverHostPort       = s"${driverBlockManagerId.host}:${driverBlockManagerId.port}"

      // Get all block managers and filter out the driver
      val executorHosts = sc.getExecutorMemoryStatus.keys
        .filter(_ != driverHostPort)
        .map(_.split(":")(0))
        .toSet

      if (executorHosts.isEmpty) {
        // In local mode or no executors registered yet, use localhost
        // Local mode runs tasks on the driver, so this is correct
        logger.debug("No executor hosts found (local mode or no executors yet), using localhost")
        Set(getCurrentHostname)
      } else {
        logger.debug(s"Found ${executorHosts.size} executor hosts (excluding driver): ${executorHosts.mkString(", ")}")
        executorHosts
      }
    } catch {
      case ex: Exception =>
        logger.warn(s"Failed to get executor hosts: ${ex.getMessage}, using localhost")
        Set(getCurrentHostname)
    }

  /**
   * Optional: Cleanup old assignments to prevent unbounded memory growth. Call this periodically with the set of
   * currently valid split paths.
   *
   * @param currentlyValidSplits
   *   Set of split paths that are still valid
   * @return
   *   Number of stale assignments pruned
   */
  def pruneStaleAssignments(currentlyValidSplits: Set[String]): Int = {
    lock.writeLock().lock()
    try {
      val staleKeys = splitAssignments.keySet().asScala.toSet -- currentlyValidSplits
      staleKeys.foreach(splitAssignments.remove)
      if (staleKeys.nonEmpty) {
        logger.debug(s"Pruned ${staleKeys.size} stale split assignments")
      }
      staleKeys.size
    } finally
      lock.writeLock().unlock()
  }

  /** Clear all assignments (for testing or session reset). */
  def clear(): Unit = {
    lock.writeLock().lock()
    try {
      val count = splitAssignments.size()
      splitAssignments.clear()
      knownHosts = Set.empty
      logger.info(s"Cleared all split assignments ($count entries) and known hosts")
    } finally
      lock.writeLock().unlock()
  }

  /** Get statistics about current assignments. */
  def getStats(): DriverLocalityStats = {
    lock.readLock().lock()
    try {
      val assignments = splitAssignments.asScala.toMap
      val hostCounts  = assignments.values.groupBy(identity).map { case (k, v) => k -> v.size }
      DriverLocalityStats(
        totalTrackedSplits = assignments.size,
        hostsWithAssignments = hostCounts.size,
        splitsPerHost = hostCounts
      )
    } finally
      lock.readLock().unlock()
  }

  /**
   * Detect new hosts and redistribute splits to them for fair load distribution. Called at the start of each query
   * while holding the write lock.
   *
   * When new hosts are detected:
   *   1. Calculate fair share per host 2. Identify overloaded hosts (above fair share) 3. Move splits from overloaded
   *      hosts to new hosts until new hosts reach fair share
   *
   * This ensures new executors get work immediately without destroying all cache locality.
   */
  private def rebalanceForNewHosts(splitPaths: Seq[String], availableHosts: Set[String]): Unit = {
    val newHosts = availableHosts -- knownHosts

    if (newHosts.isEmpty) {
      // No new hosts, nothing to rebalance
      knownHosts = availableHosts // Update for removed hosts
      return
    }

    logger.info(s"🔄 Detected ${newHosts.size} new host(s): ${newHosts.mkString(", ")}")

    // Get splits in this query that have assignments to available hosts
    val splitsInQuery = splitPaths.filter { splitPath =>
      Option(splitAssignments.get(splitPath)).exists(availableHosts.contains)
    }

    if (splitsInQuery.isEmpty) {
      // No existing assignments to rebalance
      knownHosts = availableHosts
      return
    }

    // Calculate current distribution for splits in this query
    val currentDistribution = mutable.Map[String, mutable.ArrayBuffer[String]]()
    availableHosts.foreach(host => currentDistribution(host) = mutable.ArrayBuffer.empty)

    splitsInQuery.foreach { splitPath =>
      val host = splitAssignments.get(splitPath)
      if (host != null && availableHosts.contains(host)) {
        currentDistribution(host) += splitPath
      }
    }

    // Calculate fair share per host
    val totalSplits = splitsInQuery.size
    val fairShare   = math.ceil(totalSplits.toDouble / availableHosts.size).toInt

    // New hosts should get up to fairShare splits
    val splitsToMove = mutable.ArrayBuffer[(String, String)]() // (splitPath, newHost)

    newHosts.foreach { newHost =>
      var splitsNeeded = fairShare

      // Find overloaded hosts (those with more than fairShare) and take splits from them
      val overloadedHosts = currentDistribution
        .filter { case (host, splits) => !newHosts.contains(host) && splits.size > fairShare }
        .toSeq
        .sortBy(-_._2.size) // Most overloaded first

      for ((overloadedHost, splits) <- overloadedHosts if splitsNeeded > 0) {
        val excess = splits.size - fairShare
        val toMove = math.min(excess, splitsNeeded)

        // Take splits from the end (arbitrary, but consistent)
        val movingSplits = splits.takeRight(toMove)
        movingSplits.foreach { splitPath =>
          splitsToMove += ((splitPath, newHost))
          splits -= splitPath
        }
        splitsNeeded -= toMove
      }
    }

    // Apply the moves and mark for catch-up prewarm
    if (splitsToMove.nonEmpty) {
      splitsToMove.foreach {
        case (splitPath, newHost) =>
          val oldHost = splitAssignments.get(splitPath)
          splitAssignments.put(splitPath, newHost)
          // Mark for catch-up prewarm since it moved to a new host
          markSplitNeedsCatchUp(splitPath, newHost)
          logger.debug(s"Rebalanced split $splitPath: $oldHost -> $newHost (marked for catch-up prewarm)")
      }
      logger.info(s"Rebalanced ${splitsToMove.size} splits to new hosts (fair share: $fairShare per host)")
    }

    // Update known hosts
    knownHosts = availableHosts
  }

  /** Get current hostname for this JVM. */
  private def getCurrentHostname: String =
    try
      java.net.InetAddress.getLocalHost.getHostName
    catch {
      case ex: Exception =>
        logger.warn(s"Could not determine hostname, using 'unknown': ${ex.getMessage}")
        "unknown"
    }

  /**
   * Record that a split has been prewarmed on a specific host with specific components.
   *
   * @param splitPath The split path that was prewarmed
   * @param hostname The host where prewarming occurred
   * @param segments The IndexComponents that were prewarmed
   * @param fields The fields that were prewarmed (None = all fields)
   */
  def recordPrewarmCompletion(
    splitPath: String,
    hostname: String,
    segments: Set[IndexComponent],
    fields: Option[Set[String]]
  ): Unit = {
    prewarmState.put(splitPath, PrewarmSplitState(
      hostname = hostname,
      segments = segments,
      fields = fields,
      prewarmTimestamp = System.currentTimeMillis()
    ))
    // Remove from catch-up set since it's been prewarmed
    Option(hostsNeedingCatchUp.get(hostname)).foreach(_.remove(splitPath))
    logger.debug(s"Recorded prewarm completion for split $splitPath on $hostname (${segments.size} components)")
  }

  /**
   * Get splits that need catch-up prewarming due to host reassignment or new splits.
   * This is called by the prewarm system to identify splits that need additional warming.
   *
   * @param currentHosts Currently available hosts
   * @param targetSegments The segments to prewarm (to check if previously prewarmed with different segments)
   * @return Map of hostname -> splits needing prewarm on that host
   */
  def getSplitsNeedingCatchUp(
    currentHosts: Set[String],
    targetSegments: Set[IndexComponent]
  ): Map[String, Seq[String]] = {
    lock.readLock().lock()
    try {
      val result = mutable.Map[String, mutable.ArrayBuffer[String]]()
      currentHosts.foreach(host => result(host) = mutable.ArrayBuffer.empty)

      // Check each assigned split
      splitAssignments.asScala.foreach { case (splitPath, assignedHost) =>
        if (currentHosts.contains(assignedHost)) {
          Option(prewarmState.get(splitPath)) match {
            case Some(state) =>
              // Check if prewarmed on a different host (needs catch-up)
              if (state.hostname != assignedHost) {
                result(assignedHost) += splitPath
                logger.debug(s"Split $splitPath needs catch-up: prewarmed on ${state.hostname}, now assigned to $assignedHost")
              }
              // Check if prewarmed with different/fewer segments (needs catch-up for missing segments)
              else if (!targetSegments.subsetOf(state.segments)) {
                result(assignedHost) += splitPath
                logger.debug(s"Split $splitPath needs catch-up: missing segments ${(targetSegments -- state.segments).map(_.name()).mkString(",")}")
              }
            case None =>
              // Never prewarmed - needs catch-up
              result(assignedHost) += splitPath
              logger.debug(s"Split $splitPath needs catch-up: never prewarmed")
          }
        }
      }

      // Also include explicitly tracked catch-up splits
      hostsNeedingCatchUp.asScala.foreach { case (host, splits) =>
        if (currentHosts.contains(host)) {
          splits.asScala.foreach { splitPath =>
            if (!result(host).contains(splitPath)) {
              result(host) += splitPath
            }
          }
        }
      }

      result.filter(_._2.nonEmpty).map { case (k, v) => k -> v.toSeq }.toMap
    } finally
      lock.readLock().unlock()
  }

  /**
   * Track that a split needs catch-up prewarm on a host.
   * Called when splits are reassigned to new hosts.
   */
  def markSplitNeedsCatchUp(splitPath: String, hostname: String): Unit = {
    hostsNeedingCatchUp.computeIfAbsent(hostname, _ => ConcurrentHashMap.newKeySet[String]()).add(splitPath)
    logger.debug(s"Marked split $splitPath as needing catch-up on $hostname")
  }

  /**
   * Clear catch-up tracking for a host after successful prewarm.
   */
  def clearCatchUpForHost(hostname: String): Unit = {
    hostsNeedingCatchUp.remove(hostname)
    logger.debug(s"Cleared catch-up tracking for host $hostname")
  }

  /**
   * Get splits that are new (not previously known) and need prewarming.
   *
   * @param currentSplits All splits in the current query
   * @param targetSegments The segments to prewarm
   * @return Seq of split paths that have never been prewarmed
   */
  def getNewSplitsNeedingPrewarm(
    currentSplits: Seq[String],
    targetSegments: Set[IndexComponent]
  ): Seq[String] =
    currentSplits.filter { splitPath =>
      Option(prewarmState.get(splitPath)) match {
        case None => true // Never prewarmed
        case Some(state) => !targetSegments.subsetOf(state.segments) // Missing segments
      }
    }

  /**
   * Get prewarm state for a split.
   */
  def getPrewarmState(splitPath: String): Option[PrewarmSplitState] =
    Option(prewarmState.get(splitPath))

  /**
   * Check if prewarm catch-up is enabled via configuration.
   */
  def isCatchUpEnabled(config: Map[String, String]): Boolean =
    config.getOrElse("spark.indextables.prewarm.catchUpNewHosts", "false").toBoolean
}

/** Statistics about the driver locality manager state. */
case class DriverLocalityStats(
  totalTrackedSplits: Int,
  hostsWithAssignments: Int,
  splitsPerHost: Map[String, Int])

/** Prewarm state for a single split. */
case class PrewarmSplitState(
  hostname: String,
  segments: Set[IndexComponent],
  fields: Option[Set[String]],
  prewarmTimestamp: Long) extends Serializable
