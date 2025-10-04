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

import scala.collection.concurrent.TrieMap

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext

import org.slf4j.LoggerFactory

/**
 * Broadcast-based split locality manager that enables efficient distribution of cache locality information across the
 * cluster.
 *
 * This manager addresses the key issue where preferred locations need to be computed on the driver during partition
 * planning, but cache access patterns are observed on individual executors. By using broadcast variables, we can:
 *
 *   1. Collect cache locality information from all executors 2. Broadcast this information back to all nodes in the
 *      cluster 3. Enable accurate preferredLocations() calculations during query planning
 */
object BroadcastSplitLocalityManager {
  private val logger = LoggerFactory.getLogger(getClass)

  // Local registry for tracking split access patterns
  private val localSplitAccess = TrieMap[String, Set[String]]()

  // Current broadcast variable containing cluster-wide locality information
  @volatile private var localityBroadcast: Option[Broadcast[Map[String, Array[String]]]] = None

  /**
   * Record that a split has been accessed on the current host. This is called from executors during split cache
   * operations.
   */
  def recordSplitAccess(splitPath: String, hostname: String): Unit =
    localSplitAccess.synchronized {
      val currentHosts = localSplitAccess.getOrElse(splitPath, Set.empty)
      val updatedHosts = currentHosts + hostname
      localSplitAccess.update(splitPath, updatedHosts)

      println(s"🏠 [LOCALITY] Recording split access: $splitPath on host $hostname")
      println(s"🏠 [LOCALITY] Total hosts for this split: ${updatedHosts.size} (${updatedHosts.mkString(", ")})")
      println(s"🏠 [LOCALITY] Total tracked splits: ${localSplitAccess.size}")

      logger.debug(s"Recorded local split access: $splitPath on host $hostname")
    }

  /**
   * Get preferred hosts for a split from the current broadcast locality information. This is called during partition
   * planning on the driver.
   */
  def getPreferredHosts(splitPath: String): Array[String] =
    localityBroadcast match {
      case Some(broadcast) =>
        try {
          val localityMap = broadcast.value
          val hosts       = localityMap.getOrElse(splitPath, Array.empty)
          if (hosts.nonEmpty) {
            println(s"📍 [DRIVER-LOCALITY] Found broadcast preferred hosts for $splitPath: ${hosts.mkString(", ")}")
            println(s"📍 [DRIVER-LOCALITY] Broadcast contains locality info for ${localityMap.size} splits")
            logger.debug(s"Found broadcast preferred hosts for $splitPath: ${hosts.mkString(", ")}")
          } else {
            println(s"📍 [DRIVER-LOCALITY] No preferred hosts found for $splitPath in broadcast (${localityMap.size} splits tracked)")
          }
          hosts
        } catch {
          case ex: Exception =>
            println(s"❌ [DRIVER-LOCALITY] Error accessing broadcast locality info for $splitPath: ${ex.getMessage}")
            logger.warn(s"Error accessing broadcast locality info for $splitPath: ${ex.getMessage}")
            Array.empty
        }
      case None =>
        println(s"📍 [DRIVER-LOCALITY] No broadcast locality info available for $splitPath (broadcast not initialized)")
        logger.debug(s"No broadcast locality info available for $splitPath")
        Array.empty
    }

  /**
   * Create and update the broadcast variable with current cluster-wide locality information. This should be called
   * periodically by the driver to refresh locality information.
   */
  def updateBroadcastLocality(sc: SparkContext): Broadcast[Map[String, Array[String]]] = {
    println(s"🚀 [DRIVER-LOCALITY] Starting broadcast locality update with ${sc.defaultParallelism} partitions")
    logger.info("Collecting split locality information from all executors")

    // Set descriptive names for Spark UI
    val jobGroup = "tantivy4spark-locality-update"
    val jobDescription =
      s"Updating broadcast locality: Collecting split cache information from ${sc.defaultParallelism} executors"
    val stageName = s"Collect Split Locality: ${sc.defaultParallelism} executors"

    sc.setJobGroup(jobGroup, jobDescription, interruptOnCancel = false)

    // Collect locality information from all executors
    val executorLocalityInfo =
      try
        sc.parallelize(1 to sc.defaultParallelism, sc.defaultParallelism)
          .setName(stageName)
          .mapPartitions { partitionId =>
            // Each executor contributes its local split access information
            val hostname = getCurrentHostname
            val localData = localSplitAccess
              .map {
                case (splitPath, hosts) =>
                  (splitPath, hosts.toArray)
              }
              .toIterator
              .toList

            println(s"🔧 [EXECUTOR-LOCALITY] Partition ${partitionId.mkString(",")} on host $hostname contributing ${localData.size} split records")
            localData.foreach {
              case (splitPath, hosts) =>
                println(s"🔧 [EXECUTOR-LOCALITY]   Split: $splitPath -> hosts: ${hosts.mkString(", ")}")
            }

            localData.iterator
          }
          .setName("Split Locality Data")
          .collect()
      finally
        sc.clearJobGroup()

    println(s"🔄 [DRIVER-LOCALITY] Collected ${executorLocalityInfo.length} locality records from executors")

    // Merge locality information from all executors
    val mergedLocalityMap = executorLocalityInfo
      .groupBy(_._1) // Group by splitPath
      .view
      .map {
        case (splitPath, entries) =>
          // Merge all hosts that have accessed this split
          val allHosts = entries.flatMap(_._2).distinct
          println(s"🔗 [DRIVER-LOCALITY] Merging split $splitPath: ${allHosts.mkString(", ")} (from ${entries.length} sources)")
          splitPath -> allHosts
      }
      .toMap

    println(s"📊 [DRIVER-LOCALITY] Merged locality information for ${mergedLocalityMap.size} splits:")
    mergedLocalityMap.foreach {
      case (splitPath, hosts) =>
        println(s"📊 [DRIVER-LOCALITY]   $splitPath -> ${hosts.mkString(", ")}")
    }

    logger.info(s"Merged locality information for ${mergedLocalityMap.size} splits")
    mergedLocalityMap.foreach {
      case (splitPath, hosts) =>
        logger.debug(s"Split $splitPath has cached copies on: ${hosts.mkString(", ")}")
    }

    // Unpersist old broadcast variable if it exists
    localityBroadcast.foreach { oldBroadcast =>
      println(s"🗑️  [DRIVER-LOCALITY] Unpersisting old broadcast variable (id=${oldBroadcast.id})")
      oldBroadcast.unpersist(blocking = false)
    }

    // Create new broadcast variable
    val newBroadcast = sc.broadcast(mergedLocalityMap)
    localityBroadcast = Some(newBroadcast)

    println(s"📡 [DRIVER-LOCALITY] Created new locality broadcast variable (id=${newBroadcast.id}) with ${mergedLocalityMap.size} splits")
    logger.info(s"Created new locality broadcast variable (id=${newBroadcast.id})")
    newBroadcast
  }

  /**
   * Invalidate a specific split from the broadcast locality information. This should be called when splits are removed
   * or become invalid.
   */
  def invalidateSplit(splitPath: String): Unit =
    localSplitAccess.synchronized {
      localSplitAccess.remove(splitPath)
      logger.debug(s"Invalidated local locality info for split: $splitPath")
    }

  /** Clear all locality information (both local and broadcast). This is useful for testing and cleanup operations. */
  def clearAll(): Unit = {
    localSplitAccess.synchronized {
      val clearedLocal = localSplitAccess.size
      localSplitAccess.clear()
      logger.info(s"Cleared local split locality information ($clearedLocal entries)")
    }

    localityBroadcast.foreach { broadcast =>
      broadcast.unpersist(blocking = false)
      logger.info(s"Unpersisted broadcast locality variable (id=${broadcast.id})")
    }
    localityBroadcast = None
  }

  /** Get statistics about the current locality tracking state. */
  def getLocalityStats(): BroadcastLocalityStats = {
    val localSplits     = localSplitAccess.size
    val broadcastSplits = localityBroadcast.map(_.value.size).getOrElse(0)
    val broadcastId     = localityBroadcast.map(_.id)

    println(s"📈 [LOCALITY-STATS] Local splits tracked: $localSplits")
    println(s"📈 [LOCALITY-STATS] Broadcast splits tracked: $broadcastSplits")
    println(s"📈 [LOCALITY-STATS] Broadcast ID: ${broadcastId.getOrElse("None")}")

    BroadcastLocalityStats(
      localTrackedSplits = localSplits,
      broadcastTrackedSplits = broadcastSplits,
      broadcastId = broadcastId
    )
  }

  /** Force an immediate update of broadcast locality information. Useful for testing and debugging. */
  def forceBroadcastUpdate(sc: SparkContext): Unit = {
    println(s"🔧 [FORCE-UPDATE] Manually triggering broadcast locality update...")
    updateBroadcastLocality(sc)
    println(s"🔧 [FORCE-UPDATE] Broadcast update completed")
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
}

/** Statistics about the broadcast locality manager state. */
case class BroadcastLocalityStats(
  localTrackedSplits: Int,
  broadcastTrackedSplits: Int,
  broadcastId: Option[Long])
