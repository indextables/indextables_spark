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

import io.indextables.spark.io.CloudStorageProvider
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

/**
 * Advanced optimizations for transaction log operations including backward listing, incremental checksums, and async
 * updates.
 */
class AdvancedOptimizations(
  transactionLogPath: Path,
  cloudProvider: CloudStorageProvider,
  spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(classOf[AdvancedOptimizations])

  // Thread pools
  private val asyncUpdatePool                    = TransactionLogThreadPools.asyncUpdateThreadPool
  implicit private val asyncEc: ExecutionContext = asyncUpdatePool.executionContext

  // Async update manager
  private val asyncUpdateManager = new AsyncUpdateManager()

  /** Find last checkpoint using backward listing optimization */
  def findLastCheckpointOptimized(): Option[CheckpointInstance] = {
    val latestVersion = getLatestVersion()

    if (latestVersion < 0) {
      return None
    }

    var listingEndVersion = latestVersion
    val chunkSize         = 1000 // List 1000 versions at a time

    while (listingEndVersion >= 0) {
      val listingStartVersion = Math.max(0, listingEndVersion - chunkSize + 1)

      logger.debug(s"Scanning for checkpoints between versions $listingStartVersion and $listingEndVersion")

      // List files in this version range
      val checkpoints = listVersionRange(listingStartVersion, listingEndVersion)
        .filter(_.isCheckpoint)
        .sortBy(_.version)
        .reverse // Get latest first

      checkpoints.headOption match {
        case Some(checkpoint) =>
          logger.info(s"Found checkpoint at version ${checkpoint.version}")
          return Some(
            CheckpointInstance(
              version = checkpoint.version,
              path = checkpoint.path,
              numParts = checkpoint.numParts,
              sizeInBytes = checkpoint.sizeInBytes
            )
          )
        case None =>
          // Continue searching in earlier versions
          listingEndVersion = listingStartVersion - 1
      }
    }

    logger.info("No checkpoint found in transaction log")
    None
  }

  /** Compute incremental checksum for efficient validation */
  def computeIncrementalChecksum(
    baseChecksum: Option[Checksum],
    newActions: Seq[Action]
  ): Checksum =
    baseChecksum match {
      case Some(base) =>
        // Start with existing checksum
        val updatedChecksum = base.copy()

        // Update with new actions
        newActions.foreach(action => updatedChecksum.update(action))

        updatedChecksum

      case None =>
        // Compute full checksum from scratch
        computeFullChecksum(newActions)
    }

  /** Compute full checksum for a set of actions */
  def computeFullChecksum(actions: Seq[Action]): Checksum = {
    val checksum = new Checksum()

    actions.foreach(action => checksum.update(action))

    checksum
  }

  /** Schedule an asynchronous update operation */
  def scheduleAsyncUpdate[T](operation: => T): Future[T] =
    asyncUpdateManager.scheduleUpdate(operation)

  /** Get snapshot with configurable staleness tolerance */
  def getSnapshotWithStaleness(
    tablePath: String,
    currentVersion: Long,
    maxStaleness: Duration,
    cache: EnhancedTransactionLogCache
  ): Option[Snapshot] =
    asyncUpdateManager.getSnapshotWithStaleness(
      tablePath,
      currentVersion,
      maxStaleness,
      cache
    )

  /** Extended transaction file info for checkpoint finding */
  private case class ExtendedTransactionFile(
    version: Long,
    path: String,
    isCheckpoint: Boolean,
    numParts: Int,
    sizeInBytes: Long)

  /** Optimized version listing for a range */
  private def listVersionRange(startVersion: Long, endVersion: Long): Seq[ExtendedTransactionFile] = {
    val prefix = transactionLogPath.toString
    val files  = cloudProvider.listFiles(prefix, recursive = false)

    files.flatMap { file =>
      parseVersionFromPath(file.path) match {
        case Some(version) if version >= startVersion && version <= endVersion =>
          val isCheckpoint = file.path.contains(".checkpoint.")
          val numParts = if (file.path.contains(".part.")) {
            countCheckpointParts(version)
          } else 0

          Some(
            ExtendedTransactionFile(
              version = version,
              path = file.path,
              isCheckpoint = isCheckpoint,
              numParts = numParts,
              sizeInBytes = file.size
            )
          )
        case _ => None
      }
    }
  }

  /** Count checkpoint parts for multi-part checkpoints */
  private def countCheckpointParts(version: Long): Int = {
    val prefix = new Path(transactionLogPath, f"$version%020d.checkpoint.part.").toString
    cloudProvider.listFiles(prefix, recursive = false).size
  }

  /** Parse version number from file path */
  private def parseVersionFromPath(path: String): Option[Long] =
    Try {
      val fileName   = new Path(path).getName
      val versionStr = fileName.split("\\.").head.replaceAll("_.*", "")
      versionStr.toLong
    }.toOption

  /** Get latest version from transaction log */
  private def getLatestVersion(): Long = {
    val files = cloudProvider.listFiles(transactionLogPath.toString, recursive = false)
    files.flatMap(file => parseVersionFromPath(file.path)).sorted.lastOption.getOrElse(-1L)
  }

  /** Lazy computation wrapper for expensive operations */
  def lazyCompute[T](key: String, cache: EnhancedTransactionLogCache)(compute: => T): T =
    cache.getOrCreateLazyValue(key, compute).get

  /** Prefetch data for anticipated operations */
  def prefetchVersions(versions: Seq[Long]): Unit =
    asyncUpdatePool.submitSimple {
      versions.grouped(10).foreach { batch =>
        val futures = batch.map { version =>
          Future {
            // Prefetch version data into cache
            val versionFile = new Path(transactionLogPath, f"$version%020d.json")
            if (cloudProvider.exists(versionFile.toString)) {
              cloudProvider.readFile(versionFile.toString)
            }
          }
        }
        Try(scala.concurrent.Await.result(Future.sequence(futures), 10.seconds))
      }
    }
}

/** Async update manager for handling background operations */
class AsyncUpdateManager {
  private val logger = LoggerFactory.getLogger(classOf[AsyncUpdateManager])

  // Thread pool for async updates
  private val updateExecutor = TransactionLogThreadPools.asyncUpdateThreadPool

  // Cache of ongoing updates
  private val ongoingUpdates = new ConcurrentHashMap[String, Future[Any]]()

  // Snapshot cache with timestamps
  private val snapshotCache = new ConcurrentHashMap[String, TimestampedSnapshot]()

  /** Schedule an update operation */
  def scheduleUpdate[T](operation: => T): Future[T] =
    updateExecutor.submitSimple(operation)

  /** Get snapshot with staleness tolerance */
  def getSnapshotWithStaleness(
    tablePath: String,
    currentVersion: Long,
    maxStaleness: Duration,
    cache: EnhancedTransactionLogCache
  ): Option[Snapshot] = {

    val cacheKey = s"$tablePath-$currentVersion"

    // Check if we have a cached snapshot
    Option(snapshotCache.get(cacheKey)) match {
      case Some(cached) if cached.age <= maxStaleness =>
        logger.debug(s"Using cached snapshot for $tablePath version $currentVersion (age: ${cached.age})")
        Some(cached.snapshot)

      case cached =>
        // Check if an update is already in progress
        Option(ongoingUpdates.get(cacheKey)) match {
          case Some(updateFuture) if !updateFuture.isCompleted =>
            // Update in progress, return stale snapshot if available
            logger.debug(s"Update in progress for $tablePath, returning stale snapshot")
            cached.map(_.snapshot)

          case _ =>
            // Schedule a new update
            val updateFuture = scheduleSnapshotUpdate(tablePath, currentVersion, cache)
            ongoingUpdates.put(cacheKey, updateFuture)

            // Return stale snapshot while update happens
            cached.map(_.snapshot)
        }
    }
  }

  /** Schedule snapshot update in background */
  private def scheduleSnapshotUpdate(
    tablePath: String,
    version: Long,
    cache: EnhancedTransactionLogCache
  ): Future[Snapshot] =
    updateExecutor.submitSimple {
      try {
        // Compute fresh snapshot
        val snapshot = cache.getOrComputeSnapshot(
          tablePath,
          version,
          // This would compute the actual snapshot
          computeSnapshot(tablePath, version)
        )

        // Update cache
        val cacheKey = s"$tablePath-$version"
        snapshotCache.put(cacheKey, TimestampedSnapshot(snapshot))
        ongoingUpdates.remove(cacheKey)

        snapshot
      } catch {
        case e: Exception =>
          logger.error(s"Failed to update snapshot for $tablePath version $version", e)
          throw e
      }
    }

  private def computeSnapshot(tablePath: String, version: Long): Snapshot =
    // Simplified - would compute actual snapshot
    Snapshot(
      version = version,
      files = Seq.empty,
      metadata = MetadataAction(
        id = "",
        name = None,
        description = None,
        format = FileFormat("", Map.empty),
        schemaString = "",
        partitionColumns = Seq.empty,
        configuration = Map.empty,
        createdTime = None
      )
    )

  /** Clean up old cached snapshots */
  def cleanupCache(maxAge: Duration): Unit = {
    val cutoff = System.currentTimeMillis() - maxAge.toMillis

    snapshotCache.entrySet().removeIf(entry => entry.getValue.timestamp < cutoff)
  }
}

/** Checksum for incremental validation */
class Checksum {
  private val digest      = MessageDigest.getInstance("SHA-256")
  private val fileHashes  = mutable.HashMap[String, Array[Byte]]()
  private var actionCount = 0L

  def update(action: Action): Unit =
    action match {
      case add: AddAction =>
        val hash = computeHash(add.path + add.size + add.modificationTime)
        fileHashes(add.path) = hash
        actionCount += 1

      case remove: RemoveAction =>
        fileHashes.remove(remove.path)
        actionCount += 1

      case metadata: MetadataAction =>
        val hash = computeHash(metadata.schemaString + metadata.partitionColumns.mkString(","))
        fileHashes("__metadata__") = hash
        actionCount += 1

      case _ => // Ignore other actions
    }

  def copy(): Checksum = {
    val newChecksum = new Checksum()
    newChecksum.fileHashes ++= this.fileHashes
    newChecksum.actionCount = this.actionCount
    newChecksum
  }

  def getHash(): Array[Byte] = {
    val combined = fileHashes.values.flatten.toArray
    computeHash(combined)
  }

  def getActionCount: Long = actionCount

  def getFileCount: Int = fileHashes.size

  private def computeHash(data: Any): Array[Byte] = {
    val bytes = data match {
      case s: String      => s.getBytes("UTF-8")
      case b: Array[Byte] => b
      case other          => other.toString.getBytes("UTF-8")
    }
    digest.digest(bytes)
  }
}

/** Checkpoint instance information */
case class CheckpointInstance(
  version: Long,
  path: String,
  numParts: Int,
  sizeInBytes: Long)

/** Timestamped snapshot for cache management */
case class TimestampedSnapshot(
  snapshot: Snapshot,
  timestamp: Long = System.currentTimeMillis()) {
  def age: Duration = (System.currentTimeMillis() - timestamp).millis
}
