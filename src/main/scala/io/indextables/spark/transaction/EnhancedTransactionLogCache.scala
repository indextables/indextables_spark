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

import scala.collection.concurrent.TrieMap

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.google.common.cache.{CacheStats => GuavaCacheStats}
import org.slf4j.LoggerFactory

/**
 * Companion object with GLOBAL/STATIC caches for checkpoint data. Checkpoint data is immutable once written, so safe to
 * share across TransactionLog instances.
 */
object EnhancedTransactionLogCache {
  private val logger = LoggerFactory.getLogger(getClass)

  // Global cache key types
  case class CheckpointActionsKey(tablePath: String, checkpointVersion: Long)
  case class LastCheckpointInfoCached(info: Option[LastCheckpointInfo], cachedAt: Long)

  // GLOBAL checkpoint actions cache - shared across all TransactionLog instances
  private[transaction] val globalCheckpointActionsCache: Cache[CheckpointActionsKey, Seq[Action]] = CacheBuilder
    .newBuilder()
    .expireAfterAccess(30, TimeUnit.MINUTES)
    .maximumSize(200)
    .recordStats()
    .removalListener(new RemovalListener[CheckpointActionsKey, Seq[Action]] {
      override def onRemoval(notification: RemovalNotification[CheckpointActionsKey, Seq[Action]]): Unit =
        logger.debug(s"Evicted global checkpoint actions: ${notification.getKey}, reason: ${notification.getCause}")
    })
    .build[CheckpointActionsKey, Seq[Action]]()

  // GLOBAL last checkpoint info cache - shared across all TransactionLog instances
  private[transaction] val globalLastCheckpointInfoCache: Cache[String, LastCheckpointInfoCached] = CacheBuilder
    .newBuilder()
    .expireAfterAccess(30, TimeUnit.MINUTES)
    .maximumSize(200)
    .recordStats()
    .build[String, LastCheckpointInfoCached]()

  /** Clear all global caches (for testing) */
  def clearGlobalCaches(): Unit = {
    globalCheckpointActionsCache.invalidateAll()
    globalLastCheckpointInfoCache.invalidateAll()
    logger.info("Cleared all global checkpoint caches")
  }

  /** Invalidate global caches for a specific table */
  def invalidateGlobalCachesForTable(tablePath: String): Unit = {
    globalLastCheckpointInfoCache.invalidate(tablePath)
    import scala.jdk.CollectionConverters._
    globalCheckpointActionsCache
      .asMap()
      .keySet()
      .asScala
      .filter(_.tablePath == tablePath)
      .foreach(globalCheckpointActionsCache.invalidate)
  }

  /** Get global cache statistics */
  def getGlobalCacheStats(): (GuavaCacheStats, GuavaCacheStats) =
    (globalCheckpointActionsCache.stats(), globalLastCheckpointInfoCache.stats())
}

/**
 * Enhanced multi-level caching system for transaction logs using Google Guava cache with proper eviction policies, TTL
 * management, and lazy evaluation support.
 */
class EnhancedTransactionLogCache(
  logCacheSize: Long = 1000,
  logCacheTTLMinutes: Long = 5,
  snapshotCacheSize: Long = 100,
  snapshotCacheTTLMinutes: Long = 10,
  fileListCacheSize: Long = 50,
  fileListCacheTTLMinutes: Long = 2,
  metadataCacheSize: Long = 100,
  metadataCacheTTLMinutes: Long = 30) {

  private val logger = LoggerFactory.getLogger(classOf[EnhancedTransactionLogCache])

  // Cache key types
  case class LogCacheKey(tablePath: String, version: Long)
  case class SnapshotCacheKey(tablePath: String, version: Long)
  case class FileListCacheKey(tablePath: String, checksum: String)
  case class MetadataCacheKey(tablePath: String)

  // Log-level cache for transaction log instances
  private val logCache: Cache[LogCacheKey, TransactionLogSnapshot] = CacheBuilder
    .newBuilder()
    .expireAfterAccess(logCacheTTLMinutes, TimeUnit.MINUTES)
    .maximumSize(logCacheSize)
    .recordStats()
    .removalListener(new RemovalListener[LogCacheKey, TransactionLogSnapshot] {
      override def onRemoval(notification: RemovalNotification[LogCacheKey, TransactionLogSnapshot]): Unit =
        logger.debug(s"Evicted log cache entry: ${notification.getKey}, reason: ${notification.getCause}")
    })
    .build[LogCacheKey, TransactionLogSnapshot]()

  // Snapshot cache for version states
  private val snapshotCache: Cache[SnapshotCacheKey, Snapshot] = CacheBuilder
    .newBuilder()
    .expireAfterWrite(snapshotCacheTTLMinutes, TimeUnit.MINUTES)
    .maximumSize(snapshotCacheSize)
    .recordStats()
    .removalListener(new RemovalListener[SnapshotCacheKey, Snapshot] {
      override def onRemoval(notification: RemovalNotification[SnapshotCacheKey, Snapshot]): Unit =
        logger.debug(s"Evicted snapshot cache entry: ${notification.getKey}, reason: ${notification.getCause}")
    })
    .build[SnapshotCacheKey, Snapshot]()

  // File list cache with checksum validation
  private val fileListCache: Cache[FileListCacheKey, Seq[AddAction]] = CacheBuilder
    .newBuilder()
    .expireAfterWrite(fileListCacheTTLMinutes, TimeUnit.MINUTES)
    .maximumSize(fileListCacheSize)
    .recordStats()
    .build[FileListCacheKey, Seq[AddAction]]()

  // Metadata cache
  private val metadataCache: Cache[MetadataCacheKey, MetadataAction] = CacheBuilder
    .newBuilder()
    .expireAfterAccess(metadataCacheTTLMinutes, TimeUnit.MINUTES)
    .maximumSize(metadataCacheSize)
    .recordStats()
    .build[MetadataCacheKey, MetadataAction]()

  // Protocol cache (similar to metadata cache)
  private val protocolCache: Cache[MetadataCacheKey, ProtocolAction] = CacheBuilder
    .newBuilder()
    .expireAfterAccess(metadataCacheTTLMinutes, TimeUnit.MINUTES)
    .maximumSize(metadataCacheSize)
    .recordStats()
    .build[MetadataCacheKey, ProtocolAction]()

  // Version-specific action cache
  private val versionCache: Cache[LogCacheKey, Seq[Action]] = CacheBuilder
    .newBuilder()
    .expireAfterAccess(5, TimeUnit.MINUTES)
    .maximumSize(500)
    .recordStats()
    .build[LogCacheKey, Seq[Action]]()

  // Checkpoint info cache
  private val checkpointCache: Cache[String, CheckpointInfo] = CacheBuilder
    .newBuilder()
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .maximumSize(100)
    .recordStats()
    .build[String, CheckpointInfo]()

  // Import global cache types from companion object
  import EnhancedTransactionLogCache.{CheckpointActionsKey, LastCheckpointInfoCached}

  // Lazy value cache for expensive computations
  private val lazyValueCache = new TrieMap[String, LazyValue[_]]()

  /** Get or compute a transaction log snapshot */
  def getOrComputeLogSnapshot(
    tablePath: String,
    version: Long,
    compute: => TransactionLogSnapshot
  ): TransactionLogSnapshot = {
    val key = LogCacheKey(tablePath, version)
    Option(logCache.getIfPresent(key)) match {
      case Some(snapshot) =>
        logger.debug(s"Log cache hit for $tablePath version $version")
        snapshot
      case None =>
        logger.debug(s"Log cache miss for $tablePath version $version")
        val snapshot = compute
        logCache.put(key, snapshot)
        snapshot
    }
  }

  /** Get or compute a snapshot */
  def getOrComputeSnapshot(
    tablePath: String,
    version: Long,
    compute: => Snapshot
  ): Snapshot = {
    val key = SnapshotCacheKey(tablePath, version)
    Option(snapshotCache.getIfPresent(key)) match {
      case Some(snapshot) =>
        logger.debug(s"Snapshot cache hit for $tablePath version $version")
        snapshot
      case None =>
        logger.debug(s"Snapshot cache miss for $tablePath version $version")
        val snapshot = compute
        snapshotCache.put(key, snapshot)
        snapshot
    }
  }

  /** Get or compute file list */
  def getOrComputeFileList(
    tablePath: String,
    checksum: String,
    compute: => Seq[AddAction]
  ): Seq[AddAction] = {
    val key = FileListCacheKey(tablePath, checksum)
    Option(fileListCache.getIfPresent(key)) match {
      case Some(files) =>
        logger.debug(s"File list cache hit for $tablePath")
        logger.debug(
          s" Cache hit for checksum $checksum, returning ${files.size} files: ${files.map(_.path).mkString(", ")}"
        )
        files
      case None =>
        logger.debug(s"File list cache miss for $tablePath")
        logger.debug(s" Cache miss for checksum $checksum, computing...")
        val files = compute
        fileListCache.put(key, files)
        logger.debug(s" Computed and cached ${files.size} files: ${files.map(_.path).mkString(", ")}")
        files
    }
  }

  /** Get or compute metadata */
  def getOrComputeMetadata(
    tablePath: String,
    compute: => MetadataAction
  ): MetadataAction = {
    val key = MetadataCacheKey(tablePath)
    Option(metadataCache.getIfPresent(key)) match {
      case Some(metadata) =>
        logger.debug(s"Metadata cache hit for $tablePath")
        metadata
      case None =>
        logger.debug(s"Metadata cache miss for $tablePath")
        val metadata = compute
        metadataCache.put(key, metadata)
        metadata
    }
  }

  /** Get or compute protocol */
  def getOrComputeProtocol(
    tablePath: String,
    compute: => ProtocolAction
  ): ProtocolAction = {
    val key = MetadataCacheKey(tablePath)
    Option(protocolCache.getIfPresent(key)) match {
      case Some(protocol) =>
        logger.debug(s"Protocol cache hit for $tablePath")
        protocol
      case None =>
        logger.debug(s"Protocol cache miss for $tablePath")
        val protocol = compute
        protocolCache.put(key, protocol)
        protocol
    }
  }

  /** Get or compute version actions */
  def getOrComputeVersionActions(
    tablePath: String,
    version: Long,
    compute: => Seq[Action]
  ): Seq[Action] = {
    val key = LogCacheKey(tablePath, version)
    Option(versionCache.getIfPresent(key)) match {
      case Some(actions) =>
        logger.debug(s"Version cache hit for $tablePath version $version")
        actions
      case None =>
        logger.debug(s"Version cache miss for $tablePath version $version")
        val actions = compute
        versionCache.put(key, actions)
        actions
    }
  }

  /** Get or compute checkpoint info */
  def getOrComputeCheckpoint(
    tablePath: String,
    compute: => CheckpointInfo
  ): CheckpointInfo =
    Option(checkpointCache.getIfPresent(tablePath)) match {
      case Some(info) =>
        logger.debug(s"Checkpoint cache hit for $tablePath")
        info
      case None =>
        logger.debug(s"Checkpoint cache miss for $tablePath")
        val info = compute
        checkpointCache.put(tablePath, info)
        info
    }

  /**
   * Get or compute checkpoint ACTIONS - the actual parsed Seq[Action] from checkpoint file. This is the key method for
   * avoiding repeated checkpoint file reads. Uses GLOBAL cache shared across all TransactionLog instances.
   */
  def getOrComputeCheckpointActions(
    tablePath: String,
    checkpointVersion: Long,
    compute: => Option[Seq[Action]]
  ): Option[Seq[Action]] = {
    val key = CheckpointActionsKey(tablePath, checkpointVersion)
    Option(EnhancedTransactionLogCache.globalCheckpointActionsCache.getIfPresent(key)) match {
      case Some(actions) =>
        logger.debug(
          s"GLOBAL checkpoint actions cache HIT for $tablePath version $checkpointVersion (${actions.size} actions)"
        )
        Some(actions)
      case None =>
        logger.debug(
          s"GLOBAL checkpoint actions cache MISS for $tablePath version $checkpointVersion - reading from storage"
        )
        compute match {
          case Some(actions) =>
            EnhancedTransactionLogCache.globalCheckpointActionsCache.put(key, actions)
            logger.info(
              s"Cached ${actions.size} checkpoint actions in GLOBAL cache for $tablePath version $checkpointVersion"
            )
            Some(actions)
          case None =>
            logger.debug(s"No checkpoint actions found for $tablePath version $checkpointVersion")
            None
        }
    }
  }

  /**
   * Get or compute last checkpoint info - caches the _last_checkpoint file content. Uses GLOBAL cache shared across all
   * TransactionLog instances.
   */
  def getOrComputeLastCheckpointInfo(
    tablePath: String,
    compute: => Option[LastCheckpointInfo]
  ): Option[LastCheckpointInfo] =
    Option(EnhancedTransactionLogCache.globalLastCheckpointInfoCache.getIfPresent(tablePath)) match {
      case Some(cached) =>
        logger.debug(s"GLOBAL last checkpoint info cache HIT for $tablePath: ${cached.info.map(_.version)}")
        cached.info
      case None =>
        logger.debug(s"GLOBAL last checkpoint info cache MISS for $tablePath - reading from storage")
        val info = compute
        EnhancedTransactionLogCache.globalLastCheckpointInfoCache.put(
          tablePath,
          LastCheckpointInfoCached(info, System.currentTimeMillis())
        )
        logger.info(s"Cached last checkpoint info in GLOBAL cache for $tablePath: ${info.map(_.version)}")
        info
    }

  /** Invalidate last checkpoint info cache for a table (call when new checkpoint is written) */
  def invalidateLastCheckpointInfo(tablePath: String): Unit = {
    EnhancedTransactionLogCache.globalLastCheckpointInfoCache.invalidate(tablePath)
    logger.debug(s"Invalidated GLOBAL last checkpoint info cache for $tablePath")
  }

  /** Invalidate checkpoint actions cache for a specific version */
  def invalidateCheckpointActions(tablePath: String, checkpointVersion: Long): Unit = {
    EnhancedTransactionLogCache.globalCheckpointActionsCache.invalidate(
      CheckpointActionsKey(tablePath, checkpointVersion)
    )
    logger.debug(s"Invalidated GLOBAL checkpoint actions cache for $tablePath version $checkpointVersion")
  }

  /** Get or create a lazy value for expensive computations */
  def getOrCreateLazyValue[T](key: String, compute: => T): LazyValue[T] =
    lazyValueCache.getOrElseUpdate(key, new LazyValue(compute)).asInstanceOf[LazyValue[T]]

  // Write-through cache methods for proactive cache management

  /** Directly cache version actions (write-through) */
  def putVersionActions(
    tablePath: String,
    version: Long,
    actions: Seq[Action]
  ): Unit = {
    val key = LogCacheKey(tablePath, version)
    versionCache.put(key, actions)
    logger.debug(s"Cached version actions for $tablePath version $version")
  }

  /** Directly cache file list (write-through) */
  def putFileList(
    tablePath: String,
    checksum: String,
    files: Seq[AddAction]
  ): Unit = {
    val key = FileListCacheKey(tablePath, checksum)
    fileListCache.put(key, files)
    logger.debug(s"Cached file list for $tablePath checksum $checksum")
  }

  /** Directly cache metadata (write-through) */
  def putMetadata(tablePath: String, metadata: MetadataAction): Unit = {
    val key = MetadataCacheKey(tablePath)
    metadataCache.put(key, metadata)
    logger.debug(s"Cached metadata for $tablePath")
  }

  /** Directly cache protocol (write-through) */
  def putProtocol(tablePath: String, protocol: ProtocolAction): Unit = {
    val key = MetadataCacheKey(tablePath)
    protocolCache.put(key, protocol)
    logger.debug(s"Cached protocol for $tablePath")
  }

  /** Invalidate protocol cache for a specific table */
  def invalidateProtocol(tablePath: String): Unit = {
    val key = MetadataCacheKey(tablePath)
    protocolCache.invalidate(key)
    logger.debug(s"Invalidated protocol cache for $tablePath")
  }

  /** Invalidate all caches for a specific table */
  def invalidateTable(tablePath: String): Unit = {
    logger.info(s"Invalidating all caches for table $tablePath")

    // Invalidate instance-level caches
    invalidateByPredicate(logCache, (key: LogCacheKey) => key.tablePath == tablePath)
    invalidateByPredicate(snapshotCache, (key: SnapshotCacheKey) => key.tablePath == tablePath)
    invalidateByPredicate(fileListCache, (key: FileListCacheKey) => key.tablePath == tablePath)
    invalidateByPredicate(versionCache, (key: LogCacheKey) => key.tablePath == tablePath)
    metadataCache.invalidate(MetadataCacheKey(tablePath))
    protocolCache.invalidate(MetadataCacheKey(tablePath))
    checkpointCache.invalidate(tablePath)

    // Invalidate GLOBAL checkpoint caches
    EnhancedTransactionLogCache.invalidateGlobalCachesForTable(tablePath)

    // Clear lazy values for this table
    val keysToRemove = lazyValueCache.keys.filter(_.startsWith(tablePath)).toList
    keysToRemove.foreach(lazyValueCache.remove)
  }

  /** Invalidate version-dependent caches for a table */
  def invalidateVersionDependentCaches(tablePath: String): Unit = {
    logger.debug(s"Invalidating version-dependent caches for table $tablePath")

    invalidateByPredicate(fileListCache, (key: FileListCacheKey) => key.tablePath == tablePath)
    invalidateByPredicate(snapshotCache, (key: SnapshotCacheKey) => key.tablePath == tablePath)
    metadataCache.invalidate(MetadataCacheKey(tablePath))
    protocolCache.invalidate(MetadataCacheKey(tablePath))
  }

  /** Invalidate specific versions */
  def invalidateVersions(tablePath: String, versions: Seq[Long]): Unit =
    versions.foreach { version =>
      logCache.invalidate(LogCacheKey(tablePath, version))
      snapshotCache.invalidate(SnapshotCacheKey(tablePath, version))
      versionCache.invalidate(LogCacheKey(tablePath, version))
    }

  /** Helper to invalidate cache entries by predicate */
  private def invalidateByPredicate[K, V](cache: Cache[K, V], predicate: K => Boolean): Unit = {
    import scala.jdk.CollectionConverters._
    val keysToInvalidate = cache.asMap().keySet().asScala.filter(predicate)
    cache.invalidateAll(keysToInvalidate.asJava)
  }

  /** Get cache statistics for monitoring */
  def getStatistics(): CacheStatistics =
    CacheStatistics(
      logCacheStats = logCache.stats(),
      snapshotCacheStats = snapshotCache.stats(),
      fileListCacheStats = fileListCache.stats(),
      metadataCacheStats = metadataCache.stats(),
      versionCacheStats = versionCache.stats(),
      checkpointCacheStats = checkpointCache.stats(),
      protocolCacheStats = protocolCache.stats(),
      checkpointActionsCacheStats = EnhancedTransactionLogCache.globalCheckpointActionsCache.stats(),
      lastCheckpointInfoCacheStats = EnhancedTransactionLogCache.globalLastCheckpointInfoCache.stats(),
      lazyValueCount = lazyValueCache.size
    )

  /** Clear all caches */
  def clearAll(): Unit = {
    logger.info("Clearing all caches")
    logCache.invalidateAll()
    snapshotCache.invalidateAll()
    fileListCache.invalidateAll()
    metadataCache.invalidateAll()
    protocolCache.invalidateAll()
    versionCache.invalidateAll()
    checkpointCache.invalidateAll()
    lazyValueCache.clear()
    // Also clear GLOBAL checkpoint caches
    EnhancedTransactionLogCache.clearGlobalCaches()
  }

  /** Cleanup expired entries */
  def cleanUp(): Unit = {
    logCache.cleanUp()
    snapshotCache.cleanUp()
    fileListCache.cleanUp()
    metadataCache.cleanUp()
    protocolCache.cleanUp()
    versionCache.cleanUp()
    checkpointCache.cleanUp()
    // Global caches are cleaned up via their own TTL expiration
    EnhancedTransactionLogCache.globalCheckpointActionsCache.cleanUp()
    EnhancedTransactionLogCache.globalLastCheckpointInfoCache.cleanUp()
  }
}

/** Lazy value wrapper for deferred computation with thread-safe caching */
class LazyValue[T](compute: => T) {
  @volatile private var cached: Option[T] = None
  private val lock                        = new Object()

  def get: T =
    cached.getOrElse {
      lock.synchronized {
        cached.getOrElse {
          val value = compute
          cached = Some(value)
          value
        }
      }
    }

  def getOption: Option[T] = cached

  def isComputed: Boolean = cached.isDefined

  def invalidate(): Unit =
    lock.synchronized {
      cached = None
    }
}

/** Snapshot representation for caching */
case class Snapshot(
  version: Long,
  files: Seq[AddAction],
  metadata: MetadataAction,
  timestamp: Long = System.currentTimeMillis()) {
  def age: Long = System.currentTimeMillis() - timestamp
}

/** Transaction log snapshot for caching */
case class TransactionLogSnapshot(
  tablePath: String,
  version: Long,
  files: Seq[AddAction],
  metadata: Option[MetadataAction],
  checkpointInfo: Option[CheckpointInfo],
  timestamp: Long = System.currentTimeMillis())

/** Cache statistics for monitoring */
case class CacheStatistics(
  logCacheStats: GuavaCacheStats,
  snapshotCacheStats: GuavaCacheStats,
  fileListCacheStats: GuavaCacheStats,
  metadataCacheStats: GuavaCacheStats,
  versionCacheStats: GuavaCacheStats,
  checkpointCacheStats: GuavaCacheStats,
  protocolCacheStats: GuavaCacheStats,
  checkpointActionsCacheStats: GuavaCacheStats,
  lastCheckpointInfoCacheStats: GuavaCacheStats,
  lazyValueCount: Int) {
  def totalHitRate: Double = {
    val allStats = Seq(
      logCacheStats,
      snapshotCacheStats,
      fileListCacheStats,
      metadataCacheStats,
      versionCacheStats,
      checkpointCacheStats,
      protocolCacheStats,
      checkpointActionsCacheStats,
      lastCheckpointInfoCacheStats
    )

    val totalHits     = allStats.map(_.hitCount()).sum
    val totalRequests = allStats.map(_.requestCount()).sum

    if (totalRequests == 0) 0.0
    else totalHits.toDouble / totalRequests
  }

  def totalEvictionCount: Long =
    Seq(
      logCacheStats,
      snapshotCacheStats,
      fileListCacheStats,
      metadataCacheStats,
      versionCacheStats,
      checkpointCacheStats,
      protocolCacheStats,
      checkpointActionsCacheStats,
      lastCheckpointInfoCacheStats
    ).map(_.evictionCount()).sum

  /** Get checkpoint actions cache hit rate specifically - useful for monitoring the fix */
  def checkpointActionsHitRate: Double = {
    val requests = checkpointActionsCacheStats.requestCount()
    if (requests == 0) 0.0
    else checkpointActionsCacheStats.hitCount().toDouble / requests
  }

  /** Get last checkpoint info cache hit rate specifically */
  def lastCheckpointInfoHitRate: Double = {
    val requests = lastCheckpointInfoCacheStats.requestCount()
    if (requests == 0) 0.0
    else lastCheckpointInfoCacheStats.hitCount().toDouble / requests
  }
}
