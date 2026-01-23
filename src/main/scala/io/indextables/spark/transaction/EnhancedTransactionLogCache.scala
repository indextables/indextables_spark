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
import io.indextables.spark.transaction.avro.StateManifest
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

  // Configurable TTL for global checkpoint caches (default 5 minutes)
  @volatile private var globalCheckpointCacheTTLMinutes: Long = 5
  @volatile private var globalCheckpointCacheMaxSize: Long = 200
  @volatile private var cachesInitialized: Boolean = false

  /**
   * Configure the global checkpoint cache TTL. Must be called before first cache access.
   * Thread-safe but will log a warning if called after caches are already initialized.
   */
  def configureGlobalCacheTTL(ttlMinutes: Long, maxSize: Long = 200): Unit = synchronized {
    if (cachesInitialized) {
      logger.warn(s"Global checkpoint caches already initialized with TTL=${globalCheckpointCacheTTLMinutes}min. " +
        s"New TTL=$ttlMinutes will take effect after cache rebuild (clearGlobalCaches + re-access).")
    }
    globalCheckpointCacheTTLMinutes = ttlMinutes
    globalCheckpointCacheMaxSize = maxSize
    logger.info(s"Configured global checkpoint cache: TTL=${ttlMinutes}min, maxSize=$maxSize")
  }

  /** Get the current global checkpoint cache TTL in minutes */
  def getGlobalCacheTTLMinutes: Long = globalCheckpointCacheTTLMinutes

  // Lazy initialization of global caches to allow configuration before first use
  private lazy val _globalCheckpointActionsCache: Cache[CheckpointActionsKey, Seq[Action]] = synchronized {
    cachesInitialized = true
    logger.info(s"Initializing global checkpoint actions cache: TTL=${globalCheckpointCacheTTLMinutes}min, maxSize=$globalCheckpointCacheMaxSize")
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(globalCheckpointCacheTTLMinutes, TimeUnit.MINUTES)
      .maximumSize(globalCheckpointCacheMaxSize)
      .recordStats()
      .removalListener(new RemovalListener[CheckpointActionsKey, Seq[Action]] {
        override def onRemoval(notification: RemovalNotification[CheckpointActionsKey, Seq[Action]]): Unit =
          logger.debug(s"Evicted global checkpoint actions: ${notification.getKey}, reason: ${notification.getCause}")
      })
      .build[CheckpointActionsKey, Seq[Action]]()
  }

  private lazy val _globalLastCheckpointInfoCache: Cache[String, LastCheckpointInfoCached] = synchronized {
    cachesInitialized = true
    logger.info(s"Initializing global last checkpoint info cache: TTL=${globalCheckpointCacheTTLMinutes}min, maxSize=$globalCheckpointCacheMaxSize")
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(globalCheckpointCacheTTLMinutes, TimeUnit.MINUTES)
      .maximumSize(globalCheckpointCacheMaxSize)
      .recordStats()
      .build[String, LastCheckpointInfoCached]()
  }

  // Global cache for Avro state manifests - keyed by state directory path (includes version)
  // e.g., "s3://bucket/table/_transaction_log/state-v00000000000000000100"
  private lazy val _globalAvroStateManifestCache: Cache[String, StateManifest] = synchronized {
    cachesInitialized = true
    logger.info(s"Initializing global Avro state manifest cache: TTL=${globalCheckpointCacheTTLMinutes}min, maxSize=$globalCheckpointCacheMaxSize")
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(globalCheckpointCacheTTLMinutes, TimeUnit.MINUTES)
      .maximumSize(globalCheckpointCacheMaxSize)
      .recordStats()
      .removalListener(new RemovalListener[String, StateManifest] {
        override def onRemoval(notification: RemovalNotification[String, StateManifest]): Unit =
          logger.debug(s"Evicted global Avro state manifest: ${notification.getKey}, reason: ${notification.getCause}")
      })
      .build[String, StateManifest]()
  }

  // Global cache for Avro file lists - keyed by (tablePath, avroVersion)
  // This caches the Seq[AddAction] result after expensive JSON processing (filterEmptyObjectMappings)
  // to avoid re-parsing schemas on every query
  case class AvroFileListKey(tablePath: String, avroVersion: Long)

  private lazy val _globalAvroFileListCache: Cache[AvroFileListKey, Seq[AddAction]] = synchronized {
    cachesInitialized = true
    logger.info(s"Initializing global Avro file list cache: TTL=${globalCheckpointCacheTTLMinutes}min, maxSize=$globalCheckpointCacheMaxSize")
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(globalCheckpointCacheTTLMinutes, TimeUnit.MINUTES)
      .maximumSize(globalCheckpointCacheMaxSize)
      .recordStats()
      .removalListener(new RemovalListener[AvroFileListKey, Seq[AddAction]] {
        override def onRemoval(notification: RemovalNotification[AvroFileListKey, Seq[AddAction]]): Unit =
          logger.debug(s"Evicted global Avro file list: ${notification.getKey}, reason: ${notification.getCause}")
      })
      .build[AvroFileListKey, Seq[AddAction]]()
  }

  // Global cache for filtered schemas - keyed by schema hash
  // filterEmptyObjectMappings is expensive (JSON parsing) but produces same result for same input
  // This cache ensures we only parse each unique schema once
  private lazy val _globalFilteredSchemaCache: Cache[String, String] = synchronized {
    cachesInitialized = true
    logger.info(s"Initializing global filtered schema cache: TTL=${globalCheckpointCacheTTLMinutes}min, maxSize=1000")
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(globalCheckpointCacheTTLMinutes, TimeUnit.MINUTES)
      .maximumSize(1000)  // Typically few unique schemas even for large tables
      .recordStats()
      .build[String, String]()
  }

  // Accessors for the lazy caches
  private[transaction] def globalCheckpointActionsCache: Cache[CheckpointActionsKey, Seq[Action]] = _globalCheckpointActionsCache
  private[transaction] def globalLastCheckpointInfoCache: Cache[String, LastCheckpointInfoCached] = _globalLastCheckpointInfoCache
  private[transaction] def globalAvroStateManifestCache: Cache[String, StateManifest] = _globalAvroStateManifestCache
  private[transaction] def globalAvroFileListCache: Cache[AvroFileListKey, Seq[AddAction]] = _globalAvroFileListCache
  private[transaction] def globalFilteredSchemaCache: Cache[String, String] = _globalFilteredSchemaCache

  /**
   * Get or compute a filtered schema - caches the result of filterEmptyObjectMappings.
   *
   * The filterEmptyObjectMappings function parses JSON which is expensive. Since the same
   * schema hash always produces the same filtered result, we cache based on the schema hash.
   *
   * @param schemaHash The schema hash (used as cache key)
   * @param compute Lazy computation that returns the filtered schema
   * @return Filtered schema string
   */
  def getOrComputeFilteredSchema(schemaHash: String, compute: => String): String =
    Option(globalFilteredSchemaCache.getIfPresent(schemaHash)) match {
      case Some(filtered) =>
        logger.debug(s"GLOBAL filtered schema cache HIT for hash $schemaHash")
        filtered
      case None =>
        logger.debug(s"GLOBAL filtered schema cache MISS for hash $schemaHash - computing")
        val filtered = compute
        globalFilteredSchemaCache.put(schemaHash, filtered)
        filtered
    }

  /** Clear all global caches (for testing) */
  def clearGlobalCaches(): Unit = {
    globalCheckpointActionsCache.invalidateAll()
    globalLastCheckpointInfoCache.invalidateAll()
    globalAvroStateManifestCache.invalidateAll()
    globalAvroFileListCache.invalidateAll()
    globalFilteredSchemaCache.invalidateAll()
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
    // Invalidate Avro state manifests for this table (state dir paths contain the table path)
    globalAvroStateManifestCache
      .asMap()
      .keySet()
      .asScala
      .filter(_.contains(tablePath))
      .foreach(globalAvroStateManifestCache.invalidate)
    // Invalidate Avro file lists for this table
    globalAvroFileListCache
      .asMap()
      .keySet()
      .asScala
      .filter(_.tablePath == tablePath)
      .foreach(globalAvroFileListCache.invalidate)
  }

  /** Get global cache statistics */
  def getGlobalCacheStats(): (GuavaCacheStats, GuavaCacheStats, GuavaCacheStats, GuavaCacheStats, GuavaCacheStats) =
    (globalCheckpointActionsCache.stats(), globalLastCheckpointInfoCache.stats(), globalAvroStateManifestCache.stats(), globalAvroFileListCache.stats(), globalFilteredSchemaCache.stats())
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

  /**
   * Get or compute Avro state manifest - caches the StateManifest read from _manifest.json.
   * Uses GLOBAL cache shared across all TransactionLog instances.
   * Key is the full state directory path (e.g., "s3://bucket/table/_transaction_log/state-v00000000000000000100")
   */
  def getOrComputeAvroStateManifest(
    stateDirPath: String,
    compute: => StateManifest
  ): StateManifest =
    Option(EnhancedTransactionLogCache.globalAvroStateManifestCache.getIfPresent(stateDirPath)) match {
      case Some(manifest) =>
        logger.debug(s"GLOBAL Avro state manifest cache HIT for $stateDirPath (version ${manifest.stateVersion})")
        manifest
      case None =>
        logger.debug(s"GLOBAL Avro state manifest cache MISS for $stateDirPath - reading from storage")
        val manifest = compute
        EnhancedTransactionLogCache.globalAvroStateManifestCache.put(stateDirPath, manifest)
        logger.info(s"Cached Avro state manifest in GLOBAL cache for $stateDirPath (version ${manifest.stateVersion}, ${manifest.numFiles} files)")
        manifest
    }

  /** Invalidate Avro state manifest cache for a specific state directory */
  def invalidateAvroStateManifest(stateDirPath: String): Unit = {
    EnhancedTransactionLogCache.globalAvroStateManifestCache.invalidate(stateDirPath)
    logger.debug(s"Invalidated GLOBAL Avro state manifest cache for $stateDirPath")
  }

  /**
   * Get or compute Avro file list - caches the Seq[AddAction] after expensive schema processing.
   * Uses GLOBAL cache shared across all TransactionLog instances.
   * Key is (tablePath, avroVersion) to ensure cache is invalidated when new checkpoints are written.
   *
   * This is critical for performance: the toAddActions() conversion calls filterEmptyObjectMappings()
   * which parses JSON for every file entry. Without this global cache, every query would re-parse.
   */
  def getOrComputeAvroFileList(
    tablePath: String,
    avroVersion: Long,
    compute: => Seq[AddAction]
  ): Seq[AddAction] = {
    import EnhancedTransactionLogCache.AvroFileListKey
    val key = AvroFileListKey(tablePath, avroVersion)
    Option(EnhancedTransactionLogCache.globalAvroFileListCache.getIfPresent(key)) match {
      case Some(files) =>
        logger.debug(s"GLOBAL Avro file list cache HIT for $tablePath version $avroVersion (${files.size} files)")
        files
      case None =>
        logger.debug(s"GLOBAL Avro file list cache MISS for $tablePath version $avroVersion - computing with schema processing")
        val files = compute
        EnhancedTransactionLogCache.globalAvroFileListCache.put(key, files)
        logger.info(s"Cached Avro file list in GLOBAL cache for $tablePath version $avroVersion (${files.size} files)")
        files
    }
  }

  /** Invalidate Avro file list cache for a specific table */
  def invalidateAvroFileList(tablePath: String): Unit = {
    import scala.jdk.CollectionConverters._
    EnhancedTransactionLogCache.globalAvroFileListCache
      .asMap()
      .keySet()
      .asScala
      .filter(_.tablePath == tablePath)
      .foreach(EnhancedTransactionLogCache.globalAvroFileListCache.invalidate)
    logger.debug(s"Invalidated GLOBAL Avro file list cache for $tablePath")
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

    // Also invalidate partition filter evaluation cache
    PartitionFilterCache.invalidate()
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
      avroStateManifestCacheStats = EnhancedTransactionLogCache.globalAvroStateManifestCache.stats(),
      avroFileListCacheStats = EnhancedTransactionLogCache.globalAvroFileListCache.stats(),
      filteredSchemaCacheStats = EnhancedTransactionLogCache.globalFilteredSchemaCache.stats(),
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
    // Also clear partition filter evaluation cache
    PartitionFilterCache.invalidate()
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
    EnhancedTransactionLogCache.globalAvroStateManifestCache.cleanUp()
    EnhancedTransactionLogCache.globalAvroFileListCache.cleanUp()
    EnhancedTransactionLogCache.globalFilteredSchemaCache.cleanUp()
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
  avroStateManifestCacheStats: GuavaCacheStats,
  avroFileListCacheStats: GuavaCacheStats,
  filteredSchemaCacheStats: GuavaCacheStats,
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
      lastCheckpointInfoCacheStats,
      avroStateManifestCacheStats,
      avroFileListCacheStats,
      filteredSchemaCacheStats
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
      lastCheckpointInfoCacheStats,
      avroStateManifestCacheStats,
      avroFileListCacheStats,
      filteredSchemaCacheStats
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

  /** Get Avro state manifest cache hit rate specifically */
  def avroStateManifestHitRate: Double = {
    val requests = avroStateManifestCacheStats.requestCount()
    if (requests == 0) 0.0
    else avroStateManifestCacheStats.hitCount().toDouble / requests
  }

  /** Get Avro file list cache hit rate specifically - critical for avoiding expensive JSON parsing */
  def avroFileListHitRate: Double = {
    val requests = avroFileListCacheStats.requestCount()
    if (requests == 0) 0.0
    else avroFileListCacheStats.hitCount().toDouble / requests
  }

  /** Get filtered schema cache hit rate specifically - avoids JSON parsing in filterEmptyObjectMappings */
  def filteredSchemaHitRate: Double = {
    val requests = filteredSchemaCacheStats.requestCount()
    if (requests == 0) 0.0
    else filteredSchemaCacheStats.hitCount().toDouble / requests
  }
}
