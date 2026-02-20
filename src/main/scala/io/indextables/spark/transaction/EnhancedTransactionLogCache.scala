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

  // ============================================================================
  // GLOBAL JSON PARSE COUNTER - tracks ALL ObjectMapper.readTree calls
  // ============================================================================
  // This counter is incremented by all JSON parsing operations across the codebase.
  // Use this to verify that caching is working and we're not parsing more than expected.
  private val globalJsonParseCounter = new java.util.concurrent.atomic.AtomicLong(0)

  // Debug mode: when enabled, incrementGlobalJsonParseCounter() throws an exception
  // to capture stack traces showing where JSON parsing is happening
  @volatile private var throwOnJsonParse: Boolean = false

  /** Get the total number of ObjectMapper.readTree calls across all components (for testing) */
  def getGlobalJsonParseCount(): Long = globalJsonParseCounter.get()

  /** Reset the global JSON parse counter (for testing) */
  def resetGlobalJsonParseCounter(): Unit = globalJsonParseCounter.set(0)

  /** Enable debug mode that throws on JSON parse (for stack trace capture) */
  def enableThrowOnJsonParse(): Unit = throwOnJsonParse = true

  /** Disable debug mode that throws on JSON parse */
  def disableThrowOnJsonParse(): Unit = throwOnJsonParse = false

  /** Increment the global JSON parse counter - call this from any ObjectMapper.readTree usage */
  def incrementGlobalJsonParseCounter(): Unit = {
    globalJsonParseCounter.incrementAndGet()
    if (throwOnJsonParse) {
      throw new RuntimeException("DEBUG: JSON parse detected - stack trace for debugging")
    }
  }

  // Global cache key types
  case class CheckpointActionsKey(tablePath: String, checkpointVersion: Long)
  case class LastCheckpointInfoCached(info: Option[LastCheckpointInfo], cachedAt: Long)

  // Configurable TTL for global checkpoint caches (default 5 minutes)
  @volatile private var globalCheckpointCacheTTLMinutes: Long = 5
  @volatile private var globalCheckpointCacheMaxSize: Long    = 200
  @volatile private var cachesInitialized: Boolean            = false

  /**
   * Configure the global checkpoint cache TTL. Must be called before first cache access. Thread-safe but will log a
   * warning if called after caches are already initialized.
   */
  def configureGlobalCacheTTL(ttlMinutes: Long, maxSize: Long = 200): Unit = synchronized {
    if (cachesInitialized) {
      logger.warn(
        s"Global checkpoint caches already initialized with TTL=${globalCheckpointCacheTTLMinutes}min. " +
          s"New TTL=$ttlMinutes will take effect after cache rebuild (clearGlobalCaches + re-access)."
      )
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
      .maximumSize(1000) // Typically few unique schemas even for large tables
      .recordStats()
      .build[String, String]()
  }

  // Global cache for parsed DocMappingMetadata - keyed by schema hash or raw JSON
  // Avoids parsing docMappingJson multiple times to extract field names, fast fields, etc.
  private lazy val _globalDocMappingMetadataCache: Cache[String, DocMappingMetadata] = synchronized {
    cachesInitialized = true
    logger.info(
      s"Initializing global doc mapping metadata cache: TTL=${globalCheckpointCacheTTLMinutes}min, maxSize=1000"
    )
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(globalCheckpointCacheTTLMinutes, TimeUnit.MINUTES)
      .maximumSize(1000) // Typically few unique schemas
      .recordStats()
      .build[String, DocMappingMetadata]()
  }

  // Global cache for Avro manifest file contents - keyed by manifest file path
  // Manifest files are immutable once written, so safe to cache indefinitely
  // This avoids re-reading .avro files on every partition-filtered query
  private lazy val _globalAvroManifestFileCache: Cache[String, Seq[io.indextables.spark.transaction.avro.FileEntry]] =
    synchronized {
      cachesInitialized = true
      logger.info(
        s"Initializing global Avro manifest file cache: TTL=${globalCheckpointCacheTTLMinutes}min, maxSize=500"
      )
      CacheBuilder
        .newBuilder()
        .expireAfterAccess(globalCheckpointCacheTTLMinutes, TimeUnit.MINUTES)
        .maximumSize(500) // Each manifest can be large, limit total cached manifests
        .recordStats()
        .build[String, Seq[io.indextables.spark.transaction.avro.FileEntry]]()
    }

  // Global cache for MetadataAction - keyed by table path
  // Metadata is immutable for a given table version, so safe to cache across TransactionLog instances
  // This avoids re-parsing transaction log JSON when creating new DataFrames on the same table
  private lazy val _globalMetadataCache: Cache[String, MetadataAction] = synchronized {
    cachesInitialized = true
    logger.info(s"Initializing global metadata cache: TTL=${globalCheckpointCacheTTLMinutes}min, maxSize=$globalCheckpointCacheMaxSize")
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(globalCheckpointCacheTTLMinutes, TimeUnit.MINUTES)
      .maximumSize(globalCheckpointCacheMaxSize)
      .recordStats()
      .build[String, MetadataAction]()
  }

  // Global cache for ProtocolAction - keyed by table path
  private lazy val _globalProtocolCache: Cache[String, ProtocolAction] = synchronized {
    cachesInitialized = true
    logger.info(s"Initializing global protocol cache: TTL=${globalCheckpointCacheTTLMinutes}min, maxSize=$globalCheckpointCacheMaxSize")
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(globalCheckpointCacheTTLMinutes, TimeUnit.MINUTES)
      .maximumSize(globalCheckpointCacheMaxSize)
      .recordStats()
      .build[String, ProtocolAction]()
  }

  // Global cache for version actions - keyed by (tablePath, version)
  // Caches parsed actions from transaction log version files
  case class VersionCacheKey(tablePath: String, version: Long)
  private lazy val _globalVersionCache: Cache[VersionCacheKey, Seq[Action]] = synchronized {
    cachesInitialized = true
    logger.info(s"Initializing global version cache: TTL=${globalCheckpointCacheTTLMinutes}min, maxSize=500")
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(globalCheckpointCacheTTLMinutes, TimeUnit.MINUTES)
      .maximumSize(500) // Cache multiple versions across tables
      .recordStats()
      .build[VersionCacheKey, Seq[Action]]()
  }

  // Accessors for the lazy caches
  private[transaction] def globalCheckpointActionsCache: Cache[CheckpointActionsKey, Seq[Action]] =
    _globalCheckpointActionsCache
  private[transaction] def globalLastCheckpointInfoCache: Cache[String, LastCheckpointInfoCached] =
    _globalLastCheckpointInfoCache
  private[transaction] def globalAvroStateManifestCache: Cache[String, StateManifest] = _globalAvroStateManifestCache
  private[transaction] def globalAvroFileListCache: Cache[AvroFileListKey, Seq[AddAction]] = _globalAvroFileListCache
  private[transaction] def globalFilteredSchemaCache: Cache[String, String]                = _globalFilteredSchemaCache
  private[transaction] def globalDocMappingMetadataCache: Cache[String, DocMappingMetadata] =
    _globalDocMappingMetadataCache
  private[transaction] def globalAvroManifestFileCache
    : Cache[String, Seq[io.indextables.spark.transaction.avro.FileEntry]] = _globalAvroManifestFileCache
  private[transaction] def globalMetadataCache: Cache[String, MetadataAction]      = _globalMetadataCache
  private[transaction] def globalProtocolCache: Cache[String, ProtocolAction]      = _globalProtocolCache
  private[transaction] def globalVersionCache: Cache[VersionCacheKey, Seq[Action]] = _globalVersionCache

  /**
   * Get or compute MetadataAction from global cache - shared across all TransactionLog instances. This avoids
   * re-parsing transaction log JSON when creating new DataFrames on the same table.
   */
  def getOrComputeGlobalMetadata(tablePath: String, compute: => MetadataAction): MetadataAction = {
    val cached = globalMetadataCache.getIfPresent(tablePath)
    if (cached != null) {
      cached
    } else {
      logger.debug(s"GLOBAL metadata cache MISS for $tablePath - computing")
      val metadata = compute
      globalMetadataCache.put(tablePath, metadata)
      metadata
    }
  }

  /** Get or compute ProtocolAction from global cache - shared across all TransactionLog instances. */
  def getOrComputeGlobalProtocol(tablePath: String, compute: => ProtocolAction): ProtocolAction = {
    val cached = globalProtocolCache.getIfPresent(tablePath)
    if (cached != null) {
      cached
    } else {
      logger.debug(s"GLOBAL protocol cache MISS for $tablePath - computing")
      val protocol = compute
      globalProtocolCache.put(tablePath, protocol)
      protocol
    }
  }

  /**
   * Get or compute version actions from global cache - shared across all TransactionLog instances. This avoids
   * re-parsing transaction log JSON files when creating new DataFrames on the same table.
   */
  def getOrComputeGlobalVersionActions(
    tablePath: String,
    version: Long,
    compute: => Seq[Action]
  ): Seq[Action] = {
    val key    = VersionCacheKey(tablePath, version)
    val cached = globalVersionCache.getIfPresent(key)
    if (cached != null) {
      cached
    } else {
      logger.debug(s"GLOBAL version cache MISS for $tablePath version $version - computing")
      val actions = compute
      globalVersionCache.put(key, actions)
      actions
    }
  }

  /**
   * Get or compute a filtered schema - caches the result of filterEmptyObjectMappings.
   *
   * The filterEmptyObjectMappings function parses JSON which is expensive. Since the same schema hash always produces
   * the same filtered result, we cache based on the schema hash.
   *
   * @param schemaHash
   *   The schema hash (used as cache key)
   * @param compute
   *   Lazy computation that returns the filtered schema
   * @return
   *   Filtered schema string
   */
  def getOrComputeFilteredSchema(schemaHash: String, compute: => String): String = {
    val cached = globalFilteredSchemaCache.getIfPresent(schemaHash)
    if (cached != null) {
      cached
    } else {
      logger.debug(s"GLOBAL filtered schema cache MISS for hash $schemaHash - computing (cache size=${globalFilteredSchemaCache.size()})")
      val filtered = compute
      globalFilteredSchemaCache.put(schemaHash, filtered)
      filtered
    }
  }

  /**
   * Get or compute DocMappingMetadata - parses docMappingJson once and caches the extracted metadata.
   *
   * This avoids repeated JSON parsing to extract field names, fast fields, etc. The key can be either a schema hash (if
   * available) or the raw JSON string.
   *
   * @param key
   *   The cache key (schema hash or raw JSON)
   * @param docMappingJson
   *   The raw JSON to parse if not cached
   * @return
   *   Parsed DocMappingMetadata
   */
  def getOrComputeDocMappingMetadata(key: String, docMappingJson: String): DocMappingMetadata = {
    val cached = globalDocMappingMetadataCache.getIfPresent(key)
    if (cached != null) {
      cached
    } else {
      val metadata = DocMappingMetadata.parse(docMappingJson)
      globalDocMappingMetadataCache.put(key, metadata)
      metadata
    }
  }

  /** Get DocMappingMetadata for an AddAction, using the schema hash as cache key when available. */
  def getDocMappingMetadata(addAction: AddAction): DocMappingMetadata =
    addAction.docMappingJson match {
      case Some(json) =>
        // Use docMappingRef (schema hash) as key if available, otherwise use the JSON itself
        val key = addAction.docMappingRef.getOrElse(json)
        getOrComputeDocMappingMetadata(key, json)
      case None =>
        DocMappingMetadata.empty
    }

  /**
   * Get or compute Avro manifest file entries - caches the parsed FileEntry records.
   *
   * Avro manifest files (.avro) are immutable once written. Caching their contents avoids re-reading from cloud storage
   * on every partition-filtered query.
   *
   * @param manifestPath
   *   Full path to the manifest file (used as cache key)
   * @param compute
   *   Lazy computation that reads and parses the manifest file
   * @return
   *   Sequence of FileEntry records
   */
  def getOrComputeAvroManifestFile(
    manifestPath: String,
    compute: => Seq[io.indextables.spark.transaction.avro.FileEntry]
  ): Seq[io.indextables.spark.transaction.avro.FileEntry] = {
    val cached = globalAvroManifestFileCache.getIfPresent(manifestPath)
    if (cached != null) {
      cached
    } else {
      logger.debug(s"GLOBAL Avro manifest file cache MISS for $manifestPath - reading from storage")
      val entries = compute
      globalAvroManifestFileCache.put(manifestPath, entries)
      entries
    }
  }

  /** Clear all global caches (for testing) */
  def clearGlobalCaches(): Unit = {
    globalCheckpointActionsCache.invalidateAll()
    globalLastCheckpointInfoCache.invalidateAll()
    globalAvroStateManifestCache.invalidateAll()
    globalAvroFileListCache.invalidateAll()
    globalFilteredSchemaCache.invalidateAll()
    globalDocMappingMetadataCache.invalidateAll()
    globalAvroManifestFileCache.invalidateAll()
    globalMetadataCache.invalidateAll()
    globalProtocolCache.invalidateAll()
    globalVersionCache.invalidateAll()
    logger.info("Cleared all global checkpoint caches")
  }

  /** Invalidate global caches for a specific table */
  def invalidateGlobalCachesForTable(tablePath: String): Unit = {
    globalLastCheckpointInfoCache.invalidate(tablePath)
    globalMetadataCache.invalidate(tablePath)
    globalProtocolCache.invalidate(tablePath)
    import scala.jdk.CollectionConverters._
    globalCheckpointActionsCache
      .asMap()
      .keySet()
      .asScala
      .filter(_.tablePath == tablePath)
      .foreach(globalCheckpointActionsCache.invalidate)
    // Invalidate Avro state manifests for this table (state dir paths start with table path)
    val tablePathWithSep = if (tablePath.endsWith("/")) tablePath else tablePath + "/"
    globalAvroStateManifestCache
      .asMap()
      .keySet()
      .asScala
      .filter(k => k.startsWith(tablePathWithSep) || k == tablePath)
      .foreach(globalAvroStateManifestCache.invalidate)
    // Invalidate Avro file lists for this table
    globalAvroFileListCache
      .asMap()
      .keySet()
      .asScala
      .filter(_.tablePath == tablePath)
      .foreach(globalAvroFileListCache.invalidate)
    // Invalidate version cache entries for this table
    globalVersionCache
      .asMap()
      .keySet()
      .asScala
      .filter(_.tablePath == tablePath)
      .foreach(globalVersionCache.invalidate)
  }

  /** Get global cache statistics */
  def getGlobalCacheStats()
    : (GuavaCacheStats, GuavaCacheStats, GuavaCacheStats, GuavaCacheStats, GuavaCacheStats, GuavaCacheStats) =
    (
      globalCheckpointActionsCache.stats(),
      globalLastCheckpointInfoCache.stats(),
      globalAvroStateManifestCache.stats(),
      globalAvroFileListCache.stats(),
      globalFilteredSchemaCache.stats(),
      globalAvroManifestFileCache.stats()
    )
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
  ): MetadataAction =
    // Delegate to GLOBAL cache for cross-TransactionLog caching
    EnhancedTransactionLogCache.getOrComputeGlobalMetadata(tablePath, compute)

  /** Get or compute protocol */
  def getOrComputeProtocol(
    tablePath: String,
    compute: => ProtocolAction
  ): ProtocolAction =
    // Delegate to GLOBAL cache for cross-TransactionLog caching
    EnhancedTransactionLogCache.getOrComputeGlobalProtocol(tablePath, compute)

  /** Get or compute version actions */
  def getOrComputeVersionActions(
    tablePath: String,
    version: Long,
    compute: => Seq[Action]
  ): Seq[Action] =
    // Delegate to GLOBAL cache for cross-TransactionLog caching
    EnhancedTransactionLogCache.getOrComputeGlobalVersionActions(tablePath, version, compute)

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
            logger.debug(
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
        logger.debug(s"Cached last checkpoint info in GLOBAL cache for $tablePath: ${info.map(_.version)}")
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
   * Get or compute Avro state manifest - caches the StateManifest read from _manifest.avro. Uses GLOBAL cache shared
   * across all TransactionLog instances. Key is the full state directory path (e.g.,
   * "s3://bucket/table/_transaction_log/state-v00000000000000000100")
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
        logger.debug(s"Cached Avro state manifest in GLOBAL cache for $stateDirPath (version ${manifest.stateVersion}, ${manifest.numFiles} files)")
        manifest
    }

  /** Invalidate Avro state manifest cache for a specific state directory */
  def invalidateAvroStateManifest(stateDirPath: String): Unit = {
    EnhancedTransactionLogCache.globalAvroStateManifestCache.invalidate(stateDirPath)
    logger.debug(s"Invalidated GLOBAL Avro state manifest cache for $stateDirPath")
  }

  /**
   * Get or compute Avro file list - caches the Seq[AddAction] after expensive schema processing. Uses GLOBAL cache
   * shared across all TransactionLog instances. Key is (tablePath, avroVersion) to ensure cache is invalidated when new
   * checkpoints are written.
   *
   * This is critical for performance: the toAddActions() conversion calls filterEmptyObjectMappings() which parses JSON
   * for every file entry. Without this global cache, every query would re-parse.
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
        logger.debug(
          s"GLOBAL Avro file list cache MISS for $tablePath version $avroVersion - computing with schema processing"
        )
        val files = compute
        EnhancedTransactionLogCache.globalAvroFileListCache.put(key, files)
        logger.debug(s"Cached Avro file list in GLOBAL cache for $tablePath version $avroVersion (${files.size} files)")
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
      // Use global cache stats since metadata/protocol/version now use global caches
      metadataCacheStats = EnhancedTransactionLogCache.globalMetadataCache.stats(),
      versionCacheStats = EnhancedTransactionLogCache.globalVersionCache.stats(),
      checkpointCacheStats = checkpointCache.stats(),
      protocolCacheStats = EnhancedTransactionLogCache.globalProtocolCache.stats(),
      checkpointActionsCacheStats = EnhancedTransactionLogCache.globalCheckpointActionsCache.stats(),
      lastCheckpointInfoCacheStats = EnhancedTransactionLogCache.globalLastCheckpointInfoCache.stats(),
      avroStateManifestCacheStats = EnhancedTransactionLogCache.globalAvroStateManifestCache.stats(),
      avroFileListCacheStats = EnhancedTransactionLogCache.globalAvroFileListCache.stats(),
      filteredSchemaCacheStats = EnhancedTransactionLogCache.globalFilteredSchemaCache.stats(),
      avroManifestFileCacheStats = EnhancedTransactionLogCache.globalAvroManifestFileCache.stats(),
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
    EnhancedTransactionLogCache.globalAvroManifestFileCache.cleanUp()
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
  avroManifestFileCacheStats: GuavaCacheStats,
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
      filteredSchemaCacheStats,
      avroManifestFileCacheStats
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
      filteredSchemaCacheStats,
      avroManifestFileCacheStats
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

  /** Get Avro manifest file cache hit rate - avoids re-reading .avro files from cloud storage */
  def avroManifestFileHitRate: Double = {
    val requests = avroManifestFileCacheStats.requestCount()
    if (requests == 0) 0.0
    else avroManifestFileCacheStats.hitCount().toDouble / requests
  }
}
