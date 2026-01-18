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

import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.slf4j.LoggerFactory

/**
 * Time-based cache for transaction log data with configurable expiration. Designed to reduce repeated
 * CloudStorageProvider calls for read operations.
 */
class TransactionLogCache(expirationSeconds: Long = 5 * 60L) {

  private val expirationMillis = expirationSeconds * 1000L

  private val logger = LoggerFactory.getLogger(classOf[TransactionLogCache])

  // Cache entries with expiration timestamps
  private val versionCache                            = new ConcurrentHashMap[Long, CacheEntry[Seq[Action]]]()
  private val versionsListCache                       = new AtomicLong(0L) // timestamp when versions list was cached
  private var cachedVersionsList: Option[Seq[Long]]   = None
  private val filesCache                              = new AtomicLong(0L) // timestamp when files list was cached
  private var cachedFilesList: Option[Seq[AddAction]] = None
  private val metadataCache                           = new AtomicLong(0L) // timestamp when metadata was cached
  private var cachedMetadata: Option[MetadataAction]  = None
  private val protocolCache                           = new AtomicLong(0L) // timestamp when protocol was cached
  private var cachedProtocol: Option[ProtocolAction]  = None

  // Partition index cache for optimized partition pruning
  private val partitionIndexCache                       = new AtomicLong(0L) // timestamp when partition index was cached
  private var cachedPartitionIndex: Option[PartitionIndex] = None

  // Statistics
  private val hitCount  = new AtomicLong(0)
  private val missCount = new AtomicLong(0)

  // Background cleanup task
  private val cleanupExecutor = Executors.newSingleThreadScheduledExecutor { r =>
    val thread = new Thread(r, "TransactionLogCache-Cleanup")
    thread.setDaemon(true)
    thread
  }

  // Schedule cleanup every minute
  cleanupExecutor.scheduleAtFixedRate(() => cleanup(), 1, 1, TimeUnit.MINUTES)

  /** Cache entry with expiration timestamp */
  private case class CacheEntry[T](value: T, timestamp: Long) {
    def isExpired(currentTime: Long): Boolean = currentTime - timestamp > expirationMillis
  }

  /** Get cached version data or None if not cached/expired */
  def getCachedVersion(version: Long): Option[Seq[Action]] = {
    val currentTime = System.currentTimeMillis()
    Option(versionCache.get(version)).flatMap { entry =>
      if (entry.isExpired(currentTime)) {
        versionCache.remove(version)
        missCount.incrementAndGet()
        None
      } else {
        hitCount.incrementAndGet()
        Some(entry.value)
      }
    }
  }

  /** Cache version data */
  def cacheVersion(version: Long, actions: Seq[Action]): Unit = {
    val entry = CacheEntry(actions, System.currentTimeMillis())
    versionCache.put(version, entry)
    logger.debug(s"Cached version $version with ${actions.length} actions")
  }

  /** Get cached versions list or None if not cached/expired */
  def getCachedVersions(): Option[Seq[Long]] = {
    val currentTime = System.currentTimeMillis()
    val lastUpdate  = versionsListCache.get()

    if (currentTime - lastUpdate > expirationMillis || cachedVersionsList.isEmpty) {
      missCount.incrementAndGet()
      None
    } else {
      hitCount.incrementAndGet()
      cachedVersionsList
    }
  }

  /** Cache versions list */
  def cacheVersions(versions: Seq[Long]): Unit =
    synchronized {
      cachedVersionsList = Some(versions)
      versionsListCache.set(System.currentTimeMillis())
      logger.debug(s"Cached versions list: ${versions.length} versions")
    }

  /** Get cached files list or None if not cached/expired */
  def getCachedFiles(): Option[Seq[AddAction]] = {
    val currentTime = System.currentTimeMillis()
    val lastUpdate  = filesCache.get()

    if (currentTime - lastUpdate > expirationMillis || cachedFilesList.isEmpty) {
      missCount.incrementAndGet()
      None
    } else {
      hitCount.incrementAndGet()
      cachedFilesList
    }
  }

  /** Cache files list */
  def cacheFiles(files: Seq[AddAction]): Unit =
    synchronized {
      cachedFilesList = Some(files)
      filesCache.set(System.currentTimeMillis())
      logger.debug(s"Cached files list: ${files.length} files")
    }

  /** Get cached metadata or None if not cached/expired */
  def getCachedMetadata(): Option[MetadataAction] = {
    val currentTime = System.currentTimeMillis()
    val lastUpdate  = metadataCache.get()

    if (currentTime - lastUpdate > expirationMillis || cachedMetadata.isEmpty) {
      missCount.incrementAndGet()
      None
    } else {
      hitCount.incrementAndGet()
      cachedMetadata
    }
  }

  /** Cache metadata */
  def cacheMetadata(metadata: MetadataAction): Unit =
    synchronized {
      cachedMetadata = Some(metadata)
      metadataCache.set(System.currentTimeMillis())
      logger.debug(s"Cached metadata: ${metadata.id}")
    }

  /** Get cached protocol or None if not cached/expired */
  def getCachedProtocol(): Option[ProtocolAction] = {
    val currentTime = System.currentTimeMillis()
    val lastUpdate  = protocolCache.get()

    if (currentTime - lastUpdate > expirationMillis || cachedProtocol.isEmpty) {
      missCount.incrementAndGet()
      None
    } else {
      hitCount.incrementAndGet()
      cachedProtocol
    }
  }

  /** Cache protocol */
  def cacheProtocol(protocol: ProtocolAction): Unit =
    synchronized {
      cachedProtocol = Some(protocol)
      protocolCache.set(System.currentTimeMillis())
      logger.debug(s"Cached protocol: ${protocol.minReaderVersion}/${protocol.minWriterVersion}")
    }

  /** Invalidate protocol cache only (useful when protocol is upgraded) */
  def invalidateProtocol(): Unit = {
    synchronized {
      cachedProtocol = None
      protocolCache.set(0)
    }
    logger.debug("Invalidated protocol cache")
  }

  /** Get cached partition index or None if not cached/expired */
  def getCachedPartitionIndex(): Option[PartitionIndex] = {
    val currentTime = System.currentTimeMillis()
    val lastUpdate  = partitionIndexCache.get()

    if (currentTime - lastUpdate > expirationMillis || cachedPartitionIndex.isEmpty) {
      missCount.incrementAndGet()
      None
    } else {
      hitCount.incrementAndGet()
      cachedPartitionIndex
    }
  }

  /** Cache partition index */
  def cachePartitionIndex(index: PartitionIndex): Unit =
    synchronized {
      cachedPartitionIndex = Some(index)
      partitionIndexCache.set(System.currentTimeMillis())
      logger.debug(s"Cached partition index: ${index.partitionCount} partitions, ${index.totalFiles} files")
    }

  /** Invalidate partition index cache */
  def invalidatePartitionIndex(): Unit =
    synchronized {
      cachedPartitionIndex = None
      partitionIndexCache.set(0)
      // Also invalidate the filter evaluation cache
      PartitionFilterCache.invalidate()
    }

  /** Invalidate all caches (useful after write operations) */
  def invalidateAll(): Unit = {
    versionCache.clear()
    synchronized {
      cachedVersionsList = None
      cachedFilesList = None
      cachedMetadata = None
      cachedProtocol = None
      cachedPartitionIndex = None
      versionsListCache.set(0)
      filesCache.set(0)
      metadataCache.set(0)
      protocolCache.set(0)
      partitionIndexCache.set(0)
    }
    // Also invalidate partition filter evaluation cache
    PartitionFilterCache.invalidate()
    logger.debug("Invalidated all caches")
  }

  /** Invalidate caches that depend on the versions list (called when new versions are added) */
  def invalidateVersionDependentCaches(): Unit = {
    synchronized {
      cachedVersionsList = None
      cachedFilesList = None
      cachedMetadata = None
      cachedProtocol = None
      cachedPartitionIndex = None
      versionsListCache.set(0)
      filesCache.set(0)
      metadataCache.set(0)
      protocolCache.set(0)
      partitionIndexCache.set(0)
    }
    // Also invalidate partition filter evaluation cache
    PartitionFilterCache.invalidate()
    logger.debug("Invalidated version-dependent caches")
  }

  /** Get cache statistics */
  def getStats(): CacheStats = {
    val hits    = hitCount.get()
    val misses  = missCount.get()
    val hitRate = if (hits + misses > 0) hits.toDouble / (hits + misses) else 0.0

    CacheStats(
      hits = hits,
      misses = misses,
      hitRate = hitRate,
      versionsInCache = versionCache.size(),
      expirationSeconds = expirationSeconds
    )
  }

  /** Clean up expired entries */
  private def cleanup(): Unit = {
    val currentTime  = System.currentTimeMillis()
    var removedCount = 0

    // Clean up version cache
    val expiredVersions = versionCache.asScala
      .filter {
        case (_, entry) =>
          entry.isExpired(currentTime)
      }
      .keys
      .toSeq

    expiredVersions.foreach { version =>
      versionCache.remove(version)
      removedCount += 1
    }

    if (removedCount > 0) {
      logger.debug(s"Cleaned up $removedCount expired cache entries")
    }
  }

  /** Shutdown the cache and cleanup resources */
  def shutdown(): Unit = {
    try {
      cleanupExecutor.shutdown()
      if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        cleanupExecutor.shutdownNow()
      }
    } catch {
      case _: InterruptedException =>
        cleanupExecutor.shutdownNow()
        Thread.currentThread().interrupt()
    }

    versionCache.clear()
    invalidateAll()
    logger.debug("Transaction log cache shut down")
  }
}

/** Cache statistics */
case class CacheStats(
  hits: Long,
  misses: Long,
  hitRate: Double,
  versionsInCache: Int,
  expirationSeconds: Long) {
  override def toString: String =
    f"CacheStats(hits=$hits, misses=$misses, hitRate=${hitRate * 100}%.1f%%, " +
      f"versionsInCache=$versionsInCache, expirationSecs=$expirationSeconds)"
}
