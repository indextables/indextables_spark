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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory
import scala.jdk.CollectionConverters._

/**
 * Factory for creating transaction log instances. Automatically selects optimized or standard implementation based on
 * configuration.
 */
object TransactionLogFactory {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create a transaction log instance with automatic optimization selection.
   *
   * @param tablePath
   *   The path to the table
   * @param spark
   *   The Spark session
   * @param options
   *   Configuration options
   * @return
   *   A TransactionLog instance (optimized or standard)
   */
  def create(
    tablePath: Path,
    spark: SparkSession,
    options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
  ): TransactionLog = {
    logger.debug(s" create method called for path: $tablePath")

    // Check if optimization is explicitly disabled
    val useOptimized = options.getBoolean(
      "spark.indextables.transaction.optimized.enabled",
      options.getBoolean("spark.indextables.transaction.optimized.enabled", true)
    )

    // Also check if caching is disabled - if so, use standard transaction log
    val cacheEnabled = options.getBoolean("spark.indextables.transaction.cache.enabled", true)

    // For very short cache expiration times (like in tests), use standard transaction log
    // since enhanced cache uses minute-based TTL which doesn't work well for sub-minute tests
    val expirationSeconds             = options.getLong("spark.indextables.transaction.cache.expirationSeconds", 300L)
    val useStandardForShortExpiration = expirationSeconds > 0 && expirationSeconds < 60

    val shouldUseOptimized = useOptimized && cacheEnabled && !useStandardForShortExpiration

    logger.debug(s" Creating transaction log for $tablePath (shouldUseOptimized: $shouldUseOptimized)")
    logger.info(s"Creating transaction log for $tablePath (useOptimized: $useOptimized, cacheEnabled: $cacheEnabled, expirationSeconds: $expirationSeconds, useStandardForShortExpiration: $useStandardForShortExpiration, shouldUseOptimized: $shouldUseOptimized)")

    if (shouldUseOptimized) {
      // Create adapter that wraps OptimizedTransactionLog to match TransactionLog interface
      new TransactionLogAdapter(new OptimizedTransactionLog(tablePath, spark, options), spark, options)
    } else {
      // Create standard TransactionLog for testing/compatibility
      logger.info(s"[DEBUG FACTORY] Creating standard TransactionLog for $tablePath")
      logger.debug(s" Creating standard TransactionLog for $tablePath")
      new TransactionLog(
        tablePath,
        spark,
        new CaseInsensitiveStringMap(
          (options.asCaseSensitiveMap().asScala + ("spark.indextables.transaction.allowDirectUsage" -> "true")).asJava
        )
      )
    }
  }
}

/**
 * Adapter to make OptimizedTransactionLog compatible with existing TransactionLog interface. This allows seamless
 * integration without changing existing code.
 */
class TransactionLogAdapter(
  private val optimizedLog: OptimizedTransactionLog,
  spark: SparkSession,
  options: CaseInsensitiveStringMap)
    extends TransactionLog(
      optimizedLog.getTablePath(),
      spark,
      new CaseInsensitiveStringMap(
        (options.asCaseSensitiveMap().asScala + ("spark.indextables.transaction.allowDirectUsage" -> "true")).asJava
      )
    ) {

  private val logger = LoggerFactory.getLogger(classOf[TransactionLogAdapter])

  override def getTablePath(): Path = optimizedLog.getTablePath()

  override def close(): Unit =
    optimizedLog.close()

  override def initialize(schema: org.apache.spark.sql.types.StructType): Unit =
    optimizedLog.initialize(schema)

  override def initialize(schema: org.apache.spark.sql.types.StructType, partitionColumns: Seq[String]): Unit =
    optimizedLog.initialize(schema, partitionColumns)

  override def addFile(addAction: AddAction): Long = {
    val result = optimizedLog.addFiles(Seq(addAction))
    // Invalidate adapter's cache after add operation
    super.invalidateCache()
    result
  }

  override def addFiles(addActions: Seq[AddAction]): Long = {
    val result = optimizedLog.addFiles(addActions)
    // Invalidate adapter's cache after add operation
    super.invalidateCache()
    result
  }

  override def overwriteFiles(addActions: Seq[AddAction]): Long = {
    logger.debug(s" overwriteFiles called with ${addActions.size} actions")
    val result = optimizedLog.overwriteFiles(addActions)
    // Invalidate adapter's cache after overwrite operation
    super.invalidateCache()
    result
  }

  override def listFiles(): Seq[AddAction] =
    optimizedLog.listFiles()

  override def getTotalRowCount(): Long =
    optimizedLog.getTotalRowCount()

  override def getPartitionColumns(): Seq[String] =
    optimizedLog.getPartitionColumns()

  override def isPartitioned(): Boolean =
    optimizedLog.isPartitioned()

  override def getMetadata(): MetadataAction =
    optimizedLog.getMetadata()

  override def getLastCheckpointVersion(): Option[Long] =
    optimizedLog.getLastCheckpointVersion()

  override def getProtocol(): ProtocolAction =
    optimizedLog.getProtocol()

  override def assertTableReadable(): Unit =
    optimizedLog.assertTableReadable()

  override def assertTableWritable(): Unit =
    optimizedLog.assertTableWritable()

  override def upgradeProtocol(newMinReaderVersion: Int, newMinWriterVersion: Int): Unit =
    optimizedLog.upgradeProtocol(newMinReaderVersion, newMinWriterVersion)

  override def prewarmCache(): Unit =
    optimizedLog.prewarmCache()

  override def removeFile(path: String, deletionTimestamp: Long = System.currentTimeMillis()): Long =
    optimizedLog.removeFile(path, deletionTimestamp)

  override def getCacheStats(): Option[CacheStats] = {
    // Convert EnhancedTransactionLogCache statistics to legacy CacheStats format
    val enhancedStats = optimizedLog.getCacheStatistics()

    // Calculate aggregate statistics from all enhanced cache components
    val totalHits = enhancedStats.logCacheStats.hitCount() +
      enhancedStats.snapshotCacheStats.hitCount() +
      enhancedStats.fileListCacheStats.hitCount() +
      enhancedStats.metadataCacheStats.hitCount() +
      enhancedStats.versionCacheStats.hitCount()

    val totalMisses = enhancedStats.logCacheStats.missCount() +
      enhancedStats.snapshotCacheStats.missCount() +
      enhancedStats.fileListCacheStats.missCount() +
      enhancedStats.metadataCacheStats.missCount() +
      enhancedStats.versionCacheStats.missCount()

    val hitRate = if (totalHits + totalMisses > 0) totalHits.toDouble / (totalHits + totalMisses) else 0.0

    // Use request count as a proxy for "versions in cache" since size() is not available
    val versionsInCache = enhancedStats.versionCacheStats.requestCount().toInt

    // Read actual expiration setting from options
    val expirationSeconds = options.getLong("spark.indextables.transaction.cache.expirationSeconds", 5 * 60L) // 5 minutes default

    Some(
      CacheStats(
        hits = totalHits,
        misses = totalMisses,
        hitRate = hitRate,
        versionsInCache = versionsInCache,
        expirationSeconds = expirationSeconds
      )
    )
  }

  override def invalidateCache(): Unit = {
    logger.debug(s" invalidateCache called")
    // Invalidate both the optimized log cache and the parent TransactionLog cache
    optimizedLog.invalidateCache(getTablePath().toString)
    super.invalidateCache()
  }

  // Note: Some methods like mergeFiles, getLatestVersion, readVersion, getVersions
  // are not in the base TransactionLog class but could be added if needed.
  // The OptimizedTransactionLog handles these operations internally.
}
