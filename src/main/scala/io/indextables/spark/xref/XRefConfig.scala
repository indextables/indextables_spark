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

package io.indextables.spark.xref

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Filter type for XRef Binary Fuse filters.
 *
 * Controls the trade-off between filter size and false positive rate (FPR).
 */
sealed trait XRefFilterType {
  def value: String
  def falsePositiveRate: Double
  def bytesPerKey: Double
}

object XRefFilterType {
  /**
   * 8-bit fingerprints (~1.24 bytes per key).
   * - False positive rate: ~0.39% (1/256)
   * - Best for: Most use cases, good balance of size and accuracy
   */
  case object Fuse8 extends XRefFilterType {
    val value = "fuse8"
    val falsePositiveRate = 1.0 / 256   // ~0.39%
    val bytesPerKey = 1.24
  }

  /**
   * 16-bit fingerprints (~2.24 bytes per key).
   * - False positive rate: ~0.0015% (1/65536)
   * - Best for: When minimizing false positives is critical
   */
  case object Fuse16 extends XRefFilterType {
    val value = "fuse16"
    val falsePositiveRate = 1.0 / 65536 // ~0.0015%
    val bytesPerKey = 2.24
  }

  /**
   * Parse filter type from string (case-insensitive).
   */
  def fromString(s: String): XRefFilterType = s.toLowerCase match {
    case "fuse8"  => Fuse8
    case "fuse16" => Fuse16
    case other    => throw new IllegalArgumentException(s"Unknown filter type: $other. Valid values: fuse8, fuse16")
  }

  /**
   * Default filter type (Fuse8).
   */
  val Default: XRefFilterType = Fuse8
}

/**
 * Configuration for XRef auto-indexing during transaction commits.
 *
 * @param enabled
 *   Whether auto-indexing is enabled (default: false)
 * @param maxSourceSplits
 *   Maximum splits per XRef (default: 1024)
 * @param minSplitsToTrigger
 *   Minimum splits to trigger XRef build (default: 10)
 * @param minUncoveredSplitsToTrigger
 *   Minimum uncovered splits to trigger auto-indexing (default: 10)
 * @param minIntervalMs
 *   Minimum interval between auto-index runs in milliseconds (default: 60000 = 1 minute)
 * @param rebuildOnSourceChange
 *   Whether to rebuild XRef when source splits change (default: true)
 */
case class XRefAutoIndexConfig(
  enabled: Boolean = false, // Disabled by default
  maxSourceSplits: Int = 1024,
  minSplitsToTrigger: Int = 10,
  minUncoveredSplitsToTrigger: Int = 10,
  minIntervalMs: Long = 60000L,
  rebuildOnSourceChange: Boolean = true
)

/**
 * Configuration for XRef build operations.
 *
 * Note: FuseXRef (Binary Fuse Filter) implementation does NOT support includePositions.
 * Positions are not stored in Fuse filters. This parameter was removed in the FuseXRef migration.
 *
 * @param parallelism
 *   Parallelism for XRef builds (default: auto)
 * @param tempDirectoryPath
 *   Temporary directory for builds (default: auto-detect, falls back to indexWriter.tempDirectoryPath)
 * @param heapSize
 *   Heap size for XRef builds in bytes (default: auto, falls back to indexWriter.heapSize)
 * @param distributedBuild
 *   Whether to use distributed XRef build on executors (default: true)
 *   When enabled, XRef builds run on executors with locality preferences based on
 *   which nodes have the source splits cached.
 * @param maxSourceSplits
 *   Maximum source splits per XRef (default: 1024).
 *   Applies to all XRef builds including INDEX CROSSREFERENCES SQL command and auto-indexing.
 *   Larger values create fewer, larger XRefs. Smaller values create more, smaller XRefs.
 * @param maxXRefsPerRun
 *   Maximum number of XRefs to build in a single INDEX CROSSREFERENCES or auto-index run (default: None = unlimited).
 *   When set, limits how many XRefs are created per operation, allowing incremental XRef building
 *   over multiple runs rather than one large operation. Remaining splits will be indexed in subsequent runs.
 * @param batchSize
 *   Number of XRefs to build and commit atomically per batch (default: None = 2x defaultParallelism).
 *   Larger batches reduce transaction overhead but increase risk of partial failures.
 *   Similar to MERGE SPLITS batch behavior for consistent transaction granularity.
 * @param filterType
 *   Filter type for Binary Fuse filters (default: Fuse8).
 *   - Fuse8: ~0.39% FPR, ~1.24 bytes/key (good balance)
 *   - Fuse16: ~0.0015% FPR, ~2.24 bytes/key (260x fewer false positives)
 */
case class XRefBuildConfig(
  parallelism: Option[Int] = None,
  tempDirectoryPath: Option[String] = None,
  heapSize: Option[Long] = None,
  distributedBuild: Boolean = true,
  maxSourceSplits: Int = 1024,
  maxXRefsPerRun: Option[Int] = None,
  batchSize: Option[Int] = None,
  filterType: XRefFilterType = XRefFilterType.Default
)

/**
 * Configuration for XRef query-time operations.
 *
 * @param enabled
 *   Whether XRef pre-scan is enabled at query time (default: true)
 * @param minSplitsForXRef
 *   Minimum candidate splits to trigger XRef check (default: 128)
 * @param timeoutMs
 *   XRef query timeout in milliseconds (default: 5000)
 * @param fallbackOnError
 *   Whether to fallback to full scan on XRef error (default: true)
 * @param distributedSearch
 *   Whether to use distributed XRef search on executors (default: true)
 *   When enabled, XRef searches run on executors with locality preferences based on
 *   which nodes have the XRef splits cached.
 */
case class XRefQueryConfig(
  enabled: Boolean = true,
  minSplitsForXRef: Int = 128,
  timeoutMs: Int = 5000,
  fallbackOnError: Boolean = true,
  distributedSearch: Boolean = true
)

/**
 * Configuration for XRef storage.
 *
 * @param directory
 *   XRef storage directory relative to table root (default: "_xrefsplits")
 * @param compressionEnabled
 *   Whether to compress XRef splits (default: true)
 */
case class XRefStorageConfig(
  directory: String = "_xrefsplits",
  compressionEnabled: Boolean = true
)

/**
 * Configuration for XRef local cache on executors.
 *
 * When enabled, executors lazily download XRef splits from cloud storage to local disk
 * before searching. This improves performance for repeated queries by avoiding repeated
 * cloud storage reads.
 *
 * @param enabled
 *   Whether local caching is enabled (default: false)
 *   When disabled, XRef splits are accessed directly from their storage location.
 * @param directory
 *   Local cache directory on executors (default: auto-detect)
 *   Falls back to: xref.build.tempDirectoryPath -> indexWriter.tempDirectoryPath -> /local_disk0 -> system temp
 *   XRef splits are stored in a "_xref_cache" subdirectory.
 */
case class XRefLocalCacheConfig(
  enabled: Boolean = false,
  directory: Option[String] = None
)

/**
 * Complete XRef configuration combining all aspects.
 */
case class XRefConfig(
  autoIndex: XRefAutoIndexConfig = XRefAutoIndexConfig(),
  build: XRefBuildConfig = XRefBuildConfig(),
  query: XRefQueryConfig = XRefQueryConfig(),
  storage: XRefStorageConfig = XRefStorageConfig(),
  localCache: XRefLocalCacheConfig = XRefLocalCacheConfig()
)

object XRefConfig {

  // Configuration keys
  object Keys {
    // Auto-index configuration
    val AUTO_INDEX_ENABLED                   = "spark.indextables.xref.autoIndex.enabled"
    val AUTO_INDEX_MAX_SOURCE_SPLITS         = "spark.indextables.xref.autoIndex.maxSourceSplits"
    val AUTO_INDEX_MIN_SPLITS                = "spark.indextables.xref.autoIndex.minSplitsToTrigger"
    val AUTO_INDEX_MIN_UNCOVERED_SPLITS      = "spark.indextables.xref.autoIndex.minUncoveredSplitsToTrigger"
    val AUTO_INDEX_MIN_INTERVAL_MS           = "spark.indextables.xref.autoIndex.minIntervalMs"
    val AUTO_INDEX_REBUILD_ON_CHANGE         = "spark.indextables.xref.autoIndex.rebuildOnSourceChange"

    // Build configuration
    // Note: BUILD_INCLUDE_POSITIONS was removed - FuseXRef doesn't support positions
    val BUILD_PARALLELISM        = "spark.indextables.xref.build.parallelism"
    val BUILD_TEMP_DIRECTORY     = "spark.indextables.xref.build.tempDirectoryPath"
    val BUILD_HEAP_SIZE          = "spark.indextables.xref.build.heapSize"
    val BUILD_DISTRIBUTED        = "spark.indextables.xref.build.distributed"
    val BUILD_MAX_SOURCE_SPLITS  = "spark.indextables.xref.build.maxSourceSplits"
    val BUILD_MAX_XREFS_PER_RUN  = "spark.indextables.xref.build.maxXRefsPerRun"
    val BUILD_BATCH_SIZE         = "spark.indextables.xref.build.batchSize"
    val BUILD_FILTER_TYPE        = "spark.indextables.xref.build.filterType"

    // Query configuration
    val QUERY_ENABLED           = "spark.indextables.xref.query.enabled"
    val QUERY_MIN_SPLITS        = "spark.indextables.xref.query.minSplitsForXRef"
    val QUERY_TIMEOUT_MS        = "spark.indextables.xref.query.timeoutMs"
    val QUERY_FALLBACK_ON_ERROR = "spark.indextables.xref.query.fallbackOnError"
    val QUERY_DISTRIBUTED       = "spark.indextables.xref.query.distributed"

    // Storage configuration
    val STORAGE_DIRECTORY   = "spark.indextables.xref.storage.directory"
    val STORAGE_COMPRESSION = "spark.indextables.xref.storage.compressionEnabled"

    // Local cache configuration
    val LOCAL_CACHE_ENABLED   = "spark.indextables.xref.localCache.enabled"
    val LOCAL_CACHE_DIRECTORY = "spark.indextables.xref.localCache.directory"
  }

  /**
   * Create XRefConfig from SparkSession configuration.
   */
  def fromSparkSession(spark: SparkSession): XRefConfig = {
    val conf = spark.conf

    XRefConfig(
      autoIndex = XRefAutoIndexConfig(
        enabled = conf.get(Keys.AUTO_INDEX_ENABLED, "false").toBoolean,
        maxSourceSplits = conf.get(Keys.AUTO_INDEX_MAX_SOURCE_SPLITS, "1024").toInt,
        minSplitsToTrigger = conf.get(Keys.AUTO_INDEX_MIN_SPLITS, "10").toInt,
        minUncoveredSplitsToTrigger = conf.get(Keys.AUTO_INDEX_MIN_UNCOVERED_SPLITS, "10").toInt,
        minIntervalMs = conf.get(Keys.AUTO_INDEX_MIN_INTERVAL_MS, "60000").toLong,
        rebuildOnSourceChange = conf.get(Keys.AUTO_INDEX_REBUILD_ON_CHANGE, "true").toBoolean
      ),
      build = XRefBuildConfig(
        parallelism = Option(conf.get(Keys.BUILD_PARALLELISM, null)).map(_.toInt),
        tempDirectoryPath = Option(conf.get(Keys.BUILD_TEMP_DIRECTORY, null)),
        heapSize = Option(conf.get(Keys.BUILD_HEAP_SIZE, null)).map(parseSize),
        distributedBuild = conf.get(Keys.BUILD_DISTRIBUTED, "true").toBoolean,
        // New location with fallback to old autoIndex location for backward compatibility
        maxSourceSplits = Option(conf.get(Keys.BUILD_MAX_SOURCE_SPLITS, null))
          .orElse(Option(conf.get(Keys.AUTO_INDEX_MAX_SOURCE_SPLITS, null)))
          .map(_.toInt)
          .getOrElse(1024),
        maxXRefsPerRun = Option(conf.get(Keys.BUILD_MAX_XREFS_PER_RUN, null)).map(_.toInt),
        batchSize = Option(conf.get(Keys.BUILD_BATCH_SIZE, null)).map(_.toInt),
        filterType = Option(conf.get(Keys.BUILD_FILTER_TYPE, null))
          .map(XRefFilterType.fromString)
          .getOrElse(XRefFilterType.Default)
      ),
      query = XRefQueryConfig(
        enabled = conf.get(Keys.QUERY_ENABLED, "true").toBoolean,
        minSplitsForXRef = conf.get(Keys.QUERY_MIN_SPLITS, "128").toInt,
        timeoutMs = conf.get(Keys.QUERY_TIMEOUT_MS, "5000").toInt,
        fallbackOnError = conf.get(Keys.QUERY_FALLBACK_ON_ERROR, "true").toBoolean,
        distributedSearch = conf.get(Keys.QUERY_DISTRIBUTED, "true").toBoolean
      ),
      storage = XRefStorageConfig(
        directory = conf.get(Keys.STORAGE_DIRECTORY, "_xrefsplits"),
        compressionEnabled = conf.get(Keys.STORAGE_COMPRESSION, "true").toBoolean
      ),
      localCache = XRefLocalCacheConfig(
        enabled = conf.get(Keys.LOCAL_CACHE_ENABLED, "false").toBoolean,
        directory = Option(conf.get(Keys.LOCAL_CACHE_DIRECTORY, null))
      )
    )
  }

  /**
   * Create XRefConfig from CaseInsensitiveStringMap options.
   */
  def fromOptions(options: CaseInsensitiveStringMap): XRefConfig =
    XRefConfig(
      autoIndex = XRefAutoIndexConfig(
        enabled = options.getBoolean(Keys.AUTO_INDEX_ENABLED, false),
        maxSourceSplits = options.getInt(Keys.AUTO_INDEX_MAX_SOURCE_SPLITS, 1024),
        minSplitsToTrigger = options.getInt(Keys.AUTO_INDEX_MIN_SPLITS, 10),
        minUncoveredSplitsToTrigger = options.getInt(Keys.AUTO_INDEX_MIN_UNCOVERED_SPLITS, 10),
        minIntervalMs = options.getLong(Keys.AUTO_INDEX_MIN_INTERVAL_MS, 60000L),
        rebuildOnSourceChange = options.getBoolean(Keys.AUTO_INDEX_REBUILD_ON_CHANGE, true)
      ),
      build = XRefBuildConfig(
        parallelism = Option(options.get(Keys.BUILD_PARALLELISM)).map(_.toInt),
        tempDirectoryPath = Option(options.get(Keys.BUILD_TEMP_DIRECTORY)),
        heapSize = Option(options.get(Keys.BUILD_HEAP_SIZE)).map(parseSize),
        distributedBuild = options.getBoolean(Keys.BUILD_DISTRIBUTED, true),
        // New location with fallback to old autoIndex location for backward compatibility
        maxSourceSplits = Option(options.get(Keys.BUILD_MAX_SOURCE_SPLITS))
          .orElse(Option(options.get(Keys.AUTO_INDEX_MAX_SOURCE_SPLITS)))
          .map(_.toInt)
          .getOrElse(1024),
        maxXRefsPerRun = Option(options.get(Keys.BUILD_MAX_XREFS_PER_RUN)).map(_.toInt),
        batchSize = Option(options.get(Keys.BUILD_BATCH_SIZE)).map(_.toInt),
        filterType = Option(options.get(Keys.BUILD_FILTER_TYPE))
          .map(XRefFilterType.fromString)
          .getOrElse(XRefFilterType.Default)
      ),
      query = XRefQueryConfig(
        enabled = options.getBoolean(Keys.QUERY_ENABLED, true),
        minSplitsForXRef = options.getInt(Keys.QUERY_MIN_SPLITS, 128),
        timeoutMs = options.getInt(Keys.QUERY_TIMEOUT_MS, 5000),
        fallbackOnError = options.getBoolean(Keys.QUERY_FALLBACK_ON_ERROR, true),
        distributedSearch = options.getBoolean(Keys.QUERY_DISTRIBUTED, true)
      ),
      storage = XRefStorageConfig(
        directory = options.getOrDefault(Keys.STORAGE_DIRECTORY, "_xrefsplits"),
        compressionEnabled = options.getBoolean(Keys.STORAGE_COMPRESSION, true)
      ),
      localCache = XRefLocalCacheConfig(
        enabled = options.getBoolean(Keys.LOCAL_CACHE_ENABLED, false),
        directory = Option(options.get(Keys.LOCAL_CACHE_DIRECTORY))
      )
    )

  /**
   * Create default configuration.
   */
  def default(): XRefConfig = XRefConfig()

  // Use SizeParser utility for parsing size strings
  private def parseSize(value: String): Long =
    io.indextables.spark.util.SizeParser.parseSize(value)
}
