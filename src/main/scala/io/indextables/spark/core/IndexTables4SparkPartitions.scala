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

package io.indextables.spark.core

import java.io.IOException
import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.prewarm.PreWarmManager
import io.indextables.spark.search.{SplitSearchEngine, TantivySearchEngine}
import io.indextables.spark.storage.SplitCacheConfig
import io.indextables.spark.transaction.{AddAction, PartitionUtils}
import io.indextables.spark.util.StatisticsCalculator
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import org.slf4j.LoggerFactory

/** Utility for consistent path resolution across different scan types. */
object PathResolutionUtils {

  /**
   * Resolves a path from AddAction against a table path, handling absolute and relative paths correctly.
   *
   * @param splitPath
   *   Path from AddAction (could be relative like "part-00000-xxx.split" or absolute)
   * @param tablePath
   *   Base table path for resolving relative paths
   * @return
   *   Resolved Hadoop Path object
   */
  def resolveSplitPath(splitPath: String, tablePath: String): Path =
    if (isAbsolutePath(splitPath)) {
      // Already absolute path - handle file:/ URIs properly
      if (splitPath.startsWith("file:")) {
        // For file:/ URIs, use the URI directly rather than Hadoop Path constructor
        // to avoid path resolution issues
        new Path(java.net.URI.create(splitPath))
      } else {
        new Path(splitPath)
      }
    } else {
      // Relative path, resolve against table path
      new Path(tablePath, splitPath)
    }

  /**
   * Resolves a path and returns it as a string suitable for tantivy4java.
   *
   * @param splitPath
   *   Path from AddAction
   * @param tablePath
   *   Base table path for resolving relative paths
   * @return
   *   Resolved path as string
   */
  def resolveSplitPathAsString(splitPath: String, tablePath: String): String =
    if (isAbsolutePath(splitPath)) {
      // Already absolute path - handle file:/ URIs properly
      if (splitPath.startsWith("file:")) {
        // Keep file URIs as URIs for tantivy4java to avoid working directory resolution issues
        splitPath
      } else {
        splitPath
      }
    } else {
      // Relative path, resolve against table path
      // Handle case where tablePath might already be a file:/ URI to avoid double-prefixing
      if (tablePath.startsWith("file:")) {
        // Convert file URI to local path, resolve, then convert back to avoid Path constructor issues
        val tableDirPath = new java.io.File(java.net.URI.create(tablePath)).getAbsolutePath
        new java.io.File(tableDirPath, splitPath).getAbsolutePath
      } else {
        new Path(tablePath, splitPath).toString
      }
    }

  /** Checks if a path is absolute (starts with "/", contains "://" for URLs, or starts with "file:"). */
  private def isAbsolutePath(path: String): Boolean =
    path.startsWith("/") || path.contains("://") || path.startsWith("file:")
}

class IndexTables4SparkInputPartition(
  val addAction: AddAction,
  val readSchema: StructType,
  val fullTableSchema: StructType, // Full table schema for type lookup (filters may reference non-projected columns)
  val filters: Array[Filter],
  val partitionId: Int,
  val limit: Option[Int] = None,
  val indexQueryFilters: Array[Any] = Array.empty,
  val preferredHost: Option[String] = None)
    extends InputPartition {

  /**
   * Provide preferred locations for this partition based on driver-side split assignment. The preferredHost is computed
   * during partition planning using per-query load balancing while maintaining sticky assignments for cache locality.
   */
  override def preferredLocations(): Array[String] =
    preferredHost.toArray
}

/**
 * InputPartition holding multiple splits for batch processing. All splits share the same preferredHost for cache
 * locality.
 *
 * This reduces Spark scheduler overhead by processing multiple splits in a single task while honoring cache locality
 * assignments from DriverSplitLocalityManager.
 *
 * @param addActions
 *   Multiple splits to process in this partition
 * @param readSchema
 *   Schema for reading data
 * @param fullTableSchema
 *   Full table schema for type lookup (filters may reference non-projected columns)
 * @param filters
 *   Pushed-down filters to apply
 * @param partitionId
 *   Partition index for logging/debugging
 * @param limit
 *   Optional pushed-down limit
 * @param indexQueryFilters
 *   IndexQuery filters for full-text search
 * @param preferredHost
 *   Preferred host for all splits in this partition
 */
class IndexTables4SparkMultiSplitInputPartition(
  val addActions: Seq[AddAction],
  val readSchema: StructType,
  val fullTableSchema: StructType,
  val filters: Array[Filter],
  val partitionId: Int,
  val limit: Option[Int] = None,
  val indexQueryFilters: Array[Any] = Array.empty,
  val preferredHost: Option[String] = None)
    extends InputPartition {

  override def preferredLocations(): Array[String] = preferredHost.toArray
}

class IndexTables4SparkReaderFactory(
  readSchema: StructType,
  limit: Option[Int] = None,
  config: Map[String, String], // Direct config instead of broadcast
  tablePath: Path,
  metricsAccumulator: Option[io.indextables.spark.storage.BatchOptimizationMetricsAccumulator] = None)
    extends PartitionReaderFactory {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkReaderFactory])

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    partition match {
      case multi: IndexTables4SparkMultiSplitInputPartition =>
        logger.info(
          s"Creating multi-split reader for partition ${multi.partitionId} with ${multi.addActions.length} splits"
        )
        new IndexTables4SparkMultiSplitPartitionReader(
          multi.addActions,
          readSchema,
          multi.fullTableSchema,
          multi.filters,
          multi.limit.orElse(limit),
          config,
          tablePath,
          multi.indexQueryFilters,
          metricsAccumulator
        )

      case single: IndexTables4SparkInputPartition =>
        logger.info(s"Creating reader for partition ${single.partitionId}")
        new IndexTables4SparkPartitionReader(
          single.addAction,
          readSchema,
          single.fullTableSchema,
          single.filters,
          single.limit.orElse(limit),
          config,
          tablePath,
          single.indexQueryFilters,
          metricsAccumulator
        )

      case other =>
        throw new IllegalArgumentException(s"Unexpected partition type: ${other.getClass}")
    }
}

class IndexTables4SparkPartitionReader(
  addAction: AddAction,
  readSchema: StructType,
  fullTableSchema: StructType, // Full table schema for type lookup (filters may reference non-projected columns)
  filters: Array[Filter],
  limit: Option[Int] = None,
  config: Map[String, String], // Direct config instead of broadcast
  tablePath: Path,
  indexQueryFilters: Array[Any] = Array.empty,
  metricsAccumulator: Option[io.indextables.spark.storage.BatchOptimizationMetricsAccumulator] = None)
    extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkPartitionReader])

  // Calculate effective limit: use pushed limit, then configurable default, then hardcoded fallback
  // Configuration key: spark.indextables.read.defaultLimit (default: 250)
  private val configuredDefaultLimit: Int = config
    .get("spark.indextables.read.defaultLimit")
    .flatMap(s => scala.util.Try(s.toInt).toOption)
    .getOrElse(250)
  private val effectiveLimit: Int = limit.getOrElse(configuredDefaultLimit)

  // Resolve relative path from AddAction against table path
  private val filePath = PathResolutionUtils.resolveSplitPathAsString(addAction.path, tablePath.toString)

  private var splitSearchEngine: SplitSearchEngine  = _
  private var resultIterator: Iterator[InternalRow] = Iterator.empty
  private var initialized                           = false

  // Note: recordsRead is automatically tracked by Spark's V2 DataSourceRDD (MetricsHandler)
  // We only need to track bytesRead since Spark's Hadoop filesystem callbacks don't work
  // for our direct S3/Azure/local file reading via tantivy4java

  // Capture baseline metrics at partition reader creation for delta computation
  // This allows accurate per-partition metrics when using the accumulator
  private val baselineMetrics: io.indextables.spark.storage.BatchOptMetrics =
    if (metricsAccumulator.isDefined) {
      io.indextables.spark.storage.BatchOptMetrics.fromJavaMetrics()
    } else {
      io.indextables.spark.storage.BatchOptMetrics.empty
    }

  // Cached options map for legacy methods that require CaseInsensitiveStringMap
  // NOTE: HadoopConf creation eliminated - Map-based fast path used instead
  private lazy val cachedOptionsMap = {
    import scala.jdk.CollectionConverters._
    new org.apache.spark.sql.util.CaseInsensitiveStringMap(config.asJava)
  }

  private def createCacheConfig(): SplitCacheConfig = {
    // Diagnostic: log companion config state on executor
    val hasCompanionKey = config.contains("spark.indextables.companion.parquetTableRoot")
    val companionRoot = config.get("spark.indextables.companion.parquetTableRoot")
    logger.info(s"[EXECUTOR] createCacheConfig for ${addAction.path}: " +
      s"companionMode=$hasCompanionKey, " +
      s"parquetTableRoot=${companionRoot.getOrElse("NONE")}, " +
      s"totalConfigKeys=${config.size}")
    io.indextables.spark.util.ConfigUtils.createSplitCacheConfig(
      config,
      Some(tablePath.toString)
    )
  }

  private def initialize(): Unit = {
    if (!initialized) {
      try {
        // Note: Split locality is now tracked by DriverSplitLocalityManager on the driver
        // No executor-side recording needed - assignments are managed during partition planning

        // Check if pre-warm is enabled and try to join warmup future
        val broadcasted      = config
        val isPreWarmEnabled = broadcasted.getOrElse("spark.indextables.cache.prewarm.enabled", "false").toBoolean
        if (isPreWarmEnabled) {
          // Generate query hash from filters for warmup future lookup
          val allFilters   = filters.asInstanceOf[Array[Any]] ++ indexQueryFilters
          val queryHash    = generateQueryHash(allFilters)
          val warmupJoined = PreWarmManager.joinWarmupFuture(addAction.path, queryHash, isPreWarmEnabled)
          if (warmupJoined) {
            logger.info(s"🔥 Successfully joined warmup future for split: ${addAction.path}")
          }
        }

        // Create cache configuration from Spark options
        val cacheConfig = createCacheConfig()

        // Defensive check: detect companion splits that are missing companion config
        if (addAction.companionDeltaVersion.isDefined && cacheConfig.companionSourceTableRoot.isEmpty) {
          logger.error(s"COMPANION CONFIG MISSING: Split ${addAction.path} has companionDeltaVersion=" +
            s"${addAction.companionDeltaVersion.get} but companionSourceTableRoot is None. " +
            s"Config keys: ${config.keys.filter(_.contains("companion")).mkString(", ")}. " +
            s"This will cause 'parquet_table_root was not set' error during document retrieval.")
        }

        // Create split search engine using footer offset optimization when available
        // Normalize URLs for tantivy4java compatibility (S3, Azure, etc.)
        // Uses centralized normalization: s3a://->s3://, abfss://->azure://, etc.
        val actualPath = if (filePath.toString.startsWith("file:")) {
          filePath.toString // Keep file URIs as-is for tantivy4java
        } else {
          // Use CloudStorageProviderFactory's Map-based normalization (FAST PATH - no HadoopConf)
          io.indextables.spark.io.CloudStorageProviderFactory.normalizePathForTantivy(
            filePath.toString,
            config
          )
        }

        // Footer offset metadata is required for all split reading operations
        if (!addAction.hasFooterOffsets || addAction.footerStartOffset.isEmpty) {
          throw new RuntimeException(
            s"AddAction for $actualPath does not contain required footer offsets. All 'add' entries in the transaction log must contain footer offset metadata."
          )
        }

        // Reconstruct COMPLETE SplitMetadata from AddAction - all fields required for proper operation
        import java.time.Instant
        import scala.jdk.CollectionConverters._

        // Safe conversion functions for Option[Any] to Long to handle JSON deserialization type variations
        def toLongSafeOption(opt: Option[Any]): Long = opt match {
          case Some(value) =>
            value match {
              case l: Long              => l
              case i: Int               => i.toLong
              case i: java.lang.Integer => i.toLong
              case l: java.lang.Long    => l
              case _                    => value.toString.toLong
            }
          case None => 0L
        }

        val splitMetadata = new io.indextables.tantivy4java.split.merge.QuickwitSplit.SplitMetadata(
          addAction.path.split("/").last.replace(".split", ""),         // splitId from filename
          "tantivy4spark-index",                                        // indexUid (NEW - required)
          0L,                                                           // partitionId (NEW - required)
          "tantivy4spark-source",                                       // sourceId (NEW - required)
          "tantivy4spark-node",                                         // nodeId (NEW - required)
          toLongSafeOption(addAction.numRecords),                       // numDocs
          toLongSafeOption(addAction.uncompressedSizeBytes),            // uncompressedSizeBytes
          addAction.timeRangeStart.map(Instant.parse).orNull,           // timeRangeStart
          addAction.timeRangeEnd.map(Instant.parse).orNull,             // timeRangeEnd
          System.currentTimeMillis() / 1000,                            // createTimestamp (NEW - required)
          "Mature",                                                     // maturity (NEW - required)
          addAction.splitTags.getOrElse(Set.empty[String]).asJava,      // tags
          toLongSafeOption(addAction.footerStartOffset),                // footerStartOffset
          toLongSafeOption(addAction.footerEndOffset),                  // footerEndOffset
          toLongSafeOption(addAction.deleteOpstamp),                    // deleteOpstamp
          addAction.numMergeOps.getOrElse(0),                           // numMergeOps (Int is OK for this field)
          "doc-mapping-uid",                                            // docMappingUid (NEW - required)
          addAction.docMappingJson.orNull,                              // docMappingJson (MOVED - for performance)
          java.util.Collections.emptyList[QuickwitSplit.SkippedSplit]() // skippedSplits
        )

        // Create IndexTables4SparkOptions from config map for JSON field support
        val options = Some(IndexTables4SparkOptions(config))

        // Use full readSchema since partition values are stored directly in splits (consistent with Quickwit)
        splitSearchEngine =
          SplitSearchEngine.fromSplitFileWithMetadata(readSchema, actualPath, splitMetadata, cacheConfig, options)

        // Get the schema from the split to validate filters
        // CRITICAL: Schema must be closed to prevent native memory leak
        val splitFieldNames = {
          val splitSchema = splitSearchEngine.getSchema()
          try {
            import scala.jdk.CollectionConverters._
            val fieldNames = splitSchema.getFieldNames().asScala.toSet
            logger.info(s"Split schema contains fields: ${fieldNames.mkString(", ")}")
            fieldNames
          } catch {
            case e: Exception =>
              logger.warn(s"Could not retrieve field names from split schema: ${e.getMessage}")
              Set.empty[String]
          } finally
            splitSchema.close() // Prevent native memory leak
        }

        // Log the filters and limit
        logger.info(s"  - Filters: ${filters.length} filter(s) - ${filters.mkString(", ")}")
        logger.info(
          s"  - IndexQuery Filters: ${indexQueryFilters.length} filter(s) - ${indexQueryFilters.mkString(", ")}"
        )
        logger.info(s"  - Limit: $effectiveLimit")

        // Get partition column names from the AddAction
        val partitionColumnNames: Set[String] = addAction.partitionValues.keys.toSet

        // Filter out partition-only filters - these are already handled by partition pruning
        // and are unnecessary/expensive for Tantivy (especially range queries like >, <, between)
        val nonPartitionFilters = if (partitionColumnNames.nonEmpty) {
          val (partitionOnly, nonPartition) = filters.partition(f => isPartitionOnlyFilter(f, partitionColumnNames))
          if (partitionOnly.nonEmpty) {
            logger.info(s"Excluding ${partitionOnly.length} partition filter(s) from Tantivy query: ${partitionOnly.mkString(", ")}")
          }
          nonPartition
        } else {
          filters
        }

        // Filter out range filters that are redundant based on statistics
        // If min/max values indicate ALL data in split is within the filter's range, skip the filter
        // Use fullTableSchema for type lookup since filters may reference non-projected columns
        val optimizedFilters = if (addAction.minValues.nonEmpty && addAction.maxValues.nonEmpty) {
          val (redundantByStats, remaining) = nonPartitionFilters.partition(f =>
            isRangeFilterRedundantByStats(f, addAction.minValues.get, addAction.maxValues.get, fullTableSchema)
          )
          if (redundantByStats.nonEmpty) {
            logger.info(s"Excluding ${redundantByStats.length} range filter(s) redundant by statistics: ${redundantByStats.mkString(", ")}")
          }
          remaining
        } else {
          nonPartitionFilters
        }

        val allFilters: Array[Any] = optimizedFilters.asInstanceOf[Array[Any]] ++ indexQueryFilters

        // Convert filters to SplitQuery object with schema validation
        val splitQuery = if (allFilters.nonEmpty) {
          // Use cached options map for field configuration (avoid repeated creation)
          val queryObj = if (splitFieldNames.nonEmpty) {
            // Use mixed filter converter to handle both Spark filters and IndexQuery filters
            val validatedQuery = FiltersToQueryConverter.convertToSplitQuery(
              allFilters,
              splitSearchEngine,
              Some(splitFieldNames),
              Some(cachedOptionsMap)
            )
            logger.info(s"  - SplitQuery (with schema validation): ${validatedQuery.getClass.getSimpleName}")
            validatedQuery
          } else {
            // Fall back to no schema validation if we can't get field names
            val fallbackQuery =
              FiltersToQueryConverter.convertToSplitQuery(allFilters, splitSearchEngine, None, Some(cachedOptionsMap))
            logger.info(s"  - SplitQuery (no schema validation): ${fallbackQuery.getClass.getSimpleName}")
            fallbackQuery
          }
          queryObj
        } else {
          import io.indextables.tantivy4java.split.SplitMatchAllQuery
          new SplitMatchAllQuery() // Use match-all query for no filters
        }

        // Push down SplitQuery and limit to split searcher
        logger.info(s"Executing search with SplitQuery object and limit: $effectiveLimit")
        val searchResults = splitSearchEngine.search(splitQuery, limit = effectiveLimit)
        logger.info(s"Search returned ${searchResults.length} results (pushed limit: $effectiveLimit)")

        // For companion splits, partition column values are NOT stored in the parquet data
        // (Delta/Iceberg store them in directory paths). Inject from AddAction.partitionValues.
        val isCompanionSplit = config.contains("spark.indextables.companion.parquetTableRoot")
        resultIterator = if (isCompanionSplit && addAction.partitionValues.nonEmpty) {
          val partitionIndices = readSchema.fields.zipWithIndex.collect {
            case (field, idx) if addAction.partitionValues.contains(field.name) =>
              (idx, field.dataType, addAction.partitionValues(field.name))
          }
          if (partitionIndices.nonEmpty) {
            logger.info(s"Companion split: injecting ${partitionIndices.length} partition column value(s)")
            searchResults.iterator.map { row =>
              val values = row.toSeq(readSchema).toArray
              partitionIndices.foreach { case (idx, dataType, strVal) =>
                values(idx) = convertPartitionValue(strVal, dataType)
              }
              InternalRow.fromSeq(values)
            }
          } else {
            searchResults.iterator
          }
        } else {
          searchResults.iterator
        }
        initialized = true
        logger.info(s"Pushdown complete for ${addAction.path}: splitQuery='$splitQuery', limit=$effectiveLimit, results=${searchResults.length}")

      } catch {
        case ex: Exception =>
          logger.error(s"Failed to initialize reader for ${addAction.path}", ex)
          // Set safe default for resultIterator to prevent NPE in next() calls
          resultIterator = Iterator.empty
          initialized = true
          // Still throw the exception to signal the failure
          throw new IOException(s"Failed to read Tantivy index: ${ex.getMessage}", ex)
      }
    }
  }

  override def next(): Boolean =
    try {
      initialize()
      if (resultIterator != null) {
        resultIterator.hasNext
      } else {
        false
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Error in next() for ${addAction.path}", ex)
        // Re-throw the exception to ensure the task fails rather than silently skipping data
        throw new RuntimeException(s"Failed to read partition for ${addAction.path}: ${ex.getMessage}", ex)
    }

  override def get(): InternalRow =
    if (resultIterator != null) {
      resultIterator.next()
    } else {
      throw new IllegalStateException(s"No data available for ${addAction.path}")
    }

  override def close(): Unit = {
    // Collect batch optimization metrics delta for this partition
    // Each partition contributes its delta to the accumulator, which aggregates across all partitions
    metricsAccumulator.foreach { acc =>
      try {
        val currentMetrics = io.indextables.spark.storage.BatchOptMetrics.fromJavaMetrics()
        val delta = io.indextables.spark.storage.BatchOptMetrics(
          totalOperations = currentMetrics.totalOperations - baselineMetrics.totalOperations,
          totalDocuments = currentMetrics.totalDocuments - baselineMetrics.totalDocuments,
          totalRequests = currentMetrics.totalRequests - baselineMetrics.totalRequests,
          consolidatedRequests = currentMetrics.consolidatedRequests - baselineMetrics.consolidatedRequests,
          bytesTransferred = currentMetrics.bytesTransferred - baselineMetrics.bytesTransferred,
          bytesWasted = currentMetrics.bytesWasted - baselineMetrics.bytesWasted,
          totalPrefetchDurationMs = currentMetrics.totalPrefetchDurationMs - baselineMetrics.totalPrefetchDurationMs,
          segmentsProcessed = currentMetrics.segmentsProcessed - baselineMetrics.segmentsProcessed
        )
        if (delta.totalOperations > 0 || delta.totalDocuments > 0) {
          acc.add(delta)
          logger.debug(s"Added batch metrics delta for ${addAction.path}: ops=${delta.totalOperations}, docs=${delta.totalDocuments}")
        }
      } catch {
        case ex: Exception =>
          logger.warn(s"Error collecting batch optimization metrics for ${addAction.path}", ex)
      }
    }

    // Report bytesRead to Spark UI
    // Note: recordsRead is automatically tracked by Spark's V2 DataSourceRDD (MetricsHandler)
    // We only report bytesRead since Spark's Hadoop filesystem callbacks don't work for our direct file reading
    val bytesRead = addAction.size
    if (org.apache.spark.sql.indextables.OutputMetricsUpdater.incInputMetrics(bytesRead, 0)) {
      logger.debug(s"Reported input metrics for ${addAction.path}: $bytesRead bytes")
    }

    if (splitSearchEngine != null) {
      try
        splitSearchEngine.close()
      catch {
        case ex: Exception =>
          // Log but don't rethrow - close() should be idempotent and not fail the task
          logger.warn(s"Error closing splitSearchEngine for ${addAction.path}", ex)
      }
    }
  }

  /** Generate a consistent hash for the query filters to identify warmup futures. */
  private def generateQueryHash(allFilters: Array[Any]): String = {
    val filterString = allFilters.map(_.toString).mkString("|")
    java.util.UUID.nameUUIDFromBytes(filterString.getBytes).toString.take(8)
  }

  /**
   * Check if a filter only references partition columns. These filters are already handled by partition pruning and
   * don't need to be sent to Tantivy.
   *
   * @param filter
   *   The Spark filter to check
   * @param partitionColumns
   *   Set of partition column names
   * @return
   *   true if the filter only references partition columns
   */
  private def isPartitionOnlyFilter(filter: Filter, partitionColumns: Set[String]): Boolean = {
    import org.apache.spark.sql.sources._

    def getFilterFieldNames(f: Filter): Set[String] = f match {
      case EqualTo(attribute, _)            => Set(attribute)
      case EqualNullSafe(attribute, _)      => Set(attribute)
      case GreaterThan(attribute, _)        => Set(attribute)
      case GreaterThanOrEqual(attribute, _) => Set(attribute)
      case LessThan(attribute, _)           => Set(attribute)
      case LessThanOrEqual(attribute, _)    => Set(attribute)
      case In(attribute, _)                 => Set(attribute)
      case IsNull(attribute)                => Set(attribute)
      case IsNotNull(attribute)             => Set(attribute)
      case StringStartsWith(attribute, _)   => Set(attribute)
      case StringEndsWith(attribute, _)     => Set(attribute)
      case StringContains(attribute, _)     => Set(attribute)
      case And(left, right)                 => getFilterFieldNames(left) ++ getFilterFieldNames(right)
      case Or(left, right)                  => getFilterFieldNames(left) ++ getFilterFieldNames(right)
      case Not(child)                       => getFilterFieldNames(child)
      case _                                => Set.empty
    }

    val fieldNames = getFilterFieldNames(filter)
    // A filter is partition-only if ALL its fields are partition columns
    // Empty field set means unknown filter type - don't exclude it
    fieldNames.nonEmpty && fieldNames.forall(partitionColumns.contains)
  }

  /**
   * Check if a range filter is redundant based on min/max statistics. A filter is redundant if the split's entire data
   * range is within the filter's range, meaning all records in the split would pass the filter anyway.
   *
   * Only applies to Date and Timestamp columns to avoid type conversion complexity.
   *
   * @param filter
   *   The Spark filter to check
   * @param minValues
   *   Min values from split statistics
   * @param maxValues
   *   Max values from split statistics
   * @param schema
   *   The read schema to determine column types
   * @return
   *   true if the filter is redundant (all data passes), false otherwise
   */
  private def isRangeFilterRedundantByStats(
    filter: Filter,
    minValues: Map[String, String],
    maxValues: Map[String, String],
    schema: StructType
  ): Boolean = {
    import org.apache.spark.sql.sources._
    import org.apache.spark.sql.types._
    import java.sql.{Date, Timestamp}
    import java.time.{Instant, LocalDate}

    def isDateOrTimestampColumn(attribute: String): Boolean =
      schema.fields.find(_.name == attribute).exists { field =>
        field.dataType match {
          case DateType | TimestampType => true
          case _                        => false
        }
      }

    def getColumnType(attribute: String): Option[DataType] =
      schema.fields.find(_.name == attribute).map(_.dataType)

    // Parse timestamp/date values from statistics and filter values
    // Statistics store timestamps as microseconds (Long string) and dates as days since epoch
    def parseTimestamp(value: Any, fromStats: Boolean): Option[Long] = value match {
      // getTime() returns millis since epoch (includes sub-second millis)
      // getNanos() returns the fractional second in nanos (0-999,999,999) INCLUDING the millis
      // To avoid double-counting millis: use epochSeconds (truncated) + getNanos()/1000
      case ts: Timestamp =>
        val epochSeconds = ts.getTime / 1000
        Some(epochSeconds * 1000000 + ts.getNanos / 1000)
      case s: String =>
        // Statistics are stored as microseconds (Long as String)
        try {
          val micros = s.toLong
          Some(micros)
        } catch {
          case _: NumberFormatException =>
            // Try parsing as ISO instant or timestamp string
            try {
              val instant = Instant.parse(s)
              Some(instant.getEpochSecond * 1000000 + instant.getNano / 1000)
            } catch {
              case _: Exception =>
                try {
                  val ts           = Timestamp.valueOf(s)
                  val epochSeconds = ts.getTime / 1000
                  Some(epochSeconds * 1000000 + ts.getNanos / 1000)
                } catch { case _: Exception => None }
            }
        }
      case l: Long => Some(if (fromStats) l else l * 1000) // Stats already in micros
      case i: Int  => Some(i.toLong * 1000)
      case _       => None
    }

    def parseDate(value: Any, fromStats: Boolean): Option[Long] = value match {
      case d: Date   => Some(d.toLocalDate.toEpochDay) // Convert to days since epoch
      case s: String =>
        // Statistics are stored as days since epoch (Int as String)
        try {
          val days = s.toLong
          Some(days)
        } catch {
          case _: NumberFormatException =>
            // Try parsing as ISO date string
            try Some(LocalDate.parse(s).toEpochDay)
            catch {
              case _: Exception =>
                try Some(Date.valueOf(s).toLocalDate.toEpochDay)
                catch { case _: Exception => None }
            }
        }
      case l: Long => Some(l) // Already days since epoch
      case i: Int  => Some(i.toLong)
      case _       => None
    }

    def parseValue(
      value: Any,
      dataType: DataType,
      fromStats: Boolean
    ): Option[Long] = dataType match {
      case TimestampType => parseTimestamp(value, fromStats)
      case DateType      => parseDate(value, fromStats)
      case _             => None
    }

    // Check if filter is redundant:
    // - GreaterThan(attr, v): redundant if splitMin > v (all values greater than v)
    // - GreaterThanOrEqual(attr, v): redundant if splitMin >= v
    // - LessThan(attr, v): redundant if splitMax < v (all values less than v)
    // - LessThanOrEqual(attr, v): redundant if splitMax <= v
    filter match {
      case GreaterThan(attribute, value) if isDateOrTimestampColumn(attribute) =>
        (for {
          dataType  <- getColumnType(attribute)
          splitMin  <- minValues.get(attribute).flatMap(parseValue(_, dataType, fromStats = true))
          filterVal <- parseValue(value, dataType, fromStats = false)
        } yield splitMin > filterVal).getOrElse(false)

      case GreaterThanOrEqual(attribute, value) if isDateOrTimestampColumn(attribute) =>
        (for {
          dataType  <- getColumnType(attribute)
          splitMin  <- minValues.get(attribute).flatMap(parseValue(_, dataType, fromStats = true))
          filterVal <- parseValue(value, dataType, fromStats = false)
        } yield splitMin >= filterVal).getOrElse(false)

      case LessThan(attribute, value) if isDateOrTimestampColumn(attribute) =>
        (for {
          dataType  <- getColumnType(attribute)
          splitMax  <- maxValues.get(attribute).flatMap(parseValue(_, dataType, fromStats = true))
          filterVal <- parseValue(value, dataType, fromStats = false)
        } yield splitMax < filterVal).getOrElse(false)

      case LessThanOrEqual(attribute, value) if isDateOrTimestampColumn(attribute) =>
        (for {
          dataType  <- getColumnType(attribute)
          splitMax  <- maxValues.get(attribute).flatMap(parseValue(_, dataType, fromStats = true))
          filterVal <- parseValue(value, dataType, fromStats = false)
        } yield splitMax <= filterVal).getOrElse(false)

      // For AND filters, both sides must be redundant for the whole filter to be redundant
      case And(left, right) =>
        isRangeFilterRedundantByStats(left, minValues, maxValues, schema) &&
        isRangeFilterRedundantByStats(right, minValues, maxValues, schema)

      case _ => false
    }
  }

  /** Convert a partition value string to the appropriate Spark internal representation. */
  private def convertPartitionValue(value: String, dataType: org.apache.spark.sql.types.DataType): Any = {
    import org.apache.spark.sql.types._
    import org.apache.spark.unsafe.types.UTF8String
    if (value == null) return null
    dataType match {
      case StringType    => UTF8String.fromString(value)
      case IntegerType   => value.toInt
      case LongType      => value.toLong
      case DoubleType    => value.toDouble
      case FloatType     => value.toFloat
      case BooleanType   => value.toBoolean
      case ShortType     => value.toShort
      case ByteType      => value.toByte
      case _             => UTF8String.fromString(value) // fallback: treat as string
    }
  }
}

/**
 * PartitionReader that processes multiple splits sequentially. Stops early if pushed limit is satisfied before querying
 * all splits.
 *
 * This reader iterates through multiple splits, maintaining running state to:
 *   - Track total rows returned across all splits
 *   - Apply remaining limit to each subsequent split
 *   - Skip remaining splits once limit is satisfied (early termination)
 *   - Close each split's search engine before moving to the next
 *
 * @param addActions
 *   Multiple splits to process
 * @param readSchema
 *   Schema for reading data
 * @param fullTableSchema
 *   Full table schema for type lookup
 * @param filters
 *   Pushed-down filters to apply
 * @param limit
 *   Optional pushed-down limit
 * @param config
 *   Configuration options
 * @param tablePath
 *   Base table path for resolving relative paths
 * @param indexQueryFilters
 *   IndexQuery filters for full-text search
 * @param metricsAccumulator
 *   Optional accumulator for batch optimization metrics
 */
class IndexTables4SparkMultiSplitPartitionReader(
  addActions: Seq[AddAction],
  readSchema: StructType,
  fullTableSchema: StructType,
  filters: Array[Filter],
  limit: Option[Int] = None,
  config: Map[String, String],
  tablePath: Path,
  indexQueryFilters: Array[Any] = Array.empty,
  metricsAccumulator: Option[io.indextables.spark.storage.BatchOptimizationMetricsAccumulator] = None)
    extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkMultiSplitPartitionReader])

  // Calculate effective limit: use pushed limit, then configurable default, then hardcoded fallback
  private val configuredDefaultLimit: Int = config
    .get("spark.indextables.read.defaultLimit")
    .flatMap(s => scala.util.Try(s.toInt).toOption)
    .getOrElse(250)
  private val effectiveLimit: Int = limit.getOrElse(configuredDefaultLimit)

  // Multi-split iteration state
  private var currentSplitIndex                                       = 0
  private var currentReader: Option[IndexTables4SparkPartitionReader] = None
  private var totalRowsReturned                                       = 0L
  private var initialized                                             = false

  // Capture baseline metrics at partition reader creation for delta computation
  private val baselineMetrics: io.indextables.spark.storage.BatchOptMetrics =
    if (metricsAccumulator.isDefined) {
      io.indextables.spark.storage.BatchOptMetrics.fromJavaMetrics()
    } else {
      io.indextables.spark.storage.BatchOptMetrics.empty
    }

  logger.info(s"MultiSplitPartitionReader created with ${addActions.length} splits, effectiveLimit=$effectiveLimit")

  override def next(): Boolean = {
    if (!initialized) {
      initialized = true
      logger.debug(s"MultiSplitPartitionReader: initializing with ${addActions.length} splits")
    }

    // Check if current reader has more rows
    if (currentReader.exists(_.next())) {
      return true
    }

    // Close current reader before moving to next
    closeCurrentReader()

    // Check if we've satisfied the limit
    val remainingLimit = effectiveLimit - totalRowsReturned.toInt
    if (remainingLimit <= 0) {
      logger.debug(
        s"MultiSplitPartitionReader: limit satisfied ($totalRowsReturned >= $effectiveLimit), skipping remaining ${addActions.length - currentSplitIndex} splits"
      )
      return false
    }

    // Move to next split
    while (currentSplitIndex < addActions.length) {
      val addAction = addActions(currentSplitIndex)
      currentSplitIndex += 1

      logger.debug(
        s"MultiSplitPartitionReader: initializing split $currentSplitIndex/${addActions.length}: ${addAction.path}"
      )

      // Create a new single-split reader with the remaining limit
      val singleSplitReader = new IndexTables4SparkPartitionReader(
        addAction,
        readSchema,
        fullTableSchema,
        filters,
        Some(remainingLimit),
        config,
        tablePath,
        indexQueryFilters,
        metricsAccumulator
      )

      currentReader = Some(singleSplitReader)

      if (singleSplitReader.next()) {
        return true
      }

      // This split had no results, close and try next
      closeCurrentReader()
    }

    false
  }

  override def get(): InternalRow =
    currentReader match {
      case Some(reader) =>
        val row = reader.get()
        totalRowsReturned += 1
        row
      case None =>
        throw new IllegalStateException("get() called without successful next()")
    }

  private def closeCurrentReader(): Unit = {
    currentReader.foreach { reader =>
      try
        reader.close()
      catch {
        case ex: Exception =>
          logger.warn(s"Error closing split reader: ${ex.getMessage}")
      }
    }
    currentReader = None
  }

  override def close(): Unit = {
    closeCurrentReader()

    // Collect batch optimization metrics delta for this partition (all splits combined)
    metricsAccumulator.foreach { acc =>
      try {
        val currentMetrics = io.indextables.spark.storage.BatchOptMetrics.fromJavaMetrics()
        val delta = io.indextables.spark.storage.BatchOptMetrics(
          totalOperations = currentMetrics.totalOperations - baselineMetrics.totalOperations,
          totalDocuments = currentMetrics.totalDocuments - baselineMetrics.totalDocuments,
          totalRequests = currentMetrics.totalRequests - baselineMetrics.totalRequests,
          consolidatedRequests = currentMetrics.consolidatedRequests - baselineMetrics.consolidatedRequests,
          bytesTransferred = currentMetrics.bytesTransferred - baselineMetrics.bytesTransferred,
          bytesWasted = currentMetrics.bytesWasted - baselineMetrics.bytesWasted,
          totalPrefetchDurationMs = currentMetrics.totalPrefetchDurationMs - baselineMetrics.totalPrefetchDurationMs,
          segmentsProcessed = currentMetrics.segmentsProcessed - baselineMetrics.segmentsProcessed
        )
        if (delta.totalOperations > 0 || delta.totalDocuments > 0) {
          acc.add(delta)
          logger.debug(s"Added batch metrics delta for multi-split partition: ops=${delta.totalOperations}, docs=${delta.totalDocuments}")
        }
      } catch {
        case ex: Exception =>
          logger.warn(s"Error collecting batch optimization metrics for multi-split partition", ex)
      }
    }

    // Report bytesRead to Spark UI for all processed splits
    // Note: recordsRead is automatically tracked by Spark's V2 DataSourceRDD (MetricsHandler)
    // We only report bytesRead since Spark's Hadoop filesystem callbacks don't work for our direct file reading
    val bytesRead = addActions.take(currentSplitIndex).map(_.size).sum
    if (org.apache.spark.sql.indextables.OutputMetricsUpdater.incInputMetrics(bytesRead, 0)) {
      logger.debug(s"Reported input metrics for multi-split partition: $bytesRead bytes from $currentSplitIndex splits")
    }

    logger.info(s"MultiSplitPartitionReader closed: processed $currentSplitIndex/${addActions.length} splits, returned $totalRowsReturned rows")
  }
}

class IndexTables4SparkWriterFactory(
  tablePath: Path,
  writeSchema: StructType,
  serializedOptions: Map[String, String],
  partitionColumns: Seq[String] = Seq.empty)
    extends DataWriterFactory {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkWriterFactory])

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    logger.info(s"Creating writer for partition $partitionId, task $taskId")
    if (partitionColumns.nonEmpty) {
      logger.info(s"Creating partitioned writer with columns: ${partitionColumns.mkString(", ")}")
    }

    // Use Map-based config directly - no HadoopConf reconstruction needed (fast path)
    new IndexTables4SparkDataWriter(
      tablePath,
      writeSchema,
      partitionId,
      taskId,
      serializedOptions,
      partitionColumns
    )
  }
}

class IndexTables4SparkDataWriter(
  tablePath: Path,
  writeSchema: StructType,
  partitionId: Int,
  taskId: Long,
  serializedOptions: Map[String, String],
  partitionColumns: Seq[String] = Seq.empty // Partition columns from metadata
) extends DataWriter[InternalRow] {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkDataWriter])

  // Create CaseInsensitiveStringMap from serialized options for components that need it
  private lazy val options: CaseInsensitiveStringMap = {
    import scala.jdk.CollectionConverters._
    new CaseInsensitiveStringMap(serializedOptions.asJava)
  }

  // Initialize split conversion throttle (first access per executor)
  // This limits the parallelism of tantivy index -> quickwit split conversions
  {
    // Get configured max parallelism. If not set, we'll use a conservative default of 1
    // The driver should set this configuration based on defaultParallelism
    val defaultMaxParallelism = 1
    val maxParallelism = options
      .getLong(
        io.indextables.spark.config.IndexTables4SparkSQLConf.TANTIVY4SPARK_SPLIT_CONVERSION_MAX_PARALLELISM,
        defaultMaxParallelism
      )
      .toInt

    // Initialize throttle (idempotent - only initializes once per parallelism value)
    io.indextables.spark.storage.SplitConversionThrottle.initialize(maxParallelism)
    logger.info(s"Split conversion throttle initialized: maxParallelism=$maxParallelism")
  }

  // Normalize table path for consistent S3 protocol handling (s3a:// -> s3://)
  private val normalizedTablePath = {
    val pathStr       = tablePath.toString
    val normalizedStr = io.indextables.spark.util.ProtocolNormalizer.normalizeAllProtocols(pathStr)
    new Path(normalizedStr)
  }

  // Debug: Log options available in executor (Map-based config - no HadoopConf)
  if (logger.isDebugEnabled) {
    logger.debug("IndexTables4SparkDataWriter executor options:")
    options.entrySet().asScala.foreach { entry =>
      val value = if (entry.getKey.contains("secretKey")) "***" else entry.getValue
      logger.debug(s"  ${entry.getKey} = $value")
    }
  }

  // For partitioned tables, we need to maintain separate writers per unique partition value combination
  // Key: serialized partition values (e.g., "event_date=2023-01-15"), Value: (searchEngine, statistics, recordCount)
  private val partitionWriters =
    scala.collection.mutable.Map[String, (TantivySearchEngine, StatisticsCalculator.DatasetStatistics, Long)]()

  // For non-partitioned tables, use a single writer
  // Uses Map-based TantivySearchEngine constructor (fast path - no HadoopConf)
  private var singleWriter: Option[(TantivySearchEngine, StatisticsCalculator.DatasetStatistics, Long)] =
    if (partitionColumns.isEmpty)
      Some(
        (
          new TantivySearchEngine(writeSchema, options, serializedOptions),
          new StatisticsCalculator.DatasetStatistics(writeSchema, serializedOptions),
          0L
        )
      )
    else None

  // Precomputed partition column info for O(1) per-row extraction instead of O(schema.size)
  // This is critical for wide schemas (400+ columns) where building field map per row is expensive
  private val partitionInfo: PartitionUtils.PartitionColumnInfo =
    PartitionUtils.precomputePartitionInfo(writeSchema, partitionColumns)

  // Split rolling support for balanced mode: when maxRowsPerSplit is set,
  // finalize and upload the current split when the row count reaches the limit,
  // then continue writing to a fresh split.
  private val maxRowsPerSplit: Option[Long] = Option(serializedOptions.getOrElse("__maxRowsPerSplit", null))
    .filter(_.nonEmpty).map(_.toLong)
  private val rolledActions = scala.collection.mutable.ArrayBuffer[AddAction]()
  private var cumulativeBytes: Long = 0L
  private var cumulativeRecords: Long = 0L

  // Debug: log partition columns being used
  logger.info(
    s"DataWriter initialized for partition $partitionId with partitionColumns: ${partitionColumns.mkString("[", ", ", "]")}" +
      maxRowsPerSplit.map(m => s", maxRowsPerSplit=$m").getOrElse("")
  )

  /**
   * Check if the current writer should be rolled (finalized and replaced with a fresh one).
   * Called after each row is written. If the row count reaches maxRowsPerSplit, the current
   * split is committed (uploaded + cleaned up), and a fresh writer is returned.
   */
  private def maybeRollSplit(
    engine: TantivySearchEngine,
    stats: StatisticsCalculator.DatasetStatistics,
    count: Long,
    partitionValues: Map[String, String],
    partitionKey: String
  ): (TantivySearchEngine, StatisticsCalculator.DatasetStatistics, Long) =
    maxRowsPerSplit match {
      case Some(maxRows) if count >= maxRows =>
        // Finalize current split (uploads to storage + cleans up temp files)
        val addAction = commitWriter(engine, stats, count, partitionValues, partitionKey)
        rolledActions += addAction

        // Progressive metrics update so Spark UI shows real-time progress
        cumulativeBytes += addAction.size
        cumulativeRecords += addAction.numRecords.getOrElse(0L)
        org.apache.spark.sql.indextables.OutputMetricsUpdater.updateOutputMetrics(cumulativeBytes, cumulativeRecords)

        logger.info(s"Rolled split after $count rows (cumulative: $cumulativeRecords records, " +
          s"${rolledActions.size} splits, partition=$partitionKey)")

        // Create fresh writer
        (
          new TantivySearchEngine(writeSchema, options, serializedOptions),
          new StatisticsCalculator.DatasetStatistics(writeSchema, serializedOptions),
          0L
        )
      case _ =>
        (engine, stats, count) // No change
    }

  override def write(record: InternalRow): Unit =
    if (partitionColumns.isEmpty) {
      // Non-partitioned write - use single writer
      val (engine, stats, count) = singleWriter.get
      engine.addDocument(record)
      stats.updateRow(record)
      singleWriter = Some(maybeRollSplit(engine, stats, count + 1, Map.empty, ""))
    } else {
      // Partitioned write - extract partition values using precomputed indices (O(1) vs O(schema.size))
      val partitionValues = PartitionUtils.extractPartitionValuesFast(record, partitionInfo)
      val partitionKey    = PartitionUtils.createPartitionPath(partitionValues, partitionColumns)

      // Get or create writer for this partition value combination
      // Uses Map-based TantivySearchEngine constructor (fast path - no HadoopConf)
      val (engine, stats, count) = partitionWriters.getOrElseUpdate(
        partitionKey, {
          logger.info(s"Creating new writer for partition values: $partitionValues")
          (
            new TantivySearchEngine(writeSchema, options, serializedOptions),
            new StatisticsCalculator.DatasetStatistics(writeSchema, serializedOptions),
            0L
          )
        }
      )

      // Store the complete record in the split (including partition columns)
      engine.addDocument(record)
      stats.updateRow(record)
      partitionWriters(partitionKey) = maybeRollSplit(engine, stats, count + 1, partitionValues, partitionKey)
    }

  override def commit(): WriterCommitMessage = {
    // Start with already-uploaded rolled splits
    val allActions = scala.collection.mutable.ArrayBuffer[AddAction]()
    allActions ++= rolledActions

    // Handle non-partitioned writes (remaining data in current writer)
    if (singleWriter.isDefined) {
      val (searchEngine, statistics, recordCount) = singleWriter.get
      if (recordCount > 0) {
        logger.info(s"Committing partition $partitionId with $recordCount records (final segment)")
        val addAction = commitWriter(searchEngine, statistics, recordCount, Map.empty, "")
        allActions += addAction
      } else if (rolledActions.isEmpty) {
        logger.info(s"Skipping transaction log entry for partition $partitionId - no records written")
        return IndexTables4SparkCommitMessage(Seq.empty)
      }
    }

    // Handle partitioned writes (remaining data in each partition writer)
    if (partitionWriters.nonEmpty) {
      logger.info(s"Committing ${partitionWriters.size} partition writers (final segments)")

      partitionWriters.foreach {
        case (partitionKey, (searchEngine, statistics, recordCount)) =>
          if (recordCount > 0) {
            val partitionValues = parsePartitionKey(partitionKey)
            val addAction       = commitWriter(searchEngine, statistics, recordCount, partitionValues, partitionKey)
            allActions += addAction
          } else {
            logger.debug(s"Skipping empty partition writer: $partitionKey (data already rolled)")
          }
      }
    }

    if (allActions.isEmpty) {
      logger.info(s"No records written in partition $partitionId")
      return IndexTables4SparkCommitMessage(Seq.empty)
    }

    // Report final output metrics to Spark UI (bytesWritten, recordsWritten)
    val totalBytes   = allActions.map(_.size).sum
    val totalRecords = allActions.flatMap(_.numRecords).sum
    if (org.apache.spark.sql.indextables.OutputMetricsUpdater.updateOutputMetrics(totalBytes, totalRecords)) {
      logger.debug(s"Reported output metrics: $totalBytes bytes, $totalRecords records")
    }

    logger.info(
      s"Committed partition $partitionId with ${allActions.size} splits " +
        s"(${rolledActions.size} rolled + ${allActions.size - rolledActions.size} final), " +
        s"$totalBytes bytes, $totalRecords records"
    )
    IndexTables4SparkCommitMessage(allActions.toSeq)
  }

  private def parsePartitionKey(partitionKey: String): Map[String, String] =
    // Parse partition key like "event_date=2023-01-15" into Map("event_date" -> "2023-01-15")
    partitionKey
      .split("/")
      .map { part =>
        val Array(key, value) = part.split("=", 2)
        key -> value
      }
      .toMap

  private def commitWriter(
    searchEngine: TantivySearchEngine,
    statistics: StatisticsCalculator.DatasetStatistics,
    recordCount: Long,
    partitionValues: Map[String, String],
    partitionKey: String
  ): AddAction = {
    logger.debug(s"Committing Tantivy index with $recordCount documents for partition: $partitionKey")

    // Create split file name with UUID for guaranteed uniqueness
    // Format: [partitionDir/]part-{partitionId}-{taskId}-{uuid}.split
    val splitId  = UUID.randomUUID().toString
    val fileName = f"part-$partitionId%05d-$taskId-$splitId.split"

    // For partitioned tables, create file in partition directory
    val filePath = if (partitionValues.nonEmpty) {
      val partitionDir = new Path(normalizedTablePath, partitionKey)
      new Path(partitionDir, fileName)
    } else {
      new Path(normalizedTablePath, fileName)
    }

    // Use raw filesystem path for tantivy, not file:// URI
    // For S3Mock, apply path flattening via CloudStorageProvider
    val outputPath = if (filePath.toString.startsWith("file:")) {
      // Extract the local filesystem path from file:// URI
      new java.io.File(filePath.toUri).getAbsolutePath
    } else {
      // For cloud paths (S3), normalize the path for storage compatibility
      // Uses Map-based normalization (fast path - no HadoopConf)
      val normalized = CloudStorageProviderFactory.normalizePathForTantivy(filePath.toString, serializedOptions)
      normalized
    }

    // Generate node ID for the split (hostname + executor ID)
    val nodeId = java.net.InetAddress.getLocalHost.getHostName + "-" +
      Option(System.getProperty("spark.executor.id")).getOrElse("driver")

    // Create split from the index using the search engine
    val (splitPath, splitMetadata) = searchEngine.commitAndCreateSplit(outputPath, partitionId.toLong, nodeId)

    // Get split file size using cloud storage provider (Map-based fast path)
    val splitSize = {
      val cloudProvider = CloudStorageProviderFactory.createProvider(outputPath, serializedOptions)
      try {
        val fileInfo = cloudProvider.getFileInfo(outputPath)
        fileInfo.map(_.size).getOrElse {
          logger.warn(s"Could not get file info for $outputPath using cloud provider")
          0L
        }
      } finally
        cloudProvider.close()
    }

    // Normalize the splitPath for tantivy4java compatibility (convert s3a:// to s3://)
    val _ = {
      val cloudProvider = CloudStorageProviderFactory.createProvider(outputPath, serializedOptions)
      try
        cloudProvider.normalizePathForTantivy(splitPath)
      finally
        cloudProvider.close()
    }

    logger.info(s"Created split file $fileName with $splitSize bytes, $recordCount records")

    val rawMinValues = statistics.getMinValues
    val rawMaxValues = statistics.getMaxValues

    // Apply statistics truncation to prevent transaction log bloat from long values
    import io.indextables.spark.util.StatisticsTruncation
    val configMap = options.asCaseSensitiveMap().asScala.toMap
    val (minValues, maxValues) = StatisticsTruncation.truncateStatistics(
      rawMinValues,
      rawMaxValues,
      configMap
    )

    // For AddAction path, we need to store the relative path including partition directory
    // Format: [partitionDir/]filename.split
    val addActionPath = if (partitionValues.nonEmpty) {
      // Include partition directory in the path
      s"$partitionKey/$fileName"
    } else if (outputPath != filePath.toString) {
      // Path normalization was applied - calculate relative path from table path to normalized output path
      val tablePath = normalizedTablePath.toString
      val tableUri  = java.net.URI.create(tablePath)
      val outputUri = java.net.URI.create(outputPath)

      if (tableUri.getScheme == outputUri.getScheme && tableUri.getHost == outputUri.getHost) {
        // Same scheme and host - calculate relative path
        val tableKey  = tableUri.getPath.stripPrefix("/")
        val outputKey = outputUri.getPath.stripPrefix("/")

        // For S3Mock flattening, we need to store the complete relative path that will
        // resolve to the flattened location when combined with the table path
        if (outputKey.contains("___") && !tableKey.contains("___")) {
          // S3Mock flattening occurred - store the entire flattened key relative to bucket
          val flattenedKey = outputKey
          flattenedKey
        } else if (outputKey.startsWith(tableKey)) {
          // Standard case - remove table prefix to get relative path
          val relativePath = outputKey.substring(tableKey.length).stripPrefix("/")
          relativePath
        } else {
          fileName
        }
      } else {
        fileName
      }
    } else {
      fileName // No normalization was applied
    }

    // Extract ALL metadata from tantivy4java SplitMetadata for complete pipeline coverage
    val (
      footerStartOffset,
      footerEndOffset,
      hasFooterOffsets,
      timeRangeStart,
      timeRangeEnd,
      splitTags,
      deleteOpstamp,
      numMergeOps,
      docMappingJson,
      uncompressedSizeBytes
    ) =
      if (splitMetadata != null) {
        val timeStart = Option(splitMetadata.getTimeRangeStart()).map(_.toString)
        val timeEnd   = Option(splitMetadata.getTimeRangeEnd()).map(_.toString)
        val tags = Option(splitMetadata.getTags()).filter(!_.isEmpty).map { tagSet =>
          import scala.jdk.CollectionConverters._
          tagSet.asScala.toSet
        }
        val originalDocMapping = Option(splitMetadata.getDocMappingJson())
        logger.debug(s"EXTRACTED docMappingJson from tantivy4java: ${if (originalDocMapping.isDefined)
            s"PRESENT (${originalDocMapping.get.length} chars)"
          else "MISSING/NULL"}")

        val docMapping = if (originalDocMapping.isDefined) {
          logger.debug(s"docMappingJson FULL CONTENT: ${originalDocMapping.get}")
          originalDocMapping
        } else {
          // WORKAROUND: If tantivy4java didn't provide docMappingJson, create a minimal schema mapping
          logger.warn(s"🔧 WORKAROUND: tantivy4java docMappingJson is missing - creating minimal field mapping")

          // Create a minimal field mapping that tantivy4java can understand
          // Based on Quickwit/Tantivy schema format expectations
          val fieldMappings = writeSchema.fields
            .map { field =>
              val fieldType = field.dataType.typeName match {
                case "string"             => "text"
                case "integer" | "long"   => "i64"
                case "float" | "double"   => "f64"
                case "boolean"            => "bool"
                case "date" | "timestamp" => "datetime"
                case _                    => "text" // Default fallback
              }
              s""""${field.name}": {"type": "$fieldType", "indexed": true}"""
            }
            .mkString(", ")

          val minimalSchema = s"""{"fields": {$fieldMappings}}"""
          logger.warn(s"🔧 Using minimal field mapping as docMappingJson: ${minimalSchema
              .take(200)}${if (minimalSchema.length > 200) "..." else ""}")

          Some(minimalSchema)
        }

        if (splitMetadata.hasFooterOffsets()) {
          (
            Some(splitMetadata.getFooterStartOffset()),
            Some(splitMetadata.getFooterEndOffset()),
            true,
            timeStart,
            timeEnd,
            tags,
            Some(splitMetadata.getDeleteOpstamp()),
            Some(splitMetadata.getNumMergeOps()),
            docMapping,
            Some(splitMetadata.getUncompressedSizeBytes())
          )
        } else {
          (
            None,
            None,
            false,
            timeStart,
            timeEnd,
            tags,
            Some(splitMetadata.getDeleteOpstamp()),
            Some(splitMetadata.getNumMergeOps()),
            docMapping,
            Some(splitMetadata.getUncompressedSizeBytes())
          )
        }
      } else {
        (None, None, false, None, None, None, None, None, None, None)
      }

    val addAction = AddAction(
      path = addActionPath,              // Use the path that will correctly resolve during read
      partitionValues = partitionValues, // Use extracted partition values for metadata
      size = splitSize,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(recordCount),
      minValues = if (minValues.nonEmpty) Some(minValues) else None,
      maxValues = if (maxValues.nonEmpty) Some(maxValues) else None,
      // Footer offset optimization metadata for 87% network traffic reduction
      footerStartOffset = footerStartOffset,
      footerEndOffset = footerEndOffset,
      // Hotcache fields deprecated in v0.24.1 - no longer stored in transaction log
      hotcacheStartOffset = None,
      hotcacheLength = None,
      hasFooterOffsets = hasFooterOffsets,
      // Complete tantivy4java SplitMetadata fields for full pipeline coverage
      timeRangeStart = timeRangeStart,
      timeRangeEnd = timeRangeEnd,
      splitTags = splitTags,
      deleteOpstamp = deleteOpstamp,
      numMergeOps = numMergeOps,
      docMappingJson = docMappingJson,
      uncompressedSizeBytes = uncompressedSizeBytes
    )

    if (partitionValues.nonEmpty) {
      logger.info(s"📁 Created partitioned split with values: $partitionValues")
    }

    // Log footer offset optimization status
    if (hasFooterOffsets) {
      logger.info(s"🚀 FOOTER OFFSET OPTIMIZATION: Split created with metadata for 87% network traffic reduction")
      logger.debug(s"   Footer offsets: ${footerStartOffset.get}-${footerEndOffset.get}")
      logger.debug(s"   Hotcache: deprecated (using footer offsets instead)")
    } else {
      logger.debug(s"📁 STANDARD: Split created without footer offset optimization")
    }

    logger.info(s"📝 AddAction created with path: ${addAction.path}")

    addAction
  }

  override def abort(): Unit = {
    logger.warn(s"Aborting writer for partition $partitionId (${rolledActions.size} splits already uploaded)")
    singleWriter.foreach { case (engine, _, _) => engine.close() }
    partitionWriters.values.foreach { case (engine, _, _) => engine.close() }
    rolledActions.clear()
  }

  override def close(): Unit = {
    singleWriter.foreach { case (engine, _, _) => engine.close() }
    partitionWriters.values.foreach { case (engine, _, _) => engine.close() }
  }
}
