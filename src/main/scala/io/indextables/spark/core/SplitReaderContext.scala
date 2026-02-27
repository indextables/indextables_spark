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

import java.time.Instant

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.filters.MixedBooleanFilter
import io.indextables.spark.search.SplitSearchEngine
import io.indextables.spark.storage.{BatchOptMetrics, BatchOptimizationMetricsAccumulator, SplitCacheConfig}
import io.indextables.spark.transaction.AddAction
import io.indextables.tantivy4java.split.{SplitMatchAllQuery, SplitQuery}
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import org.slf4j.LoggerFactory

/**
 * Shared initialization context for split-based partition readers (both row and columnar).
 *
 * Encapsulates the duplicated logic for: effective limit calculation, path resolution, cache config,
 * companion validation, path normalization, footer validation, SplitMetadata reconstruction,
 * SplitSearchEngine creation, split field name retrieval, partition filter separation, range filter
 * stats optimization, IndexQuery cleanup, and SplitQuery building.
 *
 * Both [[IndexTables4SparkPartitionReader]] and [[CompanionColumnarPartitionReader]] compose this
 * class (has-a) rather than inheriting from it, avoiding trait mixin complexity with different
 * `PartitionReader[T]` type parameters.
 */
class SplitReaderContext(
  addAction: AddAction,
  readSchema: StructType,
  fullTableSchema: StructType,
  filters: Array[Filter],
  limit: Option[Int],
  config: Map[String, String],
  tablePath: Path,
  indexQueryFilters: Array[Any],
  metricsAccumulator: Option[BatchOptimizationMetricsAccumulator]
) {

  private val logger = LoggerFactory.getLogger(classOf[SplitReaderContext])

  /** Effective limit: use pushed limit, then configurable default, then hardcoded fallback. */
  val effectiveLimit: Int = {
    val configuredDefault = config
      .get("spark.indextables.read.defaultLimit")
      .flatMap(s => scala.util.Try(s.toInt).toOption)
      .getOrElse(250)
    limit.getOrElse(configuredDefault)
  }

  /** Resolved file path from AddAction against table path. */
  val filePath: String = PathResolutionUtils.resolveSplitPathAsString(addAction.path, tablePath.toString)

  /** Partition column names from AddAction. */
  lazy val partitionColumnNames: Set[String] = addAction.partitionValues.keys.toSet

  /** Data field names: readSchema fields excluding partition columns (not stored in parquet). */
  lazy val dataFieldNames: Array[String] =
    readSchema.fields.map(_.name).filterNot(partitionColumnNames.contains)

  /** Baseline metrics captured at creation for delta computation. */
  val baselineMetrics: BatchOptMetrics =
    if (metricsAccumulator.isDefined) BatchOptMetrics.fromJavaMetrics()
    else BatchOptMetrics.empty

  /** Cached options map for filter conversion. */
  lazy val cachedOptionsMap: CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(config.asJava)

  /** Create cache configuration from Spark options with diagnostic logging. */
  def createCacheConfig(): SplitCacheConfig = {
    val hasCompanionKey = config.contains("spark.indextables.companion.parquetTableRoot")
    val companionRoot   = config.get("spark.indextables.companion.parquetTableRoot")
    logger.info(
      s"[EXECUTOR] createCacheConfig for ${addAction.path}: " +
        s"companionMode=$hasCompanionKey, " +
        s"parquetTableRoot=${companionRoot.getOrElse("NONE")}, " +
        s"totalConfigKeys=${config.size}"
    )
    io.indextables.spark.util.ConfigUtils.createSplitCacheConfig(config, Some(tablePath.toString))
  }

  /**
   * Create a SplitSearchEngine from the AddAction metadata.
   *
   * Handles companion validation, path normalization, footer validation, SplitMetadata
   * reconstruction, and SplitSearchEngine creation.
   */
  def createSplitSearchEngine(): SplitSearchEngine = {
    val cacheConfig = createCacheConfig()

    // Fail fast: companion splits must have companion config
    if (addAction.companionDeltaVersion.isDefined && cacheConfig.companionSourceTableRoot.isEmpty) {
      throw new IllegalStateException(
        s"COMPANION CONFIG MISSING: Split ${addAction.path} has companionDeltaVersion=" +
          s"${addAction.companionDeltaVersion.get} but companionSourceTableRoot is None. " +
          s"Config keys: ${config.keys.filter(_.contains("companion")).mkString(", ")}. " +
          s"This indicates the companion parquetTableRoot was not propagated from ScanBuilder.effectiveConfig."
      )
    }

    // Normalize path for tantivy4java (s3a://->s3://, abfss://->azure://, etc.)
    val actualPath = if (filePath.startsWith("file:")) {
      filePath
    } else {
      io.indextables.spark.io.CloudStorageProviderFactory.normalizePathForTantivy(filePath, config)
    }

    // Footer offsets required
    if (!addAction.hasFooterOffsets || addAction.footerStartOffset.isEmpty) {
      throw new RuntimeException(
        s"AddAction for $actualPath does not contain required footer offsets. " +
          "All 'add' entries in the transaction log must contain footer offset metadata."
      )
    }

    // Reconstruct SplitMetadata from AddAction
    val splitMetadata = new QuickwitSplit.SplitMetadata(
      addAction.path.split("/").last.replace(".split", ""),
      "tantivy4spark-index",
      0L,
      "tantivy4spark-source",
      "tantivy4spark-node",
      toLongSafeOption(addAction.numRecords),
      toLongSafeOption(addAction.uncompressedSizeBytes),
      addAction.timeRangeStart.map(Instant.parse).orNull,
      addAction.timeRangeEnd.map(Instant.parse).orNull,
      System.currentTimeMillis() / 1000,
      "Mature",
      addAction.splitTags.getOrElse(Set.empty[String]).asJava,
      toLongSafeOption(addAction.footerStartOffset),
      toLongSafeOption(addAction.footerEndOffset),
      toLongSafeOption(addAction.deleteOpstamp),
      addAction.numMergeOps.getOrElse(0),
      "doc-mapping-uid",
      addAction.docMappingJson.orNull,
      java.util.Collections.emptyList[QuickwitSplit.SkippedSplit]()
    )

    val options = Some(IndexTables4SparkOptions(config))

    SplitSearchEngine.fromSplitFileWithMetadata(readSchema, actualPath, splitMetadata, cacheConfig, options)
  }

  /**
   * Build a SplitQuery from the filters and IndexQuery filters.
   *
   * Handles split field name retrieval, partition filter separation, range filter stats
   * optimization, IndexQuery cleanup, and filter-to-query conversion.
   */
  def buildSplitQuery(engine: SplitSearchEngine): SplitQuery = {
    // Get field names from split schema
    val splitFieldNames = {
      val splitSchema = engine.getSchema()
      try splitSchema.getFieldNames().asScala.toSet
      catch { case _: Exception => Set.empty[String] }
      finally splitSchema.close()
    }

    // Filter out partition-only filters (already handled by partition pruning)
    val nonPartitionFilters = if (partitionColumnNames.nonEmpty) {
      val (partitionOnly, nonPartition) =
        filters.partition(f => MixedBooleanFilter.isPartitionOnlyFilter(f, partitionColumnNames))
      if (partitionOnly.nonEmpty)
        logger.info(s"Excluding ${partitionOnly.length} partition filter(s) from Tantivy query: ${partitionOnly.mkString(", ")}")
      nonPartition
    } else {
      filters
    }

    // Filter out range filters redundant by statistics
    val optimizedFilters = if (addAction.minValues.nonEmpty && addAction.maxValues.nonEmpty) {
      val (redundantByStats, remaining) = nonPartitionFilters.partition(f =>
        isRangeFilterRedundantByStats(f, addAction.minValues.get, addAction.maxValues.get, fullTableSchema)
      )
      if (redundantByStats.nonEmpty)
        logger.info(s"Excluding ${redundantByStats.length} range filter(s) redundant by statistics: ${redundantByStats.mkString(", ")}")
      remaining
    } else {
      nonPartitionFilters
    }

    // Strip partition-only filters from IndexQuery filters
    val cleanedIndexQueryFilters = if (partitionColumnNames.nonEmpty && indexQueryFilters.nonEmpty) {
      val cleaned = MixedBooleanFilter.stripPartitionFiltersFromArray(indexQueryFilters, partitionColumnNames)
      if (cleaned.length != indexQueryFilters.length)
        logger.info(s"Stripped ${indexQueryFilters.length - cleaned.length} partition-only IndexQuery filter(s) from Tantivy query")
      cleaned
    } else {
      indexQueryFilters
    }

    val allFilters: Array[Any] = optimizedFilters.asInstanceOf[Array[Any]] ++ cleanedIndexQueryFilters

    if (allFilters.nonEmpty) {
      if (splitFieldNames.nonEmpty) {
        FiltersToQueryConverter.convertToSplitQuery(
          allFilters, engine, Some(splitFieldNames), Some(cachedOptionsMap)
        )
      } else {
        FiltersToQueryConverter.convertToSplitQuery(allFilters, engine, None, Some(cachedOptionsMap))
      }
    } else {
      new SplitMatchAllQuery()
    }
  }

  /** Collect batch optimization metrics delta and add to accumulator. */
  def collectMetricsDelta(): Unit = {
    metricsAccumulator.foreach { acc =>
      try {
        val currentMetrics = BatchOptMetrics.fromJavaMetrics()
        val delta = BatchOptMetrics(
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
  }

  /** Report bytesRead to Spark UI. */
  def reportBytesRead(): Unit = {
    val bytesRead = addAction.size
    org.apache.spark.sql.indextables.OutputMetricsUpdater.incInputMetrics(bytesRead, 0)
  }

  /** Safe conversion for Option[Any] to Long to handle JSON deserialization type variations. */
  private def toLongSafeOption(opt: Option[Any]): Long = opt match {
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

  /**
   * Check if a range filter is redundant based on min/max statistics. A filter is redundant if the
   * split's entire data range is within the filter's range, meaning all records in the split would
   * pass the filter anyway.
   *
   * Only applies to Date and Timestamp columns to avoid type conversion complexity.
   */
  private def isRangeFilterRedundantByStats(
    filter: Filter,
    minValues: Map[String, String],
    maxValues: Map[String, String],
    schema: StructType
  ): Boolean = {
    import org.apache.spark.sql.sources._

    def isDateOrTimestampColumn(attribute: String): Boolean =
      schema.fields.find(_.name == attribute).exists { field =>
        field.dataType match {
          case DateType | TimestampType => true
          case _                        => false
        }
      }

    def getColumnType(attribute: String): Option[DataType] =
      schema.fields.find(_.name == attribute).map(_.dataType)

    def parseTimestamp(value: Any, fromStats: Boolean): Option[Long] = value match {
      case ts: java.sql.Timestamp =>
        val epochSeconds = ts.getTime / 1000
        Some(epochSeconds * 1000000 + ts.getNanos / 1000)
      case s: String =>
        try {
          val micros = s.toLong
          Some(micros)
        } catch {
          case _: NumberFormatException =>
            try {
              val instant = Instant.parse(s)
              Some(instant.getEpochSecond * 1000000 + instant.getNano / 1000)
            } catch {
              case _: Exception =>
                try {
                  val ts           = java.sql.Timestamp.valueOf(s)
                  val epochSeconds = ts.getTime / 1000
                  Some(epochSeconds * 1000000 + ts.getNanos / 1000)
                } catch { case _: Exception => None }
            }
        }
      case l: Long => Some(if (fromStats) l else l * 1000)
      case i: Int  => Some(i.toLong * 1000)
      case _       => None
    }

    def parseDate(value: Any, fromStats: Boolean): Option[Long] = value match {
      case d: java.sql.Date => Some(d.toLocalDate.toEpochDay)
      case s: String =>
        try {
          val days = s.toLong
          Some(days)
        } catch {
          case _: NumberFormatException =>
            try Some(java.time.LocalDate.parse(s).toEpochDay)
            catch {
              case _: Exception =>
                try Some(java.sql.Date.valueOf(s).toLocalDate.toEpochDay)
                catch { case _: Exception => None }
            }
        }
      case l: Long => Some(l)
      case i: Int  => Some(i.toLong)
      case _       => None
    }

    def parseValue(value: Any, dataType: DataType, fromStats: Boolean): Option[Long] = dataType match {
      case TimestampType => parseTimestamp(value, fromStats)
      case DateType      => parseDate(value, fromStats)
      case _             => None
    }

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

      case And(left, right) =>
        isRangeFilterRedundantByStats(left, minValues, maxValues, schema) &&
        isRangeFilterRedundantByStats(right, minValues, maxValues, schema)

      case _ => false
    }
  }
}
