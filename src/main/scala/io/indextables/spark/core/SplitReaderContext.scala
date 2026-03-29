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
import org.slf4j.LoggerFactory

/**
 * Shared initialization context for split-based columnar partition readers.
 *
 * Encapsulates the duplicated logic for: effective limit calculation, path resolution, cache config, companion
 * validation, path normalization, footer validation, SplitMetadata reconstruction, SplitSearchEngine creation, split
 * field name retrieval, partition filter separation, range filter stats optimization, IndexQuery cleanup, and
 * SplitQuery building.
 *
 * [[ColumnarPartitionReader]] composes this class (has-a) rather than inheriting from it, avoiding trait mixin
 * complexity with the `PartitionReader[ColumnarBatch]` type parameter.
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
  metricsAccumulator: Option[BatchOptimizationMetricsAccumulator]) {

  private val logger = LoggerFactory.getLogger(classOf[SplitReaderContext])

  val readMode: String    = SplitReaderContext.resolveReadMode(config)
  val effectiveLimit: Int = SplitReaderContext.computeEffectiveLimit(config, limit)

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
   * Handles companion validation, path normalization, footer validation, SplitMetadata reconstruction, and
   * SplitSearchEngine creation.
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
    val splitMetadata = io.indextables.spark.util.SplitMetadataFactory.fromAddAction(addAction, tablePath.toString)

    val options = Some(IndexTables4SparkOptions(config))

    SplitSearchEngine.fromSplitFileWithMetadata(readSchema, actualPath, splitMetadata, cacheConfig, options)
  }

  /**
   * Build a SplitQuery from the filters and IndexQuery filters.
   *
   * Handles split field name retrieval, partition filter separation, range filter stats optimization, IndexQuery
   * cleanup, and filter-to-query conversion.
   */
  def buildSplitQuery(engine: SplitSearchEngine): SplitQuery = {
    // Get field names from split schema
    // CRITICAL: Schema must be closed to prevent native memory leak
    val splitFieldNames = {
      val splitSchema = engine.getSchema()
      try {
        val fieldNames = splitSchema.getFieldNames().asScala.toSet
        logger.debug(s"Split schema contains fields: ${fieldNames.mkString(", ")}")
        fieldNames
      } catch {
        case e: Exception =>
          logger.warn(s"Could not retrieve field names from split schema: ${e.getMessage}")
          Set.empty[String]
      } finally splitSchema.close()
    }

    // Filter out partition-only filters (already handled by partition pruning)
    // Then resolve partition column references in mixed filters (e.g., Or(score>250, category="A"))
    // by evaluating them against the known partition values for this split.
    val nonPartitionFilters = if (partitionColumnNames.nonEmpty) {
      val (partitionOnly, nonPartition) =
        filters.partition(f => MixedBooleanFilter.isPartitionOnlyFilter(f, partitionColumnNames))
      if (partitionOnly.nonEmpty)
        logger.info(
          s"Excluding ${partitionOnly.length} partition filter(s) from Tantivy query: ${partitionOnly.mkString(", ")}"
        )
      // Resolve partition column references within remaining mixed filters
      nonPartition.flatMap { f =>
        resolvePartitionReferences(f) match {
          case ResolvedTrue =>
            logger.info(s"Filter satisfied by partition values, eliminating: $f")
            None // Filter is always true for this split — drop it
          case ResolvedFalse =>
            // Filter is always false for this split — this shouldn't normally happen since
            // partition pruning already excluded non-matching splits, but handle gracefully
            logger.info(s"Filter contradicts partition values: $f")
            Some(f) // Keep it — tantivy will return 0 results
          case ResolvedFilter(resolved) =>
            if (resolved != f)
              logger.info(s"Simplified mixed filter: $f → $resolved")
            Some(resolved)
        }
      }
    } else {
      filters
    }

    // Range filter elimination by min/max statistics is now handled natively (FR4).
    // The native split search engine automatically eliminates redundant range filters
    // using per-file stats cached during listFilesArrowFfi().
    val optimizedFilters = nonPartitionFilters

    // Strip partition-only filters from IndexQuery filters
    val cleanedIndexQueryFilters = if (partitionColumnNames.nonEmpty && indexQueryFilters.nonEmpty) {
      val cleaned = MixedBooleanFilter.stripPartitionFiltersFromArray(indexQueryFilters, partitionColumnNames)
      if (cleaned.length != indexQueryFilters.length)
        logger.info(
          s"Stripped ${indexQueryFilters.length - cleaned.length} partition-only IndexQuery filter(s) from Tantivy query"
        )
      cleaned
    } else {
      indexQueryFilters
    }

    val allFilters: Array[Any] = optimizedFilters.asInstanceOf[Array[Any]] ++ cleanedIndexQueryFilters

    val splitQuery = if (allFilters.nonEmpty) {
      if (splitFieldNames.nonEmpty) {
        val q = FiltersToQueryConverter.convertToSplitQuery(
          allFilters,
          engine,
          Some(splitFieldNames),
          Some(cachedOptionsMap)
        )
        logger.debug(s"SplitQuery (with schema validation): ${q.getClass.getSimpleName}")
        q
      } else {
        val q = FiltersToQueryConverter.convertToSplitQuery(allFilters, engine, None, Some(cachedOptionsMap))
        logger.debug(s"SplitQuery (no schema validation): ${q.getClass.getSimpleName}")
        q
      }
    } else {
      new SplitMatchAllQuery()
    }
    splitQuery
  }

  /** Result of resolving partition column references within a filter. */
  sealed private trait PartitionResolution
  private case object ResolvedTrue                  extends PartitionResolution
  private case object ResolvedFalse                 extends PartitionResolution
  private case class ResolvedFilter(filter: Filter) extends PartitionResolution

  /**
   * Resolve partition column references in a filter against this split's partition values. For leaf filters on
   * partition columns, evaluates them against the known partition values. For And/Or, recursively resolves and
   * simplifies using boolean logic.
   */
  private def resolvePartitionReferences(filter: Filter): PartitionResolution = {
    import org.apache.spark.sql.sources._

    // Check if a leaf filter references only partition columns
    def isPartitionLeaf(f: Filter): Boolean =
      MixedBooleanFilter.isPartitionOnlyFilter(f, partitionColumnNames)

    // Evaluate a partition-only leaf filter against the known partition values
    def evaluatePartitionFilter(f: Filter): Boolean = f match {
      case EqualTo(attr, value) =>
        addAction.partitionValues.get(attr).exists(pv => partitionValueEquals(pv, value, attr))
      case EqualNullSafe(attr, value) =>
        addAction.partitionValues.get(attr).exists(pv => partitionValueEquals(pv, value, attr))
      case In(attr, values) =>
        addAction.partitionValues.get(attr).exists(pv => values.exists(v => partitionValueEquals(pv, v, attr)))
      case GreaterThan(attr, value) =>
        addAction.partitionValues.get(attr).exists(pv => partitionValueCompare(pv, value, attr) > 0)
      case GreaterThanOrEqual(attr, value) =>
        addAction.partitionValues.get(attr).exists(pv => partitionValueCompare(pv, value, attr) >= 0)
      case LessThan(attr, value) =>
        addAction.partitionValues.get(attr).exists(pv => partitionValueCompare(pv, value, attr) < 0)
      case LessThanOrEqual(attr, value) =>
        addAction.partitionValues.get(attr).exists(pv => partitionValueCompare(pv, value, attr) <= 0)
      case IsNotNull(attr) =>
        addAction.partitionValues.contains(attr)
      case IsNull(attr) =>
        !addAction.partitionValues.contains(attr)
      case Not(child) =>
        !evaluatePartitionFilter(child)
      case _ =>
        true // Conservative: assume true for unsupported filter types
    }

    filter match {
      case Or(left, right) =>
        val l = resolvePartitionReferences(left)
        val r = resolvePartitionReferences(right)
        (l, r) match {
          case (ResolvedTrue, _) | (_, ResolvedTrue)    => ResolvedTrue
          case (ResolvedFalse, other)                   => other
          case (other, ResolvedFalse)                   => other
          case (ResolvedFilter(lf), ResolvedFilter(rf)) => ResolvedFilter(Or(lf, rf))
        }
      case And(left, right) =>
        val l = resolvePartitionReferences(left)
        val r = resolvePartitionReferences(right)
        (l, r) match {
          case (ResolvedFalse, _) | (_, ResolvedFalse)  => ResolvedFalse
          case (ResolvedTrue, other)                    => other
          case (other, ResolvedTrue)                    => other
          case (ResolvedFilter(lf), ResolvedFilter(rf)) => ResolvedFilter(And(lf, rf))
        }
      case Not(child) =>
        resolvePartitionReferences(child) match {
          case ResolvedTrue      => ResolvedFalse
          case ResolvedFalse     => ResolvedTrue
          case ResolvedFilter(f) => ResolvedFilter(Not(f))
        }
      case leaf if isPartitionLeaf(leaf) =>
        if (evaluatePartitionFilter(leaf)) ResolvedTrue else ResolvedFalse
      case other =>
        ResolvedFilter(other)
    }
  }

  /** Compare a partition value string with a filter value for equality. */
  private def partitionValueEquals(
    partitionValue: String,
    filterValue: Any,
    attr: String
  ): Boolean =
    filterValue match {
      case s: String  => partitionValue == s
      case i: Int     => scala.util.Try(partitionValue.toInt).toOption.contains(i)
      case l: Long    => scala.util.Try(partitionValue.toLong).toOption.contains(l)
      case d: Double  => scala.util.Try(partitionValue.toDouble).toOption.contains(d)
      case f: Float   => scala.util.Try(partitionValue.toFloat).toOption.contains(f)
      case b: Boolean => scala.util.Try(partitionValue.toBoolean).toOption.contains(b)
      case date: java.sql.Date =>
        scala.util
          .Try(java.time.LocalDate.parse(partitionValue))
          .toOption
          .exists(_.toEpochDay == date.toLocalDate.toEpochDay)
      case ts: java.sql.Timestamp =>
        scala.util
          .Try(java.time.Instant.parse(partitionValue))
          .orElse(scala.util.Try(java.time.Instant.parse(partitionValue + "Z")))
          .toOption
          .exists(_.toEpochMilli == ts.getTime)
      case _ => partitionValue == filterValue.toString
    }

  /** Compare a partition value with a filter value, returning negative/zero/positive like Comparable. */
  private def partitionValueCompare(
    partitionValue: String,
    filterValue: Any,
    attr: String
  ): Int = {
    val fieldType = fullTableSchema.fields.find(_.name == attr).map(_.dataType)
    fieldType match {
      case Some(IntegerType) =>
        scala.util.Try(partitionValue.toInt).getOrElse(0).compareTo(filterValue.asInstanceOf[Int])
      case Some(LongType) =>
        scala.util.Try(partitionValue.toLong).getOrElse(0L).compareTo(filterValue.asInstanceOf[Long])
      case Some(DoubleType) =>
        scala.util.Try(partitionValue.toDouble).getOrElse(0.0).compareTo(filterValue.asInstanceOf[Double])
      case Some(FloatType) =>
        scala.util.Try(partitionValue.toFloat).getOrElse(0.0f).compareTo(filterValue.asInstanceOf[Float])
      case Some(DateType) =>
        val pvDays = scala.util.Try(java.time.LocalDate.parse(partitionValue).toEpochDay.toInt).getOrElse(0)
        filterValue match {
          case d: java.sql.Date => pvDays.compareTo(d.toLocalDate.toEpochDay.toInt)
          case s: String        => partitionValue.compareTo(s)
          case i: Int           => pvDays.compareTo(i)
          case _                => partitionValue.compareTo(filterValue.toString)
        }
      case _ => partitionValue.compareTo(filterValue.toString)
    }
  }

  /** Collect batch optimization metrics delta and add to accumulator. */
  def collectMetricsDelta(): Unit =
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

  /** Report bytesRead to Spark UI. */
  def reportBytesRead(): Unit = {
    val bytesRead = addAction.size
    org.apache.spark.sql.indextables.OutputMetricsUpdater.incInputMetrics(bytesRead, 0)
  }

  /**
   * Check if a range filter is redundant based on min/max statistics. A filter is redundant if the split's entire data
   * Redundant range filter elimination is now handled natively (FR4).
   * The native split search engine automatically uses per-file stats
   * cached during listFilesArrowFfi() to eliminate redundant range filters.
   * See: TANTIVY4JAVA_FR4_API_GAP.md (resolved)
   */
}

object SplitReaderContext {

  /**
   * Resolve and validate the read mode from configuration.
   *
   * @return
   *   "fast" or "complete"
   * @throws IllegalArgumentException
   *   if an invalid mode is configured
   */
  def resolveReadMode(config: Map[String, String]): String = {
    val mode = config.getOrElse(IndexTables4SparkOptions.READ_MODE, "fast").toLowerCase
    require(
      mode == "fast" || mode == "complete",
      s"Invalid ${IndexTables4SparkOptions.READ_MODE} value '$mode'. Must be 'fast' or 'complete'."
    )
    mode
  }

  /**
   * Compute the effective result limit from config and any pushed limit.
   *
   *   - **fast** (default): pushed limit → configured `defaultLimit` → 250
   *   - **complete**: pushed limit → Int.MaxValue (return everything)
   */
  def computeEffectiveLimit(config: Map[String, String], limit: Option[Int]): Int =
    resolveReadMode(config) match {
      case "complete" =>
        limit.getOrElse(Int.MaxValue)
      case _ =>
        val configuredDefault = config
          .get("spark.indextables.read.defaultLimit")
          .flatMap(s => scala.util.Try(s.toInt).toOption)
          .getOrElse(250)
        limit.getOrElse(configuredDefault)
    }
}
