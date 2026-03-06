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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import io.indextables.spark.arrow.AggregationArrowFfiHelper
import io.indextables.spark.expressions.{BucketAggregationConfig, DateHistogramConfig, HistogramConfig, RangeConfig}
import io.indextables.spark.search.SplitSearchEngine
import io.indextables.spark.storage.GlobalSplitCacheManager
import io.indextables.tantivy4java.aggregation._
import org.slf4j.LoggerFactory

/**
 * Columnar partition reader for GROUP BY aggregations using Arrow FFI.
 *
 * Handles single-column terms, multi-column terms (FR-1), bucket aggregations (DateHistogram, Histogram, Range),
 * bucket + nested terms (FR-2), and partition-only GROUP BY. Returns results as ColumnarBatch directly from Arrow FFI.
 */
class GroupByAggregateColumnarReader(
  partition: IndexTables4SparkGroupByAggregatePartition,
  sparkSession: SparkSession,
  schema: StructType)
    extends PartitionReader[ColumnarBatch] {

  private val logger      = LoggerFactory.getLogger(classOf[GroupByAggregateColumnarReader])
  private val helper      = new AggregationArrowFfiHelper()
  private var batch: ColumnarBatch = _
  private var consumed    = false
  private var initialized = false

  private val U = GroupByColumnarReaderUtils
  private val aggExprs = partition.aggregation.aggregateExpressions

  override def next(): Boolean = {
    if (consumed) return false
    if (!initialized) {
      initialize()
      initialized = true
    }
    consumed = true
    batch != null && batch.numRows() > 0
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = {
    if (batch != null) batch.close()
    helper.close()

    val bytesRead = partition.split.size
    org.apache.spark.sql.indextables.OutputMetricsUpdater.incInputMetrics(bytesRead, 0)
  }

  private def initialize(): Unit = {
    logger.debug(s"GroupByAggregateColumnarReader: initializing for split ${partition.split.path}")

    try {
      val cacheConfig = createCacheConfig()

      val resolvedPath = PathResolutionUtils.resolveSplitPathAsString(
        partition.split.path,
        partition.tablePath.toString
      )
      val splitPath = io.indextables.spark.io.CloudStorageProviderFactory.normalizePathForTantivy(
        resolvedPath,
        partition.config
      )

      val splitMetadata = io.indextables.spark.util.SplitMetadataFactory.fromAddAction(
        partition.split,
        partition.tablePath.toString
      )
      val options = Some(IndexTables4SparkOptions(partition.config))

      val splitSearchEngine = SplitSearchEngine.fromSplitFileWithMetadata(
        partition.schema,
        splitPath,
        splitMetadata,
        cacheConfig,
        options
      )
      val searcher = splitSearchEngine.getSplitSearcher()

      val queryAstJson = U.buildQueryAstJson(
        partition.pushedFilters, partition.indexQueryFilters,
        partition.partitionColumns, splitSearchEngine, partition.config
      )

      // Dispatch based on bucket config
      partition.bucketConfig match {
        case Some(bucketCfg) =>
          batch = executeBucketAggregation(searcher, queryAstJson, bucketCfg)
        case None =>
          batch = executeTermsAggregation(searcher, queryAstJson, cacheConfig)
      }

      logger.debug(s"GroupByAggregateColumnarReader: ${if (batch != null) batch.numRows() else 0} rows from split ${partition.split.path}")

    } catch {
      case e: IllegalArgumentException => throw e
      case e: io.indextables.spark.exceptions.IndexQueryParseException => throw e
      case e: Exception =>
        throw new RuntimeException(
          s"GroupByAggregateColumnarReader: failed for split ${partition.split.path}", e)
    }
  }

  private def executeTermsAggregation(
    searcher: io.indextables.tantivy4java.split.SplitSearcher,
    queryAstJson: String,
    cacheConfig: io.indextables.spark.storage.SplitCacheConfig
  ): ColumnarBatch = {
    val allGroupByColumns = partition.groupByColumns
    val (partitionGroupByCols, dataGroupByCols) = allGroupByColumns.partition(partition.partitionColumns.contains)

    // Handle all-partition GROUP BY: run metric aggregations via FFI, wrap with partition key columns
    if (dataGroupByCols.isEmpty) {
      return executePartitionOnlyAggregation(searcher, queryAstJson, cacheConfig, allGroupByColumns)
    }

    // Build TermsAggregation with sub-aggs
    val (termsAgg, aggName) = if (dataGroupByCols.length == 1) {
      val agg = new TermsAggregation("group_by_terms", dataGroupByCols(0), 1000, 0)
      U.addSubAggregations(agg, aggExprs)
      (agg.asInstanceOf[io.indextables.tantivy4java.split.SplitAggregation], "group_by_terms")
    } else {
      val agg = new MultiTermsAggregation("group_by_terms", dataGroupByCols, 1000, 0)
      U.addMultiTermsSubAggregations(agg, aggExprs)
      (agg.asInstanceOf[io.indextables.tantivy4java.split.SplitAggregation], "group_by_terms")
    }

    // Execute via FFI
    val aggJson  = helper.buildAggJson(aggName, termsAgg)
    val (_, columnNames, columnTypes, _) = helper.querySchema(searcher, queryAstJson, aggName, aggJson)
    val ffiBatch = helper.executeSingleSplit(searcher, queryAstJson, aggName, aggJson)

    try {
      val isMultiKey = dataGroupByCols.length > 1
      val numFfiKeyColumns = columnNames.count(n => n == "key" || n.startsWith("key_"))
      val needsPipeKeySplitting = isMultiKey && numFfiKeyColumns < dataGroupByCols.length

      val assembled = if (needsPipeKeySplitting) {
        U.assembleGroupByBatchWithCompoundKeys(ffiBatch, columnNames, dataGroupByCols.length, aggExprs, dataGroupByCols, schema, columnTypes)
      } else {
        U.assembleGroupByBatch(ffiBatch, numFfiKeyColumns, columnNames, aggExprs, dataGroupByCols, schema, columnTypes)
      }

      if (partitionGroupByCols.nonEmpty) {
        U.injectPartitionColumns(assembled, allGroupByColumns, dataGroupByCols, partitionGroupByCols,
          partition.splitPartitionValues, aggExprs, schema)
      } else {
        assembled
      }
    } finally {
      ffiBatch.close()
    }
  }

  private def executePartitionOnlyAggregation(
    searcher: io.indextables.tantivy4java.split.SplitSearcher,
    queryAstJson: String,
    cacheConfig: io.indextables.spark.storage.SplitCacheConfig,
    allGroupByColumns: Array[String]
  ): ColumnarBatch = {
    val fastFields = io.indextables.spark.transaction.EnhancedTransactionLogCache
      .getDocMappingMetadata(partition.split).fastFields

    val aggregations = U.buildPartitionOnlyAggregations(aggExprs, fastFields, partition.schema)

    val cacheManager = GlobalSplitCacheManager.getInstance(cacheConfig)
    val searchers = new java.util.ArrayList[io.indextables.tantivy4java.split.SplitSearcher]()
    searchers.add(searcher)

    val ffiBatches = helper.executeMultiSplitMultiAgg(cacheManager, searchers, queryAstJson, aggregations)

    U.assemblePartitionOnlyBatch(ffiBatches, allGroupByColumns, partition.splitPartitionValues, aggExprs, schema)
  }

  private def executeBucketAggregation(
    searcher: io.indextables.tantivy4java.split.SplitSearcher,
    queryAstJson: String,
    bucketConfig: BucketAggregationConfig
  ): ColumnarBatch = {
    val aggName = "bucket_agg"
    val additionalGroupByColumns = partition.groupByColumns.drop(1)

    val bucketAgg = bucketConfig match {
      case dhc: DateHistogramConfig =>
        val agg = new DateHistogramAggregation(aggName, dhc.fieldName)
        agg.setFixedInterval(dhc.interval)
        dhc.offset.foreach(o => agg.setOffset(o))
        if (dhc.minDocCount > 0) agg.setMinDocCount(dhc.minDocCount)
        dhc.hardBoundsMin.foreach(min => dhc.hardBoundsMax.foreach(max => agg.setHardBounds(min, max)))
        dhc.extendedBoundsMin.foreach(min => dhc.extendedBoundsMax.foreach(max => agg.setExtendedBounds(min, max)))
        if (additionalGroupByColumns.nonEmpty) {
          agg.addSubAggregation(U.buildNestedTermsAggregation(additionalGroupByColumns.toList, 0, aggExprs))
        } else {
          U.addBucketSubAggregations(agg, aggExprs)
        }
        agg.asInstanceOf[io.indextables.tantivy4java.split.SplitAggregation]

      case hc: HistogramConfig =>
        val agg = new HistogramAggregation(aggName, hc.fieldName, hc.interval)
        if (hc.offset != 0.0) agg.setOffset(hc.offset)
        if (hc.minDocCount > 0) agg.setMinDocCount(hc.minDocCount)
        hc.hardBoundsMin.foreach(min => hc.hardBoundsMax.foreach(max => agg.setHardBounds(min, max)))
        hc.extendedBoundsMin.foreach(min => hc.extendedBoundsMax.foreach(max => agg.setExtendedBounds(min, max)))
        if (additionalGroupByColumns.nonEmpty) {
          agg.addSubAggregation(U.buildNestedTermsAggregation(additionalGroupByColumns.toList, 0, aggExprs))
        } else {
          U.addBucketSubAggregations(agg, aggExprs)
        }
        agg.asInstanceOf[io.indextables.tantivy4java.split.SplitAggregation]

      case rc: RangeConfig =>
        val agg = new RangeAggregation(aggName, rc.fieldName)
        rc.ranges.foreach { range =>
          val from: java.lang.Double = range.from.map(d => java.lang.Double.valueOf(d)).orNull
          val to: java.lang.Double   = range.to.map(d => java.lang.Double.valueOf(d)).orNull
          agg.addRange(range.key, from, to)
        }
        if (additionalGroupByColumns.nonEmpty) {
          agg.addSubAggregation(U.buildNestedTermsAggregation(additionalGroupByColumns.toList, 0, aggExprs))
        } else {
          U.addBucketSubAggregations(agg, aggExprs)
        }
        agg.asInstanceOf[io.indextables.tantivy4java.split.SplitAggregation]
    }

    val aggJson = helper.buildAggJson(aggName, bucketAgg)
    val (_, columnNames, columnTypes, _) = helper.querySchema(searcher, queryAstJson, aggName, aggJson)
    val ffiBatch = helper.executeSingleSplit(searcher, queryAstJson, aggName, aggJson)

    try {
      val dataGroupByCols = partition.groupByColumns.filterNot(partition.partitionColumns.contains)
      U.assembleBucketBatch(ffiBatch, columnNames, aggExprs, dataGroupByCols, schema, columnTypes)
    } finally {
      ffiBatch.close()
    }
  }

  private def createCacheConfig(): io.indextables.spark.storage.SplitCacheConfig =
    io.indextables.spark.util.ConfigUtils.createSplitCacheConfig(
      partition.config,
      Some(partition.tablePath.toString)
    )
}

/**
 * Columnar partition reader for multi-split GROUP BY aggregations using Arrow FFI.
 *
 * Uses native `SplitCacheManager.multiSplitAggregateArrowFfi()` which searches each split,
 * merges intermediate results via native `merge_fruits()`, and exports merged Arrow data.
 */
class MultiSplitGroupByAggregateColumnarReader(
  partition: IndexTables4SparkMultiSplitGroupByAggregatePartition,
  sparkSession: SparkSession,
  schema: StructType)
    extends PartitionReader[ColumnarBatch] {

  private val logger      = LoggerFactory.getLogger(classOf[MultiSplitGroupByAggregateColumnarReader])
  private val helper      = new AggregationArrowFfiHelper()
  private var batch: ColumnarBatch = _
  private var consumed    = false
  private var initialized = false

  private val U = GroupByColumnarReaderUtils
  private val aggExprs = partition.aggregation.aggregateExpressions

  override def next(): Boolean = {
    if (consumed) return false
    if (!initialized) {
      initialize()
      initialized = true
    }
    consumed = true
    batch != null && batch.numRows() > 0
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = {
    if (batch != null) batch.close()
    helper.close()

    val totalBytesRead = partition.splits.map(_.size).sum
    org.apache.spark.sql.indextables.OutputMetricsUpdater.incInputMetrics(totalBytesRead, 0)
  }

  private def initialize(): Unit = {
    logger.debug(s"MultiSplitGroupByAggregateColumnarReader: initializing with ${partition.splits.length} splits")

    try {
      val cacheConfig = io.indextables.spark.util.ConfigUtils.createSplitCacheConfig(
        partition.config,
        Some(partition.tablePath.toString)
      )
      val cacheManager = GlobalSplitCacheManager.getInstance(cacheConfig)

      // Create a SplitSearcher for each split
      val searchers = new java.util.ArrayList[io.indextables.tantivy4java.split.SplitSearcher]()
      partition.splits.foreach { split =>
        val resolvedPath = PathResolutionUtils.resolveSplitPathAsString(
          split.path, partition.tablePath.toString
        )
        val splitPath = io.indextables.spark.io.CloudStorageProviderFactory.normalizePathForTantivy(
          resolvedPath, partition.config
        )
        val splitMetadata = io.indextables.spark.util.SplitMetadataFactory.fromAddAction(
          split, partition.tablePath.toString
        )
        val options = Some(IndexTables4SparkOptions(partition.config))
        val engine = SplitSearchEngine.fromSplitFileWithMetadata(
          partition.schema, splitPath, splitMetadata, cacheConfig, options
        )
        searchers.add(engine.getSplitSearcher())
      }

      // Build query from filters using first split's engine
      val firstSplit = partition.splits.head
      val firstResolvedPath = PathResolutionUtils.resolveSplitPathAsString(
        firstSplit.path, partition.tablePath.toString
      )
      val firstSplitPath = io.indextables.spark.io.CloudStorageProviderFactory.normalizePathForTantivy(
        firstResolvedPath, partition.config
      )
      val firstMetadata = io.indextables.spark.util.SplitMetadataFactory.fromAddAction(
        firstSplit, partition.tablePath.toString
      )
      val firstEngine = SplitSearchEngine.fromSplitFileWithMetadata(
        partition.schema, firstSplitPath, firstMetadata, cacheConfig,
        Some(IndexTables4SparkOptions(partition.config))
      )
      val queryAstJson = U.buildQueryAstJson(
        partition.pushedFilters, partition.indexQueryFilters,
        partition.partitionColumns, firstEngine, partition.config
      )

      // Dispatch based on bucket config vs terms aggregation
      val allGroupByColumns = partition.groupByColumns
      val dataGroupByCols = allGroupByColumns.filterNot(partition.partitionColumns.contains)

      if (dataGroupByCols.isEmpty) {
        batch = executePartitionOnlyAggregation(cacheManager, searchers, queryAstJson, allGroupByColumns)
        return
      }

      val ffiBatch = partition.bucketConfig match {
        case Some(bucketCfg) =>
          executeBucketAggregation(cacheManager, searchers, queryAstJson, bucketCfg)
        case None =>
          executeTermsAggregation(cacheManager, searchers, queryAstJson, dataGroupByCols)
      }

      // Inject partition columns if needed
      val partitionGroupByCols = allGroupByColumns.filter(partition.partitionColumns.contains)
      if (partitionGroupByCols.nonEmpty) {
        val partitionValues = Option(partition.splits.head.partitionValues).getOrElse(Map.empty[String, String])
        batch = U.injectPartitionColumns(ffiBatch, allGroupByColumns, dataGroupByCols, partitionGroupByCols,
          partitionValues, aggExprs, schema)
      } else {
        batch = ffiBatch
      }

      logger.debug(s"MultiSplitGroupByAggregateColumnarReader: ${if (batch != null) batch.numRows() else 0} rows from ${partition.splits.length} splits")

    } catch {
      case e: IllegalArgumentException => throw e
      case e: io.indextables.spark.exceptions.IndexQueryParseException => throw e
      case e: Exception =>
        throw new RuntimeException(
          s"MultiSplitGroupByAggregateColumnarReader: failed for ${partition.splits.length} splits", e)
    }
  }

  private def executePartitionOnlyAggregation(
    cacheManager: io.indextables.tantivy4java.split.SplitCacheManager,
    searchers: java.util.List[io.indextables.tantivy4java.split.SplitSearcher],
    queryAstJson: String,
    allGroupByColumns: Array[String]
  ): ColumnarBatch = {
    val partitionValues = Option(partition.splits.head.partitionValues).getOrElse(Map.empty[String, String])
    val fastFields = io.indextables.spark.transaction.EnhancedTransactionLogCache
      .getDocMappingMetadata(partition.splits.head).fastFields

    val aggregations = U.buildPartitionOnlyAggregations(aggExprs, fastFields, partition.schema)
    val ffiBatches = helper.executeMultiSplitMultiAgg(cacheManager, searchers, queryAstJson, aggregations)

    U.assemblePartitionOnlyBatch(ffiBatches, allGroupByColumns, partitionValues, aggExprs, schema)
  }

  private def executeTermsAggregation(
    cacheManager: io.indextables.tantivy4java.split.SplitCacheManager,
    searchers: java.util.List[io.indextables.tantivy4java.split.SplitSearcher],
    queryAstJson: String,
    dataGroupByCols: Array[String]
  ): ColumnarBatch = {
    val (termsAgg, aggName) = if (dataGroupByCols.length == 1) {
      val agg = new TermsAggregation("group_by_terms", dataGroupByCols(0), 1000, 0)
      U.addSubAggregations(agg, aggExprs)
      (agg.asInstanceOf[io.indextables.tantivy4java.split.SplitAggregation], "group_by_terms")
    } else {
      val agg = new MultiTermsAggregation("group_by_terms", dataGroupByCols, 1000, 0)
      U.addMultiTermsSubAggregations(agg, aggExprs)
      (agg.asInstanceOf[io.indextables.tantivy4java.split.SplitAggregation], "group_by_terms")
    }

    val aggJson = helper.buildAggJson(aggName, termsAgg)

    val firstSearcher = searchers.get(0)
    val (_, columnNames, columnTypes, _) = helper.querySchema(firstSearcher, queryAstJson, aggName, aggJson)
    val numFfiKeyColumns = columnNames.count(n => n == "key" || n.startsWith("key_"))

    // Native multi-split merge via merge_fruits
    val ffiBatch = helper.executeMultiSplit(cacheManager, searchers, queryAstJson, aggName, aggJson)

    try {
      val isMultiKey = dataGroupByCols.length > 1
      val needsPipeKeySplitting = isMultiKey && numFfiKeyColumns < dataGroupByCols.length

      if (needsPipeKeySplitting) {
        U.assembleGroupByBatchWithCompoundKeys(ffiBatch, columnNames, dataGroupByCols.length, aggExprs, dataGroupByCols, schema, columnTypes)
      } else {
        U.assembleGroupByBatch(ffiBatch, numFfiKeyColumns, columnNames, aggExprs, dataGroupByCols, schema, columnTypes)
      }
    } finally {
      ffiBatch.close()
    }
  }

  private def executeBucketAggregation(
    cacheManager: io.indextables.tantivy4java.split.SplitCacheManager,
    searchers: java.util.List[io.indextables.tantivy4java.split.SplitSearcher],
    queryAstJson: String,
    bucketConfig: BucketAggregationConfig
  ): ColumnarBatch = {
    val aggName = "bucket_agg"
    val additionalGroupByColumns = partition.groupByColumns.drop(1)

    val bucketAgg = bucketConfig match {
      case dhc: DateHistogramConfig =>
        val agg = new DateHistogramAggregation(aggName, dhc.fieldName)
        agg.setFixedInterval(dhc.interval)
        dhc.offset.foreach(o => agg.setOffset(o))
        if (dhc.minDocCount > 0) agg.setMinDocCount(dhc.minDocCount)
        dhc.hardBoundsMin.foreach(min => dhc.hardBoundsMax.foreach(max => agg.setHardBounds(min, max)))
        dhc.extendedBoundsMin.foreach(min => dhc.extendedBoundsMax.foreach(max => agg.setExtendedBounds(min, max)))
        if (additionalGroupByColumns.nonEmpty) {
          agg.addSubAggregation(U.buildNestedTermsAggregation(additionalGroupByColumns.toList, 0, aggExprs))
        } else {
          U.addBucketSubAggregations(agg, aggExprs)
        }
        agg.asInstanceOf[io.indextables.tantivy4java.split.SplitAggregation]

      case hc: HistogramConfig =>
        val agg = new HistogramAggregation(aggName, hc.fieldName, hc.interval)
        if (hc.offset != 0.0) agg.setOffset(hc.offset)
        if (hc.minDocCount > 0) agg.setMinDocCount(hc.minDocCount)
        hc.hardBoundsMin.foreach(min => hc.hardBoundsMax.foreach(max => agg.setHardBounds(min, max)))
        hc.extendedBoundsMin.foreach(min => hc.extendedBoundsMax.foreach(max => agg.setExtendedBounds(min, max)))
        if (additionalGroupByColumns.nonEmpty) {
          agg.addSubAggregation(U.buildNestedTermsAggregation(additionalGroupByColumns.toList, 0, aggExprs))
        } else {
          U.addBucketSubAggregations(agg, aggExprs)
        }
        agg.asInstanceOf[io.indextables.tantivy4java.split.SplitAggregation]

      case rc: RangeConfig =>
        val agg = new RangeAggregation(aggName, rc.fieldName)
        rc.ranges.foreach { range =>
          val from: java.lang.Double = range.from.map(d => java.lang.Double.valueOf(d)).orNull
          val to: java.lang.Double   = range.to.map(d => java.lang.Double.valueOf(d)).orNull
          agg.addRange(range.key, from, to)
        }
        if (additionalGroupByColumns.nonEmpty) {
          agg.addSubAggregation(U.buildNestedTermsAggregation(additionalGroupByColumns.toList, 0, aggExprs))
        } else {
          U.addBucketSubAggregations(agg, aggExprs)
        }
        agg.asInstanceOf[io.indextables.tantivy4java.split.SplitAggregation]
    }

    val aggJson = helper.buildAggJson(aggName, bucketAgg)

    val firstSearcher = searchers.get(0)
    val (_, columnNames, columnTypes, _) = helper.querySchema(firstSearcher, queryAstJson, aggName, aggJson)

    // Native multi-split merge via merge_fruits
    val ffiBatch = helper.executeMultiSplit(cacheManager, searchers, queryAstJson, aggName, aggJson)

    try {
      val dataGroupByCols = partition.groupByColumns.filterNot(partition.partitionColumns.contains)
      U.assembleBucketBatch(ffiBatch, columnNames, aggExprs, dataGroupByCols, schema, columnTypes)
    } finally {
      ffiBatch.close()
    }
  }
}
