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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.SparkSession

import io.indextables.spark.arrow.AggregationArrowFfiHelper
import io.indextables.spark.filters.MixedBooleanFilter
import io.indextables.spark.search.SplitSearchEngine
import io.indextables.spark.storage.GlobalSplitCacheManager
import io.indextables.spark.transaction.EnhancedTransactionLogCache
import io.indextables.tantivy4java.split.SplitMatchAllQuery
import org.slf4j.LoggerFactory

/**
 * Columnar partition reader for simple (no GROUP BY) aggregations using Arrow FFI.
 *
 * Executes all aggregation expressions via `SplitSearcher.aggregateArrowFfi()` and returns results as a ColumnarBatch.
 * Single-batch model: next() returns true once, get() returns the assembled ColumnarBatch, close() frees resources.
 */
class SimpleAggregateColumnarReader(
  partition: IndexTables4SparkSimpleAggregatePartition,
  sparkSession: SparkSession)
    extends PartitionReader[ColumnarBatch] {

  private val logger               = LoggerFactory.getLogger(classOf[SimpleAggregateColumnarReader])
  private val helper               = new AggregationArrowFfiHelper()
  private var batch: ColumnarBatch = _
  private var consumed             = false
  private var initialized          = false

  override def next(): Boolean = {
    if (consumed) return false
    if (!initialized) {
      initialize()
      initialized = true
    }
    consumed = true
    batch != null
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = {
    if (batch != null) batch.close()
    helper.close()

    // Report bytesRead to Spark UI
    val bytesRead = partition.split.size
    org.apache.spark.sql.indextables.OutputMetricsUpdater.incInputMetrics(bytesRead, 0)
  }

  private def initialize(): Unit = {
    logger.debug(s"SimpleAggregateColumnarReader: initializing for split ${partition.split.path}")

    try {
      // Create search engine (same pattern as InternalRow reader)
      val cacheConfig = io.indextables.spark.util.ConfigUtils.createSplitCacheConfig(
        partition.config,
        Some(partition.tablePath.toString)
      )

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

      // Build query from filters (same logic as InternalRow reader)
      val queryAstJson = buildQueryAstJson(splitSearchEngine)

      // Build aggregations and execute via single-pass multi-agg FFI
      // Uses multiSplitMultiAggregateArrowFfi with single searcher — searches once, exports all aggs
      val aggregations = buildAggregations()
      val cacheManager = GlobalSplitCacheManager.getInstance(cacheConfig)
      val searchers    = new java.util.ArrayList[io.indextables.tantivy4java.split.SplitSearcher]()
      searchers.add(searcher)

      val ffiBatches = helper.executeMultiSplitMultiAgg(cacheManager, searchers, queryAstJson, aggregations)

      try {
        // Check for zero-count (empty split)
        if (hasZeroCount(ffiBatches)) {
          logger.debug(s"SimpleAggregateColumnarReader: no matching docs in split ${partition.split.path}")
          batch = null
          return
        }

        // Assemble combined batch matching readSchema
        val readSchema = createSimpleAggregateReadSchema()
        val arrowTypes = deriveArrowTypes()
        batch = helper.combineSingleRowBatches(ffiBatches, readSchema, arrowTypes)
        logger.debug(
          s"SimpleAggregateColumnarReader: assembled batch with ${readSchema.fields.length} columns (single-pass)"
        )

      } finally
        ffiBatches.foreach(b =>
          try b.close()
          catch { case _: Exception => }
        )

    } catch {
      case e: IllegalArgumentException                                 => throw e
      case e: io.indextables.spark.exceptions.IndexQueryParseException => throw e
      case e: Exception =>
        throw new RuntimeException(s"SimpleAggregateColumnarReader: failed for split ${partition.split.path}", e)
    }
  }

  private def deriveArrowTypes(): Array[String] =
    partition.aggregation.aggregateExpressions.map {
      case _: Count | _: CountStar  => "Int64"
      case _: Sum | _: Min | _: Max => "Float64"
      case _                        => ""
    }

  private def buildQueryAstJson(splitSearchEngine: SplitSearchEngine): String = {
    // Strip partition filters
    val nonPartitionPushedFilters = if (partition.partitionColumns.nonEmpty && partition.pushedFilters.nonEmpty) {
      MixedBooleanFilter.stripPartitionOnlyFilters(partition.pushedFilters, partition.partitionColumns)
    } else {
      partition.pushedFilters
    }

    val cleanedIndexQueryFilters = if (partition.partitionColumns.nonEmpty && partition.indexQueryFilters.nonEmpty) {
      MixedBooleanFilter.stripPartitionFiltersFromArray(partition.indexQueryFilters, partition.partitionColumns)
    } else {
      partition.indexQueryFilters
    }

    val allFilters = nonPartitionPushedFilters ++ cleanedIndexQueryFilters

    val splitQuery = if (allFilters.nonEmpty) {
      val splitFieldNames = {
        val schema = splitSearchEngine.getSchema()
        try Some(schema.getFieldNames().asScala.toSet)
        catch { case _: Exception => None }
        finally schema.close()
      }
      val optionsFromConfig = new org.apache.spark.sql.util.CaseInsensitiveStringMap(partition.config.asJava)
      FiltersToQueryConverter.convertToSplitQuery(allFilters, splitSearchEngine, splitFieldNames, Some(optionsFromConfig))
    } else {
      new SplitMatchAllQuery()
    }

    splitQuery.toQueryAstJson()
  }

  private def buildAggregations(): Array[(String, io.indextables.tantivy4java.split.SplitAggregation)] = {
    val result = new ArrayBuffer[(String, io.indextables.tantivy4java.split.SplitAggregation)]()

    partition.aggregation.aggregateExpressions.zipWithIndex.foreach {
      case (aggExpr, index) =>
        aggExpr match {
          case count: Count =>
            val fieldName = extractFieldName(count.column)
            val aggName   = s"count_agg_$index"
            result += ((aggName, new io.indextables.tantivy4java.aggregation.CountAggregation(aggName, fieldName)))

          case _: CountStar =>
            val selectedField = selectCountStarField()
            val aggName       = s"count_star_agg_$index"
            result += ((aggName, new io.indextables.tantivy4java.aggregation.CountAggregation(aggName, selectedField)))

          case sum: Sum =>
            val fieldName = extractFieldName(sum.column)
            val aggName   = s"sum_agg_$index"
            result += ((aggName, new io.indextables.tantivy4java.aggregation.SumAggregation(fieldName)))

          case avg: Avg =>
            val fieldName = extractFieldName(avg.column)
            throw new IllegalStateException(
              s"AVG aggregation for field '$fieldName' should have been transformed by Spark into SUM + COUNT."
            )

          case min: Min =>
            val fieldName = extractFieldName(min.column)
            val aggName   = s"min_agg_$index"
            result += ((aggName, new io.indextables.tantivy4java.aggregation.MinAggregation(fieldName)))

          case max: Max =>
            val fieldName = extractFieldName(max.column)
            val aggName   = s"max_agg_$index"
            result += ((aggName, new io.indextables.tantivy4java.aggregation.MaxAggregation(fieldName)))

          case other =>
            logger.warn(s"Unsupported aggregation type: ${other.getClass.getSimpleName}")
        }
    }

    result.toArray
  }

  private def hasZeroCount(ffiBatches: Array[ColumnarBatch]): Boolean =
    // Find COUNT or COUNT(*) batch and check if value is 0
    partition.aggregation.aggregateExpressions.zipWithIndex
      .collectFirst {
        case (_: Count, idx) if idx < ffiBatches.length     => ffiBatches(idx)
        case (_: CountStar, idx) if idx < ffiBatches.length => ffiBatches(idx)
      }
      .exists(countBatch => countBatch.numRows() > 0 && countBatch.column(0).getLong(0) == 0L)

  private def createSimpleAggregateReadSchema(): StructType = {
    val fields = partition.aggregation.aggregateExpressions.zipWithIndex.map {
      case (aggExpr, _) =>
        aggExpr match {
          case _: Count     => StructField("count", LongType, nullable = true)
          case _: CountStar => StructField("count(*)", LongType, nullable = true)
          case sum: Sum =>
            val fieldType = getInputFieldType(sum)
            val sumType = fieldType match {
              case IntegerType | LongType => LongType
              case _                      => DoubleType
            }
            StructField("sum", sumType, nullable = true)
          case _: Avg => StructField("avg", DoubleType, nullable = true)
          case min: Min =>
            StructField("min", getInputFieldType(min), nullable = true)
          case max: Max =>
            StructField("max", getInputFieldType(max), nullable = true)
          case _ => StructField("unknown", LongType, nullable = true)
        }
    }
    StructType(fields)
  }

  private def getInputFieldType(aggExpr: AggregateFunc): DataType = {
    val column    = aggExpr.children().headOption.getOrElse(return LongType)
    val fieldName = extractFieldName(column)
    partition.schema.fields.find(_.name == fieldName).map(_.dataType).getOrElse(LongType)
  }

  private def extractFieldName(column: org.apache.spark.sql.connector.expressions.Expression): String =
    if (column.getClass.getSimpleName == "FieldReference") column.toString
    else io.indextables.spark.util.ExpressionUtils.extractFieldName(column)

  private def selectCountStarField(): String = {
    val numericTypes: Set[DataType] =
      Set(IntegerType, LongType, FloatType, DoubleType, DateType, TimestampType, BooleanType)

    // Prefer companion tracking fields
    val fastFields = EnhancedTransactionLogCache.getDocMappingMetadata(partition.split).fastFields
    if (fastFields.contains("__pq_file_hash")) return "__pq_file_hash"
    if (fastFields.contains("__pq_row_in_file")) return "__pq_row_in_file"

    // Try field from other aggregations
    val fieldFromAgg = partition.aggregation.aggregateExpressions.collectFirst {
      case sum: Sum     => Some(extractFieldName(sum.column))
      case min: Min     => Some(extractFieldName(min.column))
      case max: Max     => Some(extractFieldName(max.column))
      case count: Count => Some(extractFieldName(count.column))
    }.flatten

    fieldFromAgg.getOrElse {
      if (fastFields.nonEmpty) {
        val numericFastField = fastFields.find { fieldName =>
          partition.schema.fields.find(_.name == fieldName).exists(f => numericTypes.contains(f.dataType))
        }
        numericFastField.getOrElse(fastFields.head)
      } else {
        partition.schema.fields
          .find(f => numericTypes.contains(f.dataType))
          .orElse(partition.schema.fields.find(_.dataType == StringType))
          .map(_.name)
          .getOrElse(
            throw new IllegalArgumentException(
              "COUNT(*) aggregation requires at least one fast field. " +
                "Please configure a fast field using spark.indextables.indexing.fastfields."
            )
          )
      }
    }
  }
}

/**
 * Columnar partition reader for multi-split simple aggregations using Arrow FFI.
 *
 * Uses native `SplitCacheManager.multiSplitAggregateArrowFfi()` which searches each split, merges intermediate results
 * via native `merge_fruits()`, and exports merged Arrow data.
 */
class MultiSplitSimpleAggregateColumnarReader(
  partition: IndexTables4SparkMultiSplitSimpleAggregatePartition,
  sparkSession: SparkSession)
    extends PartitionReader[ColumnarBatch] {

  private val logger               = LoggerFactory.getLogger(classOf[MultiSplitSimpleAggregateColumnarReader])
  private val helper               = new AggregationArrowFfiHelper()
  private var batch: ColumnarBatch = _
  private var consumed             = false
  private var initialized          = false

  override def next(): Boolean = {
    if (consumed) return false
    if (!initialized) {
      initialize()
      initialized = true
    }
    consumed = true
    batch != null
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = {
    if (batch != null) batch.close()
    helper.close()

    val totalBytesRead = partition.splits.map(_.size).sum
    org.apache.spark.sql.indextables.OutputMetricsUpdater.incInputMetrics(totalBytesRead, 0)
  }

  private def initialize(): Unit = {
    logger.debug(s"MultiSplitSimpleAggregateColumnarReader: initializing with ${partition.splits.length} splits")

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
          split.path,
          partition.tablePath.toString
        )
        val splitPath = io.indextables.spark.io.CloudStorageProviderFactory.normalizePathForTantivy(
          resolvedPath,
          partition.config
        )
        val splitMetadata = io.indextables.spark.util.SplitMetadataFactory.fromAddAction(
          split,
          partition.tablePath.toString
        )
        val options = Some(IndexTables4SparkOptions(partition.config))
        val engine = SplitSearchEngine.fromSplitFileWithMetadata(
          partition.schema,
          splitPath,
          splitMetadata,
          cacheConfig,
          options
        )
        searchers.add(engine.getSplitSearcher())
      }

      // Build query from filters using first split's engine
      val firstSplit = partition.splits.head
      val firstResolvedPath = PathResolutionUtils.resolveSplitPathAsString(
        firstSplit.path,
        partition.tablePath.toString
      )
      val firstSplitPath = io.indextables.spark.io.CloudStorageProviderFactory.normalizePathForTantivy(
        firstResolvedPath,
        partition.config
      )
      val firstMetadata = io.indextables.spark.util.SplitMetadataFactory.fromAddAction(
        firstSplit,
        partition.tablePath.toString
      )
      val firstEngine = SplitSearchEngine.fromSplitFileWithMetadata(
        partition.schema,
        firstSplitPath,
        firstMetadata,
        cacheConfig,
        Some(IndexTables4SparkOptions(partition.config))
      )
      val queryAstJson = buildQueryAstJson(firstEngine)

      // Build aggregations and execute via native multi-split single-pass FFI (FR-C)
      val aggregations = buildAggregations()
      val ffiBatches   = helper.executeMultiSplitMultiAgg(cacheManager, searchers, queryAstJson, aggregations)

      try {
        // Check for zero-count
        if (hasZeroCount(ffiBatches)) {
          logger.debug(
            s"MultiSplitSimpleAggregateColumnarReader: no matching docs across ${partition.splits.length} splits"
          )
          batch = null
          return
        }

        // Assemble combined batch matching readSchema
        val readSchema = createSimpleAggregateReadSchema()
        val arrowTypes = deriveArrowTypes()
        batch = helper.combineSingleRowBatches(ffiBatches, readSchema, arrowTypes)
        logger.debug(s"MultiSplitSimpleAggregateColumnarReader: assembled batch with ${readSchema.fields.length} columns from ${partition.splits.length} splits (single-pass)")

      } finally
        ffiBatches.foreach(b =>
          try b.close()
          catch { case _: Exception => }
        )

    } catch {
      case e: IllegalArgumentException                                 => throw e
      case e: io.indextables.spark.exceptions.IndexQueryParseException => throw e
      case e: Exception =>
        throw new RuntimeException(
          s"MultiSplitSimpleAggregateColumnarReader: failed for ${partition.splits.length} splits",
          e
        )
    }
  }

  private def deriveArrowTypes(): Array[String] =
    partition.aggregation.aggregateExpressions.map {
      case _: Count | _: CountStar  => "Int64"
      case _: Sum | _: Min | _: Max => "Float64"
      case _                        => ""
    }

  private def buildQueryAstJson(splitSearchEngine: SplitSearchEngine): String = {
    val nonPartitionPushedFilters = if (partition.partitionColumns.nonEmpty && partition.pushedFilters.nonEmpty) {
      MixedBooleanFilter.stripPartitionOnlyFilters(partition.pushedFilters, partition.partitionColumns)
    } else {
      partition.pushedFilters
    }

    val cleanedIndexQueryFilters = if (partition.partitionColumns.nonEmpty && partition.indexQueryFilters.nonEmpty) {
      MixedBooleanFilter.stripPartitionFiltersFromArray(partition.indexQueryFilters, partition.partitionColumns)
    } else {
      partition.indexQueryFilters
    }

    val allFilters = nonPartitionPushedFilters ++ cleanedIndexQueryFilters

    val splitQuery = if (allFilters.nonEmpty) {
      val splitFieldNames = {
        val schema = splitSearchEngine.getSchema()
        try Some(schema.getFieldNames().asScala.toSet)
        catch { case _: Exception => None }
        finally schema.close()
      }
      val optionsFromConfig = new org.apache.spark.sql.util.CaseInsensitiveStringMap(partition.config.asJava)
      FiltersToQueryConverter.convertToSplitQuery(allFilters, splitSearchEngine, splitFieldNames, Some(optionsFromConfig))
    } else {
      new SplitMatchAllQuery()
    }

    splitQuery.toQueryAstJson()
  }

  private def buildAggregations(): Array[(String, io.indextables.tantivy4java.split.SplitAggregation)] = {
    val result = new ArrayBuffer[(String, io.indextables.tantivy4java.split.SplitAggregation)]()

    partition.aggregation.aggregateExpressions.zipWithIndex.foreach {
      case (aggExpr, index) =>
        aggExpr match {
          case count: Count =>
            val fieldName = extractFieldName(count.column)
            val aggName   = s"count_agg_$index"
            result += ((aggName, new io.indextables.tantivy4java.aggregation.CountAggregation(aggName, fieldName)))

          case _: CountStar =>
            val selectedField = selectCountStarField()
            val aggName       = s"count_star_agg_$index"
            result += ((aggName, new io.indextables.tantivy4java.aggregation.CountAggregation(aggName, selectedField)))

          case sum: Sum =>
            val fieldName = extractFieldName(sum.column)
            val aggName   = s"sum_agg_$index"
            result += ((aggName, new io.indextables.tantivy4java.aggregation.SumAggregation(fieldName)))

          case min: Min =>
            val fieldName = extractFieldName(min.column)
            val aggName   = s"min_agg_$index"
            result += ((aggName, new io.indextables.tantivy4java.aggregation.MinAggregation(fieldName)))

          case max: Max =>
            val fieldName = extractFieldName(max.column)
            val aggName   = s"max_agg_$index"
            result += ((aggName, new io.indextables.tantivy4java.aggregation.MaxAggregation(fieldName)))

          case other =>
            logger.warn(s"Unsupported aggregation type: ${other.getClass.getSimpleName}")
        }
    }

    result.toArray
  }

  private def hasZeroCount(ffiBatches: Array[ColumnarBatch]): Boolean =
    partition.aggregation.aggregateExpressions.zipWithIndex
      .collectFirst {
        case (_: Count, idx) if idx < ffiBatches.length     => ffiBatches(idx)
        case (_: CountStar, idx) if idx < ffiBatches.length => ffiBatches(idx)
      }
      .exists(countBatch => countBatch.numRows() > 0 && countBatch.column(0).getLong(0) == 0L)

  private def createSimpleAggregateReadSchema(): StructType = {
    val fields = partition.aggregation.aggregateExpressions.zipWithIndex.map {
      case (aggExpr, _) =>
        aggExpr match {
          case _: Count     => StructField("count", LongType, nullable = true)
          case _: CountStar => StructField("count(*)", LongType, nullable = true)
          case sum: Sum =>
            val fieldType = getInputFieldType(sum)
            val sumType = fieldType match {
              case IntegerType | LongType => LongType
              case _                      => DoubleType
            }
            StructField("sum", sumType, nullable = true)
          case _: Avg => StructField("avg", DoubleType, nullable = true)
          case min: Min =>
            StructField("min", getInputFieldType(min), nullable = true)
          case max: Max =>
            StructField("max", getInputFieldType(max), nullable = true)
          case _ => StructField("unknown", LongType, nullable = true)
        }
    }
    StructType(fields)
  }

  private def getInputFieldType(aggExpr: AggregateFunc): DataType = {
    val column    = aggExpr.children().headOption.getOrElse(return LongType)
    val fieldName = extractFieldName(column)
    partition.schema.fields.find(_.name == fieldName).map(_.dataType).getOrElse(LongType)
  }

  private def extractFieldName(column: org.apache.spark.sql.connector.expressions.Expression): String =
    if (column.getClass.getSimpleName == "FieldReference") column.toString
    else io.indextables.spark.util.ExpressionUtils.extractFieldName(column)

  private def selectCountStarField(): String = {
    val numericTypes: Set[DataType] =
      Set(IntegerType, LongType, FloatType, DoubleType, DateType, TimestampType, BooleanType)

    val fastFields = EnhancedTransactionLogCache.getDocMappingMetadata(partition.splits.head).fastFields
    if (fastFields.contains("__pq_file_hash")) return "__pq_file_hash"
    if (fastFields.contains("__pq_row_in_file")) return "__pq_row_in_file"

    val fieldFromAgg = partition.aggregation.aggregateExpressions.collectFirst {
      case sum: Sum     => Some(extractFieldName(sum.column))
      case min: Min     => Some(extractFieldName(min.column))
      case max: Max     => Some(extractFieldName(max.column))
      case count: Count => Some(extractFieldName(count.column))
    }.flatten

    fieldFromAgg.getOrElse {
      if (fastFields.nonEmpty) {
        val numericFastField = fastFields.find { fieldName =>
          partition.schema.fields.find(_.name == fieldName).exists(f => numericTypes.contains(f.dataType))
        }
        numericFastField.getOrElse(fastFields.head)
      } else {
        partition.schema.fields
          .find(f => numericTypes.contains(f.dataType))
          .orElse(partition.schema.fields.find(_.dataType == StringType))
          .map(_.name)
          .getOrElse(
            throw new IllegalArgumentException(
              "COUNT(*) aggregation requires at least one fast field. " +
                "Please configure a fast field using spark.indextables.indexing.fastfields."
            )
          )
      }
    }
  }
}
