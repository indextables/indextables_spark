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

package io.indextables.spark.arrow

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import io.indextables.tantivy4java.split.{SplitAggregation, SplitCacheManager, SplitSearcher}
import org.slf4j.LoggerFactory

/**
 * Shared utility encapsulating the Arrow FFI aggregation protocol.
 *
 * Manages FFI struct allocation, tantivy4java FFI calls, and Arrow import. Methods return ColumnarBatch (zero-copy Arrow
 * data) with no InternalRow conversion.
 *
 * Requires tantivy4java 0.31.2+ which provides:
 *   - `SplitSearcher.getAggregationArrowSchema()` — schema introspection
 *   - `SplitSearcher.aggregateArrowFfi()` — single-split Arrow export
 *   - `SplitCacheManager.multiSplitAggregateArrowFfi()` — multi-split merge + Arrow export
 */
class AggregationArrowFfiHelper extends AutoCloseable {

  private val logger = LoggerFactory.getLogger(classOf[AggregationArrowFfiHelper])
  private val bridge = new ArrowFfiBridge()

  /**
   * Build aggregation JSON wrapping a single named aggregation.
   *
   * @param aggName
   *   Name for the aggregation (e.g., "count_star_agg_0")
   * @param aggregation
   *   The SplitAggregation to serialize
   * @return
   *   JSON string: {"aggName": <aggregation JSON>}
   */
  def buildAggJson(aggName: String, aggregation: SplitAggregation): String =
    s"""{"$aggName": ${aggregation.toAggregationJson()}}"""

  /**
   * Build aggregation JSON wrapping multiple named aggregations.
   *
   * @param aggregations
   *   Map of aggName -> SplitAggregation
   * @return
   *   JSON string: {"name1": <json1>, "name2": <json2>, ...}
   */
  def buildMultiAggJson(aggregations: Seq[(String, SplitAggregation)]): String = {
    val entries = aggregations.map {
      case (name, agg) => s""""$name": ${agg.toAggregationJson()}"""
    }
    s"{${entries.mkString(", ")}}"
  }

  /**
   * Query the Arrow schema that would be returned for a given aggregation.
   *
   * @return
   *   (numCols, columnNames, rowCount)
   */
  def querySchema(
    searcher: SplitSearcher,
    queryAstJson: String,
    aggName: String,
    aggJson: String
  ): (Int, Array[String], Int) = {
    val schemaJson = searcher.getAggregationArrowSchema(queryAstJson, aggName, aggJson)
    parseSchemaJson(schemaJson)
  }

  /**
   * Execute single-split aggregation via Arrow FFI.
   *
   * @return
   *   ColumnarBatch with aggregation results (caller must close)
   */
  def executeSingleSplit(
    searcher: SplitSearcher,
    queryAstJson: String,
    aggName: String,
    aggJson: String
  ): ColumnarBatch = {
    // Query schema to know how many columns to allocate
    val (numCols, columnNames, _) = querySchema(searcher, queryAstJson, aggName, aggJson)
    logger.debug(s"Aggregation FFI schema: $numCols columns: ${columnNames.mkString(", ")}")

    // Allocate FFI structs
    val (arrays, schemas, arrayAddrs, schemaAddrs) = bridge.allocateStructs(numCols)

    // Execute FFI call — fills arrays/schemas with Arrow data
    val numRows = searcher.aggregateArrowFfi(queryAstJson, aggName, aggJson, arrayAddrs, schemaAddrs)
    logger.debug(s"Aggregation FFI returned $numRows rows")

    // Import into Spark ColumnarBatch
    bridge.importAsColumnarBatch(arrays, schemas, numRows)
  }

  /**
   * Execute multi-split aggregation via Arrow FFI with native merge.
   *
   * Uses `SplitCacheManager.multiSplitAggregateArrowFfi()` which:
   *   1. Searches each split with limit=0 (aggregation-only)
   *   2. Merges intermediate results via native `merge_fruits()`
   *   3. Exports merged result as Arrow columnar data
   *
   * This eliminates O(N * buckets) JNI crossings, replacing them with a single call.
   *
   * @return
   *   ColumnarBatch with merged aggregation results (caller must close)
   */
  def executeMultiSplit(
    cacheManager: SplitCacheManager,
    searchers: java.util.List[SplitSearcher],
    queryAstJson: String,
    aggName: String,
    aggJson: String
  ): ColumnarBatch = {
    // Query schema from first searcher
    val firstSearcher = searchers.get(0)
    val (numCols, columnNames, _) = querySchema(firstSearcher, queryAstJson, aggName, aggJson)
    logger.debug(s"Multi-split aggregation FFI schema: $numCols columns: ${columnNames.mkString(", ")}, ${searchers.size()} splits")

    // Allocate FFI structs
    val (arrays, schemas, arrayAddrs, schemaAddrs) = bridge.allocateStructs(numCols)

    // Execute multi-split FFI call — native merge_fruits merges across all splits
    val numRows = cacheManager.multiSplitAggregateArrowFfi(
      searchers, queryAstJson, aggName, aggJson, arrayAddrs, schemaAddrs
    )
    logger.debug(s"Multi-split aggregation FFI returned $numRows rows from ${searchers.size()} splits")

    // Import into Spark ColumnarBatch
    bridge.importAsColumnarBatch(arrays, schemas, numRows)
  }

  /**
   * Execute multi-split, multi-aggregation single-pass via Arrow FFI (FR-C).
   *
   * Searches each split ONCE with all aggregations combined, merges intermediate results
   * in native code, and exports each named aggregation as a separate Arrow RecordBatch.
   *
   * For 3 aggregations across 100 splits: 100 searches instead of 300.
   *
   * @param aggregations
   *   Array of (aggName, SplitAggregation) pairs
   * @return
   *   Array of ColumnarBatch, one per aggregation (caller must close each)
   */
  def executeMultiSplitMultiAgg(
    cacheManager: SplitCacheManager,
    searchers: java.util.List[SplitSearcher],
    queryAstJson: String,
    aggregations: Array[(String, io.indextables.tantivy4java.split.SplitAggregation)]
  ): Array[ColumnarBatch] = {
    // Query schema for each aggregation from the first searcher to determine column counts
    val firstSearcher = searchers.get(0)
    val aggInfos = aggregations.map { case (aggName, agg) =>
      val aggJson = buildAggJson(aggName, agg)
      val (numCols, columnNames, _) = querySchema(firstSearcher, queryAstJson, aggName, aggJson)
      (aggName, numCols, columnNames)
    }

    val totalCols = aggInfos.map(_._2).sum
    val colCounts = aggInfos.map(_._2)

    logger.debug(s"Multi-split multi-agg FFI: ${aggregations.length} aggs, $totalCols total columns, ${searchers.size()} splits")

    // Allocate FFI structs for all columns
    val (arrays, schemas, arrayAddrs, schemaAddrs) = bridge.allocateStructs(totalCols)

    // Build combined aggregation JSON (preserves ordering)
    val combinedAggJson = buildMultiAggJson(aggregations.toSeq)

    // Convert aggNames to Java List
    val aggNames = new java.util.ArrayList[String]()
    aggregations.foreach { case (name, _) => aggNames.add(name) }

    // Execute single-pass multi-agg FFI
    val resultJson = cacheManager.multiSplitMultiAggregateArrowFfi(
      searchers, queryAstJson, aggNames, combinedAggJson, colCounts, arrayAddrs, schemaAddrs
    )
    logger.debug(s"Multi-split multi-agg FFI result: $resultJson")

    // Parse row counts per aggregation from result JSON: {"agg_name":rowCount,...}
    val rowCountPattern = """"([^"]+)"\s*:\s*(\d+)""".r
    val rowCountMap = rowCountPattern.findAllMatchIn(resultJson).map(m => m.group(1) -> m.group(2).toInt).toMap

    // Import each aggregation's columns as a separate ColumnarBatch
    var colOffset = 0
    aggregations.zip(aggInfos).map { case ((aggName, _), (_, numCols, _)) =>
      val aggArrays  = arrays.slice(colOffset, colOffset + numCols)
      val aggSchemas = schemas.slice(colOffset, colOffset + numCols)
      val numRows    = rowCountMap.getOrElse(aggName, 0)
      colOffset += numCols
      bridge.importAsColumnarBatch(aggArrays, aggSchemas, numRows)
    }
  }

  /**
   * Assemble a final ColumnarBatch matching the target Spark schema from FFI results.
   *
   * Handles type mismatches: tantivy4java FFI returns Float64 for all metrics and Int64 for counts, but Spark may
   * expect IntegerType/LongType/FloatType. Creates OnHeapColumnVector copies with type casting when needed, and
   * reorders columns to match targetSchema field order.
   *
   * @param ffiBatch
   *   The raw FFI ColumnarBatch (will NOT be closed by this method)
   * @param targetSchema
   *   The Spark schema to conform to
   * @param columnMapping
   *   Maps target schema field index -> FFI batch column index
   * @return
   *   New ColumnarBatch matching targetSchema (caller must close both batches)
   */
  def assembleAggregateColumnarBatch(
    ffiBatch: ColumnarBatch,
    targetSchema: StructType,
    columnMapping: Array[Int]
  ): ColumnarBatch = {
    val numRows = ffiBatch.numRows()
    val vectors = new Array[ColumnVector](targetSchema.fields.length)

    targetSchema.fields.zipWithIndex.foreach {
      case (targetField, targetIdx) =>
        val sourceIdx    = columnMapping(targetIdx)
        val sourceColumn = ffiBatch.column(sourceIdx)
        val targetType   = targetField.dataType

        // Check if type conversion is needed
        if (needsTypeCast(sourceColumn, targetType)) {
          vectors(targetIdx) = castColumn(sourceColumn, targetType, numRows)
        } else {
          vectors(targetIdx) = sourceColumn
        }
    }

    new ColumnarBatch(vectors, numRows)
  }

  /**
   * Create a single-row ColumnarBatch from multiple single-column FFI batches.
   *
   * Used for simple aggregations where each metric is a separate FFI call returning a 1-row, 1-column batch.
   *
   * @param ffiBatches
   *   Array of 1-row ColumnarBatch results (one per aggregation expression)
   * @param targetSchema
   *   The Spark schema for the combined result
   * @return
   *   Combined 1-row ColumnarBatch (caller must close)
   */
  def combineSingleRowBatches(
    ffiBatches: Array[ColumnarBatch],
    targetSchema: StructType
  ): ColumnarBatch = {
    require(ffiBatches.length == targetSchema.fields.length,
      s"Expected ${targetSchema.fields.length} batches but got ${ffiBatches.length}")

    val vectors = new Array[ColumnVector](targetSchema.fields.length)
    targetSchema.fields.zipWithIndex.foreach {
      case (targetField, idx) =>
        val sourceColumn = ffiBatches(idx).column(0) // Each batch has 1 column
        val targetType   = targetField.dataType

        if (needsTypeCast(sourceColumn, targetType)) {
          vectors(idx) = castColumn(sourceColumn, targetType, 1)
        } else {
          vectors(idx) = sourceColumn
        }
    }

    new ColumnarBatch(vectors, 1)
  }

  override def close(): Unit =
    bridge.close()

  // --- Private helpers ---

  private[arrow] def parseSchemaJson(json: String): (Int, Array[String], Int) = {
    // Parse JSON response from tantivy4java 0.31.2+:
    // {"columns":[{"name":"key","type":"Utf8"},{"name":"doc_count","type":"Int64"}],"row_count":2}
    // Simple regex parsing without external dependency
    val rowCountPattern = """"row_count"\s*:\s*(\d+)""".r
    val namePattern     = """"name"\s*:\s*"([^"]+)"""".r

    val rowCount    = rowCountPattern.findFirstMatchIn(json).map(_.group(1).toInt).getOrElse(0)
    val columnNames = namePattern.findAllMatchIn(json).map(_.group(1)).toArray
    val numCols     = columnNames.length

    (numCols, columnNames, rowCount)
  }

  private def needsTypeCast(sourceColumn: ColumnVector, targetType: DataType): Boolean =
    // Always cast FFI Arrow columns to ensure type safety. FFI returns Float64 for all metrics
    // (including SUM/MIN/MAX of integer fields) and Int64 for counts. Since we can't inspect
    // the underlying Arrow vector type from ColumnVector, always create a typed OnHeapColumnVector.
    // For 1-row aggregates and small GROUP BY results, the copy overhead is negligible.
    true

  private def castColumn(sourceColumn: ColumnVector, targetType: DataType, numRows: Int): ColumnVector = {
    val onHeap = new OnHeapColumnVector(numRows, targetType)
    (0 until numRows).foreach { row =>
      if (sourceColumn.isNullAt(row)) {
        onHeap.putNull(row)
      } else {
        targetType match {
          case IntegerType =>
            // Source is Float64 (metric) or Int64 (count) -> Int
            val value = try sourceColumn.getLong(row).toInt
            catch { case _: Exception => Math.round(sourceColumn.getDouble(row)).toInt }
            onHeap.putInt(row, value)

          case LongType =>
            // Source may be Int64 (count) or Float64 (SUM of integers)
            val value = try sourceColumn.getLong(row)
            catch { case _: Exception => Math.round(sourceColumn.getDouble(row)) }
            onHeap.putLong(row, value)

          case FloatType =>
            val value = try sourceColumn.getDouble(row).toFloat
            catch { case _: Exception => sourceColumn.getFloat(row) }
            onHeap.putFloat(row, value)

          case DoubleType =>
            // Source may be Float64 or Int64
            val value = try sourceColumn.getDouble(row)
            catch { case _: Exception => sourceColumn.getLong(row).toDouble }
            onHeap.putDouble(row, value)

          case StringType =>
            val utf8 = sourceColumn.getUTF8String(row)
            if (utf8 != null) {
              val bytes = utf8.getBytes
              onHeap.putByteArray(row, bytes, 0, bytes.length)
            } else {
              onHeap.putNull(row)
            }

          case TimestampType =>
            // FFI returns microseconds as Int64
            val value = try sourceColumn.getLong(row)
            catch { case _: Exception => sourceColumn.getDouble(row).toLong }
            onHeap.putLong(row, value)

          case DateType =>
            // FFI returns days as Int32
            val value = try sourceColumn.getInt(row)
            catch { case _: Exception => sourceColumn.getLong(row).toInt }
            onHeap.putInt(row, value)

          case BooleanType =>
            val value = try sourceColumn.getLong(row) != 0
            catch { case _: Exception => sourceColumn.getDouble(row) != 0.0 }
            onHeap.putBoolean(row, value)

          case other =>
            logger.warn(s"Unsupported type cast target: $other, putting null")
            onHeap.putNull(row)
        }
      }
    }
    onHeap
  }
}
