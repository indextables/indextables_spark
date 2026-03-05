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

import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.execution.vectorized.{ConstantColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.unsafe.types.UTF8String

import io.indextables.tantivy4java.aggregation._
import io.indextables.tantivy4java.split.SplitAggregation

/**
 * Shared utilities for GROUP BY columnar readers (single-split and multi-split).
 *
 * Extracts common batch assembly, type casting, aggregation building, and partition column
 * injection logic to avoid duplication between GroupByAggregateColumnarReader and
 * MultiSplitGroupByAggregateColumnarReader.
 */
object GroupByColumnarReaderUtils {

  // --- Type casting ---

  def castColumnSafe(source: ColumnVector, targetType: DataType, numRows: Int): ColumnVector = {
    val onHeap = new OnHeapColumnVector(numRows, targetType)
    (0 until numRows).foreach { row =>
      if (source.isNullAt(row)) {
        onHeap.putNull(row)
      } else {
        targetType match {
          case StringType =>
            val utf8 = source.getUTF8String(row)
            if (utf8 != null) {
              val bytes = utf8.getBytes
              onHeap.putByteArray(row, bytes, 0, bytes.length)
            } else {
              onHeap.putNull(row)
            }
          case IntegerType =>
            val value = try source.getLong(row).toInt
            catch { case _: Exception =>
              try Math.round(source.getDouble(row)).toInt
              catch { case _: Exception =>
                source.getUTF8String(row).toString.toDouble.toInt
              }
            }
            onHeap.putInt(row, value)
          case LongType =>
            val value = try source.getLong(row)
            catch { case _: Exception =>
              try Math.round(source.getDouble(row))
              catch { case _: Exception =>
                source.getUTF8String(row).toString.toDouble.toLong
              }
            }
            onHeap.putLong(row, value)
          case DoubleType =>
            val value = try source.getDouble(row)
            catch { case _: Exception =>
              try source.getLong(row).toDouble
              catch { case _: Exception =>
                source.getUTF8String(row).toString.toDouble
              }
            }
            onHeap.putDouble(row, value)
          case FloatType =>
            val value = try source.getDouble(row).toFloat
            catch { case _: Exception =>
              try source.getFloat(row)
              catch { case _: Exception =>
                source.getUTF8String(row).toString.toFloat
              }
            }
            onHeap.putFloat(row, value)
          case _ =>
            try onHeap.putLong(row, source.getLong(row))
            catch {
              case _: Exception =>
                try onHeap.putDouble(row, source.getDouble(row))
                catch { case _: Exception => onHeap.putNull(row) }
            }
        }
      }
    }
    onHeap
  }

  def getAggOutputType(aggExpr: AggregateFunc, schema: StructType): DataType = aggExpr match {
    case _: Count | _: CountStar => LongType
    case sum: Sum =>
      val fieldName = extractFieldName(sum.column)
      val inputType = schema.fields.find(_.name == fieldName).map(_.dataType).getOrElse(LongType)
      inputType match {
        case IntegerType | LongType => LongType
        case _                      => DoubleType
      }
    case min: Min =>
      val fieldName = extractFieldName(min.column)
      schema.fields.find(_.name == fieldName).map(_.dataType).getOrElse(LongType)
    case max: Max =>
      val fieldName = extractFieldName(max.column)
      schema.fields.find(_.name == fieldName).map(_.dataType).getOrElse(LongType)
    case _ => LongType
  }

  // --- Batch assembly ---

  def assembleGroupByBatch(
    ffiBatch: ColumnarBatch,
    numKeyColumns: Int,
    columnNames: Array[String],
    aggExprs: Array[AggregateFunc],
    dataGroupByCols: Array[String],
    schema: StructType
  ): ColumnarBatch = {
    val numRows  = ffiBatch.numRows()
    val numOutCols = numKeyColumns + aggExprs.length
    val vectors  = new Array[ColumnVector](numOutCols)

    (0 until numKeyColumns).foreach { i =>
      val keyColName = dataGroupByCols(i)
      val targetType = schema.fields.find(_.name == keyColName).map(_.dataType).getOrElse(StringType)
      vectors(i) = castColumnSafe(ffiBatch.column(i), targetType, numRows)
    }

    val colNameToIdx = if (columnNames.nonEmpty) columnNames.zipWithIndex.toMap
                       else Map.empty[String, Int]

    val docCountIdx = if (colNameToIdx.nonEmpty) colNameToIdx.getOrElse("doc_count", numKeyColumns)
                      else numKeyColumns

    aggExprs.zipWithIndex.foreach {
      case (aggExpr, outIdx) =>
        val ffiColIdx = aggExpr match {
          case _: Count | _: CountStar => docCountIdx
          case _: Sum => colNameToIdx.getOrElse(s"sum_$outIdx", findNextSubAggCol(colNameToIdx, docCountIdx))
          case _: Min => colNameToIdx.getOrElse(s"min_$outIdx", findNextSubAggCol(colNameToIdx, docCountIdx))
          case _: Max => colNameToIdx.getOrElse(s"max_$outIdx", findNextSubAggCol(colNameToIdx, docCountIdx))
          case _      => colNameToIdx.getOrElse(s"agg_$outIdx", findNextSubAggCol(colNameToIdx, docCountIdx))
        }
        val targetType = getAggOutputType(aggExpr, schema)
        vectors(numKeyColumns + outIdx) = castColumnSafe(ffiBatch.column(ffiColIdx), targetType, numRows)
    }

    new ColumnarBatch(vectors, numRows)
  }

  private def findNextSubAggCol(colNameToIdx: Map[String, Int], docCountIdx: Int): Int =
    docCountIdx + 1

  def assembleBucketBatch(
    ffiBatch: ColumnarBatch,
    columnNames: Array[String],
    aggExprs: Array[AggregateFunc],
    dataGroupByCols: Array[String],
    schema: StructType
  ): ColumnarBatch = {
    val numRows = ffiBatch.numRows()
    val isRange = columnNames.contains("from") || columnNames.contains("to")

    val ffiKeyIndices = columnNames.zipWithIndex.collect {
      case (name, idx) if name == "key" || name.startsWith("key_") => idx
    }
    val numKeyColumns = ffiKeyIndices.length

    val vectors = new Array[ColumnVector](numKeyColumns + aggExprs.length)

    ffiKeyIndices.zipWithIndex.foreach { case (ffiIdx, outIdx) =>
      val keyColName = if (outIdx < dataGroupByCols.length) dataGroupByCols(outIdx) else "key"
      val keyTargetType = if (outIdx == 0 && isRange) StringType
                          else schema.fields.find(_.name == keyColName).map(_.dataType).getOrElse(StringType)
      vectors(outIdx) = castColumnSafe(ffiBatch.column(ffiIdx), keyTargetType, numRows)
    }

    val docCountIdx = columnNames.indexOf("doc_count")
    val colNameToIdx = columnNames.zipWithIndex.toMap
    val reservedNames = Set("doc_count", "from", "to")
    val subAggColIndices = columnNames.zipWithIndex.collect {
      case (name, idx) if !reservedNames.contains(name) && name != "key" && !name.startsWith("key_") => idx
    }

    var subAggOffset = 0
    aggExprs.zipWithIndex.foreach {
      case (aggExpr, outIdx) =>
        val ffiColIdx = aggExpr match {
          case _: Count | _: CountStar => docCountIdx
          case _: Sum =>
            colNameToIdx.getOrElse(s"sum_$outIdx", {
              val idx = if (subAggOffset < subAggColIndices.length) subAggColIndices(subAggOffset) else docCountIdx
              subAggOffset += 1; idx
            })
          case _: Min =>
            colNameToIdx.getOrElse(s"min_$outIdx", {
              val idx = if (subAggOffset < subAggColIndices.length) subAggColIndices(subAggOffset) else docCountIdx
              subAggOffset += 1; idx
            })
          case _: Max =>
            colNameToIdx.getOrElse(s"max_$outIdx", {
              val idx = if (subAggOffset < subAggColIndices.length) subAggColIndices(subAggOffset) else docCountIdx
              subAggOffset += 1; idx
            })
          case _ =>
            val idx = if (subAggOffset < subAggColIndices.length) subAggColIndices(subAggOffset) else docCountIdx
            subAggOffset += 1; idx
        }
        val targetType = getAggOutputType(aggExpr, schema)
        vectors(numKeyColumns + outIdx) = castColumnSafe(ffiBatch.column(ffiColIdx), targetType, numRows)
    }

    new ColumnarBatch(vectors, numRows)
  }

  def assembleGroupByBatchWithCompoundKeys(
    ffiBatch: ColumnarBatch,
    columnNames: Array[String],
    numOutputKeys: Int,
    aggExprs: Array[AggregateFunc],
    dataGroupByCols: Array[String],
    schema: StructType
  ): ColumnarBatch = {
    val numRows  = ffiBatch.numRows()
    val numOutCols = numOutputKeys + aggExprs.length
    val vectors  = new Array[ColumnVector](numOutCols)

    val ffiKeyIndices = columnNames.zipWithIndex.collect {
      case (name, idx) if name == "key" || name.startsWith("key_") => idx
    }
    val docCountIdx = columnNames.indexOf("doc_count")

    (0 until numOutputKeys).foreach { keyIdx =>
      val keyColName = dataGroupByCols(keyIdx)
      val targetType = schema.fields.find(_.name == keyColName).map(_.dataType).getOrElse(StringType)
      val onHeap = new OnHeapColumnVector(numRows, targetType)

      (0 until numRows).foreach { row =>
        val parts = ffiKeyIndices.flatMap { ffiIdx =>
          if (ffiBatch.column(ffiIdx).isNullAt(row)) Array("")
          else ffiBatch.column(ffiIdx).getUTF8String(row).toString.split("\\|", -1)
        }
        val part = if (keyIdx < parts.length) parts(keyIdx) else ""

        targetType match {
          case StringType =>
            val bytes = part.getBytes("UTF-8")
            onHeap.putByteArray(row, bytes, 0, bytes.length)
          case IntegerType => onHeap.putInt(row, part.toDouble.toInt)
          case LongType    => onHeap.putLong(row, part.toDouble.toLong)
          case FloatType   => onHeap.putFloat(row, part.toFloat)
          case DoubleType  => onHeap.putDouble(row, part.toDouble)
          case _           =>
            val bytes = part.getBytes("UTF-8")
            onHeap.putByteArray(row, bytes, 0, bytes.length)
        }
      }
      vectors(keyIdx) = onHeap
    }

    val colNameToIdx = columnNames.zipWithIndex.toMap
    aggExprs.zipWithIndex.foreach {
      case (aggExpr, outIdx) =>
        val ffiColIdx = aggExpr match {
          case _: Count | _: CountStar => docCountIdx
          case _: Sum => colNameToIdx.getOrElse(s"sum_$outIdx", docCountIdx + 1)
          case _: Min => colNameToIdx.getOrElse(s"min_$outIdx", docCountIdx + 1)
          case _: Max => colNameToIdx.getOrElse(s"max_$outIdx", docCountIdx + 1)
          case _      => colNameToIdx.getOrElse(s"agg_$outIdx", docCountIdx + 1)
        }
        val targetType = getAggOutputType(aggExpr, schema)
        vectors(numOutputKeys + outIdx) = castColumnSafe(ffiBatch.column(ffiColIdx), targetType, numRows)
    }

    new ColumnarBatch(vectors, numRows)
  }

  // --- Partition column injection ---

  def injectPartitionColumns(
    ffiBatch: ColumnarBatch,
    allGroupByColumns: Array[String],
    dataGroupByCols: Array[String],
    partitionGroupByCols: Array[String],
    partitionValues: Map[String, String],
    aggExprs: Array[AggregateFunc],
    schema: StructType
  ): ColumnarBatch = {
    val numRows     = ffiBatch.numRows()
    val numTotalCols = allGroupByColumns.length + aggExprs.length

    val vectors = new Array[ColumnVector](numTotalCols)
    var ffiColIdx = 0

    allGroupByColumns.zipWithIndex.foreach {
      case (colName, idx) =>
        if (partitionGroupByCols.contains(colName)) {
          vectors(idx) = createPartitionConstantVector(colName, partitionValues, schema, numRows)
        } else {
          vectors(idx) = ffiBatch.column(ffiColIdx)
          ffiColIdx += 1
        }
    }

    val aggStartIdx = allGroupByColumns.length
    aggExprs.zipWithIndex.foreach {
      case (_, aggIdx) =>
        if (ffiColIdx < ffiBatch.numCols()) {
          vectors(aggStartIdx + aggIdx) = ffiBatch.column(ffiColIdx)
          ffiColIdx += 1
        }
    }

    new ColumnarBatch(vectors, numRows)
  }

  def createPartitionConstantVector(
    colName: String,
    partitionValues: Map[String, String],
    schema: StructType,
    numRows: Int
  ): ConstantColumnVector = {
    val fieldType = schema.fields.find(_.name == colName).map(_.dataType).getOrElse(StringType)
    val constVec = new ConstantColumnVector(numRows, fieldType)
    val value = partitionValues.getOrElse(colName, "")
    fieldType match {
      case StringType    => constVec.setUtf8String(UTF8String.fromString(value))
      case IntegerType   => constVec.setInt(value.toInt)
      case LongType      => constVec.setLong(value.toLong)
      case FloatType     => constVec.setFloat(value.toFloat)
      case DoubleType    => constVec.setDouble(value.toDouble)
      case DateType      =>
        val days = java.time.LocalDate.parse(value).toEpochDay.toInt
        constVec.setInt(days)
      case TimestampType =>
        val micros = java.sql.Timestamp.valueOf(value).getTime * 1000L
        constVec.setLong(micros)
      case _             => constVec.setUtf8String(UTF8String.fromString(value))
    }
    constVec
  }

  // --- Aggregation building ---

  def addSubAggregations(termsAgg: TermsAggregation, aggExprs: Array[AggregateFunc]): Unit =
    aggExprs.zipWithIndex.foreach {
      case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar =>
          case sum: Sum =>
            termsAgg.addSubAggregation(s"sum_$index", new SumAggregation(extractFieldName(sum.column)))
          case min: Min =>
            termsAgg.addSubAggregation(s"min_$index", new MinAggregation(extractFieldName(min.column)))
          case max: Max =>
            termsAgg.addSubAggregation(s"max_$index", new MaxAggregation(extractFieldName(max.column)))
          case _ =>
        }
    }

  def addMultiTermsSubAggregations(multiTermsAgg: MultiTermsAggregation, aggExprs: Array[AggregateFunc]): Unit =
    aggExprs.zipWithIndex.foreach {
      case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar =>
          case sum: Sum =>
            multiTermsAgg.addSubAggregation(s"sum_$index", new SumAggregation(extractFieldName(sum.column)))
          case min: Min =>
            multiTermsAgg.addSubAggregation(s"min_$index", new MinAggregation(extractFieldName(min.column)))
          case max: Max =>
            multiTermsAgg.addSubAggregation(s"max_$index", new MaxAggregation(extractFieldName(max.column)))
          case _ =>
        }
    }

  def addBucketSubAggregations(agg: SplitAggregation, aggExprs: Array[AggregateFunc]): Unit = {
    aggExprs.zipWithIndex.foreach {
      case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar =>
          case sum: Sum =>
            val subAgg = new SumAggregation(extractFieldName(sum.column))
            agg match {
              case h: HistogramAggregation    => h.addSubAggregation(s"sum_$index", subAgg)
              case d: DateHistogramAggregation => d.addSubAggregation(s"sum_$index", subAgg)
              case r: RangeAggregation         => r.addSubAggregation(s"sum_$index", subAgg)
              case _                           =>
            }
          case min: Min =>
            val subAgg = new MinAggregation(extractFieldName(min.column))
            agg match {
              case h: HistogramAggregation    => h.addSubAggregation(s"min_$index", subAgg)
              case d: DateHistogramAggregation => d.addSubAggregation(s"min_$index", subAgg)
              case r: RangeAggregation         => r.addSubAggregation(s"min_$index", subAgg)
              case _                           =>
            }
          case max: Max =>
            val subAgg = new MaxAggregation(extractFieldName(max.column))
            agg match {
              case h: HistogramAggregation    => h.addSubAggregation(s"max_$index", subAgg)
              case d: DateHistogramAggregation => d.addSubAggregation(s"max_$index", subAgg)
              case r: RangeAggregation         => r.addSubAggregation(s"max_$index", subAgg)
              case _                           =>
            }
          case _ =>
        }
    }
  }

  def buildNestedTermsAggregation(columns: List[String], depth: Int, aggExprs: Array[AggregateFunc]): TermsAggregation = {
    val columnName = columns.head
    val aggName    = if (depth == 0) "nested_terms" else s"nested_terms_$depth"
    val termsAgg   = new TermsAggregation(aggName, columnName, 1000, 0)

    if (columns.tail.isEmpty) {
      addSubAggregations(termsAgg, aggExprs)
    } else {
      val nestedAgg = buildNestedTermsAggregation(columns.tail, depth + 1, aggExprs)
      val nestedAggName = if (depth + 1 == 1) "nested_terms_1" else s"nested_terms_${depth + 1}"
      termsAgg.addSubAggregation(nestedAggName, nestedAgg)
    }

    termsAgg
  }

  // --- Partition-only aggregation ---

  /**
   * Build metric aggregations for partition-only GROUP BY.
   *
   * For COUNT/CountStar, uses `selectCountField` which tries companion tracking fields,
   * fields from other aggregation expressions, then any available fast field.
   * Throws if no fast field is available (same behavior as SimpleAggregateColumnarReader).
   */
  def buildPartitionOnlyAggregations(
    aggExprs: Array[AggregateFunc],
    fastFields: Set[String],
    tableSchema: StructType
  ): Array[(String, SplitAggregation)] = {
    val result = scala.collection.mutable.ArrayBuffer[(String, SplitAggregation)]()

    aggExprs.zipWithIndex.foreach {
      case (aggExpr, index) =>
        aggExpr match {
          case count: Count =>
            val fieldName = extractFieldName(count.column)
            val aggName = s"count_$index"
            result += ((aggName, new CountAggregation(aggName, fieldName).asInstanceOf[SplitAggregation]))

          case _: CountStar =>
            val countField = selectCountField(fastFields, aggExprs, tableSchema)
            val aggName = s"count_$index"
            result += ((aggName, new CountAggregation(aggName, countField).asInstanceOf[SplitAggregation]))

          case sum: Sum =>
            val fieldName = extractFieldName(sum.column)
            result += ((s"sum_$index", new SumAggregation(fieldName).asInstanceOf[SplitAggregation]))

          case min: Min =>
            val fieldName = extractFieldName(min.column)
            result += ((s"min_$index", new MinAggregation(fieldName).asInstanceOf[SplitAggregation]))

          case max: Max =>
            val fieldName = extractFieldName(max.column)
            result += ((s"max_$index", new MaxAggregation(fieldName).asInstanceOf[SplitAggregation]))

          case _ =>
        }
    }

    result.toArray
  }

  /**
   * Select a field for COUNT(*) aggregation. Priority:
   *   1. Companion tracking fields (__pq_file_hash / __pq_row_in_file)
   *   2. Field from another aggregation expression in the query
   *   3. Any fast field from the split's docMapping
   *   4. Throws if no fast field is available
   */
  private def selectCountField(
    fastFields: Set[String],
    aggExprs: Array[AggregateFunc],
    tableSchema: StructType
  ): String = {
    if (fastFields.contains("__pq_file_hash")) return "__pq_file_hash"
    if (fastFields.contains("__pq_row_in_file")) return "__pq_row_in_file"

    val fieldFromAgg = aggExprs.collectFirst {
      case sum: Sum     => Some(extractFieldName(sum.column))
      case min: Min     => Some(extractFieldName(min.column))
      case max: Max     => Some(extractFieldName(max.column))
      case count: Count => Some(extractFieldName(count.column))
    }.flatten

    fieldFromAgg.getOrElse {
      val numericTypes: Set[DataType] = Set(IntegerType, LongType, FloatType, DoubleType, DateType, TimestampType, BooleanType)
      if (fastFields.nonEmpty) {
        val numericFastField = fastFields.find { fieldName =>
          tableSchema.fields.find(_.name == fieldName).exists(f => numericTypes.contains(f.dataType))
        }
        numericFastField.getOrElse(fastFields.head)
      } else {
        throw new IllegalArgumentException(
          "COUNT(*) aggregation requires at least one fast field. " +
            "Please configure a fast field using spark.indextables.indexing.fastfields."
        )
      }
    }
  }

  def assemblePartitionOnlyBatch(
    ffiBatches: Array[ColumnarBatch],
    allGroupByColumns: Array[String],
    partitionValues: Map[String, String],
    aggExprs: Array[AggregateFunc],
    schema: StructType
  ): ColumnarBatch = {
    val numRows = 1
    val numOutCols = allGroupByColumns.length + aggExprs.length
    val vectors = new Array[ColumnVector](numOutCols)

    // Partition key columns as constants
    allGroupByColumns.zipWithIndex.foreach { case (colName, idx) =>
      vectors(idx) = createPartitionConstantVector(colName, partitionValues, schema, numRows)
    }

    // Metric columns from FFI batches
    var ffiBatchIdx = 0
    aggExprs.zipWithIndex.foreach { case (aggExpr, outIdx) =>
      val targetType = getAggOutputType(aggExpr, schema)
      val colIdx = allGroupByColumns.length + outIdx

      if (ffiBatchIdx < ffiBatches.length && ffiBatches(ffiBatchIdx) != null && ffiBatches(ffiBatchIdx).numRows() > 0) {
        vectors(colIdx) = castColumnSafe(ffiBatches(ffiBatchIdx).column(0), targetType, numRows)
        ffiBatchIdx += 1
      } else {
        val v = new OnHeapColumnVector(numRows, targetType)
        targetType match {
          case LongType => v.putLong(0, 0L)
          case _        => v.putDouble(0, 0.0)
        }
        vectors(colIdx) = v
      }
    }

    new ColumnarBatch(vectors, numRows)
  }

  // --- Query building ---

  def buildQueryAstJson(
    pushedFilters: Array[org.apache.spark.sql.sources.Filter],
    indexQueryFilters: Array[Any],
    partitionColumns: Set[String],
    splitSearchEngine: io.indextables.spark.search.SplitSearchEngine,
    config: Map[String, String]
  ): String = {
    val nonPartitionPushedFilters = if (partitionColumns.nonEmpty && pushedFilters.nonEmpty) {
      io.indextables.spark.filters.MixedBooleanFilter.stripPartitionOnlyFilters(pushedFilters, partitionColumns)
    } else {
      pushedFilters
    }

    val cleanedIndexQueryFilters = if (partitionColumns.nonEmpty && indexQueryFilters.nonEmpty) {
      io.indextables.spark.filters.MixedBooleanFilter.stripPartitionFiltersFromArray(indexQueryFilters, partitionColumns)
    } else {
      indexQueryFilters
    }

    val allFilters = nonPartitionPushedFilters ++ cleanedIndexQueryFilters

    val splitQuery = if (allFilters.nonEmpty) {
      val splitFieldNames = {
        val schema = splitSearchEngine.getSchema()
        try Some(schema.getFieldNames().asScala.toSet)
        catch { case _: Exception => None }
        finally schema.close()
      }
      val optionsFromConfig = new org.apache.spark.sql.util.CaseInsensitiveStringMap(config.asJava)
      FiltersToQueryConverter.convertToSplitQuery(allFilters, splitSearchEngine, splitFieldNames, Some(optionsFromConfig))
    } else {
      new io.indextables.tantivy4java.split.SplitMatchAllQuery()
    }

    splitQuery.toQueryAstJson()
  }

  // --- Field name extraction ---

  def extractFieldName(column: org.apache.spark.sql.connector.expressions.Expression): String =
    if (column.getClass.getSimpleName == "FieldReference") column.toString
    else io.indextables.spark.util.ExpressionUtils.extractFieldName(column)
}
