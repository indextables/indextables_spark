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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.read.PartitionReader
// NOTE: ConstantColumnVector is a Spark-internal API (execution.vectorized package).
// Stable across 3.4+ but not guaranteed across major versions.
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.hadoop.fs.Path

import io.indextables.spark.arrow.ArrowFfiBridge
import io.indextables.spark.search.SplitSearchEngine
import io.indextables.spark.transaction.AddAction
import io.indextables.tantivy4java.split.SplitSearcher
import org.slf4j.LoggerFactory

/**
 * Columnar partition reader for all split types using Arrow FFI streaming.
 *
 * Uses the streaming startStreamingRetrieval()/nextBatch() pipeline for all reads (both companion and regular splits).
 * This eliminates BM25 scoring overhead, avoids intermediate protobuf allocations, and keeps memory bounded at ~24MB
 * regardless of result set size.
 *
 * The effective result limit is controlled by `spark.indextables.read.mode`:
 *   - **fast** (default): Low default limit (250 or configured `defaultLimit`)
 *   - **complete**: Int.MaxValue (returns all matching rows), unless an explicit SQL LIMIT is pushed
 *
 * Falls back to a partition-only path when all projected columns are partition columns (no data columns to stream).
 *
 * Shared initialization logic is delegated to [[SplitReaderContext]].
 */
class ColumnarPartitionReader(
  addAction: AddAction,
  readSchema: StructType,
  fullTableSchema: StructType,
  filters: Array[Filter],
  limit: Option[Int] = None,
  config: Map[String, String],
  tablePath: Path,
  indexQueryFilters: Array[Any] = Array.empty,
  metricsAccumulator: Option[io.indextables.spark.storage.BatchOptimizationMetricsAccumulator] = None)
    extends PartitionReader[ColumnarBatch] {

  private val logger = LoggerFactory.getLogger(classOf[ColumnarPartitionReader])

  private val ctx = new SplitReaderContext(
    addAction,
    readSchema,
    fullTableSchema,
    filters,
    limit,
    config,
    tablePath,
    indexQueryFilters,
    metricsAccumulator
  )

  private def effectiveLimit       = ctx.effectiveLimit
  private def partitionColumnNames = ctx.partitionColumnNames
  private def dataFieldNames       = ctx.dataFieldNames

  private val bridge                               = new ArrowFfiBridge()
  private var splitSearchEngine: SplitSearchEngine = _
  private var initialized                          = false
  private var initFailed                           = false

  // Streaming state
  private var currentBatch: ColumnarBatch                      = _
  private var streamingSession: SplitSearcher.StreamingSession = _
  private var finished                                         = false
  private var totalRowsReturned                                = 0L
  // Partition-only single-batch state
  private var partitionOnlyConsumed = false

  // NOTE: Pre-warm join is intentionally omitted for the streaming columnar reader.
  // The streaming path (startStreamingRetrieval/nextBatch) does not benefit from the
  // tantivy component pre-warming path used by the old row-based reader.
  private def initialize(): Unit =
    if (!initialized) {
      if (initFailed) throw new IOException(s"Columnar reader previously failed to initialize for ${addAction.path}")
      try {
        splitSearchEngine = ctx.createSplitSearchEngine()
        initialized = true
        logger.info(
          s"ColumnarPartitionReader initialized for ${addAction.path}, " +
            s"effectiveLimit=$effectiveLimit, readMode=${ctx.readMode}"
        )
      } catch {
        case ex: Exception =>
          logger.error(s"Failed to initialize columnar reader for ${addAction.path}", ex)
          initFailed = true
          throw new IOException(s"Failed to initialize columnar reader: ${ex.getMessage}", ex)
      }
    }

  override def next(): Boolean = {
    if (finished) return false

    try {
      initialize()

      // Close previous batch before producing the next one
      closePreviousBatch()

      val numCols = dataFieldNames.length

      if (numCols > 0) {
        nextStreaming(splitSearchEngine.getSplitSearcher(), numCols)
      } else {
        nextPartitionOnly(splitSearchEngine.getSplitSearcher())
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Error in columnar next() for ${addAction.path}", ex)
        throw new RuntimeException(s"Failed to read columnar partition for ${addAction.path}: ${ex.getMessage}", ex)
    }
  }

  /**
   * Streaming path: uses startStreamingRetrieval()/nextBatch() for all companion reads with data columns. Returns
   * batches of ~128K rows with bounded ~24MB memory. Stops when the stream is exhausted or effectiveLimit rows have been
   * returned.
   */
  private def nextStreaming(searcher: SplitSearcher, numCols: Int): Boolean = {
    // Check if we've already satisfied the limit
    if (totalRowsReturned >= effectiveLimit) {
      finished = true
      logger.info(s"Streaming: limit satisfied ($totalRowsReturned >= $effectiveLimit) for ${addAction.path}")
      return false
    }

    // Start streaming session on first call
    if (streamingSession == null) {
      val splitQuery = ctx.buildSplitQuery(splitSearchEngine)
      val queryAstJson = splitQuery.toQueryAstJson()
      val typeHints = buildTypeHints()
      val maxDocs = if (effectiveLimit == Int.MaxValue) -1 else effectiveLimit
      streamingSession = if (typeHints != null || maxDocs > 0) {
        searcher.startStreamingRetrieval(queryAstJson, dataFieldNames, typeHints, maxDocs)
      } else {
        searcher.startStreamingRetrieval(queryAstJson, dataFieldNames: _*)
      }
      logger.info(
        s"Streaming: started session for ${addAction.path}, " +
          s"columnCount=${streamingSession.getColumnCount}, effectiveLimit=$effectiveLimit, maxDocs=$maxDocs" +
          (if (typeHints != null) s", typeHints=${typeHints.length / 2} fields" else "")
      )
    }

    // Get next batch
    val (arrays, schemas, arrayAddrs, schemaAddrs) = bridge.allocateStructs(numCols)

    val rows =
      try {
        streamingSession.nextBatch(arrayAddrs, schemaAddrs)
      } catch {
        case ex: Exception =>
          cleanupFfiStructs(arrays, schemas)
          throw ex
      }

    if (rows <= 0) {
      cleanupFfiStructs(arrays, schemas)
      finished = true
      if (rows < 0) {
        throw new RuntimeException(
          s"Streaming retrieval error (nextBatch returned $rows) for ${addAction.path}. " +
            "This typically indicates a parquet read failure or storage error in the native layer."
        )
      }
      logger.info(s"Streaming: end of stream for ${addAction.path}, totalRowsReturned=$totalRowsReturned")
      return false
    }

    val dataBatch = bridge.importAsColumnarBatchStreaming(arrays, schemas, rows)
    currentBatch = assembleColumnarBatch(dataBatch, rows)
    totalRowsReturned += rows
    logger.debug(s"Streaming: batch with $rows rows for ${addAction.path}, totalRowsReturned=$totalRowsReturned")
    true
  }

  /** Partition-only projection path: no FFI needed, just count matching rows and build constant vectors. */
  private def nextPartitionOnly(searcher: SplitSearcher): Boolean = {
    if (partitionOnlyConsumed) return false
    partitionOnlyConsumed = true
    finished = true

    val splitQuery = ctx.buildSplitQuery(splitSearchEngine)
    val searchResult = searcher.search(splitQuery, effectiveLimit)
    try {
      val numHits = searchResult.getHits.size()
      if (numHits == 0) {
        logger.info(s"PartitionOnly: 0 hits for ${addAction.path}")
        return false
      }
      currentBatch = buildPartitionOnlyBatch(numHits)
      logger.info(s"PartitionOnly: $numHits rows for ${addAction.path}")
      currentBatch.numRows() > 0
    } finally
      searchResult.close()
  }

  override def get(): ColumnarBatch = currentBatch

  override def close(): Unit = {
    ctx.collectMetricsDelta()
    ctx.reportBytesRead()

    closePreviousBatch()

    if (streamingSession != null) {
      try streamingSession.close()
      catch { case e: Exception => logger.warn("Error closing StreamingSession", e) }
    }

    try bridge.close()
    catch { case e: Exception => logger.warn("Error closing ArrowFfiBridge", e) }

    if (splitSearchEngine != null) {
      try splitSearchEngine.close()
      catch { case e: Exception => logger.warn("Error closing SplitSearchEngine", e) }
    }
  }

  private def closePreviousBatch(): Unit =
    if (currentBatch != null) {
      try currentBatch.close()
      catch { case e: Exception => logger.warn("Error closing ColumnarBatch", e) }
      currentBatch = null
    }

  private def cleanupFfiStructs(
    arrays: Array[org.apache.arrow.c.ArrowArray],
    schemas: Array[org.apache.arrow.c.ArrowSchema]
  ): Unit = {
    arrays.foreach(a =>
      try a.close()
      catch { case _: Exception => }
    )
    schemas.foreach(s =>
      try s.close()
      catch { case _: Exception => }
    )
  }

  /**
   * Assemble the final ColumnarBatch in readSchema column order by interleaving data columns (from FFI) with partition
   * columns (as ConstantColumnVector).
   *
   * Uses name-based mapping from actual FFI vector field names (not positional), since the native FFI export may return
   * columns in parquet schema order rather than the requested dataFieldNames order.
   */
  private def assembleColumnarBatch(dataBatch: ColumnarBatch, numRows: Int): ColumnarBatch = {
    val ffiColumnMap: Map[String, Int] = (0 until dataBatch.numCols()).map { i =>
      val name = dataBatch.column(i).asInstanceOf[ArrowColumnVector].getValueVector.getField.getName
      name -> i
    }.toMap

    val allVectors: Array[ColumnVector] = readSchema.fields.map { field =>
      if (partitionColumnNames.contains(field.name)) {
        createConstantColumnVector(addAction.partitionValues(field.name), field.dataType, numRows)
      } else {
        ffiColumnMap.get(field.name) match {
          case Some(idx) => dataBatch.column(idx)
          case None =>
            throw new IllegalStateException(
              s"Column '${field.name}' not found in FFI batch. " +
                s"Available columns: [${ffiColumnMap.keys.mkString(", ")}]"
            )
        }
      }
    }

    new ColumnarBatch(allVectors, numRows)
  }

  /** Build a batch containing only partition columns (when all projected columns are partition columns). */
  private def buildPartitionOnlyBatch(numRows: Int): ColumnarBatch = {
    val vectors: Array[ColumnVector] = readSchema.fields.map { field =>
      val strVal = addAction.partitionValues(field.name)
      createConstantColumnVector(strVal, field.dataType, numRows)
    }
    new ColumnarBatch(vectors, numRows)
  }

  /**
   * Build Arrow type hints for non-companion splits where tantivy's internal types (i64, f64) may
   * differ from the Spark schema types (Int32, Float32, etc.). Returns null for companion splits
   * since parquet preserves original types.
   */
  private def buildTypeHints(): Array[String] = {
    val isCompanion = config.contains("spark.indextables.companion.parquetTableRoot")
    if (isCompanion) return null

    // Build alternating [fieldName, arrowType] pairs for fields that need narrowing
    val hints = scala.collection.mutable.ArrayBuffer[String]()
    readSchema.fields.foreach { field =>
      if (!partitionColumnNames.contains(field.name)) {
        val hint = field.dataType match {
          case IntegerType => "i32"
          case ShortType   => "i16"
          case ByteType    => "i8"
          case FloatType   => "f32"
          case _           => null // Default Arrow type is correct
        }
        if (hint != null) {
          hints += field.name
          hints += hint
        }
      }
    }
    if (hints.isEmpty) null else hints.toArray
  }

  /** Create a ConstantColumnVector for a partition column value. */
  private def createConstantColumnVector(
    value: String,
    dataType: DataType,
    numRows: Int
  ): ColumnVector = {
    val vec = new ConstantColumnVector(numRows, dataType)
    if (value == null) {
      vec.setNull()
    } else {
      dataType match {
        case StringType  => vec.setUtf8String(UTF8String.fromString(value))
        case IntegerType => vec.setInt(value.toInt)
        case LongType    => vec.setLong(value.toLong)
        case DoubleType  => vec.setDouble(value.toDouble)
        case FloatType   => vec.setFloat(value.toFloat)
        case BooleanType => vec.setBoolean(value.toBoolean)
        case ShortType   => vec.setShort(value.toShort)
        case ByteType    => vec.setByte(value.toByte)
        case DateType =>
          vec.setInt(java.time.LocalDate.parse(value).toEpochDay.toInt)
        case TimestampType =>
          val instant = if (value.contains("T")) {
            java.time.LocalDateTime.parse(value).atZone(java.time.ZoneOffset.UTC).toInstant
          } else {
            java.time.LocalDate.parse(value).atStartOfDay(java.time.ZoneOffset.UTC).toInstant
          }
          vec.setLong(instant.getEpochSecond * 1000000L + instant.getNano / 1000L)
        case _ =>
          vec.setUtf8String(UTF8String.fromString(value))
      }
    }
    vec
  }
}
