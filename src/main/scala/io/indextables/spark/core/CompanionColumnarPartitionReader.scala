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
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.hadoop.fs.Path

import io.indextables.spark.arrow.ArrowFfiBridge
import io.indextables.spark.search.SplitSearchEngine
import io.indextables.spark.transaction.AddAction
import org.slf4j.LoggerFactory

/**
 * Columnar partition reader for companion mode splits using Arrow FFI.
 *
 * Uses the Arrow C Data Interface to transfer parquet data directly from the native Rust layer as Arrow columnar
 * batches — zero serialization, zero row conversion.
 *
 * Single-batch model: next() executes search + Arrow FFI retrieval once, get() returns the ColumnarBatch, close()
 * frees resources.
 *
 * Shared initialization logic (effective limit, path resolution, cache config, companion validation, path
 * normalization, footer validation, SplitMetadata reconstruction, SplitSearchEngine creation, query building)
 * is delegated to [[SplitReaderContext]].
 */
class CompanionColumnarPartitionReader(
  addAction: AddAction,
  readSchema: StructType,
  fullTableSchema: StructType,
  filters: Array[Filter],
  limit: Option[Int] = None,
  config: Map[String, String],
  tablePath: Path,
  indexQueryFilters: Array[Any] = Array.empty,
  metricsAccumulator: Option[io.indextables.spark.storage.BatchOptimizationMetricsAccumulator] = None
) extends PartitionReader[ColumnarBatch] {

  private val logger = LoggerFactory.getLogger(classOf[CompanionColumnarPartitionReader])

  private val ctx = new SplitReaderContext(
    addAction, readSchema, fullTableSchema, filters, limit,
    config, tablePath, indexQueryFilters, metricsAccumulator
  )

  private def effectiveLimit = ctx.effectiveLimit
  private def partitionColumnNames = ctx.partitionColumnNames
  private def dataFieldNames = ctx.dataFieldNames

  private val bridge                                = new ArrowFfiBridge()
  private var splitSearchEngine: SplitSearchEngine  = _
  private var batch: ColumnarBatch                  = _
  private var initialized                           = false
  private var consumed                              = false

  // NOTE: Pre-warm join is intentionally omitted for columnar companion reader.
  // Companion splits use parquet-native reads which don't benefit from the tantivy
  // component pre-warming path.
  private def initialize(): Unit =
    if (!initialized) {
      try {
        splitSearchEngine = ctx.createSplitSearchEngine()
        initialized = true
        logger.info(s"CompanionColumnarPartitionReader initialized for ${addAction.path}, effectiveLimit=$effectiveLimit")
      } catch {
        case ex: Exception =>
          logger.error(s"Failed to initialize columnar reader for ${addAction.path}", ex)
          initialized = true
          throw new IOException(s"Failed to initialize columnar reader: ${ex.getMessage}", ex)
      }
    }

  override def next(): Boolean = {
    if (consumed) return false

    try {
      initialize()
      consumed = true

      val splitQuery = ctx.buildSplitQuery(splitSearchEngine)

      // --- Execute search ---
      val searchResult = splitSearchEngine.getSplitSearcher().search(splitQuery, effectiveLimit)
      try {
        val hits = searchResult.getHits.asScala.toArray
        if (hits.isEmpty) {
          logger.info(s"CompanionColumnarPartitionReader: 0 hits for ${addAction.path}")
          return false
        }

        // Sort DocAddresses for sequential I/O (cache locality + range read efficiency)
        val docAddresses = hits.map(_.getDocAddress).sortBy(addr => (addr.getSegmentOrd, addr.getDoc))
        val numCols      = dataFieldNames.length

        logger.info(
          s"CompanionColumnarPartitionReader: ${docAddresses.length} hits, " +
            s"$numCols data columns [${dataFieldNames.mkString(", ")}] for ${addAction.path}"
        )

        if (numCols == 0) {
          // All requested columns are partition columns — no FFI needed
          batch = buildPartitionOnlyBatch(docAddresses.length)
          return batch.numRows() > 0
        }

        // Defensive check: verify the searcher actually supports Arrow FFI (per developer guide)
        val searcher = splitSearchEngine.getSplitSearcher()
        if (!searcher.supportsArrowFfi()) {
          throw new IllegalStateException(
            s"Arrow FFI not supported for companion split ${addAction.path}. " +
              "supportsArrowFfi() returned false — the split may not have a parquet manifest. " +
              "Disable columnar reads with spark.indextables.read.columnar.enabled=false to fall back to row path."
          )
        }

        // Allocate Arrow FFI structs
        val (arrays, schemas, arrayAddrs, schemaAddrs) = bridge.allocateStructs(numCols)

        // Call native FFI export — wrap in try/catch to clean up FFI structs on failure
        val numRows = try {
          val n = searcher.docBatchArrowFfi(docAddresses, arrayAddrs, schemaAddrs, dataFieldNames: _*)

          if (n < 0) {
            // FFI returned -1 despite supportsArrowFfi() check — unexpected
            arrays.foreach(a => try a.close() catch { case _: Exception => })
            schemas.foreach(s => try s.close() catch { case _: Exception => })
            throw new IllegalStateException(
              s"docBatchArrowFfi returned -1 for companion split ${addAction.path} " +
                "despite supportsArrowFfi() returning true. " +
                "Arrow FFI not available — the split may not have a parquet manifest."
            )
          }
          n
        } catch {
          case ex: IllegalStateException => throw ex // re-throw our own exception
          case ex: Exception =>
            // Clean up FFI structs not yet consumed by importVector
            arrays.foreach(a => try a.close() catch { case _: Exception => })
            schemas.foreach(s => try s.close() catch { case _: Exception => })
            throw ex
        }

        // Import into ColumnarBatch via ArrowFfiBridge
        val dataBatch = bridge.importAsColumnarBatch(arrays, schemas, numRows)

        // Assemble final batch with partition columns in readSchema order.
        // assembleColumnarBatch extracts column vectors from dataBatch into a new ColumnarBatch.
        // The dataBatch reference goes out of scope but is NOT closed — its vectors are now
        // owned by the assembled batch, which is closed in close().
        batch = assembleColumnarBatch(dataBatch, numRows)
        logger.info(s"CompanionColumnarPartitionReader: produced batch with $numRows rows for ${addAction.path}")
        numRows > 0

      } finally
        searchResult.close()

    } catch {
      case ex: Exception =>
        logger.error(s"Error in columnar next() for ${addAction.path}", ex)
        throw new RuntimeException(s"Failed to read columnar partition for ${addAction.path}: ${ex.getMessage}", ex)
    }
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = {
    ctx.collectMetricsDelta()
    ctx.reportBytesRead()

    if (batch != null) {
      try batch.close()
      catch { case e: Exception => logger.warn("Error closing ColumnarBatch", e) }
    }

    try bridge.close()
    catch { case e: Exception => logger.warn("Error closing ArrowFfiBridge", e) }

    if (splitSearchEngine != null) {
      try splitSearchEngine.close()
      catch { case e: Exception => logger.warn("Error closing SplitSearchEngine", e) }
    }
  }

  /**
   * Assemble the final ColumnarBatch in readSchema column order by interleaving data columns (from FFI) with
   * partition columns (as ConstantColumnVector).
   */
  private def assembleColumnarBatch(dataBatch: ColumnarBatch, numRows: Int): ColumnarBatch = {
    if (partitionColumnNames.isEmpty) return dataBatch

    // Build index mapping: data field name -> column index in dataBatch
    val dataFieldIndex = dataFieldNames.zipWithIndex.toMap

    val allVectors: Array[ColumnVector] = readSchema.fields.map { field =>
      if (partitionColumnNames.contains(field.name)) {
        // Partition column: create constant vector
        val strVal = addAction.partitionValues(field.name)
        createConstantColumnVector(strVal, field.dataType, numRows)
      } else {
        // Data column: reference from FFI batch
        val idx = dataFieldIndex(field.name)
        dataBatch.column(idx)
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
        case StringType    => vec.setUtf8String(UTF8String.fromString(value))
        case IntegerType   => vec.setInt(value.toInt)
        case LongType      => vec.setLong(value.toLong)
        case DoubleType    => vec.setDouble(value.toDouble)
        case FloatType     => vec.setFloat(value.toFloat)
        case BooleanType   => vec.setBoolean(value.toBoolean)
        case ShortType     => vec.setShort(value.toShort)
        case ByteType      => vec.setByte(value.toByte)
        case DateType      =>
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
