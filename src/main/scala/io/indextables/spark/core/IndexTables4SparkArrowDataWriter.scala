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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.arrow.ArrowFfiWriteBridge
import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.json.{SparkSchemaToTantivyMapper, SparkToTantivyConverter}
import io.indextables.spark.transaction.{AddAction, PartitionUtils}
import io.indextables.spark.util.{JsonUtil, ProtocolNormalizer, StatisticsCalculator, StatisticsTruncation}
import io.indextables.spark.write.ArrowFfiWriteConfig
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Arrow FFI DataWriter that buffers InternalRows into Arrow columnar batches and sends them to Rust via the Arrow C
 * Data Interface for zero-copy ingestion.
 *
 * TWO-LAYER ARCHITECTURE:
 *
 * Layer 1 (current, Spark 3.5/4.x constraint): InternalRow → ArrowFfiWriteBridge → VectorSchemaRoot (row-level
 * buffering). This layer exists because Spark V2 `DataWriter[T]` restricts T to `InternalRow`.
 *
 * Layer 2 (permanent): VectorSchemaRoot → FFI export → Rust via `QuickwitSplit.addArrowBatch()`.
 *
 * When Spark adds `DataWriter[ColumnarBatch]` support, Layer 1 disappears and this class becomes
 * `DataWriter[ColumnarBatch]` that passes batches directly to Layer 2 — the ColumnarBatch's underlying
 * ArrowColumnVectors are exported via `Data.exportVectorSchemaRoot()` with no row-level buffering.
 *
 * @param tablePath
 *   Root path of the IndexTables table
 * @param writeSchema
 *   Spark schema for the data being written
 * @param partitionId
 *   Spark partition ID for this writer task
 * @param taskId
 *   Spark task ID
 * @param serializedOptions
 *   Configuration options (serialized for executor transport)
 * @param partitionColumns
 *   Partition column names (empty for non-partitioned tables)
 * @param arrowFfiConfig
 *   Arrow FFI write configuration
 */
class IndexTables4SparkArrowDataWriter(
  tablePath: Path,
  writeSchema: StructType,
  partitionId: Int,
  taskId: Long,
  serializedOptions: Map[String, String],
  partitionColumns: Seq[String],
  arrowFfiConfig: ArrowFfiWriteConfig)
    extends DataWriter[InternalRow] {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkArrowDataWriter])

  private lazy val options: CaseInsensitiveStringMap = {
    new CaseInsensitiveStringMap(serializedOptions.asJava)
  }

  // Normalize table path (s3a:// -> s3://)
  private val normalizedTablePath = {
    val pathStr       = tablePath.toString
    val normalizedStr = io.indextables.spark.util.ProtocolNormalizer.normalizeAllProtocols(pathStr)
    new Path(normalizedStr)
  }

  // Build JSON serializer function for complex types (Struct/Array/Map)
  private lazy val tantivyOptions  = IndexTables4SparkOptions(options)
  private lazy val jsonFieldMapper = new SparkSchemaToTantivyMapper(tantivyOptions)
  private lazy val jsonConverter   = new SparkToTantivyConverter(writeSchema, jsonFieldMapper)

  private def serializeComplexType(row: InternalRow, colIdx: Int, dataType: DataType): String =
    dataType match {
      case st: StructType =>
        val internalRow = row.getStruct(colIdx, st.fields.length)
        val genericRow = org.apache.spark.sql.Row.fromSeq(
          st.fields.zipWithIndex.map { case (field, idx) => internalRow.get(idx, field.dataType) }
        )
        val jsonMap = jsonConverter.structToJsonMap(genericRow, st)
        JsonUtil.toJson(jsonMap)

      case at: ArrayType =>
        val arrayData = row.getArray(colIdx)
        val seq       = (0 until arrayData.numElements()).map(i => arrayData.get(i, at.elementType))
        val jsonList  = jsonConverter.arrayToJsonList(seq, at)
        // Arrays need wrapping for tantivy JSON field format
        val wrappedMap = jsonConverter.wrapArrayInObject(jsonList)
        JsonUtil.toJson(wrappedMap)

      case mt: MapType =>
        val mapData = row.getMap(colIdx)
        val sparkMap = scala.collection.Map(
          mapData.keyArray().toSeq[Any](mt.keyType).zip(mapData.valueArray().toSeq[Any](mt.valueType)): _*
        )
        val jsonMap = jsonConverter.mapToJsonMap(sparkMap, mt)
        JsonUtil.toJson(jsonMap)

      case _ =>
        row.get(colIdx, dataType).toString
    }

  // Arrow FFI bridge for row→Arrow→FFI conversion
  private val bridge = new ArrowFfiWriteBridge(
    writeSchema,
    arrowFfiConfig.batchSize,
    jsonSerializer = Some(serializeComplexType)
  )

  // Per-partition statistics for data skipping
  private val partitionStats = mutable.Map[String, StatisticsCalculator.DatasetStatistics]()

  // Precomputed partition column info
  private val partitionInfo: PartitionUtils.PartitionColumnInfo =
    PartitionUtils.precomputePartitionInfo(writeSchema, partitionColumns)

  // Native handle from beginSplitFromArrow — lazily initialized on first write
  private var nativeHandle: Long    = 0L
  private var initialized: Boolean  = false
  private var totalRowCount: Long   = 0L

  // Partition column names as Java array for JNI
  private val partitionColsArray: Array[String] = partitionColumns.toArray

  // Split rolling support
  private val maxRowsPerSplit: Option[Long] = Option(serializedOptions.getOrElse("__maxRowsPerSplit", null))
    .filter(_.nonEmpty)
    .map(_.toLong)

  // Heap size for native split writer (default 128MB)
  private val heapSize: Long = Option(serializedOptions.getOrElse("spark.indextables.write.arrowFfi.heapSize", null))
    .filter(_.nonEmpty)
    .map(_.toLong)
    .getOrElse(128L * 1024 * 1024)

  logger.info(
    s"ArrowDataWriter initialized for partition $partitionId, batchSize=${arrowFfiConfig.batchSize}, " +
      s"partitionColumns=[${partitionColumns.mkString(", ")}]"
  )

  /** Initialize the native split writer on first write. */
  private def ensureInitialized(): Unit =
    if (!initialized) {
      val schemaAddr = bridge.exportSchema()
      nativeHandle = QuickwitSplit.beginSplitFromArrow(schemaAddr, partitionColsArray, heapSize)
      initialized = true
      logger.info(s"Native Arrow split writer initialized (handle=$nativeHandle)")
    }

  override def write(record: InternalRow): Unit = {
    ensureInitialized()

    // Update per-partition statistics (needed for data skipping, cheap per-row)
    val statsKey = if (partitionColumns.isEmpty) {
      ""
    } else {
      val partitionValues = PartitionUtils.extractPartitionValuesFast(record, partitionInfo)
      PartitionUtils.createPartitionPath(partitionValues, partitionColumns)
    }
    val stats = partitionStats.getOrElseUpdate(
      statsKey,
      new StatisticsCalculator.DatasetStatistics(writeSchema, serializedOptions)
    )
    stats.updateRow(record)

    // Buffer row into Arrow vectors; flush when batch is full
    val batchFull = bridge.bufferRow(record)
    totalRowCount += 1

    if (batchFull) {
      flushBatch()
    }
  }

  /** Export the current Arrow batch to Rust via FFI. */
  private def flushBatch(): Unit = {
    if (!bridge.hasBufferedRows) return

    val (arrayAddr, schemaAddr) = bridge.exportBatch()
    val cumulativeDocCount      = QuickwitSplit.addArrowBatch(nativeHandle, arrayAddr, schemaAddr)

    logger.debug(
      s"Flushed Arrow batch: cumulativeDocCount=$cumulativeDocCount, totalRowCount=$totalRowCount"
    )
  }

  override def commit(): WriterCommitMessage = {
    if (!initialized || totalRowCount == 0) {
      logger.info(s"No records written in partition $partitionId")
      bridge.close()
      return IndexTables4SparkCommitMessage(Seq.empty)
    }

    // Flush any remaining buffered rows
    flushBatch()

    // Finalize all splits — Rust handles partition routing and writes split files
    val outputDir = resolveOutputDir()
    val results   = QuickwitSplit.finishAllSplits(nativeHandle, outputDir)

    logger.info(s"finishAllSplits produced ${results.size()} splits for partition $partitionId")

    // Build AddActions from PartitionSplitResult
    val allActions = results.asScala.map { result =>
      buildAddAction(result)
    }.toSeq

    // Report output metrics
    val totalBytes   = allActions.map(_.size).sum
    val totalRecords = allActions.flatMap(_.numRecords).sum
    if (org.apache.spark.sql.indextables.OutputMetricsUpdater.updateOutputMetrics(totalBytes, totalRecords)) {
      logger.debug(s"Reported output metrics: $totalBytes bytes, $totalRecords records")
    }

    logger.info(
      s"Committed partition $partitionId with ${allActions.size} splits, " +
        s"$totalBytes bytes, $totalRecords records"
    )

    bridge.close()
    IndexTables4SparkCommitMessage(allActions)
  }

  override def abort(): Unit = {
    logger.warn(s"Aborting Arrow writer for partition $partitionId")
    if (initialized) {
      try
        QuickwitSplit.cancelSplit(nativeHandle)
      catch {
        case e: Exception =>
          logger.warn("Error cancelling native split writer", e)
      }
    }
    bridge.close()
  }

  override def close(): Unit =
    bridge.close()

  // ---- Private helpers ----

  /** Resolve the output directory for split files (local or cloud-normalized). Ensures directory exists for local paths. */
  private def resolveOutputDir(): String = {
    val pathStr = normalizedTablePath.toString
    val resolved = if (pathStr.startsWith("file:")) {
      new java.io.File(normalizedTablePath.toUri).getAbsolutePath
    } else if (ProtocolNormalizer.isS3Path(pathStr) || ProtocolNormalizer.isAzurePath(pathStr)) {
      CloudStorageProviderFactory.normalizePathForTantivy(pathStr, serializedOptions)
    } else {
      // Raw local path (e.g., /tmp/foo/bar)
      pathStr
    }

    // Ensure directory exists for local filesystem paths
    if (!resolved.contains("://")) {
      val dirFile = new java.io.File(resolved)
      if (!dirFile.exists()) {
        dirFile.mkdirs()
      }
    }

    resolved
  }

  /** Build an AddAction from a native PartitionSplitResult. */
  private def buildAddAction(result: QuickwitSplit.PartitionSplitResult): AddAction = {
    val partitionKey    = Option(result.getPartitionKey).getOrElse("")
    val partitionValues = if (result.getPartitionValues != null) result.getPartitionValues.asScala.toMap else Map.empty[String, String]
    val splitPath       = result.getSplitPath
    val numDocs         = result.getNumDocs

    // Get file size from cloud storage
    val splitSize = {
      val cloudProvider = CloudStorageProviderFactory.createProvider(splitPath, serializedOptions)
      try {
        val fileInfo = cloudProvider.getFileInfo(splitPath)
        fileInfo.map(_.size).getOrElse {
          logger.warn(s"Could not get file info for $splitPath")
          0L
        }
      } finally
        cloudProvider.close()
    }

    // Compute relative path for AddAction
    val addActionPath = if (partitionKey.nonEmpty) {
      val fileName = splitPath.substring(splitPath.lastIndexOf('/') + 1)
      s"$partitionKey/$fileName"
    } else {
      splitPath.substring(splitPath.lastIndexOf('/') + 1)
    }

    // Get statistics for this partition
    val statsKey = if (partitionKey.nonEmpty) partitionKey else ""
    val stats    = partitionStats.get(statsKey)
    val (rawMinValues, rawMaxValues) = stats match {
      case Some(s) => (s.getMinValues, s.getMaxValues)
      case None    => (Map.empty[String, String], Map.empty[String, String])
    }

    // Apply statistics truncation
    val configMap = options.asCaseSensitiveMap().asScala.toMap
    val (minValues, maxValues) = StatisticsTruncation.truncateStatistics(
      rawMinValues,
      rawMaxValues,
      configMap
    )

    // Generate docMappingJson from writeSchema (same workaround as TANT batch path)
    val docMappingJson = {
      val fieldMappings = writeSchema.fields
        .map { field =>
          val fieldType = field.dataType.typeName match {
            case "string"             => "text"
            case "integer" | "long"   => "i64"
            case "float" | "double"   => "f64"
            case "boolean"            => "bool"
            case "date" | "timestamp" => "datetime"
            case _                    => "text"
          }
          s""""${field.name}": {"type": "$fieldType", "indexed": true}"""
        }
        .mkString(", ")
      s"""{"fields": {$fieldMappings}}"""
    }

    AddAction(
      path = addActionPath,
      partitionValues = partitionValues,
      size = splitSize,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(numDocs),
      minValues = if (minValues.nonEmpty) Some(minValues) else None,
      maxValues = if (maxValues.nonEmpty) Some(maxValues) else None,
      footerStartOffset = Some(result.getFooterStartOffset),
      footerEndOffset = Some(result.getFooterEndOffset),
      hotcacheStartOffset = None,
      hotcacheLength = None,
      hasFooterOffsets = true,
      timeRangeStart = None,
      timeRangeEnd = None,
      splitTags = None,
      deleteOpstamp = Some(0L),
      numMergeOps = Some(0),
      docMappingJson = Some(docMappingJson),
      uncompressedSizeBytes = None
    )
  }
}
