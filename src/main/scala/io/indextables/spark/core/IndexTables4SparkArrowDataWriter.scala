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

import java.util.{Map => JMap}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.arrow.ArrowFfiWriteBridge
import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.AddAction
import io.indextables.spark.util.{ProtocolNormalizer, StatisticsCalculator, StatisticsTruncation}
import io.indextables.spark.write.ArrowFfiWriteConfig
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import org.slf4j.LoggerFactory

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

  private lazy val options: CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(serializedOptions.asJava)

  // Normalize table path (s3a:// -> s3://)
  private val normalizedTablePath = {
    val pathStr       = tablePath.toString
    val normalizedStr = io.indextables.spark.util.ProtocolNormalizer.normalizeAllProtocols(pathStr)
    new Path(normalizedStr)
  }

  private lazy val tantivyOptions = IndexTables4SparkOptions(options)

  // Arrow FFI bridge for row→Arrow→FFI conversion. Complex types (Struct/Array/Map) are passed as
  // native Arrow complex vectors — Rust converts them directly to tantivy OwnedValue without JSON serialization.
  private val bridge = new ArrowFfiWriteBridge(writeSchema, arrowFfiConfig.batchSize)

  // Stats-eligible fields (used to set "stats" flags in field config JSON)
  private lazy val statsEligibleColumns: Set[String] =
    StatisticsCalculator.getStatsEligibleFields(writeSchema, serializedOptions).map(_._1.name).toSet

  // Native handle from beginSplitFromArrow — lazily initialized on first write
  private var nativeHandle: Long   = 0L
  private var initialized: Boolean = false
  private var totalRowCount: Long  = 0L

  // Partition column names as Java array for JNI
  private val partitionColsArray: Array[String] = partitionColumns.toArray

  // Split rolling support
  private val maxRowsPerSplit: Option[Long] = Option(serializedOptions.getOrElse("__maxRowsPerSplit", null))
    .filter(_.nonEmpty)
    .map(_.toLong)

  // Heap size for native split writer
  private val heapSize: Long = arrowFfiConfig.heapSize

  logger.info(
    s"ArrowDataWriter initialized for partition $partitionId, batchSize=${arrowFfiConfig.batchSize}, " +
      s"partitionColumns=[${partitionColumns.mkString(", ")}]"
  )

  // Resolve output directory once (needed for split rolling and final commit)
  // For cloud tables, Rust writes to a local temp dir; splits are uploaded to cloud in buildAddAction.
  private lazy val outputDir: String = resolveOutputDir()

  // Whether the table is on cloud storage (S3/Azure) — determines if splits need uploading after finishAllSplits
  private val isCloudTable: Boolean = {
    val pathStr = normalizedTablePath.toString
    ProtocolNormalizer.isS3Path(pathStr) || ProtocolNormalizer.isAzurePath(pathStr)
  }

  // Cloud-normalized table path (only used for cloud tables to compute upload destinations)
  private lazy val cloudTablePath: String =
    CloudStorageProviderFactory.normalizePathForTantivy(normalizedTablePath.toString, serializedOptions)

  /** Build field configuration JSON from Spark options (typemap, tokenizers). */
  private def buildFieldConfigJson(): String = {
    val fieldTypeMapping   = tantivyOptions.getFieldTypeMapping
    val tokenizerOverrides = tantivyOptions.getTokenizerOverrides

    writeSchema.fields
      .map { field =>
        val typemapValue = fieldTypeMapping.get(field.name.toLowerCase)

        // Map typemap values and Spark DataTypes to Rust field config (type + default tokenizer)
        val (fieldType, defaultTokenizer) = typemapValue match {
          case Some("text")                => ("text", "default")
          case Some("string")              => ("text", "raw")
          case Some("json")                => ("json", "")
          case Some("ip") | Some("ipaddr") => ("ip", "")
          case Some("i64")                 => ("i64", "")
          case Some("f64")                 => ("f64", "")
          case Some("bool")                => ("bool", "")
          case Some("datetime")            => ("datetime", "")
          case Some("bytes")               => ("bytes", "")
          case Some(other)                 => (other, "")  // Schema creation handled by Rust (e.g., text_and_string dual tokenizers)
          case None =>
            field.dataType match {
              case StringType                                => ("text", "raw")
              case IntegerType | LongType                    => ("i64", "")
              case FloatType | DoubleType                    => ("f64", "")
              case BooleanType                               => ("bool", "")
              case TimestampType | DateType                  => ("datetime", "")
              case _: StructType | _: ArrayType | _: MapType => ("json", "")
              case BinaryType                                => ("bytes", "")
              case _                                         => ("text", "raw")
            }
        }

        // Explicit tokenizer override wins over default
        val effectiveTokenizer = tokenizerOverrides.getOrElse(field.name.toLowerCase, defaultTokenizer)
        val escapedName        = escapeJsonString(field.name)
        val tokenizer = if (effectiveTokenizer.nonEmpty) {
          s""","tokenizer":"${escapeJsonString(effectiveTokenizer)}""""
        } else ""
        val stats = if (statsEligibleColumns.contains(field.name)) ""","stats":true""" else ""
        s"""{"name":"$escapedName","type":"$fieldType"$tokenizer$stats}"""
      }
      .mkString("[", ",", "]")
  }

  /** Initialize the native split writer on first write. */
  private def ensureInitialized(): Unit =
    if (!initialized) {
      io.indextables.spark.memory.NativeMemoryInitializer.ensureInitialized()
      val schemaAddr      = bridge.exportSchema()
      val fieldConfigJson = buildFieldConfigJson()
      val maxDocs         = maxRowsPerSplit.getOrElse(0L)

      nativeHandle = QuickwitSplit.beginSplitFromArrow(
        schemaAddr,
        partitionColsArray,
        heapSize,
        fieldConfigJson,
        maxDocs,
        outputDir
      )
      initialized = true
      logger.info(
        s"Native Arrow split writer initialized (handle=$nativeHandle, " +
          s"fieldConfig=${fieldConfigJson.take(200)}, maxDocsPerSplit=$maxDocs)"
      )
    }

  override def write(record: InternalRow): Unit = {
    ensureInitialized()

    // Buffer row into Arrow vectors; flush when batch is full
    // Statistics are computed natively in Rust during addArrowBatch
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

    // Finalize all splits — Rust handles partition routing, writes split files, and computes statistics
    val results = QuickwitSplit.finishAllSplitsRaw(nativeHandle, outputDir)

    logger.info(s"finishAllSplitsRaw produced ${results.size()} splits for partition $partitionId")

    // Build AddActions from raw result maps (includes native statistics)
    val allActions = results.asScala.map(result => buildAddAction(result)).toSeq

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
    cleanupTempDir()
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
    cleanupTempDir()
  }

  override def close(): Unit =
    bridge.close()

  // ---- Private helpers ----

  /**
   * Resolve the output directory for split files. For cloud tables (S3/Azure), Rust's finishAllSplits writes to a local
   * temp directory; splits are then uploaded to cloud in buildAddAction. For local tables, splits are written directly
   * to the table path.
   */
  private def resolveOutputDir(): String = {
    val pathStr = normalizedTablePath.toString
    val resolved = if (isCloudTable) {
      // Rust finishAllSplits only supports local filesystem — use a temp directory
      val tmpDir = java.nio.file.Files.createTempDirectory(s"indextables-arrow-$partitionId").toFile
      tmpDir.getAbsolutePath
    } else if (pathStr.startsWith("file:")) {
      new java.io.File(normalizedTablePath.toUri).getAbsolutePath
    } else {
      // Raw local path (e.g., /tmp/foo/bar)
      pathStr
    }

    // Ensure directory exists for local filesystem paths
    val dirFile = new java.io.File(resolved)
    if (!dirFile.exists()) {
      dirFile.mkdirs()
    }

    resolved
  }

  /** Helper to extract a typed value from the raw result map. */
  private def getStr(result: JMap[String, Object], key: String): String =
    Option(result.get(key)).map(_.toString).getOrElse("")

  private def getLong(result: JMap[String, Object], key: String): Long =
    Option(result.get(key)).map(_.asInstanceOf[java.lang.Long].longValue()).getOrElse(0L)

  private def getInt(result: JMap[String, Object], key: String): Int =
    Option(result.get(key))
      .map {
        case l: java.lang.Long    => l.intValue()
        case i: java.lang.Integer => i.intValue()
        case other                => other.toString.toInt
      }
      .getOrElse(0)

  /**
   * Build an AddAction from a raw finishAllSplitsRaw result map. For cloud tables, uploads the split file to cloud
   * storage.
   */
  @SuppressWarnings(Array("unchecked"))
  private def buildAddAction(result: JMap[String, Object]): AddAction = {
    val partitionKey = getStr(result, "partitionKey")
    val partitionValues = Option(result.get("partitionValues"))
      .map(_.asInstanceOf[JMap[String, String]].asScala.toMap)
      .getOrElse(Map.empty[String, String])
    val localSplitPath = getStr(result, "splitPath")
    val numDocs        = getLong(result, "numDocs")
    val fileName       = localSplitPath.substring(localSplitPath.lastIndexOf('/') + 1)

    // For cloud tables, upload the local split file to cloud storage and capture size from local file.
    // For local tables, get file size directly from the filesystem.
    val (finalSplitPath, splitSize) = if (isCloudTable) {
      val cloudDest = if (partitionKey.nonEmpty) {
        s"$cloudTablePath/$partitionKey/$fileName"
      } else {
        s"$cloudTablePath/$fileName"
      }
      val localFile     = new java.io.File(localSplitPath)
      val localFileSize = localFile.length()
      val cloudProvider = CloudStorageProviderFactory.createProvider(cloudDest, serializedOptions)
      try {
        val inputStream = new java.io.FileInputStream(localFile)
        try
          cloudProvider.writeFileFromStream(cloudDest, inputStream, Some(localFileSize))
        finally
          inputStream.close()
        logger.info(s"Uploaded split $fileName to $cloudDest ($localFileSize bytes)")
      } finally
        cloudProvider.close()

      // Clean up local temp file
      localFile.delete()
      (cloudDest, localFileSize)
    } else {
      val localFile = new java.io.File(localSplitPath)
      val fileSize =
        if (localFile.exists()) localFile.length()
        else {
          logger.warn(s"Could not get file size for $localSplitPath")
          0L
        }
      (localSplitPath, fileSize)
    }

    // Compute relative path for AddAction
    val addActionPath = if (partitionKey.nonEmpty) {
      s"$partitionKey/$fileName"
    } else {
      fileName
    }

    // Get native statistics from Rust (computed during addArrowBatch)
    val rawMinValues = Option(result.get("minValues"))
      .map(_.asInstanceOf[JMap[String, String]].asScala.toMap)
      .getOrElse(Map.empty[String, String])
    val rawMaxValues = Option(result.get("maxValues"))
      .map(_.asInstanceOf[JMap[String, String]].asScala.toMap)
      .getOrElse(Map.empty[String, String])

    // Apply statistics truncation (Spark-side concern — prevents long strings from bloating the transaction log)
    val (minValues, maxValues) = StatisticsTruncation.truncateStatistics(
      rawMinValues,
      rawMaxValues,
      serializedOptions
    )

    // Get docMappingJson from the native split metadata (includes fast field attributes)
    val docMappingJson = Option(getStr(result, "docMappingJson")).filter(_.nonEmpty)

    // Extract time range and tags from native result (matches TANT writer's SplitMetadata extraction)
    val timeRangeStart = Option(result.get("timeRangeStart")).map(_.toString).filter(_.nonEmpty)
    val timeRangeEnd   = Option(result.get("timeRangeEnd")).map(_.toString).filter(_.nonEmpty)
    val splitTags =
      Option(result.get("splitTags")).map(_.asInstanceOf[java.util.Set[String]]).filter(!_.isEmpty).map { tagSet =>
        import scala.jdk.CollectionConverters._
        tagSet.asScala.toSet
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
      footerStartOffset = Some(getLong(result, "footerStartOffset")),
      footerEndOffset = Some(getLong(result, "footerEndOffset")),
      hotcacheStartOffset = Some(getLong(result, "hotcacheStartOffset")),
      hotcacheLength = Some(getLong(result, "hotcacheLength")),
      hasFooterOffsets = true,
      timeRangeStart = timeRangeStart,
      timeRangeEnd = timeRangeEnd,
      splitTags = splitTags,
      deleteOpstamp = Some(getLong(result, "deleteOpstamp")),
      numMergeOps = Some(getInt(result, "numMergeOps")),
      docMappingJson = docMappingJson,
      uncompressedSizeBytes = Some(getLong(result, "uncompressedSizeBytes"))
    )
  }

  /** Clean up the local temp directory used for cloud table writes. Handles partition subdirectories. */
  private def cleanupTempDir(): Unit =
    if (isCloudTable) {
      try {
        val dir = new java.io.File(outputDir)
        if (dir.exists()) deleteRecursively(dir)
      } catch {
        case e: Exception =>
          logger.debug(s"Failed to clean up temp dir $outputDir: ${e.getMessage}")
      }
    }

  private def escapeJsonString(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"")

  private def deleteRecursively(f: java.io.File): Unit = {
    if (f.isDirectory) {
      val children = f.listFiles()
      if (children != null) children.foreach(deleteRecursively)
    }
    f.delete()
  }
}
