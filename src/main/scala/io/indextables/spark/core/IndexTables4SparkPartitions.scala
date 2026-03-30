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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.AddAction
import org.slf4j.LoggerFactory

/** Utility for consistent path resolution across different scan types. */
object PathResolutionUtils {

  /**
   * Resolves a path from AddAction against a table path, handling absolute and relative paths correctly.
   *
   * @param splitPath
   *   Path from AddAction (could be relative like "part-00000-xxx.split" or absolute)
   * @param tablePath
   *   Base table path for resolving relative paths
   * @return
   *   Resolved Hadoop Path object
   */
  def resolveSplitPath(splitPath: String, tablePath: String): Path =
    if (isAbsolutePath(splitPath)) {
      // Already absolute path - handle file:/ URIs properly
      if (splitPath.startsWith("file:")) {
        // For file:/ URIs, use the URI directly rather than Hadoop Path constructor
        // to avoid path resolution issues
        new Path(java.net.URI.create(splitPath))
      } else {
        new Path(splitPath)
      }
    } else {
      // Relative path, resolve against table path
      new Path(tablePath, splitPath)
    }

  /**
   * Resolves a path and returns it as a string suitable for tantivy4java.
   *
   * @param splitPath
   *   Path from AddAction
   * @param tablePath
   *   Base table path for resolving relative paths
   * @return
   *   Resolved path as string
   */
  def resolveSplitPathAsString(splitPath: String, tablePath: String): String =
    if (isAbsolutePath(splitPath)) {
      // Already absolute path - handle file:/ URIs properly
      if (splitPath.startsWith("file:")) {
        // Keep file URIs as URIs for tantivy4java to avoid working directory resolution issues
        splitPath
      } else {
        splitPath
      }
    } else {
      // Relative path, resolve against table path
      // Handle case where tablePath might already be a file:/ URI to avoid double-prefixing
      if (tablePath.startsWith("file:")) {
        // Convert file URI to local path, resolve, then convert back to avoid Path constructor issues
        val tableDirPath = new java.io.File(java.net.URI.create(tablePath)).getAbsolutePath
        new java.io.File(tableDirPath, splitPath).getAbsolutePath
      } else {
        new Path(tablePath, splitPath).toString
      }
    }

  /** Checks if a path is absolute (starts with "/", contains "://" for URLs, or starts with "file:"). */
  private def isAbsolutePath(path: String): Boolean =
    path.startsWith("/") || path.contains("://") || path.startsWith("file:")
}

class IndexTables4SparkInputPartition(
  val addAction: AddAction,
  val readSchema: StructType,
  val fullTableSchema: StructType, // Full table schema for type lookup (filters may reference non-projected columns)
  val filters: Array[Filter],
  val partitionId: Int,
  val limit: Option[Int] = None,
  val indexQueryFilters: Array[Any] = Array.empty,
  val preferredHost: Option[String] = None)
    extends InputPartition {

  /**
   * Provide preferred locations for this partition based on driver-side split assignment. The preferredHost is computed
   * during partition planning using per-query load balancing while maintaining sticky assignments for cache locality.
   */
  override def preferredLocations(): Array[String] =
    preferredHost.toArray
}

/**
 * InputPartition holding multiple splits for batch processing. All splits share the same preferredHost for cache
 * locality.
 *
 * This reduces Spark scheduler overhead by processing multiple splits in a single task while honoring cache locality
 * assignments from DriverSplitLocalityManager.
 *
 * @param addActions
 *   Multiple splits to process in this partition
 * @param readSchema
 *   Schema for reading data
 * @param fullTableSchema
 *   Full table schema for type lookup (filters may reference non-projected columns)
 * @param filters
 *   Pushed-down filters to apply
 * @param partitionId
 *   Partition index for logging/debugging
 * @param limit
 *   Optional pushed-down limit
 * @param indexQueryFilters
 *   IndexQuery filters for full-text search
 * @param preferredHost
 *   Preferred host for all splits in this partition
 */
class IndexTables4SparkMultiSplitInputPartition(
  val addActions: Seq[AddAction],
  val readSchema: StructType,
  val fullTableSchema: StructType,
  val filters: Array[Filter],
  val partitionId: Int,
  val limit: Option[Int] = None,
  val indexQueryFilters: Array[Any] = Array.empty,
  val preferredHost: Option[String] = None)
    extends InputPartition {

  override def preferredLocations(): Array[String] = preferredHost.toArray
}

class IndexTables4SparkReaderFactory(
  readSchema: StructType,
  limit: Option[Int] = None,
  config: Map[String, String], // Direct config instead of broadcast
  tablePath: Path,
  metricsAccumulator: Option[io.indextables.spark.storage.BatchOptimizationMetricsAccumulator] = None)
    extends PartitionReaderFactory {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkReaderFactory])

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException(
      "Row-based reads are no longer supported. " +
        "Ensure spark.indextables.read.columnar.enabled is not set to false."
    )

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] =
    partition match {
      case multi: IndexTables4SparkMultiSplitInputPartition =>
        logger.info(
          s"Creating columnar multi-split reader for partition ${multi.partitionId} with ${multi.addActions.length} splits"
        )
        new ColumnarMultiSplitPartitionReader(
          multi.addActions,
          readSchema,
          multi.fullTableSchema,
          multi.filters,
          multi.limit.orElse(limit),
          config,
          tablePath,
          multi.indexQueryFilters,
          metricsAccumulator
        )

      case single: IndexTables4SparkInputPartition =>
        logger.info(s"Creating columnar reader for partition ${single.partitionId}")
        new ColumnarPartitionReader(
          single.addAction,
          readSchema,
          single.fullTableSchema,
          single.filters,
          single.limit.orElse(limit),
          config,
          tablePath,
          single.indexQueryFilters,
          metricsAccumulator
        )

      case other =>
        throw new IllegalArgumentException(s"Unexpected partition type for columnar read: ${other.getClass}")
    }
}

class IndexTables4SparkWriterFactory(
  tablePath: Path,
  writeSchema: StructType,
  serializedOptions: Map[String, String],
  partitionColumns: Seq[String] = Seq.empty)
    extends DataWriterFactory {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkWriterFactory])

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    logger.info(s"Creating writer for partition $partitionId, task $taskId")
    if (partitionColumns.nonEmpty) {
      logger.info(s"Creating partitioned writer with columns: ${partitionColumns.mkString(", ")}")
    }

    val arrowFfiConfig = io.indextables.spark.write.ArrowFfiWriteConfig.fromMap(serializedOptions)
    logger.info(s"Using Arrow FFI write path (batchSize=${arrowFfiConfig.batchSize})")

    new IndexTables4SparkArrowDataWriter(
      tablePath,
      writeSchema,
      partitionId,
      taskId,
      serializedOptions,
      partitionColumns,
      arrowFfiConfig
    )
  }
}

