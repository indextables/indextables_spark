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

package io.indextables.spark.sync

import scala.jdk.CollectionConverters._

import io.delta.kernel.{Table, Snapshot}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.InternalScanFileUtils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType

import org.slf4j.LoggerFactory

/** Simplified representation of a Delta AddFile action. */
case class DeltaAddFile(
  path: String,
  partitionValues: Map[String, String],
  size: Long)
    extends Serializable

/**
 * Wraps delta-kernel to provide a simple interface for reading Delta transaction logs.
 *
 * This class reads the Delta `_delta_log/` directory directly via Hadoop FileSystem, without
 * requiring a SparkSession on the reader side.
 *
 * @param deltaTablePath
 *   Path to the Delta table root (containing `_delta_log/`)
 * @param hadoopConf
 *   Hadoop configuration for filesystem access
 */
class DeltaLogReader(deltaTablePath: String, hadoopConf: Configuration) {
  private val logger = LoggerFactory.getLogger(classOf[DeltaLogReader])
  private val engine: Engine = DefaultEngine.create(hadoopConf)
  private val table: Table = Table.forPath(engine, deltaTablePath)

  @volatile private var cachedSnapshot: Snapshot = _

  private def snapshot(): Snapshot = {
    if (cachedSnapshot == null) {
      cachedSnapshot = table.getLatestSnapshot(engine)
    }
    cachedSnapshot
  }

  /** Get the current (latest) version of the Delta table. */
  def currentVersion(): Long = {
    val version = snapshot().getVersion(engine)
    logger.info(s"Delta table at $deltaTablePath: current version = $version")
    version
  }

  /** Get all AddFile actions at the current snapshot (full file listing). */
  def getAllFiles(): Seq[DeltaAddFile] = {
    val snap = snapshot()
    logger.info(s"Reading Delta snapshot at version ${snap.getVersion(engine)}")

    val files = scala.collection.mutable.ArrayBuffer[DeltaAddFile]()
    val scanFileIter = snap.getScanBuilder(engine).build().getScanFiles(engine)

    try {
      while (scanFileIter.hasNext) {
        val batch = scanFileIter.next()
        val rows = batch.getRows
        try {
          while (rows.hasNext) {
            val scanFileRow = rows.next()
            val fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow)
            val partitionValues = InternalScanFileUtils.getPartitionValues(scanFileRow)

            // delta-kernel returns absolute paths; make relative to table root
            val relativePath = makeRelativePath(fileStatus.getPath)

            files += DeltaAddFile(
              path = relativePath,
              partitionValues = partitionValues.asScala.toMap,
              size = fileStatus.getSize
            )
          }
        } finally {
          rows.close()
        }
      }
    } finally {
      scanFileIter.close()
    }

    logger.info(s"Delta snapshot contains ${files.size} files")
    files.toSeq
  }

  /** Get partition columns from Delta table metadata. */
  def partitionColumns(): Seq[String] =
    snapshot().getPartitionColumnNames(engine).asScala.toSeq

  /** Get the schema from Delta table metadata as a Spark StructType. */
  def schema(): StructType = {
    val kernelSchema = snapshot().getSchema(engine)
    val sparkFields = kernelSchema.fields().asScala.toSeq.map { field =>
      org.apache.spark.sql.types.StructField(
        field.getName,
        convertKernelType(field.getDataType),
        field.isNullable
      )
    }
    StructType(sparkFields)
  }

  /**
   * Strip the table root prefix from an absolute path returned by delta-kernel,
   * producing a relative path consistent with delta-standalone behavior.
   */
  private def makeRelativePath(absolutePath: String): String = {
    // Decode both paths for consistent comparison
    val decodedAbsolute = java.net.URI.create(absolutePath).getPath
    val decodedRoot = try {
      java.net.URI.create(deltaTablePath).getPath
    } catch {
      case _: Exception => deltaTablePath
    }

    val rootSuffix = if (decodedRoot.endsWith("/")) decodedRoot else decodedRoot + "/"
    if (decodedAbsolute.startsWith(rootSuffix)) {
      decodedAbsolute.stripPrefix(rootSuffix)
    } else {
      // If the absolute path uses a different scheme/authority (e.g., file: vs s3:),
      // try stripping just the path component
      val rootPath = new org.apache.hadoop.fs.Path(deltaTablePath)
      val absPath = new org.apache.hadoop.fs.Path(absolutePath)
      // If same filesystem, make relative
      if (absPath.toString.startsWith(rootPath.toString.stripSuffix("/") + "/")) {
        absPath.toString.stripPrefix(rootPath.toString.stripSuffix("/") + "/")
      } else {
        // Return absolute path as-is (caller handles both relative and absolute)
        absolutePath
      }
    }
  }

  private def convertKernelType(
    kernelType: io.delta.kernel.types.DataType
  ): org.apache.spark.sql.types.DataType =
    kernelType match {
      case _: io.delta.kernel.types.StringType    => org.apache.spark.sql.types.StringType
      case _: io.delta.kernel.types.LongType      => org.apache.spark.sql.types.LongType
      case _: io.delta.kernel.types.IntegerType   => org.apache.spark.sql.types.IntegerType
      case _: io.delta.kernel.types.ShortType     => org.apache.spark.sql.types.ShortType
      case _: io.delta.kernel.types.ByteType      => org.apache.spark.sql.types.ByteType
      case _: io.delta.kernel.types.FloatType     => org.apache.spark.sql.types.FloatType
      case _: io.delta.kernel.types.DoubleType    => org.apache.spark.sql.types.DoubleType
      case _: io.delta.kernel.types.BooleanType   => org.apache.spark.sql.types.BooleanType
      case _: io.delta.kernel.types.BinaryType    => org.apache.spark.sql.types.BinaryType
      case _: io.delta.kernel.types.DateType      => org.apache.spark.sql.types.DateType
      case _: io.delta.kernel.types.TimestampType => org.apache.spark.sql.types.TimestampType
      case _: io.delta.kernel.types.TimestampNTZType =>
        // TimestampNTZ maps to Spark's TimestampNTZType (Spark 3.4+)
        org.apache.spark.sql.types.TimestampNTZType
      case d: io.delta.kernel.types.DecimalType =>
        org.apache.spark.sql.types.DecimalType(d.getPrecision, d.getScale)
      case a: io.delta.kernel.types.ArrayType =>
        org.apache.spark.sql.types.ArrayType(
          convertKernelType(a.getElementType), a.containsNull())
      case m: io.delta.kernel.types.MapType =>
        org.apache.spark.sql.types.MapType(
          convertKernelType(m.getKeyType),
          convertKernelType(m.getValueType),
          m.isValueContainsNull)
      case s: io.delta.kernel.types.StructType =>
        val fields = s.fields().asScala.toSeq.map { f =>
          org.apache.spark.sql.types.StructField(
            f.getName, convertKernelType(f.getDataType), f.isNullable)
        }
        StructType(fields)
      case _ => org.apache.spark.sql.types.StringType // Fallback
    }
}
