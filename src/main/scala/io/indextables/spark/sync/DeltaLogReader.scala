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

import io.delta.standalone.DeltaLog
import io.delta.standalone.actions.{AddFile => DeltaStandaloneAddFile, RemoveFile => DeltaStandaloneRemoveFile}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType

import org.slf4j.LoggerFactory

/** Simplified representation of a Delta AddFile action. */
case class DeltaAddFile(
  path: String,
  partitionValues: Map[String, String],
  size: Long)
    extends Serializable

/** Simplified representation of a Delta RemoveFile action. */
case class DeltaRemoveFile(
  path: String,
  partitionValues: Map[String, String])
    extends Serializable

/**
 * Wraps delta-standalone to provide a simple interface for reading Delta transaction logs.
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
  private val deltaLog = DeltaLog.forTable(hadoopConf, deltaTablePath)

  // Cache the snapshot to ensure consistent reads across currentVersion(), getAllFiles(),
  // schema(), and partitionColumns() calls (Finding #7: multiple snapshot reads)
  @volatile private var cachedSnapshot: io.delta.standalone.Snapshot = _

  private def snapshot(): io.delta.standalone.Snapshot = {
    if (cachedSnapshot == null) {
      cachedSnapshot = deltaLog.update()
    }
    cachedSnapshot
  }

  /** Get the current (latest) version of the Delta table. */
  def currentVersion(): Long = {
    val version = snapshot().getVersion
    logger.info(s"Delta table at $deltaTablePath: current version = $version")
    version
  }

  /**
   * Get files added since a specific version.
   *
   * Compares the snapshot at `sinceVersion` with the current snapshot to find
   * files that exist now but did not exist at the older version.
   *
   * @param sinceVersion
   *   The version to compare against (exclusive - files at this version are excluded)
   * @return
   *   Files present in current snapshot but not in the older snapshot
   */
  def getFilesAddedSinceVersion(sinceVersion: Long): Seq[DeltaAddFile] = {
    logger.info(s"Getting files added since version $sinceVersion")
    val oldSnapshot = deltaLog.getSnapshotForVersionAsOf(sinceVersion)
    val oldPaths = oldSnapshot.getAllFiles.asScala.map(_.getPath).toSet
    val currentFiles = getAllFiles()
    val addedFiles = currentFiles.filterNot(f => oldPaths.contains(f.path))
    logger.info(s"Files added since version $sinceVersion: ${addedFiles.size} " +
      s"(current: ${currentFiles.size}, old: ${oldPaths.size})")
    addedFiles
  }

  /** Get all AddFile actions at the current snapshot (full file listing). */
  def getAllFiles(): Seq[DeltaAddFile] = {
    val snap = snapshot()
    logger.info(s"Reading Delta snapshot at version ${snap.getVersion}")

    snap.getAllFiles.asScala.map { addFile =>
      DeltaAddFile(
        path = addFile.getPath,
        partitionValues = addFile.getPartitionValues.asScala.toMap,
        size = addFile.getSize
      )
    }.toSeq
  }

  /**
   * Get changes between two versions (for incremental sync).
   *
   * @param fromVersionExclusive
   *   Start version (exclusive) - typically lastSyncedVersion
   * @param toVersionInclusive
   *   End version (inclusive) - typically currentVersion
   * @return
   *   Tuple of (added files, removed files)
   */
  def getChanges(fromVersionExclusive: Long, toVersionInclusive: Long): (Seq[DeltaAddFile], Seq[DeltaRemoveFile]) = {
    logger.info(s"Getting Delta changes from version ${fromVersionExclusive + 1} to $toVersionInclusive")

    // Finding #2: Validate Long-to-Int conversion for delta-standalone API
    val startVersion = fromVersionExclusive + 1
    require(startVersion <= Int.MaxValue,
      s"Delta version $startVersion exceeds Int.MaxValue; delta-standalone getChanges requires int")

    val added = scala.collection.mutable.ArrayBuffer[DeltaAddFile]()
    val removed = scala.collection.mutable.ArrayBuffer[DeltaRemoveFile]()

    // delta-standalone getChanges returns an iterator of VersionLog
    val changes = deltaLog.getChanges(startVersion.toInt, false)

    // Finding #3: Use takeWhile to stop iterating past the target version
    changes.asScala.takeWhile(_.getVersion <= toVersionInclusive).foreach { versionLog =>
      versionLog.getActions.asScala.foreach {
        case addFile: DeltaStandaloneAddFile =>
          added += DeltaAddFile(
            path = addFile.getPath,
            partitionValues = addFile.getPartitionValues.asScala.toMap,
            size = addFile.getSize
          )
        case removeFile: DeltaStandaloneRemoveFile =>
          removed += DeltaRemoveFile(
            path = removeFile.getPath,
            partitionValues = Option(removeFile.getPartitionValues)
              .map(_.asScala.toMap)
              .getOrElse(Map.empty)
          )
        case _ => // Ignore other action types (Metadata, Protocol, etc.)
      }
    }

    logger.info(s"Delta changes: ${added.size} added, ${removed.size} removed")
    (added.toSeq, removed.toSeq)
  }

  /** Get partition columns from Delta table metadata. */
  def partitionColumns(): Seq[String] = {
    val metadata = snapshot().getMetadata
    metadata.getPartitionColumns.asScala.toSeq
  }

  /** Get the schema from Delta table metadata as a Spark StructType. */
  def schema(): StructType = {
    val metadata = snapshot().getMetadata
    val deltaSchema = metadata.getSchema
    // Convert delta-standalone schema to Spark StructType
    // delta-standalone StructType is compatible with Spark's
    val sparkFields = deltaSchema.getFields.toSeq.map { field =>
      org.apache.spark.sql.types.StructField(
        field.getName,
        convertDeltaType(field.getDataType),
        field.isNullable
      )
    }
    StructType(sparkFields)
  }

  private def convertDeltaType(deltaType: io.delta.standalone.types.DataType): org.apache.spark.sql.types.DataType =
    deltaType match {
      case _: io.delta.standalone.types.StringType    => org.apache.spark.sql.types.StringType
      case _: io.delta.standalone.types.LongType      => org.apache.spark.sql.types.LongType
      case _: io.delta.standalone.types.IntegerType   => org.apache.spark.sql.types.IntegerType
      case _: io.delta.standalone.types.ShortType     => org.apache.spark.sql.types.ShortType
      case _: io.delta.standalone.types.ByteType      => org.apache.spark.sql.types.ByteType
      case _: io.delta.standalone.types.FloatType     => org.apache.spark.sql.types.FloatType
      case _: io.delta.standalone.types.DoubleType    => org.apache.spark.sql.types.DoubleType
      case _: io.delta.standalone.types.BooleanType   => org.apache.spark.sql.types.BooleanType
      case _: io.delta.standalone.types.BinaryType    => org.apache.spark.sql.types.BinaryType
      case _: io.delta.standalone.types.DateType      => org.apache.spark.sql.types.DateType
      case _: io.delta.standalone.types.TimestampType => org.apache.spark.sql.types.TimestampType
      case d: io.delta.standalone.types.DecimalType =>
        org.apache.spark.sql.types.DecimalType(d.getPrecision, d.getScale)
      case a: io.delta.standalone.types.ArrayType =>
        org.apache.spark.sql.types.ArrayType(convertDeltaType(a.getElementType), a.containsNull())
      case m: io.delta.standalone.types.MapType =>
        org.apache.spark.sql.types.MapType(
          convertDeltaType(m.getKeyType),
          convertDeltaType(m.getValueType),
          m.valueContainsNull()
        )
      case s: io.delta.standalone.types.StructType =>
        val fields = s.getFields.toSeq.map { f =>
          org.apache.spark.sql.types.StructField(f.getName, convertDeltaType(f.getDataType), f.isNullable)
        }
        StructType(fields)
      case _ => org.apache.spark.sql.types.StringType // Fallback
    }
}
