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

package io.indextables.spark.sql

import java.sql.Timestamp

import org.apache.spark.sql.{Row, SparkSession}

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction._
import org.slf4j.LoggerFactory

/**
 * Executor for DESCRIBE INDEXTABLES TRANSACTION LOG command.
 *
 * Reads the transaction log and returns all actions as rows with detailed information.
 */
class DescribeTransactionLogExecutor(
  spark: SparkSession,
  tablePath: String,
  includeAll: Boolean) {

  private val logger = LoggerFactory.getLogger(classOf[DescribeTransactionLogExecutor])

  def execute(): Seq[Row] = {
    logger.info(s"Executing DESCRIBE TRANSACTION LOG for table: $tablePath (includeAll=$includeAll)")

    val txLog = TransactionLogFactory.create(new Path(tablePath), spark)

    try {
      val rows = if (includeAll) {
        // Read all transaction log files from version 0
        readAllVersions(txLog)
      } else {
        // Read from latest checkpoint forward (current state)
        readFromCheckpoint(txLog)
      }

      logger.info(s"Successfully read ${rows.length} actions from transaction log")
      rows
    } finally
      txLog.close()
  }

  /** Read all transaction log versions from 0 to current. */
  private def readAllVersions(txLog: TransactionLog): Seq[Row] = {
    val versions = txLog.getVersions()
    logger.info(s"Reading all versions: ${versions.length} total versions")

    val rows = scala.collection.mutable.ArrayBuffer[Row]()

    for (version <- versions.sorted) {
      val actions     = txLog.readVersion(version)
      val logFilePath = getLogFilePath(txLog, version)

      actions.foreach(action => rows += actionToRow(version, logFilePath, action, isCheckpoint = false))
    }

    rows.toSeq
  }

  /** Read from latest checkpoint forward (more efficient for large transaction logs). */
  private def readFromCheckpoint(txLog: TransactionLog): Seq[Row] = {
    val checkpointVersion = txLog.getLastCheckpointVersion()

    if (checkpointVersion.isEmpty) {
      logger.info("No checkpoint found, reading all versions")
      return readAllVersions(txLog)
    }

    logger.info(s"Reading from checkpoint version ${checkpointVersion.get} forward")

    val rows = scala.collection.mutable.ArrayBuffer[Row]()

    // Get all versions
    val allVersions = txLog.getVersions()

    // Read checkpoint version to get current state
    val checkpointActions  = txLog.readVersion(checkpointVersion.get)
    val checkpointFilePath = getCheckpointFilePath(txLog, checkpointVersion.get)
    checkpointActions.foreach { action =>
      rows += actionToRow(checkpointVersion.get, checkpointFilePath, action, isCheckpoint = true)
    }

    // Then, add entries from versions after checkpoint
    val versionsAfterCheckpoint = allVersions.filter(_ > checkpointVersion.get).sorted

    for (version <- versionsAfterCheckpoint) {
      val actions     = txLog.readVersion(version)
      val logFilePath = getLogFilePath(txLog, version)

      actions.foreach(action => rows += actionToRow(version, logFilePath, action, isCheckpoint = false))
    }

    rows.toSeq
  }

  /** Convert an Action to a Row with all fields populated. */
  private def actionToRow(
    version: Long,
    logFilePath: String,
    action: Action,
    isCheckpoint: Boolean
  ): Row = {
    action match {
      case add: AddAction =>
        Row(
          version,
          logFilePath,
          "add",
          // Common fields
          add.path,
          toJsonString(add.partitionValues),
          add.size,
          add.dataChange,
          add.tags.map(toJsonString).orNull,
          // AddAction specific fields
          new Timestamp(add.modificationTime),
          add.stats.orNull,
          add.minValues.map(toJsonString).orNull,
          add.maxValues.map(toJsonString).orNull,
          toLongOrNull(add.numRecords),
          toLongOrNull(add.footerStartOffset),
          toLongOrNull(add.footerEndOffset),
          toLongOrNull(add.hotcacheStartOffset),
          toLongOrNull(add.hotcacheLength),
          add.hasFooterOffsets,
          add.timeRangeStart.orNull,
          add.timeRangeEnd.orNull,
          add.splitTags.map(tags => toJsonString(tags.toSeq)).orNull,
          toLongOrNull(add.deleteOpstamp),
          add.numMergeOps.orNull,
          add.docMappingJson.orNull,
          add.docMappingRef.orNull,
          toLongOrNull(add.uncompressedSizeBytes),
          // RemoveAction specific fields
          null,
          null,
          // SkipAction specific fields
          null,
          null,
          null,
          null,
          null,
          // ProtocolAction specific fields
          null,
          null,
          null,
          null,
          // MetadataAction specific fields
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          // Checkpoint marker
          isCheckpoint
        )

      case remove: RemoveAction =>
        Row(
          version,
          logFilePath,
          "remove",
          // Common fields
          remove.path,
          remove.partitionValues.map(toJsonString).orNull,
          toLongOrNull(remove.size),
          remove.dataChange,
          remove.tags.map(toJsonString).orNull,
          // AddAction specific fields
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          // RemoveAction specific fields
          remove.deletionTimestamp.map(ts => new Timestamp(ts)).orNull,
          remove.extendedFileMetadata.orNull,
          // SkipAction specific fields
          null,
          null,
          null,
          null,
          null,
          // ProtocolAction specific fields
          null,
          null,
          null,
          null,
          // MetadataAction specific fields
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          // Checkpoint marker
          isCheckpoint
        )

      case skip: SkipAction =>
        Row(
          version,
          logFilePath,
          "skip",
          // Common fields
          skip.path,
          skip.partitionValues.map(toJsonString).orNull,
          toLongOrNull(skip.size),
          null, // dataChange not applicable for skip
          null, // tags not applicable for skip
          // AddAction specific fields
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          // RemoveAction specific fields
          null,
          null,
          // SkipAction specific fields
          new Timestamp(skip.skipTimestamp),
          skip.reason,
          skip.operation,
          skip.retryAfter.map(ts => new Timestamp(ts)).orNull,
          skip.skipCount,
          // ProtocolAction specific fields
          null,
          null,
          null,
          null,
          // MetadataAction specific fields
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          // Checkpoint marker
          isCheckpoint
        )

      case protocol: ProtocolAction =>
        Row(
          version,
          logFilePath,
          "protocol",
          // Common fields
          null,
          null,
          null,
          null,
          null,
          // AddAction specific fields
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          // RemoveAction specific fields
          null,
          null,
          // SkipAction specific fields
          null,
          null,
          null,
          null,
          null,
          // ProtocolAction specific fields
          protocol.minReaderVersion,
          protocol.minWriterVersion,
          protocol.readerFeatures.map(features => toJsonString(features.toSeq)).orNull,
          protocol.writerFeatures.map(features => toJsonString(features.toSeq)).orNull,
          // MetadataAction specific fields
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          // Checkpoint marker
          isCheckpoint
        )

      case metadata: MetadataAction =>
        Row(
          version,
          logFilePath,
          "metadata",
          // Common fields
          null,
          null,
          null,
          null,
          null,
          // AddAction specific fields
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          // RemoveAction specific fields
          null,
          null,
          // SkipAction specific fields
          null,
          null,
          null,
          null,
          null,
          // ProtocolAction specific fields
          null,
          null,
          null,
          null,
          // MetadataAction specific fields
          metadata.id,
          metadata.name.orNull,
          metadata.description.orNull,
          metadata.format.provider,
          toJsonString(metadata.format.options),
          metadata.schemaString,
          toJsonString(metadata.partitionColumns),
          toJsonString(metadata.configuration),
          metadata.createdTime.map(ts => new Timestamp(ts)).orNull,
          // Checkpoint marker
          isCheckpoint
        )

      case _ =>
        // Unknown action type - create a row with nulls
        logger.warn(s"Unknown action type: ${action.getClass.getName}")
        Row(
          version,
          logFilePath,
          "unknown",
          // All other fields null
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          isCheckpoint
        )
    }
  }

  /** Convert a Scala object to JSON string. */
  private def toJsonString(obj: Any): String =
    try
      io.indextables.spark.util.JsonUtil.toJson(obj)
    catch {
      case e: Exception =>
        logger.warn(s"Failed to serialize object to JSON: ${e.getMessage}")
        obj.toString
    }

  /** Convert Option[Long] to proper Java Long (handles case where JSON deserializes as Integer). */
  private def toLongOrNull(opt: Option[Long]): java.lang.Long =
    if (opt.isEmpty) {
      null
    } else {
      // Handle both Int and Long from JSON deserialization by treating as Any
      val anyValue: Any = opt.get.asInstanceOf[Any]
      anyValue match {
        case i: Int  => java.lang.Long.valueOf(i.toLong)
        case l: Long => java.lang.Long.valueOf(l)
        case other   => java.lang.Long.valueOf(other.toString.toLong)
      }
    }

  /** Get the full path to a transaction log file. */
  private def getLogFilePath(txLog: TransactionLog, version: Long): String = {
    val txLogPath   = new Path(txLog.getTablePath(), "_transaction_log")
    val versionFile = new Path(txLogPath, f"$version%020d.json")
    versionFile.toString
  }

  /**
   * Get the full path to a checkpoint file.
   *
   * For multi-part checkpoints (V3), this returns the manifest path with a note indicating the number of parts. For
   * legacy single-file checkpoints, this returns the checkpoint file path.
   */
  private def getCheckpointFilePath(txLog: TransactionLog, version: Long): String = {
    val txLogPath       = new Path(txLog.getTablePath(), "_transaction_log")
    val checkpointFile  = new Path(txLogPath, f"$version%020d.checkpoint.json")
    val basePath        = checkpointFile.toString

    // Check if this is a multi-part checkpoint
    txLog.getLastCheckpointInfo() match {
      case Some(info) if info.version == version && info.parts.isDefined =>
        val numParts = info.parts.get
        val checkpointId = info.checkpointId.getOrElse("unknown")
        s"$basePath (multi-part: $numParts parts, id=$checkpointId)"
      case _ =>
        basePath
    }
  }
}
