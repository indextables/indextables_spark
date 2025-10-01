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

package com.tantivy4spark.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory}
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.fs.{FileStatus, Path}
import scala.collection.mutable
import org.slf4j.LoggerFactory

/**
 * Custom FileIndex implementation that uses transaction log for file discovery instead of filesystem scanning. This is
 * similar to Delta Lake's approach.
 */
// Serializable wrapper for FileStatus information
case class SerializableFileInfo(
  path: String,
  length: Long,
  modificationTime: Long,
  isDirectory: Boolean)

class TantivyFileIndex(
  @transient spark: SparkSession,
  options: Map[String, String],
  fileStatuses: Seq[FileStatus],
  userSpecifiedSchema: StructType)
    extends FileIndex {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[TantivyFileIndex])

  // Convert FileStatus to serializable form to avoid Hadoop Configuration serialization issues
  private val _fileInfos = fileStatuses.map { fs =>
    SerializableFileInfo(
      path = fs.getPath.toString,
      length = fs.getLen,
      modificationTime = fs.getModificationTime,
      isDirectory = fs.isDirectory
    )
  }.toArray

  override def listFiles(
    partitionFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression],
    dataFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression]
  ): Seq[PartitionDirectory] = {

    logger.debug(s"TantivyFileIndex.listFiles called with ${_fileInfos.length} files")

    // Convert SerializableFileInfo back to FileStatus for Spark
    val fileStatuses = _fileInfos.map { info =>
      val path = new Path(info.path)
      // Create a minimal FileStatus - some fields may be defaults
      new FileStatus(
        info.length,
        info.isDirectory,
        1,    // blockReplication - default
        4096, // blockSize - default
        info.modificationTime,
        path
      )
    }

    // Group files by partition (no partitioning for now, so all files in root partition)
    val partitionMap = mutable.Map[Map[String, String], mutable.ArrayBuffer[FileStatus]]()

    fileStatuses.foreach { fileStatus =>
      val partitionValues = Map.empty[String, String] // No partitioning
      partitionMap.getOrElseUpdate(partitionValues, mutable.ArrayBuffer.empty) += fileStatus
    }

    partitionMap.map {
      case (partitionValues, files) =>
        PartitionDirectory(
          values = org.apache.spark.sql.catalyst.InternalRow.empty, // No partition values
          files = files.toArray
        )
    }.toSeq
  }

  override def rootPaths: Seq[Path] =
    if (_fileInfos.nonEmpty) {
      _fileInfos.map(info => new Path(info.path).getParent).distinct.toSeq
    } else {
      Seq.empty
    }

  override def inputFiles: Array[String] =
    _fileInfos.map(_.path)

  override def refresh(): Unit =
    // Transaction log-based, no need to refresh from filesystem
    logger.debug("TantivyFileIndex.refresh called (no-op for transaction log)")

  override def sizeInBytes: Long =
    _fileInfos.map(_.length).sum

  override def partitionSchema: StructType =
    new StructType() // No partitioning for now
}
