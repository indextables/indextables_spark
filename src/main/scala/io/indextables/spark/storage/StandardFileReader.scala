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

package io.indextables.spark.storage

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.util.ErrorUtil
import org.slf4j.LoggerFactory

class StandardFileReader(path: Path, conf: Configuration) extends StorageStrategy {

  private val logger = LoggerFactory.getLogger(classOf[StandardFileReader])
  private val cloudProvider = CloudStorageProviderFactory.createProvider(
    path.toString,
    new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
    conf
  )

  private lazy val fileSize: Long =
    try {
      val fileInfo = cloudProvider.getFileInfo(path.toString)
      fileInfo.map(_.size).getOrElse(0L)
    } catch {
      case ex: Exception =>
        ErrorUtil.logAndThrow(logger, s"Failed to get file size for $path", ex)
    }

  override def readFile(): Array[Byte] = {
    logger.info(s"Reading entire file $path ($fileSize bytes)")

    try
      cloudProvider.readFile(path.toString)
    catch {
      case ex: Exception =>
        ErrorUtil.logAndThrow(logger, s"Failed to read file $path", ex)
    }
  }

  override def readRange(offset: Long, length: Long): Array[Byte] = {
    logger.debug(s"Reading range [$offset, ${offset + length}) from $path")

    try
      cloudProvider.readRange(path.toString, offset, length)
    catch {
      case ex: Exception =>
        ErrorUtil.logAndThrow(logger, s"Failed to read range [$offset, ${offset + length}) from $path", ex)
    }
  }

  override def getFileSize(): Long = fileSize

  override def close(): Unit =
    try {
      cloudProvider.close()
      logger.debug(s"Closed cloud storage provider for $path")
    } catch {
      case ex: Exception =>
        logger.warn(s"Error closing cloud storage provider for $path", ex)
    }
}
