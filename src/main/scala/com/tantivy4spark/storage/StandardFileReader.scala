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


package com.tantivy4spark.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FSDataInputStream}
import org.slf4j.LoggerFactory
import com.tantivy4spark.util.ErrorUtil
import java.io.IOException

class StandardFileReader(path: Path, conf: Configuration) extends StorageStrategy {
  
  private val logger = LoggerFactory.getLogger(classOf[StandardFileReader])
  private val fs = path.getFileSystem(conf)
  private var inputStream: FSDataInputStream = _
  
  private lazy val fileSize: Long = {
    try {
      val status = fs.getFileStatus(path)
      status.getLen
    } catch {
      case ex: Exception =>
        ErrorUtil.logAndThrow(logger, s"Failed to get file size for $path", ex)
    }
  }

  private def ensureInputStream(): FSDataInputStream = {
    if (inputStream == null) {
      try {
        inputStream = fs.open(path)
        logger.debug(s"Opened input stream for $path")
      } catch {
        case ex: Exception =>
          ErrorUtil.logAndThrow(logger, s"Failed to open input stream for $path", ex)
      }
    }
    inputStream
  }

  override def readFile(): Array[Byte] = {
    logger.info(s"Reading entire file $path (${fileSize} bytes)")
    
    val stream = ensureInputStream()
    val buffer = new Array[Byte](fileSize.toInt)
    
    try {
      stream.seek(0)
      var totalRead = 0
      var bytesRead = 0
      
      while (totalRead < fileSize && bytesRead != -1) {
        bytesRead = stream.read(buffer, totalRead, fileSize.toInt - totalRead)
        if (bytesRead > 0) {
          totalRead += bytesRead
        }
      }
      
      if (totalRead != fileSize) {
        throw new IOException(s"Expected to read $fileSize bytes, but read $totalRead bytes")
      }
      
      buffer
    } catch {
      case ex: Exception =>
        ErrorUtil.logAndThrow(logger, s"Failed to read file $path", ex)
    }
  }

  override def readRange(offset: Long, length: Long): Array[Byte] = {
    logger.debug(s"Reading range [$offset, ${offset + length}) from $path")
    
    val stream = ensureInputStream()
    val buffer = new Array[Byte](length.toInt)
    
    try {
      stream.seek(offset)
      var totalRead = 0
      var bytesRead = 0
      
      while (totalRead < length && bytesRead != -1) {
        bytesRead = stream.read(buffer, totalRead, length.toInt - totalRead)
        if (bytesRead > 0) {
          totalRead += bytesRead
        }
      }
      
      if (totalRead < length) {
        // Handle case where we've reached EOF before reading all requested bytes
        val actualBuffer = new Array[Byte](totalRead)
        System.arraycopy(buffer, 0, actualBuffer, 0, totalRead)
        actualBuffer
      } else {
        buffer
      }
    } catch {
      case ex: Exception =>
        ErrorUtil.logAndThrow(logger, s"Failed to read range [$offset, ${offset + length}) from $path", ex)
    }
  }

  override def getFileSize(): Long = fileSize

  override def close(): Unit = {
    if (inputStream != null) {
      try {
        inputStream.close()
        logger.debug(s"Closed input stream for $path")
      } catch {
        case ex: Exception =>
          logger.warn(s"Error closing input stream for $path", ex)
      } finally {
        inputStream = null
      }
    }
  }
}