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

package io.indextables.spark.io

import java.io.{InputStream, OutputStream}

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.slf4j.LoggerFactory

/**
 * Hadoop-based storage provider for local, HDFS, and other Hadoop-compatible filesystems. Falls back to standard Hadoop
 * APIs when cloud-specific optimizations aren't available.
 */
class HadoopCloudStorageProvider(hadoopConf: Configuration) extends CloudStorageProvider {

  private val logger = LoggerFactory.getLogger(classOf[HadoopCloudStorageProvider])

  override def listFiles(path: String, recursive: Boolean = false): Seq[CloudFileInfo] = {
    val hadoopPath = new Path(path)

    try {
      val fs = hadoopPath.getFileSystem(hadoopConf)

      val statuses = fs.listStatus(hadoopPath)
      statuses.map(convertFileStatus).toSeq
    } catch {
      case ex: java.io.FileNotFoundException =>
        logger.debug(s"Directory does not exist (normal when creating new table): $path")
        Seq.empty
      case ex: Exception =>
        logger.error(s"Failed to list files at path: $path", ex)
        Seq.empty
    }
  }

  override def exists(path: String): Boolean = {
    val hadoopPath = new Path(path)

    try {
      val fs = hadoopPath.getFileSystem(hadoopConf)
      fs.exists(hadoopPath)
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to check if file exists: $path", ex)
        false
    }
  }

  override def getFileInfo(path: String): Option[CloudFileInfo] = {
    val hadoopPath = new Path(path)

    try {
      val fs = hadoopPath.getFileSystem(hadoopConf)
      if (fs.exists(hadoopPath)) {
        val status = fs.getFileStatus(hadoopPath)
        Some(convertFileStatus(status))
      } else {
        None
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to get file info: $path", ex)
        None
    }
  }

  override def readFile(path: String): Array[Byte] = {
    val hadoopPath = new Path(path)

    try {
      val fs    = hadoopPath.getFileSystem(hadoopConf)
      val input = fs.open(hadoopPath)

      try {
        val fileStatus = fs.getFileStatus(hadoopPath)
        val buffer     = new Array[Byte](fileStatus.getLen.toInt)
        input.readFully(buffer)
        buffer
      } finally
        input.close()
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to read file: $path", ex)
        throw new RuntimeException(s"Failed to read file: ${ex.getMessage}", ex)
    }
  }

  override def readRange(
    path: String,
    offset: Long,
    length: Long
  ): Array[Byte] = {
    val hadoopPath = new Path(path)

    try {
      val fs    = hadoopPath.getFileSystem(hadoopConf)
      val input = fs.open(hadoopPath)

      try {
        input.seek(offset)
        val buffer = new Array[Byte](length.toInt)
        input.readFully(buffer)
        buffer
      } finally
        input.close()
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to read range from file: $path, offset=$offset, length=$length", ex)
        throw new RuntimeException(s"Failed to read file range: ${ex.getMessage}", ex)
    }
  }

  override def openInputStream(path: String): InputStream = {
    val hadoopPath = new Path(path)

    try {
      val fs = hadoopPath.getFileSystem(hadoopConf)
      fs.open(hadoopPath)
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to open input stream: $path", ex)
        throw new RuntimeException(s"Failed to open input stream: ${ex.getMessage}", ex)
    }
  }

  override def createOutputStream(path: String): OutputStream = {
    val hadoopPath = new Path(path)

    try {
      val fs = hadoopPath.getFileSystem(hadoopConf)
      fs.create(hadoopPath, true) // overwrite=true
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to create output stream: $path", ex)
        throw new RuntimeException(s"Failed to create output stream: ${ex.getMessage}", ex)
    }
  }

  override def writeFile(path: String, content: Array[Byte]): Unit = {
    val output = createOutputStream(path)

    try
      output.write(content)
    finally
      output.close()
  }

  override def writeFileIfNotExists(path: String, content: Array[Byte]): Boolean = {
    val hadoopPath = new Path(path)

    try {
      val fs = hadoopPath.getFileSystem(hadoopConf)

      // Check if file already exists
      if (fs.exists(hadoopPath)) {
        logger.warn(s"⚠️  Conditional write failed - file already exists: $path")
        return false // File already exists
      }

      // File doesn't exist, create it atomically with overwrite=false
      logger.info(s"🔒 HADOOP CONDITIONAL WRITE - Path: $path")
      logger.info(s"🔒 HADOOP CONDITIONAL WRITE - Will only write if file does not exist")

      val output = fs.create(hadoopPath, false) // overwrite=false ensures atomic create
      try
        output.write(content)
      finally
        output.close()

      logger.info(s"✅ Successfully wrote file (conditional): $path")
      true // File was written successfully

    } catch {
      case _: org.apache.hadoop.fs.FileAlreadyExistsException =>
        // Race condition: file was created between exists check and create
        logger.warn(s"⚠️  Conditional write failed - file created by concurrent writer: $path")
        false

      case ex: Exception =>
        logger.error(s"❌ Failed conditional write: $path", ex)
        throw new RuntimeException(s"Failed conditional write: ${ex.getMessage}", ex)
    }
  }

  override def writeFileFromStream(
    path: String,
    inputStream: InputStream,
    contentLength: Option[Long] = None
  ): Unit = {
    val output = createOutputStream(path)

    try {
      logger.info(s"🔧 HADOOP STREAMING WRITE - Path: $path")
      contentLength.foreach(length =>
        logger.info(s"🔧 HADOOP STREAMING WRITE - Content length: ${length / (1024 * 1024)} MB")
      )

      // Use a buffer to stream data efficiently without loading everything into memory
      val buffer            = new Array[Byte](64 * 1024) // 64KB buffer
      var totalBytesWritten = 0L
      var bytesRead         = 0

      while ({ bytesRead = inputStream.read(buffer); bytesRead > 0 }) {
        output.write(buffer, 0, bytesRead)
        totalBytesWritten += bytesRead
      }

      logger.info(s"✅ Hadoop streaming write completed: $path (${totalBytesWritten / (1024 * 1024)} MB)")
    } finally
      output.close()
  }

  override def deleteFile(path: String): Boolean = {
    val hadoopPath = new Path(path)

    try {
      val fs = hadoopPath.getFileSystem(hadoopConf)
      fs.delete(hadoopPath, false) // recursive=false
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to delete file: $path", ex)
        false
    }
  }

  override def createDirectory(path: String): Boolean = {
    val hadoopPath = new Path(path)

    try {
      val fs = hadoopPath.getFileSystem(hadoopConf)
      fs.mkdirs(hadoopPath)
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to create directory: $path", ex)
        false
    }
  }

  override def readFilesParallel(paths: Seq[String]): Map[String, Array[Byte]] =
    // For Hadoop, just read files sequentially as filesystem may not support high concurrency
    paths
      .map { path =>
        try
          path -> readFile(path)
        catch {
          case ex: Exception =>
            logger.error(s"Failed to read Hadoop file: $path", ex)
            path -> Array.empty[Byte]
        }
      }
      .toMap
      .filter(_._2.nonEmpty)

  override def existsParallel(paths: Seq[String]): Map[String, Boolean] =
    paths.map(path => path -> exists(path)).toMap

  override def getProviderType: String = "hadoop"

  override def normalizePathForTantivy(path: String): String =
    // Hadoop provider doesn't need protocol conversion, return path as-is
    path

  override def close(): Unit =
    // Hadoop FileSystem instances are cached and managed by Hadoop
    logger.debug("Closed Hadoop storage provider")

  private def convertFileStatus(status: FileStatus): CloudFileInfo =
    CloudFileInfo(
      path = status.getPath.toString,
      size = status.getLen,
      modificationTime = status.getModificationTime,
      isDirectory = status.isDirectory
    )
}
