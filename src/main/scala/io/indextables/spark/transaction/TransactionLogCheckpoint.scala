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

package io.indextables.spark.transaction

import java.util.concurrent.{Executors, ThreadPoolExecutor}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProvider
import io.indextables.spark.transaction.compression.{CompressionCodec, CompressionUtils}
import io.indextables.spark.util.JsonUtil
import org.slf4j.LoggerFactory

case class CheckpointInfo(
  version: Long,
  size: Long, // Number of actions in checkpoint
  sizeInBytes: Long,
  numFiles: Long, // Number of AddActions
  createdTime: Long)

case class LastCheckpointInfo(
  version: Long,
  size: Long,
  sizeInBytes: Long,
  numFiles: Long,
  createdTime: Long)

class TransactionLogCheckpoint(
  transactionLogPath: Path,
  cloudProvider: CloudStorageProvider,
  options: org.apache.spark.sql.util.CaseInsensitiveStringMap) {

  private val logger = LoggerFactory.getLogger(classOf[TransactionLogCheckpoint])

  // Configuration matching Delta Lake capabilities but using our own parameters
  private val checkpointInterval = options.getInt("spark.indextables.checkpoint.interval", 10)
  private val maxRetries         = options.getInt("spark.indextables.checkpoint.maxRetries", 3)
  private val parallelism        = options.getInt("spark.indextables.checkpoint.parallelism", 4)

  // Retention configuration - matching Delta's log retention behavior
  private val logRetentionDuration =
    options.getLong("spark.indextables.logRetention.duration", 30 * 24 * 60 * 60 * 1000L) // 30 days in ms
  private val checkpointRetentionDuration =
    options.getLong("spark.indextables.checkpointRetention.duration", 2 * 60 * 60 * 1000L) // 2 hours in ms

  // Performance configuration
  private val checkpointReadTimeoutSeconds = options.getInt("spark.indextables.checkpoint.read.timeoutSeconds", 30)

  // Compression configuration
  private val compressionEnabled = options.getBoolean("spark.indextables.checkpoint.compression.enabled", true)
  private val compressionCodec = Option(options.get("spark.indextables.transaction.compression.codec")).getOrElse("gzip")
  private val compressionLevel = options.getInt("spark.indextables.transaction.compression.gzip.level", 6)

  private val codec: Option[CompressionCodec] =
    try
      if (compressionEnabled) CompressionUtils.getCodec(true, compressionCodec, compressionLevel) else None
    catch {
      case e: IllegalArgumentException =>
        logger.warn(
          s"Invalid compression configuration for checkpoint: ${e.getMessage}. Falling back to no compression."
        )
        None
    }

  private val executor                      = Executors.newFixedThreadPool(parallelism).asInstanceOf[ThreadPoolExecutor]
  implicit private val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

  val LAST_CHECKPOINT = new Path(transactionLogPath, "_last_checkpoint")

  def close(): Unit = {
    executor.shutdown()
    if (!executor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
      executor.shutdownNow()
    }
  }

  def shouldCreateCheckpoint(currentVersion: Long): Boolean =
    getLastCheckpointVersion() match {
      case Some(lastCheckpointVersion) =>
        (currentVersion - lastCheckpointVersion) >= checkpointInterval
      case None =>
        currentVersion >= checkpointInterval
    }

  def createCheckpoint(
    currentVersion: Long,
    allActions: Seq[Action]
  ): Unit = {
    logger.info(s"Creating checkpoint for version $currentVersion with ${allActions.length} actions")

    try {
      val checkpointPath    = new Path(transactionLogPath, f"$currentVersion%020d.checkpoint.json")
      val checkpointPathStr = checkpointPath.toString

      val content = new StringBuilder()
      allActions.foreach { action =>
        val wrappedAction = action match {
          case protocol: ProtocolAction => Map("protocol" -> protocol)
          case metadata: MetadataAction => Map("metaData" -> metadata)
          case add: AddAction           => Map("add" -> add)
          case remove: RemoveAction     => Map("remove" -> remove)
          case skip: SkipAction         => Map("mergeskip" -> skip)
        }

        val actionJson = JsonUtil.mapper.writeValueAsString(wrappedAction)
        content.append(actionJson).append("\n")
      }

      // Apply compression if enabled
      val jsonBytes       = content.toString.getBytes("UTF-8")
      val bytesToWrite    = CompressionUtils.writeTransactionFile(jsonBytes, codec)
      val compressionInfo = codec.map(c => s" (compressed with ${c.name})").getOrElse("")

      cloudProvider.writeFile(checkpointPathStr, bytesToWrite)

      val numFiles = allActions.count(_.isInstanceOf[AddAction])
      val checkpointInfo = CheckpointInfo(
        version = currentVersion,
        size = allActions.length,
        sizeInBytes = content.length,
        numFiles = numFiles,
        createdTime = System.currentTimeMillis()
      )

      writeLastCheckpointFile(checkpointInfo)

      logger.info(
        s"Successfully created checkpoint at version $currentVersion with ${allActions.length} actions$compressionInfo"
      )
    } catch {
      case e: Exception =>
        logger.error(s"Failed to create checkpoint for version $currentVersion", e)
        throw e
    }
  }

  def getActionsFromCheckpoint(): Option[Seq[Action]] =
    getLastCheckpointInfo().flatMap { info =>
      Try {
        val checkpointPath    = new Path(transactionLogPath, f"${info.version}%020d.checkpoint.json")
        val checkpointPathStr = checkpointPath.toString

        if (!cloudProvider.exists(checkpointPathStr)) {
          logger.warn(s"Checkpoint file does not exist: $checkpointPathStr")
          return None
        }

        // Read raw bytes and decompress if needed
        val rawBytes          = cloudProvider.readFile(checkpointPathStr)
        val decompressedBytes = CompressionUtils.readTransactionFile(rawBytes)
        val content           = new String(decompressedBytes, "UTF-8")
        parseActionsFromContent(content)
      } match {
        case Success(actions) => Some(actions)
        case Failure(e) =>
          logger.error("Failed to read checkpoint file", e)
          None
      }
    }

  def readVersionsInParallel(versions: Seq[Long]): Map[Long, Seq[Action]] = {
    logger.debug(s"Reading ${versions.length} versions in parallel with parallelism=$parallelism")

    val futures = versions.map { version =>
      Future {
        version -> readSingleVersion(version)
      }
    }

    val results = Future.sequence(futures)
    val timeout = Duration(checkpointReadTimeoutSeconds, java.util.concurrent.TimeUnit.SECONDS)

    try {
      val completedResults = scala.concurrent.Await.result(results, timeout)
      completedResults.toMap
    } catch {
      case e: Exception =>
        logger.error("Failed to read versions in parallel", e)
        throw e
    }
  }

  private def readSingleVersion(version: Long): Seq[Action] = {
    val versionFile     = new Path(transactionLogPath, f"$version%020d.json")
    val versionFilePath = versionFile.toString

    if (!cloudProvider.exists(versionFilePath)) {
      return Seq.empty
    }

    var retries = 0
    while (retries <= maxRetries)
      try {
        // Read raw bytes and decompress if needed
        val rawBytes          = cloudProvider.readFile(versionFilePath)
        val decompressedBytes = CompressionUtils.readTransactionFile(rawBytes)
        val content           = new String(decompressedBytes, "UTF-8")
        return parseActionsFromContent(content)
      } catch {
        case e: Exception =>
          retries += 1
          if (retries > maxRetries) {
            logger.error(s"Failed to read version $version after $maxRetries retries", e)
            throw e
          } else {
            logger.warn(s"Retry $retries for version $version due to: ${e.getMessage}")
            Thread.sleep(100 * retries) // Exponential backoff
          }
      }

    Seq.empty // Should never reach here
  }

  private def parseActionsFromContent(content: String): Seq[Action] = {
    val lines = content.split("\n").filter(_.nonEmpty)

    lines.map { line =>
      val jsonNode = JsonUtil.mapper.readTree(line)

      if (jsonNode.has("protocol")) {
        val protocolNode = jsonNode.get("protocol")
        JsonUtil.mapper.readValue(protocolNode.toString, classOf[ProtocolAction])
      } else if (jsonNode.has("metaData")) {
        val metadataNode = jsonNode.get("metaData")
        JsonUtil.mapper.readValue(metadataNode.toString, classOf[MetadataAction])
      } else if (jsonNode.has("add")) {
        val addNode = jsonNode.get("add")
        JsonUtil.mapper.readValue(addNode.toString, classOf[AddAction])
      } else if (jsonNode.has("remove")) {
        val removeNode = jsonNode.get("remove")
        JsonUtil.mapper.readValue(removeNode.toString, classOf[RemoveAction])
      } else {
        throw new IllegalArgumentException(s"Unknown action type in line: $line")
      }
    }.toSeq
  }

  private def writeLastCheckpointFile(checkpointInfo: CheckpointInfo): Unit = {
    val lastCheckpointInfo = LastCheckpointInfo(
      version = checkpointInfo.version,
      size = checkpointInfo.size,
      sizeInBytes = checkpointInfo.sizeInBytes,
      numFiles = checkpointInfo.numFiles,
      createdTime = checkpointInfo.createdTime
    )

    val json = JsonUtil.mapper.writeValueAsString(lastCheckpointInfo)
    cloudProvider.writeFile(LAST_CHECKPOINT.toString, json.getBytes("UTF-8"))
  }

  def getLastCheckpointInfo(): Option[LastCheckpointInfo] = {
    if (!cloudProvider.exists(LAST_CHECKPOINT.toString)) {
      return None
    }

    Try {
      val content = new String(cloudProvider.readFile(LAST_CHECKPOINT.toString), "UTF-8")
      JsonUtil.mapper.readValue(content, classOf[LastCheckpointInfo])
    } match {
      case Success(info) => Some(info)
      case Failure(e) =>
        logger.warn("Failed to read last checkpoint info", e)
        None
    }
  }

  def getLastCheckpointVersion(): Option[Long] =
    getLastCheckpointInfo().map(_.version)

  def cleanupOldVersions(currentVersion: Long): Unit =
    getLastCheckpointVersion() match {
      case Some(checkpointVersion) if checkpointVersion <= currentVersion =>
        val currentTime = System.currentTimeMillis()

        // Clean up old log files based on retention policy
        val versionsToCheck = (0L until currentVersion).filter(_ < checkpointVersion)
        var deletedCount    = 0

        versionsToCheck.foreach { version =>
          val versionFile     = new Path(transactionLogPath, f"$version%020d.json")
          val versionFilePath = versionFile.toString

          if (cloudProvider.exists(versionFilePath)) {
            try {
              val fileInfo = cloudProvider
                .listFiles(transactionLogPath.toString, recursive = false)
                .find(f => new Path(f.path).getName == f"$version%020d.json")

              fileInfo match {
                case Some(info) =>
                  val fileAge = currentTime - info.modificationTime
                  if (fileAge > logRetentionDuration && version < checkpointVersion) {
                    cloudProvider.deleteFile(versionFilePath)
                    deletedCount += 1
                    logger.debug(s"Deleted old version file: $versionFilePath (age: ${fileAge / 1000}s)")
                  }
                case None =>
                  logger.debug(s"Could not get file info for $versionFilePath")
              }
            } catch {
              case e: Exception =>
                logger.warn(s"Failed to process version file $versionFilePath", e)
            }
          }
        }

        if (deletedCount > 0) {
          logger.info(s"Cleaned up $deletedCount old transaction log files (retention: ${logRetentionDuration / 1000}s)")
        }

        // Also clean up old checkpoints based on checkpoint retention
        cleanupOldCheckpoints(currentTime)

      case _ =>
        logger.debug("No cleanup needed - no checkpoint available or checkpoint is current")
    }

  private def cleanupOldCheckpoints(currentTime: Long): Unit =
    try {
      val files           = cloudProvider.listFiles(transactionLogPath.toString, recursive = false)
      val checkpointFiles = files.filter(_.path.contains(".checkpoint.json"))

      var deletedCheckpoints = 0
      checkpointFiles.foreach { file =>
        val fileAge = currentTime - file.modificationTime
        if (fileAge > checkpointRetentionDuration) {
          // Keep at least the most recent checkpoint
          val fileName = new Path(file.path).getName
          if (!getLastCheckpointInfo().exists(info => fileName.startsWith(f"${info.version}%020d"))) {
            try {
              cloudProvider.deleteFile(file.path)
              deletedCheckpoints += 1
              logger.debug(s"Deleted old checkpoint file: ${file.path} (age: ${fileAge / 1000}s)")
            } catch {
              case e: Exception =>
                logger.warn(s"Failed to delete old checkpoint file ${file.path}", e)
            }
          }
        }
      }

      if (deletedCheckpoints > 0) {
        logger.info(
          s"Cleaned up $deletedCheckpoints old checkpoint files (retention: ${checkpointRetentionDuration / 1000}s)"
        )
      }
    } catch {
      case e: Exception =>
        logger.warn("Failed during checkpoint cleanup", e)
    }
}
