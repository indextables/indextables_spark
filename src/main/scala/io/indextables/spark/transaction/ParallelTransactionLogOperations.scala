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

import java.io.{BufferedReader, InputStreamReader}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProvider
import io.indextables.spark.transaction.compression.CompressionUtils
import io.indextables.spark.util.JsonUtil
import org.slf4j.LoggerFactory

/**
 * Parallel operations enhancement for transaction log operations. Provides optimized parallel reading, writing, and
 * listing operations.
 */
class ParallelTransactionLogOperations(
  transactionLogPath: Path,
  cloudProvider: CloudStorageProvider,
  spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(classOf[ParallelTransactionLogOperations])

  // Thread pools from centralized manager
  private val fileListingPool  = TransactionLogThreadPools.fileListingThreadPool
  private val parallelReadPool = TransactionLogThreadPools.parallelReadThreadPool
  private val commitPool       = TransactionLogThreadPools.commitThreadPool

  // Execution contexts (not implicit to avoid ambiguity)
  private val fileListingEc: ExecutionContext  = fileListingPool.executionContext
  private val parallelReadEc: ExecutionContext = parallelReadPool.executionContext
  private val commitEc: ExecutionContext       = commitPool.executionContext

  /** List files in parallel with reconciliation between file system and uncommitted changes */
  def listFilesParallel(
    startVersion: Option[Long] = None,
    endVersion: Option[Long] = None
  ): Future[FileListingResult] = {

    val fsListingFuture = Future {
      listFromFileSystem(startVersion, endVersion)
    }(fileListingEc)

    val checkpointListingFuture = Future {
      listCheckpoints()
    }(fileListingEc)

    val unbackfilledCommitsFuture = Future {
      listUnbackfilledCommits()
    }(fileListingEc)

    // Combine all listings
    implicit val ec: ExecutionContext = fileListingEc
    for {
      fsFiles             <- fsListingFuture
      checkpoints         <- checkpointListingFuture
      unbackfilledCommits <- unbackfilledCommitsFuture
    } yield reconcileListings(fsFiles, checkpoints, unbackfilledCommits)
  }

  /** Read multiple versions in parallel */
  def readVersionsParallel(
    versions: Seq[Long],
    maxConcurrency: Int = 8
  ): Map[Long, Seq[Action]] = {

    if (versions.isEmpty) {
      return Map.empty
    }

    val results  = new ConcurrentHashMap[Long, Seq[Action]]()
    val promises = versions.map(v => v -> Promise[Seq[Action]]()).toMap

    // Process versions in batches to control concurrency
    versions.grouped(maxConcurrency).foreach { batch =>
      val futures = batch.map { version =>
        parallelReadPool.submitSimple {
          try {
            val actions = readVersionDirect(version)
            results.put(version, actions)
            promises(version).success(actions)
            version -> actions
          } catch {
            case e: Exception =>
              logger.warn(s"Failed to read version $version", e)
              promises(version).failure(e)
              version -> Seq.empty[Action]
          }
        }
      }

      // Wait for batch completion before starting next batch
      Try(scala.concurrent.Await.result(Future.sequence(futures)(collection.breakOut, parallelReadEc), 30.seconds))
    }

    results.asScala.toMap
  }

  /** Optimized state reconstruction with partitioned processing */
  def reconstructStateParallel(
    versions: Seq[Long],
    partitions: Int = 4
  ): Seq[AddAction] = {

    // Read all versions in parallel but process them in order
    val sortedVersions = versions.sorted

    // Read all versions in parallel
    val versionToActions = new ConcurrentHashMap[Long, Seq[Action]]()

    val futures = sortedVersions.map { version =>
      parallelReadPool.submitSimple {
        val actions = readVersionDirect(version)
        versionToActions.put(version, actions)
        version -> actions
      }
    }

    // Wait for all reads to complete
    Try(scala.concurrent.Await.result(Future.sequence(futures)(collection.breakOut, parallelReadEc), 60.seconds))

    // Now process actions in version order to maintain correct state
    val files = scala.collection.mutable.HashMap[String, AddAction]()

    sortedVersions.foreach { version =>
      val actions = versionToActions.get(version)
      if (actions != null) {
        logger.debug(s" Processing version $version with ${actions.size} actions")
        actions.foreach { action =>
          action match {
            case add: AddAction =>
              files(add.path) = add
              logger.debug(s"   Added: ${add.path}")
            case remove: RemoveAction =>
              val removed = files.remove(remove.path)
              logger.debug(s"   Removed: ${remove.path}, was present: ${removed.isDefined}")
            case _ => // Ignore other actions
          }
        }
      }
    }

    files.values.toSeq
  }

  /** Parallel file existence check */
  def checkFilesExistParallel(paths: Seq[String]): Map[String, Boolean] = {
    if (paths.isEmpty) {
      return Map.empty
    }

    val results = new ConcurrentHashMap[String, Boolean]()

    val futures = paths
      .grouped(10)
      .map { batch =>
        fileListingPool.submitSimple {
          batch.foreach(path => results.put(path, cloudProvider.exists(path)))
        }
      }
      .toSeq

    Try(scala.concurrent.Await.result(Future.sequence(futures)(collection.breakOut, fileListingEc), 30.seconds))

    results.asScala.toMap
  }

  /** Parallel checkpoint creation with streaming */
  def createCheckpointParallel(
    version: Long,
    actions: Seq[Action],
    partSize: Int = 10000
  ): Future[CheckpointInfo] =
    commitPool.submit(spark) {
      val checkpointPath = new Path(transactionLogPath, f"$version%020d.checkpoint.json")
      val parts          = actions.grouped(partSize).toSeq

      if (parts.length == 1) {
        // Single part - use streaming write to avoid OOM
        StreamingActionWriter.writeActionsStreaming(
          actions = actions,
          cloudProvider = cloudProvider,
          path = checkpointPath.toString,
          codec = None, // Parallel ops don't use compression currently
          ifNotExists = false
        )
      } else {
        // Multi-part checkpoint for large tables - use streaming write for each part
        val partFutures = parts.zipWithIndex.map {
          case (part, index) =>
            Future {
              val partPath = new Path(transactionLogPath, f"$version%020d.checkpoint.part.$index%05d.json")
              StreamingActionWriter.writeActionsStreaming(
                actions = part,
                cloudProvider = cloudProvider,
                path = partPath.toString,
                codec = None,
                ifNotExists = false
              )
            }(commitEc)
        }

        scala.concurrent.Await.result(Future.sequence(partFutures)(collection.breakOut, commitEc), 60.seconds)

        // Write manifest
        val manifest        = CheckpointManifest(version, parts.length, actions.length)
        val manifestContent = JsonUtil.mapper.writeValueAsString(manifest)
        cloudProvider.writeFile(checkpointPath.toString, manifestContent.getBytes("UTF-8"))
      }

      CheckpointInfo(
        version = version,
        size = actions.length,
        sizeInBytes = StreamingActionWriter.estimateSerializedSize(actions),
        numFiles = actions.count(_.isInstanceOf[AddAction]),
        createdTime = System.currentTimeMillis()
      )
    }

  // Helper methods

  private def listFromFileSystem(
    startVersion: Option[Long],
    endVersion: Option[Long]
  ): Seq[TransactionFile] = {
    val prefix = transactionLogPath.toString
    val files  = cloudProvider.listFiles(prefix, recursive = false)
    logger.debug(s" CloudProvider listed ${files.size} files in $prefix: ${files.map(_.path).mkString(", ")}")

    files
      .flatMap { file =>
        parseTransactionFile(file.path) match {
          case Some(tf) =>
            val inRange = startVersion.map(tf.version >= _).getOrElse(true) &&
              endVersion.map(tf.version <= _).getOrElse(true)
            if (inRange) Some(tf) else None
          case None => None
        }
      }
      .sortBy(_.version)
  }

  private def listCheckpoints(): Seq[CheckpointFile] = {
    val prefix = transactionLogPath.toString
    val files  = cloudProvider.listFiles(prefix, recursive = false)

    files
      .flatMap { file =>
        if (file.path.contains(".checkpoint.")) {
          parseCheckpointFile(file.path)
        } else None
      }
      .sortBy(_.version)
  }

  private def listUnbackfilledCommits(): Seq[UnbackfilledCommit] =
    // This would integrate with a commit coordinator if available
    // For now, return empty as we don't have unbackfilled commits
    Seq.empty

  private def reconcileListings(
    fsFiles: Seq[TransactionFile],
    checkpoints: Seq[CheckpointFile],
    unbackfilledCommits: Seq[UnbackfilledCommit]
  ): FileListingResult = {

    // Find gaps in version sequence
    val allVersions = (fsFiles.map(_.version) ++
      checkpoints.map(_.version) ++
      unbackfilledCommits.map(_.version)).distinct.sorted

    val gaps = findVersionGaps(allVersions)

    FileListingResult(
      transactionFiles = fsFiles,
      checkpoints = checkpoints,
      unbackfilledCommits = unbackfilledCommits,
      gaps = gaps,
      latestVersion = allVersions.lastOption.getOrElse(-1L)
    )
  }

  private def findVersionGaps(versions: Seq[Long]): Seq[VersionGap] = {
    if (versions.isEmpty) return Seq.empty

    val gaps            = ListBuffer[VersionGap]()
    var expectedVersion = versions.head

    versions.foreach { version =>
      if (version > expectedVersion) {
        gaps += VersionGap(expectedVersion, version - 1)
      }
      expectedVersion = version + 1
    }

    gaps.toSeq
  }

  private def readVersionDirect(version: Long): Seq[Action] = {
    val versionFile     = new Path(transactionLogPath, f"$version%020d.json")
    val versionFilePath = versionFile.toString

    if (!cloudProvider.exists(versionFilePath)) {
      return Seq.empty
    }

    Try {
      // Use full streaming: cloud storage -> decompression -> line parsing
      // This avoids OOM for large version files (>1GB)
      parseActionsFromStream(versionFilePath)
    }.getOrElse(Seq.empty)
  }

  /**
   * Parse actions directly from a cloud storage file using full streaming.
   *
   * This method provides the most memory-efficient parsing by streaming data from cloud storage directly through
   * decompression and into line-by-line parsing, without ever loading the entire file into memory.
   *
   * Flow: CloudStorage InputStream -> Decompressing InputStream -> BufferedReader -> Line parsing
   */
  private def parseActionsFromStream(filePath: String): Seq[Action] = {
    val rawStream           = cloudProvider.openInputStream(filePath)
    val decompressingStream = CompressionUtils.createDecompressingInputStream(rawStream)
    val reader              = new BufferedReader(new InputStreamReader(decompressingStream, "UTF-8"))
    val actions             = ListBuffer[Action]()

    try {
      var line = reader.readLine()
      while (line != null) {
        if (line.nonEmpty) {
          Try {
            val jsonNode = JsonUtil.mapper.readTree(line)

            // Use treeToValue instead of toString + readValue to avoid re-serializing large JSON nodes (OOM fix)
            if (jsonNode.has("metaData")) {
              Some(JsonUtil.mapper.treeToValue(jsonNode.get("metaData"), classOf[MetadataAction]))
            } else if (jsonNode.has("add")) {
              Some(JsonUtil.mapper.treeToValue(jsonNode.get("add"), classOf[AddAction]))
            } else if (jsonNode.has("remove")) {
              Some(JsonUtil.mapper.treeToValue(jsonNode.get("remove"), classOf[RemoveAction]))
            } else if (jsonNode.has("mergeskip")) {
              Some(JsonUtil.mapper.treeToValue(jsonNode.get("mergeskip"), classOf[SkipAction]))
            } else {
              None
            }
          }.toOption.flatten.foreach(actions += _)
        }
        line = reader.readLine()
      }
      actions.toSeq
    } finally
      reader.close()
  }

  // Use treeToValue instead of toString + readValue to avoid re-serializing large JSON nodes (OOM fix)
  private def parseActionsFromContent(content: String): Seq[Action] =
    content
      .split("\n")
      .filter(_.nonEmpty)
      .flatMap { line =>
        Try {
          val jsonNode = JsonUtil.mapper.readTree(line)

          if (jsonNode.has("metaData")) {
            Some(JsonUtil.mapper.treeToValue(jsonNode.get("metaData"), classOf[MetadataAction]))
          } else if (jsonNode.has("add")) {
            Some(JsonUtil.mapper.treeToValue(jsonNode.get("add"), classOf[AddAction]))
          } else if (jsonNode.has("remove")) {
            Some(JsonUtil.mapper.treeToValue(jsonNode.get("remove"), classOf[RemoveAction]))
          } else if (jsonNode.has("mergeskip")) {
            Some(JsonUtil.mapper.treeToValue(jsonNode.get("mergeskip"), classOf[SkipAction]))
          } else {
            None
          }
        }.toOption
      }
      .flatten
      .toSeq

  private def parseTransactionFile(path: String): Option[TransactionFile] = {
    val fileName = new Path(path).getName
    if (fileName.endsWith(".json") && !fileName.contains(".checkpoint.")) {
      Try {
        val versionStr = fileName.replace(".json", "").replaceAll("_.*", "")
        TransactionFile(versionStr.toLong, path)
      }.toOption
    } else None
  }

  private def parseCheckpointFile(path: String): Option[CheckpointFile] = {
    val fileName = new Path(path).getName
    if (fileName.contains(".checkpoint.")) {
      Try {
        val versionStr = fileName.split("\\.").head
        CheckpointFile(versionStr.toLong, path, isMultiPart = fileName.contains(".part."))
      }.toOption
    } else None
  }
}

// Data structures

case class TransactionFile(version: Long, path: String)
case class CheckpointFile(
  version: Long,
  path: String,
  isMultiPart: Boolean)
case class UnbackfilledCommit(version: Long, actions: Seq[Action])
case class VersionGap(startVersion: Long, endVersion: Long)

case class FileListingResult(
  transactionFiles: Seq[TransactionFile],
  checkpoints: Seq[CheckpointFile],
  unbackfilledCommits: Seq[UnbackfilledCommit],
  gaps: Seq[VersionGap],
  latestVersion: Long)

case class CheckpointManifest(
  version: Long,
  numParts: Int,
  totalActions: Long)
