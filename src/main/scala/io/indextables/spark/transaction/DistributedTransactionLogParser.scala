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

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.util.JsonUtil
import org.slf4j.LoggerFactory

/**
 * Wrapper to associate an action with its transaction version.
 * This is critical for maintaining correct transaction order during distributed processing.
 *
 * @param action The transaction log action
 * @param version The transaction version this action belongs to
 */
case class VersionedAction(action: Action, version: Long) extends Serializable

/**
 * Executor-side transaction log file parser.
 * Runs on executors to parse individual transaction files in parallel.
 *
 * This object is serializable and can be distributed to Spark executors.
 * Each executor maintains its own local cache of parsed transaction files
 * to avoid redundant S3 reads across multiple queries or partitions.
 */
object DistributedTransactionLogParser extends Serializable {

  // Use lazy val for logger to ensure it's created on each executor
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * Parse a single transaction file on an executor with version tracking.
   * Uses executor-local cache for repeated reads.
   *
   * @param fileRef Transaction file reference with metadata
   * @param tablePathStr Table path as string (e.g., "s3://bucket/table")
   * @param config Serializable configuration map for cloud provider
   * @return Sequence of versioned actions parsed from the transaction file
   */
  def parseTransactionFile(
    fileRef: TransactionFileRef,
    tablePathStr: String,
    config: Map[String, String]
  ): Seq[VersionedAction] = {
    // Construct cache key using table path and transaction file path
    val cacheKey = s"$tablePathStr/_transaction_log/${fileRef.path}"

    // Check executor-local cache first
    TransactionFileCache.get(cacheKey) match {
      case Some(cachedActions) =>
        logger.debug(s"Executor cache hit for transaction file: ${fileRef.path} (version ${fileRef.version})")
        // Wrap cached actions with version information
        cachedActions.map(action => VersionedAction(action, fileRef.version))

      case None =>
        // Cache miss - read and parse on executor
        logger.debug(s"Reading transaction file ${fileRef.path} on executor (version ${fileRef.version})")

        val actions = readAndParseFile(fileRef, tablePathStr, config)

        // Cache on executor for future reads (cache raw actions)
        TransactionFileCache.put(cacheKey, actions)
        logger.debug(s"Cached transaction file ${fileRef.path} with ${actions.length} actions")

        // Return versioned actions
        actions.map(action => VersionedAction(action, fileRef.version))
    }
  }

  /**
   * Read and parse a transaction file from storage.
   * Creates a cloud provider instance for this executor.
   *
   * @param fileRef Transaction file reference
   * @param tablePathStr Table path string
   * @param config Serializable configuration
   * @return Parsed actions
   */
  private def readAndParseFile(
    fileRef: TransactionFileRef,
    tablePathStr: String,
    config: Map[String, String]
  ): Seq[Action] = {
    // Create executor-local cloud provider
    // Note: We pass null for Hadoop config since all needed config is in the map
    val options = new CaseInsensitiveStringMap(config.asJava)
    val cloudProvider = CloudStorageProviderFactory.createProvider(
      tablePathStr,
      options,
      null // Hadoop config not needed - all config is in options map
    )

    try {
      // Construct full path to transaction file
      val transactionFilePath = s"$tablePathStr/_transaction_log/${fileRef.path}"

      // Read file content as string
      val content = new String(cloudProvider.readFile(transactionFilePath), "UTF-8")

      // Parse JSON lines to Action objects
      parseActions(content, fileRef.version)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to read/parse transaction file ${fileRef.path} (version ${fileRef.version})", e)
        // Return empty sequence on error to allow processing to continue
        // The reduce phase will handle missing data appropriately
        Seq.empty
    } finally {
      cloudProvider.close()
    }
  }

  /**
   * Parse JSON content into Action objects.
   * Follows Delta Lake JSON format - one action per line.
   *
   * @param jsonContent JSON content with one action per line
   * @param version Transaction version for error reporting
   * @return Sequence of parsed actions
   */
  private def parseActions(jsonContent: String, version: Long): Seq[Action] = {
    val lines = jsonContent.split("\n").filter(_.trim.nonEmpty)

    lines.flatMap { line =>
      Try {
        val jsonNode = JsonUtil.mapper.readTree(line)

        // Determine action type and parse accordingly
        if (jsonNode.has("add")) {
          Some(JsonUtil.mapper.treeToValue(jsonNode.get("add"), classOf[AddAction]))
        } else if (jsonNode.has("remove")) {
          Some(JsonUtil.mapper.treeToValue(jsonNode.get("remove"), classOf[RemoveAction]))
        } else if (jsonNode.has("metaData")) {
          Some(JsonUtil.mapper.treeToValue(jsonNode.get("metaData"), classOf[MetadataAction]))
        } else if (jsonNode.has("protocol")) {
          Some(JsonUtil.mapper.treeToValue(jsonNode.get("protocol"), classOf[ProtocolAction]))
        } else if (jsonNode.has("skip")) {
          Some(JsonUtil.mapper.treeToValue(jsonNode.get("skip"), classOf[SkipAction]))
        } else {
          // Unknown action type - ignore
          logger.warn(s"Ignoring unknown action type in version $version: ${line.take(100)}")
          None
        }
      } match {
        case Success(actionOpt) => actionOpt
        case Failure(e) =>
          logger.error(s"Failed to parse action in version $version: ${line.take(100)}", e)
          None
      }
    }.toSeq
  }

  /**
   * Batch parse multiple transaction files on an executor.
   * More efficient than parsing files individually.
   *
   * @param fileRefs Sequence of transaction file references
   * @param tablePathStr Table path string
   * @param config Serializable configuration
   * @return Map of version to versioned actions
   */
  def parseTransactionFiles(
    fileRefs: Seq[TransactionFileRef],
    tablePathStr: String,
    config: Map[String, String]
  ): Map[Long, Seq[VersionedAction]] = {
    logger.debug(s"Batch parsing ${fileRefs.length} transaction files on executor")

    fileRefs.map { fileRef =>
      val versionedActions = parseTransactionFile(fileRef, tablePathStr, config)
      fileRef.version -> versionedActions
    }.toMap
  }

  /**
   * Get cache statistics for this executor.
   * Useful for monitoring cache effectiveness.
   *
   * @return Cache statistics string
   */
  def getCacheStats(): String = TransactionFileCache.stats()

  /**
   * Get cache hit rate for this executor.
   *
   * @return Hit rate as a double (0.0 to 1.0)
   */
  def getCacheHitRate(): Double = TransactionFileCache.hitRate()
}
