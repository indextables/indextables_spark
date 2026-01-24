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

package io.indextables.spark.transaction.avro

import io.indextables.spark.io.CloudStorageProvider
import io.indextables.spark.transaction.EnhancedTransactionLogCache

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
 * IO operations for the state manifest (`_manifest.json`) file.
 *
 * The state manifest is a small JSON file that describes the complete table state, including references to Avro manifest
 * files and tombstones for removed files.
 */
class StateManifestIO(cloudProvider: CloudStorageProvider) {

  private val log = LoggerFactory.getLogger(getClass)

  // Use shared ObjectMapper from companion object - Jackson ObjectMapper creation is expensive
  private val mapper: ObjectMapper = StateManifestIO.sharedMapper

  /** Manifest file name within a state directory */
  val MANIFEST_FILE_NAME = "_manifest.json"

  /**
   * Read the state manifest from a state directory.
   *
   * @param stateDir
   *   Full path to state directory (e.g., "s3://bucket/table/_transaction_log/state-v00000000000000000100")
   * @return
   *   Parsed StateManifest
   */
  def readStateManifest(stateDir: String): StateManifest = {
    val manifestPath = s"$stateDir/$MANIFEST_FILE_NAME"
    log.debug(s"Reading state manifest: $manifestPath")

    // Increment counter for testing/monitoring - tracks actual cloud reads
    StateManifestIO.incrementReadCounter()

    Try {
      val content = cloudProvider.readFile(manifestPath)
      val json = new String(content, "UTF-8")
      parseStateManifest(json)
    } match {
      case Success(manifest) =>
        log.debug(
          s"Read state manifest: version=${manifest.stateVersion}, " +
            s"files=${manifest.numFiles}, manifests=${manifest.manifests.size}, " +
            s"tombstones=${manifest.tombstones.size}")
        manifest
      case Failure(e) =>
        log.error(s"Failed to read state manifest: $manifestPath", e)
        throw new RuntimeException(s"Failed to read state manifest: $manifestPath", e)
    }
  }

  /**
   * Parse a state manifest from JSON string.
   *
   * @param json
   *   JSON string
   * @return
   *   Parsed StateManifest
   */
  def parseStateManifest(json: String): StateManifest = {
    // Track actual JSON parse calls for testing/monitoring
    StateManifestIO.incrementParseCounter()
    EnhancedTransactionLogCache.incrementGlobalJsonParseCounter()
    val root = mapper.readTree(json)

    StateManifest(
      formatVersion = root.get("formatVersion").asInt(1),
      stateVersion = root.get("stateVersion").asLong(),
      createdAt = root.get("createdAt").asLong(),
      numFiles = root.get("numFiles").asLong(),
      totalBytes = root.get("totalBytes").asLong(),
      manifests = parseManifestInfoList(root.get("manifests")),
      tombstones = parseTombstones(root.get("tombstones")),
      schemaRegistry = parseSchemaRegistry(root.get("schemaRegistry")),
      protocolVersion = root.path("protocolVersion").asInt(4),
      metadata = Option(root.get("metadata")).filterNot(_.isNull).map(_.asText())
    )
  }

  private def parseManifestInfoList(node: JsonNode): Seq[ManifestInfo] = {
    if (node == null || node.isNull) {
      return Seq.empty
    }

    node.elements().asScala.map { elem =>
      ManifestInfo(
        path = elem.get("path").asText(),
        numEntries = elem.get("numEntries").asLong(),
        minAddedAtVersion = elem.get("minAddedAtVersion").asLong(),
        maxAddedAtVersion = elem.get("maxAddedAtVersion").asLong(),
        partitionBounds = parsePartitionBounds(elem.get("partitionBounds")),
        tombstoneCount = Option(elem.get("tombstoneCount")).map(_.asLong()).getOrElse(0L),
        liveEntryCount = Option(elem.get("liveEntryCount")).map(_.asLong()).getOrElse(-1L)
      )
    }.toSeq
  }

  private def parsePartitionBounds(node: JsonNode): Option[Map[String, PartitionBounds]] = {
    if (node == null || node.isNull) {
      return None
    }

    val bounds = node.fields().asScala.map { entry =>
      val column = entry.getKey
      val boundsNode = entry.getValue
      val min = Option(boundsNode.get("min")).filterNot(_.isNull).map(_.asText())
      val max = Option(boundsNode.get("max")).filterNot(_.isNull).map(_.asText())
      column -> PartitionBounds(min, max)
    }.toMap

    if (bounds.isEmpty) None else Some(bounds)
  }

  private def parseTombstones(node: JsonNode): Seq[String] = {
    if (node == null || node.isNull) {
      return Seq.empty
    }

    node.elements().asScala.map(_.asText()).toSeq
  }

  private def parseSchemaRegistry(node: JsonNode): Map[String, String] = {
    if (node == null || node.isNull) {
      return Map.empty
    }

    node.fields().asScala.map { entry =>
      entry.getKey -> entry.getValue.asText()
    }.toMap
  }

  /**
   * Write a state manifest to a state directory.
   *
   * @param stateDir
   *   Full path to state directory
   * @param manifest
   *   StateManifest to write
   */
  def writeStateManifest(stateDir: String, manifest: StateManifest): Unit = {
    val manifestPath = s"$stateDir/$MANIFEST_FILE_NAME"
    log.debug(s"Writing state manifest: $manifestPath")

    val json = serializeStateManifest(manifest)
    cloudProvider.writeFile(manifestPath, json.getBytes("UTF-8"))

    log.debug(
      s"Wrote state manifest: version=${manifest.stateVersion}, " +
        s"files=${manifest.numFiles}, manifests=${manifest.manifests.size}")
  }

  /**
   * Write a state manifest only if it doesn't already exist.
   *
   * @param stateDir
   *   Full path to state directory
   * @param manifest
   *   StateManifest to write
   * @return
   *   true if written, false if already exists
   */
  def writeStateManifestIfNotExists(stateDir: String, manifest: StateManifest): Boolean = {
    val manifestPath = s"$stateDir/$MANIFEST_FILE_NAME"
    log.debug(s"Writing state manifest (if not exists): $manifestPath")

    val json = serializeStateManifest(manifest)
    val written = cloudProvider.writeFileIfNotExists(manifestPath, json.getBytes("UTF-8"))

    if (written) {
      log.debug(
        s"Wrote state manifest: version=${manifest.stateVersion}, " +
          s"files=${manifest.numFiles}, manifests=${manifest.manifests.size}")
    } else {
      log.debug(s"State manifest already exists: $manifestPath")
    }

    written
  }

  /**
   * Serialize a StateManifest to JSON string.
   *
   * @param manifest
   *   StateManifest to serialize
   * @return
   *   JSON string
   */
  def serializeStateManifest(manifest: StateManifest): String = {
    val root = mapper.createObjectNode()

    root.put("formatVersion", manifest.formatVersion)
    root.put("stateVersion", manifest.stateVersion)
    root.put("createdAt", manifest.createdAt)
    root.put("numFiles", manifest.numFiles)
    root.put("totalBytes", manifest.totalBytes)

    // Manifests array
    val manifestsArray = root.putArray("manifests")
    manifest.manifests.foreach { info =>
      val manifestNode = manifestsArray.addObject()
      manifestNode.put("path", info.path)
      manifestNode.put("numEntries", info.numEntries)
      manifestNode.put("minAddedAtVersion", info.minAddedAtVersion)
      manifestNode.put("maxAddedAtVersion", info.maxAddedAtVersion)

      info.partitionBounds.foreach { bounds =>
        val boundsNode = manifestNode.putObject("partitionBounds")
        bounds.foreach { case (column, pb) =>
          val colNode = boundsNode.putObject(column)
          pb.min.foreach(colNode.put("min", _))
          pb.max.foreach(colNode.put("max", _))
        }
      }

      // Only write tombstone tracking fields if they have meaningful values
      if (info.tombstoneCount > 0) {
        manifestNode.put("tombstoneCount", info.tombstoneCount)
      }
      if (info.liveEntryCount >= 0) {
        manifestNode.put("liveEntryCount", info.liveEntryCount)
      }
    }

    // Tombstones array
    if (manifest.tombstones.nonEmpty) {
      val tombstonesArray = root.putArray("tombstones")
      manifest.tombstones.foreach(tombstonesArray.add)
    }

    // Schema registry
    if (manifest.schemaRegistry.nonEmpty) {
      val registryNode = root.putObject("schemaRegistry")
      manifest.schemaRegistry.foreach { case (key, value) =>
        registryNode.put(key, value)
      }
    }

    root.put("protocolVersion", manifest.protocolVersion)

    // Metadata (JSON-encoded MetadataAction for fast getMetadata)
    manifest.metadata.foreach(root.put("metadata", _))

    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root)
  }

  /**
   * Apply tombstones to filter out removed entries.
   *
   * @param entries
   *   All file entries from manifests
   * @param tombstones
   *   Paths of removed files
   * @return
   *   Live file entries (not in tombstone set)
   */
  def applyTombstones(entries: Seq[FileEntry], tombstones: Seq[String]): Seq[FileEntry] = {
    if (tombstones.isEmpty) {
      entries
    } else {
      val tombstoneSet = tombstones.toSet
      entries.filterNot(e => tombstoneSet.contains(e.path))
    }
  }

  /**
   * Check if a state directory exists.
   *
   * @param stateDir
   *   Full path to state directory
   * @return
   *   true if state directory exists (contains _manifest.json)
   */
  def stateExists(stateDir: String): Boolean = {
    cloudProvider.exists(s"$stateDir/$MANIFEST_FILE_NAME")
  }

  /**
   * Format a version number as a state directory name.
   *
   * @param version
   *   Transaction version
   * @return
   *   State directory name (e.g., "state-v00000000000000000100")
   */
  def formatStateDir(version: Long): String = {
    f"state-v$version%020d"
  }

  /**
   * Extract version number from a state directory name.
   *
   * @param stateDir
   *   State directory name (e.g., "state-v00000000000000000100")
   * @return
   *   Transaction version, or None if not a valid state directory name
   */
  def parseStateDirVersion(stateDir: String): Option[Long] = {
    val pattern = "state-v(\\d{20})".r
    val dirName = stateDir.split("/").last
    dirName match {
      case pattern(version) => Some(version.toLong)
      case _                => None
    }
  }

  /**
   * Resolve a manifest path to a full path.
   *
   * Manifest paths are stored relative to the transaction log root:
   *   - Shared manifests: "manifests/manifest-abc123.avro"
   *   - Legacy state-local manifests: "manifest-abc123.avro" (relative to state dir)
   *
   * This method handles both formats for backward compatibility.
   *
   * @param manifestInfo
   *   ManifestInfo containing the relative path
   * @param transactionLogPath
   *   Full path to the transaction log directory
   * @param stateDir
   *   Full path to the state directory (for legacy state-local manifests)
   * @return
   *   Full path to the manifest file
   */
  def resolveManifestPath(
      manifestInfo: ManifestInfo,
      transactionLogPath: String,
      stateDir: String): String = {
    val path = manifestInfo.path

    if (path.startsWith(StateConfig.SHARED_MANIFEST_DIR + "/")) {
      // New format: shared manifest relative to tx log root
      s"$transactionLogPath/$path"
    } else if (path.startsWith("state-v")) {
      // Normalized legacy format: includes state directory prefix
      // e.g., "state-v00000001/manifest-xxx.avro"
      s"$transactionLogPath/$path"
    } else if (path.startsWith("s3://") || path.startsWith("abfss://") ||
               path.startsWith("wasbs://") || path.startsWith("gs://") ||
               path.startsWith("hdfs://") || path.startsWith("/")) {
      // Absolute path (shouldn't happen, but handle defensively)
      log.warn(s"Manifest has absolute path (unexpected): $path")
      path
    } else {
      // Legacy format: relative to current state directory
      // This should only happen when reading the original state that created the manifest
      s"$stateDir/$path"
    }
  }

  /**
   * Resolve manifest paths for a state manifest.
   *
   * @param manifest
   *   StateManifest containing manifest references
   * @param transactionLogPath
   *   Full path to the transaction log directory
   * @param stateDir
   *   Full path to the state directory
   * @return
   *   Sequence of full paths to manifest files
   */
  def resolveManifestPaths(
      manifest: StateManifest,
      transactionLogPath: String,
      stateDir: String): Seq[String] = {
    manifest.manifests.map(m => resolveManifestPath(m, transactionLogPath, stateDir))
  }

  /**
   * Read current _last_checkpoint info from JSON file.
   *
   * @param transactionLogPath
   *   Path to the transaction log directory
   * @return
   *   Current LastCheckpointInfo if file exists and is valid, None otherwise
   */
  def readLastCheckpointJson(transactionLogPath: String): Option[JsonNode] = {
    val lastCheckpointPath = s"$transactionLogPath/_last_checkpoint"

    if (!cloudProvider.exists(lastCheckpointPath)) {
      return None
    }

    Try {
      val content = new String(cloudProvider.readFile(lastCheckpointPath), "UTF-8")
      EnhancedTransactionLogCache.incrementGlobalJsonParseCounter()
      mapper.readTree(content)
    } match {
      case Success(json) => Some(json)
      case Failure(e) =>
        log.warn(s"Failed to read _last_checkpoint: ${e.getMessage}")
        None
    }
  }

  /**
   * Get the current checkpoint version from _last_checkpoint file.
   *
   * @param transactionLogPath
   *   Path to the transaction log directory
   * @return
   *   Current version if file exists and is valid, None otherwise
   */
  def getCurrentCheckpointVersion(transactionLogPath: String): Option[Long] = {
    readLastCheckpointJson(transactionLogPath).flatMap { json =>
      val versionNode = json.get("version")
      if (versionNode != null && !versionNode.isNull) {
        Some(versionNode.asLong())
      } else {
        None
      }
    }
  }

  /**
   * Write _last_checkpoint only if the new version is greater than the existing version.
   *
   * This provides a form of version-based conflict resolution for _last_checkpoint updates.
   * If a concurrent writer has already advanced the checkpoint to a higher version, this
   * write will be skipped (which is the correct behavior - we don't want to regress).
   *
   * @param transactionLogPath
   *   Path to the transaction log directory
   * @param newVersion
   *   Version of the new checkpoint
   * @param lastCheckpointJson
   *   JSON content to write
   * @return
   *   true if written (new version was >= existing), false if skipped (existing version was > new)
   */
  def writeLastCheckpointIfNewer(
      transactionLogPath: String,
      newVersion: Long,
      lastCheckpointJson: String): Boolean = {

    val lastCheckpointPath = s"$transactionLogPath/_last_checkpoint"

    // Check current version (if exists)
    val currentVersion = getCurrentCheckpointVersion(transactionLogPath)

    currentVersion match {
      case Some(existingVersion) if existingVersion > newVersion =>
        // Only skip if existing version is strictly greater (newer)
        // Allow same version updates to support format upgrades (JSON -> Avro)
        log.info(s"Skipping _last_checkpoint update: existing version $existingVersion > new version $newVersion")
        false

      case _ =>
        // Either no existing file, existing version is lower, or same version (format upgrade)
        cloudProvider.writeFile(lastCheckpointPath, lastCheckpointJson.getBytes("UTF-8"))
        log.debug(s"Updated _last_checkpoint to version $newVersion")
        true
    }
  }

  /**
   * Verify a checkpoint version hint by probing for version N+1.
   *
   * This uses the monotonically incrementing version property: if version N+1 exists,
   * then the hint is stale. We keep probing N+2, N+3, etc. until we find a version
   * that doesn't exist - that's the true latest.
   *
   * This is much cheaper than a directory scan:
   * - Common case (hint is correct): 1 existence check (HEAD request)
   * - Rare regression case: O(k) existence checks where k is typically 1-2
   *
   * @param transactionLogPath
   *   Path to the transaction log directory
   * @param hintVersion
   *   Version from `_last_checkpoint` to verify
   * @return
   *   The actual latest version (>= hintVersion)
   */
  def verifyCheckpointVersion(transactionLogPath: String, hintVersion: Long): Long = {
    var version = hintVersion
    var nextStateDir = s"$transactionLogPath/${formatStateDir(version + 1)}"

    while (stateExists(nextStateDir)) {
      version += 1
      log.info(s"_last_checkpoint regression detected: found state at version $version")
      nextStateDir = s"$transactionLogPath/${formatStateDir(version + 1)}"
    }

    if (version > hintVersion) {
      log.warn(s"_last_checkpoint was stale: hint=$hintVersion, actual=$version")
    }

    version
  }
}

object StateManifestIO {

  // Instrumentation counter for testing - tracks actual cloud reads of state manifests
  private val readCounter = new java.util.concurrent.atomic.AtomicLong(0)

  // Instrumentation counter for testing - tracks actual JSON parses of state manifests
  private val parseCounter = new java.util.concurrent.atomic.AtomicLong(0)

  /** Get the number of times readStateManifest has actually read from cloud storage (for testing) */
  def getReadCount(): Long = readCounter.get()

  /** Reset the read counter (for testing) */
  def resetReadCounter(): Unit = readCounter.set(0)

  /** Increment the read counter (called by instance readStateManifest) */
  private[avro] def incrementReadCounter(): Unit = readCounter.incrementAndGet()

  /** Get the number of times parseStateManifest has actually parsed JSON (for testing) */
  def getParseCount(): Long = parseCounter.get()

  /** Reset the parse counter (for testing) */
  def resetParseCounter(): Unit = parseCounter.set(0)

  /** Increment the parse counter (called by instance parseStateManifest) */
  private[avro] def incrementParseCounter(): Unit = parseCounter.incrementAndGet()

  /**
   * Shared ObjectMapper for JSON parsing.
   * Jackson ObjectMapper creation is expensive (class loading, module registration),
   * so we share a single instance across all StateManifestIO instances.
   * ObjectMapper is thread-safe for read operations after configuration.
   */
  private[avro] lazy val sharedMapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  /**
   * Create a StateManifestIO for the given cloud provider.
   *
   * @param cloudProvider
   *   Cloud storage provider for file access
   * @return
   *   StateManifestIO instance
   */
  def apply(cloudProvider: CloudStorageProvider): StateManifestIO = {
    new StateManifestIO(cloudProvider)
  }
}
