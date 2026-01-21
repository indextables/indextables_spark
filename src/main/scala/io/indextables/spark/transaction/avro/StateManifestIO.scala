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

  private val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

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
      protocolVersion = root.path("protocolVersion").asInt(4)
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
        partitionBounds = parsePartitionBounds(elem.get("partitionBounds"))
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
}

object StateManifestIO {

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
