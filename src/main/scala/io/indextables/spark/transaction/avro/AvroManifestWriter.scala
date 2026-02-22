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

import java.io.{ByteArrayOutputStream, OutputStream}
import java.util.UUID

import scala.util.{Failure, Success, Try}

import io.indextables.spark.io.CloudStorageProvider
import io.indextables.spark.transaction.AddAction
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.slf4j.LoggerFactory

/**
 * Writer for Avro manifest files containing FileEntry records.
 *
 * This writer supports:
 *   - Writing entries with configurable compression (Zstd, Snappy, or none)
 *   - Converting AddAction to FileEntry for writing
 *   - Streaming writes for memory efficiency
 *   - Generating unique manifest IDs
 */
class AvroManifestWriter(cloudProvider: CloudStorageProvider) {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Write file entries to an Avro manifest file.
   *
   * @param manifestPath
   *   Full path to the manifest file
   * @param entries
   *   File entries to write
   * @param compression
   *   Compression codec to use (default: zstd)
   * @param compressionLevel
   *   Compression level (default: 3 for zstd)
   * @return
   *   Number of bytes written
   */
  def writeManifest(
    manifestPath: String,
    entries: Seq[FileEntry],
    compression: String = StateConfig.COMPRESSION_DEFAULT,
    compressionLevel: Int = StateConfig.COMPRESSION_LEVEL_DEFAULT
  ): Long = {

    log.debug(s"Writing Avro manifest: $manifestPath (${entries.size} entries, compression=$compression)")
    val startTime = System.currentTimeMillis()

    val bytes = writeToBytes(entries, compression, compressionLevel)
    cloudProvider.writeFile(manifestPath, bytes)

    val duration = System.currentTimeMillis() - startTime
    log.debug(s"Wrote ${entries.size} entries to $manifestPath (${bytes.length} bytes) in ${duration}ms")

    bytes.length.toLong
  }

  /**
   * Write file entries to an Avro manifest file only if it doesn't exist.
   *
   * @param manifestPath
   *   Full path to the manifest file
   * @param entries
   *   File entries to write
   * @param compression
   *   Compression codec to use
   * @param compressionLevel
   *   Compression level
   * @return
   *   Some(bytes written) if successful, None if file already exists
   */
  def writeManifestIfNotExists(
    manifestPath: String,
    entries: Seq[FileEntry],
    compression: String = StateConfig.COMPRESSION_DEFAULT,
    compressionLevel: Int = StateConfig.COMPRESSION_LEVEL_DEFAULT
  ): Option[Long] = {

    log.debug(s"Writing Avro manifest (if not exists): $manifestPath")

    val bytes   = writeToBytes(entries, compression, compressionLevel)
    val written = cloudProvider.writeFileIfNotExists(manifestPath, bytes)

    if (written) {
      log.debug(s"Wrote ${entries.size} entries to $manifestPath (${bytes.length} bytes)")
      Some(bytes.length.toLong)
    } else {
      log.debug(s"Manifest already exists: $manifestPath")
      None
    }
  }

  /**
   * Write file entries to a byte array.
   *
   * @param entries
   *   File entries to write
   * @param compression
   *   Compression codec to use
   * @param compressionLevel
   *   Compression level
   * @return
   *   Byte array containing Avro data
   */
  def writeToBytes(
    entries: Seq[FileEntry],
    compression: String = StateConfig.COMPRESSION_DEFAULT,
    compressionLevel: Int = StateConfig.COMPRESSION_LEVEL_DEFAULT
  ): Array[Byte] = {

    val baos = new ByteArrayOutputStream()
    writeToStream(entries, baos, compression, compressionLevel)
    baos.toByteArray
  }

  /**
   * Write file entries to an output stream.
   *
   * @param entries
   *   File entries to write
   * @param outputStream
   *   Output stream to write to
   * @param compression
   *   Compression codec to use
   * @param compressionLevel
   *   Compression level
   */
  def writeToStream(
    entries: Seq[FileEntry],
    outputStream: OutputStream,
    compression: String = StateConfig.COMPRESSION_DEFAULT,
    compressionLevel: Int = StateConfig.COMPRESSION_LEVEL_DEFAULT
  ): Unit = {

    val schema         = AvroSchemas.FILE_ENTRY_SCHEMA
    val datumWriter    = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)

    try {
      // Set compression codec
      val codec = compression.toLowerCase match {
        case StateConfig.Compression.ZSTD =>
          CodecFactory.zstandardCodec(compressionLevel)
        case StateConfig.Compression.SNAPPY =>
          CodecFactory.snappyCodec()
        case StateConfig.Compression.NONE | "" =>
          CodecFactory.nullCodec()
        case other =>
          log.warn(s"Unknown compression codec '$other', using zstd")
          CodecFactory.zstandardCodec(compressionLevel)
      }

      dataFileWriter.setCodec(codec)
      dataFileWriter.create(schema, outputStream)

      entries.foreach(entry => dataFileWriter.append(AvroSchemas.toGenericRecord(entry)))
    } finally
      dataFileWriter.close()
  }

  /**
   * Convert an AddAction to a FileEntry.
   *
   * @param add
   *   AddAction to convert
   * @param version
   *   Transaction version when the file was added
   * @param timestamp
   *   Timestamp when the file was added (epoch ms)
   * @return
   *   FileEntry representation
   */
  def convertFromAddAction(
    add: AddAction,
    version: Long,
    timestamp: Long
  ): FileEntry =
    FileEntry.fromAddAction(add, version, timestamp)

  /**
   * Convert multiple AddActions to FileEntries.
   *
   * @param adds
   *   AddActions to convert
   * @param version
   *   Transaction version when the files were added
   * @param timestamp
   *   Timestamp when the files were added (epoch ms)
   * @return
   *   FileEntry representations
   */
  def convertFromAddActions(
    adds: Seq[AddAction],
    version: Long,
    timestamp: Long
  ): Seq[FileEntry] =
    adds.map(add => FileEntry.fromAddAction(add, version, timestamp))

  /**
   * Generate a unique manifest ID.
   *
   * @return
   *   8-character hex string
   */
  def generateManifestId(): String =
    UUID.randomUUID().toString.replace("-", "").take(8)

  /**
   * Compute partition bounds for a set of file entries.
   *
   * @param entries
   *   File entries to analyze
   * @return
   *   Map of partition column names to their min/max bounds
   */
  def computePartitionBounds(entries: Seq[FileEntry]): Option[Map[String, PartitionBounds]] = {
    if (entries.isEmpty) {
      return None
    }

    // Get all partition columns from any entry
    val partitionColumns = entries.flatMap(_.partitionValues.keys).toSet

    if (partitionColumns.isEmpty) {
      return None
    }

    // Use numeric-aware ordering for partition bounds computation.
    // Partition values are stored as strings but may represent numbers (e.g., month=1..12).
    // Without numeric-aware ordering, lexicographic min/max would produce incorrect bounds:
    // e.g., for months 1-12, lex max would be "9" instead of "12".
    val numericAwareOrdering: Ordering[String] = (a: String, b: String) =>
      PartitionPruner.numericAwareCompare(a, b)

    val bounds = partitionColumns.map { col =>
      val values = entries.flatMap(_.partitionValues.get(col)).filter(_.nonEmpty)
      if (values.isEmpty) {
        col -> PartitionBounds(None, None) // All nulls
      } else {
        col -> PartitionBounds(Some(values.min(numericAwareOrdering)), Some(values.max(numericAwareOrdering)))
      }
    }.toMap

    Some(bounds)
  }

  /**
   * Create a ManifestInfo for a set of file entries.
   *
   * @param path
   *   Relative path to the manifest file
   * @param entries
   *   File entries in this manifest
   * @return
   *   ManifestInfo with computed bounds and version range
   */
  def createManifestInfo(path: String, entries: Seq[FileEntry]): ManifestInfo =
    if (entries.isEmpty) {
      ManifestInfo(
        path = path,
        numEntries = 0,
        minAddedAtVersion = 0,
        maxAddedAtVersion = 0,
        partitionBounds = None
      )
    } else {
      ManifestInfo(
        path = path,
        numEntries = entries.size,
        minAddedAtVersion = entries.map(_.addedAtVersion).min,
        maxAddedAtVersion = entries.map(_.addedAtVersion).max,
        partitionBounds = computePartitionBounds(entries)
      )
    }
}

object AvroManifestWriter {

  /**
   * Create a writer for the given cloud provider.
   *
   * @param cloudProvider
   *   Cloud storage provider for file access
   * @return
   *   AvroManifestWriter instance
   */
  def apply(cloudProvider: CloudStorageProvider): AvroManifestWriter =
    new AvroManifestWriter(cloudProvider)
}
