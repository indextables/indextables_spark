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

/**
 * Protocol version constants for IndexTables4Spark transaction log.
 *
 * Version History:
 *   - Version 1: Basic transaction log (AddAction, RemoveAction, MetadataAction)
 *   - Version 2: Version 1 + SkipAction, extended AddAction metadata, footer offsets
 *   - Version 3: Version 2 + Multi-part checkpoints, schema deduplication (docMappingRef)
 *   - Version 4: Version 3 + Avro state files (avroState feature)
 */
object ProtocolVersion {

  // Current versions supported by this release
  val CURRENT_READER_VERSION = 4
  val CURRENT_WRITER_VERSION = 4

  // Minimum versions we can read/write
  val MIN_READER_VERSION = 1
  val MIN_WRITER_VERSION = 1

  // Feature support (for version 3+)
  val SUPPORTED_READER_FEATURES: Set[String] = Set(
    "skippedFiles",        // SkipAction support
    "extendedMetadata",    // Extended AddAction metadata
    "footerOffsets",       // Footer offset optimization
    "multiPartCheckpoint", // Multi-part checkpoint files with UUID manifest
    "schemaDeduplication", // docMappingRef-based schema deduplication
    "avroState"            // Avro state files (V4)
  )

  val SUPPORTED_WRITER_FEATURES: Set[String] = Set(
    "skippedFiles",        // SkipAction support
    "extendedMetadata",    // Extended AddAction metadata
    "footerOffsets",       // Footer offset optimization
    "checkpoint",          // Checkpoint support
    "optimizeWrite",       // Optimized write operations
    "multiPartCheckpoint", // Multi-part checkpoint files with UUID manifest
    "schemaDeduplication", // docMappingRef-based schema deduplication
    "avroState"            // Avro state files (V4)
  )

  // Feature flags introduced in V3
  val FEATURE_MULTI_PART_CHECKPOINT = "multiPartCheckpoint"
  val FEATURE_SCHEMA_DEDUPLICATION  = "schemaDeduplication"

  // Feature flag introduced in V4
  val FEATURE_AVRO_STATE = "avroState"

  /** Features that require V3 reader */
  val V3_READER_FEATURES: Set[String] = Set(
    FEATURE_MULTI_PART_CHECKPOINT,
    FEATURE_SCHEMA_DEDUPLICATION
  )

  /** Features that require V4 reader */
  val V4_READER_FEATURES: Set[String] = Set(
    FEATURE_AVRO_STATE
  )

  /** Check if a feature requires V3 reader */
  def requiresV3Reader(feature: String): Boolean = V3_READER_FEATURES.contains(feature)

  /** Check if a feature requires V4 reader */
  def requiresV4Reader(feature: String): Boolean = V4_READER_FEATURES.contains(feature)

  // Configuration keys
  val PROTOCOL_CHECK_ENABLED          = "spark.indextables.protocol.checkEnabled"
  val PROTOCOL_AUTO_UPGRADE           = "spark.indextables.protocol.autoUpgrade"
  val PROTOCOL_ENFORCE_READER_VERSION = "spark.indextables.protocol.enforceReaderVersion"
  val PROTOCOL_ENFORCE_WRITER_VERSION = "spark.indextables.protocol.enforceWriterVersion"

  /** Check if a reader version is supported by the current system. */
  def isReaderVersionSupported(version: Int): Boolean =
    version >= MIN_READER_VERSION && version <= CURRENT_READER_VERSION

  /** Check if a writer version is supported by the current system. */
  def isWriterVersionSupported(version: Int): Boolean =
    version >= MIN_WRITER_VERSION && version <= CURRENT_WRITER_VERSION

  /** Get the default protocol for new tables. */
  def defaultProtocol(): ProtocolAction =
    ProtocolAction(
      minReaderVersion = CURRENT_READER_VERSION,
      minWriterVersion = CURRENT_WRITER_VERSION
    )

  /** Get the legacy protocol for tables without explicit protocol. */
  def legacyProtocol(): ProtocolAction =
    ProtocolAction(
      minReaderVersion = 1,
      minWriterVersion = 1
    )

  /** Get V2 protocol (for backward compatibility when V3 features not used). */
  def v2Protocol(): ProtocolAction =
    ProtocolAction(
      minReaderVersion = 2,
      minWriterVersion = 2
    )

  /**
   * Check if a table uses V3 features based on its protocol.
   *
   * @param protocol
   *   The table's protocol action
   * @return
   *   true if the table requires V3 reader
   */
  def usesV3Features(protocol: ProtocolAction): Boolean =
    protocol.minReaderVersion >= 3

  /**
   * Validate that the current system can read a table with the given protocol.
   *
   * @param protocol
   *   The table's protocol action
   * @throws ProtocolVersionException
   *   if the table requires a newer reader
   */
  def validateReaderVersion(protocol: ProtocolAction): Unit =
    if (!isReaderVersionSupported(protocol.minReaderVersion)) {
      throw new ProtocolVersionException(
        s"Table requires reader version ${protocol.minReaderVersion} but this client supports " +
          s"version $CURRENT_READER_VERSION. Please upgrade to read this table."
      )
    }

  /**
   * Validate that the current system can write to a table with the given protocol.
   *
   * @param protocol
   *   The table's protocol action
   * @throws ProtocolVersionException
   *   if the table requires a newer writer
   */
  def validateWriterVersion(protocol: ProtocolAction): Unit =
    if (!isWriterVersionSupported(protocol.minWriterVersion)) {
      throw new ProtocolVersionException(
        s"Table requires writer version ${protocol.minWriterVersion} but this client supports " +
          s"version $CURRENT_WRITER_VERSION. Please upgrade to write to this table."
      )
    }

  /**
   * Get the minimum required reader version for a set of features.
   *
   * @param features
   *   Set of feature names being used
   * @return
   *   The minimum reader version required
   */
  def getMinReaderVersionForFeatures(features: Set[String]): Int = {
    val usesV4 = features.exists(V4_READER_FEATURES.contains)
    val usesV3 = features.exists(V3_READER_FEATURES.contains)
    if (usesV4) 4 else if (usesV3) 3 else 2
  }

  /** Get V4 protocol with Avro state support. */
  def v4Protocol(): ProtocolAction =
    ProtocolAction(
      minReaderVersion = 4,
      minWriterVersion = 4,
      readerFeatures = Some(Set(FEATURE_AVRO_STATE)),
      writerFeatures = Some(Set(FEATURE_AVRO_STATE))
    )

  /**
   * Check if a table uses V4 features based on its protocol.
   *
   * @param protocol
   *   The table's protocol action
   * @return
   *   true if the table requires V4 reader
   */
  def usesV4Features(protocol: ProtocolAction): Boolean =
    protocol.minReaderVersion >= 4
}

/** Exception thrown when a protocol version requirement is not met. */
class ProtocolVersionException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)
