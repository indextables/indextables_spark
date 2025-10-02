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
 * - Version 1: Basic transaction log (AddAction, RemoveAction, MetadataAction)
 * - Version 2: Version 1 + SkipAction, extended AddAction metadata, footer offsets
 * - Version 3+: Reserved for future table features
 */
object ProtocolVersion {

  // Current versions supported by this release
  val CURRENT_READER_VERSION = 2
  val CURRENT_WRITER_VERSION = 2

  // Minimum versions we can read/write
  val MIN_READER_VERSION = 1
  val MIN_WRITER_VERSION = 1

  // Feature support (for version 3+)
  val SUPPORTED_READER_FEATURES: Set[String] = Set(
    "skippedFiles",       // SkipAction support
    "extendedMetadata",   // Extended AddAction metadata
    "footerOffsets"       // Footer offset optimization
  )

  val SUPPORTED_WRITER_FEATURES: Set[String] = Set(
    "skippedFiles",       // SkipAction support
    "extendedMetadata",   // Extended AddAction metadata
    "footerOffsets",      // Footer offset optimization
    "checkpoint",         // Checkpoint support
    "optimizeWrite"       // Optimized write operations
  )

  // Configuration keys
  val PROTOCOL_CHECK_ENABLED = "spark.indextables.protocol.checkEnabled"
  val PROTOCOL_AUTO_UPGRADE = "spark.indextables.protocol.autoUpgrade"
  val PROTOCOL_ENFORCE_READER_VERSION = "spark.indextables.protocol.enforceReaderVersion"
  val PROTOCOL_ENFORCE_WRITER_VERSION = "spark.indextables.protocol.enforceWriterVersion"

  /**
   * Check if a reader version is supported by the current system.
   */
  def isReaderVersionSupported(version: Int): Boolean = {
    version >= MIN_READER_VERSION && version <= CURRENT_READER_VERSION
  }

  /**
   * Check if a writer version is supported by the current system.
   */
  def isWriterVersionSupported(version: Int): Boolean = {
    version >= MIN_WRITER_VERSION && version <= CURRENT_WRITER_VERSION
  }

  /**
   * Get the default protocol for new tables.
   */
  def defaultProtocol(): ProtocolAction = {
    ProtocolAction(
      minReaderVersion = CURRENT_READER_VERSION,
      minWriterVersion = CURRENT_WRITER_VERSION
    )
  }

  /**
   * Get the legacy protocol for tables without explicit protocol.
   */
  def legacyProtocol(): ProtocolAction = {
    ProtocolAction(
      minReaderVersion = 1,
      minWriterVersion = 1
    )
  }
}

/**
 * Exception thrown when a protocol version requirement is not met.
 */
class ProtocolVersionException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)
