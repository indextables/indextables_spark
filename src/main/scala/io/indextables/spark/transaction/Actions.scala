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

import scala.util.Try

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

sealed trait Action extends Serializable

// Custom deserializer to handle Integer -> Long conversion for numeric fields
class OptionalLongDeserializer extends JsonDeserializer[Option[Long]] {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Option[Long] = {
    val node = p.getCodec.readTree[JsonNode](p)
    if (node.isNull) {
      None
    } else if (node.isNumber) {
      Some(node.asLong())
    } else {
      None
    }
  }
}

case class FileFormat(
  provider: String,
  options: Map[String, String])

case class MetadataAction(
  id: String,
  name: Option[String],
  description: Option[String],
  format: FileFormat,
  schemaString: String,
  partitionColumns: Seq[String],
  configuration: Map[String, String],
  createdTime: Option[Long])
    extends Action

case class AddAction(
  path: String,
  partitionValues: Map[String, String],
  size: Long,
  modificationTime: Long,
  dataChange: Boolean,
  stats: Option[String] = None,
  tags: Option[Map[String, String]] = None,
  @JsonProperty("minValues") minValues: Option[Map[String, String]] = None,
  @JsonProperty("maxValues") maxValues: Option[Map[String, String]] = None,
  @JsonProperty("numRecords") @JsonDeserialize(using = classOf[OptionalLongDeserializer]) numRecords: Option[Long] =
    None,
  // Footer offset optimization metadata for tantivy4java splits
  @JsonProperty("footerStartOffset") @JsonDeserialize(using =
    classOf[OptionalLongDeserializer]
  ) footerStartOffset: Option[Long] = None,
  @JsonProperty("footerEndOffset") @JsonDeserialize(using = classOf[OptionalLongDeserializer]) footerEndOffset: Option[
    Long
  ] = None,
  @JsonProperty("hotcacheStartOffset") hotcacheStartOffset: Option[Long] = None,
  @JsonProperty("hotcacheLength") hotcacheLength: Option[Long] = None,
  @JsonProperty("hasFooterOffsets") hasFooterOffsets: Boolean = false,
  // Complete tantivy4java SplitMetadata fields
  @JsonProperty("timeRangeStart") timeRangeStart: Option[String] = None, // Instant as ISO string
  @JsonProperty("timeRangeEnd") timeRangeEnd: Option[String] = None,     // Instant as ISO string
  @JsonProperty("splitTags") splitTags: Option[Set[String]] = None,      // tantivy4java tags (distinct from Delta tags)
  @JsonProperty("deleteOpstamp") deleteOpstamp: Option[Long] = None,
  @JsonProperty("numMergeOps") numMergeOps: Option[Int] = None,
  @JsonProperty("docMappingJson") docMappingJson: Option[String] = None,
  @JsonProperty("uncompressedSizeBytes") uncompressedSizeBytes: Option[Long] = None // SplitMetadata uncompressed size
) extends Action

case class RemoveAction(
  path: String,
  deletionTimestamp: Option[Long],
  dataChange: Boolean,
  extendedFileMetadata: Option[Boolean],
  partitionValues: Option[Map[String, String]],
  size: Option[Long],
  tags: Option[Map[String, String]] = None)
    extends Action

case class SkipAction(
  path: String,
  skipTimestamp: Long,
  reason: String,
  operation: String, // "merge", "read", etc.
  partitionValues: Option[Map[String, String]] = None,
  size: Option[Long] = None,
  retryAfter: Option[Long] = None, // Timestamp when file can be retried
  skipCount: Int = 1               // Number of times this file has been skipped
) extends Action

/**
 * Action that records the addition of a cross-reference (XRef) split to the table.
 *
 * XRef splits are lightweight indexes that consolidate term dictionaries from multiple
 * source splits into a single file, enabling fast query routing (10-100x faster split
 * identification for selective queries).
 *
 * XRef splits are partition-agnostic - a single XRef can contain splits from multiple
 * partitions. They are stored in hash-based subdirectories under _xrefsplits/ to avoid
 * directory fragmentation.
 *
 * @param path
 *   Relative path to XRef file (e.g., "_xrefsplits/kmpq/xref-{uuid}.split")
 * @param xrefId
 *   Unique identifier for this XRef
 * @param sourceSplitPaths
 *   Paths of source splits included in this XRef (can span multiple partitions)
 * @param sourceSplitCount
 *   Count of source splits for quick access
 * @param size
 *   File size in bytes
 * @param totalTerms
 *   Total unique terms indexed in this XRef
 * @param footerStartOffset
 *   Footer start byte offset for efficient opening
 * @param footerEndOffset
 *   Footer end byte offset for efficient opening
 * @param createdTime
 *   Build timestamp in milliseconds
 * @param buildDurationMs
 *   How long the build took in milliseconds
 * @param maxSourceSplits
 *   Configuration value used for this build
 */
case class AddXRefAction(
  path: String,
  xrefId: String,
  sourceSplitPaths: Seq[String],
  sourceSplitCount: Int,
  size: Long,
  totalTerms: Long,
  footerStartOffset: Long,
  footerEndOffset: Long,
  createdTime: Long,
  buildDurationMs: Long,
  maxSourceSplits: Int
) extends Action

/**
 * Action that marks a cross-reference (XRef) split as removed from the table.
 *
 * XRef splits are removed when:
 * - They are replaced by a newer XRef (reason: "replaced")
 * - Their source splits have changed (reason: "source_changed")
 * - Explicitly requested by user (reason: "explicit")
 *
 * @param path
 *   Path of XRef file to remove
 * @param xrefId
 *   XRef identifier
 * @param deletionTimestamp
 *   When the XRef was marked for removal
 * @param reason
 *   Why the XRef was removed: "replaced", "source_changed", or "explicit"
 */
case class RemoveXRefAction(
  path: String,
  xrefId: String,
  deletionTimestamp: Long,
  reason: String
) extends Action

/**
 * Protocol action that defines the minimum reader and writer versions required to access the table. This follows Delta
 * Lake's protocol versioning approach to ensure backwards compatibility.
 *
 * Readers and writers MUST check protocol version before performing any operations. Clients MUST silently ignore
 * unknown fields and actions.
 *
 * @param minReaderVersion
 *   Minimum version required to read the table
 * @param minWriterVersion
 *   Minimum version required to write to the table
 * @param readerFeatures
 *   Optional set of reader feature names (for version 3+)
 * @param writerFeatures
 *   Optional set of writer feature names (for version 3+)
 */
case class ProtocolAction(
  minReaderVersion: Int,
  minWriterVersion: Int,
  readerFeatures: Option[Set[String]] = None,
  writerFeatures: Option[Set[String]] = None)
    extends Action

case class FileStats(
  numRecords: Long,
  minValues: Map[String, String],
  maxValues: Map[String, String],
  nullCount: Map[String, Long])
