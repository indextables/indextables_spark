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

package com.tantivy4spark.transaction

import com.fasterxml.jackson.annotation.{JsonProperty, JsonCreator}
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}
import scala.util.Try

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

case class FileStats(
  numRecords: Long,
  minValues: Map[String, String],
  maxValues: Map[String, String],
  nullCount: Map[String, Long])
