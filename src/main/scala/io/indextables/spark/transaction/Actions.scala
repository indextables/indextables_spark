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

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

/**
 * Pre-parsed metadata extracted from docMappingJson. This avoids repeated JSON parsing throughout the codebase.
 *
 * @param fieldNames
 *   All field names in the schema
 * @param fastFields
 *   Fields configured with fast=true (for aggregations)
 * @param fieldTypes
 *   Map of field name -> type (text, string, i64, f64, etc.)
 * @param indexedFields
 *   Fields configured with indexed=true
 * @param storedFields
 *   Fields configured with stored=true
 */
case class DocMappingMetadata(
  fieldNames: Set[String],
  fastFields: Set[String],
  fieldTypes: Map[String, String],
  indexedFields: Set[String] = Set.empty,
  storedFields: Set[String] = Set.empty)

object DocMappingMetadata {
  private val mapper = new ObjectMapper()

  /** Empty metadata for when no docMappingJson is available */
  val empty: DocMappingMetadata = DocMappingMetadata(Set.empty, Set.empty, Map.empty, Set.empty, Set.empty)

  /** Parse docMappingJson into structured metadata. Handles both array format and field_mappings wrapper format. */
  def parse(docMappingJson: String): DocMappingMetadata = {
    if (docMappingJson == null || docMappingJson.isEmpty) {
      return empty
    }

    try {
      EnhancedTransactionLogCache.incrementGlobalJsonParseCounter()
      val root = mapper.readTree(docMappingJson)

      // Determine the array of field definitions
      // Format 1: Direct array [{"name":"field1",...}, ...]
      // Format 2: Wrapped {"field_mappings":[{"name":"field1",...}, ...]}
      val fieldsArray = if (root.isArray) {
        root
      } else if (root.has("field_mappings") && root.get("field_mappings").isArray) {
        root.get("field_mappings")
      } else {
        return empty
      }

      val fieldNames    = scala.collection.mutable.Set[String]()
      val fastFields    = scala.collection.mutable.Set[String]()
      val fieldTypes    = scala.collection.mutable.Map[String, String]()
      val indexedFields = scala.collection.mutable.Set[String]()
      val storedFields  = scala.collection.mutable.Set[String]()

      fieldsArray.elements().asScala.foreach { fieldNode =>
        val name = Option(fieldNode.get("name")).map(_.asText()).getOrElse("")
        if (name.nonEmpty) {
          fieldNames += name

          val isFast = Option(fieldNode.get("fast")).exists(_.asBoolean(false))
          if (isFast) {
            fastFields += name
          }

          val isIndexed = Option(fieldNode.get("indexed")).exists(_.asBoolean(false))
          if (isIndexed) {
            indexedFields += name
          }

          val isStored = Option(fieldNode.get("stored")).exists(_.asBoolean(false))
          if (isStored) {
            storedFields += name
          }

          val fieldType = Option(fieldNode.get("type")).map(_.asText()).getOrElse("unknown")
          fieldTypes += (name -> fieldType)
        }
      }

      DocMappingMetadata(fieldNames.toSet, fastFields.toSet, fieldTypes.toMap, indexedFields.toSet, storedFields.toSet)
    } catch {
      case _: Exception => empty
    }
  }
}

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
  @JsonProperty("hotcacheStartOffset") @JsonDeserialize(using =
    classOf[OptionalLongDeserializer]
  ) hotcacheStartOffset: Option[Long] = None,
  @JsonProperty("hotcacheLength") @JsonDeserialize(using = classOf[OptionalLongDeserializer]) hotcacheLength: Option[
    Long
  ] = None,
  @JsonProperty("hasFooterOffsets") hasFooterOffsets: Boolean = false,
  // Complete tantivy4java SplitMetadata fields
  @JsonProperty("timeRangeStart") timeRangeStart: Option[String] = None, // Instant as ISO string
  @JsonProperty("timeRangeEnd") timeRangeEnd: Option[String] = None,     // Instant as ISO string
  @JsonProperty("splitTags") splitTags: Option[Set[String]] = None,      // tantivy4java tags (distinct from Delta tags)
  @JsonProperty("deleteOpstamp") @JsonDeserialize(using = classOf[OptionalLongDeserializer]) deleteOpstamp: Option[Long] =
    None,
  @JsonProperty("numMergeOps") numMergeOps: Option[Int] = None,
  @JsonProperty("docMappingJson") docMappingJson: Option[String] = None,
  @JsonProperty("docMappingRef") docMappingRef: Option[String] = None, // Schema hash reference for deduplication
  @JsonProperty("uncompressedSizeBytes") @JsonDeserialize(using =
    classOf[OptionalLongDeserializer]
  ) uncompressedSizeBytes: Option[Long] = None, // SplitMetadata uncompressed size
  // Companion mode fields (parquet companion splits)
  @JsonProperty("companionSourceFiles") companionSourceFiles: Option[Seq[String]] = None, // Relative parquet file paths indexed into this split
  @JsonProperty("companionDeltaVersion") @JsonDeserialize(using =
    classOf[OptionalLongDeserializer]
  ) companionDeltaVersion: Option[Long] = None, // Delta version this split was built from
  @JsonProperty("companionFastFieldMode") companionFastFieldMode: Option[String] = None // "DISABLED", "HYBRID", or "PARQUET_ONLY"
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
