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

import scala.io.Source
import scala.util.{Failure, Success, Try}

import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema

/**
 * Avro schema definitions and utilities for the state file format.
 *
 * Schemas are loaded from resource files and cached for reuse.
 */
object AvroSchemas {

  /**
   * The FileEntry Avro schema definition as a JSON string.
   *
   * This is embedded as a constant to avoid resource loading issues in distributed environments.
   */
  val FILE_ENTRY_SCHEMA_JSON: String =
    """{
      |  "type": "record",
      |  "name": "FileEntry",
      |  "namespace": "io.indextables.state",
      |  "doc": "A file entry representing a split in the table state",
      |  "fields": [
      |    {
      |      "name": "path",
      |      "type": "string",
      |      "field-id": 100,
      |      "doc": "Relative path to the split file"
      |    },
      |    {
      |      "name": "partitionValues",
      |      "type": {"type": "map", "values": "string"},
      |      "field-id": 101,
      |      "doc": "Partition column values as string map"
      |    },
      |    {
      |      "name": "size",
      |      "type": "long",
      |      "field-id": 102,
      |      "doc": "File size in bytes"
      |    },
      |    {
      |      "name": "modificationTime",
      |      "type": "long",
      |      "field-id": 103,
      |      "doc": "File modification time (epoch milliseconds)"
      |    },
      |    {
      |      "name": "dataChange",
      |      "type": "boolean",
      |      "field-id": 104,
      |      "doc": "Whether this file represents a data change"
      |    },
      |    {
      |      "name": "stats",
      |      "type": ["null", "string"],
      |      "default": null,
      |      "field-id": 110,
      |      "doc": "JSON-encoded statistics"
      |    },
      |    {
      |      "name": "minValues",
      |      "type": ["null", {"type": "map", "values": "string"}],
      |      "default": null,
      |      "field-id": 111,
      |      "doc": "Minimum values per column for data skipping"
      |    },
      |    {
      |      "name": "maxValues",
      |      "type": ["null", {"type": "map", "values": "string"}],
      |      "default": null,
      |      "field-id": 112,
      |      "doc": "Maximum values per column for data skipping"
      |    },
      |    {
      |      "name": "numRecords",
      |      "type": ["null", "long"],
      |      "default": null,
      |      "field-id": 113,
      |      "doc": "Number of records in the file"
      |    },
      |    {
      |      "name": "footerStartOffset",
      |      "type": ["null", "long"],
      |      "default": null,
      |      "field-id": 120,
      |      "doc": "Byte offset where footer/metadata begins"
      |    },
      |    {
      |      "name": "footerEndOffset",
      |      "type": ["null", "long"],
      |      "default": null,
      |      "field-id": 121,
      |      "doc": "Byte offset where footer/metadata ends"
      |    },
      |    {
      |      "name": "hasFooterOffsets",
      |      "type": "boolean",
      |      "default": false,
      |      "field-id": 124,
      |      "doc": "Whether footer offsets are populated"
      |    },
      |    {
      |      "name": "splitTags",
      |      "type": ["null", {"type": "array", "items": "string"}],
      |      "default": null,
      |      "field-id": 132,
      |      "doc": "Tags associated with this split"
      |    },
      |    {
      |      "name": "numMergeOps",
      |      "type": ["null", "int"],
      |      "default": null,
      |      "field-id": 134,
      |      "doc": "Number of merge operations this split has been through"
      |    },
      |    {
      |      "name": "docMappingRef",
      |      "type": ["null", "string"],
      |      "default": null,
      |      "field-id": 135,
      |      "doc": "Reference to doc mapping in schema registry"
      |    },
      |    {
      |      "name": "uncompressedSizeBytes",
      |      "type": ["null", "long"],
      |      "default": null,
      |      "field-id": 136,
      |      "doc": "Uncompressed size of the split data"
      |    },
      |    {
      |      "name": "addedAtVersion",
      |      "type": "long",
      |      "field-id": 140,
      |      "doc": "Transaction version when this file was added"
      |    },
      |    {
      |      "name": "addedAtTimestamp",
      |      "type": "long",
      |      "field-id": 141,
      |      "doc": "Timestamp when this file was added (epoch milliseconds)"
      |    }
      |  ]
      |}""".stripMargin

  /** Parsed FileEntry schema (lazily initialized) */
  lazy val FILE_ENTRY_SCHEMA: Schema = new Schema.Parser().parse(FILE_ENTRY_SCHEMA_JSON)

  /** The PartitionBounds Avro schema definition as a JSON string. */
  val PARTITION_BOUNDS_SCHEMA_JSON: String =
    """{
      |  "type": "record",
      |  "name": "PartitionBounds",
      |  "namespace": "io.indextables.state",
      |  "doc": "Min/max bounds for a partition column",
      |  "fields": [
      |    {
      |      "name": "min",
      |      "type": ["null", "string"],
      |      "default": null,
      |      "doc": "Minimum value for this partition column (None if all nulls)"
      |    },
      |    {
      |      "name": "max",
      |      "type": ["null", "string"],
      |      "default": null,
      |      "doc": "Maximum value for this partition column (None if all nulls)"
      |    }
      |  ]
      |}""".stripMargin

  /** Parsed PartitionBounds schema (lazily initialized) */
  lazy val PARTITION_BOUNDS_SCHEMA: Schema = new Schema.Parser().parse(PARTITION_BOUNDS_SCHEMA_JSON)

  /** The ManifestInfo Avro schema definition as a JSON string. */
  val MANIFEST_INFO_SCHEMA_JSON: String =
    """{
      |  "type": "record",
      |  "name": "ManifestInfo",
      |  "namespace": "io.indextables.state",
      |  "doc": "Metadata about a manifest file within a state directory",
      |  "fields": [
      |    {
      |      "name": "path",
      |      "type": "string",
      |      "doc": "Relative path to the manifest file"
      |    },
      |    {
      |      "name": "numEntries",
      |      "type": "long",
      |      "doc": "Number of file entries in this manifest"
      |    },
      |    {
      |      "name": "minAddedAtVersion",
      |      "type": "long",
      |      "doc": "Minimum addedAtVersion across all entries"
      |    },
      |    {
      |      "name": "maxAddedAtVersion",
      |      "type": "long",
      |      "doc": "Maximum addedAtVersion across all entries"
      |    },
      |    {
      |      "name": "partitionBounds",
      |      "type": ["null", {"type": "map", "values": {
      |        "type": "record",
      |        "name": "PartitionBoundsInline",
      |        "fields": [
      |          {"name": "min", "type": ["null", "string"], "default": null},
      |          {"name": "max", "type": ["null", "string"], "default": null}
      |        ]
      |      }}],
      |      "default": null,
      |      "doc": "Optional partition bounds for partition pruning"
      |    },
      |    {
      |      "name": "tombstoneCount",
      |      "type": "long",
      |      "default": 0,
      |      "doc": "Number of tombstones affecting entries in this manifest"
      |    },
      |    {
      |      "name": "liveEntryCount",
      |      "type": "long",
      |      "default": -1,
      |      "doc": "Number of live entries (numEntries - tombstoneCount)"
      |    }
      |  ]
      |}""".stripMargin

  /** Parsed ManifestInfo schema (lazily initialized) */
  lazy val MANIFEST_INFO_SCHEMA: Schema = new Schema.Parser().parse(MANIFEST_INFO_SCHEMA_JSON)

  /**
   * The StateManifest Avro schema definition as a JSON string.
   *
   * This is the metadata file stored in each state directory that describes the complete table state at a specific
   * version.
   */
  val STATE_MANIFEST_SCHEMA_JSON: String =
    """{
      |  "type": "record",
      |  "name": "StateManifest",
      |  "namespace": "io.indextables.state",
      |  "doc": "The state manifest that describes the complete table state",
      |  "fields": [
      |    {
      |      "name": "formatVersion",
      |      "type": "int",
      |      "doc": "Version of the state file format"
      |    },
      |    {
      |      "name": "stateVersion",
      |      "type": "long",
      |      "doc": "Transaction version this state represents"
      |    },
      |    {
      |      "name": "createdAt",
      |      "type": "long",
      |      "doc": "Timestamp when this state was created (epoch milliseconds)"
      |    },
      |    {
      |      "name": "numFiles",
      |      "type": "long",
      |      "doc": "Total number of live files (after applying tombstones)"
      |    },
      |    {
      |      "name": "totalBytes",
      |      "type": "long",
      |      "doc": "Total size of all live files in bytes"
      |    },
      |    {
      |      "name": "manifests",
      |      "type": {
      |        "type": "array",
      |        "items": {
      |          "type": "record",
      |          "name": "ManifestInfoItem",
      |          "fields": [
      |            {"name": "path", "type": "string"},
      |            {"name": "numEntries", "type": "long"},
      |            {"name": "minAddedAtVersion", "type": "long"},
      |            {"name": "maxAddedAtVersion", "type": "long"},
      |            {"name": "partitionBounds", "type": ["null", {"type": "map", "values": {
      |              "type": "record",
      |              "name": "PartitionBoundsItem",
      |              "fields": [
      |                {"name": "min", "type": ["null", "string"], "default": null},
      |                {"name": "max", "type": ["null", "string"], "default": null}
      |              ]
      |            }}], "default": null},
      |            {"name": "tombstoneCount", "type": "long", "default": 0},
      |            {"name": "liveEntryCount", "type": "long", "default": -1}
      |          ]
      |        }
      |      },
      |      "doc": "List of manifest files containing file entries"
      |    },
      |    {
      |      "name": "tombstones",
      |      "type": {"type": "array", "items": "string"},
      |      "default": [],
      |      "doc": "List of paths that have been removed (applied during read)"
      |    },
      |    {
      |      "name": "schemaRegistry",
      |      "type": {"type": "map", "values": "string"},
      |      "default": {},
      |      "doc": "Schema registry for doc mapping deduplication"
      |    },
      |    {
      |      "name": "protocolVersion",
      |      "type": "int",
      |      "default": 4,
      |      "doc": "Protocol version (4 for Avro state format)"
      |    },
      |    {
      |      "name": "metadata",
      |      "type": ["null", "string"],
      |      "default": null,
      |      "doc": "JSON-encoded MetadataAction for fast getMetadata()"
      |    }
      |  ]
      |}""".stripMargin

  /** Parsed StateManifest schema (lazily initialized) */
  lazy val STATE_MANIFEST_SCHEMA: Schema = new Schema.Parser().parse(STATE_MANIFEST_SCHEMA_JSON)

  /**
   * Load schema from resources (alternative to embedded schema).
   *
   * @param resourcePath
   *   Path to schema resource file
   * @return
   *   Parsed Avro schema
   */
  def loadSchemaFromResource(resourcePath: String): Schema = {
    val stream = getClass.getResourceAsStream(resourcePath)
    if (stream == null) {
      throw new IllegalArgumentException(s"Schema resource not found: $resourcePath")
    }
    try {
      val schemaJson = Source.fromInputStream(stream).mkString
      new Schema.Parser().parse(schemaJson)
    } finally
      stream.close()
  }

  /**
   * Convert a GenericRecord to a FileEntry.
   *
   * @param record
   *   Avro GenericRecord with FileEntry schema
   * @return
   *   FileEntry model object
   */
  def toFileEntry(record: GenericRecord): FileEntry = {
    import scala.jdk.CollectionConverters._

    def getOptionalString(field: String): Option[String] =
      Option(record.get(field)).map(_.toString)

    def getOptionalLong(field: String): Option[Long] =
      Option(record.get(field)).map {
        case l: java.lang.Long    => l.toLong
        case i: java.lang.Integer => i.toLong
        case other                => other.toString.toLong
      }

    def getOptionalInt(field: String): Option[Int] =
      Option(record.get(field)).map {
        case i: java.lang.Integer => i.toInt
        case l: java.lang.Long    => l.toInt
        case other                => other.toString.toInt
      }

    def getOptionalStringMap(field: String): Option[Map[String, String]] =
      Option(record.get(field)).map { value =>
        value
          .asInstanceOf[java.util.Map[CharSequence, CharSequence]]
          .asScala
          .map {
            case (k, v) =>
              k.toString -> v.toString
          }
          .toMap
      }

    def getOptionalStringSet(field: String): Option[Set[String]] =
      Option(record.get(field)).map { value =>
        value.asInstanceOf[java.util.Collection[CharSequence]].asScala.map(_.toString).toSet
      }

    FileEntry(
      path = record.get("path").toString,
      partitionValues = record
        .get("partitionValues")
        .asInstanceOf[java.util.Map[CharSequence, CharSequence]]
        .asScala
        .map { case (k, v) => k.toString -> v.toString }
        .toMap,
      size = record.get("size").asInstanceOf[Long],
      modificationTime = record.get("modificationTime").asInstanceOf[Long],
      dataChange = record.get("dataChange").asInstanceOf[Boolean],
      stats = getOptionalString("stats"),
      minValues = getOptionalStringMap("minValues"),
      maxValues = getOptionalStringMap("maxValues"),
      numRecords = getOptionalLong("numRecords"),
      footerStartOffset = getOptionalLong("footerStartOffset"),
      footerEndOffset = getOptionalLong("footerEndOffset"),
      hasFooterOffsets = Option(record.get("hasFooterOffsets"))
        .map(_.asInstanceOf[Boolean])
        .getOrElse(false),
      splitTags = getOptionalStringSet("splitTags"),
      numMergeOps = getOptionalInt("numMergeOps"),
      docMappingRef = getOptionalString("docMappingRef"),
      uncompressedSizeBytes = getOptionalLong("uncompressedSizeBytes"),
      addedAtVersion = record.get("addedAtVersion").asInstanceOf[Long],
      addedAtTimestamp = record.get("addedAtTimestamp").asInstanceOf[Long]
    )
  }

  /**
   * Convert a FileEntry to a GenericRecord.
   *
   * @param entry
   *   FileEntry model object
   * @param schema
   *   Avro schema to use (defaults to FILE_ENTRY_SCHEMA)
   * @return
   *   Avro GenericRecord
   */
  def toGenericRecord(entry: FileEntry, schema: Schema = FILE_ENTRY_SCHEMA): GenericRecord = {
    import scala.jdk.CollectionConverters._
    import org.apache.avro.generic.GenericData

    val record = new GenericData.Record(schema)

    record.put("path", entry.path)
    record.put("partitionValues", entry.partitionValues.asJava)
    record.put("size", entry.size)
    record.put("modificationTime", entry.modificationTime)
    record.put("dataChange", entry.dataChange)
    record.put("stats", entry.stats.orNull)
    record.put("minValues", entry.minValues.map(_.asJava).orNull)
    record.put("maxValues", entry.maxValues.map(_.asJava).orNull)
    record.put("numRecords", entry.numRecords.map(java.lang.Long.valueOf).orNull)
    record.put("footerStartOffset", entry.footerStartOffset.map(java.lang.Long.valueOf).orNull)
    record.put("footerEndOffset", entry.footerEndOffset.map(java.lang.Long.valueOf).orNull)
    record.put("hasFooterOffsets", entry.hasFooterOffsets)
    record.put("splitTags", entry.splitTags.map(_.toSeq.asJava).orNull)
    record.put("numMergeOps", entry.numMergeOps.map(java.lang.Integer.valueOf).orNull)
    record.put("docMappingRef", entry.docMappingRef.orNull)
    record.put("uncompressedSizeBytes", entry.uncompressedSizeBytes.map(java.lang.Long.valueOf).orNull)
    record.put("addedAtVersion", entry.addedAtVersion)
    record.put("addedAtTimestamp", entry.addedAtTimestamp)

    record
  }

  /**
   * Convert a StateManifest to a GenericRecord.
   *
   * @param manifest
   *   StateManifest model object
   * @return
   *   Avro GenericRecord
   */
  def stateManifestToGenericRecord(manifest: StateManifest): GenericRecord = {
    import scala.jdk.CollectionConverters._
    import org.apache.avro.generic.GenericData

    val schema = STATE_MANIFEST_SCHEMA
    val record = new GenericData.Record(schema)

    record.put("formatVersion", manifest.formatVersion)
    record.put("stateVersion", manifest.stateVersion)
    record.put("createdAt", manifest.createdAt)
    record.put("numFiles", manifest.numFiles)
    record.put("totalBytes", manifest.totalBytes)

    // Convert manifests array
    val manifestInfoSchema    = schema.getField("manifests").schema().getElementType
    val partitionBoundsSchema = manifestInfoSchema.getField("partitionBounds").schema().getTypes.get(1).getValueType
    val manifestRecords = manifest.manifests.map { info =>
      val manifestRecord = new GenericData.Record(manifestInfoSchema)
      manifestRecord.put("path", info.path)
      manifestRecord.put("numEntries", info.numEntries)
      manifestRecord.put("minAddedAtVersion", info.minAddedAtVersion)
      manifestRecord.put("maxAddedAtVersion", info.maxAddedAtVersion)

      // Convert partition bounds
      val boundsMap = info.partitionBounds.map { bounds =>
        bounds.map {
          case (col, pb) =>
            val boundsRecord = new GenericData.Record(partitionBoundsSchema)
            boundsRecord.put("min", pb.min.orNull)
            boundsRecord.put("max", pb.max.orNull)
            col -> boundsRecord
        }.asJava
      }.orNull
      manifestRecord.put("partitionBounds", boundsMap)

      manifestRecord.put("tombstoneCount", info.tombstoneCount)
      manifestRecord.put("liveEntryCount", info.liveEntryCount)
      manifestRecord
    }.asJava

    record.put("manifests", manifestRecords)
    record.put("tombstones", manifest.tombstones.asJava)
    record.put("schemaRegistry", manifest.schemaRegistry.asJava)
    record.put("protocolVersion", manifest.protocolVersion)
    record.put("metadata", manifest.metadata.orNull)

    record
  }

  /**
   * Convert a GenericRecord to a StateManifest.
   *
   * @param record
   *   Avro GenericRecord with StateManifest schema
   * @return
   *   StateManifest model object
   */
  def genericRecordToStateManifest(record: GenericRecord): StateManifest = {
    import scala.jdk.CollectionConverters._

    def getOptionalString(field: String): Option[String] =
      Option(record.get(field)).map(_.toString)

    // Parse manifests array
    val manifestRecords = record.get("manifests").asInstanceOf[java.util.Collection[GenericRecord]].asScala
    val manifests = manifestRecords.map { manifestRecord =>
      val partitionBounds = Option(manifestRecord.get("partitionBounds")).map { boundsMap =>
        boundsMap
          .asInstanceOf[java.util.Map[CharSequence, GenericRecord]]
          .asScala
          .map {
            case (col, boundsRecord) =>
              val min = Option(boundsRecord.get("min")).map(_.toString)
              val max = Option(boundsRecord.get("max")).map(_.toString)
              col.toString -> PartitionBounds(min, max)
          }
          .toMap
      }

      ManifestInfo(
        path = manifestRecord.get("path").toString,
        numEntries = manifestRecord.get("numEntries").asInstanceOf[Long],
        minAddedAtVersion = manifestRecord.get("minAddedAtVersion").asInstanceOf[Long],
        maxAddedAtVersion = manifestRecord.get("maxAddedAtVersion").asInstanceOf[Long],
        partitionBounds = partitionBounds,
        tombstoneCount = Option(manifestRecord.get("tombstoneCount")).map(_.asInstanceOf[Long]).getOrElse(0L),
        liveEntryCount = Option(manifestRecord.get("liveEntryCount")).map(_.asInstanceOf[Long]).getOrElse(-1L)
      )
    }.toSeq

    // Parse tombstones
    val tombstones = record
      .get("tombstones")
      .asInstanceOf[java.util.Collection[CharSequence]]
      .asScala
      .map(_.toString)
      .toSeq

    // Parse schema registry
    val schemaRegistry = record
      .get("schemaRegistry")
      .asInstanceOf[java.util.Map[CharSequence, CharSequence]]
      .asScala
      .map { case (k, v) => k.toString -> v.toString }
      .toMap

    StateManifest(
      formatVersion = record.get("formatVersion").asInstanceOf[Int],
      stateVersion = record.get("stateVersion").asInstanceOf[Long],
      createdAt = record.get("createdAt").asInstanceOf[Long],
      numFiles = record.get("numFiles").asInstanceOf[Long],
      totalBytes = record.get("totalBytes").asInstanceOf[Long],
      manifests = manifests,
      tombstones = tombstones,
      schemaRegistry = schemaRegistry,
      protocolVersion = Option(record.get("protocolVersion")).map(_.asInstanceOf[Int]).getOrElse(4),
      metadata = getOptionalString("metadata")
    )
  }
}
