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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.io.Source
import scala.util.{Failure, Success, Try}

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
    } finally {
      stream.close()
    }
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

    def getOptionalString(field: String): Option[String] = {
      Option(record.get(field)).map(_.toString)
    }

    def getOptionalLong(field: String): Option[Long] = {
      Option(record.get(field)).map {
        case l: java.lang.Long    => l.toLong
        case i: java.lang.Integer => i.toLong
        case other                => other.toString.toLong
      }
    }

    def getOptionalInt(field: String): Option[Int] = {
      Option(record.get(field)).map {
        case i: java.lang.Integer => i.toInt
        case l: java.lang.Long    => l.toInt
        case other                => other.toString.toInt
      }
    }

    def getOptionalStringMap(field: String): Option[Map[String, String]] = {
      Option(record.get(field)).map { value =>
        value.asInstanceOf[java.util.Map[CharSequence, CharSequence]].asScala.map { case (k, v) =>
          k.toString -> v.toString
        }.toMap
      }
    }

    def getOptionalStringSet(field: String): Option[Set[String]] = {
      Option(record.get(field)).map { value =>
        value.asInstanceOf[java.util.Collection[CharSequence]].asScala.map(_.toString).toSet
      }
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
}
