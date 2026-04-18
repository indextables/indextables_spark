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

import io.indextables.spark.arrow.ArrowFfiBridge
import io.indextables.spark.util.JsonUtil
import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.impl.UnionListWriter
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.slf4j.LoggerFactory

/**
 * Converts Scala Action objects to an Arrow RecordBatch for the FR2 write path.
 *
 * Uses a unified schema with an `action_type` discriminator column. Non-applicable columns are null for each action
 * type.
 *
 * The Arrow batch is exported via Arrow C Data Interface (FFI) and passed to
 * TransactionLogWriter.writeVersionArrowFfi().
 */
object ActionsToArrowConverter {

  private val logger                     = LoggerFactory.getLogger(ActionsToArrowConverter.getClass)
  private val mapper                     = JsonUtil.mapper
  private val allocator: BufferAllocator = ArrowFfiBridge.allocator

  /**
   * Export actions as Arrow FFI structs for writeVersionArrowFfi.
   *
   * @param actions
   *   Seq of Action (AddAction, RemoveAction, SkipAction, ProtocolAction, MetadataAction)
   * @return
   *   (arrayAddr, schemaAddr) — caller passes these to writeVersionArrowFfi, then must close the ArrowArray/ArrowSchema
   */
  def exportAsFfi(actions: Seq[Action]): (ArrowArray, ArrowSchema, Long, Long) = {
    val root        = buildVectorSchemaRoot(actions)
    val arrowArray  = ArrowArray.allocateNew(allocator)
    val arrowSchema = ArrowSchema.allocateNew(allocator)
    try {
      Data.exportVectorSchemaRoot(allocator, root, null, arrowArray, arrowSchema)
      (arrowArray, arrowSchema, arrowArray.memoryAddress(), arrowSchema.memoryAddress())
    } catch {
      case e: Exception =>
        arrowArray.close()
        arrowSchema.close()
        throw e
    } finally
      root.close() // always close root; caller owns arrowArray/arrowSchema on success
  }

  private def buildVectorSchemaRoot(actions: Seq[Action]): VectorSchemaRoot = {
    val schema = buildSchema()
    val root   = VectorSchemaRoot.create(schema, allocator)

    val actionTypeVec             = root.getVector("action_type").asInstanceOf[VarCharVector]
    val pathVec                   = root.getVector("path").asInstanceOf[VarCharVector]
    val sizeVec                   = root.getVector("size").asInstanceOf[BigIntVector]
    val modTimeVec                = root.getVector("modification_time").asInstanceOf[BigIntVector]
    val dataChangeVec             = root.getVector("data_change").asInstanceOf[BitVector]
    val partitionValuesVec        = root.getVector("partition_values").asInstanceOf[VarCharVector]
    val numRecordsVec             = root.getVector("num_records").asInstanceOf[BigIntVector]
    val minValuesVec              = root.getVector("min_values").asInstanceOf[VarCharVector]
    val maxValuesVec              = root.getVector("max_values").asInstanceOf[VarCharVector]
    val statsVec                  = root.getVector("stats").asInstanceOf[VarCharVector]
    val footerStartVec            = root.getVector("footer_start_offset").asInstanceOf[BigIntVector]
    val footerEndVec              = root.getVector("footer_end_offset").asInstanceOf[BigIntVector]
    val docMappingJsonVec         = root.getVector("doc_mapping_json").asInstanceOf[VarCharVector]
    val docMappingRefVec          = root.getVector("doc_mapping_ref").asInstanceOf[VarCharVector]
    val uncompressedSizeVec       = root.getVector("uncompressed_size_bytes").asInstanceOf[BigIntVector]
    val timeRangeStartVec         = root.getVector("time_range_start").asInstanceOf[BigIntVector]
    val timeRangeEndVec           = root.getVector("time_range_end").asInstanceOf[BigIntVector]
    val companionDeltaVersionVec  = root.getVector("companion_delta_version").asInstanceOf[BigIntVector]
    val companionFastFieldModeVec = root.getVector("companion_fast_field_mode").asInstanceOf[VarCharVector]
    val deletionTimestampVec      = root.getVector("deletion_timestamp").asInstanceOf[BigIntVector]
    val skipTimestampVec          = root.getVector("skip_timestamp").asInstanceOf[BigIntVector]
    val reasonVec                 = root.getVector("reason").asInstanceOf[VarCharVector]
    val operationVec              = root.getVector("operation").asInstanceOf[VarCharVector]
    val retryAfterVec             = root.getVector("retry_after").asInstanceOf[BigIntVector]
    val skipCountVec              = root.getVector("skip_count").asInstanceOf[IntVector]
    val minReaderVersionVec       = root.getVector("min_reader_version").asInstanceOf[IntVector]
    val minWriterVersionVec       = root.getVector("min_writer_version").asInstanceOf[IntVector]
    val metadataIdVec             = root.getVector("id").asInstanceOf[VarCharVector]
    val metadataNameVec           = root.getVector("metadata_name").asInstanceOf[VarCharVector]
    val metadataDescriptionVec    = root.getVector("metadata_description").asInstanceOf[VarCharVector]
    val schemaStringVec           = root.getVector("schema_string").asInstanceOf[VarCharVector]
    val configurationVec          = root.getVector("configuration").asInstanceOf[VarCharVector]
    val createdTimeVec            = root.getVector("created_time").asInstanceOf[BigIntVector]
    val formatProviderVec         = root.getVector("format_provider").asInstanceOf[VarCharVector]
    val formatOptionsVec          = root.getVector("format_options").asInstanceOf[VarCharVector]
    // List vectors for split_tags, companion_source_files, partition_columns, reader/writer_features
    val splitTagsVec            = root.getVector("split_tags").asInstanceOf[ListVector]
    val companionSourceFilesVec = root.getVector("companion_source_files").asInstanceOf[ListVector]
    val partitionColumnsVec     = root.getVector("partition_columns").asInstanceOf[ListVector]
    val readerFeaturesVec       = root.getVector("reader_features").asInstanceOf[ListVector]
    val writerFeaturesVec       = root.getVector("writer_features").asInstanceOf[ListVector]

    // Allocate all vectors
    root.allocateNew()

    actions.zipWithIndex.foreach {
      case (action, i) =>
        action match {
          case a: AddAction =>
            setString(actionTypeVec, i, "add")
            setString(pathVec, i, a.path)
            sizeVec.setSafe(i, a.size)
            modTimeVec.setSafe(i, a.modificationTime)
            dataChangeVec.setSafe(i, if (a.dataChange) 1 else 0)
            a.partitionValues match {
              case m if m.nonEmpty => setString(partitionValuesVec, i, mapper.writeValueAsString(m.asJava))
              case _               => partitionValuesVec.setNull(i)
            }
            a.numRecords.foreach(numRecordsVec.setSafe(i, _))
            a.minValues.foreach(m => setString(minValuesVec, i, mapper.writeValueAsString(m.asJava)))
            a.maxValues.foreach(m => setString(maxValuesVec, i, mapper.writeValueAsString(m.asJava)))
            a.stats.foreach(s => setString(statsVec, i, s))
            a.footerStartOffset.foreach(footerStartVec.setSafe(i, _))
            a.footerEndOffset.foreach(footerEndVec.setSafe(i, _))
            a.docMappingJson.foreach(s => setString(docMappingJsonVec, i, s))
            a.docMappingRef.foreach(s => setString(docMappingRefVec, i, s))
            a.uncompressedSizeBytes.foreach(uncompressedSizeVec.setSafe(i, _))
            a.timeRangeStart.foreach { t =>
              try timeRangeStartVec.setSafe(i, t.toLong)
              catch {
                case _: NumberFormatException =>
                  logger.warn(s"Cannot parse timeRangeStart '$t' as Long for ${a.path}, time-based pruning disabled for this split")
              }
            }
            a.timeRangeEnd.foreach { t =>
              try timeRangeEndVec.setSafe(i, t.toLong)
              catch {
                case _: NumberFormatException =>
                  logger.warn(
                    s"Cannot parse timeRangeEnd '$t' as Long for ${a.path}, time-based pruning disabled for this split"
                  )
              }
            }
            a.companionDeltaVersion.foreach(companionDeltaVersionVec.setSafe(i, _))
            a.companionFastFieldMode.foreach(s => setString(companionFastFieldModeVec, i, s))
            a.splitTags.foreach(tags => writeStringList(splitTagsVec, i, tags.toSeq))
            a.companionSourceFiles.foreach(files => writeStringList(companionSourceFilesVec, i, files))

          case r: RemoveAction =>
            setString(actionTypeVec, i, "remove")
            setString(pathVec, i, r.path)
            r.deletionTimestamp.foreach(deletionTimestampVec.setSafe(i, _))
            dataChangeVec.setSafe(i, if (r.dataChange) 1 else 0)
            r.partitionValues.foreach(m => setString(partitionValuesVec, i, mapper.writeValueAsString(m.asJava)))
            r.size.foreach(sizeVec.setSafe(i, _))

          case s: SkipAction =>
            setString(actionTypeVec, i, "mergeskip")
            setString(pathVec, i, s.path)
            skipTimestampVec.setSafe(i, s.skipTimestamp)
            setString(reasonVec, i, s.reason)
            setString(operationVec, i, s.operation)
            s.retryAfter.foreach(retryAfterVec.setSafe(i, _))
            skipCountVec.setSafe(i, s.skipCount)
            s.partitionValues.foreach(m => setString(partitionValuesVec, i, mapper.writeValueAsString(m.asJava)))
            s.size.foreach(sizeVec.setSafe(i, _))

          case p: ProtocolAction =>
            setString(actionTypeVec, i, "protocol")
            minReaderVersionVec.setSafe(i, p.minReaderVersion)
            minWriterVersionVec.setSafe(i, p.minWriterVersion)
            p.readerFeatures.foreach(features => writeStringList(readerFeaturesVec, i, features.toSeq))
            p.writerFeatures.foreach(features => writeStringList(writerFeaturesVec, i, features.toSeq))

          case m: MetadataAction =>
            setString(actionTypeVec, i, "metadata")
            setString(metadataIdVec, i, m.id)
            m.name.foreach(n => setString(metadataNameVec, i, n))
            m.description.foreach(d => setString(metadataDescriptionVec, i, d))
            setString(schemaStringVec, i, m.schemaString)
            if (m.partitionColumns.nonEmpty) {
              writeStringList(partitionColumnsVec, i, m.partitionColumns)
            }
            if (m.configuration.nonEmpty) {
              setString(configurationVec, i, mapper.writeValueAsString(m.configuration.asJava))
            }
            m.createdTime.foreach(createdTimeVec.setSafe(i, _))
            setString(formatProviderVec, i, m.format.provider)
            if (m.format.options.nonEmpty) {
              setString(formatOptionsVec, i, mapper.writeValueAsString(m.format.options.asJava))
            }

          case _ => // Unknown action type — skip
        }
    }

    root.setRowCount(actions.size)
    root
  }

  private def setString(
    vec: VarCharVector,
    i: Int,
    value: String
  ): Unit =
    if (value != null) vec.setSafe(i, value.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    else vec.setNull(i)

  private def writeStringList(
    vec: ListVector,
    index: Int,
    values: Seq[String]
  ): Unit = {
    val writer = vec.getWriter.asInstanceOf[UnionListWriter]
    writer.setPosition(index)
    writer.startList()
    values.foreach { v =>
      val bytes = v.getBytes(java.nio.charset.StandardCharsets.UTF_8)
      val buf   = allocator.buffer(bytes.length)
      buf.writeBytes(bytes)
      writer.writeVarChar(0, bytes.length, buf)
      buf.close()
    }
    writer.endList()
  }

  private def buildSchema(): Schema = {
    val fields = new java.util.ArrayList[Field]()
    fields.add(Field.notNullable("action_type", new ArrowType.Utf8()))
    fields.add(Field.nullable("path", new ArrowType.Utf8()))
    fields.add(Field.nullable("size", new ArrowType.Int(64, true)))
    fields.add(Field.nullable("modification_time", new ArrowType.Int(64, true)))
    fields.add(Field.nullable("data_change", new ArrowType.Bool()))
    fields.add(Field.nullable("partition_values", new ArrowType.Utf8()))
    fields.add(Field.nullable("num_records", new ArrowType.Int(64, true)))
    fields.add(Field.nullable("min_values", new ArrowType.Utf8()))
    fields.add(Field.nullable("max_values", new ArrowType.Utf8()))
    fields.add(Field.nullable("stats", new ArrowType.Utf8()))
    fields.add(Field.nullable("footer_start_offset", new ArrowType.Int(64, true)))
    fields.add(Field.nullable("footer_end_offset", new ArrowType.Int(64, true)))
    fields.add(Field.nullable("doc_mapping_json", new ArrowType.Utf8()))
    fields.add(Field.nullable("doc_mapping_ref", new ArrowType.Utf8()))
    fields.add(Field.nullable("uncompressed_size_bytes", new ArrowType.Int(64, true)))
    fields.add(Field.nullable("time_range_start", new ArrowType.Int(64, true)))
    fields.add(Field.nullable("time_range_end", new ArrowType.Int(64, true)))
    fields.add(Field.nullable("companion_delta_version", new ArrowType.Int(64, true)))
    fields.add(Field.nullable("companion_fast_field_mode", new ArrowType.Utf8()))
    fields.add(Field.nullable("deletion_timestamp", new ArrowType.Int(64, true)))
    fields.add(Field.nullable("skip_timestamp", new ArrowType.Int(64, true)))
    fields.add(Field.nullable("reason", new ArrowType.Utf8()))
    fields.add(Field.nullable("operation", new ArrowType.Utf8()))
    fields.add(Field.nullable("retry_after", new ArrowType.Int(64, true)))
    fields.add(Field.nullable("skip_count", new ArrowType.Int(32, true)))
    fields.add(Field.nullable("min_reader_version", new ArrowType.Int(32, true)))
    fields.add(Field.nullable("min_writer_version", new ArrowType.Int(32, true)))
    fields.add(Field.nullable("id", new ArrowType.Utf8()))
    fields.add(Field.nullable("metadata_name", new ArrowType.Utf8()))
    fields.add(Field.nullable("metadata_description", new ArrowType.Utf8()))
    fields.add(Field.nullable("schema_string", new ArrowType.Utf8()))
    fields.add(Field.nullable("configuration", new ArrowType.Utf8()))
    fields.add(Field.nullable("created_time", new ArrowType.Int(64, true)))
    fields.add(Field.nullable("format_provider", new ArrowType.Utf8()))
    fields.add(Field.nullable("format_options", new ArrowType.Utf8()))
    // List<Utf8> columns
    val listField = new FieldType(true, new ArrowType.List(), null)
    val utf8Child = new Field("item", FieldType.nullable(new ArrowType.Utf8()), null)
    fields.add(new Field("split_tags", listField, java.util.Collections.singletonList(utf8Child)))
    fields.add(new Field("companion_source_files", listField, java.util.Collections.singletonList(utf8Child)))
    fields.add(new Field("partition_columns", listField, java.util.Collections.singletonList(utf8Child)))
    fields.add(new Field("reader_features", listField, java.util.Collections.singletonList(utf8Child)))
    fields.add(new Field("writer_features", listField, java.util.Collections.singletonList(utf8Child)))

    new Schema(fields)
  }
}
