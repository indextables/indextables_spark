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

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

/**
 * Serializes Scala Action types to JSON formats expected by the native tantivy4java transaction log
 * API.
 *
 * Two output formats:
 *   - JSON-lines for `writeVersion()` (GAP-1): one JSON object per line with action type
 *     discriminator
 *   - JSON array for `addFiles()`: array of AddAction objects
 */
object ActionJsonSerializer {

  private val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    m
  }

  /**
   * Serialize a sequence of AddActions to a JSON array string for `TransactionLogWriter.addFiles()`.
   */
  def addActionsToJson(addActions: Seq[AddAction]): String = {
    val arrayNode = mapper.createArrayNode()
    addActions.foreach { a => arrayNode.add(addActionToObjectNode(a)) }
    mapper.writeValueAsString(arrayNode)
  }

  /**
   * Serialize a ProtocolAction to a JSON-lines entry: `{"protocol":{...}}`.
   */
  def protocolToJsonLine(protocol: ProtocolAction): String = {
    val wrapper = mapper.createObjectNode()
    val node    = mapper.createObjectNode()
    node.put("minReaderVersion", protocol.minReaderVersion)
    node.put("minWriterVersion", protocol.minWriterVersion)
    protocol.readerFeatures.foreach { features =>
      val arr = mapper.createArrayNode()
      features.foreach(arr.add)
      node.set[ObjectNode]("readerFeatures", arr)
    }
    protocol.writerFeatures.foreach { features =>
      val arr = mapper.createArrayNode()
      features.foreach(arr.add)
      node.set[ObjectNode]("writerFeatures", arr)
    }
    wrapper.set[ObjectNode]("protocol", node)
    mapper.writeValueAsString(wrapper)
  }

  /**
   * Serialize a MetadataAction to a JSON-lines entry: `{"metaData":{...}}`.
   */
  def metadataToJsonLine(metadata: MetadataAction): String = {
    val wrapper = mapper.createObjectNode()
    val node    = mapper.createObjectNode()
    node.put("id", metadata.id)
    metadata.name.foreach(node.put("name", _))
    metadata.description.foreach(node.put("description", _))

    val formatNode = mapper.createObjectNode()
    formatNode.put("provider", metadata.format.provider)
    val formatOpts = mapper.createObjectNode()
    metadata.format.options.foreach { case (k, v) => formatOpts.put(k, v) }
    formatNode.set[ObjectNode]("options", formatOpts)
    node.set[ObjectNode]("format", formatNode)

    node.put("schemaString", metadata.schemaString)

    val partArr = mapper.createArrayNode()
    metadata.partitionColumns.foreach(partArr.add)
    node.set[ObjectNode]("partitionColumns", partArr)

    val configNode = mapper.createObjectNode()
    metadata.configuration.foreach { case (k, v) => configNode.put(k, v) }
    node.set[ObjectNode]("configuration", configNode)

    metadata.createdTime.foreach(node.put("createdTime", _))

    wrapper.set[ObjectNode]("metaData", node)
    mapper.writeValueAsString(wrapper)
  }

  /**
   * Serialize a RemoveAction to a JSON-lines entry: `{"remove":{...}}`.
   */
  def removeToJsonLine(remove: RemoveAction): String = {
    val wrapper = mapper.createObjectNode()
    val node    = mapper.createObjectNode()
    node.put("path", remove.path)
    remove.deletionTimestamp.foreach(node.put("deletionTimestamp", _))
    node.put("dataChange", remove.dataChange)
    remove.extendedFileMetadata.foreach(node.put("extendedFileMetadata", _))
    remove.partitionValues.foreach { pv =>
      val pvNode = mapper.createObjectNode()
      pv.foreach { case (k, v) => pvNode.put(k, v) }
      node.set[ObjectNode]("partitionValues", pvNode)
    }
    remove.size.foreach(node.put("size", _))
    remove.tags.foreach { tags =>
      val tagsNode = mapper.createObjectNode()
      tags.foreach { case (k, v) => tagsNode.put(k, v) }
      node.set[ObjectNode]("tags", tagsNode)
    }
    wrapper.set[ObjectNode]("remove", node)
    mapper.writeValueAsString(wrapper)
  }

  /**
   * Serialize an AddAction to a JSON-lines entry: `{"add":{...}}`.
   */
  def addToJsonLine(add: AddAction): String = {
    val wrapper = mapper.createObjectNode()
    wrapper.set[ObjectNode]("add", addActionToObjectNode(add))
    mapper.writeValueAsString(wrapper)
  }

  /**
   * Serialize a SkipAction to a JSON string for `TransactionLogWriter.skipFile()`.
   */
  def skipActionToJson(skip: SkipAction): String = {
    val node = mapper.createObjectNode()
    node.put("path", skip.path)
    node.put("skipTimestamp", skip.skipTimestamp)
    node.put("reason", skip.reason)
    node.put("operation", skip.operation)
    skip.partitionValues.foreach { pv =>
      val pvNode = mapper.createObjectNode()
      pv.foreach { case (k, v) => pvNode.put(k, v) }
      node.set[ObjectNode]("partitionValues", pvNode)
    }
    skip.size.foreach(node.put("size", _))
    skip.retryAfter.foreach(node.put("retryAfter", _))
    node.put("skipCount", skip.skipCount)
    mapper.writeValueAsString(node)
  }

  /**
   * Build JSON-lines string from a sequence of actions for `writeVersion()` (GAP-1).
   *
   * Each action is serialized as a single JSON line with an action type discriminator.
   */
  def actionsToJsonLines(actions: Seq[Action]): String = {
    actions.map {
      case p: ProtocolAction => protocolToJsonLine(p)
      case m: MetadataAction => metadataToJsonLine(m)
      case a: AddAction      => addToJsonLine(a)
      case r: RemoveAction   => removeToJsonLine(r)
      case s: SkipAction =>
        val wrapper = mapper.createObjectNode()
        val skipNode = mapper.createObjectNode()
        skipNode.put("path", s.path)
        skipNode.put("skipTimestamp", s.skipTimestamp)
        skipNode.put("reason", s.reason)
        skipNode.put("operation", s.operation)
        s.retryAfter.foreach(skipNode.put("retryAfter", _))
        skipNode.put("skipCount", s.skipCount)
        wrapper.set[ObjectNode]("skip", skipNode)
        mapper.writeValueAsString(wrapper)
    }.mkString("\n")
  }

  /**
   * Serialize a ProtocolAction to a standalone JSON string (no wrapper).
   */
  def protocolToJson(protocol: ProtocolAction): String = {
    val node = mapper.createObjectNode()
    node.put("minReaderVersion", protocol.minReaderVersion)
    node.put("minWriterVersion", protocol.minWriterVersion)
    protocol.readerFeatures.foreach { features =>
      val arr = mapper.createArrayNode()
      features.foreach(arr.add)
      node.set[ObjectNode]("readerFeatures", arr)
    }
    protocol.writerFeatures.foreach { features =>
      val arr = mapper.createArrayNode()
      features.foreach(arr.add)
      node.set[ObjectNode]("writerFeatures", arr)
    }
    mapper.writeValueAsString(node)
  }

  /**
   * Serialize a MetadataAction to a standalone JSON string (no wrapper).
   */
  def metadataToJson(metadata: MetadataAction): String = {
    val node = mapper.createObjectNode()
    node.put("id", metadata.id)
    metadata.name.foreach(node.put("name", _))
    metadata.description.foreach(node.put("description", _))

    val formatNode = mapper.createObjectNode()
    formatNode.put("provider", metadata.format.provider)
    val formatOpts = mapper.createObjectNode()
    metadata.format.options.foreach { case (k, v) => formatOpts.put(k, v) }
    formatNode.set[ObjectNode]("options", formatOpts)
    node.set[ObjectNode]("format", formatNode)

    node.put("schemaString", metadata.schemaString)

    val partArr = mapper.createArrayNode()
    metadata.partitionColumns.foreach(partArr.add)
    node.set[ObjectNode]("partitionColumns", partArr)

    val configNode = mapper.createObjectNode()
    metadata.configuration.foreach { case (k, v) => configNode.put(k, v) }
    node.set[ObjectNode]("configuration", configNode)

    metadata.createdTime.foreach(node.put("createdTime", _))

    mapper.writeValueAsString(node)
  }

  private def addActionToObjectNode(a: AddAction): ObjectNode = {
    val node = mapper.createObjectNode()
    node.put("path", a.path)

    val pvNode = mapper.createObjectNode()
    a.partitionValues.foreach { case (k, v) => pvNode.put(k, v) }
    node.set[ObjectNode]("partitionValues", pvNode)

    node.put("size", a.size)
    node.put("modificationTime", a.modificationTime)
    node.put("dataChange", a.dataChange)

    a.stats.foreach(node.put("stats", _))

    a.tags.foreach { tags =>
      val tagsNode = mapper.createObjectNode()
      tags.foreach { case (k, v) => tagsNode.put(k, v) }
      node.set[ObjectNode]("tags", tagsNode)
    }

    a.minValues.foreach { mv =>
      val mvNode = mapper.createObjectNode()
      mv.foreach { case (k, v) => mvNode.put(k, v) }
      node.set[ObjectNode]("minValues", mvNode)
    }
    a.maxValues.foreach { mv =>
      val mvNode = mapper.createObjectNode()
      mv.foreach { case (k, v) => mvNode.put(k, v) }
      node.set[ObjectNode]("maxValues", mvNode)
    }
    a.numRecords.foreach(node.put("numRecords", _))

    a.footerStartOffset.foreach(node.put("footerStartOffset", _))
    a.footerEndOffset.foreach(node.put("footerEndOffset", _))
    if (a.hasFooterOffsets) node.put("hasFooterOffsets", true)
    a.hotcacheStartOffset.foreach(node.put("hotcacheStartOffset", _))
    a.hotcacheLength.foreach(node.put("hotcacheLength", _))

    a.timeRangeStart.foreach(node.put("timeRangeStart", _))
    a.timeRangeEnd.foreach(node.put("timeRangeEnd", _))

    a.splitTags.foreach { tags =>
      val arr = mapper.createArrayNode()
      tags.foreach(arr.add)
      node.set[ObjectNode]("splitTags", arr)
    }

    a.deleteOpstamp.foreach(node.put("deleteOpstamp", _))
    a.numMergeOps.foreach(node.put("numMergeOps", _))
    a.docMappingJson.foreach(node.put("docMappingJson", _))
    a.docMappingRef.foreach(node.put("docMappingRef", _))
    a.uncompressedSizeBytes.foreach(node.put("uncompressedSizeBytes", _))

    a.companionSourceFiles.foreach { files =>
      val arr = mapper.createArrayNode()
      files.foreach(arr.add)
      node.set[ObjectNode]("companionSourceFiles", arr)
    }
    a.companionDeltaVersion.foreach(node.put("companionDeltaVersion", _))
    a.companionFastFieldMode.foreach(node.put("companionFastFieldMode", _))

    node
  }
}
