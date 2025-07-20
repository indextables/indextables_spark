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

package com.tantivy4spark.search

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StructType, DataType, StringType, LongType, DoubleType, BooleanType, TimestampType}
import com.tantivy4spark.transaction.WriteResult
import com.tantivy4spark.native.TantivyNative
import com.tantivy4spark.config.TantivyConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong
import scala.util.{Try, Success, Failure}
import scala.collection.mutable

class TantivyIndexWriter(
    basePath: String,
    dataSchema: StructType,
    options: Map[String, String]
) {
  
  private val recordCount = new AtomicLong(0)
  private val bytesWritten = new AtomicLong(0)
  private val messageDigest = MessageDigest.getInstance("SHA-256")
  private val segmentSize = options.getOrElse("segment.size", "67108864").toLong // 64MB
  private var currentSegmentPath = s"$basePath/segment_${System.currentTimeMillis()}.tantv"
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  
  // Native writer management
  private var configId: Option[Long] = None
  private var writerId: Option[Long] = None
  
  // Document buffer for batch processing
  private val documentBuffer = mutable.ListBuffer[Map[String, Any]]()
  private val batchSize = options.getOrElse("batch.size", "100").toInt
  
  // Initialize on first write
  private def ensureInitialized(): Unit = {
    if (configId.isEmpty) {
      configId = Some(initializeConfig())
    }
    
    if (writerId.isEmpty && configId.isDefined) {
      val schemaJson = generateTantivySchema(dataSchema)
      val id = TantivyNative.createIndexWriter(configId.get, basePath, schemaJson)
      if (id >= 0) {
        writerId = Some(id)
      }
    }
  }
  
  private def initializeConfig(): Long = {
    val globalConfig = TantivyConfig.fromSpark(dataSchema, options)
    val configJson = TantivyConfig.toJson(globalConfig)
    TantivyNative.createConfig(configJson)
  }
  
  private def generateTantivySchema(schema: StructType): String = {
    val fields = schema.fields.map { field =>
      val fieldType = mapSparkTypeToTantivy(field.dataType)
      field.name -> Map(
        "type" -> fieldType,
        "indexed" -> true,
        "stored" -> true
      )
    }.toMap
    
    val tantivySchema = Map(
      "field_mappings" -> fields,
      "timestamp_field" -> "_timestamp",
      "default_search_fields" -> schema.fields.filter(_.dataType == StringType).map(_.name).toList
    )
    
    objectMapper.writeValueAsString(tantivySchema)
  }
  
  private def mapSparkTypeToTantivy(dataType: DataType): String = {
    dataType match {
      case StringType => "text"
      case LongType => "i64"
      case DoubleType => "f64"
      case BooleanType => "bool"
      case TimestampType => "datetime"
      case _ => "text" // Default to text for unsupported types
    }
  }
  
  def writeRow(row: InternalRow): WriteResult = {
    ensureInitialized()
    
    // Calculate checksum on row data only (before adding metadata)
    val rowData = convertRowDataOnly(row)
    val rowChecksum = calculateChecksum(objectMapper.writeValueAsBytes(rowData))
    
    val document = convertRowToDocument(row)
    documentBuffer += document
    
    val serializedData = objectMapper.writeValueAsBytes(document)
    val dataSize = serializedData.length
    
    // Update counters
    recordCount.incrementAndGet()
    bytesWritten.addAndGet(dataSize)
    messageDigest.update(serializedData)
    
    // Flush batch if needed
    if (documentBuffer.size >= batchSize) {
      flushBatch()
    }
    
    // Check if we need to roll to a new segment
    if (shouldRollSegment()) {
      rollToNewSegment()
    }
    
    WriteResult(
      filePath = currentSegmentPath,
      bytesWritten = dataSize,
      recordCount = 1,
      checksum = rowChecksum
    )
  }
  
  private def convertRowDataOnly(row: InternalRow): Map[String, Any] = {
    val document = mutable.Map[String, Any]()
    
    dataSchema.fields.zipWithIndex.foreach { case (field, index) =>
      if (!row.isNullAt(index)) {
        val value = field.dataType match {
          case StringType => row.getUTF8String(index).toString
          case LongType => row.getLong(index)
          case DoubleType => row.getDouble(index)
          case BooleanType => row.getBoolean(index)
          case TimestampType => row.getLong(index) // Unix timestamp
          case _ => row.get(index, field.dataType).toString
        }
        document(field.name) = value
      }
    }
    
    document.toMap
  }

  private def convertRowToDocument(row: InternalRow): Map[String, Any] = {
    val document = mutable.Map[String, Any]()
    
    dataSchema.fields.zipWithIndex.foreach { case (field, index) =>
      if (!row.isNullAt(index)) {
        val value = field.dataType match {
          case StringType => row.getUTF8String(index).toString
          case LongType => row.getLong(index)
          case DoubleType => row.getDouble(index)
          case BooleanType => row.getBoolean(index)
          case TimestampType => row.getLong(index) // Unix timestamp
          case _ => row.get(index, field.dataType).toString
        }
        document(field.name) = value
      }
    }
    
    // Add metadata fields for S3 integration
    document("_timestamp") = System.currentTimeMillis()
    document("_bucket") = options.getOrElse("s3.bucket", "default-bucket")
    document("_key") = currentSegmentPath
    document("_offset") = bytesWritten.get()
    document("_length") = 0L // Will be updated on segment finalization
    
    document.toMap
  }
  
  private def flushBatch(): Unit = {
    if (documentBuffer.nonEmpty && writerId.isDefined) {
      documentBuffer.foreach { document =>
        val documentJson = objectMapper.writeValueAsString(document)
        TantivyNative.indexDocument(writerId.get, documentJson)
      }
      documentBuffer.clear()
    }
  }
  
  def writeBatch(rows: Array[InternalRow]): List[WriteResult] = {
    rows.map(writeRow).toList
  }
  
  private def shouldRollSegment(): Boolean = {
    // TODO: Implement segment rolling logic based on size, time, or other criteria
    bytesWritten.get() >= segmentSize
  }
  
  private def rollToNewSegment(): Unit = {
    finalizeCurrentSegment()
    currentSegmentPath = s"$basePath/segment_${System.currentTimeMillis()}.tantv"
    bytesWritten.set(0)
  }
  
  private def finalizeCurrentSegment(): Unit = {
    // TODO: Implement segment finalization
    // This would write segment metadata, flush buffers, and create search indices
  }
  
  private def calculateChecksum(data: Array[Byte]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(data)
    digest.digest().map("%02x".format(_)).mkString
  }
  
  def close(): Unit = {
    flushBatch()
    finalizeCurrentSegment()
    writerId.foreach(TantivyNative.destroyIndexWriter)
    configId.foreach(TantivyNative.destroyConfig)
    writerId = None
    configId = None
  }
  
  def commit(): Boolean = {
    flushBatch()
    writerId.exists(TantivyNative.commitIndex)
  }
  
  def getStats: (Long, Long) = (documentBuffer.size.toLong, batchSize.toLong)
}