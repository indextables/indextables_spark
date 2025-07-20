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
import com.tantivy4spark.storage.DataLocation
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.annotation.JsonProperty
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.util.{Try, Success, Failure}

/**
 * Unified manager for Tantivy indexing and searching operations.
 * Ensures that indexing and searching share the same JNI configuration and index instance.
 */
class TantivyIndexManager(
    indexPath: String,
    schema: StructType,
    options: Map[String, String]
) {
  
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  
  // Shared JNI configuration
  private var configId: Option[Long] = None
  private var writerId: Option[Long] = None
  private var searchEngineId: Option[Long] = None
  
  // Indexing state
  private val documentBuffer = mutable.ListBuffer[Map[String, Any]]()
  private val batchSize = options.getOrElse("batch.size", "100").toInt
  private val recordCount = new AtomicLong(0)
  private val bytesWritten = new AtomicLong(0)
  private val messageDigest = MessageDigest.getInstance("SHA-256")
  
  // Search state
  private val maxResults = options.getOrElse("max.results", "1000").toInt
  
  // Initialize the shared configuration
  ensureConfigInitialized()
  
  private def ensureConfigInitialized(): Unit = {
    if (configId.isEmpty) {
      val globalConfig = TantivyConfig.fromSpark(schema, options)
      val configJson = TantivyConfig.toJson(globalConfig)
      configId = Some(TantivyNative.createConfig(configJson))
    }
  }
  
  private def ensureWriterInitialized(): Unit = {
    ensureConfigInitialized()
    if (writerId.isEmpty && configId.isDefined) {
      val schemaJson = generateTantivySchema(schema)
      val id = TantivyNative.createIndexWriter(configId.get, indexPath, schemaJson)
      if (id >= 0) {
        writerId = Some(id)
      }
    }
  }
  
  private def ensureSearchEngineInitialized(): Unit = {
    ensureConfigInitialized()
    if (searchEngineId.isEmpty && configId.isDefined) {
      // Ensure index writer exists first to create the index directory/files
      ensureWriterInitialized()
      val id = TantivyNative.createSearchEngine(configId.get, indexPath)
      if (id >= 0) {
        searchEngineId = Some(id)
      }
    }
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
    ensureWriterInitialized()
    
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
    
    WriteResult(
      filePath = indexPath,
      bytesWritten = dataSize,
      recordCount = 1,
      checksum = rowChecksum
    )
  }
  
  private def convertRowDataOnly(row: InternalRow): Map[String, Any] = {
    val document = mutable.Map[String, Any]()
    
    schema.fields.zipWithIndex.foreach { case (field, index) =>
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
    
    schema.fields.zipWithIndex.foreach { case (field, index) =>
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
    document("_key") = indexPath
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
  
  private def calculateChecksum(data: Array[Byte]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(data)
    digest.digest().map("%02x".format(_)).mkString
  }
  
  def commit(): Boolean = {
    flushBatch()
    writerId.exists(TantivyNative.commitIndex)
  }
  
  def search(query: String): Iterator[SearchResult] = {
    // Ensure writer is flushed before searching
    flushBatch()
    
    // Recreate search engine to pick up newly committed documents
    searchEngineId.foreach(TantivyNative.destroySearchEngine)
    searchEngineId = None
    ensureSearchEngineInitialized()
    
    searchEngineId match {
      case Some(id) =>
        Try {
          val resultsJson = TantivyNative.search(id, query, maxResults)
          parseSearchResults(resultsJson)
        } match {
          case Success(results) => results
          case Failure(exception) =>
            println(s"Search failed: ${exception.getMessage}")
            Iterator.empty
        }
      case None =>
        println("Search engine not initialized")
        Iterator.empty
    }
  }
  
  def searchWithFilters(query: String, filters: Map[String, String]): Iterator[SearchResult] = {
    val filterQuery = if (filters.nonEmpty) {
      val filterClauses = filters.map { case (field, value) => s"$field:$value" }.mkString(" AND ")
      s"($query) AND ($filterClauses)"
    } else {
      query
    }
    
    search(filterQuery)
  }
  
  private def parseSearchResults(jsonResponse: String): Iterator[SearchResult] = {
    Try {
      val response = objectMapper.readValue(jsonResponse, classOf[TantivySearchResponse])
      response.hits.map { hit =>
        SearchResult(
          docId = hit.document.getOrElse("_id", "").toString,
          score = hit.score.toFloat,
          dataLocation = DataLocation(
            bucket = extractBucket(hit.document),
            key = extractKey(hit.document),
            offset = extractOffset(hit.document),
            length = extractLength(hit.document)
          ),
          highlights = hit.snippet
        )
      }.iterator
    }.getOrElse(Iterator.empty)
  }
  
  private def extractBucket(document: Map[String, Any]): String = {
    document.getOrElse("_bucket", "default-bucket").toString
  }
  
  private def extractKey(document: Map[String, Any]): String = {
    document.getOrElse("_key", "").toString
  }
  
  private def extractOffset(document: Map[String, Any]): Long = {
    document.getOrElse("_offset", 0L) match {
      case n: Number => n.longValue()
      case s: String => Try(s.toLong).getOrElse(0L)
      case _ => 0L
    }
  }
  
  private def extractLength(document: Map[String, Any]): Long = {
    document.getOrElse("_length", 0L) match {
      case n: Number => n.longValue()
      case s: String => Try(s.toLong).getOrElse(0L)
      case _ => 0L
    }
  }
  
  def refreshIndex(): Unit = {
    // Recreate search engine to pick up index changes
    searchEngineId.foreach(TantivyNative.destroySearchEngine)
    searchEngineId = None
  }
  
  def getStats: (Long, Long) = (documentBuffer.size.toLong, batchSize.toLong)
  
  def close(): Unit = {
    flushBatch()
    writerId.foreach(TantivyNative.destroyIndexWriter)
    searchEngineId.foreach(TantivyNative.destroySearchEngine)
    configId.foreach(TantivyNative.destroyConfig)
    writerId = None
    searchEngineId = None
    configId = None
  }
}