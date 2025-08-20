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

import com.tantivy4java._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow
import org.slf4j.LoggerFactory
import scala.util.{Try, Success, Failure}
import scala.util.Using
import scala.collection.mutable

/**
 * Direct tantivy4java interface that eliminates Long handles and thread safety issues.
 * Each instance manages its own Index and IndexWriter without cross-thread sharing.
 * This is designed for per-executor usage in Spark where each executor writes separate index files.
 */
object TantivyDirectInterface {
  private val initLock = new Object()
  @volatile private var initialized = false
  private val logger = LoggerFactory.getLogger(TantivyDirectInterface.getClass)
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      initLock.synchronized {
        if (!initialized) {
          Tantivy.initialize()
          initialized = true
        }
      }
    }
  }
  
  def fromIndexComponents(schema: StructType, components: Map[String, Array[Byte]]): TantivyDirectInterface = {
    // For tantivy4java, we can't truly restore from components, so create a new instance
    logger.info(s"Creating new TantivyDirectInterface from schema (components not restorable with tantivy4java)")
    new TantivyDirectInterface(schema)
  }
}

class TantivyDirectInterface(val schema: StructType) extends AutoCloseable {
  private val logger = LoggerFactory.getLogger(classOf[TantivyDirectInterface])
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  
  // Initialize tantivy4java library only once
  TantivyDirectInterface.ensureInitialized()
  
  // Create schema directly
  private val tantivySchema: Schema = createTantivySchema(schema)
  
  // Create in-memory index (each executor has its own)
  private val index: Index = new Index(tantivySchema, "", true) // In-memory index
  
  // Writer is created lazily and managed per instance
  private var indexWriter: Option[IndexWriter] = None
  
  private def createTantivySchema(sparkSchema: StructType): Schema = {
    // Follow tantivy4java pattern: use try-with-resources and close SchemaBuilder immediately
    Using(new SchemaBuilder()) { schemaBuilder =>
      val addedFields = mutable.Set[String]()
      
      sparkSchema.fields.foreach { field =>
        val fieldName = field.name
        val fieldType = field.dataType
        
        if (!addedFields.contains(fieldName)) {
          addedFields.add(fieldName)
          logger.info(s"Adding field: $fieldName of type: $fieldType")
          
          fieldType match {
            case org.apache.spark.sql.types.StringType =>
              schemaBuilder.addTextField(fieldName, true, false, "default", "position")
            case org.apache.spark.sql.types.LongType | org.apache.spark.sql.types.IntegerType =>
              schemaBuilder.addIntegerField(fieldName, true, true, true)
            case org.apache.spark.sql.types.DoubleType | org.apache.spark.sql.types.FloatType =>
              schemaBuilder.addFloatField(fieldName, true, true, true)
            case org.apache.spark.sql.types.BooleanType =>
              schemaBuilder.addBooleanField(fieldName, true, true, false)
            case org.apache.spark.sql.types.BinaryType =>
              schemaBuilder.addBytesField(fieldName, true, true, false, "position")
            case org.apache.spark.sql.types.TimestampType =>
              schemaBuilder.addIntegerField(fieldName, true, true, true) // Store as epoch millis
            case org.apache.spark.sql.types.DateType =>
              schemaBuilder.addIntegerField(fieldName, true, true, true) // Store as days since epoch
            case _ =>
              logger.warn(s"Unsupported field type for field $fieldName: $fieldType, treating as text")
              schemaBuilder.addTextField(fieldName, true, false, "default", "position")
          }
        } else {
          logger.warn(s"Skipping duplicate field: $fieldName")
        }
      }
      
      schemaBuilder.build()
    }.get
  }
  
  private def getOrCreateWriter(): IndexWriter = {
    indexWriter match {
      case Some(writer) => writer
      case None =>
        val writer = index.writer(50, 1) // Small memory arena like tantivy4java tests
        indexWriter = Some(writer)
        writer
    }
  }
  
  def addDocument(row: InternalRow): Unit = {
    Using(new Document()) { document =>
      // Convert InternalRow directly to Document without JSON
      schema.fields.zipWithIndex.foreach { case (field, index) =>
        val value = row.get(index, field.dataType)
        if (value != null) {
          addFieldToDocument(document, field.name, value, field.dataType)
        }
      }
      
      val writer = getOrCreateWriter()
      writer.addDocument(document)
      logger.debug(s"Added document to index")
    }.get
  }
  
  private def addFieldToDocument(document: Document, fieldName: String, value: Any, dataType: org.apache.spark.sql.types.DataType): Unit = {
    dataType match {
      case org.apache.spark.sql.types.StringType =>
        val str = value.asInstanceOf[org.apache.spark.unsafe.types.UTF8String].toString
        document.addText(fieldName, str)
      case org.apache.spark.sql.types.LongType =>
        document.addInteger(fieldName, value.asInstanceOf[Long])
      case org.apache.spark.sql.types.IntegerType =>
        document.addInteger(fieldName, value.asInstanceOf[Int].toLong)
      case org.apache.spark.sql.types.DoubleType =>
        document.addFloat(fieldName, value.asInstanceOf[Double])
      case org.apache.spark.sql.types.FloatType =>
        document.addFloat(fieldName, value.asInstanceOf[Float].toDouble)
      case org.apache.spark.sql.types.BooleanType =>
        document.addBoolean(fieldName, value.asInstanceOf[Boolean])
      case org.apache.spark.sql.types.BinaryType =>
        document.addBytes(fieldName, value.asInstanceOf[Array[Byte]])
      case org.apache.spark.sql.types.TimestampType =>
        // Convert microseconds to milliseconds
        val millis = value.asInstanceOf[Long] / 1000
        document.addInteger(fieldName, millis)
      case org.apache.spark.sql.types.DateType =>
        // Days since epoch
        document.addInteger(fieldName, value.asInstanceOf[Int].toLong)
      case _ =>
        logger.warn(s"Unsupported field type for $fieldName: $dataType")
    }
  }
  
  def addDocuments(rows: Iterator[InternalRow]): Unit = {
    rows.foreach(addDocument)
  }
  
  def commit(): Unit = {
    indexWriter.foreach { writer =>
      writer.commit()
      logger.info("IndexWriter committed successfully")
    }
    index.reload()
    logger.info("Index reloaded after commit")
  }
  
  def searchAll(limit: Int = 10000): Array[InternalRow] = {
    Using(index.searcher()) { searcher =>
      Using(Query.allQuery()) { query =>
        Using(searcher.search(query, limit)) { searchResult =>
          val hits = searchResult.getHits()
          val results = mutable.ArrayBuffer[InternalRow]()
          
          for (i <- 0 until hits.size()) {
            val hit = hits.get(i)
            val docAddress = hit.getDocAddress()
            
            Using(searcher.doc(docAddress)) { document =>
              val row = convertDocumentToInternalRow(document)
              results += row
            }.get // Get the result from the Using
          }
          
          results.toArray
        }.get
      }.get
    }.get
  }
  
  def search(queryString: String, limit: Int = 100): Array[InternalRow] = {
    if (queryString.isEmpty || queryString == "*:*") {
      return searchAll(limit)
    }
    
    Using(index.searcher()) { searcher =>
      Using(Query.allQuery()) { query => // For now, use allQuery - proper query parsing can be added later
        Using(searcher.search(query, limit)) { searchResult =>
          val hits = searchResult.getHits()
          val results = mutable.ArrayBuffer[InternalRow]()
          
          for (i <- 0 until hits.size()) {
            val hit = hits.get(i)
            val docAddress = hit.getDocAddress()
            
            Using(searcher.doc(docAddress)) { document =>
              val row = convertDocumentToInternalRow(document)
              results += row
            }.get
          }
          
          results.toArray
        }.get
      }.get
    }.get
  }
  
  private def convertDocumentToInternalRow(document: Document): InternalRow = {
    val values = new Array[Any](schema.fields.length)
    
    schema.fields.zipWithIndex.foreach { case (field, index) =>
      val fieldValues = document.get(field.name)
      if (fieldValues != null && !fieldValues.isEmpty) {
        val value = fieldValues.get(0)
        values(index) = convertValueToSpark(value, field.dataType)
      } else {
        values(index) = null
      }
    }
    
    InternalRow.fromSeq(values.toSeq)
  }
  
  private def convertValueToSpark(value: Any, dataType: org.apache.spark.sql.types.DataType): Any = {
    dataType match {
      case org.apache.spark.sql.types.StringType =>
        org.apache.spark.unsafe.types.UTF8String.fromString(value.toString)
      case org.apache.spark.sql.types.LongType =>
        value.asInstanceOf[Long]
      case org.apache.spark.sql.types.IntegerType =>
        value.asInstanceOf[Long].toInt
      case org.apache.spark.sql.types.DoubleType =>
        value.asInstanceOf[Double]
      case org.apache.spark.sql.types.FloatType =>
        value.asInstanceOf[Double].toFloat
      case org.apache.spark.sql.types.BooleanType =>
        value.asInstanceOf[Boolean]
      case org.apache.spark.sql.types.BinaryType =>
        value.asInstanceOf[Array[Byte]]
      case org.apache.spark.sql.types.TimestampType =>
        // Convert milliseconds to microseconds
        value.asInstanceOf[Long] * 1000
      case org.apache.spark.sql.types.DateType =>
        value.asInstanceOf[Long].toInt
      case _ =>
        value
    }
  }
  
  def getIndexComponents(): Map[String, Array[Byte]] = {
    // For tantivy4java, we create minimal components for compatibility
    val metaComponent = Map(
      "committed" -> true,
      "schema" -> SchemaConverter.sparkToTantivySchema(schema)
    )
    
    val metaJson = mapper.writeValueAsString(metaComponent)
    val metaBytes = metaJson.getBytes("UTF-8")
    
    // Create a simple data component
    val dataComponent = Map(
      "index_type" -> "tantivy4java",
      "in_memory" -> true
    )
    
    val dataJson = mapper.writeValueAsString(dataComponent)
    val dataBytes = dataJson.getBytes("UTF-8")
    
    Map(
      "meta.json" -> metaBytes,
      "data.json" -> dataBytes
    )
  }
  
  override def close(): Unit = {
    indexWriter.foreach(_.close())
    indexWriter = None
    index.close()
  }
}

