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
import org.slf4j.LoggerFactory
import java.nio.file.{Files, Paths, Path}
import java.io.{File, FileInputStream, FileOutputStream, ByteArrayOutputStream}
import java.util.zip.{ZipInputStream, ZipOutputStream, ZipEntry}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow
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
  
  // Global lock to prevent concurrent schema creation and field ID conflicts
  private val schemaCreationLock = new Object()
  
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
  
  // Thread-safe schema creation to prevent "Field already exists in schema id" race conditions
  private def createSchemaThreadSafe(sparkSchema: StructType): (com.tantivy4java.Schema, SchemaBuilder) = {
    schemaCreationLock.synchronized {
      logger.debug(s"Creating schema with ${sparkSchema.fields.length} fields (thread: ${Thread.currentThread().getName})")
      
      val builder = new SchemaBuilder()
      
      sparkSchema.fields.foreach { field =>
        val fieldName = field.name
        val fieldType = field.dataType
        
        logger.debug(s"Adding field: $fieldName of type: $fieldType")
        
        fieldType match {
          case org.apache.spark.sql.types.StringType =>
            builder.addTextField(fieldName, true, false, "default", "position")
          case org.apache.spark.sql.types.LongType | org.apache.spark.sql.types.IntegerType =>
            builder.addIntegerField(fieldName, true, true, true)
          case org.apache.spark.sql.types.DoubleType | org.apache.spark.sql.types.FloatType =>
            builder.addFloatField(fieldName, true, true, true)
          case org.apache.spark.sql.types.BooleanType =>
            builder.addBooleanField(fieldName, true, true, false)
          case org.apache.spark.sql.types.BinaryType =>
            builder.addBytesField(fieldName, true, true, false, "position")
          case org.apache.spark.sql.types.TimestampType =>
            builder.addIntegerField(fieldName, true, true, true) // Store as epoch millis
          case org.apache.spark.sql.types.DateType =>
            builder.addIntegerField(fieldName, true, true, true) // Store as days since epoch
          case _ =>
            throw new UnsupportedOperationException(s"Unsupported field type for field $fieldName: $fieldType. Tantivy4Spark does not support complex types like arrays, maps, or structs.")
        }
      }
      
      val tantivySchema = builder.build()
      logger.debug(s"Successfully built schema with ${sparkSchema.fields.length} fields")
      
      (tantivySchema, builder)
    }
  }
  
  def fromIndexComponents(schema: StructType, components: Map[String, Array[Byte]]): TantivyDirectInterface = {
    logger.info(s"Creating TantivyDirectInterface from ${components.size} archive components")
    
    // Look for the ZIP archive
    components.get("tantivy_index.zip") match {
      case Some(zipData) =>
        try {
          // Extract ZIP to temporary directory
          val tempDir = Files.createTempDirectory("tantivy4spark_restore_")
          extractZipArchive(zipData, tempDir)
          
          // Create instance that opens the extracted index
          new TantivyDirectInterface(schema, Some(tempDir))
          
        } catch {
          case ex: Exception =>
            logger.error(s"Failed to restore from ZIP archive", ex)
            // Fallback to new instance
            new TantivyDirectInterface(schema)
        }
      
      case None =>
        logger.warn("No tantivy_index.zip found in components, creating new instance")
        new TantivyDirectInterface(schema)
    }
  }
  
  private def extractZipArchive(zipData: Array[Byte], targetDir: Path): Unit = {
    val bais = new java.io.ByteArrayInputStream(zipData)
    val zis = new ZipInputStream(bais)
    
    try {
      var entry: ZipEntry = zis.getNextEntry
      while (entry != null) {
        if (!entry.isDirectory) {
          val filePath = targetDir.resolve(entry.getName)
          val fos = new FileOutputStream(filePath.toFile)
          
          try {
            val buffer = new Array[Byte](1024)
            var len = zis.read(buffer)
            while (len != -1) {
              fos.write(buffer, 0, len)
              len = zis.read(buffer)
            }
            logger.debug(s"Extracted from ZIP: ${entry.getName}")
          } finally {
            fos.close()
          }
        }
        
        zis.closeEntry()
        entry = zis.getNextEntry
      }
      logger.info(s"Successfully extracted ZIP archive to: ${targetDir.toAbsolutePath}")
      
    } finally {
      zis.close()
      bais.close()
    }
  }
}

class TantivyDirectInterface(val schema: StructType, restoredIndexPath: Option[Path] = None) extends AutoCloseable {
  private val logger = LoggerFactory.getLogger(classOf[TantivyDirectInterface])
  
  // Initialize tantivy4java library only once
  TantivyDirectInterface.ensureInitialized()
  
  // Keep schemaBuilder alive for the lifetime of this interface
  private var schemaBuilder: Option[SchemaBuilder] = None
  
  // Create appropriate index and schema based on whether this is a restored index or new one
  private val (index, tempIndexDir, needsCleanup, tantivySchema) = restoredIndexPath match {
    case Some(existingPath) =>
      // Open existing index from restored path following tantivy4java pattern
      val indexPath = existingPath.toAbsolutePath.toString
      logger.info(s"Opening existing tantivy index from: $indexPath")
      
      // Check if index exists first (following tantivy4java pattern)
      if (!Index.exists(indexPath)) {
        logger.error(s"Index does not exist at path: $indexPath")
        throw new IllegalStateException(s"Index does not exist at path: $indexPath")
      }
      
      val idx = Index.open(indexPath)
      // CRITICAL: Use the schema that's already stored in the index files
      // Do NOT create a new schema - this could cause field mismatches
      val tantivySchema = idx.getSchema()
      
      // CRITICAL: Reload the index after opening from extracted files
      // This ensures the index sees all committed documents
      idx.reload()
      logger.info(s"Successfully opened existing index, using stored schema, and reloaded")
      
      // Check documents are visible after reload
      Using.resource(idx.searcher()) { searcher =>
        val numDocs = searcher.getNumDocs()
        logger.info(s"Restored index contains $numDocs documents after reload")
        if (numDocs == 0) {
          logger.error(s"CRITICAL: Restored index has 0 documents after reload - this indicates a restoration problem")
        }
      }
      
      (idx, existingPath, true, tantivySchema) // Clean up extracted directory
    
    case None =>
      // Create new index in temporary directory following tantivy4java exact pattern
      val tempDir = Files.createTempDirectory("tantivy4spark_idx_")
      logger.info(s"Creating new tantivy index at: ${tempDir.toAbsolutePath}")
      
      // Synchronize schema creation to prevent race conditions in field ID generation
      val (tantivySchema, builder) = TantivyDirectInterface.createSchemaThreadSafe(schema)
      schemaBuilder = Some(builder)  // Store for cleanup later
      
      val idx = new Index(tantivySchema, tempDir.toAbsolutePath.toString, false)
      (idx, tempDir, true, tantivySchema)
  }
  
  // Use ThreadLocal to ensure each Spark task gets its own IndexWriter - no sharing between tasks
  private val threadLocalWriter = new ThreadLocal[IndexWriter]()
  
  private def getOrCreateWriter(): IndexWriter = {
    Option(threadLocalWriter.get()) match {
      case Some(writer) => writer
      case None =>
        val writer = index.writer(50, 1) // Small memory arena like tantivy4java tests
        threadLocalWriter.set(writer)
        logger.debug(s"Created new IndexWriter for Spark task thread ${Thread.currentThread().getName}")
        writer
    }
  }
  
  def addDocument(row: InternalRow): Unit = {
    // Each Spark task gets its own IndexWriter via ThreadLocal - no sharing between tasks
    val document = new Document()
    
    try {
      // Convert InternalRow directly to Document without JSON - protect against field processing errors
      logger.debug(s"Adding document with ${schema.fields.length} Spark schema fields")
      schema.fields.zipWithIndex.foreach { case (field, index) =>
        try {
          val value = row.get(index, field.dataType)
          if (value != null) {
            logger.debug(s"Adding field ${field.name} (type: ${field.dataType}) with value: $value")
            addFieldToDocument(document, field.name, value, field.dataType)
          } else {
            logger.debug(s"Skipping null field ${field.name}")
          }
        } catch {
          case ex: Exception =>
            logger.error(s"Failed to add field ${field.name} (type: ${field.dataType}) at index $index: ${ex.getMessage}")
            logger.error(s"Document created from tantivy schema, Spark schema has ${schema.fields.length} fields")
            logger.error(s"Available tantivy schema: ${tantivySchema}")
            throw ex
        }
      }
      
      val writer = getOrCreateWriter()
      writer.addDocument(document)
      logger.debug(s"Added document to index using task-local writer")
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to add document: ${ex.getMessage}")
        throw ex
    } finally {
      // Only close the document - keep writer alive for more documents in this task
      try {
        document.close()
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to close document: ${ex.getMessage}")
      }
    }
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
    // Commit the ThreadLocal writer for this task
    Option(threadLocalWriter.get()).foreach { writer =>
      writer.commit()
      logger.info(s"Committed IndexWriter for task thread ${Thread.currentThread().getName}")
    }
    index.reload()
    logger.info("Index reloaded after task writer commit")
  }
  
  def searchAll(limit: Int = 10000): Array[InternalRow] = {
    // Handle edge case where limit is 0 or negative
    if (limit <= 0) {
      logger.warn(s"Invalid limit $limit, returning empty results")
      return Array.empty[InternalRow]
    }
    
    try {
      Using.resource(index.searcher()) { searcher =>
        val numDocs = searcher.getNumDocs()
        logger.info(s"Searcher reports $numDocs documents in index")
        
        if (numDocs == 0) {
          logger.warn("Index contains 0 documents - returning empty results")
          return Array.empty[InternalRow]
        }
        
        Using.resource(Query.allQuery()) { query =>
          Using.resource(searcher.search(query, limit)) { searchResult =>
            val hits = searchResult.getHits()
            logger.info(s"Search found ${hits.size()} hits out of $numDocs total docs (limit=$limit)")
            val results = mutable.ArrayBuffer[InternalRow]()
            
            for (i <- 0 until hits.size()) {
              val hit = hits.get(i)
              val docAddress = hit.getDocAddress()
              
              Using.resource(searcher.doc(docAddress)) { document =>
                val row = convertDocumentToInternalRow(document)
                results += row
                logger.debug(s"Converted document $i to row")
              }
            }
            
            logger.info(s"Returning ${results.size} results from search (from $numDocs total docs)")
            results.toArray
          }
        }
      }
    } catch {
      case ex: Exception =>
        logger.error("Search failed", ex)
        Array.empty[InternalRow]
    }
  }
  
  def search(queryString: String, limit: Int = 100): Array[InternalRow] = {
    // Handle edge case where limit is 0 or negative
    if (limit <= 0) {
      logger.warn(s"Invalid limit $limit, returning empty results")
      return Array.empty[InternalRow]
    }
    
    if (queryString.isEmpty || queryString == "*:*") {
      return searchAll(limit)
    }
    
    try {
      Using.resource(index.searcher()) { searcher =>
        Using.resource(Query.allQuery()) { query => // For now, use allQuery - proper query parsing can be added later
          Using.resource(searcher.search(query, limit)) { searchResult =>
            val hits = searchResult.getHits()
            val results = mutable.ArrayBuffer[InternalRow]()
            
            for (i <- 0 until hits.size()) {
              val hit = hits.get(i)
              val docAddress = hit.getDocAddress()
              
              Using.resource(searcher.doc(docAddress)) { document =>
                val row = convertDocumentToInternalRow(document)
                results += row
              }
            }
            
            results.toArray
          }
        }
      }
    } catch {
      case ex: Exception =>
        logger.error("Search failed", ex)
        Array.empty[InternalRow]
    }
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
    // Create a ZIP archive containing all tantivy index files
    val zipData = createZipArchive(tempIndexDir)
    
    // Return the ZIP as a single component
    Map("tantivy_index.zip" -> zipData)
  }
  
  private def createZipArchive(indexDir: Path): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val zos = new ZipOutputStream(baos)
    
    try {
      val indexFiles = indexDir.toFile.listFiles()
      if (indexFiles != null) {
        indexFiles.foreach { file =>
          if (file.isFile) {
            val entry = new ZipEntry(file.getName)
            zos.putNextEntry(entry)
            
            val fileData = Files.readAllBytes(file.toPath)
            zos.write(fileData)
            zos.closeEntry()
            
            logger.debug(s"Added to ZIP: ${file.getName} (${fileData.length} bytes)")
          }
        }
      }
      
      zos.finish()
      val zipData = baos.toByteArray
      logger.info(s"Created ZIP archive with ${indexFiles.length} files (${zipData.length} bytes)")
      zipData
      
    } finally {
      zos.close()
      baos.close()
    }
  }
  
  override def close(): Unit = {
    // Close ThreadLocal writer for current task if it exists
    Option(threadLocalWriter.get()).foreach { writer =>
      writer.close()
      logger.debug(s"Closed IndexWriter for task thread ${Thread.currentThread().getName}")
    }
    threadLocalWriter.remove()
    
    index.close()
    
    // Close schemaBuilder if it exists
    schemaBuilder.foreach(_.close())
    schemaBuilder = None
    
    // Clean up temporary directory
    if (needsCleanup && tempIndexDir != null) {
      try {
        deleteRecursively(tempIndexDir.toFile)
        logger.debug(s"Cleaned up temporary index directory: ${tempIndexDir.toAbsolutePath}")
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to clean up temporary directory: ${ex.getMessage}")
      }
    }
  }
  
  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}