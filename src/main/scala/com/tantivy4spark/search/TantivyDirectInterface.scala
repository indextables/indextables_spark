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
// ZIP imports removed - using splits instead of archives
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
            // Try "raw" tokenizer which preserves the text as-is without complex processing
            logger.debug(s"Adding text field '$fieldName' with raw tokenizer for better searchability")
            builder.addTextField(fieldName, true, false, "raw", "position")
          case org.apache.spark.sql.types.LongType | org.apache.spark.sql.types.IntegerType =>
            builder.addIntegerField(fieldName, true, true, true)
          case org.apache.spark.sql.types.DoubleType | org.apache.spark.sql.types.FloatType =>
            builder.addFloatField(fieldName, true, true, true)
          case org.apache.spark.sql.types.BooleanType =>
            builder.addBooleanField(fieldName, true, true, true)
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
  
  // Note: fromIndexComponents and extractZipArchive removed - using splits instead of ZIP archives
}

class TantivyDirectInterface(val schema: StructType, restoredIndexPath: Option[Path] = None) extends AutoCloseable {
  private val logger = LoggerFactory.getLogger(classOf[TantivyDirectInterface])
  
  // Initialize tantivy4java library only once
  TantivyDirectInterface.ensureInitialized()
  
  // Keep schemaBuilder alive for the lifetime of this interface
  private var schemaBuilder: Option[SchemaBuilder] = None
  
  // Flag to prevent cleanup until split is created
  @volatile private var delayCleanup = false
  
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
        if (fieldName == "title") { // Debug logging for title field
          logger.debug(s"Adding title field: '$str'")
        }
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
    // Commit and close the ThreadLocal writer for this task
    Option(threadLocalWriter.get()).foreach { writer =>
      writer.commit()
      writer.close() // Close the writer after commit so files are fully flushed
      logger.info(s"Committed and closed IndexWriter for task thread ${Thread.currentThread().getName}")
    }
    threadLocalWriter.remove() // Remove from ThreadLocal since it's now closed
    
    // Reload the index to make committed documents visible
    index.reload()
    logger.info("Index reloaded after task writer commit and close")
    
    // Add a small delay to ensure all files are fully written to disk
    // This matches the pattern used in tantivy4java tests
    try {
      Thread.sleep(100)
      logger.debug("Waited 100ms for index files to be fully written to disk")
    } catch {
      case _: InterruptedException => // Ignore interruption
    }
  }
  
  /**
   * Close the index after commit for production use (write-only pattern).
   * This is called by TantivySearchEngine.commitAndCreateSplit() after split creation.
   */
  def commitAndClose(): Unit = {
    commit()
    // Close the index completely - we don't need it for reading since we use splits
    index.close()
    logger.info("Index closed after commit - all reading will be done from splits")
  }
  
  /**
   * Search operations are not supported on write-only indexes.
   * Use SplitSearchEngine to read from split files instead.
   */
  @deprecated("Direct index search is not supported. Create splits and use SplitSearchEngine.", "split-migration")
  def searchAll(limit: Int = Int.MaxValue): Array[InternalRow] = {
    throw new UnsupportedOperationException(
      "Direct index search is not supported in write-only architecture. " +
      "Use TantivySearchEngine.commitAndCreateSplit() to create a split, " +
      "then use SplitSearchEngine.fromSplitFile() to read from the split."
    )
  }
  
  /**
   * Search operations are not supported on write-only indexes.
   * Use SplitSearchEngine to read from split files instead.
   */
  @deprecated("Direct index search is not supported. Create splits and use SplitSearchEngine.", "split-migration")
  def search(queryString: String, limit: Int = 100): Array[InternalRow] = {
    throw new UnsupportedOperationException(
      "Direct index search is not supported in write-only architecture. " +
      "Use TantivySearchEngine.commitAndCreateSplit() to create a split, " +
      "then use SplitSearchEngine.fromSplitFile() to read from the split."
    )
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
    if (value == null) {
      return null
    }
    
    try {
      dataType match {
        case org.apache.spark.sql.types.StringType => 
          org.apache.spark.unsafe.types.UTF8String.fromString(value.toString)
        case org.apache.spark.sql.types.IntegerType => 
          value match {
            case i: java.lang.Integer => i.intValue()
            case l: java.lang.Long => l.intValue()
            case s: String => 
              try { s.toInt } catch { case _: NumberFormatException => 0 }
            case _ => 0
          }
        case org.apache.spark.sql.types.LongType => 
          value match {
            case l: java.lang.Long => l.longValue()
            case i: java.lang.Integer => i.longValue()
            case s: String => 
              try { s.toLong } catch { case _: NumberFormatException => 0L }
            case _ => 0L
          }
        case org.apache.spark.sql.types.DoubleType => 
          value match {
            case d: java.lang.Double => d.doubleValue()
            case f: java.lang.Float => f.doubleValue()
            case s: String => 
              try { s.toDouble } catch { case _: NumberFormatException => 0.0 }
            case i: java.lang.Integer => i.doubleValue()
            case l: java.lang.Long => l.doubleValue()
            case _ => 0.0
          }
        case org.apache.spark.sql.types.FloatType => 
          value match {
            case f: java.lang.Float => f.floatValue()
            case d: java.lang.Double => d.floatValue()
            case s: String => 
              try { s.toFloat } catch { case _: NumberFormatException => 0.0f }
            case i: java.lang.Integer => i.floatValue()
            case l: java.lang.Long => l.floatValue()
            case _ => 0.0f
          }
        case org.apache.spark.sql.types.BooleanType => 
          value match {
            case b: java.lang.Boolean => b.booleanValue()
            case i: java.lang.Integer => i != 0
            case l: java.lang.Long => l != 0
            case s: String => s.toLowerCase == "true" || s == "1"
            case _ => false
          }
        case org.apache.spark.sql.types.TimestampType => 
          value match {
            case l: java.lang.Long => l.longValue() * 1000L // Convert millis to microseconds for Spark InternalRow
            case i: java.lang.Integer => i.longValue() * 1000L
            case s: String => 
              try { s.toLong * 1000L } catch { case _: NumberFormatException => 0L }
            case _ => 0L
          }
        case org.apache.spark.sql.types.DateType => 
          value match {
            case i: java.lang.Integer => i.intValue() // Days since epoch - already correct for Spark
            case l: java.lang.Long => l.intValue()
            case s: String => 
              try { s.toInt } catch { case _: NumberFormatException => 0 }
            case _ => 0
          }
        case org.apache.spark.sql.types.BinaryType =>
          value match {
            case bytes: Array[Byte] => bytes
            case _ => Array.empty[Byte]
          }
        case _ => value
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to convert value $value (${value.getClass.getSimpleName}) to $dataType, using default: ${e.getMessage}")
        // Return appropriate default value for the type
        dataType match {
          case org.apache.spark.sql.types.StringType => org.apache.spark.unsafe.types.UTF8String.fromString("")
          case org.apache.spark.sql.types.IntegerType => 0
          case org.apache.spark.sql.types.LongType => 0L
          case org.apache.spark.sql.types.DoubleType => 0.0
          case org.apache.spark.sql.types.FloatType => 0.0f
          case org.apache.spark.sql.types.BooleanType => false
          case org.apache.spark.sql.types.TimestampType => 0L
          case org.apache.spark.sql.types.DateType => 0
          case org.apache.spark.sql.types.BinaryType => Array.empty[Byte]
          case _ => null
        }
    }
  }
  
  // Note: getIndexComponents and createZipArchive removed - using splits instead of ZIP archives
  
  /**
   * Get the path to the index directory for split creation.
   */
  def getIndexPath(): String = {
    tempIndexDir.toAbsolutePath.toString
  }
  
  /**
   * Delay cleanup to allow split creation from the index directory.
   */
  def delayCleanupForSplit(): Unit = {
    delayCleanup = true
  }
  
  /**
   * Allow cleanup after split creation is complete.
   */
  def allowCleanup(): Unit = {
    delayCleanup = false
  }
  
  /**
   * Force cleanup of temporary directory (for use after split creation).
   */
  def forceCleanup(): Unit = {
    delayCleanup = false
    if (needsCleanup && tempIndexDir != null) {
      try {
        deleteRecursively(tempIndexDir.toFile)
        logger.debug(s"Force cleaned up temporary index directory: ${tempIndexDir.toAbsolutePath}")
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to force clean up temporary directory: ${ex.getMessage}")
      }
    }
  }
  
  override def close(): Unit = {
    // Close ThreadLocal writer for current task if it exists
    Option(threadLocalWriter.get()).foreach { writer =>
      try {
        writer.close()
        logger.debug(s"Closed IndexWriter for task thread ${Thread.currentThread().getName}")
      } catch {
        case _: Exception => // Writer may already be closed
      }
    }
    threadLocalWriter.remove()
    
    // Close index if it hasn't been closed already
    try {
      index.close()
      logger.debug("Closed index in cleanup")
    } catch {
      case _: Exception => // Index may already be closed from commit()
    }
    
    // Close schemaBuilder if it exists
    schemaBuilder.foreach(_.close())
    schemaBuilder = None
    
    // Clean up temporary directory (unless cleanup is delayed for split creation)
    if (needsCleanup && tempIndexDir != null && !delayCleanup) {
      try {
        deleteRecursively(tempIndexDir.toFile)
        logger.debug(s"Cleaned up temporary index directory: ${tempIndexDir.toAbsolutePath}")
      } catch {
        case ex: Exception =>
          logger.warn(s"Failed to clean up temporary directory: ${ex.getMessage}")
      }
    } else if (delayCleanup) {
      logger.debug(s"Delaying cleanup of temporary index directory for split creation: ${tempIndexDir.toAbsolutePath}")
    }
  }
  
  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}