package com.tantivy4spark.search

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow
import org.slf4j.LoggerFactory
import scala.util.{Try, Success, Failure}

class TantivySearchEngine(schema: StructType, existingHandle: Long = -1) extends AutoCloseable {
  private val logger = LoggerFactory.getLogger(classOf[TantivySearchEngine])
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  
  private val schemaJson = SchemaConverter.sparkToTantivySchema(schema)
  private val indexHandle: Long = {
    if (!TantivyNative.ensureLibraryLoaded()) {
      throw new RuntimeException("Tantivy native library not available")
    }
    
    if (existingHandle != -1) {
      existingHandle
    } else {
      val handle = TantivyNative.createIndex(schemaJson)
      if (handle == -1) {
        throw new RuntimeException("Failed to create Tantivy index")
      }
      handle
    }
  }
  
  // Keep existing constructor for backwards compatibility
  def this(schema: StructType) = this(schema, -1)

  def addDocument(row: InternalRow): Unit = {
    val docJson = RowConverter.internalRowToJson(row, schema, mapper)
    val success = TantivyNative.addDocument(indexHandle, docJson)
    if (!success) {
      logger.warn(s"Failed to add document: $docJson")
    }
  }

  def addDocuments(rows: Iterator[InternalRow]): Unit = {
    rows.foreach(addDocument)
  }

  def commit(): Unit = {
    val success = TantivyNative.commit(indexHandle)
    if (!success) {
      throw new RuntimeException("Failed to commit index")
    }
  }
  
  def commitAndGetComponents(): Map[String, Array[Byte]] = {
    commit()
    
    // Get index components from the native library
    val componentsJson = TantivyNative.getIndexComponents(indexHandle)
    if (componentsJson == null) {
      throw new RuntimeException("Failed to get index components")
    }
    
    Try {
      val components = mapper.readValue(componentsJson, classOf[Map[String, String]])
      
      // Convert base64-encoded components to byte arrays
      components.map { case (name, base64Data) =>
        name -> java.util.Base64.getDecoder.decode(base64Data)
      }
    } match {
      case Success(components) => 
        logger.info(s"Retrieved ${components.size} index components: ${components.keys.mkString(", ")}")
        components
      case Failure(ex) =>
        logger.error("Failed to parse index components", ex)
        throw new RuntimeException("Failed to parse index components", ex)
    }
  }

  def searchAll(limit: Int = 10000): Array[InternalRow] = {
    // Try multiple approaches to get all documents
    val strategies = Array(
      () => search("", limit),  // Empty query
      () => search("*:*", limit), // Lucene-style match all
      () => searchViaAllQuery(limit) // Use native all query
    )
    
    for (strategy <- strategies) {
      try {
        val results = strategy()
        if (results.nonEmpty) {
          return results
        }
      } catch {
        case _: Exception => // Continue to next strategy
      }
    }
    
    // If all strategies fail, return empty
    Array.empty[InternalRow]
  }
  
  private def searchViaAllQuery(limit: Int): Array[InternalRow] = {
    // This will call a native method that uses Tantivy's AllQuery
    val resultsJson = TantivyNative.searchAll(indexHandle, limit)
    if (resultsJson == null) {
      Array.empty[InternalRow]
    } else {
      Try {
        val resultObj = mapper.readTree(resultsJson)
        val hits = resultObj.get("hits")
        val rows = (0 until hits.size()).map { i =>
          val hit = hits.get(i)
          val doc = hit.get("doc")
          val docMap = mapper.convertValue(doc, classOf[Map[String, Any]])
          RowConverter.jsonToInternalRow(docMap, schema)
        }
        rows.toArray
      } match {
        case Success(rows) => rows
        case Failure(ex) =>
          logger.error("Failed to parse search results from searchAll", ex)
          Array.empty[InternalRow]
      }
    }
  }

  def search(query: String, limit: Int = 100): Array[InternalRow] = {
    val resultsJson = TantivyNative.search(indexHandle, query, limit)
    if (resultsJson == null) {
      Array.empty[InternalRow]
    } else {
      Try {
        val resultObj = mapper.readTree(resultsJson)
        val hits = resultObj.get("hits")
        val rows = (0 until hits.size()).map { i =>
          val hit = hits.get(i)
          val doc = hit.get("doc")
          val docMap = mapper.convertValue(doc, classOf[Map[String, Any]])
          RowConverter.jsonToInternalRow(docMap, schema)
        }
        rows.toArray
      } match {
        case Success(rows) => rows
        case Failure(ex) =>
          logger.error("Failed to parse search results", ex)
          Array.empty[InternalRow]
      }
    }
  }

  override def close(): Unit = {
    TantivyNative.closeIndex(indexHandle)
  }
}

object TantivySearchEngine {
  private val logger = LoggerFactory.getLogger(TantivySearchEngine.getClass)
  
  /**
   * Creates a TantivySearchEngine from pre-existing index components.
   * This is used when reading from .tnt4s archive files.
   */
  def fromIndexComponents(schema: StructType, components: Map[String, Array[Byte]]): TantivySearchEngine = {
    val schemaJson = SchemaConverter.sparkToTantivySchema(schema)
    
    // Convert components to base64 for passing to native layer
    val componentsBase64 = components.map { case (name, data) =>
      name -> java.util.Base64.getEncoder.encodeToString(data)
    }
    
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val componentsJson = mapper.writeValueAsString(componentsBase64)
    
    val handle = TantivyNative.createIndexFromComponents(schemaJson, componentsJson)
    if (handle == -1) {
      throw new RuntimeException("Failed to create Tantivy index from components")
    }
    
    logger.info(s"Created TantivySearchEngine from ${components.size} components")
    new TantivySearchEngine(schema, handle)
  }
}