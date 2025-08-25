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

import com.tantivy4java.{SplitSearcher, SplitCacheManager, Query, SearchResult, Schema}
import com.tantivy4spark.storage.{GlobalSplitCacheManager, SplitCacheConfig}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import scala.util.{Try, Success, Failure}

/**
 * Search engine implementation using tantivy4java's SplitSearcher with global caching.
 * 
 * This replaces the previous TantivySearchEngine which used zip-based archives.
 * The new implementation uses .split files with optimized caching and native storage access.
 */
class SplitSearchEngine(
  sparkSchema: StructType,
  splitPath: String,
  cacheConfig: SplitCacheConfig = SplitCacheConfig()
) extends AutoCloseable {
  
  private val logger = LoggerFactory.getLogger(classOf[SplitSearchEngine])
  
  logger.debug(s"SplitSearchEngine creating cache manager with config:")
  logger.debug(s"  - cacheName: ${cacheConfig.cacheName}")
  logger.debug(s"  - awsAccessKey: ${cacheConfig.awsAccessKey.map(k => s"${k.take(4)}...").getOrElse("None")}")
  logger.debug(s"  - awsSecretKey: ${cacheConfig.awsSecretKey.map(_ => "***").getOrElse("None")}")
  logger.debug(s"  - awsRegion: ${cacheConfig.awsRegion.getOrElse("None")}")
  logger.debug(s"  - awsSessionToken: ${cacheConfig.awsSessionToken.map(_ => "***").getOrElse("None")}")
  logger.debug(s"  - awsEndpoint: ${cacheConfig.awsEndpoint.getOrElse("None")}")
  
  // Get the global cache manager for this configuration
  private val cacheManager = GlobalSplitCacheManager.getInstance(cacheConfig)
  
  // Create the split searcher using the shared cache
  logger.info(s"🔧 About to call cacheManager.createSplitSearcher() for path: $splitPath")
  protected val splitSearcher = try {
    cacheManager.createSplitSearcher(splitPath)
  } catch {
    case ex: RuntimeException if ex.getMessage.contains("region must be set") =>
      logger.error(s"❌ CONFIRMED: tantivy4java region error when creating SplitSearcher for $splitPath")
      logger.error(s"❌ This proves the AWS region is not being passed to tantivy4java properly!")
      logger.error(s"❌ cacheConfig had: awsRegion=${cacheConfig.awsRegion.getOrElse("None")}, awsEndpoint=${cacheConfig.awsEndpoint.getOrElse("None")}")
      throw ex
    case ex: Exception =>
      logger.error(s"❌ Failed to create SplitSearcher for $splitPath: ${ex.getMessage}")
      throw ex
  }
  
  // Row converter for converting search results to Spark InternalRows
  // TODO: Implement RowConverter for SplitSearchEngine
  // private val rowConverter = new com.tantivy4spark.search.RowConverter(sparkSchema)
  
  logger.info(s"Created SplitSearchEngine for $splitPath using cache: ${cacheConfig.cacheName}")
  
  /**
   * Get the Tantivy schema for this split.
   */
  def getSchema(): Schema = {
    splitSearcher.getSchema()
  }
  
  /**
   * Search the split and return results as Spark InternalRows.
   */
  def search(queryString: String, limit: Int = 10): Array[InternalRow] = {
    try {
      logger.warn(s"Searching split with query: '$queryString', limit: $limit")
      
      // Parse the query string into a Tantivy Query
      val tantivySchema = splitSearcher.getSchema()
      val query = QueryParser.parseQuery(queryString, tantivySchema)
      
      // Execute the search
      val searchResult = splitSearcher.search(query, limit)
      
      // Convert search results to Spark InternalRows
      val results = convertSearchResultToRows(searchResult)
      
      // Close the search result to free resources
      searchResult.close()
      
      logger.warn(s"Search completed: ${results.length} results returned")
      results
      
    } catch {
      case e: Exception =>
        logger.error(s"Search failed for query '$queryString'", e)
        throw e
    }
  }
  
  /**
   * Search all documents in the split (equivalent to match-all query).
   */
  def searchAll(limit: Int = 1000): Array[InternalRow] = {
    try {
      logger.debug(s"Searching all documents with limit: $limit")
      
      // Create match-all query
      val query = Query.allQuery()
      
      // Execute the search
      val searchResult = splitSearcher.search(query, limit)
      
      // Convert to InternalRows
      val results = convertSearchResultToRows(searchResult)
      
      // Close the search result to free resources
      searchResult.close()
      
      logger.debug(s"SearchAll completed: ${results.length} results returned")
      results
      
    } catch {
      case e: Exception =>
        logger.error("SearchAll failed", e)
        throw e
    }
  }
  
  /**
   * Preload index components for better performance.
   */
  def preloadComponents(components: SplitSearcher.IndexComponent*): Unit = {
    try {
      logger.info(s"Preloading components: ${components.mkString(", ")}")
      splitSearcher.preloadComponents(components: _*)
    } catch {
      case e: Exception =>
        logger.warn("Failed to preload components", e)
    }
  }
  
  /**
   * Get cache statistics for this split.
   */
  def getCacheStats(): SplitSearcher.CacheStats = {
    splitSearcher.getCacheStats()
  }
  
  /**
   * Get loading statistics for this split.
   */
  def getLoadingStats(): SplitSearcher.LoadingStats = {
    splitSearcher.getLoadingStats()
  }
  
  /**
   * Validate the split file integrity.
   */
  def validateSplit(): Boolean = {
    try {
      splitSearcher.validateSplit()
    } catch {
      case e: Exception =>
        logger.warn("Split validation failed", e)
        false
    }
  }
  
  /**
   * Get metadata about the split file.
   */
  def getSplitMetadata(): Option[SplitSearcher.SplitMetadata] = {
    Try {
      splitSearcher.getSplitMetadata()
    }.toOption
  }
  
  /**
   * Convert SearchResult to InternalRows by extracting actual document field values.
   */
  private def convertSearchResultToRows(searchResult: SearchResult): Array[InternalRow] = {
    try {
      import scala.jdk.CollectionConverters._
      val hits = searchResult.getHits().asScala.toArray
      
      logger.debug(s"Converting ${hits.length} search hits to InternalRows")
      
      // Extract actual document fields using SplitSearcher's doc() method
      val rows = hits.zipWithIndex.map { case (hit, index) =>
        var document: com.tantivy4java.Document = null
        try {
          // Retrieve the document using the new doc() method
          val docAddress = hit.getDocAddress()
          logger.debug(s"Hit $index: DocAddress = ${docAddress}")
          document = splitSearcher.doc(docAddress)
          
          if (document != null) {
            // Extract field values from the document using safer field access
            val values = sparkSchema.fields.map { field =>
              try {
                val fieldValue = document.getFirst(field.name)
                logger.debug(s"Field '${field.name}' -> value: $fieldValue (type: ${if (fieldValue == null) "null" else fieldValue.getClass.getSimpleName})")
                convertFieldValue(fieldValue, field.dataType)
              } catch {
                case e: Exception =>
                  // Handle case where field doesn't exist in this split's schema
                  logger.debug(s"Field '${field.name}' not found in split, using null value: ${e.getMessage}")
                  convertFieldValue(null, field.dataType)
              }
            }
            org.apache.spark.sql.catalyst.InternalRow.fromSeq(values)
          } else {
            // Fallback to empty row if document retrieval fails
            createEmptyRow()
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Error retrieving document for hit: ${hit.getDocAddress()}", e)
            createEmptyRow()
        } finally {
          // Always close the document to free resources
          if (document != null) {
            try {
              document.close()
            } catch {
              case e: Exception =>
                logger.debug("Error closing document", e)
            }
          }
        }
      }
      
      logger.debug(s"Created ${rows.length} InternalRows with actual field data")
      rows
      
    } catch {
      case e: Exception =>
        logger.warn("Error converting search results", e)
        Array.empty[InternalRow]
    }
  }

  /**
   * Convert a field value from tantivy4java to Spark internal format.
   */
  private def convertFieldValue(fieldValue: Any, dataType: org.apache.spark.sql.types.DataType): Any = {
    if (fieldValue == null) {
      return null
    }
    
    dataType match {
      case org.apache.spark.sql.types.StringType => 
        org.apache.spark.unsafe.types.UTF8String.fromString(fieldValue.toString)
      case org.apache.spark.sql.types.IntegerType => 
        fieldValue match {
          case i: java.lang.Integer => i.intValue()
          case l: java.lang.Long => l.intValue()
          case s: String => s.toInt
          case _ => 0
        }
      case org.apache.spark.sql.types.LongType => 
        fieldValue match {
          case l: java.lang.Long => l.longValue()
          case i: java.lang.Integer => i.longValue()
          case s: String => s.toLong
          case _ => 0L
        }
      case org.apache.spark.sql.types.DoubleType => 
        fieldValue match {
          case d: java.lang.Double => d.doubleValue()
          case f: java.lang.Float => f.doubleValue()
          case s: String => s.toDouble
          case _ => 0.0
        }
      case org.apache.spark.sql.types.FloatType => 
        fieldValue match {
          case f: java.lang.Float => f.floatValue()
          case d: java.lang.Double => d.floatValue()
          case s: String => s.toFloat
          case _ => 0.0f
        }
      case org.apache.spark.sql.types.BooleanType => 
        fieldValue match {
          case b: java.lang.Boolean => b.booleanValue()
          case i: java.lang.Integer => i != 0
          case l: java.lang.Long => l != 0
          case s: String => s.toLowerCase == "true"
          case _ => false
        }
      case org.apache.spark.sql.types.TimestampType => 
        fieldValue match {
          case l: java.lang.Long => l.longValue()
          case i: java.lang.Integer => i.longValue()
          case s: String => s.toLong
          case _ => 0L
        }
      case org.apache.spark.sql.types.DateType => 
        fieldValue match {
          case i: java.lang.Integer => i.intValue()
          case l: java.lang.Long => l.intValue()
          case s: String => s.toInt
          case _ => 0
        }
      case _ => null
    }
  }
  
  /**
   * Create an empty row with default values for all fields.
   */
  private def createEmptyRow(): InternalRow = {
    val values = sparkSchema.fields.map { field =>
      field.dataType match {
        case org.apache.spark.sql.types.StringType => org.apache.spark.unsafe.types.UTF8String.fromString("")
        case org.apache.spark.sql.types.IntegerType => 0
        case org.apache.spark.sql.types.LongType => 0L
        case org.apache.spark.sql.types.DoubleType => 0.0
        case org.apache.spark.sql.types.FloatType => 0.0f
        case org.apache.spark.sql.types.BooleanType => false
        case org.apache.spark.sql.types.TimestampType => 0L
        case org.apache.spark.sql.types.DateType => 0
        case _ => null
      }
    }
    org.apache.spark.sql.catalyst.InternalRow.fromSeq(values)
  }

  override def close(): Unit = {
    try {
      logger.debug(s"Closing SplitSearchEngine for $splitPath")
      splitSearcher.close()
    } catch {
      case e: Exception =>
        logger.warn("Error closing SplitSearchEngine", e)
    }
  }
}

/**
 * Factory for creating SplitSearchEngine instances.
 */
object SplitSearchEngine {
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Create a SplitSearchEngine from a split file path.
   */
  def fromSplitFile(
    sparkSchema: StructType,
    splitPath: String,
    cacheConfig: SplitCacheConfig = SplitCacheConfig()
  ): SplitSearchEngine = {
    
    logger.info(s"Creating SplitSearchEngine from split file: $splitPath")
    new SplitSearchEngine(sparkSchema, splitPath, cacheConfig)
  }
  
  /**
   * Create a SplitSearchEngine with a specific cache manager.
   */
  def fromSplitFileWithCache(
    sparkSchema: StructType,
    splitPath: String,
    cacheManager: SplitCacheManager
  ): SplitSearchEngine = {
    
    logger.info(s"Creating SplitSearchEngine with provided cache manager: $splitPath")
    
    // Create a custom engine that uses the provided cache manager
    new SplitSearchEngine(sparkSchema, splitPath) {
      override val splitSearcher = cacheManager.createSplitSearcher(splitPath)
    }
  }
}
