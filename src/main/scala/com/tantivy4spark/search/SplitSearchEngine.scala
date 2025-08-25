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
import com.tantivy4spark.schema.SchemaMapping
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
   * Search the split using a programmatic Query object and return results as Spark InternalRows.
   * This is the new preferred method that bypasses string parsing.
   */
  def search(query: Query, limit: Int = 10): Array[InternalRow] = {
    try {
      logger.warn(s"Searching split with Query object: ${query.getClass.getSimpleName}, limit: $limit")
      
      val tantivySchema = splitSearcher.getSchema()
      logger.warn(s"Available fields in schema: ${tantivySchema.getFieldNames()}")
      
      // Execute the search directly with the Query object
      val searchResult = splitSearcher.search(query, limit)
      
      // Convert search results to Spark InternalRows
      val results = convertSearchResultToRows(searchResult)
      
      // Close the search result to free resources
      searchResult.close()
      
      logger.warn(s"Search completed: ${results.length} results returned")
      results
      
    } catch {
      case e: Exception =>
        logger.error(s"Search failed for Query object: ${query.getClass.getSimpleName}", e)
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
   * Convert SearchResult to InternalRows using the new SchemaMapping system.
   */
  private def convertSearchResultToRows(searchResult: SearchResult): Array[InternalRow] = {
    try {
      import scala.jdk.CollectionConverters._
      val hits = searchResult.getHits().asScala.toArray
      
      logger.warn(s"Converting ${hits.length} search hits to InternalRows using SchemaMapping")
      logger.warn(s"Split file path: $splitPath")
      
      // Get the split schema once
      val splitSchema = splitSearcher.getSchema()
      logger.warn(s"Split schema from file: ${splitSchema.getFieldNames().toArray.mkString(", ")}")
      logger.warn(s"Expected Spark schema: ${sparkSchema.fields.map(_.name).mkString(", ")}")
      
      // Convert each hit using the new schema mapping
      val rows = hits.zipWithIndex.map { case (hit, index) =>
        var document: com.tantivy4java.Document = null
        try {
          // Retrieve the document
          val docAddress = hit.getDocAddress()
          logger.debug(s"Hit $index: DocAddress = ${docAddress}")
          document = splitSearcher.doc(docAddress)
          
          if (document != null) {
            // Use the new SchemaMapping.Read.convertDocument method
            val values = SchemaMapping.Read.convertDocument(document, splitSchema, sparkSchema)
            org.apache.spark.sql.catalyst.InternalRow.fromSeq(values)
          } else {
            // Fallback to empty row if document retrieval fails
            createEmptyRow()
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Error converting document for hit: ${hit.getDocAddress()}", e)
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
      
      logger.debug(s"Created ${rows.length} InternalRows using SchemaMapping system")
      rows
      
    } catch {
      case e: Exception =>
        logger.warn("Error converting search results", e)
        Array.empty[InternalRow]
    }
  }

  
  /**
   * Create an empty row with default values for all fields using SchemaMapping.
   */
  private def createEmptyRow(): InternalRow = {
    val values = sparkSchema.fields.map { field =>
      SchemaMapping.Read.getDefaultValue(field.dataType)
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
