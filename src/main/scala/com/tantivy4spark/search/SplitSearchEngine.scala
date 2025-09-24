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

import com.tantivy4java.{SplitSearcher, SplitCacheManager, SplitQuery, SplitMatchAllQuery, SplitTermQuery, SplitBooleanQuery, SearchResult, Schema}
import com.tantivy4spark.storage.{GlobalSplitCacheManager, SplitCacheConfig}
import com.tantivy4spark.schema.SchemaMapping
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import scala.util.{Try}

/**
 * Search engine implementation using tantivy4java's SplitSearcher with global caching.
 * 
 * This replaces the previous TantivySearchEngine which used zip-based archives.
 * The new implementation uses .split files with optimized caching and native storage access.
 * 
 * Private constructor - use SplitSearchEngine.fromSplitFileWithMetadata() instead.
 */
class SplitSearchEngine private(
  sparkSchema: StructType,
  splitPath: String,
  metadata: com.tantivy4java.QuickwitSplit.SplitMetadata,
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
  logger.info(s"🔧 Creating SplitSearchEngine for path: $splitPath")

  // Validate metadata is provided and non-null
  if (metadata == null) {
    throw new IllegalArgumentException(s"Split metadata cannot be null for createSplitSearcher. Path: $splitPath. Use SplitSearchEngine.fromSplitFileWithMetadata() to create with proper metadata.")
  }

  // Validate footer offsets are present and non-zero
  if (!metadata.hasFooterOffsets()) {
    throw new IllegalArgumentException(s"Split metadata for $splitPath does not contain required footer offsets. The split file may have been created with an older version of tantivy4java that didn't generate footer offset metadata. Please recreate the split file with the current version.")
  }

  // Validate footer offsets are non-zero (meaningful)
  val footerStartOffset = metadata.getFooterStartOffset()
  val footerEndOffset = metadata.getFooterEndOffset()
  // Hotcache methods deprecated in v0.24.1 - using footer offsets instead
  val hotcacheStartOffset = footerStartOffset // Fallback to footer offset
  val hotcacheLength = footerEndOffset - footerStartOffset // Calculate from footer range

  if (footerStartOffset <= 0 || footerEndOffset <= 0) {
    throw new IllegalArgumentException(s"Split metadata has invalid footer offsets. footerStartOffset: $footerStartOffset, footerEndOffset: $footerEndOffset. These must be positive non-zero values.")
  }

  if (footerEndOffset <= footerStartOffset) {
    throw new IllegalArgumentException(s"Split metadata has invalid footer offsets. footerEndOffset ($footerEndOffset) must be greater than footerStartOffset ($footerStartOffset).")
  }

  if (hotcacheStartOffset <= 0 || hotcacheLength <= 0) {
    throw new IllegalArgumentException(s"Split metadata has invalid hotcache offsets. hotcacheStartOffset: $hotcacheStartOffset, hotcacheLength: $hotcacheLength. These must be positive non-zero values.")
  }

  // Validate document mapping is available
  if (metadata.getDocMappingJson() == null || metadata.getDocMappingJson().trim.isEmpty) {
    throw new IllegalArgumentException(s"Split metadata must contain valid document mapping JSON for $splitPath. Document mapping is required for proper field extraction.")
  }

  logger.info(s"✅ Split metadata validation passed for $splitPath:")
  logger.info(s"   Footer offsets: $footerStartOffset - $footerEndOffset")
  logger.info(s"   Hotcache: $hotcacheStartOffset + $hotcacheLength")
  logger.info(s"   Document mapping: ${metadata.getDocMappingJson().length} chars")
  
  protected lazy val splitSearcher = try {
    logger.info(s"📋 Using metadata for $splitPath with footer offsets: ${metadata.hasFooterOffsets()}")
    cacheManager.createSplitSearcher(splitPath, metadata)
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
   * Search the split using a SplitQuery object and return results as Spark InternalRows.
   * This is the new preferred method that uses tantivy4java's efficient SplitQuery API.
   */
  def search(splitQuery: SplitQuery, limit: Int = 100): Array[InternalRow] = {
    try {
      logger.debug(s"Searching split with SplitQuery object, limit: $limit")
      logger.warn(s"Running SplitQuery: ${splitQuery.toString()}")
      
      val tantivySchema = splitSearcher.getSchema()
      logger.debug(s"Available fields in schema: ${tantivySchema.getFieldNames()}")
      
      // Execute the search using the new SplitQuery API
      val searchResult = splitSearcher.search(splitQuery, limit)
      
      // Convert search results to Spark InternalRows
      val results = convertSearchResultToRows(searchResult)
      
      // Close the search result to free resources
      searchResult.close()
      
      logger.debug(s"Search completed: ${results.length} results returned")
      results
      
    } catch {
      case e: Exception =>
        logger.error(s"Search failed for SplitQuery object", e)
        throw e
    }
  }

  
  
  /**
   * Search all documents in the split (equivalent to match-all query).
   */
  def searchAll(limit: Int = 1000): Array[InternalRow] = {
    try {
      logger.debug(s"Searching all documents with limit: $limit")
      
      // Create match-all query using SplitMatchAllQuery
      val splitQuery = new SplitMatchAllQuery()
      logger.warn(s"Running match-all SplitQuery: ${splitQuery.toString()}")
      
      // Execute the search using SplitQuery
      val searchResult = splitSearcher.search(splitQuery, limit)
      
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
   * Parse a query string using the split's schema and return a SplitQuery.
   * This is the preferred way to create queries for split searching.
   */
  def parseQuery(queryString: String): SplitQuery = {
    splitSearcher.parseQuery(queryString)
  }

  /**
   * Parse a query string with field-specific search using the enhanced SplitSearcher API.
   * This constrains the search to the specified field using the new parseQuery methods.
   */
  def parseQuery(queryString: String, fieldName: String): SplitQuery = {
    // Use the enhanced SplitSearcher parseQuery method with single field
    splitSearcher.parseQuery(queryString, fieldName)
  }

  /**
   * Parse a query string with multi-field search using the enhanced SplitSearcher API.
   * This constrains the search to the specified fields using the new parseQuery methods.
   */
  def parseQuery(queryString: String, fieldNames: java.util.List[String]): SplitQuery = {
    // Use the enhanced SplitSearcher parseQuery method with multiple fields
    splitSearcher.parseQuery(queryString, fieldNames)
  }

  /**
   * Search using a query string. The string will be parsed into a SplitQuery.
   */
  def searchByString(queryString: String, limit: Int = 100): Array[InternalRow] = {
    val splitQuery = parseQuery(queryString)
    search(splitQuery, limit)
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
      
      logger.debug(s"Converting ${hits.length} search hits to InternalRows using SchemaMapping")
      logger.debug(s"Split file path: $splitPath")
      
      // Get the split schema once
      val splitSchema = splitSearcher.getSchema()
      logger.debug(s"Split schema from file: ${splitSchema.getFieldNames().toArray.mkString(", ")}")
      logger.debug(s"Expected Spark schema: ${sparkSchema.fields.map(_.name).mkString(", ")}")
      
      // Collect all DocAddresses first
      val docAddresses = hits.map(hit => hit.getDocAddress())

      // Use configurable document retrieval strategy
      val documents: Array[com.tantivy4java.Document] = if (cacheConfig.enableDocBatch && docAddresses.length > 1) {
        logger.debug(s"Using docBatch for efficient bulk retrieval: ${docAddresses.length} addresses (max batch size: ${cacheConfig.docBatchMaxSize})")

        // Process in batches to respect maximum batch size
        val batches = docAddresses.grouped(cacheConfig.docBatchMaxSize).toArray
        logger.debug(s"Processing ${batches.length} batch(es) of documents")

        batches.flatMap { batchAddresses =>
          try {
            import scala.jdk.CollectionConverters._
            val javaAddresses = batchAddresses.toList.asJava
            val javaDocuments = splitSearcher.docBatch(javaAddresses)
            javaDocuments.asScala
          } catch {
            case e: Exception =>
              logger.warn(s"Error retrieving document batch of size ${batchAddresses.length}, falling back to individual retrieval", e)
              // Fallback to individual doc calls for this batch
              batchAddresses.map { address =>
                try {
                  splitSearcher.doc(address)
                } catch {
                  case ex: Exception =>
                    logger.warn(s"Error retrieving individual document for address: $address", ex)
                    null
                }
              }
          }
        }
      } else {
        logger.debug(s"Using individual document retrieval: enableDocBatch=${cacheConfig.enableDocBatch}, docCount=${docAddresses.length}")
        // Use individual doc calls when batch is disabled or only one document
        hits.map { hit =>
          try {
            splitSearcher.doc(hit.getDocAddress())
          } catch {
            case ex: Exception =>
              logger.warn(s"Error retrieving individual document for address: ${hit.getDocAddress()}", ex)
              null
          }
        }
      }

      // Convert documents to InternalRows
      val rows = documents.zipWithIndex.map { case (document, index) =>
        try {
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
            logger.warn(s"Error converting document at index $index", e)
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

  /**
   * Get the underlying SplitSearcher for direct operations like warmup.
   * This provides access to advanced features like cache warming.
   */
  def getSplitSearcher(): SplitSearcher = {
    splitSearcher
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
   * Create a SplitSearchEngine with footer offset optimization (87% network traffic reduction).
   * Uses pre-computed metadata to enable lazy loading with minimal initial network traffic.
   */
  def fromSplitFileWithMetadata(
    sparkSchema: StructType,
    splitPath: String,
    metadata: com.tantivy4java.QuickwitSplit.SplitMetadata,
    cacheConfig: SplitCacheConfig = SplitCacheConfig()
  ): SplitSearchEngine = {
    
    if (metadata != null && metadata.hasFooterOffsets()) {
      logger.info(s"🚀 OPTIMIZED LOADING: Creating SplitSearchEngine with footer offset optimization: $splitPath")
      logger.debug(s"   Footer optimization saves ~87% network traffic during initialization")
    } else {
      logger.info(s"📁 STANDARD LOADING: Creating SplitSearchEngine without optimization metadata: $splitPath")
    }
    
    new SplitSearchEngine(sparkSchema, splitPath, metadata, cacheConfig)
  }
}
