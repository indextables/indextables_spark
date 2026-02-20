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

package io.indextables.spark.search

import scala.util.Try

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import io.indextables.spark.schema.SchemaMapping
import io.indextables.spark.storage.{GlobalSplitCacheManager, SplitCacheConfig}
import io.indextables.tantivy4java.core.Schema
import io.indextables.tantivy4java.result.SearchResult
import io.indextables.tantivy4java.split.{SplitMatchAllQuery, SplitQuery, SplitSearcher}
import org.slf4j.LoggerFactory

/**
 * Search engine implementation using tantivy4java's SplitSearcher with global caching.
 *
 * This replaces the previous TantivySearchEngine which used zip-based archives. The new implementation uses .split
 * files with optimized caching and native storage access.
 *
 * Private constructor - use SplitSearchEngine.fromSplitFileWithMetadata() instead.
 */
class SplitSearchEngine private (
  sparkSchema: StructType,
  splitPath: String,
  metadata: io.indextables.tantivy4java.split.merge.QuickwitSplit.SplitMetadata,
  cacheConfig: SplitCacheConfig,
  options: Option[io.indextables.spark.core.IndexTables4SparkOptions])
    extends AutoCloseable {

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
  logger.info(s"Creating SplitSearchEngine for path: $splitPath")

  // Validate metadata is provided and non-null
  if (metadata == null) {
    throw new IllegalArgumentException(
      s"Split metadata cannot be null for createSplitSearcher. Path: $splitPath. Use SplitSearchEngine.fromSplitFileWithMetadata() to create with proper metadata."
    )
  }

  // Validate footer offsets are present and non-zero
  if (!metadata.hasFooterOffsets()) {
    throw new IllegalArgumentException(
      s"Split metadata for $splitPath does not contain required footer offsets. The split file may have been created with an older version of tantivy4java that didn't generate footer offset metadata. Please recreate the split file with the current version."
    )
  }

  // Validate footer offsets are non-zero (meaningful)
  val footerStartOffset = metadata.getFooterStartOffset()
  val footerEndOffset   = metadata.getFooterEndOffset()
  // Hotcache methods deprecated in v0.24.1 - using footer offsets instead
  val hotcacheStartOffset = footerStartOffset                   // Fallback to footer offset
  val hotcacheLength      = footerEndOffset - footerStartOffset // Calculate from footer range

  if (footerStartOffset <= 0 || footerEndOffset <= 0) {
    throw new IllegalArgumentException(
      s"Split metadata has invalid footer offsets. footerStartOffset: $footerStartOffset, footerEndOffset: $footerEndOffset. These must be positive non-zero values."
    )
  }

  if (footerEndOffset <= footerStartOffset) {
    throw new IllegalArgumentException(
      s"Split metadata has invalid footer offsets. footerEndOffset ($footerEndOffset) must be greater than footerStartOffset ($footerStartOffset)."
    )
  }

  if (hotcacheStartOffset <= 0 || hotcacheLength <= 0) {
    throw new IllegalArgumentException(
      s"Split metadata has invalid hotcache offsets. hotcacheStartOffset: $hotcacheStartOffset, hotcacheLength: $hotcacheLength. These must be positive non-zero values."
    )
  }

  // Validate document mapping is available
  if (metadata.getDocMappingJson() == null || metadata.getDocMappingJson().trim.isEmpty) {
    throw new IllegalArgumentException(
      s"Split metadata must contain valid document mapping JSON for $splitPath. Document mapping is required for proper field extraction."
    )
  }

  logger.info(s"Split metadata validation passed for $splitPath:")
  logger.info(s"   Footer offsets: $footerStartOffset - $footerEndOffset")
  logger.info(s"   Hotcache: $hotcacheStartOffset + $hotcacheLength")
  logger.info(s"   Document mapping: ${metadata.getDocMappingJson().length} chars")

  protected lazy val splitSearcher =
    try {
      logger.info(s"Using metadata for $splitPath with footer offsets: ${metadata.hasFooterOffsets()}")
      // Use per-split overrides for parquetTableRoot and parquetStorageConfig.
      // This ensures the correct values reach the native layer regardless of
      // SplitCacheManager singleton caching (getCacheKey() intentionally omits
      // parquet fields so a single cache manager can serve multiple table roots).
      cacheConfig.companionSourceTableRoot match {
        case Some(tableRoot) =>
          val pqStorageConfig = buildParquetStorageConfig()
          logger.info(
            s"Companion mode: per-split parquetTableRoot=$tableRoot, " +
              s"parquetStorageConfig=${if (pqStorageConfig.isDefined) "present" else "none"} for $splitPath"
          )
          cacheManager.createSplitSearcher(splitPath, metadata, tableRoot, pqStorageConfig.orNull)
        case None =>
          logger.info(
            s"Non-companion mode: companionSourceTableRoot=None for $splitPath " +
              s"(parquetAwsAccessKey=${cacheConfig.parquetAwsAccessKey.map(_.take(4) + "...").getOrElse("None")})"
          )
          cacheManager.createSplitSearcher(splitPath, metadata)
      }
    } catch {
      case ex: RuntimeException if ex.getMessage.contains("region must be set") =>
        logger.error(s"tantivy4java region error when creating SplitSearcher for $splitPath")
        logger.error(s"AWS region is not being passed to tantivy4java properly")
        logger.error(s"cacheConfig had: awsRegion=${cacheConfig.awsRegion.getOrElse("None")}, awsEndpoint=${cacheConfig.awsEndpoint.getOrElse("None")}")
        throw ex
      case ex: Exception =>
        logger.error(s"Failed to create SplitSearcher for $splitPath: ${ex.getMessage}")
        throw ex
    }

  // Row converter for converting search results to Spark InternalRows
  // TODO: Implement RowConverter for SplitSearchEngine
  // private val rowConverter = new io.indextables.spark.search.RowConverter(sparkSchema)

  logger.info(s"Created SplitSearchEngine for $splitPath using cache: ${cacheConfig.cacheName}")

  /** Get the Tantivy schema for this split. */
  def getSchema(): Schema =
    splitSearcher.getSchema()

  /** Get the Spark schema for this split. */
  def getSparkSchema(): StructType = sparkSchema

  /**
   * Search the split using a SplitQuery object and return results as Spark InternalRows. This is the new preferred
   * method that uses tantivy4java's efficient SplitQuery API.
   */
  def search(splitQuery: SplitQuery, limit: Int = 100): Array[InternalRow] = {
    logger.debug(s"Searching split with SplitQuery object, limit: $limit")
    logger.debug(s"Running SplitQuery: ${splitQuery.toString()}")

    // Log schema field names for debugging, ensuring schema is properly closed
    if (logger.isDebugEnabled) {
      val tantivySchema = splitSearcher.getSchema()
      try
        logger.debug(s"Available fields in schema: ${tantivySchema.getFieldNames()}")
      finally
        tantivySchema.close() // Prevent native memory leak
    }

    // Execute the search using the new SplitQuery API
    val searchResult = splitSearcher.search(splitQuery, limit)

    // Use try-finally to ensure SearchResult is always closed (resource safety)
    try {
      // Convert search results to Spark InternalRows
      val results = convertSearchResultToRows(searchResult)
      logger.debug(s"Search completed: ${results.length} results returned")
      results
    } catch {
      case e: Exception =>
        logger.error(s"Search failed for SplitQuery object", e)
        throw e
    } finally
      // Always close the search result to free resources
      try
        searchResult.close()
      catch {
        case closeEx: Exception =>
          logger.warn(s"Error closing SearchResult for split $splitPath", closeEx)
      }
  }

  /** Search all documents in the split (equivalent to match-all query). */
  def searchAll(limit: Int = 1000): Array[InternalRow] = {
    logger.debug(s"Searching all documents with limit: $limit")

    // Create match-all query using SplitMatchAllQuery
    val splitQuery = new SplitMatchAllQuery()
    logger.debug(s"Running match-all SplitQuery: ${splitQuery.toString()}")

    // Execute the search using SplitQuery
    val searchResult = splitSearcher.search(splitQuery, limit)

    // Use try-finally to ensure SearchResult is always closed (resource safety)
    try {
      // Convert to InternalRows
      val results = convertSearchResultToRows(searchResult)
      logger.debug(s"SearchAll completed: ${results.length} results returned")
      results
    } catch {
      case e: Exception =>
        logger.error("SearchAll failed", e)
        throw e
    } finally
      // Always close the search result to free resources
      try
        searchResult.close()
      catch {
        case closeEx: Exception =>
          logger.warn(s"Error closing SearchResult for split $splitPath", closeEx)
      }
  }

  /**
   * Parse a query string using the split's schema and return a SplitQuery. This is the preferred way to create queries
   * for split searching.
   */
  def parseQuery(queryString: String): SplitQuery =
    splitSearcher.parseQuery(queryString)

  /**
   * Parse a query string with field-specific search using the enhanced SplitSearcher API. This constrains the search to
   * the specified field using the new parseQuery methods.
   */
  def parseQuery(queryString: String, fieldName: String): SplitQuery =
    // Use the enhanced SplitSearcher parseQuery method with single field
    splitSearcher.parseQuery(queryString, fieldName)

  /**
   * Parse a query string with multi-field search using the enhanced SplitSearcher API. This constrains the search to
   * the specified fields using the new parseQuery methods.
   */
  def parseQuery(queryString: String, fieldNames: java.util.List[String]): SplitQuery =
    // Use the enhanced SplitSearcher parseQuery method with multiple fields
    splitSearcher.parseQuery(queryString, fieldNames)

  /** Search using a query string. The string will be parsed into a SplitQuery. */
  def searchByString(queryString: String, limit: Int = 100): Array[InternalRow] = {
    val splitQuery = parseQuery(queryString)
    search(splitQuery, limit)
  }

  /** Preload index components for better performance. */
  def preloadComponents(components: SplitSearcher.IndexComponent*): Unit =
    try {
      logger.info(s"Preloading components: ${components.mkString(", ")}")
      splitSearcher.preloadComponents(components: _*)
    } catch {
      case e: Exception =>
        logger.warn("Failed to preload components", e)
    }

  /** Get cache statistics for this split. */
  def getCacheStats(): SplitSearcher.CacheStats =
    splitSearcher.getCacheStats()

  /** Get loading statistics for this split. */
  def getLoadingStats(): SplitSearcher.LoadingStats =
    splitSearcher.getLoadingStats()

  /** Validate the split file integrity. */
  def validateSplit(): Boolean =
    try
      splitSearcher.validateSplit()
    catch {
      case e: Exception =>
        logger.warn("Split validation failed", e)
        false
    }

  /** Get metadata about the split file. */
  def getSplitMetadata(): Option[SplitSearcher.SplitMetadata] =
    Try {
      splitSearcher.getSplitMetadata()
    }.toOption

  /** Convert SearchResult to InternalRows using the new SchemaMapping system. */
  private def convertSearchResultToRows(searchResult: SearchResult): Array[InternalRow] = {
    // Get the split schema once and ensure it's closed when done (prevent native memory leak)
    val splitSchema = splitSearcher.getSchema()
    try {
      import scala.jdk.CollectionConverters._
      val hits = searchResult.getHits().asScala.toArray

      logger.debug(s"Converting ${hits.length} search hits to InternalRows using SchemaMapping")
      logger.debug(s"Split file path: $splitPath")
      logger.debug(s"Split schema from file: ${splitSchema.getFieldNames().toArray.mkString(", ")}")
      logger.debug(s"Expected Spark schema: ${sparkSchema.fields.map(_.name).mkString(", ")}")

      // Collect all DocAddresses and sort them for sequential I/O access
      // Sorting addresses improves cache locality and S3 range read efficiency
      // Sort by (segmentOrd, docId) for sequential reads within segments
      val docAddresses = hits.map(hit => hit.getDocAddress()).sortBy(addr => (addr.getSegmentOrd, addr.getDoc))

      // Use docBatchProjected for all splits (standard and companion).
      // Returns List<Document> with native-side field projection for both split types.
      val fieldNames = sparkSchema.fields.map(_.name)
      logger.debug(s"Using docBatchProjected for ${docAddresses.length} addresses, ${fieldNames.length} fields")

      val documents: Array[io.indextables.tantivy4java.core.Document] = {
        val javaDocuments = splitSearcher.docBatchProjected(docAddresses, fieldNames: _*)
        javaDocuments.asScala.toArray
      }

      // Build field type cache once before processing documents (avoids N×M JNI calls)
      val fieldTypeCache = SchemaMapping.Read.buildFieldTypeCache(splitSchema, sparkSchema)
      logger.debug(s"Built field type cache for ${fieldTypeCache.size} fields")

      // Convert documents to InternalRows
      val rows = documents.zipWithIndex.map {
        case (document, index) =>
          try
            if (document != null) {
              // Use cached field types for fast conversion (no JNI calls per field)
              val values = SchemaMapping.Read.convertDocumentWithCache(document, sparkSchema, fieldTypeCache, options)
              org.apache.spark.sql.catalyst.InternalRow.fromSeq(values)
            } else {
              // Fallback to empty row if document retrieval fails
              createEmptyRow()
            }
          catch {
            case e: Exception =>
              logger.warn(s"Error converting document at index $index", e)
              createEmptyRow()
          } finally
            // Always close the document to free resources
            if (document != null) {
              try
                document.close()
              catch {
                case e: Exception =>
                  logger.debug("Error closing document", e)
              }
            }
      }

      logger.debug(s"Created ${rows.length} InternalRows using SchemaMapping system")
      rows

    } catch {
      case e: Exception =>
        // Fail-fast: propagate conversion errors instead of silently returning empty results
        logger.error(s"Error converting search results for split $splitPath", e)
        throw new RuntimeException(s"Failed to convert search results for split $splitPath: ${e.getMessage}", e)
    } finally
      // CRITICAL: Close the schema to prevent native memory leak
      // Schema.getSchema() returns a NEW Schema object each time with its own native pointer
      splitSchema.close()
  }

  /** Create an empty row with default values for all fields using SchemaMapping. */
  private def createEmptyRow(): InternalRow = {
    val values = sparkSchema.fields.map(field => SchemaMapping.Read.getDefaultValue(field.dataType))
    org.apache.spark.sql.catalyst.InternalRow.fromSeq(values)
  }

  /**
   * Build a per-split ParquetStorageConfig from cacheConfig's parquet credential fields. Returns None if no separate
   * parquet credentials are configured (native layer falls back to the split's own credentials).
   */
  private def buildParquetStorageConfig(): Option[io.indextables.tantivy4java.split.ParquetCompanionConfig.ParquetStorageConfig] =
    (cacheConfig.parquetAwsAccessKey, cacheConfig.parquetAwsSecretKey) match {
      case (Some(key), Some(secret)) =>
        val pqStorage = new io.indextables.tantivy4java.split.ParquetCompanionConfig.ParquetStorageConfig()
        cacheConfig.parquetAwsSessionToken match {
          case Some(token) => pqStorage.withAwsCredentials(key, secret, token)
          case None        => pqStorage.withAwsCredentials(key, secret)
        }
        cacheConfig.parquetAwsRegion.foreach(pqStorage.withAwsRegion)
        cacheConfig.parquetAwsEndpoint.foreach(pqStorage.withAwsEndpoint)
        Some(pqStorage)
      case _ =>
        None
    }

  /**
   * Get the underlying SplitSearcher for direct operations like warmup. This provides access to advanced features like
   * cache warming.
   */
  def getSplitSearcher(): SplitSearcher =
    splitSearcher

  override def close(): Unit =
    try {
      logger.debug(s"Closing SplitSearchEngine for $splitPath")
      splitSearcher.close()
    } catch {
      case e: Exception =>
        logger.warn("Error closing SplitSearchEngine", e)
    }
}

/** Factory for creating SplitSearchEngine instances. */
object SplitSearchEngine {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create a SplitSearchEngine with footer offset optimization (87% network traffic reduction). Uses pre-computed
   * metadata to enable lazy loading with minimal initial network traffic.
   */
  def fromSplitFileWithMetadata(
    sparkSchema: StructType,
    splitPath: String,
    metadata: io.indextables.tantivy4java.split.merge.QuickwitSplit.SplitMetadata,
    cacheConfig: SplitCacheConfig = SplitCacheConfig(),
    options: Option[io.indextables.spark.core.IndexTables4SparkOptions] = None
  ): SplitSearchEngine = {

    if (metadata != null && metadata.hasFooterOffsets()) {
      logger.info(s"OPTIMIZED LOADING: Creating SplitSearchEngine with footer offset optimization: $splitPath")
      logger.debug(s"   Footer optimization saves ~87% network traffic during initialization")
    } else {
      logger.info(s"STANDARD LOADING: Creating SplitSearchEngine without optimization metadata: $splitPath")
    }

    new SplitSearchEngine(sparkSchema, splitPath, metadata, cacheConfig, options)
  }
}
