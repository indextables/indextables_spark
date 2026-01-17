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

package io.indextables.spark.core

import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Configuration for field indexing behavior. */
case class FieldIndexingConfig(
  fieldType: Option[String] = None,         // "string", "text", or "json"
  isFast: Boolean = false,                  // Whether to enable fast fields
  isStoreOnly: Boolean = false,             // Store but don't index
  isIndexOnly: Boolean = false,             // Index but don't store
  tokenizerOverride: Option[String] = None, // Custom tokenizer for text fields
  indexRecordOption: Option[String] = None  // "basic", "freq", or "position" for text fields
)

/** Utilities for handling IndexTables4Spark write and read options. Similar to Delta Lake's DeltaOptions. */
class IndexTables4SparkOptions(options: CaseInsensitiveStringMap) {

  /** Whether to enable bloom filters. */
  def bloomFiltersEnabled: Option[Boolean] =
    Option(options.get("bloomFiltersEnabled")).map(_.toBoolean)

  /** Force standard storage operations (disable S3 optimizations). */
  def forceStandardStorage: Option[Boolean] =
    Option(options.get("forceStandardStorage")).map(_.toBoolean)

  /**
   * Get field type mapping configuration. Maps field names to their indexing types: "string", "text", or "json".
   *
   * NOTE: Field names are stored in lowercase to handle case-insensitive matching with schema field names.
   */
  def getFieldTypeMapping: Map[String, String] = {
    import scala.jdk.CollectionConverters._
    val prefix = "spark.indextables.indexing.typemap."
    options
      .asCaseSensitiveMap()
      .asScala
      .toMap
      .filter { case (key, _) => key.startsWith(prefix) }
      .map {
        case (key, value) =>
          // Normalize field name to lowercase for case-insensitive matching
          key.substring(prefix.length).toLowerCase -> value.toLowerCase
      }
  }

  /** Get fast fields configuration. Returns set of field names that should get "fast" indexing. */
  def getFastFields: Set[String] =
    Option(options.get("spark.indextables.indexing.fastfields"))
      .map(_.split(",").map(_.trim).filterNot(_.isEmpty).toSet)
      .getOrElse(Set.empty)

  /**
   * Get non-fast fields configuration. Returns set of field names that should be excluded from default fast field
   * behavior. This allows users to exclude specific fields from being auto-configured as fast.
   */
  def getNonFastFields: Set[String] =
    Option(options.get("spark.indextables.indexing.nonfastfields"))
      .map(_.split(",").map(_.trim).filterNot(_.isEmpty).toSet)
      .getOrElse(Set.empty)

  /** Get store-only fields configuration. Returns set of field names that should be stored but not indexed. */
  def getStoreOnlyFields: Set[String] =
    Option(options.get("spark.indextables.indexing.storeonlyfields"))
      .map(_.split(",").map(_.trim).filterNot(_.isEmpty).toSet)
      .getOrElse(Set.empty)

  /** Get index-only fields configuration. Returns set of field names that should be indexed but not stored. */
  def getIndexOnlyFields: Set[String] =
    Option(options.get("spark.indextables.indexing.indexonlyfields"))
      .map(_.split(",").map(_.trim).filterNot(_.isEmpty).toSet)
      .getOrElse(Set.empty)

  /** Get tokenizer override configuration. Maps field names to their tokenizer types. */
  def getTokenizerOverrides: Map[String, String] = {
    import scala.jdk.CollectionConverters._
    val prefix = "spark.indextables.indexing.tokenizer."
    options
      .asCaseSensitiveMap()
      .asScala
      .toMap
      .filter { case (key, _) => key.startsWith(prefix) }
      .map { case (key, value) => key.substring(prefix.length) -> value }
  }

  /**
   * Get the default index record option for text fields.
   * Controls what information is stored in the inverted index for text fields.
   * Options: "basic" (doc IDs only), "freq" (doc IDs + term frequency), "position" (doc IDs + freq + positions)
   * Default: "position" (required for phrase queries and exact phrase matching)
   */
  def getDefaultIndexRecordOption: String =
    Option(options.get("spark.indextables.indexing.text.indexRecordOption"))
      .map(_.toLowerCase)
      .getOrElse("position")

  /**
   * Get per-field index record option overrides.
   * Maps field names to their record options: "basic", "freq", or "position".
   * Use spark.indextables.indexing.indexrecordoption.<fieldname> = "position" to enable phrase queries on that field.
   */
  def getIndexRecordOptionOverrides: Map[String, String] = {
    import scala.jdk.CollectionConverters._
    val prefix = "spark.indextables.indexing.indexrecordoption."
    options
      .asCaseSensitiveMap()
      .asScala
      .toMap
      .filter { case (key, _) => key.startsWith(prefix) }
      .map { case (key, value) => key.substring(prefix.length).toLowerCase -> value.toLowerCase }
  }

  // ===== Batch Optimization Configuration =====

  /** Whether to enable batch retrieval optimization (reduces S3 requests by 90-95%). */
  def batchOptimizationEnabled: Option[Boolean] =
    Option(options.get("spark.indextables.read.batchOptimization.enabled")).map(_.toBoolean)

  /** Preset batch optimization profile: "conservative", "balanced", "aggressive", or "disabled". */
  def batchOptimizationProfile: Option[String] =
    Option(options.get("spark.indextables.read.batchOptimization.profile")).map(_.toLowerCase)

  /** Maximum size of a consolidated byte range. Supports formats: "16M", "512K", or bytes. */
  def batchOptMaxRangeSize: Option[String] =
    Option(options.get("spark.indextables.read.batchOptimization.maxRangeSize"))

  /** Maximum gap between documents to merge into single range. Supports formats: "512K", "1M", or bytes. */
  def batchOptGapTolerance: Option[String] =
    Option(options.get("spark.indextables.read.batchOptimization.gapTolerance"))

  /** Minimum batch size to trigger optimization. Default: 50 documents. */
  def batchOptMinDocs: Option[Int] =
    Option(options.get("spark.indextables.read.batchOptimization.minDocsForOptimization")).map(_.toInt)

  /** Number of parallel range fetch requests. Default: 8. */
  def batchOptMaxConcurrentPrefetch: Option[Int] =
    Option(options.get("spark.indextables.read.batchOptimization.maxConcurrentPrefetch")).map(_.toInt)

  // ===== Adaptive Tuning Configuration =====

  /** Whether to enable adaptive parameter tuning based on performance metrics. Default: true. */
  def adaptiveTuningEnabled: Option[Boolean] =
    Option(options.get("spark.indextables.read.adaptiveTuning.enabled")).map(_.toBoolean)

  /** Minimum batches to track before making parameter adjustments. Default: 5. */
  def adaptiveTuningMinBatches: Option[Int] =
    Option(options.get("spark.indextables.read.adaptiveTuning.minBatchesBeforeAdjustment")).map(_.toInt)

  // ===== L2 Disk Cache Configuration =====

  /** Whether to enable L2 disk cache. Default: auto-enabled if /local_disk0 detected. */
  def diskCacheEnabled: Option[Boolean] =
    Option(options.get("spark.indextables.cache.disk.enabled")).map(_.toBoolean)

  /** Path for disk cache. Default: /local_disk0/tantivy4spark_slicecache if available. */
  def diskCachePath: Option[String] =
    Option(options.get("spark.indextables.cache.disk.path"))

  /** Maximum disk cache size. Supports "100G", "500M" formats. Default: auto (2/3 disk). */
  def diskCacheMaxSize: Option[String] =
    Option(options.get("spark.indextables.cache.disk.maxSize"))

  /** Compression algorithm: "lz4" (default), "zstd", "none". */
  def diskCacheCompression: Option[String] =
    Option(options.get("spark.indextables.cache.disk.compression"))

  /** Minimum size to compress. Supports "4K", "8K" formats. Default: 4096 bytes. */
  def diskCacheMinCompressSize: Option[String] =
    Option(options.get("spark.indextables.cache.disk.minCompressSize"))

  /** Seconds between manifest sync to disk. Default: 30. */
  def diskCacheManifestSyncInterval: Option[Int] =
    Option(options.get("spark.indextables.cache.disk.manifestSyncInterval")).map(_.toInt)

  /** Get indexing configuration for a specific field. */
  def getFieldIndexingConfig(fieldName: String): FieldIndexingConfig = {
    val fieldTypeMapping         = getFieldTypeMapping
    val fastFields               = getFastFields
    val storeOnlyFields          = getStoreOnlyFields
    val indexOnlyFields          = getIndexOnlyFields
    val tokenizerOverrides       = getTokenizerOverrides
    val indexRecordOverrides     = getIndexRecordOptionOverrides
    val defaultIndexRecordOption = getDefaultIndexRecordOption

    // DEBUG: Log what we're checking
    import org.slf4j.LoggerFactory
    val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkOptions])
    logger.debug(s"getFieldIndexingConfig DEBUG: fieldName=$fieldName")
    logger.debug(s"getFieldIndexingConfig DEBUG: fastFields=${fastFields.mkString(", ")}")
    logger.debug(s"getFieldIndexingConfig DEBUG: fastFields.contains($fieldName)=${fastFields.contains(fieldName)}")

    // Get index record option: per-field override, or default
    val indexRecord = indexRecordOverrides.get(fieldName.toLowerCase).orElse(Some(defaultIndexRecordOption))

    FieldIndexingConfig(
      fieldType = fieldTypeMapping.get(fieldName),
      isFast = fastFields.contains(fieldName),
      isStoreOnly = storeOnlyFields.contains(fieldName),
      isIndexOnly = indexOnlyFields.contains(fieldName),
      tokenizerOverride = tokenizerOverrides.get(fieldName),
      indexRecordOption = indexRecord
    )
  }

  /** Get all configured options as a map for logging. */
  def getAllOptions: Map[String, String] = {
    import scala.jdk.CollectionConverters._
    options.asCaseSensitiveMap().asScala.toMap
  }

  /**
   * Validate that all field-specific indexing configuration options reference fields that exist in the schema.
   *
   * This prevents silent misconfiguration due to typos in field names. The validation checks:
   *   - typemap.<field> - field type configuration
   *   - fastfields - comma-separated list of fast fields
   *   - nonfastfields - comma-separated list of non-fast fields
   *   - storeonlyfields - comma-separated list of store-only fields
   *   - indexonlyfields - comma-separated list of index-only fields
   *   - tokenizer.<field> - tokenizer override configuration
   *   - indexrecordoption.<field> - index record option configuration
   *
   * @param schema
   *   The Spark schema to validate against
   * @throws IllegalArgumentException
   *   if any configured fields don't exist in the schema
   */
  def validateFieldsExist(schema: org.apache.spark.sql.types.StructType): Unit = {
    val schemaFieldNames = schema.fieldNames.map(_.toLowerCase).toSet
    val errors           = scala.collection.mutable.ListBuffer[String]()

    // Helper to check field existence (case-insensitive)
    // For nested paths like "user.age", only the root field "user" needs to exist in the schema
    def checkField(fieldName: String, configType: String): Unit = {
      if (fieldName.nonEmpty) {
        // Extract the root field name (before the first dot, if any)
        val rootFieldName = fieldName.split('.').head.toLowerCase
        if (!schemaFieldNames.contains(rootFieldName)) {
          errors += s"$configType field '$fieldName' does not exist in schema"
        }
      }
    }

    // Check typemap fields
    getFieldTypeMapping.keys.foreach { fieldName =>
      checkField(fieldName, "typemap")
    }

    // Check fastfields
    getFastFields.foreach { fieldName =>
      checkField(fieldName, "fastfields")
    }

    // Check nonfastfields
    getNonFastFields.foreach { fieldName =>
      checkField(fieldName, "nonfastfields")
    }

    // Check storeonlyfields
    getStoreOnlyFields.foreach { fieldName =>
      checkField(fieldName, "storeonlyfields")
    }

    // Check indexonlyfields
    getIndexOnlyFields.foreach { fieldName =>
      checkField(fieldName, "indexonlyfields")
    }

    // Check tokenizer overrides
    getTokenizerOverrides.keys.foreach { fieldName =>
      checkField(fieldName, "tokenizer")
    }

    // Check indexrecordoption fields
    getIndexRecordOptionOverrides.keys.foreach { fieldName =>
      checkField(fieldName, "indexrecordoption")
    }

    if (errors.nonEmpty) {
      val availableFields = schema.fieldNames.sorted.mkString(", ")
      throw new IllegalArgumentException(
        s"Invalid indexing configuration:\n${errors.mkString("\n")}\n\nAvailable schema fields: [$availableFields]"
      )
    }
  }
}

object IndexTables4SparkOptions {

  /** Create IndexTables4SparkOptions from a CaseInsensitiveStringMap. */
  def apply(options: CaseInsensitiveStringMap): IndexTables4SparkOptions =
    new IndexTables4SparkOptions(options)

  /** Create IndexTables4SparkOptions from a Map. */
  def apply(options: Map[String, String]): IndexTables4SparkOptions = {
    import scala.jdk.CollectionConverters._
    new IndexTables4SparkOptions(new CaseInsensitiveStringMap(options.asJava))
  }

  // Option keys
  val BLOOM_FILTERS_ENABLED  = "bloomFiltersEnabled"
  val FORCE_STANDARD_STORAGE = "forceStandardStorage"

  // Indexing configuration keys
  val INDEXING_TYPEMAP_PREFIX   = "spark.indextables.indexing.typemap."
  val INDEXING_FASTFIELDS       = "spark.indextables.indexing.fastfields"
  val INDEXING_NONFASTFIELDS    = "spark.indextables.indexing.nonfastfields"
  val INDEXING_STOREONLY_FIELDS = "spark.indextables.indexing.storeonlyfields"
  val INDEXING_INDEXONLY_FIELDS = "spark.indextables.indexing.indexonlyfields"
  val INDEXING_TOKENIZER_PREFIX = "spark.indextables.indexing.tokenizer."
  val INDEXING_JSON_MODE        = "spark.indextables.indexing.json.mode"

  // Batch optimization configuration keys
  val BATCH_OPTIMIZATION_ENABLED                 = "spark.indextables.read.batchOptimization.enabled"
  val BATCH_OPTIMIZATION_PROFILE                 = "spark.indextables.read.batchOptimization.profile"
  val BATCH_OPTIMIZATION_MAX_RANGE_SIZE          = "spark.indextables.read.batchOptimization.maxRangeSize"
  val BATCH_OPTIMIZATION_GAP_TOLERANCE           = "spark.indextables.read.batchOptimization.gapTolerance"
  val BATCH_OPTIMIZATION_MIN_DOCS                = "spark.indextables.read.batchOptimization.minDocsForOptimization"
  val BATCH_OPTIMIZATION_MAX_CONCURRENT_PREFETCH = "spark.indextables.read.batchOptimization.maxConcurrentPrefetch"

  // Adaptive tuning configuration keys
  val ADAPTIVE_TUNING_ENABLED     = "spark.indextables.read.adaptiveTuning.enabled"
  val ADAPTIVE_TUNING_MIN_BATCHES = "spark.indextables.read.adaptiveTuning.minBatchesBeforeAdjustment"

  // L2 Disk Cache configuration keys
  val DISK_CACHE_ENABLED                = "spark.indextables.cache.disk.enabled"
  val DISK_CACHE_PATH                   = "spark.indextables.cache.disk.path"
  val DISK_CACHE_MAX_SIZE               = "spark.indextables.cache.disk.maxSize"
  val DISK_CACHE_COMPRESSION            = "spark.indextables.cache.disk.compression"
  val DISK_CACHE_MIN_COMPRESS_SIZE      = "spark.indextables.cache.disk.minCompressSize"
  val DISK_CACHE_MANIFEST_SYNC_INTERVAL = "spark.indextables.cache.disk.manifestSyncInterval"
}
