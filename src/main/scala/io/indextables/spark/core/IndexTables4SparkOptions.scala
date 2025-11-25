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
  fieldType: Option[String] = None,        // "string", "text", or "json"
  isFast: Boolean = false,                 // Whether to enable fast fields
  isStoreOnly: Boolean = false,            // Store but don't index
  isIndexOnly: Boolean = false,            // Index but don't store
  tokenizerOverride: Option[String] = None // Custom tokenizer for text fields
)

/** Utilities for handling IndexTables4Spark write and read options. Similar to Delta Lake's DeltaOptions. */
class IndexTables4SparkOptions(options: CaseInsensitiveStringMap) {

  /** Whether to enable optimized writes. */
  def optimizeWrite: Option[Boolean] =
    Option(options.get("optimizeWrite")).map(_.toBoolean)

  /** Target number of records per split for optimized writes. */
  def targetRecordsPerSplit: Option[Long] =
    Option(options.get("targetRecordsPerSplit")).map { valueStr =>
      val value = valueStr.toLong
      require(value > 0, s"targetRecordsPerSplit must be greater than 0, got: $value")
      value
    }

  /** Whether to enable bloom filters. */
  def bloomFiltersEnabled: Option[Boolean] =
    Option(options.get("bloomFiltersEnabled")).map(_.toBoolean)

  /** Force standard storage operations (disable S3 optimizations). */
  def forceStandardStorage: Option[Boolean] =
    Option(options.get("forceStandardStorage")).map(_.toBoolean)

  /** Whether to enable auto-sizing based on historical split data. */
  def autoSizeEnabled: Option[Boolean] =
    Option(options.get("spark.indextables.autoSize.enabled")).filter(_.trim.nonEmpty).map { valueStr =>
      val trimmedValue = valueStr.trim.toLowerCase
      trimmedValue match {
        case "true" | "1" | "yes" | "on"  => true
        case "false" | "0" | "no" | "off" => false
        case _ =>
          throw new IllegalArgumentException(
            s"Invalid boolean value for autoSize.enabled: '$valueStr'. Valid values are: true, false, 1, 0, yes, no, on, off"
          )
      }
    }

  /**
   * Target split size for auto-sizing feature. Supports formats: "123456" (bytes), "1M" (megabytes), "1G" (gigabytes).
   */
  def autoSizeTargetSplitSize: Option[String] =
    Option(options.get("spark.indextables.autoSize.targetSplitSize")).filter(_.trim.nonEmpty)

  /**
   * Input row count for auto-sizing feature. When provided, this exact count will be used for partitioning
   * calculations.
   */
  def autoSizeInputRowCount: Option[Long] =
    Option(options.get("spark.indextables.autoSize.inputRowCount")).filter(_.trim.nonEmpty).map(_.toLong)

  /**
   * Get field type mapping configuration. Maps field names to their indexing types: "string", "text", or "json".
   * Supports both spark.tantivy4spark and spark.indextables prefixes.
   *
   * NOTE: Field names are stored in lowercase to handle case-insensitive matching with schema field names.
   */
  def getFieldTypeMapping: Map[String, String] = {
    import scala.jdk.CollectionConverters._
    options
      .asCaseSensitiveMap()
      .asScala
      .toMap
      .filter {
        case (key, _) =>
          key.startsWith("spark.indextables.indexing.typemap.") ||
          key.startsWith("spark.indextables.indexing.typemap.")
      }
      .map {
        case (key, value) =>
          val fieldName = if (key.startsWith("spark.indextables.indexing.typemap.")) {
            key.substring("spark.indextables.indexing.typemap.".length)
          } else {
            key.substring("spark.indextables.indexing.typemap.".length)
          }
          // Normalize field name to lowercase for case-insensitive matching
          fieldName.toLowerCase -> value.toLowerCase
      }
  }

  /**
   * Get fast fields configuration. Returns set of field names that should get "fast" indexing. Supports both
   * spark.tantivy4spark and spark.indextables prefixes.
   */
  def getFastFields: Set[String] =
    // Check indextables prefix first (preferred)
    Option(options.get("spark.indextables.indexing.fastfields"))
      .orElse(Option(options.get("spark.indextables.indexing.fastfields")))
      .map(_.split(",").map(_.trim).filterNot(_.isEmpty).toSet)
      .getOrElse(Set.empty)

  /**
   * Get non-fast fields configuration. Returns set of field names that should be excluded from default fast field
   * behavior. This allows users to exclude specific fields from being auto-configured as fast. Supports both
   * spark.tantivy4spark and spark.indextables prefixes.
   */
  def getNonFastFields: Set[String] =
    // Check indextables prefix first (preferred)
    Option(options.get("spark.indextables.indexing.nonfastfields"))
      .orElse(Option(options.get("spark.indextables.indexing.nonfastfields")))
      .map(_.split(",").map(_.trim).filterNot(_.isEmpty).toSet)
      .getOrElse(Set.empty)

  /**
   * Get store-only fields configuration. Returns set of field names that should be stored but not indexed. Supports
   * both spark.tantivy4spark and spark.indextables prefixes.
   */
  def getStoreOnlyFields: Set[String] =
    Option(options.get("spark.indextables.indexing.storeonlyfields"))
      .orElse(Option(options.get("spark.indextables.indexing.storeonlyfields")))
      .map(_.split(",").map(_.trim).filterNot(_.isEmpty).toSet)
      .getOrElse(Set.empty)

  /**
   * Get index-only fields configuration. Returns set of field names that should be indexed but not stored. Supports
   * both spark.tantivy4spark and spark.indextables prefixes.
   */
  def getIndexOnlyFields: Set[String] =
    Option(options.get("spark.indextables.indexing.indexonlyfields"))
      .orElse(Option(options.get("spark.indextables.indexing.indexonlyfields")))
      .map(_.split(",").map(_.trim).filterNot(_.isEmpty).toSet)
      .getOrElse(Set.empty)

  /**
   * Get tokenizer override configuration. Maps field names to their tokenizer types. Supports both spark.tantivy4spark
   * and spark.indextables prefixes.
   */
  def getTokenizerOverrides: Map[String, String] = {
    import scala.jdk.CollectionConverters._
    options
      .asCaseSensitiveMap()
      .asScala
      .toMap
      .filter {
        case (key, _) =>
          key.startsWith("spark.indextables.indexing.tokenizer.") ||
          key.startsWith("spark.indextables.indexing.tokenizer.")
      }
      .map {
        case (key, value) =>
          val fieldName = if (key.startsWith("spark.indextables.indexing.tokenizer.")) {
            key.substring("spark.indextables.indexing.tokenizer.".length)
          } else {
            key.substring("spark.indextables.indexing.tokenizer.".length)
          }
          fieldName -> value
      }
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

  /** Get indexing configuration for a specific field. */
  def getFieldIndexingConfig(fieldName: String): FieldIndexingConfig = {
    val fieldTypeMapping   = getFieldTypeMapping
    val fastFields         = getFastFields
    val storeOnlyFields    = getStoreOnlyFields
    val indexOnlyFields    = getIndexOnlyFields
    val tokenizerOverrides = getTokenizerOverrides

    // DEBUG: Log what we're checking
    import org.slf4j.LoggerFactory
    val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkOptions])
    logger.debug(s"getFieldIndexingConfig DEBUG: fieldName=$fieldName")
    logger.debug(s"getFieldIndexingConfig DEBUG: fastFields=${fastFields.mkString(", ")}")
    logger.debug(s"getFieldIndexingConfig DEBUG: fastFields.contains($fieldName)=${fastFields.contains(fieldName)}")

    FieldIndexingConfig(
      fieldType = fieldTypeMapping.get(fieldName),
      isFast = fastFields.contains(fieldName),
      isStoreOnly = storeOnlyFields.contains(fieldName),
      isIndexOnly = indexOnlyFields.contains(fieldName),
      tokenizerOverride = tokenizerOverrides.get(fieldName)
    )
  }

  /** Get all configured options as a map for logging. */
  def getAllOptions: Map[String, String] = {
    import scala.jdk.CollectionConverters._
    options.asCaseSensitiveMap().asScala.toMap
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
  val OPTIMIZE_WRITE           = "optimizeWrite"
  val TARGET_RECORDS_PER_SPLIT = "targetRecordsPerSplit"
  val BLOOM_FILTERS_ENABLED    = "bloomFiltersEnabled"
  val FORCE_STANDARD_STORAGE   = "forceStandardStorage"

  // Auto-sizing option keys
  val AUTO_SIZE_ENABLED           = "spark.indextables.autoSize.enabled"
  val AUTO_SIZE_TARGET_SPLIT_SIZE = "spark.indextables.autoSize.targetSplitSize"
  val AUTO_SIZE_INPUT_ROW_COUNT   = "spark.indextables.autoSize.inputRowCount"

  // Indexing configuration keys
  val INDEXING_TYPEMAP_PREFIX   = "spark.indextables.indexing.typemap."
  val INDEXING_FASTFIELDS       = "spark.indextables.indexing.fastfields"
  val INDEXING_NONFASTFIELDS    = "spark.indextables.indexing.nonfastfields"
  val INDEXING_STOREONLY_FIELDS = "spark.indextables.indexing.storeonlyfields"
  val INDEXING_INDEXONLY_FIELDS = "spark.indextables.indexing.indexonlyfields"
  val INDEXING_TOKENIZER_PREFIX = "spark.indextables.indexing.tokenizer."
  val INDEXING_JSON_MODE        = "spark.indextables.indexing.json.mode"

  // Batch optimization configuration keys
  val BATCH_OPTIMIZATION_ENABLED              = "spark.indextables.read.batchOptimization.enabled"
  val BATCH_OPTIMIZATION_PROFILE              = "spark.indextables.read.batchOptimization.profile"
  val BATCH_OPTIMIZATION_MAX_RANGE_SIZE       = "spark.indextables.read.batchOptimization.maxRangeSize"
  val BATCH_OPTIMIZATION_GAP_TOLERANCE        = "spark.indextables.read.batchOptimization.gapTolerance"
  val BATCH_OPTIMIZATION_MIN_DOCS             = "spark.indextables.read.batchOptimization.minDocsForOptimization"
  val BATCH_OPTIMIZATION_MAX_CONCURRENT_PREFETCH = "spark.indextables.read.batchOptimization.maxConcurrentPrefetch"

  // Adaptive tuning configuration keys
  val ADAPTIVE_TUNING_ENABLED     = "spark.indextables.read.adaptiveTuning.enabled"
  val ADAPTIVE_TUNING_MIN_BATCHES = "spark.indextables.read.adaptiveTuning.minBatchesBeforeAdjustment"
}
