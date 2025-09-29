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

package com.tantivy4spark.core

import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Configuration for field indexing behavior.
 */
case class FieldIndexingConfig(
  fieldType: Option[String] = None,      // "string", "text", or "json"
  isFast: Boolean = false,               // Whether to enable fast fields
  isStoreOnly: Boolean = false,          // Store but don't index
  isIndexOnly: Boolean = false,          // Index but don't store
  tokenizerOverride: Option[String] = None // Custom tokenizer for text fields
)

/**
 * Utilities for handling Tantivy4Spark write and read options.
 * Similar to Delta Lake's DeltaOptions.
 */
class Tantivy4SparkOptions(options: CaseInsensitiveStringMap) {

  /**
   * Whether to enable optimized writes.
   */
  def optimizeWrite: Option[Boolean] = {
    Option(options.get("optimizeWrite")).map(_.toBoolean)
  }

  /**
   * Target number of records per split for optimized writes.
   */
  def targetRecordsPerSplit: Option[Long] = {
    Option(options.get("targetRecordsPerSplit")).map { valueStr =>
      val value = valueStr.toLong
      require(value > 0, s"targetRecordsPerSplit must be greater than 0, got: $value")
      value
    }
  }

  /**
   * Whether to enable bloom filters.
   */
  def bloomFiltersEnabled: Option[Boolean] = {
    Option(options.get("bloomFiltersEnabled")).map(_.toBoolean)
  }

  /**
   * Force standard storage operations (disable S3 optimizations).
   */
  def forceStandardStorage: Option[Boolean] = {
    Option(options.get("forceStandardStorage")).map(_.toBoolean)
  }

  /**
   * Whether to enable auto-sizing based on historical split data.
   */
  def autoSizeEnabled: Option[Boolean] = {
    Option(options.get("spark.indextables.autoSize.enabled")).filter(_.trim.nonEmpty).map { valueStr =>
      val trimmedValue = valueStr.trim.toLowerCase
      trimmedValue match {
        case "true" | "1" | "yes" | "on" => true
        case "false" | "0" | "no" | "off" => false
        case _ => throw new IllegalArgumentException(s"Invalid boolean value for autoSize.enabled: '$valueStr'. Valid values are: true, false, 1, 0, yes, no, on, off")
      }
    }
  }

  /**
   * Target split size for auto-sizing feature.
   * Supports formats: "123456" (bytes), "1M" (megabytes), "1G" (gigabytes).
   */
  def autoSizeTargetSplitSize: Option[String] = {
    Option(options.get("spark.indextables.autoSize.targetSplitSize")).filter(_.trim.nonEmpty)
  }

  /**
   * Input row count for auto-sizing feature.
   * When provided, this exact count will be used for partitioning calculations.
   */
  def autoSizeInputRowCount: Option[Long] = {
    Option(options.get("spark.indextables.autoSize.inputRowCount")).filter(_.trim.nonEmpty).map(_.toLong)
  }

  /**
   * Get field type mapping configuration.
   * Maps field names to their indexing types: "string", "text", or "json".
   * Supports both spark.tantivy4spark and spark.indextables prefixes.
   */
  def getFieldTypeMapping: Map[String, String] = {
    import scala.jdk.CollectionConverters._
    options.asCaseSensitiveMap().asScala.toMap
      .filter { case (key, _) =>
        key.startsWith("spark.indextables.indexing.typemap.") ||
        key.startsWith("spark.indextables.indexing.typemap.")
      }
      .map { case (key, value) =>
        val fieldName = if (key.startsWith("spark.indextables.indexing.typemap.")) {
          key.substring("spark.indextables.indexing.typemap.".length)
        } else {
          key.substring("spark.indextables.indexing.typemap.".length)
        }
        fieldName -> value.toLowerCase
      }
  }

  /**
   * Get fast fields configuration.
   * Returns set of field names that should get "fast" indexing.
   * Supports both spark.tantivy4spark and spark.indextables prefixes.
   */
  def getFastFields: Set[String] = {
    // Check indextables prefix first (preferred)
    Option(options.get("spark.indextables.indexing.fastfields"))
      .orElse(Option(options.get("spark.indextables.indexing.fastfields")))
      .map(_.split(",").map(_.trim).filterNot(_.isEmpty).toSet)
      .getOrElse(Set.empty)
  }

  /**
   * Get non-fast fields configuration.
   * Returns set of field names that should be excluded from default fast field behavior.
   * This allows users to exclude specific fields from being auto-configured as fast.
   * Supports both spark.tantivy4spark and spark.indextables prefixes.
   */
  def getNonFastFields: Set[String] = {
    // Check indextables prefix first (preferred)
    Option(options.get("spark.indextables.indexing.nonfastfields"))
      .orElse(Option(options.get("spark.indextables.indexing.nonfastfields")))
      .map(_.split(",").map(_.trim).filterNot(_.isEmpty).toSet)
      .getOrElse(Set.empty)
  }

  /**
   * Get store-only fields configuration.
   * Returns set of field names that should be stored but not indexed.
   * Supports both spark.tantivy4spark and spark.indextables prefixes.
   */
  def getStoreOnlyFields: Set[String] = {
    Option(options.get("spark.indextables.indexing.storeonlyfields"))
      .orElse(Option(options.get("spark.indextables.indexing.storeonlyfields")))
      .map(_.split(",").map(_.trim).filterNot(_.isEmpty).toSet)
      .getOrElse(Set.empty)
  }

  /**
   * Get index-only fields configuration.
   * Returns set of field names that should be indexed but not stored.
   * Supports both spark.tantivy4spark and spark.indextables prefixes.
   */
  def getIndexOnlyFields: Set[String] = {
    Option(options.get("spark.indextables.indexing.indexonlyfields"))
      .orElse(Option(options.get("spark.indextables.indexing.indexonlyfields")))
      .map(_.split(",").map(_.trim).filterNot(_.isEmpty).toSet)
      .getOrElse(Set.empty)
  }

  /**
   * Get tokenizer override configuration.
   * Maps field names to their tokenizer types.
   * Supports both spark.tantivy4spark and spark.indextables prefixes.
   */
  def getTokenizerOverrides: Map[String, String] = {
    import scala.jdk.CollectionConverters._
    options.asCaseSensitiveMap().asScala.toMap
      .filter { case (key, _) =>
        key.startsWith("spark.indextables.indexing.tokenizer.") ||
        key.startsWith("spark.indextables.indexing.tokenizer.")
      }
      .map { case (key, value) =>
        val fieldName = if (key.startsWith("spark.indextables.indexing.tokenizer.")) {
          key.substring("spark.indextables.indexing.tokenizer.".length)
        } else {
          key.substring("spark.indextables.indexing.tokenizer.".length)
        }
        fieldName -> value
      }
  }

  /**
   * Get indexing configuration for a specific field.
   */
  def getFieldIndexingConfig(fieldName: String): FieldIndexingConfig = {
    val fieldTypeMapping = getFieldTypeMapping
    val fastFields = getFastFields
    val storeOnlyFields = getStoreOnlyFields
    val indexOnlyFields = getIndexOnlyFields
    val tokenizerOverrides = getTokenizerOverrides

    FieldIndexingConfig(
      fieldType = fieldTypeMapping.get(fieldName),
      isFast = fastFields.contains(fieldName),
      isStoreOnly = storeOnlyFields.contains(fieldName),
      isIndexOnly = indexOnlyFields.contains(fieldName),
      tokenizerOverride = tokenizerOverrides.get(fieldName)
    )
  }

  /**
   * Get all configured options as a map for logging.
   */
  def getAllOptions: Map[String, String] = {
    import scala.jdk.CollectionConverters._
    options.asCaseSensitiveMap().asScala.toMap
  }
}

object Tantivy4SparkOptions {
  
  /**
   * Create Tantivy4SparkOptions from a CaseInsensitiveStringMap.
   */
  def apply(options: CaseInsensitiveStringMap): Tantivy4SparkOptions = {
    new Tantivy4SparkOptions(options)
  }

  /**
   * Create Tantivy4SparkOptions from a Map.
   */
  def apply(options: Map[String, String]): Tantivy4SparkOptions = {
    import scala.jdk.CollectionConverters._
    new Tantivy4SparkOptions(new CaseInsensitiveStringMap(options.asJava))
  }

  // Option keys
  val OPTIMIZE_WRITE = "optimizeWrite"
  val TARGET_RECORDS_PER_SPLIT = "targetRecordsPerSplit"
  val BLOOM_FILTERS_ENABLED = "bloomFiltersEnabled"
  val FORCE_STANDARD_STORAGE = "forceStandardStorage"

  // Auto-sizing option keys
  val AUTO_SIZE_ENABLED = "spark.indextables.autoSize.enabled"
  val AUTO_SIZE_TARGET_SPLIT_SIZE = "spark.indextables.autoSize.targetSplitSize"
  val AUTO_SIZE_INPUT_ROW_COUNT = "spark.indextables.autoSize.inputRowCount"

  // Indexing configuration keys
  val INDEXING_TYPEMAP_PREFIX = "spark.indextables.indexing.typemap."
  val INDEXING_FASTFIELDS = "spark.indextables.indexing.fastfields"
  val INDEXING_NONFASTFIELDS = "spark.indextables.indexing.nonfastfields"
  val INDEXING_STOREONLY_FIELDS = "spark.indextables.indexing.storeonlyfields"
  val INDEXING_INDEXONLY_FIELDS = "spark.indextables.indexing.indexonlyfields"
  val INDEXING_TOKENIZER_PREFIX = "spark.indextables.indexing.tokenizer."
}