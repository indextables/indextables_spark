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
}