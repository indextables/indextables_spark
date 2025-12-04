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

package io.indextables.spark.config

import io.indextables.spark.transaction.MetadataAction

/**
 * Configuration properties for IndexTables4Spark tables. These can be set as table properties in the transaction log.
 */
object IndexTables4SparkConfig {

  /** Base trait for all IndexTables4Spark configuration entries. */
  sealed trait ConfigEntry[T] {
    def key: String
    def defaultValue: T
    def fromString(value: String): T
    def toString(value: T): String
    def fromMetadata(metadata: MetadataAction): Option[T] =
      metadata.configuration.get(key).map(fromString)
  }

  /** Boolean configuration entry. */
  case class BooleanConfigEntry(
    key: String,
    defaultValue: Boolean)
      extends ConfigEntry[Boolean] {
    override def fromString(value: String): Boolean = value.toBoolean
    override def toString(value: Boolean): String   = value.toString
  }

  /** Long configuration entry. */
  case class LongConfigEntry(
    key: String,
    defaultValue: Long,
    validator: Long => Boolean = _ => true)
      extends ConfigEntry[Long] {
    override def fromString(value: String): Long = {
      val parsed = value.toLong
      require(validator(parsed), s"Invalid value for $key: $value")
      parsed
    }
    override def toString(value: Long): String = value.toString
  }

  /////////////////////
  // General Table Configuration
  /////////////////////

  /** Enable/disable bloom filters for this table. */
  val BLOOM_FILTERS_ENABLED: BooleanConfigEntry = BooleanConfigEntry(
    "tantivy4spark.bloom.filters.enabled",
    defaultValue = true
  )

  /** Force standard storage operations (disable S3 optimizations). */
  val STORAGE_FORCE_STANDARD: BooleanConfigEntry = BooleanConfigEntry(
    "tantivy4spark.storage.force.standard",
    defaultValue = false
  )

  /** All configuration entries for easy access. */
  val ALL_CONFIGS: Seq[ConfigEntry[_]] = Seq(
    BLOOM_FILTERS_ENABLED,
    STORAGE_FORCE_STANDARD
  )

  /**
   * Get configuration value with fallback hierarchy:
   *   1. Table property (from metadata) 2. Default value
   */
  def getConfigValue[T](config: ConfigEntry[T], metadata: MetadataAction): T =
    config.fromMetadata(metadata).getOrElse(config.defaultValue)
}
