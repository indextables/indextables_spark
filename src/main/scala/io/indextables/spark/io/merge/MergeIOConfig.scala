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

package io.indextables.spark.io.merge

import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Configuration for merge I/O operations including downloads and uploads.
 *
 * @param maxConcurrencyPerCore
 *   Maximum concurrent downloads per CPU core (default: 8)
 * @param memoryBudgetBytes
 *   Memory budget for download operations in bytes (default: 2GB)
 * @param downloadRetries
 *   Number of retry attempts for failed downloads (default: 3)
 * @param uploadMaxConcurrency
 *   Maximum concurrent upload threads (default: 6)
 * @param retryBaseDelayMs
 *   Base delay in milliseconds for exponential backoff (default: 1000)
 * @param retryMaxDelayMs
 *   Maximum delay in milliseconds for exponential backoff (default: 30000)
 */
case class MergeIOConfig(
  maxConcurrencyPerCore: Int = MergeIOConfig.DEFAULT_MAX_CONCURRENCY_PER_CORE,
  memoryBudgetBytes: Long = MergeIOConfig.DEFAULT_MEMORY_BUDGET_BYTES,
  downloadRetries: Int = MergeIOConfig.DEFAULT_DOWNLOAD_RETRIES,
  uploadMaxConcurrency: Int = MergeIOConfig.DEFAULT_UPLOAD_MAX_CONCURRENCY,
  retryBaseDelayMs: Long = MergeIOConfig.DEFAULT_RETRY_BASE_DELAY_MS,
  retryMaxDelayMs: Long = MergeIOConfig.DEFAULT_RETRY_MAX_DELAY_MS) {

  /** Calculate maximum concurrent downloads based on available processors. */
  def maxConcurrentDownloads: Int =
    Runtime.getRuntime.availableProcessors() * maxConcurrencyPerCore

  /** Validate configuration values. */
  def validate(): MergeIOConfig = {
    require(maxConcurrencyPerCore > 0, s"maxConcurrencyPerCore must be > 0, got: $maxConcurrencyPerCore")
    require(memoryBudgetBytes > 0, s"memoryBudgetBytes must be > 0, got: $memoryBudgetBytes")
    require(downloadRetries >= 0, s"downloadRetries must be >= 0, got: $downloadRetries")
    require(uploadMaxConcurrency > 0, s"uploadMaxConcurrency must be > 0, got: $uploadMaxConcurrency")
    require(retryBaseDelayMs > 0, s"retryBaseDelayMs must be > 0, got: $retryBaseDelayMs")
    require(retryMaxDelayMs >= retryBaseDelayMs, s"retryMaxDelayMs must be >= retryBaseDelayMs")
    this
  }
}

object MergeIOConfig {

  // Configuration key constants
  val KEY_MAX_CONCURRENCY_PER_CORE = "spark.indextables.merge.download.maxConcurrencyPerCore"
  val KEY_MEMORY_BUDGET            = "spark.indextables.merge.download.memoryBudget"
  val KEY_DOWNLOAD_RETRIES         = "spark.indextables.merge.download.retries"
  val KEY_UPLOAD_MAX_CONCURRENCY   = "spark.indextables.merge.upload.maxConcurrency"
  val KEY_RETRY_BASE_DELAY_MS      = "spark.indextables.merge.download.retryBaseDelayMs"
  val KEY_RETRY_MAX_DELAY_MS       = "spark.indextables.merge.download.retryMaxDelayMs"

  // Default values
  val DEFAULT_MAX_CONCURRENCY_PER_CORE = 8
  val DEFAULT_MEMORY_BUDGET_BYTES      = 2L * 1024 * 1024 * 1024 // 2GB
  val DEFAULT_DOWNLOAD_RETRIES         = 3
  val DEFAULT_UPLOAD_MAX_CONCURRENCY   = 6
  val DEFAULT_RETRY_BASE_DELAY_MS      = 1000L
  val DEFAULT_RETRY_MAX_DELAY_MS       = 30000L

  /** Create default configuration. */
  def default: MergeIOConfig = MergeIOConfig()

  /** Create configuration from a case-insensitive string map (Spark options). */
  def fromOptions(options: CaseInsensitiveStringMap): MergeIOConfig =
    MergeIOConfig(
      maxConcurrencyPerCore = getIntOption(options, KEY_MAX_CONCURRENCY_PER_CORE, DEFAULT_MAX_CONCURRENCY_PER_CORE),
      memoryBudgetBytes = parseBytes(
        options.getOrDefault(KEY_MEMORY_BUDGET, formatBytes(DEFAULT_MEMORY_BUDGET_BYTES))
      ),
      downloadRetries = getIntOption(options, KEY_DOWNLOAD_RETRIES, DEFAULT_DOWNLOAD_RETRIES),
      uploadMaxConcurrency = getIntOption(options, KEY_UPLOAD_MAX_CONCURRENCY, DEFAULT_UPLOAD_MAX_CONCURRENCY),
      retryBaseDelayMs = getLongOption(options, KEY_RETRY_BASE_DELAY_MS, DEFAULT_RETRY_BASE_DELAY_MS),
      retryMaxDelayMs = getLongOption(options, KEY_RETRY_MAX_DELAY_MS, DEFAULT_RETRY_MAX_DELAY_MS)
    ).validate()

  /**
   * Create configuration from a Map[String, String] (e.g., serializable config).
   *
   * Key lookup is case-insensitive.
   */
  def fromMap(configs: Map[String, String]): MergeIOConfig = {
    // Build a case-insensitive lookup map
    val lowerCaseConfigs = configs.map { case (k, v) => k.toLowerCase -> v }
    def get(key: String): Option[String] =
      lowerCaseConfigs.get(key.toLowerCase)

    MergeIOConfig(
      maxConcurrencyPerCore =
        get(KEY_MAX_CONCURRENCY_PER_CORE).map(_.toInt).getOrElse(DEFAULT_MAX_CONCURRENCY_PER_CORE),
      memoryBudgetBytes = get(KEY_MEMORY_BUDGET).map(parseBytes).getOrElse(DEFAULT_MEMORY_BUDGET_BYTES),
      downloadRetries = get(KEY_DOWNLOAD_RETRIES).map(_.toInt).getOrElse(DEFAULT_DOWNLOAD_RETRIES),
      uploadMaxConcurrency = get(KEY_UPLOAD_MAX_CONCURRENCY).map(_.toInt).getOrElse(DEFAULT_UPLOAD_MAX_CONCURRENCY),
      retryBaseDelayMs = get(KEY_RETRY_BASE_DELAY_MS).map(_.toLong).getOrElse(DEFAULT_RETRY_BASE_DELAY_MS),
      retryMaxDelayMs = get(KEY_RETRY_MAX_DELAY_MS).map(_.toLong).getOrElse(DEFAULT_RETRY_MAX_DELAY_MS)
    ).validate()
  }

  private def getIntOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: Int
  ): Int = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default else value.toInt
  }

  private def getLongOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: Long
  ): Long = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default else value.toLong
  }

  /** Parse a byte size string (e.g., "2G", "512M", "1024K") into bytes. */
  def parseBytes(size: String): Long = {
    val trimmed = size.trim.toUpperCase
    val multiplier = trimmed.last match {
      case 'K' => 1024L
      case 'M' => 1024L * 1024
      case 'G' => 1024L * 1024 * 1024
      case 'T' => 1024L * 1024 * 1024 * 1024
      case _   => 1L
    }

    val numericPart = if (multiplier > 1) {
      trimmed.dropRight(1).trim
    } else {
      trimmed
    }

    numericPart.toLong * multiplier
  }

  /** Format bytes as a human-readable string. */
  def formatBytes(bytes: Long): String =
    if (bytes >= 1024L * 1024 * 1024 * 1024) s"${bytes / (1024L * 1024 * 1024 * 1024)}T"
    else if (bytes >= 1024L * 1024 * 1024) s"${bytes / (1024L * 1024 * 1024)}G"
    else if (bytes >= 1024L * 1024) s"${bytes / (1024L * 1024)}M"
    else if (bytes >= 1024L) s"${bytes / 1024L}K"
    else s"${bytes}B"
}
