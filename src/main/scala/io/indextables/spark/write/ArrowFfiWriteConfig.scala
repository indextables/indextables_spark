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

package io.indextables.spark.write

import io.indextables.spark.util.{ConfigParsingUtils, SizeParser}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

/**
 * Configuration for Arrow FFI columnar ingestion on the write path.
 *
 * When enabled, InternalRows are buffered into Arrow VectorSchemaRoot on the JVM and flushed to Rust via Arrow C Data
 * Interface FFI (zero-copy pointer handoff). This eliminates the TANT serialize/deserialize overhead (~9ms per batch),
 * replacing it with FFI pointer handoff (~0.25ms per batch).
 *
 * MIGRATION NOTE: The `batchSize` config controls how many InternalRows are buffered before exporting via FFI. This
 * parameter becomes irrelevant when Spark adds native `DataWriter[ColumnarBatch]` support (Spark controls batch size
 * upstream). The config is retained for backward compatibility but will be deprecated when columnar writes are
 * available.
 *
 * @param enabled
 *   Whether Arrow FFI write path is enabled (default: true)
 * @param batchSize
 *   Number of rows to buffer before flushing via FFI (default: 8192)
 * @param heapSize
 *   Heap size in bytes for the native split writer. Reads from the shared `spark.indextables.indexWriter.heapSize`
 *   config (same as the TANT batch path) so users don't need to configure heap size separately. Default: 256MB.
 */
case class ArrowFfiWriteConfig(
  enabled: Boolean = ArrowFfiWriteConfig.DEFAULT_ENABLED,
  batchSize: Int = ArrowFfiWriteConfig.DEFAULT_BATCH_SIZE,
  heapSize: Long = ArrowFfiWriteConfig.DEFAULT_HEAP_SIZE) {

  def validate(): ArrowFfiWriteConfig = {
    require(
      batchSize > 0,
      s"batchSize must be > 0, got: $batchSize"
    )
    require(
      heapSize > 0,
      s"heapSize must be > 0, got: $heapSize"
    )
    this
  }
}

object ArrowFfiWriteConfig {
  private val logger = LoggerFactory.getLogger(getClass)

  // Configuration key constants
  val KEY_ENABLED    = "spark.indextables.write.arrowFfi.enabled"
  val KEY_BATCH_SIZE = "spark.indextables.write.arrowFfi.batchSize"
  // Shared with TANT batch path — both write paths use the same heap size config
  val KEY_HEAP_SIZE = "spark.indextables.indexWriter.heapSize"

  // Default values
  val DEFAULT_ENABLED: Boolean = true
  val DEFAULT_BATCH_SIZE: Int  = 8192
  val DEFAULT_HEAP_SIZE: Long  = 256L * 1024 * 1024 // 256MB

  def default: ArrowFfiWriteConfig = ArrowFfiWriteConfig()

  def fromOptions(options: CaseInsensitiveStringMap): ArrowFfiWriteConfig = {
    val enabled   = getBooleanOption(options, KEY_ENABLED, DEFAULT_ENABLED)
    val batchSize = getIntOption(options, KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE, mustBePositive = true)
    val heapSize =
      getLongOption(options, KEY_HEAP_SIZE, DEFAULT_HEAP_SIZE, mustBePositive = true, supportSizeSuffix = true)

    val config = ArrowFfiWriteConfig(
      enabled = enabled,
      batchSize = batchSize,
      heapSize = heapSize
    )

    if (enabled) {
      logger.info(s"ArrowFfiWriteConfig: enabled=$enabled, batchSize=$batchSize")
    } else {
      logger.debug(s"ArrowFfiWriteConfig: disabled")
    }

    config
  }

  def fromMap(configs: Map[String, String]): ArrowFfiWriteConfig = {
    val get = ConfigParsingUtils.caseInsensitiveLookup(configs)

    ArrowFfiWriteConfig(
      enabled = get(KEY_ENABLED).map(_.toBoolean).getOrElse(DEFAULT_ENABLED),
      batchSize = get(KEY_BATCH_SIZE).map(_.toInt).getOrElse(DEFAULT_BATCH_SIZE),
      heapSize = get(KEY_HEAP_SIZE).map(SizeParser.parseSize).getOrElse(DEFAULT_HEAP_SIZE)
    ).validate()
  }

  private def getBooleanOption(options: CaseInsensitiveStringMap, key: String, default: Boolean): Boolean =
    ConfigParsingUtils.getBooleanOption(options, key, default)

  private def getIntOption(options: CaseInsensitiveStringMap, key: String, default: Int, mustBePositive: Boolean = false): Int =
    ConfigParsingUtils.getIntOption(options, key, default, mustBePositive)

  private def getLongOption(options: CaseInsensitiveStringMap, key: String, default: Long, mustBePositive: Boolean = false, supportSizeSuffix: Boolean = false): Long =
    ConfigParsingUtils.getLongOption(options, key, default, mustBePositive, supportSizeSuffix)
}
