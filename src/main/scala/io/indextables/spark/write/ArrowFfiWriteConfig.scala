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

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.util.{ConfigParsingUtils, SizeParser}
import org.slf4j.LoggerFactory

/**
 * Configuration for Arrow FFI columnar ingestion on the write path.
 *
 * InternalRows are buffered into Arrow VectorSchemaRoot on the JVM and flushed to Rust via Arrow C Data Interface FFI
 * (zero-copy pointer handoff).
 *
 * MIGRATION NOTE: The `batchSize` config controls how many InternalRows are buffered before exporting via FFI. This
 * parameter becomes irrelevant when Spark adds native `DataWriter[ColumnarBatch]` support (Spark controls batch size
 * upstream). The config is retained for backward compatibility but will be deprecated when columnar writes are
 * available.
 *
 * @param batchSize
 *   Number of rows to buffer before flushing via FFI (default: 8192)
 * @param heapSize
 *   Heap size in bytes for the native split writer. Default: 256MB.
 */
case class ArrowFfiWriteConfig(
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

  // Deprecated key — the TANT batch write path has been removed and Arrow FFI is now the only write path.
  // Retained only for logging a deprecation warning when users still pass this config.
  private val KEY_ENABLED_DEPRECATED = "spark.indextables.write.arrowFfi.enabled"

  // Configuration key constants
  val KEY_BATCH_SIZE = "spark.indextables.write.arrowFfi.batchSize"
  val KEY_HEAP_SIZE  = "spark.indextables.indexWriter.heapSize"

  // Default values
  val DEFAULT_BATCH_SIZE: Int = 8192
  val DEFAULT_HEAP_SIZE: Long = 256L * 1024 * 1024 // 256MB

  def default: ArrowFfiWriteConfig = ArrowFfiWriteConfig()

  private val deprecatedKeyWarned = new java.util.concurrent.atomic.AtomicBoolean(false)

  private def warnIfDeprecatedKeyPresent(hasKey: String => Boolean): Unit =
    if (hasKey(KEY_ENABLED_DEPRECATED) && deprecatedKeyWarned.compareAndSet(false, true)) {
      logger.warn(
        s"Configuration key '$KEY_ENABLED_DEPRECATED' is deprecated and ignored. " +
          "The legacy TANT batch write path has been removed; Arrow FFI is now the only write path. " +
          "You can safely remove this setting."
      )
    }

  def fromOptions(options: CaseInsensitiveStringMap): ArrowFfiWriteConfig = {
    warnIfDeprecatedKeyPresent(key => options.containsKey(key))

    val batchSize = ConfigParsingUtils.getIntOption(options, KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE, mustBePositive = true)
    val heapSize = ConfigParsingUtils.getLongOption(
      options,
      KEY_HEAP_SIZE,
      DEFAULT_HEAP_SIZE,
      mustBePositive = true,
      supportSizeSuffix = true
    )

    val config = ArrowFfiWriteConfig(
      batchSize = batchSize,
      heapSize = heapSize
    ).validate()

    logger.info(s"ArrowFfiWriteConfig: batchSize=$batchSize")
    config
  }

  def fromMap(configs: Map[String, String]): ArrowFfiWriteConfig = {
    val get = ConfigParsingUtils.caseInsensitiveLookup(configs)
    warnIfDeprecatedKeyPresent(key => get(key).isDefined)

    ArrowFfiWriteConfig(
      batchSize = get(KEY_BATCH_SIZE).map(_.toInt).getOrElse(DEFAULT_BATCH_SIZE),
      heapSize = get(KEY_HEAP_SIZE).map(SizeParser.parseSize).getOrElse(DEFAULT_HEAP_SIZE)
    ).validate()
  }

}
