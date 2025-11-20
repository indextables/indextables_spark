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

import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsOverwrite, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.config.IndexTables4SparkConfig
import io.indextables.spark.transaction.TransactionLog
import org.slf4j.LoggerFactory

class IndexTables4SparkWriteBuilder(
  transactionLog: TransactionLog,
  tablePath: Path,
  info: LogicalWriteInfo,
  options: CaseInsensitiveStringMap,
  hadoopConf: org.apache.hadoop.conf.Configuration)
    extends WriteBuilder
    with SupportsTruncate
    with SupportsOverwrite {

  private val logger      = LoggerFactory.getLogger(classOf[IndexTables4SparkWriteBuilder])
  private var isOverwrite = false

  override def truncate(): WriteBuilder = {
    logger.info("Truncate mode enabled for write operation")
    isOverwrite = true
    this
  }

  override def overwrite(filters: Array[org.apache.spark.sql.sources.Filter]): WriteBuilder = {
    logger.info(s"Overwrite mode enabled with ${filters.length} filters")
    isOverwrite = true
    // For now, ignore filters and do full table overwrite
    // TODO: Implement filter-based overwrite (replaceWhere functionality)
    this
  }

  override def build(): org.apache.spark.sql.connector.write.Write = {
    logger.info(s"Building write for table at: $tablePath (overwrite mode: $isOverwrite)")

    // Serialize options to Map[String, String] to avoid CaseInsensitiveStringMap serialization issues
    // Use the enhanced options that may contain partition information, not info.options()
    import scala.jdk.CollectionConverters._
    var serializedOptions = options.entrySet().asScala.map(entry => entry.getKey -> entry.getValue).toMap

    // Check if optimized write is enabled
    val tantivyOptions = IndexTables4SparkOptions(options)
    val spark          = org.apache.spark.sql.SparkSession.active

    // Set split conversion max parallelism if not already configured
    // Default: max(1, availableProcessors / 4)
    val configKey = io.indextables.spark.config.IndexTables4SparkSQLConf.TANTIVY4SPARK_SPLIT_CONVERSION_MAX_PARALLELISM
    if (!serializedOptions.contains(configKey)) {
      val availableProcessors = Runtime.getRuntime.availableProcessors()
      val maxParallelism      = Math.max(1, availableProcessors / 4)
      logger.info(s"Auto-configuring split conversion max parallelism: $maxParallelism (from availableProcessors=$availableProcessors)")
      serializedOptions = serializedOptions + (configKey -> maxParallelism.toString)
    }

    // Check DataFrame write options first
    val optimizeWriteEnabled = tantivyOptions.optimizeWrite.getOrElse {
      // Check Spark session configuration
      spark.conf
        .getOption("spark.indextables.optimizeWrite.enabled")
        .map(_.toBoolean)
        .getOrElse {
          // Check table properties or use default
          try {
            val metadata = transactionLog.getMetadata()
            IndexTables4SparkConfig.OPTIMIZE_WRITE
              .fromMetadata(metadata)
              .getOrElse(
                IndexTables4SparkConfig.OPTIMIZE_WRITE.defaultValue
              )
          } catch {
            case _: Exception => IndexTables4SparkConfig.OPTIMIZE_WRITE.defaultValue
          }
        }
    }

    logger.debug(
      s"WriteBuilder decision: optimizeWriteEnabled = $optimizeWriteEnabled, options = ${tantivyOptions.optimizeWrite}"
    )

    if (optimizeWriteEnabled) {
      logger.info("Using IndexTables4SparkOptimizedWrite with RequiresDistributionAndOrdering")

      // Try to get row count hint from options or compute it
      val estimatedRowCount = serializedOptions.get("estimatedRowCount").map(_.toLong).getOrElse {
        // Default estimate - in production you might sample the DataFrame
        1000000L
      }

      logger.info(s"Creating IndexTables4SparkOptimizedWrite with estimatedRowCount = $estimatedRowCount")
      val optimizedWrite = new IndexTables4SparkOptimizedWrite(
        transactionLog,
        tablePath,
        info,
        serializedOptions,
        hadoopConf,
        isOverwrite,
        estimatedRowCount
      )
      logger.info(s"Created write instance: ${optimizedWrite.getClass.getSimpleName}")
      optimizedWrite
    } else {
      logger.info("Using IndexTables4SparkStandardWrite without RequiresDistributionAndOrdering")
      val standardWrite =
        new IndexTables4SparkStandardWrite(transactionLog, tablePath, info, serializedOptions, hadoopConf, isOverwrite)
      logger.info(s"Created write instance: ${standardWrite.getClass.getSimpleName}")
      standardWrite
    }
  }
}
