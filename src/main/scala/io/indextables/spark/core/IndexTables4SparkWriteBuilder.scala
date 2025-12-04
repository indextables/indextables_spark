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

    // Set split conversion max parallelism if not already configured
    // Default: max(1, availableProcessors / 4)
    val configKey = io.indextables.spark.config.IndexTables4SparkSQLConf.TANTIVY4SPARK_SPLIT_CONVERSION_MAX_PARALLELISM
    if (!serializedOptions.contains(configKey)) {
      val availableProcessors = Runtime.getRuntime.availableProcessors()
      val maxParallelism      = Math.max(1, availableProcessors / 4)
      logger.info(s"Auto-configuring split conversion max parallelism: $maxParallelism (from availableProcessors=$availableProcessors)")
      serializedOptions = serializedOptions + (configKey -> maxParallelism.toString)
    }

    logger.info("Using IndexTables4SparkStandardWrite")
    val standardWrite =
      new IndexTables4SparkStandardWrite(transactionLog, tablePath, info, serializedOptions, hadoopConf, isOverwrite)
    logger.info(s"Created write instance: ${standardWrite.getClass.getSimpleName}")
    standardWrite
  }
}
