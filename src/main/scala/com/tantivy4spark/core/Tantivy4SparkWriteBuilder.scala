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

import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder, SupportsTruncate, SupportsOverwrite}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.hadoop.fs.Path
import com.tantivy4spark.transaction.TransactionLog
import org.slf4j.LoggerFactory

class Tantivy4SparkWriteBuilder(
    transactionLog: TransactionLog,
    tablePath: Path,
    info: LogicalWriteInfo,
    options: CaseInsensitiveStringMap,
    hadoopConf: org.apache.hadoop.conf.Configuration
) extends WriteBuilder with SupportsTruncate with SupportsOverwrite {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkWriteBuilder])
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
    logger.info(s"Building optimized write for table at: $tablePath (overwrite mode: $isOverwrite)")
    // Use write options from info (DataFrame .option() calls), not table-level options
    // This ensures write-specific options override table/session configuration
    
    // Serialize options to Map[String, String] to avoid CaseInsensitiveStringMap serialization issues
    import scala.jdk.CollectionConverters._
    val serializedOptions = info.options().entrySet().asScala.map { entry =>
      entry.getKey -> entry.getValue
    }.toMap
    
    new Tantivy4SparkOptimizedWrite(transactionLog, tablePath, info, serializedOptions, hadoopConf, isOverwrite)
  }
}