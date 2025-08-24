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

import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
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
) extends WriteBuilder {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkWriteBuilder])

  override def build(): org.apache.spark.sql.connector.write.Write = {
    logger.info(s"Building optimized write for table at: $tablePath")
    // Use write options from info (DataFrame .option() calls), not table-level options
    // This ensures write-specific options override table/session configuration
    new Tantivy4SparkOptimizedWrite(transactionLog, tablePath, info, info.options(), hadoopConf)
  }
}