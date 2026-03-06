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

package io.indextables.spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StringType
import io.indextables.spark.transaction.TransactionLogFactory
import org.slf4j.LoggerFactory

/**
 * SQL command to list all registered table roots for a companion index.
 *
 * Syntax:
 *   - DESCRIBE INDEXTABLES TABLE ROOTS AT '/path/to/index'
 *   - DESCRIBE TANTIVY4SPARK TABLE ROOTS AT table_name
 *
 * Returns rows with root_name, root_path, and set_timestamp for each registered table root.
 */
case class DescribeTableRootsCommand(tablePath: String) extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[DescribeTableRootsCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("root_name", StringType)(),
    AttributeReference("root_path", StringType)(),
    AttributeReference("set_timestamp", StringType)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] =
    try {
      val resolvedPath = TableRootUtils.resolveTablePath(tablePath, sparkSession)
      logger.info(s"DESCRIBE TABLE ROOTS at $resolvedPath")

      val options = TableRootUtils.buildOptions(sparkSession, Some("PATH_READ"))

      val transactionLog = TransactionLogFactory.create(resolvedPath, sparkSession, options)
      try {
        transactionLog.invalidateCache()
        val metadata = transactionLog.getMetadata()

        // Extract root entries (exclude .timestamp suffix keys)
        val rootEntries = metadata.configuration
          .filter { case (key, _) => TableRootUtils.isRootKey(key) }
          .map {
            case (key, path) =>
              val rootName = TableRootUtils.extractRootName(key)
              val timestamp = metadata.configuration
                .getOrElse(TableRootUtils.timestampKey(rootName), "unknown")
              Row(rootName, path, timestamp)
          }
          .toSeq
          .sortBy(_.getString(0))

        logger.info(s"Found ${rootEntries.size} table roots")
        rootEntries
      } finally
        transactionLog.close()
    } catch {
      case e: Exception =>
        val errorMsg = s"Failed to describe table roots: ${e.getMessage}"
        logger.error(errorMsg, e)
        throw new RuntimeException(errorMsg, e)
    }

}
