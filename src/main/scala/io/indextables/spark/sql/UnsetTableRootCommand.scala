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
 * SQL command to remove a named table root from companion index metadata.
 *
 * Syntax:
 *   - UNSET INDEXTABLES TABLE ROOT 'us-west-2' AT '/path/to/index'
 *   - UNSET TANTIVY4SPARK TABLE ROOT 'eu-central-1' AT table_name
 *
 * This is idempotent: if the root doesn't exist, the command succeeds with a warning message.
 *
 * Concurrent safety: This command uses commitMetadataUpdate which re-reads metadata and re-applies the transform on
 * each retry attempt, ensuring concurrent SET/UNSET TABLE ROOT operations compose correctly without silent overwrites.
 */
case class UnsetTableRootCommand(
  rootName: String,
  tablePath: String)
    extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[UnsetTableRootCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("status", StringType)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] =
    try {
      val resolvedPath = TableRootUtils.resolveTablePath(tablePath, sparkSession)
      logger.info(s"UNSET TABLE ROOT '$rootName' at $resolvedPath")

      val options = TableRootUtils.buildOptions(sparkSession, Some("PATH_READ_WRITE"))

      val transactionLog = TransactionLogFactory.create(resolvedPath, sparkSession, options)
      try {
        // Invalidate cache to ensure fresh metadata read (global caches may have stale data
        // from prior operations on the same table path)
        transactionLog.invalidateCache()
        // Check if root exists (read once before entering retry loop for the no-op case)
        val metadata = transactionLog.getMetadata()
        val rKey     = TableRootUtils.rootKey(rootName)
        val tKey     = TableRootUtils.timestampKey(rootName)

        if (!metadata.configuration.contains(rKey)) {
          logger.warn(s"Table root '$rootName' does not exist, nothing to unset")
          Seq(Row(s"Table root '$rootName' does not exist (no-op)"))
        } else {
          // Use commitMetadataUpdate for concurrent safety — the transform is re-applied
          // on each retry after re-reading fresh metadata, so concurrent writers compose correctly.
          transactionLog.commitMetadataUpdate { currentMetadata =>
            currentMetadata.copy(configuration = currentMetadata.configuration - rKey - tKey)
          }
          transactionLog.invalidateCache()

          logger.info(s"Successfully unset table root '$rootName'")
          Seq(Row(s"Table root '$rootName' removed"))
        }
      } finally
        transactionLog.close()
    } catch {
      case e: Exception =>
        val errorMsg = s"Failed to unset table root: ${e.getMessage}"
        logger.error(errorMsg, e)
        throw new RuntimeException(errorMsg, e)
    }

}
