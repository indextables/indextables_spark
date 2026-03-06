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
 * SQL command to register a named table root in companion index metadata.
 *
 * Syntax:
 *   - SET INDEXTABLES TABLE ROOT 'us-west-2' = 's3://bucket-west/data' AT '/path/to/index'
 *   - SET TANTIVY4SPARK TABLE ROOT 'eu-central-1' = 's3://bucket-eu/data' AT table_name
 *
 * This stores the root name and path in the transaction log metadata configuration so readers in different regions can
 * select the appropriate data root.
 *
 * Concurrent safety: This command uses commitMetadataUpdate which re-reads metadata and re-applies the transform on
 * each retry attempt, ensuring concurrent SET/UNSET TABLE ROOT operations compose correctly without silent overwrites.
 */
case class SetTableRootCommand(
  rootName: String,
  rootPath: String,
  tablePath: String)
    extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[SetTableRootCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("status", StringType)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] =
    try {
      val resolvedPath = TableRootUtils.resolveTablePath(tablePath, sparkSession)
      logger.info(s"SET TABLE ROOT '$rootName' = '$rootPath' at $resolvedPath")

      val options = TableRootUtils.buildOptions(sparkSession, Some("PATH_READ_WRITE"))

      val transactionLog = TransactionLogFactory.create(resolvedPath, sparkSession, options)
      try {
        // Invalidate cache to ensure fresh metadata read (global caches may have stale data
        // from prior operations on the same table path)
        transactionLog.invalidateCache()
        // Validate companion mode is enabled (read once before entering retry loop)
        val metadata    = transactionLog.getMetadata()
        val isCompanion = metadata.configuration.getOrElse("indextables.companion.enabled", "false") == "true"
        if (!isCompanion) {
          throw new IllegalStateException(
            "Cannot SET TABLE ROOT on a non-companion table. " +
              "Use BUILD INDEXTABLES COMPANION first to enable companion mode."
          )
        }

        // Use commitMetadataUpdate for concurrent safety — the transform is re-applied
        // on each retry after re-reading fresh metadata, so concurrent writers compose correctly.
        transactionLog.commitMetadataUpdate { currentMetadata =>
          val timestamp = System.currentTimeMillis().toString
          currentMetadata.copy(configuration =
            currentMetadata.configuration +
              (TableRootUtils.rootKey(rootName)      -> rootPath) +
              (TableRootUtils.timestampKey(rootName) -> timestamp)
          )
        }
        transactionLog.invalidateCache()

        logger.info(s"Successfully set table root '$rootName' = '$rootPath'")
        Seq(Row(s"Table root '$rootName' set to '$rootPath'"))
      } finally
        transactionLog.close()
    } catch {
      case e: IllegalStateException =>
        throw e
      case e: Exception =>
        val errorMsg = s"Failed to set table root: ${e.getMessage}"
        logger.error(errorMsg, e)
        throw new RuntimeException(errorMsg, e)
    }

}
