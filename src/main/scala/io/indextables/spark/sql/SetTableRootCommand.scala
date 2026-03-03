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
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

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
 * Note: Concurrent SET TABLE ROOT operations on the same table can race. The underlying commitSyncActions retry
 * mechanism does not re-read metadata on conflict, so the last writer wins and may silently overwrite a concurrent
 * writer's root entry. This is acceptable for an admin-level operation — use DESCRIBE TABLE ROOTS to verify all roots
 * are registered after concurrent modifications.
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
      val resolvedPath = resolveTablePath(tablePath, sparkSession)
      logger.info(s"SET TABLE ROOT '$rootName' = '$rootPath' at $resolvedPath")

      // Build options map from Spark configuration
      val optionsMap = new java.util.HashMap[String, String]()
      sparkSession.conf.getAll.filter(_._1.startsWith("spark.indextables.")).foreach {
        case (k, v) => optionsMap.put(k, v)
      }
      optionsMap.put("spark.indextables.databricks.credential.operation", "PATH_READ_WRITE")
      val options = new CaseInsensitiveStringMap(optionsMap)

      val transactionLog = TransactionLogFactory.create(resolvedPath, sparkSession, options)
      try {
        // Read current metadata
        val metadata = transactionLog.getMetadata()

        // Validate companion mode is enabled
        val isCompanion = metadata.configuration.getOrElse("indextables.companion.enabled", "false") == "true"
        if (!isCompanion) {
          throw new IllegalStateException(
            "Cannot SET TABLE ROOT on a non-companion table. " +
              "Use BUILD INDEXTABLES COMPANION first to enable companion mode."
          )
        }

        // Build updated configuration with new root entry
        val rootKey      = s"indextables.companion.tableRoots.$rootName"
        val timestampKey = s"indextables.companion.tableRoots.$rootName.timestamp"
        val newConfig = metadata.configuration +
          (rootKey      -> rootPath) +
          (timestampKey -> System.currentTimeMillis().toString)

        val updatedMetadata = metadata.copy(configuration = newConfig)

        // Write updated metadata via commitSyncActions (uses retry internally)
        transactionLog.commitSyncActions(
          removeActions = Seq.empty,
          addActions = Seq.empty,
          metadataUpdate = Some(updatedMetadata)
        )
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

  /** Resolve table path from string path or table identifier. */
  private def resolveTablePath(pathOrTable: String, sparkSession: SparkSession): Path =
    if (
      pathOrTable.startsWith("/") || pathOrTable.startsWith("s3://") || pathOrTable.startsWith("s3a://") ||
      pathOrTable.startsWith("hdfs://") || pathOrTable.startsWith("file://") ||
      pathOrTable.startsWith("abfss://") || pathOrTable.startsWith("wasbs://")
    ) {
      new Path(pathOrTable)
    } else {
      try {
        val tableIdentifier = sparkSession.sessionState.sqlParser.parseTableIdentifier(pathOrTable)
        val catalog         = sparkSession.sessionState.catalog
        if (catalog.tableExists(tableIdentifier)) {
          val tableMetadata = catalog.getTableMetadata(tableIdentifier)
          new Path(tableMetadata.location)
        } else {
          throw new IllegalArgumentException(s"Table not found: $pathOrTable")
        }
      } catch {
        case _: Exception =>
          new Path(pathOrTable)
      }
    }
}
