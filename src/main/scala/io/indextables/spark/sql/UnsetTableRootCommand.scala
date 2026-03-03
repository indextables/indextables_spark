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
 * SQL command to remove a named table root from companion index metadata.
 *
 * Syntax:
 *   - UNSET INDEXTABLES TABLE ROOT 'us-west-2' AT '/path/to/index'
 *   - UNSET TANTIVY4SPARK TABLE ROOT 'eu-central-1' AT table_name
 *
 * This is idempotent: if the root doesn't exist, the command succeeds with a warning message.
 *
 * Note: Concurrent SET/UNSET TABLE ROOT operations on the same table can race. See SetTableRootCommand for details.
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
      val resolvedPath = resolveTablePath(tablePath, sparkSession)
      logger.info(s"UNSET TABLE ROOT '$rootName' at $resolvedPath")

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

        // Check if root exists
        val rootKey      = s"indextables.companion.tableRoots.$rootName"
        val timestampKey = s"indextables.companion.tableRoots.$rootName.timestamp"

        if (!metadata.configuration.contains(rootKey)) {
          logger.warn(s"Table root '$rootName' does not exist, nothing to unset")
          return Seq(Row(s"Table root '$rootName' does not exist (no-op)"))
        }

        // Build updated configuration without the root entry
        val newConfig       = metadata.configuration - rootKey - timestampKey
        val updatedMetadata = metadata.copy(configuration = newConfig)

        // Write updated metadata via commitSyncActions (uses retry internally)
        transactionLog.commitSyncActions(
          removeActions = Seq.empty,
          addActions = Seq.empty,
          metadataUpdate = Some(updatedMetadata)
        )
        transactionLog.invalidateCache()

        logger.info(s"Successfully unset table root '$rootName'")
        Seq(Row(s"Table root '$rootName' removed"))
      } finally
        transactionLog.close()
    } catch {
      case e: Exception =>
        val errorMsg = s"Failed to unset table root: ${e.getMessage}"
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
