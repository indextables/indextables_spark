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
      val resolvedPath = resolveTablePath(tablePath, sparkSession)
      logger.info(s"DESCRIBE TABLE ROOTS at $resolvedPath")

      // Build options map from Spark configuration
      val optionsMap = new java.util.HashMap[String, String]()
      sparkSession.conf.getAll.filter(_._1.startsWith("spark.indextables.")).foreach {
        case (k, v) => optionsMap.put(k, v)
      }
      val options = new CaseInsensitiveStringMap(optionsMap)

      val transactionLog = TransactionLogFactory.create(resolvedPath, sparkSession, options)
      try {
        val metadata = transactionLog.getMetadata()
        val prefix   = "indextables.companion.tableRoots."

        // Extract root entries (exclude .timestamp suffix keys)
        val rootEntries = metadata.configuration
          .filter {
            case (key, _) =>
              key.startsWith(prefix) && !key.endsWith(".timestamp")
          }
          .map {
            case (key, path) =>
              val rootName = key.stripPrefix(prefix)
              val timestamp = metadata.configuration
                .getOrElse(s"$prefix$rootName.timestamp", "unknown")
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
