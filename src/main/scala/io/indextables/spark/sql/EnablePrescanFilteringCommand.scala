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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StringType

import io.indextables.spark.prescan.PrescanSessionState

import org.slf4j.LoggerFactory

/**
 * SQL command to enable prescan filtering for the current SparkSession.
 *
 * Syntax:
 *   ENABLE INDEXTABLES PRESCAN FILTERING                        -- Enable globally for session
 *   ENABLE INDEXTABLES PRESCAN FILTERING FOR 's3://bucket/path' -- Enable for specific path
 *   ENABLE INDEXTABLES PRESCAN FILTERING FOR my_catalog.my_table -- Enable for specific table
 *
 * This command sets session-scoped state that enables prescan filtering for queries.
 * The setting persists until the session ends or DISABLE is called.
 *
 * @param pathOption Optional path to enable prescan for
 * @param tableOption Optional table identifier to enable prescan for
 */
case class EnablePrescanFilteringCommand(
  pathOption: Option[String],
  tableOption: Option[TableIdentifier]
) extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[EnablePrescanFilteringCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("scope", StringType, nullable = false)(),
    AttributeReference("message", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info(s"Executing ENABLE INDEXTABLES PRESCAN FILTERING command")

    try {
      // Determine the table path for session state
      val tablePath = resolveTablePath(sparkSession)

      // Enable prescan filtering for this session
      PrescanSessionState.enable(sparkSession, tablePath)

      val scope = tablePath.getOrElse("all tables in session")
      logger.info(s"Prescan filtering enabled for: $scope")

      Seq(Row(
        "success",
        scope,
        s"Prescan filtering enabled for $scope. This setting is session-scoped."
      ))

    } catch {
      case ex: Exception =>
        logger.error("Error enabling prescan filtering", ex)
        Seq(Row(
          "failed",
          "error",
          s"Failed to enable prescan filtering: ${ex.getMessage}"
        ))
    }
  }

  /**
   * Resolve the table path from pathOption or tableOption.
   *
   * @param sparkSession SparkSession for catalog resolution
   * @return Some(path) for a specific table, None for global session setting
   */
  private def resolveTablePath(sparkSession: SparkSession): Option[String] = {
    if (pathOption.isDefined) {
      pathOption
    } else if (tableOption.isDefined) {
      // Use the table identifier as the path
      // Note: We don't resolve to physical location since session state is keyed by logical identifier
      Some(tableOption.get.unquotedString)
    } else {
      // Global session setting
      None
    }
  }
}
