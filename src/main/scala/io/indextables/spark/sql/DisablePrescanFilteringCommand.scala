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
 * SQL command to disable prescan filtering for the current SparkSession.
 *
 * Syntax:
 *   DISABLE INDEXTABLES PRESCAN FILTERING                        -- Disable globally for session
 *   DISABLE INDEXTABLES PRESCAN FILTERING FOR 's3://bucket/path' -- Disable for specific path
 *   DISABLE INDEXTABLES PRESCAN FILTERING FOR my_catalog.my_table -- Disable for specific table
 *
 * This command sets session-scoped state that disables prescan filtering for queries.
 * The setting persists until the session ends or ENABLE is called.
 *
 * @param pathOption Optional path to disable prescan for
 * @param tableOption Optional table identifier to disable prescan for
 */
case class DisablePrescanFilteringCommand(
  pathOption: Option[String],
  tableOption: Option[TableIdentifier]
) extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[DisablePrescanFilteringCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("scope", StringType, nullable = false)(),
    AttributeReference("message", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info(s"Executing DISABLE INDEXTABLES PRESCAN FILTERING command")

    try {
      // Determine the table path for session state
      val tablePath = resolveTablePath(sparkSession)

      // Disable prescan filtering for this session
      PrescanSessionState.disable(sparkSession, tablePath)

      val scope = tablePath.getOrElse("all tables in session")
      logger.info(s"Prescan filtering disabled for: $scope")

      Seq(Row(
        "success",
        scope,
        s"Prescan filtering disabled for $scope. This setting is session-scoped."
      ))

    } catch {
      case ex: Exception =>
        logger.error("Error disabling prescan filtering", ex)
        Seq(Row(
          "failed",
          "error",
          s"Failed to disable prescan filtering: ${ex.getMessage}"
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
