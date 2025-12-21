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

package io.indextables.spark.prescan

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.SparkSession

import org.slf4j.LoggerFactory

/**
 * Manages session-scoped ENABLE/DISABLE state for prescan filtering.
 *
 * State is keyed by (applicationId, tablePath) where tablePath can be:
 * - A specific table path (e.g., "s3://bucket/table")
 * - "*" for global session-level setting
 *
 * Priority when checking if prescan is enabled:
 * 1. Table-specific setting (if exists)
 * 2. Global session setting (if exists)
 * 3. None (fall back to config property resolution)
 *
 * State is automatically scoped to the Spark application and cleared when the
 * application ends.
 */
object PrescanSessionState {

  private val logger = LoggerFactory.getLogger(getClass)

  // Key: "applicationId:tablePath", Value: enabled state
  private val stateMap = new ConcurrentHashMap[String, Boolean]()

  // Sentinel value for global (all tables) setting
  private val GLOBAL_TABLE_PATH = "*"

  /**
   * Build a state map key from application ID and optional table path.
   */
  private def buildKey(applicationId: String, tablePath: Option[String]): String =
    s"$applicationId:${tablePath.getOrElse(GLOBAL_TABLE_PATH)}"

  /**
   * Enable prescan filtering for a specific table or globally for the session.
   *
   * @param spark SparkSession
   * @param tablePath Optional table path; None means global for all tables in session
   */
  def enable(spark: SparkSession, tablePath: Option[String]): Unit = {
    val applicationId = spark.sparkContext.applicationId
    val key = buildKey(applicationId, tablePath)
    stateMap.put(key, true)

    val scope = tablePath.getOrElse("all tables in session")
    logger.info(s"Prescan filtering enabled for $scope (applicationId=$applicationId)")
  }

  /**
   * Disable prescan filtering for a specific table or globally for the session.
   *
   * @param spark SparkSession
   * @param tablePath Optional table path; None means global for all tables in session
   */
  def disable(spark: SparkSession, tablePath: Option[String]): Unit = {
    val applicationId = spark.sparkContext.applicationId
    val key = buildKey(applicationId, tablePath)
    stateMap.put(key, false)

    val scope = tablePath.getOrElse("all tables in session")
    logger.info(s"Prescan filtering disabled for $scope (applicationId=$applicationId)")
  }

  /**
   * Check if prescan filtering is enabled for a specific table.
   *
   * Checks table-specific setting first, then falls back to global session setting.
   *
   * @param tablePath Table path to check
   * @param spark Implicit SparkSession
   * @return Some(true) if enabled, Some(false) if disabled, None if no session override
   */
  def isEnabled(tablePath: String)(implicit spark: SparkSession): Option[Boolean] = {
    val applicationId = spark.sparkContext.applicationId

    // Check table-specific state first
    val tableKey = buildKey(applicationId, Some(tablePath))
    if (stateMap.containsKey(tableKey)) {
      return Some(stateMap.get(tableKey))
    }

    // Fall back to global session state
    val globalKey = buildKey(applicationId, None)
    if (stateMap.containsKey(globalKey)) {
      return Some(stateMap.get(globalKey))
    }

    // No session override
    None
  }

  /**
   * Clear all prescan state for the given SparkSession.
   * Typically called when the application ends or for testing.
   *
   * @param spark SparkSession
   */
  def clear(spark: SparkSession): Unit = {
    val applicationId = spark.sparkContext.applicationId
    val prefix = applicationId + ":"

    val keysToRemove = new java.util.ArrayList[String]()
    stateMap.keySet().forEach { key =>
      if (key.startsWith(prefix)) {
        keysToRemove.add(key)
      }
    }

    keysToRemove.forEach(stateMap.remove(_))
    logger.debug(s"Cleared ${keysToRemove.size()} prescan state entries for applicationId=$applicationId")
  }

  /**
   * Get the current state for a table (for debugging/testing).
   *
   * @param tablePath Table path to check
   * @param spark Implicit SparkSession
   * @return Description of current state
   */
  def getStateDescription(tablePath: String)(implicit spark: SparkSession): String = {
    isEnabled(tablePath) match {
      case Some(true) => "enabled (session override)"
      case Some(false) => "disabled (session override)"
      case None => "no session override"
    }
  }

  /**
   * Clear all state (for testing only).
   */
  private[prescan] def clearAll(): Unit = {
    stateMap.clear()
  }
}
