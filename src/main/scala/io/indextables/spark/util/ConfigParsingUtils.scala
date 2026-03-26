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

package io.indextables.spark.util

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

/**
 * Shared type-safe configuration parsing helpers for CaseInsensitiveStringMap.
 *
 * All methods follow the same pattern: extract value from options, parse it, validate it, and return the default on
 * failure with a warning log.
 */
object ConfigParsingUtils {

  private val logger = LoggerFactory.getLogger(getClass)

  def getBooleanOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: Boolean
  ): Boolean = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default
    else {
      try
        value.toBoolean
      catch {
        case _: IllegalArgumentException =>
          logger.warn(s"Invalid boolean value for $key: '$value', using default: $default")
          default
      }
    }
  }

  def getIntOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: Int,
    mustBePositive: Boolean = false
  ): Int = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default
    else {
      try {
        val parsed = value.toInt
        if (mustBePositive && parsed <= 0) {
          logger.warn(s"Invalid value for $key: '$value' (must be > 0), using default: $default")
          default
        } else {
          parsed
        }
      } catch {
        case _: NumberFormatException =>
          logger.warn(s"Invalid integer value for $key: '$value', using default: $default")
          default
      }
    }
  }

  def getLongOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: Long,
    mustBePositive: Boolean = false,
    supportSizeSuffix: Boolean = false
  ): Long = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default
    else {
      try {
        val parsed = if (supportSizeSuffix) SizeParser.parseSize(value) else value.toLong
        if (mustBePositive && parsed <= 0) {
          logger.warn(s"Invalid value for $key: '$value' (must be > 0), using default: $default")
          default
        } else {
          parsed
        }
      } catch {
        case _: NumberFormatException | _: IllegalArgumentException =>
          logger.warn(s"Invalid long value for $key: '$value', using default: $default")
          default
      }
    }
  }

  def getDoubleOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: Double,
    minExclusive: Option[Double] = None,
    maxInclusive: Option[Double] = None
  ): Double = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default
    else {
      try {
        val parsed  = value.toDouble
        val tooLow  = minExclusive.exists(min => parsed <= min)
        val tooHigh = maxInclusive.exists(max => parsed > max)
        if (tooLow || tooHigh) {
          val range = (minExclusive, maxInclusive) match {
            case (Some(min), Some(max)) => s"must be > $min and <= $max"
            case (Some(min), None)      => s"must be > $min"
            case (None, Some(max))      => s"must be <= $max"
            case _                      => "out of range"
          }
          logger.warn(s"Invalid value for $key: '$value' ($range), using default: $default")
          default
        } else {
          parsed
        }
      } catch {
        case _: NumberFormatException =>
          logger.warn(s"Invalid double value for $key: '$value', using default: $default")
          default
      }
    }
  }

  def getStringOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: String,
    validValues: Set[String] = Set.empty
  ): String = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default
    else if (validValues.isEmpty) value
    else {
      val lower = value.toLowerCase
      if (validValues.contains(lower)) lower
      else {
        logger.warn(
          s"Invalid value for $key: '$value' (must be one of ${validValues.mkString(", ")}), using default: $default"
        )
        default
      }
    }
  }

  def parseSizeOption(
    options: CaseInsensitiveStringMap,
    key: String,
    default: Long
  ): Long = {
    val value = options.get(key)
    if (value == null || value.isEmpty) default
    else {
      try
        SizeParser.parseSize(value)
      catch {
        case _: IllegalArgumentException =>
          logger.warn(s"Invalid size value for $key: '$value', using default: ${SizeParser.formatBytes(default)}")
          default
      }
    }
  }

  /** Case-insensitive lookup from a Map[String, String], filtering empty values. */
  def caseInsensitiveGet(configs: Map[String, String], key: String): Option[String] =
    configs
      .get(key)
      .orElse(configs.find(_._1.equalsIgnoreCase(key)).map(_._2))
      .filter(_.nonEmpty)

  /** Case-insensitive lookup from a Map[String, String], preserving empty values. */
  def caseInsensitiveGetRaw(configs: Map[String, String], key: String): Option[String] =
    configs
      .get(key)
      .orElse(configs.find(_._1.equalsIgnoreCase(key)).map(_._2))

  /**
   * Create a case-insensitive lookup function from a Map. Pre-lowercases keys once for efficient repeated lookups.
   * Useful in Config.fromMap() methods that need multiple lookups.
   */
  def caseInsensitiveLookup(configs: Map[String, String]): String => Option[String] = {
    val lowered = configs.map { case (k, v) => k.toLowerCase -> v }
    (key: String) => lowered.get(key.toLowerCase)
  }
}
