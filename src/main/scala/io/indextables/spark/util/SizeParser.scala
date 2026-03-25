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

import scala.util.{Failure, Success, Try}

import org.slf4j.LoggerFactory

/**
 * Utility for parsing size strings in various formats (bytes, MB, GB).
 *
 * Supported formats:
 *   - Pure numbers: "123456" -> 123456 bytes
 *   - Megabytes: "1M", "100M" -> bytes
 *   - Gigabytes: "1G", "2G" -> bytes
 */
object SizeParser {

  private val logger = LoggerFactory.getLogger(SizeParser.getClass)

  // Size multipliers
  private val KB = 1024L
  private val MB = KB * 1024
  private val GB = MB * 1024
  private val TB = GB * 1024

  /**
   * Parse a size string into bytes.
   *
   * @param sizeStr
   *   Size string (e.g., "100M", "1G", "123456")
   * @return
   *   Size in bytes
   * @throws IllegalArgumentException
   *   if the format is invalid
   */
  def parseSize(sizeStr: String): Long = {
    if (sizeStr == null || sizeStr.trim.isEmpty) {
      throw new IllegalArgumentException("Size string cannot be null or empty")
    }

    val trimmed = sizeStr.trim.toUpperCase

    Try {
      val (numberPart, multiplier) = if (trimmed.endsWith("T")) {
        (trimmed.dropRight(1), TB)
      } else if (trimmed.endsWith("G")) {
        (trimmed.dropRight(1), GB)
      } else if (trimmed.endsWith("M")) {
        (trimmed.dropRight(1), MB)
      } else if (trimmed.endsWith("K")) {
        (trimmed.dropRight(1), KB)
      } else {
        (trimmed, 1L)
      }

      if (multiplier > 1) {
        // Unit suffix present - use BigDecimal to support fractional values (e.g., "1.5G")
        val value = BigDecimal(numberPart)
        if (value <= 0) {
          throw new IllegalArgumentException(s"Size value must be positive when using units: $sizeStr")
        }
        (value * multiplier).toLong
      } else {
        // Pure number - interpret as bytes
        val value = trimmed.toLong
        if (value < 0) {
          throw new IllegalArgumentException(s"Size value must be non-negative: $sizeStr")
        }
        value
      }
    } match {
      case Success(size) =>
        logger.debug(s"Parsed size '$sizeStr' to $size bytes")
        size
      case Failure(ex) =>
        throw new IllegalArgumentException(
          s"Invalid size format: '$sizeStr'. Supported formats: '123456' (bytes), '1K', '100M', '1G', '1T'",
          ex
        )
    }
  }

  /**
   * Format bytes into a human-readable string.
   *
   * @param bytes
   *   Size in bytes
   * @return
   *   Human-readable size string
   */
  def formatBytes(bytes: Long): String =
    if (bytes >= TB && bytes % TB == 0) {
      s"${bytes / TB}T"
    } else if (bytes >= GB && bytes % GB == 0) {
      s"${bytes / GB}G"
    } else if (bytes >= MB && bytes % MB == 0) {
      s"${bytes / MB}M"
    } else if (bytes >= KB && bytes % KB == 0) {
      s"${bytes / KB}K"
    } else {
      s"${bytes}B"
    }

  /**
   * Validate that a size string is in a supported format.
   *
   * @param sizeStr
   *   Size string to validate
   * @return
   *   true if valid, false otherwise
   */
  def isValidSizeFormat(sizeStr: String): Boolean =
    Try(parseSize(sizeStr)).isSuccess

  /**
   * Format bytes into a human-readable string with decimal precision.
   *
   * Suitable for log messages and user-facing output (e.g., "1.50 GB", "256.00 MB").
   */
  def formatBytesHuman(bytes: Long): String =
    if (bytes >= GB) {
      f"${bytes.toDouble / GB}%.2f GB"
    } else if (bytes >= MB) {
      f"${bytes.toDouble / MB}%.2f MB"
    } else if (bytes >= KB) {
      f"${bytes.toDouble / KB}%.2f KB"
    } else {
      s"$bytes bytes"
    }
}
