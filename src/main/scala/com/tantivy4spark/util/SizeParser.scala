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

package com.tantivy4spark.util

import org.slf4j.LoggerFactory
import scala.util.{Try, Success, Failure}

/**
 * Utility for parsing size strings in various formats (bytes, MB, GB).
 *
 * Supported formats:
 * - Pure numbers: "123456" -> 123456 bytes
 * - Megabytes: "1M", "100M" -> bytes
 * - Gigabytes: "1G", "2G" -> bytes
 */
object SizeParser {

  private val logger = LoggerFactory.getLogger(SizeParser.getClass)

  // Size multipliers
  private val KB = 1024L
  private val MB = KB * 1024
  private val GB = MB * 1024

  /**
   * Parse a size string into bytes.
   *
   * @param sizeStr Size string (e.g., "100M", "1G", "123456")
   * @return Size in bytes
   * @throws IllegalArgumentException if the format is invalid
   */
  def parseSize(sizeStr: String): Long = {
    if (sizeStr == null || sizeStr.trim.isEmpty) {
      throw new IllegalArgumentException("Size string cannot be null or empty")
    }

    val trimmed = sizeStr.trim.toUpperCase

    Try {
      // Check for unit suffixes
      if (trimmed.endsWith("G")) {
        val numberPart = trimmed.dropRight(1)
        val value = numberPart.toLong
        if (value <= 0) {
          throw new IllegalArgumentException(s"Size value must be positive: $sizeStr")
        }
        value * GB
      } else if (trimmed.endsWith("M")) {
        val numberPart = trimmed.dropRight(1)
        val value = numberPart.toLong
        if (value <= 0) {
          throw new IllegalArgumentException(s"Size value must be positive: $sizeStr")
        }
        value * MB
      } else if (trimmed.endsWith("K")) {
        val numberPart = trimmed.dropRight(1)
        val value = numberPart.toLong
        if (value <= 0) {
          throw new IllegalArgumentException(s"Size value must be positive: $sizeStr")
        }
        value * KB
      } else {
        // Pure number - interpret as bytes
        val value = trimmed.toLong
        if (value <= 0) {
          throw new IllegalArgumentException(s"Size value must be positive: $sizeStr")
        }
        value
      }
    } match {
      case Success(size) =>
        logger.debug(s"Parsed size '$sizeStr' to $size bytes")
        size
      case Failure(ex) =>
        throw new IllegalArgumentException(s"Invalid size format: '$sizeStr'. Supported formats: '123456' (bytes), '1M' (megabytes), '1G' (gigabytes)", ex)
    }
  }

  /**
   * Format bytes into a human-readable string.
   *
   * @param bytes Size in bytes
   * @return Human-readable size string
   */
  def formatBytes(bytes: Long): String = {
    if (bytes >= GB && bytes % GB == 0) {
      s"${bytes / GB}G"
    } else if (bytes >= MB && bytes % MB == 0) {
      s"${bytes / MB}M"
    } else if (bytes >= KB && bytes % KB == 0) {
      s"${bytes / KB}K"
    } else {
      s"$bytes bytes"
    }
  }

  /**
   * Validate that a size string is in a supported format.
   *
   * @param sizeStr Size string to validate
   * @return true if valid, false otherwise
   */
  def isValidSizeFormat(sizeStr: String): Boolean = {
    Try(parseSize(sizeStr)).isSuccess
  }
}