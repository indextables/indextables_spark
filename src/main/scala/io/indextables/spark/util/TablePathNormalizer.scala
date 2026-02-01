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

import java.net.URI

/**
 * Utility for normalizing paths to their table root level.
 *
 * This is critical for credential providers (like UnityCatalogAWSCredentialProvider) to ensure consistent cache keys.
 * Without normalization, different splits from the same table would result in different cache keys, causing redundant
 * HTTP calls.
 *
 * Examples:
 *   - s3://bucket/table/part-00000.split -> s3://bucket/table
 *   - s3://bucket/table/year=2024/month=01/part-00000.split -> s3://bucket/table
 *   - s3://bucket/table/year=2024/month=01 -> s3://bucket/table
 *   - s3://bucket/table/_transaction_log/00000.json -> s3://bucket/table
 */
object TablePathNormalizer {

  // Known data file extensions that should be stripped
  private val DataFileExtensions: Seq[String] = Seq(".split", ".avro", ".json", ".gz")

  /**
   * Normalize a path to its table root by stripping file names and partition paths.
   *
   * This ensures that credential providers are always created with the table root path, not individual split file paths
   * or partition subdirectories, which is essential for cache consistency.
   *
   * Normalization rules (applied in order):
   *   1. Strip transaction log paths (_transaction_log/...) 2. If path ends with a data file (.split, .avro, .json,
   *      .gz), truncate to the last '/' 3. Strip all trailing Hive-style partition paths (/key=value/)
   *
   * @param path
   *   The path to normalize (can be full URI like s3://bucket/path or relative path)
   * @return
   *   Normalized path at table root level
   */
  def normalizeToTablePath(path: String): String = {
    if (path == null || path.isEmpty) return path

    try {
      val uri      = new URI(path)
      var pathPart = uri.getPath

      if (pathPart == null || pathPart.isEmpty) {
        return path
      }

      // Step 0: Strip _transaction_log directory and everything after it
      val txLogIndex = pathPart.indexOf("/_transaction_log")
      if (txLogIndex > 0) {
        pathPart = pathPart.substring(0, txLogIndex)
      }

      // Step 1: If the path ends with a data file extension, truncate to the last '/'
      if (DataFileExtensions.exists(ext => pathPart.endsWith(ext))) {
        val lastSlash = pathPart.lastIndexOf('/')
        if (lastSlash > 0) {
          pathPart = pathPart.substring(0, lastSlash)
        }
      }

      // Step 2: Strip all trailing Hive-style partition paths (/key=value)
      // Keep stripping while the last path segment contains '=' (Hive partition format)
      var modified = true
      while (modified && pathPart.length > 1) {
        val lastSlash = pathPart.lastIndexOf('/')
        if (lastSlash > 0) {
          val lastSegment = pathPart.substring(lastSlash + 1)
          // Check if this segment is a Hive-style partition (contains '=' but is not empty and doesn't start with '=')
          if (lastSegment.nonEmpty && lastSegment.contains('=') && !lastSegment.startsWith("=")) {
            pathPart = pathPart.substring(0, lastSlash)
          } else {
            modified = false
          }
        } else {
          modified = false
        }
      }

      // Preserve the original scheme, authority, query, and fragment
      new URI(uri.getScheme, uri.getAuthority, pathPart, uri.getQuery, uri.getFragment).toString
    } catch {
      case _: Exception =>
        // If parsing fails, return the original path
        path
    }
  }

  /**
   * Check if a path segment is a Hive-style partition (key=value format).
   *
   * @param segment
   *   Path segment to check
   * @return
   *   true if the segment matches Hive partition format
   */
  def isHivePartitionSegment(segment: String): Boolean =
    segment.nonEmpty && segment.contains('=') && !segment.startsWith("=")
}
