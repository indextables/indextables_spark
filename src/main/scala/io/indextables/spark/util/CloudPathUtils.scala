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
 * Shared utilities for parsing S3 and Azure cloud storage paths.
 *
 * Handles scheme normalization (s3a:// -> s3://, wasb/abfs -> azure://) and extracts bucket/container + key/path
 * components.
 */
object CloudPathUtils {

  /**
   * Parse an S3 path into (bucket, key).
   *
   * Handles both `s3://` and `s3a://` schemes.
   *
   * @param path
   *   S3 path (e.g., "s3://bucket/key" or "s3a://bucket/key")
   * @return
   *   Tuple of (bucket, key)
   */
  def parseS3Path(path: String): (String, String) = {
    val normalizedPath = path.replaceFirst("^s3a://", "s3://")
    val uri            = new URI(normalizedPath)
    val bucket         = uri.getHost
    val key            = uri.getPath.stripPrefix("/")
    (bucket, key)
  }

  /**
   * Parse an Azure storage path into (container, blobPath).
   *
   * Supports azure://, wasb://, wasbs://, abfs://, and abfss:// schemes. Handles Hadoop-style
   * container@account.host URLs.
   *
   * @param path
   *   Azure path (e.g., "wasbs://container@account.blob.core.windows.net/path")
   * @return
   *   Tuple of (container, blobPath)
   */
  def parseAzurePath(path: String): (String, String) = {
    val uri    = new URI(path)
    val scheme = uri.getScheme

    require(
      scheme == "azure" || scheme == "wasb" || scheme == "wasbs" || scheme == "abfs" || scheme == "abfss",
      s"Invalid Azure URI scheme: $scheme (expected: azure, wasb, wasbs, abfs, or abfss)"
    )

    scheme match {
      case "azure" =>
        val container = uri.getHost
        require(container != null && container.nonEmpty, s"Invalid Azure URI - missing container: $path")
        val blobPath = uri.getPath.stripPrefix("/")
        (container, blobPath)

      case "wasb" | "wasbs" | "abfs" | "abfss" =>
        // wasb://container@account.blob.core.windows.net/path
        // abfs://container@account.dfs.core.windows.net/path
        val authority = uri.getAuthority
        require(
          authority != null && authority.contains("@"),
          s"Invalid $scheme URI - expected format: $scheme://container@account.blob.core.windows.net/path"
        )
        val container = authority.split("@")(0)
        require(container.nonEmpty, s"Invalid $scheme URI - missing container: $path")
        val blobPath = uri.getPath.stripPrefix("/")
        (container, blobPath)
    }
  }
}
