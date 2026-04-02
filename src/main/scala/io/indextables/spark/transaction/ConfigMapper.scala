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

package io.indextables.spark.transaction

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Maps Spark/Hadoop configuration keys to the native tantivy4java config map format expected by JNI
 * transaction log methods.
 *
 * This centralizes the credential translation that was previously duplicated across DeltaLogReader,
 * ParquetDirectoryReader, and IcebergSourceReader.
 */
object ConfigMapper {

  /** Direct 1:1 Spark key → native key mappings. */
  private val directMappings: Seq[(String, String)] = Seq(
    // AWS credentials
    "spark.indextables.aws.accessKey"     -> "aws_access_key_id",
    "spark.indextables.aws.secretKey"     -> "aws_secret_access_key",
    "spark.indextables.aws.sessionToken"  -> "aws_session_token",
    "spark.indextables.aws.region"        -> "aws_region",
    "spark.indextables.aws.endpoint"      -> "aws_endpoint",
    "spark.indextables.aws.forcePathStyle" -> "aws_force_path_style",
    // Azure credentials
    "spark.indextables.azure.accountName" -> "azure_account_name",
    "spark.indextables.azure.accountKey"  -> "azure_access_key",
    "spark.indextables.azure.bearerToken" -> "azure_bearer_token",
    // Transaction log cache configuration
    "spark.indextables.transaction.cache.enabled"            -> "cache.enabled",
    "spark.indextables.transaction.cache.ttl.ms"             -> "cache.ttl.ms",
    "spark.indextables.transaction.cache.version.ttl.ms"     -> "cache.version.ttl.ms",
    "spark.indextables.transaction.cache.snapshot.ttl.ms"    -> "cache.snapshot.ttl.ms",
    "spark.indextables.transaction.cache.fileList.ttl.ms"    -> "cache.file_list.ttl.ms",
    "spark.indextables.transaction.cache.metadata.ttl.ms"    -> "cache.metadata.ttl.ms",
    "spark.indextables.transaction.cache.version.capacity"   -> "cache.version.capacity",
    "spark.indextables.transaction.cache.snapshot.capacity"   -> "cache.snapshot.capacity",
    "spark.indextables.transaction.cache.fileList.capacity"  -> "cache.file_list.capacity",
    // Transaction log concurrency
    "spark.indextables.transaction.maxConcurrentReads"       -> "max_concurrent_reads",
    // Checkpoint interval
    "spark.indextables.checkpoint.interval"                  -> "checkpoint_interval"
  )

  /**
   * Translate Spark indextables.* options to native config keys.
   *
   * @param options
   *   Spark CaseInsensitiveStringMap containing spark.indextables.* keys
   * @return
   *   Java Map with native keys (aws_access_key_id, azure_account_name, etc.)
   */
  def toNativeConfig(options: CaseInsensitiveStringMap): java.util.Map[String, String] = {
    // Use CaseInsensitiveStringMap.get() directly for case-insensitive key lookup.
    // Do NOT convert via asCaseSensitiveMap() — it lowercases keys, breaking camelCase lookups.
    val config = new java.util.HashMap[String, String]()

    // Apply direct 1:1 mappings
    for ((sparkKey, nativeKey) <- directMappings) {
      Option(options.get(sparkKey)).foreach(v => config.put(nativeKey, v))
    }

    // S3 endpoint/pathStyle alternate keys (conditional — only set if primary key not already present)
    Option(options.get("spark.indextables.s3.endpoint")).foreach { v =>
      if (!config.containsKey("aws_endpoint")) config.put("aws_endpoint", v)
    }
    Option(options.get("spark.indextables.s3.pathStyleAccess")).foreach { v =>
      if (v.toBoolean) config.put("aws_force_path_style", "true")
    }

    config
  }

  /**
   * Translate Spark indextables.* options to native config keys.
   *
   * @param options
   *   Scala Map containing spark.indextables.* keys
   * @return
   *   Java Map with native keys (aws_access_key_id, azure_account_name, etc.)
   */
  def toNativeConfig(options: Map[String, String]): java.util.Map[String, String] = {
    val config = new java.util.HashMap[String, String]()

    // Apply direct 1:1 mappings
    for ((sparkKey, nativeKey) <- directMappings) {
      options.get(sparkKey).foreach(v => config.put(nativeKey, v))
    }

    // S3 endpoint/pathStyle alternate keys (conditional — only set if primary key not already present)
    options.get("spark.indextables.s3.endpoint").foreach { v =>
      if (!config.containsKey("aws_endpoint")) config.put("aws_endpoint", v)
    }
    options.get("spark.indextables.s3.pathStyleAccess").foreach { v =>
      if (v.toBoolean) config.put("aws_force_path_style", "true")
    }

    config
  }

  /**
   * Normalize a Hadoop Path to a string suitable for native txlog methods.
   *
   * Delegates to ProtocolNormalizer for cloud scheme conversion (s3a→s3, abfss→azure, wasbs→azure),
   * which correctly extracts container names from full Azure URLs
   * (e.g. abfss://container@account.dfs.core.windows.net/path → azure://container/path).
   *
   * Also handles local paths by adding file:// scheme for native object_store.
   */
  def normalizeTablePath(path: org.apache.hadoop.fs.Path): String = {
    val pathStr = path.toString
    if (pathStr.startsWith("file:///")) {
      // Already has file:/// scheme — pass through
      pathStr
    } else if (pathStr.startsWith("file:")) {
      // Normalize file:/path or file://path to file:///path for native object_store
      val stripped = pathStr.stripPrefix("file:").stripPrefix("//").stripPrefix("/")
      "file:///" + stripped
    } else if (pathStr.startsWith("/")) {
      // Local absolute path — add file:// scheme for native object_store
      "file://" + pathStr
    } else {
      io.indextables.spark.util.ProtocolNormalizer.normalizeAllProtocols(pathStr)
    }
  }
}
