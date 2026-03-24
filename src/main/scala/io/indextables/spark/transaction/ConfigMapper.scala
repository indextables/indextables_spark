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

  /**
   * Translate Spark indextables.* options to native config keys.
   *
   * @param options
   *   Spark CaseInsensitiveStringMap containing spark.indextables.* keys
   * @return
   *   Java Map with native keys (aws_access_key_id, azure_account_name, etc.)
   */
  def toNativeConfig(options: CaseInsensitiveStringMap): java.util.Map[String, String] = {
    val scalaMap = options.asCaseSensitiveMap().asScala.toMap
    toNativeConfig(scalaMap)
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

    // AWS credentials
    options.get("spark.indextables.aws.accessKey").foreach(v => config.put("aws_access_key_id", v))
    options.get("spark.indextables.aws.secretKey").foreach(v => config.put("aws_secret_access_key", v))
    options.get("spark.indextables.aws.sessionToken").foreach(v => config.put("aws_session_token", v))
    options.get("spark.indextables.aws.region").foreach(v => config.put("aws_region", v))
    options.get("spark.indextables.aws.endpoint").foreach(v => config.put("aws_endpoint", v))
    options.get("spark.indextables.aws.forcePathStyle").foreach(v => config.put("aws_force_path_style", v))

    // S3 endpoint/pathStyle alternate keys
    options.get("spark.indextables.s3.endpoint").foreach { v =>
      if (!config.containsKey("aws_endpoint")) config.put("aws_endpoint", v)
    }
    options.get("spark.indextables.s3.pathStyleAccess").foreach { v =>
      if (v.toBoolean) config.put("aws_force_path_style", "true")
    }

    // Azure credentials
    options.get("spark.indextables.azure.accountName").foreach(v => config.put("azure_account_name", v))
    options.get("spark.indextables.azure.accountKey").foreach(v => config.put("azure_access_key", v))
    options.get("spark.indextables.azure.bearerToken").foreach(v => config.put("azure_bearer_token", v))

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
