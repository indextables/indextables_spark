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

import com.tantivy4spark.storage.SplitCacheConfig
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory

/**
 * Utility functions for configuration management.
 */
object ConfigUtils {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Create a SplitCacheConfig from broadcast configuration.
   *
   * This utility consolidates the duplicated cache configuration logic
   * across partition readers, aggregate readers, and other components.
   *
   * @param broadcastConfig Broadcast variable containing configuration map
   * @param tablePathOpt Optional table path for generating unique cache names
   * @return Configured SplitCacheConfig instance
   */
  def createSplitCacheConfigFromBroadcast(
    broadcastConfig: Broadcast[Map[String, String]],
    tablePathOpt: Option[String] = None
  ): SplitCacheConfig = {

    val broadcasted = broadcastConfig.value

    // Helper function to get config from broadcast with defaults
    def getBroadcastConfig(configKey: String, default: String = ""): String = {
      val value = broadcasted.getOrElse(configKey, default)
      Option(value).getOrElse(default)
    }

    def getBroadcastConfigOption(configKey: String): Option[String] = {
      // Try both the original key and lowercase version (CaseInsensitiveStringMap lowercases keys)
      broadcasted.get(configKey).orElse(broadcasted.get(configKey.toLowerCase))
    }

    SplitCacheConfig(
      cacheName = {
        val configName = getBroadcastConfig("spark.tantivy4spark.cache.name", "")
        if (configName.trim().nonEmpty) {
          configName.trim()
        } else {
          // Use table path as cache name for table-specific caching
          tablePathOpt match {
            case Some(tablePath) =>
              s"tantivy4spark-${tablePath.replaceAll("[^a-zA-Z0-9]", "_")}"
            case None =>
              s"tantivy4spark-default-${System.currentTimeMillis()}"
          }
        }
      },
      maxCacheSize = {
        val value = getBroadcastConfig("spark.tantivy4spark.cache.maxSize", "200000000")
        try {
          value.toLong
        } catch {
          case e: NumberFormatException =>
            logger.error(s"Invalid numeric value for spark.tantivy4spark.cache.maxSize: '$value'")
            throw e
        }
      },
      maxConcurrentLoads = {
        val value = getBroadcastConfig("spark.tantivy4spark.cache.maxConcurrentLoads", "8")
        try {
          value.toInt
        } catch {
          case e: NumberFormatException =>
            logger.error(s"Invalid numeric value for spark.tantivy4spark.cache.maxConcurrentLoads: '$value'")
            throw e
        }
      },
      enableQueryCache = getBroadcastConfig("spark.tantivy4spark.cache.queryCache", "true").toBoolean,
      splitCachePath = getBroadcastConfigOption("spark.tantivy4spark.cache.directoryPath")
        .orElse(SplitCacheConfig.getDefaultCachePath()),
      // AWS configuration from broadcast
      awsAccessKey = getBroadcastConfigOption("spark.tantivy4spark.aws.accessKey"),
      awsSecretKey = getBroadcastConfigOption("spark.tantivy4spark.aws.secretKey"),
      awsSessionToken = getBroadcastConfigOption("spark.tantivy4spark.aws.sessionToken"),
      awsRegion = getBroadcastConfigOption("spark.tantivy4spark.aws.region"),
      awsEndpoint = getBroadcastConfigOption("spark.tantivy4spark.s3.endpoint"),
      awsPathStyleAccess = getBroadcastConfigOption("spark.tantivy4spark.s3.pathStyleAccess").map(_.toBoolean),
      // Azure configuration from broadcast
      azureAccountName = getBroadcastConfigOption("spark.tantivy4spark.azure.accountName"),
      azureAccountKey = getBroadcastConfigOption("spark.tantivy4spark.azure.accountKey"),
      azureConnectionString = getBroadcastConfigOption("spark.tantivy4spark.azure.connectionString"),
      azureEndpoint = getBroadcastConfigOption("spark.tantivy4spark.azure.endpoint"),
      // GCP configuration from broadcast
      gcpProjectId = getBroadcastConfigOption("spark.tantivy4spark.gcp.projectId"),
      gcpServiceAccountKey = getBroadcastConfigOption("spark.tantivy4spark.gcp.serviceAccountKey"),
      gcpCredentialsFile = getBroadcastConfigOption("spark.tantivy4spark.gcp.credentialsFile"),
      gcpEndpoint = getBroadcastConfigOption("spark.tantivy4spark.gcp.endpoint")
    )
  }
}