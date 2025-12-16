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

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import io.indextables.spark.sql.{SerializableAwsConfig, SerializableAzureConfig}

/**
 * Utility for extracting cloud configuration (AWS/Azure) from SparkSession.
 *
 * This centralizes the credential extraction logic using ConfigNormalization
 * and produces SerializableAwsConfig/SerializableAzureConfig that can be
 * broadcast to executors.
 *
 * Uses the configuration hierarchy: options > sparkConf > hadoopConf
 */
object CloudConfigExtractor {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Extract AWS configuration from SparkSession.
   *
   * Uses ConfigNormalization to properly merge configs from SparkSession and Hadoop
   * with correct precedence.
   *
   * @param sparkSession The Spark session
   * @param overrideOptions Optional override options (highest precedence)
   * @return SerializableAwsConfig that can be broadcast to executors
   */
  def extractAwsConfig(
    sparkSession: SparkSession,
    overrideOptions: Option[Map[String, String]] = None
  ): SerializableAwsConfig =
    try {
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

      // Extract and normalize all indextables configs from both Spark and Hadoop
      val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
      val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
      val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

      // Helper function to get config with priority: overrideOptions > mergedConfigs
      // Case-insensitive lookup to handle write options (lowercase) vs expected keys (camelCase)
      def getConfigWithFallback(sparkKey: String): Option[String] = {
        val overrideValue = overrideOptions.flatMap { opts =>
          opts.get(sparkKey).orElse {
            opts.find { case (k, _) => k.equalsIgnoreCase(sparkKey) }.map(_._2)
          }
        }
        overrideValue.orElse(mergedConfigs.get(sparkKey))
      }

      val accessKey = getConfigWithFallback("spark.indextables.aws.accessKey")
      val secretKey = getConfigWithFallback("spark.indextables.aws.secretKey")
      val sessionToken = getConfigWithFallback("spark.indextables.aws.sessionToken")
      val region = getConfigWithFallback("spark.indextables.aws.region")
      val endpoint = getConfigWithFallback("spark.indextables.s3.endpoint")
      val pathStyleAccess = getConfigWithFallback("spark.indextables.s3.pathStyleAccess")
        .map(_.toLowerCase == "true")
        .getOrElse(false)

      // Extract temporary directory configuration with fallback chain
      val tempDirectoryPath = getConfigWithFallback("spark.indextables.merge.tempDirectoryPath")
        .orElse(getConfigWithFallback("spark.indextables.indexWriter.tempDirectoryPath"))
        .orElse(if (isLocalDisk0Available()) Some("/local_disk0/tmp") else None)

      // Extract custom credential provider class name
      val credentialsProviderClass = getConfigWithFallback("spark.indextables.aws.credentialsProviderClass")

      // Extract heap size for operations
      val heapSize = getConfigWithFallback("spark.indextables.merge.heapSize")
        .orElse(getConfigWithFallback("spark.indextables.indexWriter.heapSize"))
        .map(SizeParser.parseSize)
        .map(java.lang.Long.valueOf)
        .getOrElse(java.lang.Long.valueOf(1073741824L)) // 1GB default

      val debugEnabled = getConfigWithFallback("spark.indextables.merge.debug")
        .exists(v => v.equalsIgnoreCase("true") || v == "1")

      logger.debug(s"Extracted AWS config: region=${region.getOrElse("None")}, " +
        s"endpoint=${endpoint.getOrElse("None")}, pathStyle=$pathStyleAccess")
      logger.debug(s"AWS credentials: accessKey=${accessKey.map(k => s"${k.take(4)}***").getOrElse("None")}, " +
        s"sessionToken=${sessionToken.map(_ => "***").getOrElse("None")}")

      SerializableAwsConfig(
        accessKey.getOrElse(""),
        secretKey.getOrElse(""),
        sessionToken,
        region.getOrElse("us-east-1"),
        endpoint,
        pathStyleAccess,
        tempDirectoryPath,
        credentialsProviderClass,
        heapSize,
        debugEnabled
      )
    } catch {
      case ex: Exception =>
        logger.error("Failed to extract AWS config from Spark session", ex)
        throw new RuntimeException("Failed to extract AWS config", ex)
    }

  /**
   * Extract Azure configuration from SparkSession.
   *
   * @param sparkSession The Spark session
   * @param overrideOptions Optional override options (highest precedence)
   * @return SerializableAzureConfig that can be broadcast to executors
   */
  def extractAzureConfig(
    sparkSession: SparkSession,
    overrideOptions: Option[Map[String, String]] = None
  ): SerializableAzureConfig =
    try {
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

      // Extract and normalize all indextables configs from both Spark and Hadoop
      val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
      val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
      val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

      // Helper function to get config with priority: overrideOptions > mergedConfigs
      def getConfigWithFallback(sparkKey: String): Option[String] = {
        val overrideValue = overrideOptions.flatMap { opts =>
          opts.get(sparkKey).orElse {
            opts.find { case (k, _) => k.equalsIgnoreCase(sparkKey) }.map(_._2)
          }
        }
        overrideValue.orElse(mergedConfigs.get(sparkKey))
      }

      val accountName = getConfigWithFallback("spark.indextables.azure.accountName")
      val accountKey = getConfigWithFallback("spark.indextables.azure.accountKey")
      val connectionString = getConfigWithFallback("spark.indextables.azure.connectionString")
      val endpoint = getConfigWithFallback("spark.indextables.azure.endpoint")
      val bearerToken = getConfigWithFallback("spark.indextables.azure.bearerToken")
      val tenantId = getConfigWithFallback("spark.indextables.azure.tenantId")
      val clientId = getConfigWithFallback("spark.indextables.azure.clientId")
      val clientSecret = getConfigWithFallback("spark.indextables.azure.clientSecret")

      logger.debug(s"Extracted Azure config: accountName=${accountName.getOrElse("None")}, " +
        s"endpoint=${endpoint.getOrElse("None")}")
      logger.debug(s"Azure credentials: accountKey=${accountKey.map(_ => "***").getOrElse("None")}, " +
        s"connectionString=${connectionString.map(_ => "***").getOrElse("None")}")

      SerializableAzureConfig(
        accountName,
        accountKey,
        connectionString,
        endpoint,
        bearerToken,
        tenantId,
        clientId,
        clientSecret
      )
    } catch {
      case ex: Exception =>
        logger.error("Failed to extract Azure config from Spark session", ex)
        throw new RuntimeException("Failed to extract Azure config", ex)
    }

  /**
   * Check if /local_disk0 is available (Databricks/EMR).
   */
  private def isLocalDisk0Available(): Boolean = {
    val localDisk0 = new java.io.File("/local_disk0/tmp")
    localDisk0.exists() || localDisk0.mkdirs()
  }

  /**
   * Determine if a path is an S3 path.
   */
  def isS3Path(path: String): Boolean =
    path.toLowerCase.startsWith("s3://") || path.toLowerCase.startsWith("s3a://")

  /**
   * Determine if a path is an Azure path.
   */
  def isAzurePath(path: String): Boolean =
    path.toLowerCase.startsWith("abfss://") ||
      path.toLowerCase.startsWith("azure://") ||
      path.toLowerCase.startsWith("wasbs://") ||
      path.toLowerCase.startsWith("abfs://")

  /**
   * Determine if a path is a cloud path (S3 or Azure).
   */
  def isCloudPath(path: String): Boolean = isS3Path(path) || isAzurePath(path)
}
