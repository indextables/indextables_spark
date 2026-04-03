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
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

import org.slf4j.LoggerFactory

/**
 * Factory for creating transaction log instances. Returns NativeTransactionLog backed by tantivy4java Rust
 * implementation.
 */
object TransactionLogFactory {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create a transaction log instance.
   *
   * Resolves credentials from Hadoop config, Spark config, and DataSource options using the same
   * precedence chain as the read path (ConfigNormalization + CredentialProviderFactory).
   *
   * @param tablePath
   *   The path to the table
   * @param spark
   *   The Spark session (used for credential resolution from Hadoop/Spark config)
   * @param options
   *   Configuration options (highest precedence)
   * @return
   *   A TransactionLogInterface instance backed by native FFI
   */
  def create(
    tablePath: Path,
    spark: SparkSession,
    options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
  ): TransactionLogInterface = {
    logger.debug(s"Creating NativeTransactionLog for $tablePath")

    // Merge configs with same precedence as read path: Hadoop < Spark config < options
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val hadoopConfigs = io.indextables.spark.util.ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    val sparkConfigs = io.indextables.spark.util.ConfigNormalization.extractTantivyConfigsFromSpark(spark)
    val optionConfigs = io.indextables.spark.util.ConfigNormalization.extractTantivyConfigsFromOptions(options)

    val mergedConfigs = io.indextables.spark.util.ConfigNormalization.mergeWithPrecedence(
      hadoopConfigs,
      sparkConfigs,
      optionConfigs
    )

    // Resolve credentials for the native layer. The Rust txlog cannot invoke Java credential
    // providers, so we resolve them in Scala and supply explicit credentials to the native layer.
    // resolveCredentialsOnDriver invokes the custom provider (if configured), strips the provider
    // class key, and injects explicit credentials. We re-add the provider class afterward so
    // CloudStorageProvider and executors can still invoke it for fresh credential refresh.
    val providerClassKey = "spark.indextables.aws.credentialsProviderClass"
    val providerClassOpt = mergedConfigs.get(providerClassKey)
      .orElse(mergedConfigs.get(providerClassKey.toLowerCase))

    // Strip source-table-specific keys before resolving credentials for this txlog's storage path.
    // spark.indextables.iceberg.uc.tableId is injected by SyncToExternalCommand into mergedConfigs
    // to identify the SOURCE table in Unity Catalog. If left in place when creating the destination
    // txlog, CredentialProviderFactory.resolveCredentialsOnDriver hits Priority 1.5 and calls
    // getTableCredentials(sourceTableId) — returning credentials scoped to the source table's S3
    // location, not the txlog destination. Removing it falls through to path-based resolution
    // (Priority 2), which correctly fetches credentials for tablePath.
    val configsForDestAuth = mergedConfigs - "spark.indextables.iceberg.uc.tableId"

    val resolvedConfigs = io.indextables.spark.utils.CredentialProviderFactory
      .resolveCredentialsOnDriver(configsForDestAuth, tablePath.toString)

    // Re-add provider class if it was stripped during resolution
    val finalConfigs = providerClassOpt match {
      case Some(cls) if cls.nonEmpty && !resolvedConfigs.contains(providerClassKey) =>
        resolvedConfigs + (providerClassKey -> cls)
      case _ => resolvedConfigs
    }

    val resolvedOptions = new CaseInsensitiveStringMap(finalConfigs.asJava)
    new NativeTransactionLog(tablePath, resolvedOptions)
  }

  /**
   * Create a native transaction log instance directly.
   *
   * Convenience method that bypasses SparkSession parameter. Useful for tests.
   * Credentials must already be present in the options map.
   */
  def createNative(
    tablePath: Path,
    options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
  ): NativeTransactionLog =
    new NativeTransactionLog(tablePath, options)
}
