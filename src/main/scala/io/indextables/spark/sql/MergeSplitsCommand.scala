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

package io.indextables.spark.sql

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.{AddAction, RemoveAction, TransactionLog, TransactionLogFactory}
import io.indextables.spark.util.{ConfigNormalization, ConfigUtils}
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import org.slf4j.LoggerFactory

/**
 * SQL command to merge small split files into larger ones. Modeled after Delta Lake's OPTIMIZE command structure and
 * behavior.
 *
 * Syntax: MERGE SPLITS ('/path/to/table' | table_name) [WHERE partition_predicates] [TARGET SIZE target_size] [MAX
 * GROUPS max_groups] [PRECOMMIT]
 *
 * Examples:
 *   - MERGE SPLITS '/path/to/table'
 *   - MERGE SPLITS my_table WHERE partition_col = 'value'
 *   - MERGE SPLITS my_table TARGET SIZE 5368709120 -- 5GB in bytes
 *   - MERGE SPLITS '/path/to/table' WHERE year = 2023 TARGET SIZE 2147483648 -- 2GB
 *   - MERGE SPLITS my_table MAX GROUPS 5 -- Limit to 5 oldest merge groups
 *   - MERGE SPLITS '/path/to/table' TARGET SIZE 1G MAX GROUPS 3 -- 1GB target, max 3 groups
 *   - MERGE SPLITS events PRECOMMIT -- Pre-commit merge (framework complete, core implementation pending)
 *
 * This command:
 *   1. Merges only within partitions (follows Delta Lake OPTIMIZE pattern) 2. Selects mergeable splits in transaction
 *      log order 3. Concatenates splits up to configurable target size (default 5GB) 4. Assumes merged split size
 *      equals sum of input splits 5. Does not merge splits already at target size 6. Uses atomic REMOVE+ADD operations
 *      in transaction log 7. Ensures queries after merge only read merged splits 8. MAX GROUPS option: Limits merge
 *      operation to N oldest destination merge groups 9. PRECOMMIT option: Merges splits during write process before
 *      transaction log commit (eliminates small file problems at source - framework complete, core logic pending)
 */
abstract class MergeSplitsCommandBase extends RunnableCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("table_path", StringType)(),
    AttributeReference(
      "metrics",
      StructType(
        Seq(
          StructField("status", StringType, nullable = false),
          StructField("merged_files", LongType, nullable = true),
          StructField("merge_groups", LongType, nullable = true),
          StructField("original_size_bytes", LongType, nullable = true),
          StructField("merged_size_bytes", LongType, nullable = true),
          StructField("message", StringType, nullable = true)
        )
      )
    )(),
    AttributeReference("temp_directory_path", StringType)(),
    AttributeReference("heap_size_bytes", LongType)()
  )

  /** Default target size for merged splits (5GB in bytes) */
  val DEFAULT_TARGET_SIZE: Long = 5L * 1024L * 1024L * 1024L // 5GB

  protected def validateTargetSize(targetSize: Long): Unit = {
    if (targetSize <= 0) {
      throw new IllegalArgumentException(s"Target size must be positive, got: $targetSize")
    }
    if (targetSize < 1024 * 1024) { // 1MB minimum
      throw new IllegalArgumentException(s"Target size must be at least 1MB, got: $targetSize")
    }
  }
}

/** MERGE SPLITS command implementation for Spark SQL. */
case class MergeSplitsCommand(
  override val child: LogicalPlan,
  userPartitionPredicates: Seq[String],
  targetSize: Option[Long],
  maxDestSplits: Option[Int],
  maxSourceSplitsPerMerge: Option[Int],
  preCommitMerge: Boolean = false)
    extends MergeSplitsCommandBase
    with UnaryNode {

  private val logger = LoggerFactory.getLogger(classOf[MergeSplitsCommand])

  override protected def withNewChildInternal(newChild: LogicalPlan): MergeSplitsCommand =
    copy(child = newChild)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Validate target size first (for all cases)
    val actualTargetSize = targetSize.getOrElse(DEFAULT_TARGET_SIZE)
    validateTargetSize(actualTargetSize)

    // Handle pre-commit merge early (before any table path resolution)
    if (preCommitMerge) {
      logger.info("PRE-COMMIT MERGE: Executing pre-commit merge functionality")
      return Seq(
        Row(
          "PRE-COMMIT MERGE",
          Row("pending", null, null, null, null, "Functionality pending implementation"),
          null,
          null
        )
      )
    }

    // Resolve table path from child logical plan
    val tablePath =
      try
        resolveTablePath(child, sparkSession)
      catch {
        case e: IllegalArgumentException =>
          // Handle non-existent table gracefully
          val pathStr = child match {
            case resolved: UnresolvedDeltaPathOrIdentifier =>
              resolved.path.getOrElse(resolved.tableIdentifier.map(_.toString).getOrElse("unknown"))
            case _ => "unknown"
          }
          logger.info(s"Table or path not found: $pathStr")
          return Seq(Row(pathStr, Row("error", null, null, null, null, "Table or path does not exist"), null, null))
      }

    // Create transaction log
    val transactionLog =
      TransactionLogFactory.create(tablePath, sparkSession, new CaseInsensitiveStringMap(java.util.Collections.emptyMap()))

    try {
      // Check if transaction log is initialized
      val hasMetadata =
        try {
          transactionLog.getMetadata()
          true
        } catch {
          case _: Exception => false
        }

      if (!hasMetadata) {
        logger.info(s"No transaction log found at: $tablePath")
        return Seq(
          Row(
            tablePath.toString,
            Row("error", null, null, null, null, "Not a valid IndexTables4Spark table"),
            null,
            null
          )
        )
      }

      // Validate table has files
      val files = transactionLog.listFiles()
      if (files.isEmpty) {
        logger.info(s"No files found in table: $tablePath")
        return Seq(Row(tablePath.toString, Row("no_action", null, null, null, null, "Table is empty"), null, null))
      }

      new MergeSplitsExecutor(
        sparkSession,
        transactionLog,
        tablePath,
        userPartitionPredicates,
        actualTargetSize,
        maxDestSplits,
        maxSourceSplitsPerMerge,
        preCommitMerge
      ).merge()
    } finally
      transactionLog.close()
  }

  /** Resolve table path from logical plan child. */
  private def resolveTablePath(child: LogicalPlan, sparkSession: SparkSession): Path =
    child match {
      case resolved: UnresolvedDeltaPathOrIdentifier =>
        resolved.path match {
          case Some(pathStr) => new Path(pathStr)
          case None =>
            resolved.tableIdentifier match {
              case Some(tableId) =>
                // Try to resolve table identifier to path
                val catalog = sparkSession.sessionState.catalog
                if (catalog.tableExists(tableId)) {
                  val tableMetadata = catalog.getTableMetadata(tableId)
                  new Path(tableMetadata.location)
                } else {
                  throw new IllegalArgumentException(s"Table not found: $tableId")
                }
              case None =>
                throw new IllegalArgumentException("Either path or table identifier must be specified")
            }
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported child plan type: ${child.getClass.getSimpleName}")
    }
}

object MergeSplitsCommand {

  /**
   * Retry a merge operation up to 3 times if it fails with a RuntimeException containing "streaming error" or "timed
   * out". After 3 total attempts (1 initial + 2 retries), the exception is propagated.
   */
  def retryOnStreamingError[T](
    operation: () => T,
    operationDesc: String,
    logger: org.slf4j.Logger
  ): T = {
    val maxAttempts                     = 3
    var attempt                         = 1
    var lastException: RuntimeException = null

    while (attempt <= maxAttempts)
      try {
        if (attempt > 1) {
          logger.warn(s"Retrying $operationDesc (attempt $attempt of $maxAttempts)")
        }
        return operation()
      } catch {
        case e: RuntimeException
            if e.getMessage != null &&
              (e.getMessage.contains("streaming error") || e.getMessage.contains("timed out")) =>
          lastException = e
          logger.warn(s"Caught retryable error on attempt $attempt of $maxAttempts for $operationDesc: ${e.getMessage}")
          if (attempt == maxAttempts) {
            logger.error(s"All $maxAttempts attempts failed for $operationDesc, propagating exception")
            throw e
          }
          attempt += 1
        case e: Exception =>
          // Non-retryable errors are propagated immediately
          throw e
      }

    // This should never be reached, but for completeness
    throw lastException
  }

  /**
   * Alternate constructor that converts a provided path or table identifier into the correct child LogicalPlan node.
   */
  def apply(
    path: Option[String],
    tableIdentifier: Option[org.apache.spark.sql.catalyst.TableIdentifier],
    userPartitionPredicates: Seq[String],
    targetSize: Option[Long],
    maxDestSplits: Option[Int],
    maxSourceSplitsPerMerge: Option[Int],
    preCommitMerge: Boolean
  ): MergeSplitsCommand = {
    val plan = UnresolvedDeltaPathOrIdentifier(path, tableIdentifier, "MERGE SPLITS")
    MergeSplitsCommand(plan, userPartitionPredicates, targetSize, maxDestSplits, maxSourceSplitsPerMerge, preCommitMerge)
  }

  /**
   * Detect if /local_disk0 is available and writable for high-performance local storage. Follows the same pattern as
   * SplitManager.scala for consistency.
   *
   * @return
   *   true if /local_disk0 exists, is a directory, and is writable; false otherwise
   */
  def isLocalDisk0Available(): Boolean = {
    val localDisk0 = new java.io.File("/local_disk0")
    localDisk0.exists() && localDisk0.isDirectory && localDisk0.canWrite()
  }
}

/** Serializable wrapper for Azure configuration that can be broadcast across executors. */
case class SerializableAzureConfig(
  accountName: Option[String],
  accountKey: Option[String],
  connectionString: Option[String],
  endpoint: Option[String],
  bearerToken: Option[String],
  tenantId: Option[String],
  clientId: Option[String],
  clientSecret: Option[String])
    extends Serializable {

  /** Convert to tantivy4java AzureConfig instance. */
  def toQuickwitSplitAzureConfig(): QuickwitSplit.AzureConfig =
    // Priority 1: Bearer Token (OAuth)
    (accountName, bearerToken) match {
      case (Some(name), Some(token)) =>
        // Use bearer token constructor for OAuth authentication
        QuickwitSplit.AzureConfig.withBearerToken(name, token)
      case _ =>
        // Priority 2: Account Key
        (accountName, accountKey) match {
          case (Some(name), Some(key)) =>
            // Use account name and key constructor
            new QuickwitSplit.AzureConfig(name, key)
          case _ =>
            // Priority 3: Connection String
            connectionString match {
              case Some(connStr) =>
                // Use connection string static factory method
                QuickwitSplit.AzureConfig.fromConnectionString(connStr)
              case None =>
                null // No Azure credentials configured
            }
        }
    }
}

/** Serializable wrapper for AWS configuration that can be broadcast across executors. */
case class SerializableAwsConfig(
  accessKey: String,
  secretKey: String,
  sessionToken: Option[String],
  region: String,
  endpoint: Option[String],
  pathStyleAccess: Boolean,
  tempDirectoryPath: Option[String] = None,
  credentialsProviderClass: Option[String] = None,                // Custom credential provider class name
  heapSize: java.lang.Long = java.lang.Long.valueOf(1073741824L), // Heap size for merge operations (default 1GB)
  debugEnabled: Boolean = false,                                  // Enable debug logging in merge operations
  // All spark.indextables.* configs for credential provider (includes databricks keys)
  allIndextablesConfigs: Map[String, String] = Map.empty
) extends Serializable {

  /** Convert to tantivy4java AwsConfig instance. Resolves custom credential providers if specified. */
  def toQuickwitSplitAwsConfig(tablePath: String): QuickwitSplit.AwsConfig =
    credentialsProviderClass match {
      case Some(providerClassName) =>
        try {
          // Resolve credentials using custom credential provider
          val (resolvedAccessKey, resolvedSecretKey, resolvedSessionToken) =
            resolveCredentialsFromProvider(providerClassName, tablePath)
          new QuickwitSplit.AwsConfig(
            resolvedAccessKey,
            resolvedSecretKey,
            resolvedSessionToken.orNull,
            region,
            endpoint.orNull,
            pathStyleAccess
          )
        } catch {
          case ex: Exception =>
            // Fall back to explicit credentials if provider fails
            System.err.println(
              s"⚠️ [EXECUTOR] Failed to resolve credentials from provider $providerClassName: ${ex.getMessage}"
            )
            System.err.println(s"⚠️ [EXECUTOR] Falling back to explicit credentials")
            new QuickwitSplit.AwsConfig(
              accessKey,
              secretKey,
              sessionToken.orNull,
              region,
              endpoint.orNull,
              pathStyleAccess
            )
        }
      case None =>
        // Use explicit credentials
        new QuickwitSplit.AwsConfig(
          accessKey,
          secretKey,
          sessionToken.orNull,
          region,
          endpoint.orNull,
          pathStyleAccess
        )
    }

  /**
   * Resolve AWS credentials from a custom credential provider class.
   * Returns (accessKey, secretKey, sessionToken).
   *
   * Populates the Hadoop Configuration with all spark.indextables.* configs (including databricks keys)
   * so credential providers like UnityCatalogAWSCredentialProvider can find their configuration.
   */
  private def resolveCredentialsFromProvider(providerClassName: String, tablePath: String)
    : (String, String, Option[String]) = {
    import java.net.URI
    import org.apache.hadoop.conf.Configuration
    import io.indextables.spark.utils.CredentialProviderFactory

    // Use the provided table path for the credential provider constructor
    val tableUri   = new URI(tablePath)
    val hadoopConf = new Configuration()

    // CRITICAL: Populate Hadoop config with all spark.indextables.* configs
    // This includes databricks keys needed by UnityCatalogAWSCredentialProvider:
    //   - spark.indextables.databricks.workspaceUrl
    //   - spark.indextables.databricks.apiToken
    //   - etc.
    allIndextablesConfigs.foreach { case (key, value) =>
      hadoopConf.set(key, value)
    }

    System.err.println(s"[EXECUTOR] Resolving credentials from provider $providerClassName with ${allIndextablesConfigs.size} config keys")

    // Use CredentialProviderFactory to instantiate and extract credentials
    val provider         = CredentialProviderFactory.createCredentialProvider(providerClassName, tableUri, hadoopConf)
    val basicCredentials = CredentialProviderFactory.extractCredentialsViaReflection(provider)

    (basicCredentials.accessKey, basicCredentials.secretKey, basicCredentials.sessionToken)
  }

  /**
   * Execute merge operation using direct/in-process merge. Returns the result as SerializableSplitMetadata for
   * consistency.
   */
  def executeMerge(
    inputSplitPaths: java.util.List[String],
    outputSplitPath: String,
    mergeConfig: QuickwitSplit.MergeConfig
  ): SerializableSplitMetadata = {
    val logger = LoggerFactory.getLogger(this.getClass)
    // Direct merging using QuickwitSplit.mergeSplits with retry logic for streaming errors and timeouts
    val metadata = MergeSplitsCommand.retryOnStreamingError(
      () => QuickwitSplit.mergeSplits(inputSplitPaths, outputSplitPath, mergeConfig),
      s"execute merge ${inputSplitPaths.size()} splits to $outputSplitPath",
      logger
    )
    SerializableSplitMetadata.fromQuickwitSplitMetadata(metadata)
  }
}

/** Executor for merge splits operation. Follows Delta Lake's OptimizeExecutor pattern. */
class MergeSplitsExecutor(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  tablePath: Path,
  partitionPredicates: Seq[String],
  targetSize: Long,
  maxDestSplits: Option[Int],
  maxSourceSplitsPerMerge: Option[Int],
  preCommitMerge: Boolean = false,
  overrideOptions: Option[Map[String, String]] = None) {

  private val logger = LoggerFactory.getLogger(classOf[MergeSplitsExecutor])

  /**
   * Threshold for skipping already-large splits from merge consideration.
   * Splits >= this size are excluded from merge groups to avoid merging files that are already close to target.
   * Default: 45% of target size. Configurable via spark.indextables.merge.skipSplitThreshold (0.0 to 1.0).
   */
  private val skipSplitThresholdPercent: Double = sparkSession.conf
    .getOption("spark.indextables.merge.skipSplitThreshold")
    .map(_.toDouble)
    .getOrElse(0.45)

  private val skipSplitThreshold: Long = (targetSize * skipSplitThresholdPercent).toLong

  logger.info(s"Skip split threshold: $skipSplitThreshold bytes (${(skipSplitThresholdPercent * 100).toInt}% of target size $targetSize)")

  /**
   * Extract AWS configuration from SparkSession for tantivy4java merge operations. Uses same pattern as
   * TantivySearchEngine for consistency. Returns a serializable wrapper that can be broadcast across executors.
   */
  private def extractAwsConfig(): SerializableAwsConfig =
    try {
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

      // Extract and normalize all tantivy4spark configs from both Spark and Hadoop
      val sparkConfigs  = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
      val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
      val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

      // Helper function to get config with priority: overrideOptions > mergedConfigs
      // CASE-INSENSITIVE lookup to handle write options (lowercase) vs expected keys (camelCase)
      def getConfigWithFallback(sparkKey: String): Option[String] = {
        // Try exact match first, then case-insensitive match
        val overrideValue = overrideOptions.flatMap { opts =>
          opts.get(sparkKey).orElse {
            // Case-insensitive fallback for write options
            opts.find { case (k, _) => k.equalsIgnoreCase(sparkKey) }.map(_._2)
          }
        }

        val result = overrideValue.orElse(mergedConfigs.get(sparkKey))

        logger.debug(s"AWS Config fallback for $sparkKey: override=${overrideValue.getOrElse("None")}, merged=${result.getOrElse("None")}")
        result
      }

      val accessKey    = getConfigWithFallback("spark.indextables.aws.accessKey")
      val secretKey    = getConfigWithFallback("spark.indextables.aws.secretKey")
      val sessionToken = getConfigWithFallback("spark.indextables.aws.sessionToken")
      val region       = getConfigWithFallback("spark.indextables.aws.region")
      val endpoint     = getConfigWithFallback("spark.indextables.s3.endpoint")
      val pathStyleAccess = getConfigWithFallback("spark.indextables.s3.pathStyleAccess")
        .map(_.toLowerCase == "true")
        .getOrElse(false)

      // Extract temporary directory configuration for merge operations with fallback chain:
      // 1. Explicit config, 2. /local_disk0 if available, 3. null (system default)
      val tempDirectoryPath = getConfigWithFallback("spark.indextables.merge.tempDirectoryPath")
        .orElse(if (MergeSplitsCommand.isLocalDisk0Available()) Some("/local_disk0/tantivy4spark-temp") else None)

      // Extract custom credential provider class name
      val credentialsProviderClass = getConfigWithFallback("spark.indextables.aws.credentialsProviderClass")

      // Extract merge operation configuration (default heap size: 1GB)
      val heapSize = getConfigWithFallback("spark.indextables.merge.heapSize")
        .map(io.indextables.spark.util.SizeParser.parseSize)
        .map(java.lang.Long.valueOf)
        .getOrElse(java.lang.Long.valueOf(1073741824L)) // 1GB default
      val debugEnabled =
        getConfigWithFallback("spark.indextables.merge.debug").exists(v => v.equalsIgnoreCase("true") || v == "1")

      logger.info(s"Creating AwsConfig with: region=${region.getOrElse("None")}, endpoint=${endpoint.getOrElse("None")}, pathStyle=$pathStyleAccess")
      logger.info(
        s"AWS credentials: accessKey=${accessKey.map(k => s"${k.take(4)}***").getOrElse("None")}, sessionToken=${sessionToken.map(_ => "***").getOrElse("None")}"
      )
      logger.info(s"Credentials provider class: ${credentialsProviderClass.getOrElse("None")}")
      logger.info(s"Merge temp directory: ${tempDirectoryPath.getOrElse("system default")}")

      // Validate temp directory path if specified
      tempDirectoryPath.foreach { path =>
        try {
          val dir = new java.io.File(path)
          if (!dir.exists()) {
            logger.warn(s"⚠️ Custom temp directory does not exist: $path - will fall back to system temp directory")
          } else if (!dir.isDirectory()) {
            logger.warn(
              s"⚠️ Custom temp directory path is not a directory: $path - will fall back to system temp directory"
            )
          } else if (!dir.canWrite()) {
            logger.warn(s"⚠️ Custom temp directory is not writable: $path - will fall back to system temp directory")
          } else {
            logger.info(s"✅ Custom temp directory validated: $path")
          }
        } catch {
          case ex: Exception =>
            logger.warn(s"⚠️ Failed to validate custom temp directory '$path': ${ex.getMessage} - will fall back to system temp directory")
        }
      }

      // Resolve credentials from custom provider on driver if configured
      // This fetches actual AWS credentials so workers don't need to run the provider
      val resolvedConfigs = credentialsProviderClass match {
        case Some(providerClass) if providerClass.nonEmpty =>
          logger.info(s"Resolving credentials from provider $providerClass for merge operation")
          ConfigUtils.resolveCredentialsFromProviderOnDriver(mergedConfigs, tablePath.toString)
        case _ =>
          mergedConfigs
      }

      // Re-extract credentials from resolved configs (may have been updated by provider)
      val resolvedAccessKey = resolvedConfigs.get("spark.indextables.aws.accessKey")
        .orElse(accessKey)
      val resolvedSecretKey = resolvedConfigs.get("spark.indextables.aws.secretKey")
        .orElse(secretKey)
      val resolvedSessionToken = resolvedConfigs.get("spark.indextables.aws.sessionToken")
        .orElse(sessionToken)

      // Create SerializableAwsConfig with the extracted credentials and temp directory
      // Include ALL spark.indextables.* configs for credential providers (e.g., databricks keys)
      logger.info(s"Including ${resolvedConfigs.size} spark.indextables.* configs for credential provider")
      if (resolvedConfigs.keys.exists(_.contains("databricks"))) {
        val databricksKeys = resolvedConfigs.keys.filter(_.contains("databricks"))
        logger.info(s"Databricks configs included: ${databricksKeys.mkString(", ")}")
      }

      SerializableAwsConfig(
        resolvedAccessKey.getOrElse(""),
        resolvedSecretKey.getOrElse(""),
        resolvedSessionToken, // Can be None for permanent credentials
        region.getOrElse("us-east-1"),
        endpoint, // Can be None for default AWS endpoint
        pathStyleAccess,
        tempDirectoryPath,        // Custom temp directory path for merge operations
        credentialsProviderClass, // Custom credential provider class name
        heapSize,                 // Heap size for merge operations
        debugEnabled,             // Debug logging for merge operations
        resolvedConfigs           // All spark.indextables.* configs for credential provider
      )
    } catch {
      case ex: Exception =>
        logger.error("Failed to extract AWS config from Spark session", ex)
        throw new RuntimeException("Failed to extract AWS config for merge operation", ex)
    }

  /**
   * Extract Azure configuration from SparkSession for tantivy4java merge operations. Returns a serializable wrapper
   * that can be broadcast across executors.
   */
  private def extractAzureConfig(): SerializableAzureConfig =
    try {
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

      // Extract and normalize all tantivy4spark configs from both Spark and Hadoop
      val sparkConfigs  = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
      val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
      val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

      // Helper function to get config with priority: overrideOptions > mergedConfigs
      def getConfigWithFallback(sparkKey: String): Option[String] = {
        val result = overrideOptions.flatMap(_.get(sparkKey)).orElse(mergedConfigs.get(sparkKey))
        logger.debug(s"Azure Config fallback for $sparkKey: override=${overrideOptions.flatMap(_.get(sparkKey)).getOrElse("None")}, merged=${result.getOrElse("None")}")
        result
      }

      val accountName      = getConfigWithFallback("spark.indextables.azure.accountName")
      val accountKey       = getConfigWithFallback("spark.indextables.azure.accountKey")
      val connectionString = getConfigWithFallback("spark.indextables.azure.connectionString")
      val endpoint         = getConfigWithFallback("spark.indextables.azure.endpoint")
      val bearerToken      = getConfigWithFallback("spark.indextables.azure.bearerToken")
      val tenantId         = getConfigWithFallback("spark.indextables.azure.tenantId")
      val clientId         = getConfigWithFallback("spark.indextables.azure.clientId")
      val clientSecret     = getConfigWithFallback("spark.indextables.azure.clientSecret")

      logger.info(s"Creating AzureConfig with: accountName=${accountName.getOrElse("None")}, endpoint=${endpoint.getOrElse("None")}")
      logger.info(
        s"Azure credentials: accountKey=${accountKey.map(_ => "***").getOrElse("None")}, connectionString=${connectionString
            .map(_ => "***")
            .getOrElse("None")}, bearerToken=${bearerToken.map(_ => "***").getOrElse("None")}"
      )
      logger.info(s"OAuth credentials: tenantId=${tenantId.getOrElse("None")}, clientId=${clientId
          .getOrElse("None")}, clientSecret=${clientSecret.map(_ => "***").getOrElse("None")}")

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
        // Return empty config - Azure is optional
        SerializableAzureConfig(None, None, None, None, None, None, None, None)
    }

  def merge(): Seq[Row] = {
    if (preCommitMerge) {
      logger.info(
        s"Starting PRE-COMMIT MERGE SPLITS operation for table: $tablePath with target size: $targetSize bytes"
      )
      return performPreCommitMerge()
    }

    logger.info(s"Starting MERGE SPLITS operation for table: $tablePath with target size: $targetSize bytes")

    // Get current metadata to understand partition schema
    val metadata = transactionLog.getMetadata()

    // DEBUG: Log the metadata details
    logger.info(s"MERGE DEBUG: Retrieved metadata from transaction log:")
    logger.info(s"MERGE DEBUG:   Metadata ID: ${metadata.id}")
    logger.info(s"MERGE DEBUG:   Partition columns: ${metadata.partitionColumns}")
    logger.info(s"MERGE DEBUG:   Partition columns size: ${metadata.partitionColumns.size}")
    logger.info(s"MERGE DEBUG:   Configuration: ${metadata.configuration}")

    val partitionSchema = StructType(
      metadata.partitionColumns.map(name => StructField(name, StringType, nullable = true))
    )

    logger.info(s"MERGE DEBUG: Constructed partition schema: ${partitionSchema.fieldNames.mkString(", ")}")

    // If no partition columns are defined in metadata, skip partition validation
    if (metadata.partitionColumns.isEmpty) {
      logger.info("No partition columns defined in metadata - treating table as non-partitioned")
    } else {
      logger.info(
        s"Found ${metadata.partitionColumns.size} partition columns: ${metadata.partitionColumns.mkString(", ")}"
      )
    }

    // Get current files from transaction log (in order they were added)
    val allFiles = transactionLog.listFiles().sortBy(_.modificationTime)
    logger.info(s"Found ${allFiles.length} split files in transaction log")

    // Filter out files that are currently in cooldown period
    val trackingEnabled = sparkSession.conf.get("spark.indextables.skippedFiles.trackingEnabled", "true").toBoolean
    val currentFiles = if (trackingEnabled) {
      val filtered      = transactionLog.filterFilesInCooldown(allFiles)
      val filteredCount = allFiles.length - filtered.length
      if (filteredCount > 0) {
        logger.info(s"Filtered out $filteredCount files in cooldown period")
      }
      filtered
    } else {
      allFiles
    }

    logger.info(s"Processing ${currentFiles.length} files for merge (after cooldown filtering)")
    currentFiles.foreach { file =>
      logger.info(s"  File: ${file.path} (${file.size} bytes, partition: ${file.partitionValues})")
    }

    // Group files by partition (merge only within partitions - Delta Lake pattern)
    val partitionsToMerge = currentFiles.groupBy(_.partitionValues).toSeq
    logger.info(s"Found ${partitionsToMerge.length} partitions to potentially merge")
    partitionsToMerge.foreach {
      case (partitionValues, files) =>
        logger.info(s"  Partition $partitionValues: ${files.length} files")
    }

    // Apply partition predicates if specified
    val filteredPartitions = if (partitionPredicates.nonEmpty) {
      applyPartitionPredicates(partitionsToMerge, partitionSchema)
    } else {
      partitionsToMerge
    }

    // Get effective maxSourceSplitsPerMerge (from parameter or config default)
    val effectiveMaxSourceSplitsPerMerge = maxSourceSplitsPerMerge.orElse {
      sparkSession.conf.getOption(
        io.indextables.spark.config.IndexTables4SparkSQLConf.TANTIVY4SPARK_MERGE_MAX_SOURCE_SPLITS_PER_MERGE
      ).map(_.toInt)
    }.getOrElse(io.indextables.spark.config.IndexTables4SparkSQLConf.TANTIVY4SPARK_MERGE_MAX_SOURCE_SPLITS_PER_MERGE_DEFAULT)

    logger.info(s"Using MAX SOURCE SPLITS PER MERGE: $effectiveMaxSourceSplitsPerMerge")

    // Find mergeable splits within each partition
    val mergeGroups = filteredPartitions.flatMap {
      case (partitionValues, files) =>
        logger.info(s"Processing partition $partitionValues with ${files.length} files:")
        files.foreach(file => logger.info(s"  File: ${file.path} (${file.size} bytes)"))

        val groups = findMergeableGroups(partitionValues, files, effectiveMaxSourceSplitsPerMerge)
        logger.info(s"Found ${groups.length} potential merge groups in partition $partitionValues")

        // Double-check: filter out any single-file groups that might have slipped through
        logger.debug(s"MERGE DEBUG: Before filtering: ${groups.length} groups")
        groups.foreach { group =>
          logger.debug(
            s"MERGE DEBUG:   Group has ${group.files.length} files: ${group.files.map(_.path).mkString(", ")}"
          )
        }
        val validGroups = groups.filter(_.files.length >= 2)
        logger.debug(s"MERGE DEBUG: After filtering: ${validGroups.length} valid groups")
        if (groups.length != validGroups.length) {
          logger.warn(
            s"Filtered out ${groups.length - validGroups.length} single-file groups from partition $partitionValues"
          )
        }

        validGroups.foreach { group =>
          logger.info(s"  Valid merge group: ${group.files.length} files (${group.files.map(_.size).sum} bytes total)")
          group.files.foreach(file => logger.info(s"    - ${file.path} (${file.size} bytes)"))
        }

        validGroups
    }

    logger.info(s"Found ${mergeGroups.length} merge groups containing ${mergeGroups.map(_.files.length).sum} files")

    // Sort ALL merge groups by:
    // 1. Primary: source split count (descending) - prioritize high-impact merges
    // 2. Secondary: oldest modification time (ascending) - tie-break with oldest first
    val prioritizedMergeGroups = mergeGroups.sortBy { g =>
      val sourceCount = -g.files.length // Negative for descending order
      val oldestTime = g.files.map(_.modificationTime).min // Ascending (oldest first for ties)
      (sourceCount, oldestTime)
    }
    logger.info(s"Prioritized merge groups by source split count (ties broken by oldest): ${prioritizedMergeGroups.map(_.files.length).take(10).mkString(", ")}${if (prioritizedMergeGroups.length > 10) "..." else ""}")

    // Apply MAX DEST SPLITS limit if specified (formerly MAX GROUPS)
    // Now takes the top N groups by source split count (highest impact merges first)
    val limitedMergeGroups = maxDestSplits match {
      case Some(maxLimit) if prioritizedMergeGroups.length > maxLimit =>
        logger.info(s"Limiting merge operation to $maxLimit highest-impact dest splits (out of ${prioritizedMergeGroups.length} total)")

        // Take the top N groups (already sorted by source split count descending, oldest first for ties)
        val limitedGroups = prioritizedMergeGroups.take(maxLimit)

        val limitedFilesCount = limitedGroups.map(_.files.length).sum
        val totalFilesCount   = prioritizedMergeGroups.map(_.files.length).sum
        val skippedGroups = prioritizedMergeGroups.drop(maxLimit)
        logger.info(
          s"MAX DEST SPLITS limit applied: processing $limitedFilesCount files from $maxLimit highest-impact groups " +
          s"(source splits range: ${limitedGroups.map(_.files.length).min}-${limitedGroups.map(_.files.length).max})"
        )
        if (skippedGroups.nonEmpty) {
          logger.info(
            s"Skipping ${skippedGroups.length} lower-impact groups with ${skippedGroups.map(_.files.length).sum} files " +
            s"(source splits range: ${skippedGroups.map(_.files.length).min}-${skippedGroups.map(_.files.length).max})"
          )
        }

        limitedGroups
      case Some(maxLimit) =>
        logger.info(s"MAX DEST SPLITS limit of $maxLimit not reached (found ${prioritizedMergeGroups.length} groups)")
        prioritizedMergeGroups
      case None =>
        logger.debug("No MAX DEST SPLITS limit specified")
        prioritizedMergeGroups
    }

    // Final safety check: ensure no single-file groups exist
    val singleFileGroups = limitedMergeGroups.filter(_.files.length < 2)
    if (singleFileGroups.nonEmpty) {
      logger.error(s"CRITICAL: Found ${singleFileGroups.length} single-file groups that should have been filtered out!")
      singleFileGroups.foreach { group =>
        logger.error(s"  Single-file group: ${group.files.head.path} in partition ${group.partitionValues}")
      }
      throw new IllegalStateException(s"Internal error: Found ${singleFileGroups.length} single-file merge groups")
    }

    // Extract AWS and Azure configuration early so it's available for all code paths
    val awsConfig   = extractAwsConfig()
    val azureConfig = extractAzureConfig()

    if (limitedMergeGroups.isEmpty) {
      logger.info("No splits require merging")
      return Seq(
        Row(
          tablePath.toString,
          Row("no_action", null, null, null, null, "All splits are already optimal size"),
          awsConfig.tempDirectoryPath.getOrElse(null),
          if (awsConfig.heapSize == null) null else awsConfig.heapSize.asInstanceOf[Long]
        )
      )
    }

    // Get batch configuration - Delta Lake-inspired batching strategy
    val defaultParallelism = sparkSession.sparkContext.defaultParallelism
    val batchSize = sparkSession.conf
      .getOption("spark.indextables.merge.batchSize")
      .map(_.toInt)
      .getOrElse(defaultParallelism)

    val maxConcurrentBatches = sparkSession.conf
      .getOption("spark.indextables.merge.maxConcurrentBatches")
      .map(_.toInt)
      .getOrElse(2)

    logger.info(s"Batch configuration: batchSize=$batchSize (defaultParallelism=$defaultParallelism), maxConcurrentBatches=$maxConcurrentBatches")

    // limitedMergeGroups is already sorted by source split count (descending), oldest first for ties
    // Split merge groups into batches based on batch size
    val batches = limitedMergeGroups.grouped(batchSize).toSeq
    logger.info(s"Split ${limitedMergeGroups.length} merge groups into ${batches.length} batches")

    batches.zipWithIndex.foreach {
      case (batch, idx) =>
        logger.info(s"Batch ${idx + 1}/${batches.length}: ${batch.length} merge groups")
    }

    // Execute batches concurrently with controlled parallelism
    logger.info(s"Processing ${batches.length} batches with up to $maxConcurrentBatches concurrent batches")

    // Broadcast Azure configuration and table path (these don't expire)
    // AWS config is refreshed per-batch to get fresh credentials from provider
    val broadcastAzureConfig = sparkSession.sparkContext.broadcast(azureConfig)
    val broadcastTablePath   = sparkSession.sparkContext.broadcast(tablePath.toString)

    // Process batches with controlled concurrency using Scala parallel collections
    import scala.collection.parallel.ForkJoinTaskSupport
    import java.util.concurrent.ForkJoinPool
    import scala.util.{Try, Success, Failure}

    val batchResults      = batches.zipWithIndex.par
    val customTaskSupport = new ForkJoinTaskSupport(new ForkJoinPool(maxConcurrentBatches))
    batchResults.tasksupport = customTaskSupport

    // Track batch failures
    var failedBatchCount     = 0
    var successfulBatchCount = 0

    val allResults =
      try {
        batchResults.flatMap {
          case (batch, batchIdx) =>
            Try {
              val batchNum    = batchIdx + 1
              val totalSplits = batch.map(_.files.length).sum
              val totalSizeGB = batch.map(_.files.map(_.size).sum).sum / (1024.0 * 1024.0 * 1024.0)

              logger.info(s"Starting batch $batchNum/${batches.length}: ${batch.length} merge groups, $totalSplits splits, $totalSizeGB%.2f GB")

              // Refresh AWS credentials for this batch (credentials may have expired)
              // This calls the credential provider on the driver to get fresh credentials
              logger.info(s"Refreshing AWS credentials for batch $batchNum")
              val batchAwsConfig = extractAwsConfig()
              val broadcastAwsConfig = sparkSession.sparkContext.broadcast(batchAwsConfig)

              // Set descriptive names for Spark UI
              val jobGroup = s"tantivy4spark-merge-splits-batch-$batchNum"
              val jobDescription =
                f"MERGE SPLITS Batch $batchNum/${batches.length}: $totalSplits splits merging to ${batch.length} splits ($totalSizeGB%.2f GB)"
              val stageName = f"Merge Batch $batchNum/${batches.length}: ${batch.length} groups, $totalSplits splits"

              sparkSession.sparkContext.setJobGroup(jobGroup, jobDescription, interruptOnCancel = true)

              val batchStartTime = System.currentTimeMillis()

              val physicalMergeResults =
                try {
                  val mergeGroupsRDD = sparkSession.sparkContext
                    .parallelize(batch, batch.length)
                    .setName(stageName)
                  mergeGroupsRDD
                    .map(group =>
                      MergeSplitsExecutor
                        .executeMergeGroupDistributed(
                          group,
                          broadcastTablePath.value,
                          broadcastAwsConfig.value,
                          broadcastAzureConfig.value
                        )
                    )
                    .setName(s"Merge Results Batch $batchNum")
                    .collect()
                } finally {
                  sparkSession.sparkContext.clearJobGroup()
                  // Clean up per-batch broadcast to free memory
                  try {
                    broadcastAwsConfig.destroy()
                    logger.debug(s"Cleaned up AWS config broadcast for batch $batchNum")
                  } catch {
                    case ex: Exception =>
                      logger.warn(s"Failed to destroy AWS config broadcast for batch $batchNum: ${ex.getMessage}")
                  }
                }

              val batchElapsed = System.currentTimeMillis() - batchStartTime
              logger.info(s"Batch $batchNum/${batches.length} physical merge completed in ${batchElapsed}ms")

              // Now handle transaction log operations on driver (these cannot be distributed)
              logger.info(
                s"Processing ${physicalMergeResults.length} merge results for batch $batchNum transaction log updates"
              )

              // CRITICAL: Validate all merged files actually exist before updating transaction log
              logger.debug(
                s"[DRIVER] Batch $batchNum: Validating ${physicalMergeResults.length} merged files"
              )
              physicalMergeResults.foreach { result =>
                val fullMergedPath =
                  if (tablePath.toString.startsWith("s3://") || tablePath.toString.startsWith("s3a://")) {
                    s"${tablePath.toString.replaceAll("/$", "")}/${result.mergedSplitInfo.path}"
                  } else {
                    new org.apache.hadoop.fs.Path(tablePath.toString, result.mergedSplitInfo.path).toString
                  }

                try
                  // For S3, we can't easily check file existence from driver, but we can at least log the expected path
                  logger.debug(s"[Batch $batchNum] Merged file should exist at: $fullMergedPath")
                catch {
                  case ex: Exception =>
                    logger.warn(s"[Batch $batchNum] Could not validate merged file existence: ${ex.getMessage}", ex)
                }
              }

              // Collect all remove and add actions from this batch's merge results
              val batchRemoveActions = ArrayBuffer[RemoveAction]()
              val batchAddActions    = ArrayBuffer[AddAction]()

              val batchResults = physicalMergeResults.map { result =>
                val startTime = System.currentTimeMillis()

                logger.info(s"Processing transaction log for merge group with ${result.mergeGroup.files.length} files")

                // Check if merge was actually performed by examining indexUid in metadata
                val mergedMetadata = result.mergedSplitInfo.metadata
                val indexUid       = mergedMetadata.indexUid

                // Extract skipped splits from the merge result to avoid marking them as removed
                val skippedSplitPaths = Option(result.mergedSplitInfo.metadata.getSkippedSplits())
                  .map(_.asScala.toSet)
                  .getOrElse(Set.empty[String])

                // Always record skipped files regardless of whether merge was performed
                if (skippedSplitPaths.nonEmpty) {
                  logger.warn(s"⚠️  Merge operation skipped ${skippedSplitPaths.size} files (due to corruption/missing files): ${skippedSplitPaths.mkString(", ")}")

                  // Record skipped files in transaction log with cooldown period
                  val cooldownHours =
                    sparkSession.conf.get("spark.indextables.skippedFiles.cooldownDuration", "24").toInt
                  val trackingEnabled =
                    sparkSession.conf.get("spark.indextables.skippedFiles.trackingEnabled", "true").toBoolean

                  if (trackingEnabled) {
                    skippedSplitPaths.foreach { skippedPath =>
                      // Find the corresponding file in merge group to get partition info and size
                      val correspondingFile = result.mergeGroup.files.find { file =>
                        val fullPath =
                          if (tablePath.toString.startsWith("s3://") || tablePath.toString.startsWith("s3a://")) {
                            val normalizedBaseUri =
                              tablePath.toString.replaceFirst("^s3a://", "s3://").replaceAll("/$", "")
                            s"$normalizedBaseUri/${file.path}"
                          } else {
                            new org.apache.hadoop.fs.Path(tablePath.toString, file.path).toString
                          }
                        fullPath == skippedPath || file.path == skippedPath
                      }

                      val reason = "Merge operation failed - possibly corrupted file or read error"
                      transactionLog.recordSkippedFile(
                        filePath = correspondingFile.map(_.path).getOrElse(skippedPath),
                        reason = reason,
                        operation = "merge",
                        partitionValues = correspondingFile.map(_.partitionValues),
                        size = correspondingFile.map(_.size),
                        cooldownHours = cooldownHours
                      )
                      logger.debug(s"Recorded skipped file with ${cooldownHours}h cooldown: ${correspondingFile.map(_.path).getOrElse(skippedPath)}")
                    }
                  }
                }

                // Check if no merge was performed (null or empty indexUid indicates this)
                if (indexUid.isEmpty || indexUid.contains(null) || indexUid.exists(_.trim.isEmpty)) {
                  logger.warn(s"⚠️  No merge was performed for group with ${result.mergeGroup.files.length} files (null/empty indexUid) - skipping ADD/REMOVE operations but preserving skipped files tracking")

                  // Return without performing ADD/REMOVE operations
                  // Note: We still recorded the skipped files above, which is the desired behavior
                  result.copy(
                    mergedFiles = 0, // No files were actually merged
                    mergedSize = 0L, // No merged split was created
                    originalSize = result.mergeGroup.files.map(_.size).sum,
                    executionTimeMs = result.executionTimeMs + (System.currentTimeMillis() - startTime)
                  )
                } else {

                  // Prepare transaction actions (REMOVE + ADD pattern from Delta Lake)
                  // CRITICAL: Only remove files that were actually merged, not skipped files
                  val removeActions = result.mergeGroup.files
                    .filter { file =>
                      val fullPath =
                        if (tablePath.toString.startsWith("s3://") || tablePath.toString.startsWith("s3a://")) {
                          // For S3 paths, construct full URL to match what tantivy4java reports
                          val normalizedBaseUri =
                            tablePath.toString.replaceFirst("^s3a://", "s3://").replaceAll("/$", "")
                          s"$normalizedBaseUri/${file.path}"
                        } else {
                          // For local paths, use absolute path
                          new org.apache.hadoop.fs.Path(tablePath.toString, file.path).toString
                        }

                      val wasSkipped = skippedSplitPaths.contains(fullPath) || skippedSplitPaths.contains(file.path)
                      if (wasSkipped) {
                        logger.info(s"Preserving skipped file in transaction log (not marking as removed): ${file.path}")
                      }
                      !wasSkipped
                    }
                    .map { file =>
                      RemoveAction(
                        path = file.path,
                        deletionTimestamp = Some(startTime),
                        dataChange = false, // This is compaction, not data change
                        extendedFileMetadata = Some(true),
                        partitionValues = Some(file.partitionValues),
                        size = Some(file.size),
                        tags = file.tags
                      )
                    }

                  // Extract ALL metadata from merged split for complete pipeline coverage
                  // (mergedMetadata already extracted above for indexUid check)
                  val (
                    footerStartOffset,
                    footerEndOffset,
                    hotcacheStartOffset,
                    hotcacheLength,
                    hasFooterOffsets,
                    timeRangeStart,
                    timeRangeEnd,
                    splitTags,
                    deleteOpstamp,
                    numMergeOps,
                    docMappingJson,
                    uncompressedSizeBytes,
                    numDocs
                  ) =
                    if (mergedMetadata != null) {
                      val timeStart = Option(mergedMetadata.getTimeRangeStart()).map(_.toString)
                      val timeEnd   = Option(mergedMetadata.getTimeRangeEnd()).map(_.toString)
                      val tags = Option(mergedMetadata.getTags()).filter(!_.isEmpty).map { tagSet =>
                        import scala.jdk.CollectionConverters._
                        tagSet.asScala.toSet
                      }
                      val docMapping = Option(mergedMetadata.getDocMappingJson())

                      // CRITICAL DEBUG: Verify docMappingJson returned from tantivy4java merge
                      docMapping match {
                        case Some(json) =>
                          logger.warn(
                            s"✅ MERGE RESULT: docMappingJson extracted from merged split (${json.length} chars)"
                          )
                          logger.warn(s"✅ MERGE RESULT: docMappingJson content: $json")
                        case None =>
                          logger.error(
                            s"❌ MERGE RESULT: No docMappingJson in merged split metadata - tantivy4java did not preserve it!"
                          )
                      }

                      if (mergedMetadata.hasFooterOffsets) {
                        (
                          Some(mergedMetadata.getFooterStartOffset()),
                          Some(mergedMetadata.getFooterEndOffset()),
                          None, // hotcacheStartOffset - deprecated, use footer offsets instead
                          None, // hotcacheLength - deprecated, use footer offsets instead
                          true,
                          timeStart,
                          timeEnd,
                          tags,
                          Some(mergedMetadata.getDeleteOpstamp()),
                          Some(mergedMetadata.getNumMergeOps()),
                          docMapping,
                          Some(mergedMetadata.getUncompressedSizeBytes()),
                          Some(mergedMetadata.getNumDocs())
                        )
                      } else {
                        throw new IllegalStateException(
                          s"Merged split ${result.mergedSplitInfo.path} does not have footer offsets. This indicates a problem with the merge operation or tantivy4java library."
                        )
                      }
                    } else {
                      throw new IllegalStateException(
                        s"Failed to extract metadata from merged split ${result.mergedSplitInfo.path}. This indicates a problem with the merge operation or tantivy4java library."
                      )
                    }

                  // CRITICAL DEBUG: Verify docMappingJson being saved to AddAction
                  docMappingJson match {
                    case Some(json) =>
                      logger.warn(s"✅ TRANSACTION LOG: Saving AddAction with docMappingJson (${json.length} chars)")
                      logger.warn(s"✅ TRANSACTION LOG: docMappingJson being saved: $json")
                    case None =>
                      logger.error(s"❌ TRANSACTION LOG: AddAction has NO docMappingJson - fast fields will be lost!")
                  }

                  val addAction = AddAction(
                    path = result.mergedSplitInfo.path,
                    partitionValues = result.mergeGroup.partitionValues,
                    size = result.mergedSplitInfo.size,
                    modificationTime = startTime,
                    dataChange = false, // This is compaction, not data change
                    stats = None,
                    tags = None,
                    numRecords = numDocs, // Number of documents in the merged split
                    // Footer offset optimization metadata preserved from merge operation
                    footerStartOffset = footerStartOffset,
                    footerEndOffset = footerEndOffset,
                    hotcacheStartOffset = hotcacheStartOffset,
                    hotcacheLength = hotcacheLength,
                    hasFooterOffsets = hasFooterOffsets,
                    // Complete tantivy4java SplitMetadata fields preserved from merge
                    timeRangeStart = timeRangeStart,
                    timeRangeEnd = timeRangeEnd,
                    splitTags = splitTags,
                    deleteOpstamp = deleteOpstamp,
                    numMergeOps = numMergeOps,
                    docMappingJson = docMappingJson,
                    uncompressedSizeBytes = uncompressedSizeBytes
                  )

                  // Collect actions for batch commit instead of committing individually
                  batchRemoveActions ++= removeActions
                  batchAddActions += addAction

                  logger.info(s"[Batch $batchNum] Prepared transaction actions: ${removeActions.length} removes, 1 add")

                  // Return the merge result with updated timing
                  result.copy(executionTimeMs = result.executionTimeMs + (System.currentTimeMillis() - startTime))
                } // End of else block for indexUid check
              }

              // Commit this batch's actions in a single transaction
              if (batchAddActions.nonEmpty || batchRemoveActions.nonEmpty) {
                val txnStartTime = System.currentTimeMillis()
                logger.info(s"[Batch $batchNum] Committing batch transaction with ${batchRemoveActions.length} removes and ${batchAddActions.length} adds")

                val version = transactionLog.commitMergeSplits(batchRemoveActions, batchAddActions)
                transactionLog.invalidateCache() // Ensure cache is updated

                val txnElapsed = System.currentTimeMillis() - txnStartTime
                logger.info(s"[Batch $batchNum] Transaction log updated at version $version in ${txnElapsed}ms")
              } else {
                logger.info(s"[Batch $batchNum] No transaction actions to commit")
              }

              val batchTotalTime = System.currentTimeMillis() - batchStartTime
              logger.info(s"[Batch $batchNum] Total batch time (merge + transaction): ${batchTotalTime}ms")

              successfulBatchCount += 1

              // Return results from this batch
              batchResults
            } match {
              case Success(results) => results
              case Failure(ex) =>
                failedBatchCount += 1
                val batchNum = batchIdx + 1
                logger.error(s"[Batch $batchNum] Failed to process batch", ex)
                Seq.empty // Return empty sequence for failed batches
            }
        }.toList
      } finally
        customTaskSupport.environment.shutdown()

    val totalMergedFiles  = allResults.map(_.mergedFiles).sum
    val totalMergeGroups  = allResults.length
    val totalOriginalSize = allResults.map(_.originalSize).sum
    val totalMergedSize   = allResults.map(_.mergedSize).sum

    val status = if (failedBatchCount == 0) "success" else "partial_success"

    logger.info(s"MERGE SPLITS completed: merged $totalMergedFiles files into $totalMergeGroups new splits across ${batches.length} batches")
    logger.info(s"Batch summary: $successfulBatchCount successful, $failedBatchCount failed")
    logger.info(s"Size change: $totalOriginalSize bytes -> $totalMergedSize bytes")

    Seq(
      Row(
        tablePath.toString,
        Row(
          status,
          totalMergedFiles.asInstanceOf[Long],
          totalMergeGroups.asInstanceOf[Long],
          totalOriginalSize,
          totalMergedSize,
          s"batches: ${batches.length}, successful: $successfulBatchCount, failed: $failedBatchCount"
        ),
        awsConfig.tempDirectoryPath.getOrElse(null),
        if (awsConfig.heapSize == null) null else awsConfig.heapSize.asInstanceOf[Long]
      )
    )
  }

  /**
   * Performs pre-commit merge where splits are merged before being added to the transaction log. In this mode, original
   * fragmental splits are deleted after the merged split is uploaded and the transaction log never sees the original
   * splits.
   */
  private def performPreCommitMerge(): Seq[Row] = {
    logger.info("PRE-COMMIT MERGE: This functionality merges splits before they appear in transaction log")
    logger.info("PRE-COMMIT MERGE: Original fragmental splits are deleted and never logged")

    // For pre-commit merge, we need to work with pending/staging splits rather than committed ones
    // This would typically integrate with the write path to merge splits during the commit process

    // TODO: Implement actual pre-commit merge logic that:
    // 1. Identifies pending splits that haven't been committed yet
    // 2. Groups them by partition
    // 3. Merges groups that exceed fragmentation thresholds
    // 4. Deletes original fragmental splits from storage
    // 5. Commits only the merged splits to transaction log

    logger.warn("PRE-COMMIT MERGE: Implementation pending - this is a placeholder")

    Seq(
      Row(tablePath.toString, Row("pending", null, null, null, null, "Functionality pending implementation"), null, null)
    )
  }

  /**
   * Find groups of files that should be merged within a partition. Follows bin packing approach similar to Delta Lake's
   * OPTIMIZE. Only creates groups with 2+ files to satisfy tantivy4java merge requirements. CRITICAL: Ensures all files
   * in each group have identical partition values.
   *
   * @param partitionValues partition values for the files in this group
   * @param files files to group for merging
   * @param maxSourceSplitsPerMerge maximum number of source splits that can be merged in a single merge operation
   */
  private def findMergeableGroups(
    partitionValues: Map[String, String],
    files: Seq[AddAction],
    maxSourceSplitsPerMerge: Int
  ): Seq[MergeGroup] = {

    val groups           = ArrayBuffer[MergeGroup]()
    val currentGroup     = ArrayBuffer[AddAction]()
    var currentGroupSize = 0L

    // CRITICAL: Validate all input files belong to the expected partition
    val invalidFiles = files.filterNot(_.partitionValues == partitionValues)
    if (invalidFiles.nonEmpty) {
      val errorMsg = s"findMergeableGroups received files from wrong partitions! Expected: $partitionValues\n" +
        "Invalid files:\n" + invalidFiles.map(f => s"  - ${f.path}: ${f.partitionValues}").mkString("\n")
      logger.error(errorMsg)
      throw new IllegalStateException(errorMsg)
    }

    logger.debug(s"✅ Partition validation passed: All ${files.length} input files belong to partition $partitionValues")

    // Filter out files that are already at or above the skip threshold
    // Default: 45% of target size - prevents merging already-large splits
    val mergeableFiles = files.filter { file =>
      if (file.size >= skipSplitThreshold) {
        logger.debug(s"Skipping file ${file.path} (size: ${file.size} bytes) - already at or above skip threshold ($skipSplitThreshold bytes, ${(skipSplitThresholdPercent * 100).toInt}% of target)")
        false
      } else {
        true
      }
    }

    val skippedCount = files.length - mergeableFiles.length
    if (skippedCount > 0) {
      logger.info(s"Skipped $skippedCount files already at or above skip threshold ($skipSplitThreshold bytes)")
    }
    logger.debug(s"MERGE DEBUG: Found ${mergeableFiles.length} files eligible for merging (< $skipSplitThreshold bytes)")

    // If we have fewer than 2 mergeable files, no groups can be created
    if (mergeableFiles.length < 2) {
      logger.debug(s"MERGE DEBUG: Cannot create merge groups - need at least 2 files but found ${mergeableFiles.length}")
      return groups.toSeq
    }

    for ((file, index) <- mergeableFiles.zipWithIndex) {
      logger.debug(
        s"MERGE DEBUG: Processing file ${index + 1}/${mergeableFiles.length}: ${file.path} (${file.size} bytes)"
      )

      // Check if adding this file would exceed target size OR max source splits per merge
      val wouldExceedTargetSize = currentGroupSize > 0 && currentGroupSize + file.size > targetSize
      val wouldExceedMaxSplits = currentGroup.length >= maxSourceSplitsPerMerge

      if (wouldExceedTargetSize || wouldExceedMaxSplits) {
        if (wouldExceedMaxSplits) {
          logger.debug(s"MERGE DEBUG: Adding ${file.path} would exceed max source splits per merge ($maxSourceSplitsPerMerge)")
        } else {
          logger.debug(s"MERGE DEBUG: Adding ${file.path} (${file.size} bytes) to current group ($currentGroupSize bytes) would exceed target ($targetSize bytes)")
        }

        // Current group is full, save it if it has multiple files
        logger.debug(s"MERGE DEBUG: Current group has ${currentGroup.length} files before saving")
        if (currentGroup.length > 1) {
          // VALIDATION: Ensure all files in the group have identical partition values
          val groupFiles        = currentGroup.clone().toSeq
          val inconsistentFiles = groupFiles.filterNot(_.partitionValues == partitionValues)
          if (inconsistentFiles.nonEmpty) {
            val errorMsg =
              s"CRITICAL: Group validation failed during creation! Found files with inconsistent partitions:\n" +
                inconsistentFiles
                  .map(f => s"  - ${f.path}: ${f.partitionValues} (expected: $partitionValues)")
                  .mkString("\n")
            logger.error(errorMsg)
            throw new IllegalStateException(errorMsg)
          }

          groups += MergeGroup(partitionValues, groupFiles)
          logger.debug(s"MERGE DEBUG: ✓ Created merge group with ${currentGroup.length} files ($currentGroupSize bytes): ${currentGroup.map(_.path).mkString(", ")}")
        } else {
          logger.debug(
            s"MERGE DEBUG: ✗ Discarding single-file group: ${currentGroup.head.path} ($currentGroupSize bytes)"
          )
        }

        // Start new group
        currentGroup.clear()
        currentGroup += file
        currentGroupSize = file.size
        logger.debug(s"MERGE DEBUG: Started new group with ${file.path} (${file.size} bytes)")
      } else {
        // Add file to current group
        currentGroup += file
        currentGroupSize += file.size
        logger.debug(s"MERGE DEBUG: Added ${file.path} (${file.size} bytes) to current group. Group now has ${currentGroup.length} files ($currentGroupSize bytes total)")
      }
    }

    // Handle remaining group - only save if it has multiple files
    if (currentGroup.length > 1) {
      // FINAL VALIDATION: Ensure all files in the group have identical partition values
      val groupFiles        = currentGroup.toSeq
      val inconsistentFiles = groupFiles.filterNot(_.partitionValues == partitionValues)
      if (inconsistentFiles.nonEmpty) {
        val errorMsg = s"CRITICAL: Final group validation failed! Found files with inconsistent partitions:\n" +
          inconsistentFiles.map(f => s"  - ${f.path}: ${f.partitionValues} (expected: $partitionValues)").mkString("\n")
        logger.error(errorMsg)
        throw new IllegalStateException(errorMsg)
      }

      groups += MergeGroup(partitionValues, groupFiles)
      logger.debug(s"✓ Created final merge group with ${currentGroup.length} files ($currentGroupSize bytes): ${currentGroup.map(_.path).mkString(", ")}")
    } else if (currentGroup.length == 1) {
      logger.debug(s"✗ Discarding final single-file group: ${currentGroup.head.path} ($currentGroupSize bytes)")
    } else {
      logger.debug(s"No remaining group to process")
    }

    // CRITICAL: Final validation of all created groups
    groups.foreach { group =>
      val inconsistentFiles = group.files.filterNot(_.partitionValues == group.partitionValues)
      if (inconsistentFiles.nonEmpty) {
        val errorMsg = s"CRITICAL: Created group with inconsistent partition values!\n" +
          s"Group partition: ${group.partitionValues}\n" +
          "Inconsistent files:\n" + inconsistentFiles.map(f => s"  - ${f.path}: ${f.partitionValues}").mkString("\n")
        logger.error(errorMsg)
        throw new IllegalStateException(errorMsg)
      }
    }

    logger.debug(s"MERGE DEBUG: Created ${groups.length} merge groups from ${mergeableFiles.length} mergeable files")
    logger.info(s"✅ All ${groups.length} merge groups passed partition consistency validation")
    groups.toSeq
  }

  /**
   * Count the number of valid merge groups that would be created without performing the actual merge. This is used by
   * merge-on-write evaluation to determine if merge is worthwhile.
   *
   * This method uses the exact same logic as merge() to ensure consistency.
   *
   * @return
   *   Number of valid merge groups (with >= 2 files each)
   */
  def countMergeGroups(): Int =
    try {
      // Get current metadata
      val metadata = transactionLog.getMetadata()

      // Get current files from transaction log
      val allFiles = transactionLog.listFiles().sortBy(_.modificationTime)

      // Filter out files in cooldown period (if tracking enabled)
      val trackingEnabled = sparkSession.conf.get("spark.indextables.skippedFiles.trackingEnabled", "true").toBoolean
      val currentFiles = if (trackingEnabled) {
        transactionLog.filterFilesInCooldown(allFiles)
      } else {
        allFiles
      }

      // Group files by partition
      val partitionsToMerge = currentFiles.groupBy(_.partitionValues).toSeq

      // Apply partition predicates if specified
      val filteredPartitions = if (partitionPredicates.nonEmpty) {
        val partitionSchema = StructType(
          metadata.partitionColumns.map(name => StructField(name, StringType, nullable = true))
        )
        applyPartitionPredicates(partitionsToMerge, partitionSchema)
      } else {
        partitionsToMerge
      }

      // Get effective maxSourceSplitsPerMerge for counting
      val effectiveMaxSourceSplitsPerMerge = maxSourceSplitsPerMerge.orElse {
        sparkSession.conf.getOption(
          io.indextables.spark.config.IndexTables4SparkSQLConf.TANTIVY4SPARK_MERGE_MAX_SOURCE_SPLITS_PER_MERGE
        ).map(_.toInt)
      }.getOrElse(io.indextables.spark.config.IndexTables4SparkSQLConf.TANTIVY4SPARK_MERGE_MAX_SOURCE_SPLITS_PER_MERGE_DEFAULT)

      // Count merge groups in each partition
      val totalGroups = filteredPartitions.map {
        case (partitionValues, files) =>
          val groups = findMergeableGroups(partitionValues, files, effectiveMaxSourceSplitsPerMerge)
          // Filter to valid groups (>= 2 files)
          groups.count(_.files.length >= 2)
      }.sum

      totalGroups

    } catch {
      case e: Exception =>
        logger.warn(s"Failed to count merge groups: ${e.getMessage}")
        0
    }

  /**
   * Apply partition predicates to filter which partitions should be processed. Follows Delta Lake pattern of parsing
   * WHERE clause expressions.
   */
  private def applyPartitionPredicates(
    partitions: Seq[(Map[String, String], Seq[AddAction])],
    partitionSchema: StructType
  ): Seq[(Map[String, String], Seq[AddAction])] = {

    // If no partition columns are defined, reject any WHERE clauses
    if (partitionSchema.isEmpty && partitionPredicates.nonEmpty) {
      throw new IllegalArgumentException(
        s"WHERE clause not supported for non-partitioned tables. Partition predicates: ${partitionPredicates.mkString(", ")}"
      )
    }

    val parsedPredicates = partitionPredicates.flatMap { predicate =>
      try {
        val expression = sparkSession.sessionState.sqlParser.parseExpression(predicate)
        validatePartitionColumnReferences(expression, partitionSchema)
        Some(expression)
      } catch {
        case ex: Exception =>
          logger.error(s"Failed to parse partition predicate: $predicate", ex)
          throw new IllegalArgumentException(s"Invalid partition predicate: $predicate", ex)
      }
    }

    if (parsedPredicates.isEmpty) return partitions

    partitions.filter {
      case (partitionValues, _) =>
        val row = createRowFromPartitionValues(partitionValues, partitionSchema)
        parsedPredicates.forall { predicate =>
          try {
            // Resolve the expression against the partition schema before evaluation
            val resolvedPredicate = resolveExpression(predicate, partitionSchema)
            resolvedPredicate.eval(row).asInstanceOf[Boolean]
          } catch {
            case ex: Exception =>
              logger.error(s"Failed to evaluate predicate $predicate on partition $partitionValues", ex)
              false
          }
        }
    }
  }

  /** Validate that the expression only references partition columns. */
  private def validatePartitionColumnReferences(expression: Expression, partitionSchema: StructType): Unit = {
    val partitionColumns  = partitionSchema.fieldNames.toSet
    val referencedColumns = expression.references.map(_.name).toSet

    val invalidColumns = referencedColumns -- partitionColumns
    if (invalidColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"WHERE clause references non-partition columns: ${invalidColumns.mkString(", ")}. " +
          s"Only partition columns are allowed: ${partitionColumns.mkString(", ")}"
      )
    }
  }

  /** Create an InternalRow from partition values for predicate evaluation. */
  private def createRowFromPartitionValues(
    partitionValues: Map[String, String],
    partitionSchema: StructType
  ): InternalRow = {
    val values = partitionSchema.fieldNames.map { fieldName =>
      partitionValues.get(fieldName) match {
        case Some(value) => UTF8String.fromString(value)
        case None        => null
      }
    }
    InternalRow.fromSeq(values)
  }

  /**
   * Resolve an expression against a schema to handle UnresolvedAttribute references and cast literals to UTF8String.
   */
  private def resolveExpression(expression: Expression, schema: StructType): Expression =
    expression.transform {
      case unresolvedAttr: org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute =>
        val fieldName  = unresolvedAttr.name
        val fieldIndex = schema.fieldIndex(fieldName)
        val field      = schema(fieldIndex)
        org.apache.spark.sql.catalyst.expressions.BoundReference(fieldIndex, field.dataType, field.nullable)
      case literal: org.apache.spark.sql.catalyst.expressions.Literal =>
        // Cast all literals to UTF8String since partition values are stored as strings
        import org.apache.spark.sql.types._
        literal.dataType match {
          case StringType => literal
          case _          =>
            // Convert non-string literals to UTF8String for comparison with partition values
            org.apache.spark.sql.catalyst.expressions.Literal(UTF8String.fromString(literal.value.toString), StringType)
        }
    }
}

/** Companion object for MergeSplitsExecutor with static methods for distributed execution. */
object MergeSplitsExecutor {

  /**
   * Normalize Azure URLs to the azure:// scheme that tantivy4java expects. Handles wasb://, wasbs://, abfs://, abfss://
   * and converts them to azure://.
   */
  private def normalizeAzureUrl(url: String): String = {
    import io.indextables.spark.io.CloudStorageProviderFactory
    import org.apache.spark.sql.util.CaseInsensitiveStringMap
    import scala.jdk.CollectionConverters._

    CloudStorageProviderFactory.normalizePathForTantivy(
      url,
      new CaseInsensitiveStringMap(Map.empty[String, String].asJava),
      new org.apache.hadoop.conf.Configuration()
    )
  }

  /**
   * Execute merge for a single group of splits in executor context. This static method is designed to run on Spark
   * executors and handles serialization properly.
   */
  def executeMergeGroupDistributed(
    mergeGroup: MergeGroup,
    tablePathStr: String,
    awsConfig: SerializableAwsConfig,
    azureConfig: SerializableAzureConfig
  ): MergeResult = {
    val startTime = System.currentTimeMillis()
    val logger    = LoggerFactory.getLogger(classOf[MergeSplitsExecutor])

    logger.info(s"[EXECUTOR] Merging ${mergeGroup.files.length} splits in partition ${mergeGroup.partitionValues}")

    try {
      // Create merged split using physical merge (executor-friendly version)
      val mergedSplit = createMergedSplitDistributed(mergeGroup, tablePathStr, awsConfig, azureConfig)

      // Return result for driver to handle transaction operations
      // Note: We don't do transactionLog operations here since those must be done on driver
      val originalSize = mergeGroup.files.map(_.size).sum
      val mergedSize   = mergedSplit.size
      val mergedFiles  = mergeGroup.files.length

      logger.info(
        s"[EXECUTOR] Successfully merged $mergedFiles files ($originalSize bytes) into 1 split ($mergedSize bytes)"
      )

      MergeResult(
        mergeGroup = mergeGroup,
        mergedSplitInfo = mergedSplit,
        mergedFiles = mergedFiles,
        originalSize = originalSize,
        mergedSize = mergedSize,
        executionTimeMs = System.currentTimeMillis() - startTime
      )
    } catch {
      case ex: Exception =>
        logger.error(s"[EXECUTOR] Failed to merge group in partition ${mergeGroup.partitionValues}", ex)
        throw ex
    }
  }

  /**
   * Create a new merged split in executor context using tantivy4java. This static method uses broadcast configuration
   * parameters for executor-safe operation.
   */
  private def createMergedSplitDistributed(
    mergeGroup: MergeGroup,
    tablePathStr: String,
    awsConfig: SerializableAwsConfig,
    azureConfig: SerializableAzureConfig
  ): MergedSplitInfo = {
    val logger = LoggerFactory.getLogger(classOf[MergeSplitsExecutor])

    // Validate group has at least 2 files (required by tantivy4java)
    if (mergeGroup.files.length < 2) {
      throw new IllegalArgumentException(
        s"Cannot merge group with ${mergeGroup.files.length} files - at least 2 required"
      )
    }

    // CRITICAL: Validate all files in the merge group are from the same partition
    val groupPartitionValues = mergeGroup.partitionValues
    val invalidFiles         = mergeGroup.files.filterNot(_.partitionValues == groupPartitionValues)
    if (invalidFiles.nonEmpty) {
      val errorMsg =
        s"Cross-partition merge detected! Group expects partition $groupPartitionValues but found files from different partitions:\n" +
          invalidFiles.map(f => s"  - ${f.path}: ${f.partitionValues}").mkString("\n") +
          s"\nAll files in group:\n" +
          mergeGroup.files.map(f => s"  - ${f.path}: ${f.partitionValues}").mkString("\n")
      logger.error(errorMsg)
      throw new IllegalStateException(errorMsg)
    }

    logger.info(
      s"✅ Partition validation passed: All ${mergeGroup.files.length} files belong to partition $groupPartitionValues"
    )

    // Generate new split path with UUID for uniqueness
    val uuid = java.util.UUID.randomUUID().toString
    val partitionPath =
      if (mergeGroup.partitionValues.isEmpty) ""
      else {
        mergeGroup.partitionValues.map { case (k, v) => s"$k=$v" }.mkString("/") + "/"
      }
    val mergedPath = s"$partitionPath$uuid.split"

    // Create full paths for input splits and output split
    // Handle S3 and Azure paths specially to preserve proper schemes for tantivy4java
    val isS3Path = tablePathStr.startsWith("s3://") || tablePathStr.startsWith("s3a://")
    val isAzurePath = tablePathStr.startsWith("azure://") || tablePathStr.startsWith("wasb://") ||
      tablePathStr.startsWith("wasbs://") || tablePathStr.startsWith("abfs://") ||
      tablePathStr.startsWith("abfss://")

    val inputSplitPaths = mergeGroup.files.map { file =>
      if (isS3Path) {
        // For S3 paths, handle cases where file.path might already be a full S3 URL
        if (file.path.startsWith("s3://") || file.path.startsWith("s3a://")) {
          // file.path is already a full S3 URL, just normalize the scheme
          val normalized = file.path.replaceFirst("^s3a://", "s3://")
          logger.warn(s"🔄 [EXECUTOR] Normalized full S3 path: ${file.path} -> $normalized")
          normalized
        } else {
          // file.path is relative, construct full URL with normalized scheme
          val normalizedBaseUri = tablePathStr.replaceFirst("^s3a://", "s3://").replaceAll("/$", "")
          val fullPath          = s"$normalizedBaseUri/${file.path}"
          logger.warn(s"🔄 [EXECUTOR] Constructed relative S3 path: ${file.path} -> $fullPath")
          fullPath
        }
      } else if (isAzurePath) {
        // For Azure paths, normalize to azure:// scheme for tantivy4java
        if (
          file.path.startsWith("azure://") || file.path.startsWith("wasb://") ||
          file.path.startsWith("wasbs://") || file.path.startsWith("abfs://") ||
          file.path.startsWith("abfss://")
        ) {
          // file.path is already a full Azure URL, normalize to azure://
          val normalized = normalizeAzureUrl(file.path)
          logger.info(s"🔄 [EXECUTOR] Normalized full Azure path: ${file.path} -> $normalized")
          normalized
        } else {
          // file.path is relative, construct full URL with normalized scheme
          val normalizedBaseUri = normalizeAzureUrl(tablePathStr).replaceAll("/$", "")
          val fullPath          = s"$normalizedBaseUri/${file.path}"
          logger.info(s"🔄 [EXECUTOR] Constructed relative Azure path: ${file.path} -> $fullPath")
          fullPath
        }
      } else {
        // For local/HDFS paths, use Path concatenation and extract raw path for tantivy4java
        val fullPath = new org.apache.hadoop.fs.Path(tablePathStr, file.path)
        // tantivy4java expects raw filesystem paths, not file: URIs
        fullPath.toUri.getPath
      }
    }.asJava

    val outputSplitPath = if (isS3Path) {
      // For S3 paths, construct the URL directly with s3:// normalization for tantivy4java compatibility
      val normalizedBaseUri = tablePathStr.replaceFirst("^s3a://", "s3://").replaceAll("/$", "")
      val outputPath        = s"$normalizedBaseUri/$mergedPath"
      logger.warn(s"🔄 [EXECUTOR] Normalized output path: $tablePathStr/$mergedPath -> $outputPath")
      outputPath
    } else if (isAzurePath) {
      // For Azure paths, construct the URL with azure:// normalization for tantivy4java compatibility
      val normalizedBaseUri = normalizeAzureUrl(tablePathStr).replaceAll("/$", "")
      val outputPath        = s"$normalizedBaseUri/$mergedPath"
      logger.warn(s"🔄 [EXECUTOR] Normalized Azure output path: $tablePathStr/$mergedPath -> $outputPath")
      outputPath
    } else {
      // For local/HDFS paths, extract raw path for tantivy4java (not file: URI)
      new org.apache.hadoop.fs.Path(tablePathStr, mergedPath).toUri.getPath
    }

    logger.info(s"[EXECUTOR] Merging ${inputSplitPaths.size()} splits into $outputSplitPath")
    logger.debug(s"[EXECUTOR] Input splits: ${inputSplitPaths.asScala.mkString(", ")}")

    logger.info("[EXECUTOR] Attempting to merge splits using Tantivy4Java merge functionality")

    // CRITICAL DEBUG: Check ALL source splits for their docMappingJson
    logger.debug(s"SOURCE SPLITS DEBUG: Checking docMappingJson from ${mergeGroup.files.length} source splits")
    mergeGroup.files.zipWithIndex.foreach {
      case (file, idx) =>
        file.docMappingJson match {
          case Some(json) =>
            logger.debug(s"SOURCE SPLIT[$idx]: ${file.path} HAS docMappingJson (${json.length} chars)")
            logger.debug(s"SOURCE SPLIT[$idx]: Content: $json")
          case None =>
            logger.error(s"❌ SOURCE SPLIT[$idx]: ${file.path} has NO docMappingJson!")
        }
    }

    // Extract docMappingJson from first file to preserve fast fields configuration
    val docMappingJson = mergeGroup.files.headOption
      .flatMap(_.docMappingJson)
      .getOrElse {
        val errorMsg =
          "[EXECUTOR] FATAL: No docMappingJson found in source splits - cannot preserve fast fields configuration during merge"
        logger.error(errorMsg)
        throw new IllegalStateException(errorMsg)
      }
    logger.warn(s"✅ MERGE INPUT: Using docMappingJson from source split[0]: $docMappingJson")

    // Create merge configuration with broadcast AWS and Azure credentials and temp directory
    val mergeConfigBuilder = QuickwitSplit.MergeConfig
      .builder()
      .indexUid("merged-index-uid")
      .sourceId("tantivy4spark")
      .nodeId("merge-node")
      .docMappingUid(docMappingJson) // extracted from source splits to preserve fast fields
      .partitionId(0L)
      .deleteQueries(java.util.Collections.emptyList[String]())
      .awsConfig(awsConfig.toQuickwitSplitAwsConfig(tablePathStr))
      .debugEnabled(awsConfig.debugEnabled)

    // Add optional Azure configuration if available
    val azureConf = azureConfig.toQuickwitSplitAzureConfig()
    if (azureConf != null) {
      mergeConfigBuilder.azureConfig(azureConf)
    }

    // Add optional temp directory path if available
    awsConfig.tempDirectoryPath.foreach(path => mergeConfigBuilder.tempDirectoryPath(path))

    // Add optional heap size if available
    Option(awsConfig.heapSize).foreach(heap => mergeConfigBuilder.heapSizeBytes(heap))

    val mergeConfig = mergeConfigBuilder.build()

    // Perform the actual merge using direct/in-process merge
    logger.warn(s"⚙️  [EXECUTOR] Executing direct merge with ${inputSplitPaths.size()} input paths")
    logger.warn(s"📁 [EXECUTOR] Input paths:")
    inputSplitPaths.asScala.zipWithIndex.foreach {
      case (path, idx) =>
        logger.warn(s"📁 [EXECUTOR]   [$idx]: $path")
    }
    logger.warn(s"📁 [EXECUTOR] Output path: $outputSplitPath")
    logger.warn(s"📁 [EXECUTOR] Relative path for transaction log: $mergedPath")
    logger.info(s"[EXECUTOR] Executing direct merge with ${inputSplitPaths.size()} input paths")

    val serializedMetadata =
      try
        awsConfig.executeMerge(inputSplitPaths, outputSplitPath, mergeConfig)
      catch {
        case ex: Exception =>
          println(
            s"💥 [EXECUTOR] CRITICAL: Direct merge threw exception: ${ex.getClass.getSimpleName}: ${ex.getMessage}"
          )
          logger.error(s"[EXECUTOR] Direct merge failed", ex)
          ex.printStackTrace()
          throw new RuntimeException(s"Direct merge operation failed: ${ex.getMessage}", ex)
      }

    logger.info(s"[EXECUTOR] Successfully merged splits: ${serializedMetadata.getNumDocs} documents, ${serializedMetadata.getUncompressedSizeBytes} bytes")
    logger.debug(s"[EXECUTOR] Merge metadata: split_id=${serializedMetadata.getSplitId}, merge_ops=${serializedMetadata.getNumMergeOps}")

    // CRITICAL: Verify the merged file actually exists at the expected location
    try
      if (isS3Path) {
        logger.debug(s"[EXECUTOR] S3 merge - cannot easily verify file existence in executor context")
        logger.debug(s"[EXECUTOR] Assuming tantivy4java successfully created: $outputSplitPath")
      } else if (isAzurePath) {
        logger.debug(s"[EXECUTOR] Azure merge - cannot easily verify file existence in executor context")
        logger.debug(s"[EXECUTOR] Assuming tantivy4java successfully created: $outputSplitPath")
      } else {
        // outputSplitPath is now a raw filesystem path (no file: URI prefix)
        val outputFile = new java.io.File(outputSplitPath)
        val exists     = outputFile.exists()
        logger.debug(s"[EXECUTOR] File verification: $outputSplitPath exists = $exists")
        if (!exists) {
          throw new RuntimeException(s"CRITICAL: Merged file was not created at expected location: $outputSplitPath")
        }
      }
    catch {
      case ex: Exception =>
        logger.warn(s"[EXECUTOR] File existence check failed", ex)
    }

    MergedSplitInfo(mergedPath, serializedMetadata.getUncompressedSizeBytes, serializedMetadata)
  }
}

/** Group of files that should be merged together. */
case class MergeGroup(
  partitionValues: Map[String, String],
  files: Seq[AddAction])
    extends Serializable

/** Result of merging a group of splits. */
case class MergeResult(
  mergeGroup: MergeGroup,
  mergedSplitInfo: MergedSplitInfo,
  mergedFiles: Int,
  originalSize: Long,
  mergedSize: Long,
  executionTimeMs: Long)
    extends Serializable {
  // Provide backward compatibility
  def mergedPath: String = mergedSplitInfo.path
}

/** Information about a newly created merged split. */
/** Serializable wrapper for QuickwitSplit.SplitMetadata */
case class SerializableSplitMetadata(
  footerStartOffset: Long,
  footerEndOffset: Long,
  hotcacheStartOffset: Long,
  hotcacheLength: Long,
  hasFooterOffsets: Boolean,
  timeRangeStart: Option[String],
  timeRangeEnd: Option[String],
  tags: Option[Map[String, String]],
  deleteOpstamp: Option[Long],
  numMergeOps: Option[Int],
  docMappingJson: Option[String],
  uncompressedSizeBytes: Long,
  // Additional fields needed for complete SplitMetadata
  splitId: Option[String] = None,
  numDocs: Option[Long] = None,
  skippedSplits: List[String] = List.empty,
  indexUid: Option[String] = None // Store indexUid to detect if merge was performed
) extends Serializable {

  def getFooterStartOffset(): Long   = footerStartOffset
  def getFooterEndOffset(): Long     = footerEndOffset
  def getHotcacheStartOffset(): Long = hotcacheStartOffset
  def getHotcacheLength(): Long      = hotcacheLength
  def getTimeRangeStart(): String    = timeRangeStart.orNull
  def getTimeRangeEnd(): String      = timeRangeEnd.orNull
  def getTags(): java.util.Set[String] =
    tags
      .map { tagMap =>
        import scala.jdk.CollectionConverters._
        tagMap.keySet.asJava
      }
      .getOrElse(java.util.Collections.emptySet())
  def getDeleteOpstamp(): Long         = deleteOpstamp.getOrElse(0L)
  def getNumMergeOps(): Int            = numMergeOps.getOrElse(0)
  def getDocMappingJson(): String      = docMappingJson.orNull
  def getUncompressedSizeBytes(): Long = uncompressedSizeBytes
  def getSplitId(): String             = splitId.orNull
  def getNumDocs(): Long               = numDocs.getOrElse(0L)
  def getSkippedSplits(): java.util.List[String] = {
    import scala.jdk.CollectionConverters._
    skippedSplits.asJava
  }
  def getIndexUid(): String = indexUid.orNull

  def toQuickwitSplitMetadata(): io.indextables.tantivy4java.split.merge.QuickwitSplit.SplitMetadata = {
    import scala.jdk.CollectionConverters._
    new io.indextables.tantivy4java.split.merge.QuickwitSplit.SplitMetadata(
      splitId.getOrElse("unknown"),                                          // splitId
      "tantivy4spark-index",                                                 // indexUid (NEW - required)
      0L,                                                                    // partitionId (NEW - required)
      "tantivy4spark-source",                                                // sourceId (NEW - required)
      "tantivy4spark-node",                                                  // nodeId (NEW - required)
      numDocs.getOrElse(0L),                                                 // numDocs
      uncompressedSizeBytes,                                                 // uncompressedSizeBytes
      timeRangeStart.map(java.time.Instant.parse).orNull,                    // timeRangeStart
      timeRangeEnd.map(java.time.Instant.parse).orNull,                      // timeRangeEnd
      System.currentTimeMillis() / 1000,                                     // createTimestamp (NEW - required)
      "Mature",                                                              // maturity (NEW - required)
      tags.map(_.keySet.asJava).getOrElse(java.util.Collections.emptySet()), // tags
      footerStartOffset,                                                     // footerStartOffset
      footerEndOffset,                                                       // footerEndOffset
      deleteOpstamp.getOrElse(0L),                                           // deleteOpstamp
      numMergeOps.getOrElse(0),                                              // numMergeOps
      "doc-mapping-uid",                                                     // docMappingUid (NEW - required)
      docMappingJson.orNull,                                                 // docMappingJson (MOVED - for performance)
      skippedSplits.asJava                                                   // skippedSplits
    )
  }
}

object SerializableSplitMetadata {
  def fromQuickwitSplitMetadata(metadata: io.indextables.tantivy4java.split.merge.QuickwitSplit.SplitMetadata)
    : SerializableSplitMetadata = {
    val timeStart = Option(metadata.getTimeRangeStart()).map(_.toString)
    val timeEnd   = Option(metadata.getTimeRangeEnd()).map(_.toString)
    val tags = Option(metadata.getTags()).filter(!_.isEmpty).map { tagSet =>
      import scala.jdk.CollectionConverters._
      tagSet.asScala.map(_ -> "").toMap // Convert Set to Map with empty values
    }
    val docMapping = Option(metadata.getDocMappingJson())
    val skippedSplitsList = Option(metadata.getSkippedSplits()) match {
      case Some(splits) =>
        import scala.jdk.CollectionConverters._
        splits.asScala.toList
      case None => List.empty[String]
    }

    SerializableSplitMetadata(
      metadata.getFooterStartOffset(),
      metadata.getFooterEndOffset(),
      0L, // hotcacheStartOffset - deprecated, using footer offsets instead
      0L, // hotcacheLength - deprecated, using footer offsets instead
      metadata.hasFooterOffsets(),
      timeStart,
      timeEnd,
      tags,
      Some(metadata.getDeleteOpstamp()),
      Some(metadata.getNumMergeOps().toInt),
      docMapping,
      metadata.getUncompressedSizeBytes(),
      splitId = Option(metadata.getSplitId()).filter(_.nonEmpty),
      numDocs = Some(metadata.getNumDocs()),
      skippedSplits = skippedSplitsList,
      indexUid = Option(metadata.getIndexUid()).filter(_.nonEmpty) // Extract indexUid to detect successful merge
    )
  }
}

case class MergedSplitInfo(
  path: String,
  size: Long,
  metadata: SerializableSplitMetadata)
    extends Serializable

/** Placeholder for unresolved table path or identifier. Similar to Delta Lake's UnresolvedDeltaPathOrIdentifier. */
case class UnresolvedDeltaPathOrIdentifier(
  path: Option[String],
  tableIdentifier: Option[org.apache.spark.sql.catalyst.TableIdentifier],
  commandName: String)
    extends org.apache.spark.sql.catalyst.plans.logical.LeafNode {
  override def output: Seq[Attribute] = Nil
}
