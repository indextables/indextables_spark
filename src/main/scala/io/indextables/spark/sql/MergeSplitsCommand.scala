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

import io.indextables.spark.transaction.{
  AddAction,
  PartitionPredicateUtils,
  RemoveAction,
  TransactionLog,
  TransactionLogFactory
}
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

    // Extract and merge configuration with proper precedence for transaction log access
    val hadoopConf         = sparkSession.sparkContext.hadoopConfiguration
    val sparkConfigs       = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
    val hadoopConfigs      = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    val txLogMergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

    // Create transaction log - CloudStorageProvider will handle credential resolution
    // with proper refresh logic via V1ToV2CredentialsProviderAdapter
    import scala.jdk.CollectionConverters._
    val transactionLog =
      TransactionLogFactory.create(tablePath, sparkSession, new CaseInsensitiveStringMap(txLogMergedConfigs.asJava))

    // Invalidate cache to ensure fresh read of transaction log state for merge planning
    transactionLog.invalidateCache()

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
          // Exponential backoff before retry (1s, 2s, ...)
          val delayMs = 1000L * math.pow(2, attempt - 1).toLong
          logger.warn(s"Retrying $operationDesc in ${delayMs}ms")
          Thread.sleep(delayMs)
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
/**
 * Simplified AWS configuration for merge operations. Holds merged config map and resolves credentials on executor using
 * CredentialProviderFactory.
 */
case class SerializableAwsConfig(
  configs: Map[String, String],
  tablePath: String,
  tempDirectoryPath: Option[String] = None,
  heapSize: java.lang.Long = java.lang.Long.valueOf(1073741824L),
  debugEnabled: Boolean = false)
    extends Serializable {

  private def getConfig(key: String): Option[String] =
    configs.get(key).orElse(configs.get(key.toLowerCase))

  def accessKey: String            = getConfig("spark.indextables.aws.accessKey").getOrElse("")
  def secretKey: String            = getConfig("spark.indextables.aws.secretKey").getOrElse("")
  def sessionToken: Option[String] = getConfig("spark.indextables.aws.sessionToken")
  def region: String               = getConfig("spark.indextables.aws.region").getOrElse("us-east-1")
  def endpoint: Option[String]     = getConfig("spark.indextables.s3.endpoint")
  def pathStyleAccess: Boolean = getConfig("spark.indextables.s3.pathStyleAccess").exists(_.equalsIgnoreCase("true"))

  /**
   * Resolve AWS credentials using centralized CredentialProviderFactory. Priority: explicit credentials > custom
   * provider class > default chain (returns None)
   */
  private def resolveAWSCredentials(): (Option[String], Option[String], Option[String]) = {
    import io.indextables.spark.utils.CredentialProviderFactory

    CredentialProviderFactory.resolveAWSCredentialsFromConfig(configs, tablePath) match {
      case Some(creds) => (Some(creds.accessKey), Some(creds.secretKey), creds.sessionToken)
      case None        => (None, None, None)
    }
  }

  /** Convert to tantivy4java AwsConfig instance. Resolves credentials on executor if needed. */
  def toQuickwitSplitAwsConfig(tablePathStr: String): QuickwitSplit.AwsConfig = {
    val (resolvedAccessKey, resolvedSecretKey, resolvedSessionToken) = resolveAWSCredentials()
    new QuickwitSplit.AwsConfig(
      resolvedAccessKey.orNull,
      resolvedSecretKey.orNull,
      resolvedSessionToken.orNull,
      region,
      endpoint.orNull,
      pathStyleAccess
    )
  }

  /** Execute merge operation using direct/in-process merge. */
  def executeMerge(
    inputSplitPaths: java.util.List[String],
    outputSplitPath: String,
    mergeConfig: QuickwitSplit.MergeConfig
  ): SerializableSplitMetadata = {
    val logger = LoggerFactory.getLogger(this.getClass)
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
  overrideOptions: Option[Map[String, String]] = None,
  batchSizeOverride: Option[Int] = None,
  maxConcurrentBatchesOverride: Option[Int] = None) {

  private val logger = LoggerFactory.getLogger(classOf[MergeSplitsExecutor])

  /**
   * Threshold for skipping already-large splits from merge consideration. Splits >= this size are excluded from merge
   * groups to avoid merging files that are already close to target. Default: 45% of target size. Configurable via
   * spark.indextables.merge.skipSplitThreshold (0.0 to 1.0).
   */
  private val skipSplitThresholdPercent: Double = sparkSession.conf
    .getOption("spark.indextables.merge.skipSplitThreshold")
    .map(_.toDouble)
    .getOrElse(0.45)

  private val skipSplitThreshold: Long = (targetSize * skipSplitThresholdPercent).toLong

  logger.info(
    s"Skip split threshold: $skipSplitThreshold bytes (${(skipSplitThresholdPercent * 100).toInt}% of target size $targetSize)"
  )

  /**
   * Maximum number of merge passes to execute in a single call. Each pass finds merge groups based on the current
   * transaction log state and executes them. The loop continues until no more merge groups are found or maxPasses is
   * reached. Default: 10.
   */
  private val maxPasses: Int = sparkSession.conf
    .getOption("spark.indextables.merge.maxPasses")
    .map(_.toInt)
    .getOrElse(10)

  logger.info(s"Max merge passes: $maxPasses")

  /**
   * Extract AWS configuration from SparkSession for merge operations. Merges configs from: overrideOptions >
   * sparkSession > hadoopConfiguration Credentials are resolved on executor using CredentialProviderFactory (same as
   * S3CloudStorageProvider).
   */
  private def extractAwsConfig(): SerializableAwsConfig = {
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

    // Merge configs: hadoop < spark < overrideOptions (highest priority)
    val sparkConfigs  = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
    val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    val baseConfigs   = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)
    val mergedConfigs = overrideOptions match {
      case Some(overrides) => baseConfigs ++ overrides
      case None            => baseConfigs
    }

    // Helper to get config with case-insensitive fallback
    def getConfig(key: String): Option[String] =
      mergedConfigs.get(key).orElse(mergedConfigs.get(key.toLowerCase))

    // Extract temp directory with fallback to /local_disk0 if available
    val tempDirectoryPath = getConfig("spark.indextables.merge.tempDirectoryPath")
      .orElse(if (MergeSplitsCommand.isLocalDisk0Available()) Some("/local_disk0/tantivy4spark-temp") else None)

    // Extract heap size (default 1GB)
    val heapSize = getConfig("spark.indextables.merge.heapSize")
      .map(io.indextables.spark.util.SizeParser.parseSize)
      .map(java.lang.Long.valueOf)
      .getOrElse(java.lang.Long.valueOf(1073741824L))

    val debugEnabled = getConfig("spark.indextables.merge.debug")
      .exists(v => v.equalsIgnoreCase("true") || v == "1")

    logger.info(s"Merge config: ${mergedConfigs.size} entries, tempDir=${tempDirectoryPath.getOrElse("default")}")

    SerializableAwsConfig(
      configs = mergedConfigs + ("spark.indextables.databricks.credential.operation" -> "PATH_READ_WRITE"),
      tablePath = tablePath.toString,
      tempDirectoryPath = tempDirectoryPath,
      heapSize = heapSize,
      debugEnabled = debugEnabled
    )
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
      case ex: NoSuchElementException =>
        // A required config key was not found — Azure is optional so return empty config
        logger.debug(s"Azure config key not found, returning empty config: ${ex.getMessage}")
        SerializableAzureConfig(None, None, None, None, None, None, None, None)
      case ex: Exception =>
        logger.warn(s"Unexpected error extracting Azure config from Spark session: ${ex.getMessage}", ex)
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

    // Log metadata details (avoid logging full configuration as it may contain large schema registry)
    logger.debug(s"MERGE DEBUG: Retrieved metadata - ID: ${metadata.id}, partition columns: ${metadata.partitionColumns.mkString(",")}")
    if (logger.isDebugEnabled) {
      logger.debug(s"MERGE DEBUG: Configuration has ${metadata.configuration.size} entries")
    }

    val partitionSchema = StructType(
      metadata.partitionColumns.map(name => StructField(name, StringType, nullable = true))
    )

    logger.debug(s"MERGE DEBUG: Constructed partition schema: ${partitionSchema.fieldNames.mkString(", ")}")

    // If no partition columns are defined in metadata, skip partition validation
    if (metadata.partitionColumns.isEmpty) {
      logger.info("No partition columns defined in metadata - treating table as non-partitioned")
    } else {
      logger.info(
        s"Found ${metadata.partitionColumns.size} partition columns: ${metadata.partitionColumns.mkString(", ")}"
      )
    }

    // Extract AWS and Azure configuration early for use across all passes
    val awsConfig   = extractAwsConfig()
    val azureConfig = extractAzureConfig()

    // Get effective maxSourceSplitsPerMerge (from parameter or config default)
    val effectiveMaxSourceSplitsPerMerge = maxSourceSplitsPerMerge
      .orElse {
        sparkSession.conf
          .getOption(
            io.indextables.spark.config.IndexTables4SparkSQLConf.TANTIVY4SPARK_MERGE_MAX_SOURCE_SPLITS_PER_MERGE
          )
          .map(_.toInt)
      }
      .getOrElse(
        io.indextables.spark.config.IndexTables4SparkSQLConf.TANTIVY4SPARK_MERGE_MAX_SOURCE_SPLITS_PER_MERGE_DEFAULT
      )

    logger.info(s"Using MAX SOURCE SPLITS PER MERGE: $effectiveMaxSourceSplitsPerMerge")

    // === MULTI-GENERATION MERGE LOOP ===
    // Execute merge generations until no more work is found.
    // Each iteration: read fresh files from txlog, plan one generation, execute it.

    // Accumulator variables for tracking total results across all generations
    var totalMergedFilesAccum       = 0
    var totalMergeGroupsAccum       = 0
    var totalOriginalSizeAccum      = 0L
    var totalMergedSizeAccum        = 0L
    var totalSuccessfulBatchesAccum = 0
    var totalFailedBatchesAccum     = 0
    var totalBatchesAccum           = 0
    var currentGeneration           = 0
    var totalGenerations            = 0
    var hasMoreWork                 = true

    // Parse partition predicates once (doesn't change between generations)
    // Parse partition predicates and convert to Spark Filters for Avro manifest pruning
    val (parsedPredicates, partitionFilters) = if (partitionPredicates.nonEmpty && metadata.partitionColumns.nonEmpty) {
      try {
        val parsed = PartitionPredicateUtils.parseAndValidatePredicates(
          partitionPredicates,
          partitionSchema,
          sparkSession
        )
        val filters = PartitionPredicateUtils.expressionsToFilters(parsed)
        logger.info(s"Converted ${filters.length} of ${parsed.length} predicates to Spark Filters for manifest pruning")
        (parsed, filters)
      } catch {
        case e: IllegalArgumentException =>
          logger.warn(s"Failed to parse partition predicates for manifest pruning: ${e.getMessage}")
          (Seq.empty, Seq.empty)
      }
    } else {
      (Seq.empty, Seq.empty)
    }

    // === GENERATION LOOP: Execute until no more merge groups are found ===
    while (hasMoreWork && currentGeneration < maxPasses) {
      currentGeneration += 1
      logger.info(s"=== Starting merge generation $currentGeneration ===")

      // Invalidate transaction log cache to get fresh state after previous generation
      if (currentGeneration > 1) {
        transactionLog.invalidateCache()
      }

      // Get current files from transaction log with manifest-level pruning if filters are available
      val allFiles = if (partitionFilters.nonEmpty) {
        val files = transactionLog.listFilesWithPartitionFilters(partitionFilters).sortBy(_.modificationTime)
        logger.info(s"Found ${files.length} split files using Avro manifest pruning")
        files
      } else {
        val files = transactionLog.listFiles().sortBy(_.modificationTime)
        logger.info(s"Found ${files.length} split files in transaction log")
        files
      }

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
      if (logger.isDebugEnabled) {
        currentFiles.foreach { file =>
          logger.debug(s"  File: ${file.path} (${file.size} bytes, partition: ${file.partitionValues})")
        }
      }

      // Apply partition predicates if specified to filter files before planning
      val filesToPlan = if (partitionPredicates.nonEmpty) {
        val partitionsToMerge  = currentFiles.groupBy(_.partitionValues).toSeq
        val filteredPartitions = applyPartitionPredicates(partitionsToMerge, partitionSchema)
        filteredPartitions.flatMap(_._2)
      } else {
        currentFiles
      }

      logger.info(s"Planning merge groups from ${filesToPlan.length} files across ${filesToPlan.map(_.partitionValues).distinct.length} partitions")

      // Plan ALL merge groups upfront (simulates multi-generation merges)
      val (allMergeGroups, generationCount) = planAllMergeGroups(
        filesToPlan,
        metadata.partitionColumns,
        effectiveMaxSourceSplitsPerMerge
      )

      logger.info(s"Planned ${allMergeGroups.length} merge groups for generation $currentGeneration (estimated $generationCount total generations)")

      // If no merge groups found in this generation, we're done
      if (allMergeGroups.isEmpty) {
        logger.info(s"Generation $currentGeneration: No merge groups found - all splits are optimal size")
        hasMoreWork = false
        // For generation 1, this means no work at all - will be handled after the loop
        // For generation 2+, this means we've completed all generations
      } else {
        // Continue with execution for this generation

        // Sort ALL merge groups by:
        // 1. Primary: source split count (descending) - prioritize high-impact merges
        // 2. Secondary: oldest modification time (ascending) - tie-break with oldest first
        val prioritizedMergeGroups = allMergeGroups.sortBy { g =>
          val sourceCount = -g.files.length                     // Negative for descending order
          val oldestTime  = g.files.map(_.modificationTime).min // Ascending (oldest first for ties)
          (sourceCount, oldestTime)
        }
        logger.info(s"Prioritized merge groups by source split count: ${prioritizedMergeGroups
            .map(_.files.length)
            .take(10)
            .mkString(", ")}${if (prioritizedMergeGroups.length > 10) "..." else ""}")

        // Apply MAX DEST SPLITS limit if specified
        val limitedMergeGroups = maxDestSplits match {
          case Some(maxLimit) if prioritizedMergeGroups.length > maxLimit =>
            logger.info(s"Limiting merge operation to $maxLimit highest-impact dest splits (out of ${prioritizedMergeGroups.length} total)")
            val limitedGroups     = prioritizedMergeGroups.take(maxLimit)
            val limitedFilesCount = limitedGroups.map(_.files.length).sum
            val skippedGroups     = prioritizedMergeGroups.drop(maxLimit)
            logger.info(
              s"MAX DEST SPLITS limit applied: processing $limitedFilesCount files from $maxLimit highest-impact groups " +
                s"(source splits range: ${limitedGroups.map(_.files.length).min}-${limitedGroups.map(_.files.length).max})"
            )
            if (skippedGroups.nonEmpty) {
              logger.info(
                s"Skipping ${skippedGroups.length} lower-impact groups with ${skippedGroups.map(_.files.length).sum} files"
              )
            }
            limitedGroups
          case Some(maxLimit) =>
            logger.info(
              s"MAX DEST SPLITS limit of $maxLimit not reached (found ${prioritizedMergeGroups.length} groups)"
            )
            prioritizedMergeGroups
          case None =>
            logger.debug("No MAX DEST SPLITS limit specified")
            prioritizedMergeGroups
        }

        // Final safety check: ensure no single-file groups exist
        val singleFileGroups = limitedMergeGroups.filter(_.files.length < 2)
        if (singleFileGroups.nonEmpty) {
          logger.error(
            s"CRITICAL: Found ${singleFileGroups.length} single-file groups that should have been filtered out!"
          )
          throw new IllegalStateException(s"Internal error: Found ${singleFileGroups.length} single-file merge groups")
        }

        // === SINGLE EXECUTION PHASE: Execute all batches with global numbering ===

        // Get batch configuration — prefer constructor overrides (from async manager) over SparkSession conf
        val defaultParallelism = sparkSession.sparkContext.defaultParallelism
        val batchSize = batchSizeOverride.getOrElse(
          sparkSession.conf
            .getOption("spark.indextables.merge.batchSize")
            .map(_.toInt)
            .getOrElse(defaultParallelism)
        )

        val maxConcurrentBatches = maxConcurrentBatchesOverride.getOrElse(
          sparkSession.conf
            .getOption("spark.indextables.merge.maxConcurrentBatches")
            .map(_.toInt)
            .getOrElse(2)
        )

        logger.info(s"Batch configuration: batchSize=$batchSize (defaultParallelism=$defaultParallelism), maxConcurrentBatches=$maxConcurrentBatches")

        // Split merge groups into batches (global batch numbering)
        val batches = limitedMergeGroups.grouped(batchSize).toSeq
        logger.info(s"Split ${limitedMergeGroups.length} merge groups into ${batches.length} batches")

        batches.zipWithIndex.foreach {
          case (batch, idx) =>
            logger.info(s"Batch ${idx + 1}/${batches.length}: ${batch.length} merge groups")
        }

        // Broadcast Azure configuration and table path
        val broadcastAzureConfig = sparkSession.sparkContext.broadcast(azureConfig)
        val broadcastTablePath   = sparkSession.sparkContext.broadcast(tablePath.toString)

        // Process batches with controlled concurrency using Scala parallel collections
        import scala.collection.parallel.ForkJoinTaskSupport
        import java.util.concurrent.ForkJoinPool
        import scala.util.{Try, Success, Failure}

        val batchResults      = batches.zipWithIndex.par
        val customTaskSupport = new ForkJoinTaskSupport(new ForkJoinPool(maxConcurrentBatches))
        batchResults.tasksupport = customTaskSupport

        // Track batch success/failure using AtomicInteger for thread-safe increments in parallel collections
        val failedBatchCount     = new java.util.concurrent.atomic.AtomicInteger(0)
        val successfulBatchCount = new java.util.concurrent.atomic.AtomicInteger(0)
        val totalBatches         = batches.length

        val allResults =
          try {
            batchResults.flatMap {
              case (batch, batchIdx) =>
                Try {
                  // Global batch numbering (no per-pass reset)
                  val batchNum    = batchIdx + 1
                  val totalSplits = batch.map(_.files.length).sum
                  val totalSizeGB = batch.map(_.files.map(_.size).sum).sum / (1024.0 * 1024.0 * 1024.0)

                  logger.info(s"Starting batch $batchNum/${batches.length}: ${batch.length} merge groups, $totalSplits splits, $totalSizeGB%.2f GB")

                  // Refresh AWS credentials for this batch (credentials may have expired)
                  // This calls the credential provider on the driver to get fresh credentials
                  logger.info(s"Refreshing AWS credentials for batch $batchNum")
                  val batchAwsConfig     = extractAwsConfig()
                  val broadcastAwsConfig = sparkSession.sparkContext.broadcast(batchAwsConfig)

                  // Set descriptive names for Spark UI
                  val jobGroup = s"tantivy4spark-merge-splits-batch-$batchNum"
                  val jobDescription =
                    f"MERGE SPLITS Batch $batchNum/${batches.length}: $totalSplits splits merging to ${batch.length} splits ($totalSizeGB%.2f GB)"
                  val stageName =
                    f"Merge Batch $batchNum/${batches.length}: ${batch.length} groups, $totalSplits splits"

                  sparkSession.sparkContext.setJobGroup(jobGroup, jobDescription, interruptOnCancel = true)

                  // Set scheduler pool for FAIR scheduling (if enabled)
                  // With spark.scheduler.mode=FAIR, the root pool uses FAIR scheduling between pools
                  // while dynamically-created pools use FIFO within themselves.
                  //
                  // We use separate pools for writes and merges:
                  // - Writes should use "indextables-write" pool (user sets via setLocalProperty)
                  // - Merges use "indextables-merge" pool (automatic)
                  //
                  // This gives each operation type a fair share of cluster resources.
                  // No XML config needed - just spark.scheduler.mode=FAIR.
                  val schedulerPool = sparkSession.conf
                    .getOption("spark.indextables.merge.schedulerPool")
                    .getOrElse("indextables-merge")
                  sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", schedulerPool)

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
                      sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", null)
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

                  val perGroupResults = physicalMergeResults.map { result =>
                    val startTime = System.currentTimeMillis()

                    logger.info(
                      s"Processing transaction log for merge group with ${result.mergeGroup.files.length} files"
                    )

                    // Check if merge was actually performed by examining indexUid in metadata
                    val mergedMetadata = result.mergedSplitInfo.metadata
                    val indexUid       = mergedMetadata.indexUid

                    // Extract skipped splits from the merge result to avoid marking them as removed
                    val skippedSplitPaths = Option(result.mergedSplitInfo.metadata.getSkippedSplits())
                      .map(_.asScala.toSet)
                      .getOrElse(Set.empty[String])

                    // Always record skipped files regardless of whether merge was performed
                    if (skippedSplitPaths.nonEmpty) {
                      logger.info(s"Merge operation skipped ${skippedSplitPaths.size} files (due to corruption/missing files): ${skippedSplitPaths.mkString(", ")}")

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
                      logger.info(s"No merge was performed for group with ${result.mergeGroup.files.length} files (null/empty indexUid) - skipping ADD/REMOVE operations but preserving skipped files tracking")

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
                            logger.info(
                              s"Preserving skipped file in transaction log (not marking as removed): ${file.path}"
                            )
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

                          docMapping match {
                            case Some(json) =>
                              logger.debug(
                                s"docMappingJson extracted from merged split (${json.length} chars)"
                              )
                            case None =>
                              logger.debug(
                                s"No docMappingJson in merged split metadata"
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

                      if (docMappingJson.isEmpty) {
                        logger.warn(s"AddAction has no docMappingJson - fast fields may not be preserved for this split")
                      }

                      // Merge companion fields from all source splits
                      val mergedCompanionSourceFiles = {
                        val allSources = result.mergeGroup.files
                          .flatMap(_.companionSourceFiles.getOrElse(Seq.empty))
                          .distinct
                        if (allSources.nonEmpty) Some(allSources) else None
                      }
                      val mergedCompanionDeltaVersion = {
                        val versions = result.mergeGroup.files.flatMap(_.companionDeltaVersion)
                        if (versions.nonEmpty) Some(versions.max) else None
                      }
                      val mergedCompanionFastFieldMode = result.mergeGroup.files
                        .flatMap(_.companionFastFieldMode)
                        .headOption

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
                        uncompressedSizeBytes = uncompressedSizeBytes,
                        // Companion mode fields (combined from all source splits)
                        companionSourceFiles = mergedCompanionSourceFiles,
                        companionDeltaVersion = mergedCompanionDeltaVersion,
                        companionFastFieldMode = mergedCompanionFastFieldMode
                      )

                      // Collect actions for batch commit instead of committing individually
                      batchRemoveActions ++= removeActions
                      batchAddActions += addAction

                      logger.info(
                        s"[Batch $batchNum] Prepared transaction actions: ${removeActions.length} removes, 1 add"
                      )

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

                  successfulBatchCount.incrementAndGet()

                  // Return results from this batch
                  perGroupResults
                } match {
                  case Success(results) => results
                  case Failure(ex) =>
                    failedBatchCount.incrementAndGet()
                    val batchNum = batchIdx + 1
                    logger.error(s"[Batch $batchNum] Failed to process batch", ex)
                    Seq.empty // Return empty sequence for failed batches
                }
            }.toList
          } finally {
            customTaskSupport.environment.shutdown()
            try { broadcastAzureConfig.destroy() } catch { case _: Exception => }
            try { broadcastTablePath.destroy() } catch { case _: Exception => }
          }

        // Aggregate results from this generation's batches into accumulators
        val genMergedFiles  = allResults.map(_.mergedFiles).sum
        val genMergeGroups  = allResults.length
        val genOriginalSize = allResults.map(_.originalSize).sum
        val genMergedSize   = allResults.map(_.mergedSize).sum

        totalMergedFilesAccum += genMergedFiles
        totalMergeGroupsAccum += genMergeGroups
        totalOriginalSizeAccum += genOriginalSize
        totalMergedSizeAccum += genMergedSize
        totalSuccessfulBatchesAccum += successfulBatchCount.get()
        totalFailedBatchesAccum += failedBatchCount.get()
        totalBatchesAccum += totalBatches
        totalGenerations = currentGeneration

        // H4: Detect when all batches failed to provide a clear error rather than silent partial/no-action result
        if (failedBatchCount.get() > 0 && successfulBatchCount.get() == 0) {
          throw new RuntimeException(
            s"All ${failedBatchCount.get()} merge batches failed in generation $currentGeneration. Check executor logs for details."
          )
        }

        logger.info(
          s"Generation $currentGeneration completed: merged $genMergedFiles files into $genMergeGroups new splits"
        )
        logger.info(s"Generation $currentGeneration batches: $totalBatches, successful: ${successfulBatchCount.get()}, failed: ${failedBatchCount.get()}")
        logger.info(s"Generation $currentGeneration size change: $genOriginalSize bytes -> $genMergedSize bytes")

        // Check if we need to continue (more generations might be needed)
        // If this generation produced merged files, there might be more work in the next generation
        if (genMergedFiles == 0) {
          hasMoreWork = false
          logger.info(s"Generation $currentGeneration produced no merges, stopping")
        } else {
          logger.info(s"Generation $currentGeneration produced $genMergeGroups merged splits, checking for more work...")
        }

      } // End of else block (allMergeGroups.nonEmpty)
    }   // End of generation while loop

    // Final result aggregation
    logger.info(s"MERGE SPLITS completed: merged $totalMergedFilesAccum files into $totalMergeGroupsAccum new splits across $totalGenerations merge generations")
    logger.info(
      s"Total batches: $totalBatchesAccum, successful: $totalSuccessfulBatchesAccum, failed: $totalFailedBatchesAccum"
    )
    logger.info(s"Size change: $totalOriginalSizeAccum bytes -> $totalMergedSizeAccum bytes")

    val status =
      if (totalFailedBatchesAccum == 0 && totalMergedFilesAccum > 0) "success"
      else if (totalMergedFilesAccum == 0) "no_action"
      else "partial_success"

    if (totalMergedFilesAccum == 0) {
      Seq(
        Row(
          tablePath.toString,
          Row("no_action", null, null, null, null, "All splits are already optimal size"),
          awsConfig.tempDirectoryPath.getOrElse(null),
          if (awsConfig.heapSize == null) null else awsConfig.heapSize.asInstanceOf[Long]
        )
      )
    } else {
      Seq(
        Row(
          tablePath.toString,
          Row(
            status,
            totalMergedFilesAccum.asInstanceOf[Long],
            totalMergeGroupsAccum.asInstanceOf[Long],
            totalOriginalSizeAccum,
            totalMergedSizeAccum,
            s"batches: $totalBatchesAccum, successful: $totalSuccessfulBatchesAccum, failed: $totalFailedBatchesAccum, merge_generations: $totalGenerations"
          ),
          awsConfig.tempDirectoryPath.getOrElse(null),
          if (awsConfig.heapSize == null) null else awsConfig.heapSize.asInstanceOf[Long]
        )
      )
    }
  }

  /**
   * Performs pre-commit merge where splits are merged before being added to the transaction log. In this mode, original
   * fragmental splits are deleted after the merged split is uploaded and the transaction log never sees the original
   * splits.
   */
  private def performPreCommitMerge(): Seq[Row] =
    throw new UnsupportedOperationException(
      "PRECOMMIT merge is not yet implemented. Use the default post-commit merge instead."
    )

  /**
   * Find groups of files that should be merged within a partition. Follows bin packing approach similar to Delta Lake's
   * OPTIMIZE. Only creates groups with 2+ files to satisfy tantivy4java merge requirements. CRITICAL: Ensures all files
   * in each group have identical partition values.
   *
   * @param partitionValues
   *   partition values for the files in this group
   * @param files
   *   files to group for merging
   * @param maxSourceSplitsPerMerge
   *   maximum number of source splits that can be merged in a single merge operation
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
        logger.debug(
          s"Skipping file ${file.path} (size: ${file.size} bytes) - already at or above skip threshold ($skipSplitThreshold bytes, ${(skipSplitThresholdPercent * 100).toInt}% of target)"
        )
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
      val wouldExceedMaxSplits  = currentGroup.length >= maxSourceSplitsPerMerge

      if (wouldExceedTargetSize || wouldExceedMaxSplits) {
        if (wouldExceedMaxSplits) {
          logger.debug(
            s"MERGE DEBUG: Adding ${file.path} would exceed max source splits per merge ($maxSourceSplitsPerMerge)"
          )
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
   * Plans merge groups by simulating multi-pass merging. Returns ONLY first-generation groups (which have real file
   * paths), along with the total number of merge generations that would be needed.
   *
   * IMPORTANT: Only first-generation groups are returned because later generations have simulated file paths. The
   * caller should execute the returned groups, then call this method again with fresh transaction log state to get the
   * next generation.
   *
   * @param files
   *   files to consider for merging
   * @param partitionColumns
   *   partition columns from metadata
   * @param maxSourceSplitsPerMerge
   *   maximum source splits per merge operation
   * @return
   *   tuple of (first-generation merge groups, total number of generations needed)
   */
  private def planAllMergeGroups(
    files: Seq[AddAction],
    partitionColumns: Seq[String],
    maxSourceSplitsPerMerge: Int
  ): (Seq[MergeGroup], Int) = {
    val allGroups = ArrayBuffer[MergeGroup]()

    // Track files: key = file path, value = current state (real or simulated)
    var currentFiles = files.map(f => f.path -> f).toMap
    var generation   = 0
    var foundGroups  = true

    logger.info(s"Planning all merge groups upfront from ${files.length} files")

    while (foundGroups && generation < maxPasses) {
      generation += 1
      logger.debug(s"Planning generation $generation with ${currentFiles.size} files")

      // Group current files by partition
      val filesByPartition = currentFiles.values.toSeq.groupBy(_.partitionValues)

      // Find merge groups for this generation
      val generationGroups = filesByPartition.flatMap {
        case (partitionValues, partitionFiles) =>
          val groups = findMergeableGroups(partitionValues, partitionFiles, maxSourceSplitsPerMerge)
          // Filter to valid groups (>= 2 files)
          groups.filter(_.files.length >= 2)
      }.toSeq

      if (generationGroups.isEmpty) {
        foundGroups = false
        logger.debug(s"Generation $generation: No merge groups found, planning complete")
      } else {
        logger.debug(s"Generation $generation: Found ${generationGroups.length} merge groups")

        // Only add first generation groups - they have real file paths
        // Later generations are counted for metrics but executed separately after re-reading actual paths
        if (generation == 1) {
          allGroups ++= generationGroups
        }
        // Note: Generation 2+ groups have simulated paths and are NOT added to allGroups
        // The generationCount return value tells the caller how many passes are needed

        // Simulate: remove source files, add simulated merged files
        for (group <- generationGroups) {
          // Remove source files from tracking
          group.files.foreach(f => currentFiles -= f.path)

          // Create simulated merged file with estimated size
          val simulatedSize = group.files.map(_.size).sum
          val simulatedPath = s"simulated-merge-gen$generation-${java.util.UUID.randomUUID()}"

          // Use first file as template for metadata, update path and size
          val simulatedFile = group.files.head.copy(
            path = simulatedPath,
            size = simulatedSize,
            modificationTime = System.currentTimeMillis()
          )
          currentFiles += (simulatedPath -> simulatedFile)
        }

        logger.debug(s"Generation $generation: Simulated ${generationGroups.length} merges, now tracking ${currentFiles.size} files")
      }
    }

    val actualGenerations = if (foundGroups) generation else math.max(0, generation - 1)
    logger.info(s"Planning complete: ${allGroups.length} total merge groups across $actualGenerations generations")

    (allGroups.toSeq, actualGenerations)
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
      val effectiveMaxSourceSplitsPerMerge = maxSourceSplitsPerMerge
        .orElse {
          sparkSession.conf
            .getOption(
              io.indextables.spark.config.IndexTables4SparkSQLConf.TANTIVY4SPARK_MERGE_MAX_SOURCE_SPLITS_PER_MERGE
            )
            .map(_.toInt)
        }
        .getOrElse(
          io.indextables.spark.config.IndexTables4SparkSQLConf.TANTIVY4SPARK_MERGE_MAX_SOURCE_SPLITS_PER_MERGE_DEFAULT
        )

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

    // Determine if this is a cloud path
    val isS3Path = tablePathStr.startsWith("s3://") || tablePathStr.startsWith("s3a://")
    val isAzurePath = tablePathStr.startsWith("azure://") || tablePathStr.startsWith("wasb://") ||
      tablePathStr.startsWith("wasbs://") || tablePathStr.startsWith("abfs://") ||
      tablePathStr.startsWith("abfss://")
    val isCloudPath = isS3Path || isAzurePath

    // Determine temp directory for downloads
    val configuredTempDir = awsConfig.tempDirectoryPath
      .getOrElse(
        if (MergeSplitsCommand.isLocalDisk0Available()) "/local_disk0/tantivy4spark-merge"
        else System.getProperty("java.io.tmpdir")
      )

    val mergeId = java.util.UUID.randomUUID().toString

    // Try the configured temp directory; fall back to system temp if it can't be created
    val (tempDir, downloadDir) = {
      val primaryDir = new java.io.File(configuredTempDir, s"merge-download-$mergeId")
      if (primaryDir.mkdirs() || primaryDir.exists()) {
        (configuredTempDir, primaryDir)
      } else {
        val systemTmpDir = System.getProperty("java.io.tmpdir")
        logger.warn(
          s"[EXECUTOR] Failed to create merge temp directory at configured path '$configuredTempDir', " +
            s"falling back to system temp directory: $systemTmpDir"
        )
        val fallbackDir = new java.io.File(systemTmpDir, s"merge-download-$mergeId")
        if (!fallbackDir.mkdirs() && !fallbackDir.exists()) {
          throw new RuntimeException(
            s"Failed to create merge temp directory: tried '$configuredTempDir' and system temp '$systemTmpDir'"
          )
        }
        (systemTmpDir, fallbackDir)
      }
    }

    // Extract docMappingJson from first file to preserve fast fields configuration.
    // Older splits (pre-docMappingJson) may not have this field; fall back gracefully.
    val docMappingJsonOpt = mergeGroup.files.headOption.flatMap(_.docMappingJson)
    if (docMappingJsonOpt.isEmpty) {
      logger.warn(
        s"No docMappingJson in source splits for merge group with ${mergeGroup.files.length} files - fast fields configuration may not be preserved"
      )
    }

    try {
      // ============ PHASE 1: DOWNLOAD SOURCE SPLITS ============
      val localInputPaths: Seq[String] = if (isCloudPath) {
        logger.info(s"[EXECUTOR] Phase 1: Downloading ${mergeGroup.files.length} source splits")

        // Construct full cloud URLs for source splits
        val sourceUrls = mergeGroup.files.map { file =>
          if (isS3Path) {
            if (file.path.startsWith("s3://") || file.path.startsWith("s3a://")) {
              file.path.replaceFirst("^s3a://", "s3://")
            } else {
              val normalizedBaseUri = tablePathStr.replaceFirst("^s3a://", "s3://").replaceAll("/$", "")
              s"$normalizedBaseUri/${file.path}"
            }
          } else {
            // Azure path
            if (
              file.path.startsWith("azure://") || file.path.startsWith("wasb://") ||
              file.path.startsWith("wasbs://") || file.path.startsWith("abfs://") ||
              file.path.startsWith("abfss://")
            ) {
              normalizeAzureUrl(file.path)
            } else {
              val normalizedBaseUri = normalizeAzureUrl(tablePathStr).replaceAll("/$", "")
              s"$normalizedBaseUri/${file.path}"
            }
          }
        }

        // Create download requests
        val downloadRequests = sourceUrls.zipWithIndex.map {
          case (sourceUrl, idx) =>
            val localPath = new java.io.File(downloadDir, s"source-$idx.split").getAbsolutePath
            io.indextables.spark.io.merge.DownloadRequest(
              sourcePath = sourceUrl,
              destinationPath = localPath,
              expectedSize = mergeGroup.files(idx).size,
              batchId = 0,
              index = idx
            )
        }

        // Create appropriate downloader based on protocol
        // Note: tablePath is passed for credential provider resolution (e.g., Unity Catalog)
        val downloader: io.indextables.spark.io.merge.AsyncDownloader = if (isS3Path) {
          io.indextables.spark.io.merge.S3AsyncDownloader.fromConfig(awsConfig.configs, tablePathStr)
        } else {
          io.indextables.spark.io.merge.AzureAsyncDownloader
            .fromConfig(awsConfig.configs)
            .getOrElse(throw new RuntimeException("Failed to create Azure downloader - check credentials"))
        }

        // Download all source splits
        val ioConfig        = io.indextables.spark.io.merge.MergeIOConfig.fromMap(awsConfig.configs)
        val downloadManager = io.indextables.spark.io.merge.CloudDownloadManager.getInstance(ioConfig)
        val downloadResults = downloadManager.submitBatch(downloadRequests, downloader, ioConfig.downloadRetries).get()

        // Check for failures
        val failures = downloadResults.filter(!_.success)
        if (failures.nonEmpty) {
          val errorMsgs = failures
            .map(f => s"${f.request.sourcePath}: ${f.error.map(_.getMessage).getOrElse("unknown")}")
            .mkString("; ")
          throw new RuntimeException(s"Failed to download ${failures.length} source splits: $errorMsgs")
        }

        // Update Spark input metrics
        val totalDownloadBytes = downloadResults.map(_.bytesDownloaded).sum
        org.apache.spark.sql.indextables.OutputMetricsUpdater.incInputMetrics(totalDownloadBytes, downloadResults.length)

        logger.info(
          s"[EXECUTOR] Phase 1 complete: Downloaded ${downloadResults.length} splits ($totalDownloadBytes bytes)"
        )

        // Return local paths
        downloadResults.map(_.localPath)
      } else {
        // Local paths - no download needed, just resolve the paths
        logger.info(s"[EXECUTOR] Phase 1: Using local paths (no download needed)")
        mergeGroup.files.map(file => new org.apache.hadoop.fs.Path(tablePathStr, file.path).toUri.getPath)
      }

      // ============ PHASE 2: LOCAL MERGE (tantivy4java) ============
      logger.info(s"[EXECUTOR] Phase 2: Merging ${localInputPaths.size} splits locally")

      val localOutputPath = new java.io.File(downloadDir, s"merged-$mergeId.split").getAbsolutePath

      // Create merge configuration WITHOUT cloud credentials - all paths are local!
      val mergeConfigBuilder = QuickwitSplit.MergeConfig
        .builder()
        .indexUid("merged-index-uid")
        .sourceId("tantivy4spark")
        .nodeId("merge-node")
        .partitionId(0L)
        .deleteQueries(java.util.Collections.emptyList[String]())
        .debugEnabled(awsConfig.debugEnabled)

      // Add temp directory and heap size
      mergeConfigBuilder.tempDirectoryPath(tempDir)
      Option(awsConfig.heapSize).foreach(heap => mergeConfigBuilder.heapSizeBytes(heap))
      // Conditionally set docMappingUid — older splits may not have this field
      docMappingJsonOpt.foreach(json => mergeConfigBuilder.docMappingUid(json))

      val mergeConfig = mergeConfigBuilder.build()

      val serializedMetadata =
        try {
          val metadata = MergeSplitsCommand.retryOnStreamingError(
            () => QuickwitSplit.mergeSplits(localInputPaths.asJava, localOutputPath, mergeConfig),
            s"merge ${localInputPaths.size} local splits",
            logger
          )
          SerializableSplitMetadata.fromQuickwitSplitMetadata(metadata)
        } catch {
          case ex: Exception =>
            logger.error(s"Merge failed: ${ex.getMessage}", ex)
            throw new RuntimeException(s"Merge operation failed: ${ex.getMessage}", ex)
        }

      logger.info(s"[EXECUTOR] Phase 2 complete: Merged ${serializedMetadata.getNumDocs} docs, ${serializedMetadata.getUncompressedSizeBytes} bytes")

      // Verify local merged file exists
      val mergedFile = new java.io.File(localOutputPath)
      if (!mergedFile.exists()) {
        throw new RuntimeException(s"Merged file was not created at: $localOutputPath")
      }
      val mergedSize = mergedFile.length()

      // ============ PHASE 3: UPLOAD MERGED SPLIT ============
      val finalMergedSize = if (isCloudPath) {
        logger.info(s"[EXECUTOR] Phase 3: Uploading merged split ($mergedSize bytes)")

        // Construct cloud destination path
        val destPath = if (isS3Path) {
          val normalizedBaseUri = tablePathStr.replaceFirst("^s3a://", "s3://").replaceAll("/$", "")
          s"$normalizedBaseUri/$mergedPath"
        } else {
          val normalizedBaseUri = normalizeAzureUrl(tablePathStr).replaceAll("/$", "")
          s"$normalizedBaseUri/$mergedPath"
        }

        // Upload using same credential resolution as download (CredentialProviderFactory)
        val uploadedBytes = io.indextables.spark.io.merge.MergeUploader.uploadWithRetry(
          localOutputPath,
          destPath,
          awsConfig.configs,
          tablePathStr, // Pass tablePath for credential provider resolution
          maxRetries = 3
        )

        // Update Spark output metrics
        org.apache.spark.sql.indextables.OutputMetricsUpdater.updateOutputMetrics(uploadedBytes, 1)

        logger.info(s"[EXECUTOR] Phase 3 complete: Uploaded to $destPath ($uploadedBytes bytes)")
        uploadedBytes
      } else {
        // For local paths, tantivy4java already wrote to the correct location
        // Just move the file to the final destination
        val destPath = new org.apache.hadoop.fs.Path(tablePathStr, mergedPath).toUri.getPath
        val destFile = new java.io.File(destPath)
        destFile.getParentFile.mkdirs()
        java.nio.file.Files.move(mergedFile.toPath, destFile.toPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
        logger.info(s"[EXECUTOR] Phase 3: Moved local merge result to $destPath")
        mergedSize
      }

      // ============ PHASE 4: CLEANUP ============
      logger.info(s"[EXECUTOR] Phase 4: Cleaning up temp files")

      // Delete source temp files (downloaded splits) - only if we downloaded them
      if (isCloudPath) {
        localInputPaths.foreach { path =>
          try
            java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(path))
          catch {
            case e: Exception => logger.warn(s"Failed to delete temp source file: $path", e)
          }
        }
      }

      // Delete merged temp file (after upload success) - only if we uploaded it
      if (isCloudPath) {
        try
          java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(localOutputPath))
        catch {
          case e: Exception => logger.warn(s"Failed to delete temp merged file: $localOutputPath", e)
        }
      }

      // Delete download directory
      try
        if (downloadDir.exists()) {
          org.apache.commons.io.FileUtils.deleteDirectory(downloadDir)
        }
      catch {
        case e: Exception => logger.warn(s"Failed to delete download directory: ${downloadDir.getAbsolutePath}", e)
      }

      logger.info(
        s"[EXECUTOR] Merge complete: ${mergeGroup.files.length} splits -> $mergedPath ($finalMergedSize bytes)"
      )

      MergedSplitInfo(mergedPath, finalMergedSize, serializedMetadata)

    } catch {
      case e: Exception =>
        // Cleanup on failure - scrub all temp files
        logger.error(s"[EXECUTOR] Merge failed, cleaning up temp files", e)
        try
          if (downloadDir.exists()) {
            org.apache.commons.io.FileUtils.deleteDirectory(downloadDir)
          }
        catch {
          case cleanupEx: Exception =>
            logger.warn(s"Failed to cleanup download directory on error: ${downloadDir.getAbsolutePath}", cleanupEx)
        }
        throw e
    }
  }
}

/** Group of files that should be merged together. */
case class MergeGroup(
  partitionValues: Map[String, String],
  files: Seq[AddAction])
    extends Serializable {
  override def toString: String =
    s"MergeGroup(partition=$partitionValues, files=${files.length})"
}

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
      skippedSplits.map(s => new QuickwitSplit.SkippedSplit(s, "")).asJava   // skippedSplits
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
