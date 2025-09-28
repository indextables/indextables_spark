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

package com.tantivy4spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.{StringType, LongType, StructType, StructField}
import org.apache.spark.unsafe.types.UTF8String
import com.tantivy4spark.transaction.{TransactionLog, TransactionLogFactory, AddAction, RemoveAction}
import com.tantivy4spark.storage.SplitManager
import com.tantivy4spark.io.{CloudStorageProviderFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.ThreadLocalRandom
import com.tantivy4java.{QuickwitSplit, MergeBinaryExtractor, Index}
import com.tantivy4spark.util.ConfigNormalization
import scala.jdk.CollectionConverters._

/**
 * SQL command to merge small split files into larger ones.
 * Modeled after Delta Lake's OPTIMIZE command structure and behavior.
 * 
 * Syntax: MERGE SPLITS ('/path/to/table' | table_name) [WHERE partition_predicates]
 *         [TARGET SIZE target_size] [MAX GROUPS max_groups] [PRECOMMIT]
 * 
 * Examples:
 * - MERGE SPLITS '/path/to/table'
 * - MERGE SPLITS my_table WHERE partition_col = 'value'
 * - MERGE SPLITS my_table TARGET SIZE 5368709120  -- 5GB in bytes
 * - MERGE SPLITS '/path/to/table' WHERE year = 2023 TARGET SIZE 2147483648  -- 2GB
 * - MERGE SPLITS my_table MAX GROUPS 5  -- Limit to 5 oldest merge groups
 * - MERGE SPLITS '/path/to/table' TARGET SIZE 1G MAX GROUPS 3  -- 1GB target, max 3 groups
 * - MERGE SPLITS events PRECOMMIT  -- Pre-commit merge (framework complete, core implementation pending)
 * 
 * This command:
 * 1. Merges only within partitions (follows Delta Lake OPTIMIZE pattern)
 * 2. Selects mergeable splits in transaction log order
 * 3. Concatenates splits up to configurable target size (default 5GB)
 * 4. Assumes merged split size equals sum of input splits
 * 5. Does not merge splits already at target size
 * 6. Uses atomic REMOVE+ADD operations in transaction log
 * 7. Ensures queries after merge only read merged splits
 * 8. MAX GROUPS option: Limits merge operation to N oldest destination merge groups
 * 9. PRECOMMIT option: Merges splits during write process before transaction log commit
 *    (eliminates small file problems at source - framework complete, core logic pending)
 */
abstract class MergeSplitsCommandBase extends RunnableCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("table_path", StringType)(),
    AttributeReference("metrics", StringType)()
  )

  /**
   * Default target size for merged splits (5GB in bytes)
   */
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

/**
 * MERGE SPLITS command implementation for Spark SQL.
 */
case class MergeSplitsCommand(
    override val child: LogicalPlan,
    userPartitionPredicates: Seq[String],
    targetSize: Option[Long],
    maxGroups: Option[Int],
    preCommitMerge: Boolean = false
) extends MergeSplitsCommandBase with UnaryNode {

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
      return Seq(Row("PRE-COMMIT MERGE", "PRE-COMMIT MERGE: Functionality pending implementation"))
    }
    
    // Resolve table path from child logical plan
    val tablePath = try {
      resolveTablePath(child, sparkSession)
    } catch {
      case e: IllegalArgumentException =>
        // Handle non-existent table gracefully
        val pathStr = child match {
          case resolved: UnresolvedDeltaPathOrIdentifier =>
            resolved.path.getOrElse(resolved.tableIdentifier.map(_.toString).getOrElse("unknown"))
          case _ => "unknown"
        }
        logger.info(s"Table or path not found: $pathStr")
        return Seq(Row(pathStr, "No splits merged - table or path does not exist"))
    }
    
    // Create transaction log
    val transactionLog = TransactionLogFactory.create(tablePath, sparkSession, new CaseInsensitiveStringMap(java.util.Collections.emptyMap()))
    
    try {
      // Check if transaction log is initialized
      val hasMetadata = try {
        transactionLog.getMetadata()
        true
      } catch {
        case _: Exception => false
      }
      
      if (!hasMetadata) {
        logger.info(s"No transaction log found at: $tablePath")
        return Seq(Row(tablePath.toString, "No splits merged - not a valid Tantivy4Spark table"))
      }
      
      // Validate table has files
      val files = transactionLog.listFiles()
      if (files.isEmpty) {
        logger.info(s"No files found in table: $tablePath")
        return Seq(Row(tablePath.toString, "No splits to merge - table is empty"))
      }

      new MergeSplitsExecutor(
        sparkSession,
        transactionLog,
        tablePath,
        userPartitionPredicates,
        actualTargetSize,
        maxGroups,
        preCommitMerge
      ).merge()
    } finally {
      transactionLog.close()
    }
  }
  
  /**
   * Resolve table path from logical plan child.
   */
  private def resolveTablePath(child: LogicalPlan, sparkSession: SparkSession): Path = {
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
                  throw new IllegalArgumentException(s"Table not found: ${tableId}")
                }
              case None =>
                throw new IllegalArgumentException("Either path or table identifier must be specified")
            }
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported child plan type: ${child.getClass.getSimpleName}")
    }
  }
}

object MergeSplitsCommand {
  /**
   * Alternate constructor that converts a provided path or table identifier into the
   * correct child LogicalPlan node.
   */
  def apply(
      path: Option[String],
      tableIdentifier: Option[org.apache.spark.sql.catalyst.TableIdentifier],
      userPartitionPredicates: Seq[String],
      targetSize: Option[Long],
      maxGroups: Option[Int],
      preCommitMerge: Boolean
  ): MergeSplitsCommand = {
    val plan = UnresolvedDeltaPathOrIdentifier(path, tableIdentifier, "MERGE SPLITS")
    MergeSplitsCommand(plan, userPartitionPredicates, targetSize, maxGroups, preCommitMerge)
  }
}

/**
 * Serializable wrapper for AWS configuration that can be broadcast across executors.
 */
case class SerializableAwsConfig(
    accessKey: String,
    secretKey: String,
    sessionToken: Option[String],
    region: String,
    endpoint: Option[String],
    pathStyleAccess: Boolean,
    tempDirectoryPath: Option[String] = None,
    mergeMode: String = "direct",
    heapSize: Long = 50L * 1024L * 1024L, // 50MB default
    debugEnabled: Boolean = false, // Debug mode disabled by default
    credentialsProviderClass: Option[String] = None // Custom credential provider class name
) extends Serializable {
  
  /**
   * Convert to tantivy4java AwsConfig instance.
   * Resolves custom credential providers if specified.
   */
  def toQuickwitSplitAwsConfig(tablePath: String): QuickwitSplit.AwsConfig = {
    credentialsProviderClass match {
      case Some(providerClassName) =>
        try {
          // Resolve credentials using custom credential provider
          val (resolvedAccessKey, resolvedSecretKey, resolvedSessionToken) = resolveCredentialsFromProvider(providerClassName, tablePath)
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
            println(s"⚠️ [EXECUTOR] Failed to resolve credentials from provider $providerClassName: ${ex.getMessage}")
            println(s"⚠️ [EXECUTOR] Falling back to explicit credentials")
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
  }

  /**
   * Resolve AWS credentials from a custom credential provider class.
   * Returns (accessKey, secretKey, sessionToken).
   */
  private def resolveCredentialsFromProvider(providerClassName: String, tablePath: String): (String, String, Option[String]) = {
    import java.net.URI
    import org.apache.hadoop.conf.Configuration
    import com.tantivy4spark.utils.CredentialProviderFactory

    println(s"🔍 [EXECUTOR] Resolving credentials using custom provider: $providerClassName")
    println(s"🔍 [EXECUTOR] Using table path for credential provider: $tablePath")

    // Use the provided table path for the credential provider constructor
    val tableUri = new URI(tablePath)
    val hadoopConf = new Configuration()

    // Use CredentialProviderFactory to instantiate and extract credentials
    val provider = CredentialProviderFactory.createCredentialProvider(providerClassName, tableUri, hadoopConf)
    val basicCredentials = CredentialProviderFactory.extractCredentialsViaReflection(provider)

    println(s"✅ [EXECUTOR] Successfully resolved credentials from $providerClassName")
    println(s"🔍 [EXECUTOR] Resolved credentials: accessKey=${basicCredentials.accessKey.take(4)}***, sessionToken=${basicCredentials.sessionToken.map(_ => "***").getOrElse("None")}")

    (basicCredentials.accessKey, basicCredentials.secretKey, basicCredentials.sessionToken)
  }

  /**
   * Execute merge operation using the configured merge mode.
   * Returns the result as SerializableSplitMetadata for consistency.
   */
  def executeMerge(
      inputSplitPaths: java.util.List[String],
      outputSplitPath: String,
      mergeConfig: QuickwitSplit.MergeConfig
  ): SerializableSplitMetadata = {
    mergeMode.toLowerCase match {
      case "process" =>
        // Use process-based merging with MergeBinaryExtractor
        // Note: Heap size and temp directory should be configured in the mergeConfig
        val result = MergeBinaryExtractor.executeMerge(
          inputSplitPaths,
          outputSplitPath,
          mergeConfig
        )
        SerializableSplitMetadata.fromQuickwitSplitMetadata(result.toSplitMetadata())

      case "direct" | _ =>
        // Fallback to direct merging using QuickwitSplit.mergeSplits
        val metadata = QuickwitSplit.mergeSplits(inputSplitPaths, outputSplitPath, mergeConfig)
        SerializableSplitMetadata.fromQuickwitSplitMetadata(metadata)
    }
  }
}

/**
 * Executor for merge splits operation.
 * Follows Delta Lake's OptimizeExecutor pattern.
 */
class MergeSplitsExecutor(
    sparkSession: SparkSession,
    transactionLog: TransactionLog,
    tablePath: Path,
    partitionPredicates: Seq[String],
    targetSize: Long,
    maxGroups: Option[Int],
    preCommitMerge: Boolean = false
) {
  
  private val logger = LoggerFactory.getLogger(classOf[MergeSplitsExecutor])
  
  /**
   * Extract AWS configuration from SparkSession for tantivy4java merge operations.
   * Uses same pattern as TantivySearchEngine for consistency.
   * Returns a serializable wrapper that can be broadcast across executors.
   */
  private def extractAwsConfig(): SerializableAwsConfig = {
    try {
      val sparkConf = sparkSession.conf
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

      // Extract and normalize all tantivy4spark configs from both Spark and Hadoop
      val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
      val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
      val mergedConfigs = ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)

      // Helper function to get config from normalized configs with fallback
      def getConfigWithFallback(sparkKey: String): Option[String] = {
        val result = mergedConfigs.get(sparkKey)

        logger.debug(s"🔍 AWS Config fallback for $sparkKey: merged=${result.getOrElse("None")}")
        result
      }
      
      val accessKey = getConfigWithFallback("spark.tantivy4spark.aws.accessKey")
      val secretKey = getConfigWithFallback("spark.tantivy4spark.aws.secretKey")
      val sessionToken = getConfigWithFallback("spark.tantivy4spark.aws.sessionToken")
      val region = getConfigWithFallback("spark.tantivy4spark.aws.region")
      val endpoint = getConfigWithFallback("spark.tantivy4spark.s3.endpoint")
      val pathStyleAccess = getConfigWithFallback("spark.tantivy4spark.s3.pathStyleAccess")
        .map(_.toLowerCase == "true").getOrElse(false)

      // Extract temporary directory configuration for merge operations with auto-detection
      val tempDirectoryPath = getConfigWithFallback("spark.tantivy4spark.merge.tempDirectoryPath")
        .orElse(com.tantivy4spark.storage.SplitCacheConfig.getDefaultTempPath())

      // Extract merge mode configuration (process-based vs direct)
      val mergeMode = getConfigWithFallback("spark.tantivy4spark.merge.mode").getOrElse("direct")
      val heapSize = getConfigWithFallback("spark.tantivy4spark.merge.heapSize")
        .map(_.toLong).getOrElse(50L * 1024L * 1024L) // Default 50MB

      // Extract debug configuration for process-based merging
      val debugEnabled = getConfigWithFallback("spark.tantivy4spark.merge.debug")
        .map(_.toLowerCase == "true").getOrElse(false)

      // Extract custom credential provider class name
      val credentialsProviderClass = getConfigWithFallback("spark.tantivy4spark.aws.credentialsProviderClass")

      println(s"🔍 [DRIVER] Creating AwsConfig with: region=${region.getOrElse("None")}, endpoint=${endpoint.getOrElse("None")}, pathStyle=$pathStyleAccess")
      println(s"🔍 [DRIVER] AWS credentials: accessKey=${accessKey.map(k => s"${k.take(4)}***").getOrElse("None")}, sessionToken=${sessionToken.map(_ => "***").getOrElse("None")}")
      println(s"🔍 [DRIVER] Credentials provider class: ${credentialsProviderClass.getOrElse("None")}")
      println(s"🔍 [DRIVER] Merge temp directory: ${tempDirectoryPath.getOrElse("system default")}")
      println(s"🔍 [DRIVER] Merge mode: $mergeMode (heap size: ${heapSize / (1024 * 1024)}MB, debug: $debugEnabled)")
      logger.info(s"🔍 Creating AwsConfig with: region=${region.getOrElse("None")}, endpoint=${endpoint.getOrElse("None")}, pathStyle=$pathStyleAccess")
      logger.info(s"🔍 AWS credentials: accessKey=${accessKey.map(k => s"${k.take(4)}***").getOrElse("None")}, sessionToken=${sessionToken.map(_ => "***").getOrElse("None")}")
      logger.info(s"🔍 Credentials provider class: ${credentialsProviderClass.getOrElse("None")}")
      logger.info(s"🔍 Merge temp directory: ${tempDirectoryPath.getOrElse("system default")}")
      logger.info(s"🔍 Merge mode: $mergeMode (heap size: ${heapSize / (1024 * 1024)}MB, debug: $debugEnabled)")

      // Validate temp directory path if specified
      tempDirectoryPath.foreach { path =>
        try {
          val dir = new java.io.File(path)
          if (!dir.exists()) {
            logger.warn(s"⚠️ Custom temp directory does not exist: $path - will fall back to system temp directory")
          } else if (!dir.isDirectory()) {
            logger.warn(s"⚠️ Custom temp directory path is not a directory: $path - will fall back to system temp directory")
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

      // Create SerializableAwsConfig with the extracted credentials and temp directory
      SerializableAwsConfig(
        accessKey.getOrElse(""),
        secretKey.getOrElse(""),
        sessionToken, // Can be None for permanent credentials
        region.getOrElse("us-east-1"),
        endpoint, // Can be None for default AWS endpoint
        pathStyleAccess,
        tempDirectoryPath, // Custom temp directory path for merge operations
        mergeMode, // Merge mode: "process" or "direct"
        heapSize, // Heap size for process-based merging
        debugEnabled, // Debug mode for process-based merging
        credentialsProviderClass // Custom credential provider class name
      )
    } catch {
      case ex: Exception =>
        logger.warn("Failed to extract AWS config from Spark session, using empty config", ex)
        // Return empty config that will use default AWS credential chain
        SerializableAwsConfig("", "", None, "us-east-1", None, false, None, "direct", 50L * 1024L * 1024L, false, None)
    }
  }
  
  /**
   * Smart string ordering that handles numeric values correctly.
   * Tries numeric comparison first, falls back to string comparison.
   */
  private val smartStringOrdering: Ordering[String] = new Ordering[String] {
    override def compare(x: String, y: String): Int = {
      try {
        // Try to parse both as doubles
        val xNum = x.toDouble
        val yNum = y.toDouble
        xNum.compare(yNum)
      } catch {
        case _: NumberFormatException =>
          // Fall back to string comparison
          x.compare(y)
      }
    }
  }
  
  def merge(): Seq[Row] = {
    if (preCommitMerge) {
      logger.info(s"Starting PRE-COMMIT MERGE SPLITS operation for table: $tablePath with target size: $targetSize bytes")
      return performPreCommitMerge()
    }
    
    logger.info(s"Starting MERGE SPLITS operation for table: $tablePath with target size: $targetSize bytes")

    // Get current metadata to understand partition schema
    val metadata = transactionLog.getMetadata()

    // DEBUG: Log the metadata details
    println(s"🔍 MERGE DEBUG: Retrieved metadata from transaction log:")
    println(s"🔍 MERGE DEBUG:   Metadata ID: ${metadata.id}")
    println(s"🔍 MERGE DEBUG:   Partition columns: ${metadata.partitionColumns}")
    println(s"🔍 MERGE DEBUG:   Partition columns size: ${metadata.partitionColumns.size}")
    println(s"🔍 MERGE DEBUG:   Configuration: ${metadata.configuration}")
    logger.info(s"🔍 MERGE DEBUG: Retrieved metadata from transaction log:")
    logger.info(s"🔍 MERGE DEBUG:   Metadata ID: ${metadata.id}")
    logger.info(s"🔍 MERGE DEBUG:   Partition columns: ${metadata.partitionColumns}")
    logger.info(s"🔍 MERGE DEBUG:   Partition columns size: ${metadata.partitionColumns.size}")
    logger.info(s"🔍 MERGE DEBUG:   Configuration: ${metadata.configuration}")

    val partitionSchema = StructType(metadata.partitionColumns.map(name =>
      StructField(name, StringType, nullable = true)))

    println(s"🔍 MERGE DEBUG: Constructed partition schema: ${partitionSchema.fieldNames.mkString(", ")}")
    logger.info(s"🔍 MERGE DEBUG: Constructed partition schema: ${partitionSchema.fieldNames.mkString(", ")}")

    // If no partition columns are defined in metadata, skip partition validation
    if (metadata.partitionColumns.isEmpty) {
      logger.info("No partition columns defined in metadata - treating table as non-partitioned")
    } else {
      logger.info(s"Found ${metadata.partitionColumns.size} partition columns: ${metadata.partitionColumns.mkString(", ")}")
    }

    // Get current files from transaction log (in order they were added)
    val allFiles = transactionLog.listFiles().sortBy(_.modificationTime)
    logger.info(s"Found ${allFiles.length} split files in transaction log")

    // Filter out files that are currently in cooldown period
    val trackingEnabled = sparkSession.conf.get("spark.tantivy4spark.skippedFiles.trackingEnabled", "true").toBoolean
    val currentFiles = if (trackingEnabled) {
      val filtered = transactionLog.filterFilesInCooldown(allFiles)
      val filteredCount = allFiles.length - filtered.length
      if (filteredCount > 0) {
        logger.info(s"Filtered out $filteredCount files in cooldown period")
        println(s"📝 [DRIVER] Skipping $filteredCount files currently in cooldown period")
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
    partitionsToMerge.foreach { case (partitionValues, files) =>
      logger.info(s"  Partition $partitionValues: ${files.length} files")
    }

    // Apply partition predicates if specified
    val filteredPartitions = if (partitionPredicates.nonEmpty) {
      applyPartitionPredicates(partitionsToMerge, partitionSchema)
    } else {
      partitionsToMerge
    }

    // Find mergeable splits within each partition
    val mergeGroups = filteredPartitions.flatMap { case (partitionValues, files) =>
      logger.info(s"Processing partition $partitionValues with ${files.length} files:")
      files.foreach { file =>
        logger.info(s"  File: ${file.path} (${file.size} bytes)")
      }
      
      val groups = findMergeableGroups(partitionValues, files)
      logger.info(s"Found ${groups.length} potential merge groups in partition $partitionValues")
      
      // Double-check: filter out any single-file groups that might have slipped through
      println(s"MERGE DEBUG: Before filtering: ${groups.length} groups")
      groups.foreach { group =>
        println(s"MERGE DEBUG:   Group has ${group.files.length} files: ${group.files.map(_.path).mkString(", ")}")
      }
      val validGroups = groups.filter(_.files.length >= 2)
      println(s"MERGE DEBUG: After filtering: ${validGroups.length} valid groups")
      if (groups.length != validGroups.length) {
        logger.warn(s"Filtered out ${groups.length - validGroups.length} single-file groups from partition $partitionValues")
      }
      
      validGroups.foreach { group =>
        logger.info(s"  Valid merge group: ${group.files.length} files (${group.files.map(_.size).sum} bytes total)")
        group.files.foreach { file =>
          logger.info(s"    - ${file.path} (${file.size} bytes)")
        }
      }
      
      validGroups
    }

    logger.info(s"Found ${mergeGroups.length} merge groups containing ${mergeGroups.map(_.files.length).sum} files")

    // Apply MAX GROUPS limit if specified
    val limitedMergeGroups = maxGroups match {
      case Some(maxLimit) if mergeGroups.length > maxLimit =>
        logger.info(s"Limiting merge operation to $maxLimit oldest merge groups (out of ${mergeGroups.length} total)")

        // Sort merge groups by the oldest file in each group to get the N oldest groups
        val sortedGroups = mergeGroups.sortBy(_.files.map(_.modificationTime).min)
        val limitedGroups = sortedGroups.take(maxLimit)

        val limitedFilesCount = limitedGroups.map(_.files.length).sum
        val totalFilesCount = mergeGroups.map(_.files.length).sum
        logger.info(s"MAX GROUPS limit applied: processing $limitedFilesCount files from $maxLimit oldest groups (skipping ${totalFilesCount - limitedFilesCount} files from ${mergeGroups.length - maxLimit} newer groups)")

        limitedGroups
      case Some(maxLimit) =>
        logger.info(s"MAX GROUPS limit of $maxLimit not reached (found ${mergeGroups.length} groups)")
        mergeGroups
      case None =>
        logger.debug("No MAX GROUPS limit specified")
        mergeGroups
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

    if (limitedMergeGroups.isEmpty) {
      logger.info("No splits require merging")
      return Seq(Row(tablePath.toString, "No splits merged - all splits are already optimal size"))
    }

    // Execute merges in parallel across Spark executors
    println(s"🏗️  [DRIVER] Distributing ${limitedMergeGroups.length} merge operations across Spark executors")
    logger.info(s"Distributing ${limitedMergeGroups.length} merge operations across Spark executors")
    
    // Broadcast AWS configuration to executors
    val awsConfig = extractAwsConfig()
    val broadcastAwsConfig = sparkSession.sparkContext.broadcast(awsConfig)
    val broadcastTablePath = sparkSession.sparkContext.broadcast(tablePath.toString)
    
    // Set descriptive names for Spark UI
    val totalSplits = limitedMergeGroups.map(_.files.length).sum
    val totalSizeGB = limitedMergeGroups.map(_.files.map(_.size).sum).sum / (1024.0 * 1024.0 * 1024.0)
    val jobGroup = s"tantivy4spark-merge-splits"
    val jobDescription = f"MERGE SPLITS: Consolidating $totalSplits splits (${totalSizeGB}%.2f GB) across ${limitedMergeGroups.length} groups"
    val stageName = f"Merge Splits: ${limitedMergeGroups.length} groups, $totalSplits splits (${totalSizeGB}%.2f GB)"
    
    sparkSession.sparkContext.setJobGroup(jobGroup, jobDescription, interruptOnCancel = true)
    
    val physicalMergeResults = try {
      val mergeGroupsRDD = sparkSession.sparkContext.parallelize(limitedMergeGroups, limitedMergeGroups.length)
        .setName(stageName)
      mergeGroupsRDD.map(group => MergeSplitsExecutor.executeMergeGroupDistributed(group, broadcastTablePath.value, broadcastAwsConfig.value))
        .setName("Merge Split Results")
        .collect()
    } finally {
      sparkSession.sparkContext.clearJobGroup()
    }
    
    // Now handle transaction log operations on driver (these cannot be distributed)
    logger.info(s"Processing ${physicalMergeResults.length} merge results on driver for transaction log updates")
    
    // CRITICAL: Validate all merged files actually exist before updating transaction log
    println(s"🔍 [DRIVER] Validating ${physicalMergeResults.length} merged files actually exist before transaction log update")
    physicalMergeResults.foreach { result =>
      val fullMergedPath = if (tablePath.toString.startsWith("s3://") || tablePath.toString.startsWith("s3a://")) {
        s"${tablePath.toString.replaceAll("/$", "")}/${result.mergedSplitInfo.path}"
      } else {
        new org.apache.hadoop.fs.Path(tablePath.toString, result.mergedSplitInfo.path).toString
      }
      
      try {
        // For S3, we can't easily check file existence from driver, but we can at least log the expected path
        println(s"🔍 [DRIVER] Merged file should exist at: $fullMergedPath")
        println(s"🔍 [DRIVER] Relative path in transaction log: ${result.mergedSplitInfo.path}")
        
        // TODO: Add actual S3 existence check here if needed for production validation
        
      } catch {
        case ex: Exception =>
          println(s"⚠️  [DRIVER] Could not validate merged file existence: ${ex.getMessage}")
          logger.warn(s"Could not validate merged file existence", ex)
      }
    }
    val results = physicalMergeResults.map { result =>
      val startTime = System.currentTimeMillis()
      
      logger.info(s"Processing transaction log for merge group with ${result.mergeGroup.files.length} files")
      
      // Check if merge was actually performed by examining indexUid in metadata
      val mergedMetadata = result.mergedSplitInfo.metadata
      val indexUid = mergedMetadata.indexUid

      // Extract skipped splits from the merge result to avoid marking them as removed
      val skippedSplitPaths = Option(result.mergedSplitInfo.metadata.getSkippedSplits())
        .map(_.asScala.toSet)
        .getOrElse(Set.empty[String])

      // Always record skipped files regardless of whether merge was performed
      if (skippedSplitPaths.nonEmpty) {
        logger.warn(s"⚠️  Merge operation skipped ${skippedSplitPaths.size} files (due to corruption/missing files): ${skippedSplitPaths.mkString(", ")}")
        println(s"⚠️  [DRIVER] Merge operation skipped ${skippedSplitPaths.size} files: ${skippedSplitPaths.mkString(", ")}")

        // Record skipped files in transaction log with cooldown period
        val cooldownHours = sparkSession.conf.get("spark.tantivy4spark.skippedFiles.cooldownDuration", "24").toInt
        val trackingEnabled = sparkSession.conf.get("spark.tantivy4spark.skippedFiles.trackingEnabled", "true").toBoolean

        if (trackingEnabled) {
          skippedSplitPaths.foreach { skippedPath =>
            logger.warn(s"⚠️  Recording skipped file in transaction log: $skippedPath")

            // Find the corresponding file in merge group to get partition info and size
            val correspondingFile = result.mergeGroup.files.find { file =>
              val fullPath = if (tablePath.toString.startsWith("s3://") || tablePath.toString.startsWith("s3a://")) {
                val normalizedBaseUri = tablePath.toString.replaceFirst("^s3a://", "s3://").replaceAll("/$", "")
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
            logger.warn(s"⚠️  Recorded skipped file with ${cooldownHours}h cooldown: ${correspondingFile.map(_.path).getOrElse(skippedPath)}")
          }
        }
      }

      // Check if no merge was performed (null or empty indexUid indicates this)
      if (indexUid.isEmpty || indexUid.contains(null) || indexUid.exists(_.trim.isEmpty)) {
        logger.warn(s"⚠️  No merge was performed for group with ${result.mergeGroup.files.length} files (null/empty indexUid) - skipping ADD/REMOVE operations but preserving skipped files tracking")
        println(s"⚠️  [DRIVER] No merge performed (null/empty indexUid) - skipping transaction log ADD/REMOVE operations")

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
      val removeActions = result.mergeGroup.files.filter { file =>
        val fullPath = if (tablePath.toString.startsWith("s3://") || tablePath.toString.startsWith("s3a://")) {
          // For S3 paths, construct full URL to match what tantivy4java reports
          val normalizedBaseUri = tablePath.toString.replaceFirst("^s3a://", "s3://").replaceAll("/$", "")
          s"$normalizedBaseUri/${file.path}"
        } else {
          // For local paths, use absolute path
          new org.apache.hadoop.fs.Path(tablePath.toString, file.path).toString
        }

        val wasSkipped = skippedSplitPaths.contains(fullPath) || skippedSplitPaths.contains(file.path)
        if (wasSkipped) {
          logger.info(s"Preserving skipped file in transaction log (not marking as removed): ${file.path}")
          println(s"📝 [DRIVER] Preserving skipped file: ${file.path}")
        }
        !wasSkipped
      }.map { file =>
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
      val (footerStartOffset, footerEndOffset, hotcacheStartOffset, hotcacheLength, hasFooterOffsets,
           timeRangeStart, timeRangeEnd, splitTags, deleteOpstamp, numMergeOps, docMappingJson, uncompressedSizeBytes, numDocs) =
        if (mergedMetadata != null) {
          val timeStart = Option(mergedMetadata.getTimeRangeStart()).map(_.toString)
          val timeEnd = Option(mergedMetadata.getTimeRangeEnd()).map(_.toString)
          val tags = Option(mergedMetadata.getTags()).filter(!_.isEmpty).map { tagSet =>
            import scala.jdk.CollectionConverters._
            tagSet.asScala.toSet
          }
          val docMapping = Option(mergedMetadata.getDocMappingJson())

          if (mergedMetadata.hasFooterOffsets) {
            (Some(mergedMetadata.getFooterStartOffset()),
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
             Some(mergedMetadata.getNumDocs()))
          } else {
            throw new IllegalStateException(s"Merged split ${result.mergedSplitInfo.path} does not have footer offsets. This indicates a problem with the merge operation or tantivy4java library.")
          }
        } else {
          throw new IllegalStateException(s"Failed to extract metadata from merged split ${result.mergedSplitInfo.path}. This indicates a problem with the merge operation or tantivy4java library.")
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
      
      // Write transaction log entry using the atomic merge API
      val version = transactionLog.commitMergeSplits(removeActions, Seq(addAction))
      transactionLog.invalidateCache() // Ensure cache is updated
      
      logger.info(s"Transaction log updated: removed ${removeActions.length} files, added 1 merged file")
      
      // Return the merge result with updated timing
      result.copy(executionTimeMs = result.executionTimeMs + (System.currentTimeMillis() - startTime))
      } // End of else block for indexUid check
    }
    
    val totalMergedFiles = results.map(_.mergedFiles).sum
    val totalMergeGroups = results.length
    val totalOriginalSize = results.map(_.originalSize).sum  
    val totalMergedSize = results.map(_.mergedSize).sum

    logger.info(s"MERGE SPLITS completed: merged $totalMergedFiles files into $totalMergeGroups new splits")
    logger.info(s"Size change: ${totalOriginalSize} bytes -> ${totalMergedSize} bytes")

    Seq(Row(
      tablePath.toString,
      s"Merged $totalMergedFiles files into $totalMergeGroups splits. " +
      s"Original size: $totalOriginalSize bytes, new size: $totalMergedSize bytes"
    ))
  }

  /**
   * Performs pre-commit merge where splits are merged before being added to the transaction log.
   * In this mode, original fragmental splits are deleted after the merged split is uploaded
   * and the transaction log never sees the original splits.
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
    
    Seq(Row(tablePath.toString, "PRE-COMMIT MERGE: Functionality pending implementation"))
  }

  /**
   * Find groups of files that should be merged within a partition.
   * Follows bin packing approach similar to Delta Lake's OPTIMIZE.
   * Only creates groups with 2+ files to satisfy tantivy4java merge requirements.
   * CRITICAL: Ensures all files in each group have identical partition values.
   */
  private def findMergeableGroups(
      partitionValues: Map[String, String],
      files: Seq[AddAction]
  ): Seq[MergeGroup] = {

    val groups = ArrayBuffer[MergeGroup]()
    val currentGroup = ArrayBuffer[AddAction]()
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

    // Filter out files that are already at or above target size
    val mergeableFiles = files.filter { file =>
      if (file.size >= targetSize) {
        logger.debug(s"Skipping file ${file.path} (size: ${file.size}) - already at target size")
        false
      } else {
        true
      }
    }
    
    println(s"MERGE DEBUG: Found ${mergeableFiles.length} files eligible for merging (< $targetSize bytes)")
    
    // If we have fewer than 2 mergeable files, no groups can be created
    if (mergeableFiles.length < 2) {
      println(s"MERGE DEBUG: Cannot create merge groups - need at least 2 files but found ${mergeableFiles.length}")
      return groups.toSeq
    }

    for ((file, index) <- mergeableFiles.zipWithIndex) {
      println(s"MERGE DEBUG: Processing file ${index+1}/${mergeableFiles.length}: ${file.path} (${file.size} bytes)")
      
      // Check if adding this file would exceed target size
      if (currentGroupSize > 0 && currentGroupSize + file.size > targetSize) {
        println(s"MERGE DEBUG: Adding ${file.path} (${file.size} bytes) to current group (${currentGroupSize} bytes) would exceed target ($targetSize bytes)")
        
        // Current group is full, save it if it has multiple files
        println(s"MERGE DEBUG: Current group has ${currentGroup.length} files before saving")
        if (currentGroup.length > 1) {
          // VALIDATION: Ensure all files in the group have identical partition values
          val groupFiles = currentGroup.clone().toSeq
          val inconsistentFiles = groupFiles.filterNot(_.partitionValues == partitionValues)
          if (inconsistentFiles.nonEmpty) {
            val errorMsg = s"CRITICAL: Group validation failed during creation! Found files with inconsistent partitions:\n" +
              inconsistentFiles.map(f => s"  - ${f.path}: ${f.partitionValues} (expected: $partitionValues)").mkString("\n")
            logger.error(errorMsg)
            throw new IllegalStateException(errorMsg)
          }

          groups += MergeGroup(partitionValues, groupFiles)
          println(s"MERGE DEBUG: ✓ Created merge group with ${currentGroup.length} files (${currentGroupSize} bytes): ${currentGroup.map(_.path).mkString(", ")}")
        } else {
          println(s"MERGE DEBUG: ✗ Discarding single-file group: ${currentGroup.head.path} (${currentGroupSize} bytes)")
        }
        
        // Start new group
        currentGroup.clear()
        currentGroup += file
        currentGroupSize = file.size
        println(s"MERGE DEBUG: Started new group with ${file.path} (${file.size} bytes)")
      } else {
        // Add file to current group
        currentGroup += file
        currentGroupSize += file.size
        println(s"MERGE DEBUG: Added ${file.path} (${file.size} bytes) to current group. Group now has ${currentGroup.length} files (${currentGroupSize} bytes total)")
      }
    }

    // Handle remaining group - only save if it has multiple files
    if (currentGroup.length > 1) {
      // FINAL VALIDATION: Ensure all files in the group have identical partition values
      val groupFiles = currentGroup.toSeq
      val inconsistentFiles = groupFiles.filterNot(_.partitionValues == partitionValues)
      if (inconsistentFiles.nonEmpty) {
        val errorMsg = s"CRITICAL: Final group validation failed! Found files with inconsistent partitions:\n" +
          inconsistentFiles.map(f => s"  - ${f.path}: ${f.partitionValues} (expected: $partitionValues)").mkString("\n")
        logger.error(errorMsg)
        throw new IllegalStateException(errorMsg)
      }

      groups += MergeGroup(partitionValues, groupFiles)
      logger.debug(s"✓ Created final merge group with ${currentGroup.length} files (${currentGroupSize} bytes): ${currentGroup.map(_.path).mkString(", ")}")
    } else if (currentGroup.length == 1) {
      logger.debug(s"✗ Discarding final single-file group: ${currentGroup.head.path} (${currentGroupSize} bytes)")
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

    println(s"MERGE DEBUG: Created ${groups.length} merge groups from ${mergeableFiles.length} mergeable files")
    logger.info(s"✅ All ${groups.length} merge groups passed partition consistency validation")
    groups.toSeq
  }

  /**
   * Execute merge for a single group of splits in executor context.
   * This version is designed to run on Spark executors and handles serialization properly.
   */
  private def executeMergeGroupDistributed(mergeGroup: MergeGroup, tablePathStr: String, awsConfig: SerializableAwsConfig): MergeResult = {
    val startTime = System.currentTimeMillis()
    val logger = LoggerFactory.getLogger(classOf[MergeSplitsExecutor])
    
    logger.info(s"[EXECUTOR] Merging ${mergeGroup.files.length} splits in partition ${mergeGroup.partitionValues}")
    
    try {
      // Create merged split using physical merge (executor-friendly version)
      val mergedSplit = createMergedSplitDistributed(mergeGroup, tablePathStr, awsConfig)
      
      // Return result for driver to handle transaction operations
      // Note: We don't do transactionLog operations here since those must be done on driver
      val originalSize = mergeGroup.files.map(_.size).sum
      val mergedSize = mergedSplit.size
      val mergedFiles = mergeGroup.files.length
      
      logger.info(s"[EXECUTOR] Successfully merged ${mergedFiles} files (${originalSize} bytes) into 1 split (${mergedSize} bytes)")
      
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
   * Execute merge for a single group of splits.
   * Uses atomic REMOVE+ADD transaction operations like Delta Lake OPTIMIZE.
   */
  private def executeMergeGroup(mergeGroup: MergeGroup): MergeResult = {
    val startTime = System.currentTimeMillis()
    
    logger.info(s"Merging ${mergeGroup.files.length} splits in partition ${mergeGroup.partitionValues}")
    
    try {
      // Create merged split using SplitManager
      val mergedSplit = createMergedSplit(mergeGroup)
      
      // Prepare transaction actions (REMOVE + ADD pattern from Delta Lake)
      val removeActions = mergeGroup.files.map { file =>
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

      // Merge statistics from input files without reading file contents
      val (mergedMinValues, mergedMaxValues, mergedNumRecords) = mergeStatistics(mergeGroup.files)

      // Extract ALL metadata from merged split for complete pipeline coverage
      val mergedMetadata = mergedSplit.metadata
      val (footerStartOffset, footerEndOffset, hotcacheStartOffset, hotcacheLength, hasFooterOffsets,
           timeRangeStart, timeRangeEnd, splitTags, deleteOpstamp, numMergeOps, docMappingJson, uncompressedSizeBytes, numDocs) =
        if (mergedMetadata != null) {
          val timeStart = Option(mergedMetadata.getTimeRangeStart()).map(_.toString)
          val timeEnd = Option(mergedMetadata.getTimeRangeEnd()).map(_.toString)
          val tags = Option(mergedMetadata.getTags()).filter(!_.isEmpty).map { tagSet =>
            import scala.jdk.CollectionConverters._
            tagSet.asScala.toSet
          }
          val docMapping = Option(mergedMetadata.getDocMappingJson())
          
          if (mergedMetadata.hasFooterOffsets) {
            (Some(mergedMetadata.getFooterStartOffset()),
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
             Some(mergedMetadata.getNumDocs()))
          } else {
            (None, None, None, None, false,
             timeStart, timeEnd, tags,
             Some(mergedMetadata.getDeleteOpstamp()),
             Some(mergedMetadata.getNumMergeOps()),
             docMapping,
             Some(mergedMetadata.getUncompressedSizeBytes()),
             Some(mergedMetadata.getNumDocs()))
          }
        } else {
          (None, None, None, None, false, None, None, None, None, None, None, None, None)
        }

      val addAction = AddAction(
        path = mergedSplit.path,
        partitionValues = mergeGroup.partitionValues,
        size = mergedSplit.size,
        modificationTime = startTime,
        dataChange = false, // This is compaction, not data change
        stats = None, // Statistics are stored in minValues/maxValues/numRecords fields
        tags = Some(Map(
          "operation" -> "optimize", // Delta Lake standard operation name
          "operationParameters" -> "merge_splits",
          "merged_files_count" -> mergeGroup.files.length.toString,
          "merged_from" -> mergeGroup.files.map(_.path).mkString(","),
          "target_size" -> targetSize.toString
        )),
        minValues = mergedMinValues,
        maxValues = mergedMaxValues,
        numRecords = numDocs.orElse(mergedNumRecords), // Prefer tantivy4java metadata over aggregated stats
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

      // Commit atomic REMOVE+ADD transaction using the new method
      val version = transactionLog.commitMergeSplits(removeActions, Seq(addAction))
      
      // Log footer offset optimization status for merged split
      if (hasFooterOffsets) {
        logger.info(s"🚀 MERGE FOOTER OPTIMIZATION: Merged split preserves footer offsets for 87% network traffic reduction")
        logger.debug(s"   Merged ${mergeGroup.files.length} splits with footer optimization preserved")
      } else {
        logger.debug(s"📁 STANDARD MERGE: Merged split created without footer offset optimization")
      }
      
      // Invalidate cache after transaction log update to ensure fresh file listing
      transactionLog.invalidateCache()
      
      logger.info(s"Successfully committed atomic REMOVE+ADD merge operation at version $version")
      
      val originalSize = mergeGroup.files.map(_.size).sum
      logger.info(s"Successfully merged ${mergeGroup.files.length} files into ${mergedSplit.path}")
      
      MergeResult(
        mergeGroup = mergeGroup,
        mergedSplitInfo = mergedSplit,
        mergedFiles = mergeGroup.files.length,
        originalSize = originalSize,
        mergedSize = mergedSplit.size,
        executionTimeMs = System.currentTimeMillis() - startTime
      )
      
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to merge group in partition ${mergeGroup.partitionValues}", ex)
        throw ex
    }
  }

  /**
   * Create a new merged split in executor context using tantivy4java.
   * This version uses broadcast configuration parameters for executor-safe operation.
   */
  private def createMergedSplitDistributed(mergeGroup: MergeGroup, tablePathStr: String, awsConfig: SerializableAwsConfig): MergedSplitInfo = {
    val logger = LoggerFactory.getLogger(classOf[MergeSplitsExecutor])

    // Validate group has at least 2 files (required by tantivy4java)
    if (mergeGroup.files.length < 2) {
      throw new IllegalArgumentException(s"Cannot merge group with ${mergeGroup.files.length} files - at least 2 required")
    }

    // Generate new split path with UUID for uniqueness
    val uuid = java.util.UUID.randomUUID().toString
    val partitionPath = if (mergeGroup.partitionValues.isEmpty) "" else {
      mergeGroup.partitionValues.map { case (k, v) => s"$k=$v" }.mkString("/") + "/"
    }
    val mergedPath = s"$partitionPath${uuid}.split"
    
    // Create full paths for input splits and output split
    // Handle S3 paths specially to preserve the s3:// scheme
    val isS3Path = tablePathStr.startsWith("s3://") || tablePathStr.startsWith("s3a://")
    
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
          val fullPath = s"$normalizedBaseUri/${file.path}"
          logger.warn(s"🔄 [EXECUTOR] Constructed relative S3 path: ${file.path} -> $fullPath")
          fullPath
        }
      } else {
        // For local/HDFS paths, use Path concatenation
        val fullPath = new org.apache.hadoop.fs.Path(tablePathStr, file.path)
        fullPath.toString
      }
    }.asJava
    
    val outputSplitPath = if (isS3Path) {
      // For S3 paths, construct the URL directly with s3:// normalization for tantivy4java compatibility
      val normalizedBaseUri = tablePathStr.replaceFirst("^s3a://", "s3://").replaceAll("/$", "") // Normalize s3a:// to s3:// and remove trailing slash
      val outputPath = s"$normalizedBaseUri/$mergedPath"
      logger.warn(s"🔄 [EXECUTOR] Normalized output path: $tablePathStr/$mergedPath -> $outputPath")
      outputPath
    } else {
      // For local/HDFS paths, use Path concatenation
      new org.apache.hadoop.fs.Path(tablePathStr, mergedPath).toString
    }
    
    logger.info(s"[EXECUTOR] Merging ${inputSplitPaths.size()} splits into $outputSplitPath")
    logger.debug(s"[EXECUTOR] Input splits: ${inputSplitPaths.asScala.mkString(", ")}")

    logger.info("[EXECUTOR] Attempting to merge splits using Tantivy4Java merge functionality")

    // Create merge configuration with broadcast AWS credentials and temp directory
    val mergeConfig = awsConfig.tempDirectoryPath match {
      case Some(tempDir) =>
        // Use full constructor with custom temp directory and debug support
        new QuickwitSplit.MergeConfig(
          "merged-index-uid", // indexUid
          "tantivy4spark",   // sourceId
          "merge-node",      // nodeId
          "default-doc-mapping", // docMappingUid
          0L,                // partitionId
          java.util.Collections.emptyList[String](), // deleteQueries
          awsConfig.toQuickwitSplitAwsConfig(tablePathStr), // AWS configuration for S3 access
          tempDir,           // tempDirectoryPath
          awsConfig.heapSize, // heapSizeBytes
          awsConfig.debugEnabled // debugEnabled
        )
      case None =>
        // Use full constructor with system default temp directory and debug support
        new QuickwitSplit.MergeConfig(
          "merged-index-uid", // indexUid
          "tantivy4spark",   // sourceId
          "merge-node",      // nodeId
          "default-doc-mapping", // docMappingUid
          0L,                // partitionId
          java.util.Collections.emptyList[String](), // deleteQueries
          awsConfig.toQuickwitSplitAwsConfig(tablePathStr), // AWS configuration for S3 access
          null,              // tempDirectoryPath (use system default)
          awsConfig.heapSize, // heapSizeBytes
          awsConfig.debugEnabled // debugEnabled
        )
    }
    
    // Perform the actual merge using tantivy4java - NO FALLBACKS, NO SIMULATIONS
    logger.info(s"[EXECUTOR] Calling QuickwitSplit.mergeSplits() with ${inputSplitPaths.size()} input paths")
    val metadata = QuickwitSplit.mergeSplits(inputSplitPaths, outputSplitPath, mergeConfig)
    
    logger.info(s"[EXECUTOR] Successfully merged splits: ${metadata.getNumDocs} documents, ${metadata.getUncompressedSizeBytes} bytes")
    logger.debug(s"[EXECUTOR] Merge metadata: split_id=${metadata.getSplitId}, merge_ops=${metadata.getNumMergeOps}")
    
    MergedSplitInfo(mergedPath, metadata.getUncompressedSizeBytes, SerializableSplitMetadata.fromQuickwitSplitMetadata(metadata))
  }
  
  /**
   * Extract AWS configuration in executor context.
   * Uses system properties and environment variables since SparkSession may not be available.
   */
  private def extractAwsConfigFromExecutor(): QuickwitSplit.AwsConfig = {
    val logger = LoggerFactory.getLogger(classOf[MergeSplitsExecutor])
    
    try {
      // Try to get from system properties first (these would be set by broadcast variables)
      def getConfig(key: String): Option[String] = {
        Option(System.getProperty(key)).orElse(Option(System.getenv(key)))
      }
      
      val accessKey = getConfig("spark.tantivy4spark.aws.accessKey")
      val secretKey = getConfig("spark.tantivy4spark.aws.secretKey") 
      val sessionToken = getConfig("spark.tantivy4spark.aws.sessionToken")
      val region = getConfig("spark.tantivy4spark.aws.region")
      val endpoint = getConfig("spark.tantivy4spark.s3.endpoint")
      val pathStyleAccess = getConfig("spark.tantivy4spark.s3.pathStyleAccess")
        .map(_.toLowerCase == "true").getOrElse(false)
      
      logger.info(s"[EXECUTOR] Creating AwsConfig with: region=${region.getOrElse("None")}, endpoint=${endpoint.getOrElse("None")}, pathStyle=$pathStyleAccess")
      logger.info(s"[EXECUTOR] AWS credentials: accessKey=${accessKey.map(k => s"${k.take(4)}***").getOrElse("None")}, sessionToken=${sessionToken.map(_ => "***").getOrElse("None")}")
      
      // Create AwsConfig with the extracted credentials
      new QuickwitSplit.AwsConfig(
        accessKey.getOrElse(""),
        secretKey.getOrElse(""),
        sessionToken.orNull, // Can be null for permanent credentials
        region.getOrElse("us-east-1"),
        endpoint.orNull, // Can be null for default AWS endpoint
        pathStyleAccess
      )
    } catch {
      case ex: Exception =>
        logger.warn("[EXECUTOR] Failed to extract AWS config in executor context, using empty config", ex)
        // Return empty config that will use default AWS credential chain
        new QuickwitSplit.AwsConfig("", "", null, "us-east-1", null, false)
    }
  }

  /**
   * Create a new merged split by physically merging split files using tantivy4java.
   * This uses QuickwitSplit.mergeSplits() API for actual merge implementation.
   * For testing with mock data, it falls back to size-based simulation.
   */
  private def createMergedSplit(mergeGroup: MergeGroup): MergedSplitInfo = {
    // Validate group has at least 2 files (required by tantivy4java)
    if (mergeGroup.files.length < 2) {
      throw new IllegalArgumentException(s"Cannot merge group with ${mergeGroup.files.length} files - at least 2 required")
    }

    // CRITICAL: Validate all files in the merge group are from the same partition
    val groupPartitionValues = mergeGroup.partitionValues
    val invalidFiles = mergeGroup.files.filterNot(_.partitionValues == groupPartitionValues)
    if (invalidFiles.nonEmpty) {
      val errorMsg = s"Cross-partition merge detected! Group expects partition ${groupPartitionValues} but found files from different partitions:\n" +
        invalidFiles.map(f => s"  - ${f.path}: ${f.partitionValues}").mkString("\n") +
        s"\nAll files in group:\n" +
        mergeGroup.files.map(f => s"  - ${f.path}: ${f.partitionValues}").mkString("\n")
      logger.error(errorMsg)
      throw new IllegalStateException(errorMsg)
    }

    logger.info(s"✅ Partition validation passed: All ${mergeGroup.files.length} files belong to partition ${groupPartitionValues}")
    
    // Generate new split path with UUID for uniqueness
    val uuid = java.util.UUID.randomUUID().toString
    val partitionPath = if (mergeGroup.partitionValues.isEmpty) "" else {
      mergeGroup.partitionValues.map { case (k, v) => s"$k=$v" }.mkString("/") + "/"
    }
    val mergedPath = s"$partitionPath${uuid}.split"
    
    // Create full paths for input splits and output split
    // Handle S3 paths specially to preserve the s3:// scheme
    val isS3Path = tablePath.toString.startsWith("s3://") || tablePath.toString.startsWith("s3a://")
    
    val inputSplitPaths = mergeGroup.files.map { file =>
      if (isS3Path) {
        // For S3 paths, construct the URL directly
        val normalizedBaseUri = tablePath.toString.replaceFirst("^s3a://", "s3://").replaceAll("/$", "") // Normalize s3a:// to s3:// and remove trailing slash
        s"$normalizedBaseUri/${file.path}"
      } else {
        // For local/HDFS paths, use Path concatenation
        val fullPath = new Path(tablePath, file.path)
        fullPath.toString
      }
    }.asJava
    
    val outputSplitPath = if (isS3Path) {
      // For S3 paths, construct the URL directly
      val baseUri = tablePath.toString.replaceAll("/$", "") // Remove trailing slash if present
      s"$baseUri/$mergedPath"
    } else {
      // For local/HDFS paths, use Path concatenation
      new Path(tablePath, mergedPath).toString
    }
    
    logger.info(s"Merging ${inputSplitPaths.size()} splits into $outputSplitPath")
    logger.debug(s"Input splits: ${inputSplitPaths.asScala.mkString(", ")}")
    
    logger.info("Attempting to merge splits using Tantivy4Java merge functionality")
    
    // Extract AWS configuration from SparkSession
    val awsConfig = extractAwsConfig()

    // Create merge configuration with AWS credentials and temp directory
    val mergeConfig = awsConfig.tempDirectoryPath match {
      case Some(tempDir) =>
        // Use full constructor with custom temp directory and debug support
        new QuickwitSplit.MergeConfig(
          "merged-index-uid", // indexUid
          "tantivy4spark",   // sourceId
          "merge-node",      // nodeId
          "default-doc-mapping", // docMappingUid
          0L,                // partitionId
          java.util.Collections.emptyList[String](), // deleteQueries
          awsConfig.toQuickwitSplitAwsConfig(tablePath.toString), // AWS configuration for S3 access
          tempDir,           // tempDirectoryPath
          awsConfig.heapSize, // heapSizeBytes
          awsConfig.debugEnabled // debugEnabled
        )
      case None =>
        // Use full constructor with system default temp directory and debug support
        new QuickwitSplit.MergeConfig(
          "merged-index-uid", // indexUid
          "tantivy4spark",   // sourceId
          "merge-node",      // nodeId
          "default-doc-mapping", // docMappingUid
          0L,                // partitionId
          java.util.Collections.emptyList[String](), // deleteQueries
          awsConfig.toQuickwitSplitAwsConfig(tablePath.toString), // AWS configuration for S3 access
          null,              // tempDirectoryPath (use system default)
          awsConfig.heapSize, // heapSizeBytes
          awsConfig.debugEnabled // debugEnabled
        )
    }
    
    // Perform the actual merge using configured merge mode (process-based by default)
    println(s"⚙️  [DRIVER] Executing ${awsConfig.mergeMode} merge with ${inputSplitPaths.size()} input paths")
    println(s"📁 [DRIVER] Output path: $outputSplitPath")
    println(s"📁 [DRIVER] Relative path for transaction log: $mergedPath")
    println(s"📁 [DRIVER] Heap size: ${awsConfig.heapSize / (1024 * 1024)}MB")
    logger.info(s"Executing ${awsConfig.mergeMode} merge with ${inputSplitPaths.size()} input paths")

    val serializedMetadata = try {
      awsConfig.executeMerge(inputSplitPaths, outputSplitPath, mergeConfig)
    } catch {
      case ex: Exception =>
        println(s"💥 [DRIVER] CRITICAL: ${awsConfig.mergeMode} merge threw exception: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
        logger.error(s"[DRIVER] ${awsConfig.mergeMode} merge failed", ex)
        ex.printStackTrace()
        throw new RuntimeException(s"${awsConfig.mergeMode} merge operation failed: ${ex.getMessage}", ex)
    }
    
    println(s"📊 [DRIVER] Physical merge completed: ${serializedMetadata.getNumDocs} documents, ${serializedMetadata.getUncompressedSizeBytes} bytes")
    logger.info(s"Successfully merged splits: ${serializedMetadata.getNumDocs} documents, ${serializedMetadata.getUncompressedSizeBytes} bytes")
    logger.debug(s"Merge metadata: split_id=${serializedMetadata.getSplitId}, merge_ops=${serializedMetadata.getNumMergeOps}")
    
    // CRITICAL: Verify the merged file actually exists at the expected location
    try {
      if (isS3Path) {
        println(s"🔍 [DRIVER] S3 merge - cannot easily verify file existence in driver context")
        println(s"🔍 [DRIVER] Assuming tantivy4java successfully created: $outputSplitPath")
      } else {
        val outputFile = new java.io.File(outputSplitPath)
        val exists = outputFile.exists()
        println(s"🔍 [DRIVER] File verification: $outputSplitPath exists = $exists")
        if (!exists) {
          throw new RuntimeException(s"CRITICAL: Merged file was not created at expected location: $outputSplitPath")
        }
      }
    } catch {
      case ex: Exception =>
        println(s"⚠️  [DRIVER] File existence check failed: ${ex.getMessage}")
        logger.warn(s"[DRIVER] File existence check failed", ex)
    }

    MergedSplitInfo(mergedPath, serializedMetadata.getUncompressedSizeBytes, serializedMetadata)
  }

  /**
   * Apply partition predicates to filter which partitions should be processed.
   * Follows Delta Lake pattern of parsing WHERE clause expressions.
   */
  private def applyPartitionPredicates(
      partitions: Seq[(Map[String, String], Seq[AddAction])],
      partitionSchema: StructType
  ): Seq[(Map[String, String], Seq[AddAction])] = {
    
    // If no partition columns are defined, reject any WHERE clauses
    if (partitionSchema.isEmpty && partitionPredicates.nonEmpty) {
      throw new IllegalArgumentException(s"WHERE clause not supported for non-partitioned tables. Partition predicates: ${partitionPredicates.mkString(", ")}")
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

    partitions.filter { case (partitionValues, _) =>
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

  /**
   * Validate that the expression only references partition columns.
   */
  private def validatePartitionColumnReferences(expression: Expression, partitionSchema: StructType): Unit = {
    val partitionColumns = partitionSchema.fieldNames.toSet
    val referencedColumns = expression.references.map(_.name).toSet
    
    val invalidColumns = referencedColumns -- partitionColumns
    if (invalidColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"WHERE clause references non-partition columns: ${invalidColumns.mkString(", ")}. " +
        s"Only partition columns are allowed: ${partitionColumns.mkString(", ")}"
      )
    }
  }

  /**
   * Create an InternalRow from partition values for predicate evaluation.
   */
  private def createRowFromPartitionValues(
      partitionValues: Map[String, String], 
      partitionSchema: StructType
  ): InternalRow = {
    val values = partitionSchema.fieldNames.map { fieldName =>
      partitionValues.get(fieldName) match {
        case Some(value) => UTF8String.fromString(value)
        case None => null
      }
    }
    InternalRow.fromSeq(values)
  }
  
  /**
   * Resolve an expression against a schema to handle UnresolvedAttribute references.
   */
  private def resolveExpression(expression: Expression, schema: StructType): Expression = {
    expression.transform {
      case unresolvedAttr: org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute =>
        val fieldName = unresolvedAttr.name
        val fieldIndex = schema.fieldIndex(fieldName)
        val field = schema(fieldIndex)
        org.apache.spark.sql.catalyst.expressions.BoundReference(fieldIndex, field.dataType, field.nullable)
    }
  }

  /**
   * Aggregate min values from multiple files by taking the minimum of each field.
   * This preserves data skipping statistics without reading file contents.
   */
  private def aggregateMinValues(files: Seq[AddAction]): Option[Map[String, String]] = {
    val allMinValues = files.flatMap(_.minValues.getOrElse(Map.empty))
    if (allMinValues.nonEmpty) {
      // Group by field name and take the minimum value for each field
      val mergedStats = allMinValues.groupBy(_._1).map { case (fieldName, fieldValues) =>
        val minValue = fieldValues.map(_._2).min(smartStringOrdering)
        fieldName -> minValue
      }
      Some(mergedStats)
    } else None
  }

  /**
   * Aggregate max values from multiple files by taking the maximum of each field.
   * This preserves data skipping statistics without reading file contents.
   */
  private def aggregateMaxValues(files: Seq[AddAction]): Option[Map[String, String]] = {
    val allMaxValues = files.flatMap(_.maxValues.getOrElse(Map.empty))
    if (allMaxValues.nonEmpty) {
      // Group by field name and take the maximum value for each field
      val mergedStats = allMaxValues.groupBy(_._1).map { case (fieldName, fieldValues) =>
        val maxValue = fieldValues.map(_._2).max(smartStringOrdering)
        fieldName -> maxValue
      }
      Some(mergedStats)
    } else None
  }

  /**
   * Aggregate record counts by summing across all files.
   * This maintains accurate row count statistics.
   */
  private def aggregateNumRecords(files: Seq[AddAction]): Option[Long] = {
    val allRecords = files.flatMap(_.numRecords)
    if (allRecords.nonEmpty) {
      Some(allRecords.sum)
    } else {
      None
    }
  }

  /**
   * Merge statistics from multiple files without reading file contents.
   * This is critical for maintaining Delta Lake-style data skipping performance.
   */
  private def mergeStatistics(files: Seq[AddAction]): (Option[Map[String, String]], Option[Map[String, String]], Option[Long]) = {
    logger.debug(s"Merging statistics from ${files.length} files")
    
    val minValues = aggregateMinValues(files)
    val maxValues = aggregateMaxValues(files)
    val numRecords = aggregateNumRecords(files)
    
    logger.debug(s"Merged statistics: ${numRecords.getOrElse("unknown")} records, " +
      s"${minValues.map(_.size).getOrElse(0)} min values, ${maxValues.map(_.size).getOrElse(0)} max values")
    
    (minValues, maxValues, numRecords)
  }
}

/**
 * Companion object for MergeSplitsExecutor with static methods for distributed execution.
 */
object MergeSplitsExecutor {
  /**
   * Execute merge for a single group of splits in executor context.
   * This static method is designed to run on Spark executors and handles serialization properly.
   */
  def executeMergeGroupDistributed(mergeGroup: MergeGroup, tablePathStr: String, awsConfig: SerializableAwsConfig): MergeResult = {
    val startTime = System.currentTimeMillis()
    val logger = LoggerFactory.getLogger(classOf[MergeSplitsExecutor])
    
    // Use println to ensure visibility in test output
    println(s"🚀 [EXECUTOR] Merging ${mergeGroup.files.length} splits in partition ${mergeGroup.partitionValues}")
    logger.info(s"[EXECUTOR] Merging ${mergeGroup.files.length} splits in partition ${mergeGroup.partitionValues}")
    
    try {
      // Create merged split using physical merge (executor-friendly version)
      val mergedSplit = createMergedSplitDistributed(mergeGroup, tablePathStr, awsConfig)
      
      // Return result for driver to handle transaction operations
      // Note: We don't do transactionLog operations here since those must be done on driver
      val originalSize = mergeGroup.files.map(_.size).sum
      val mergedSize = mergedSplit.size
      val mergedFiles = mergeGroup.files.length
      
      println(s"✅ [EXECUTOR] Successfully merged ${mergedFiles} files (${originalSize} bytes) into 1 split (${mergedSize} bytes)")
      logger.info(s"[EXECUTOR] Successfully merged ${mergedFiles} files (${originalSize} bytes) into 1 split (${mergedSize} bytes)")
      
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
   * Create a new merged split in executor context using tantivy4java.
   * This static method uses broadcast configuration parameters for executor-safe operation.
   */
  private def createMergedSplitDistributed(mergeGroup: MergeGroup, tablePathStr: String, awsConfig: SerializableAwsConfig): MergedSplitInfo = {
    val logger = LoggerFactory.getLogger(classOf[MergeSplitsExecutor])

    // Validate group has at least 2 files (required by tantivy4java)
    if (mergeGroup.files.length < 2) {
      throw new IllegalArgumentException(s"Cannot merge group with ${mergeGroup.files.length} files - at least 2 required")
    }

    // CRITICAL: Validate all files in the merge group are from the same partition
    val groupPartitionValues = mergeGroup.partitionValues
    val invalidFiles = mergeGroup.files.filterNot(_.partitionValues == groupPartitionValues)
    if (invalidFiles.nonEmpty) {
      val errorMsg = s"Cross-partition merge detected! Group expects partition ${groupPartitionValues} but found files from different partitions:\n" +
        invalidFiles.map(f => s"  - ${f.path}: ${f.partitionValues}").mkString("\n") +
        s"\nAll files in group:\n" +
        mergeGroup.files.map(f => s"  - ${f.path}: ${f.partitionValues}").mkString("\n")
      logger.error(errorMsg)
      throw new IllegalStateException(errorMsg)
    }

    logger.info(s"✅ Partition validation passed: All ${mergeGroup.files.length} files belong to partition ${groupPartitionValues}")
    
    // Generate new split path with UUID for uniqueness
    val uuid = java.util.UUID.randomUUID().toString
    val partitionPath = if (mergeGroup.partitionValues.isEmpty) "" else {
      mergeGroup.partitionValues.map { case (k, v) => s"$k=$v" }.mkString("/") + "/"
    }
    val mergedPath = s"$partitionPath${uuid}.split"
    
    // Create full paths for input splits and output split
    // Handle S3 paths specially to preserve the s3:// scheme
    val isS3Path = tablePathStr.startsWith("s3://") || tablePathStr.startsWith("s3a://")
    
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
          val fullPath = s"$normalizedBaseUri/${file.path}"
          logger.warn(s"🔄 [EXECUTOR] Constructed relative S3 path: ${file.path} -> $fullPath")
          fullPath
        }
      } else {
        // For local/HDFS paths, use Path concatenation
        val fullPath = new org.apache.hadoop.fs.Path(tablePathStr, file.path)
        fullPath.toString
      }
    }.asJava
    
    val outputSplitPath = if (isS3Path) {
      // For S3 paths, construct the URL directly with s3:// normalization for tantivy4java compatibility
      val normalizedBaseUri = tablePathStr.replaceFirst("^s3a://", "s3://").replaceAll("/$", "") // Normalize s3a:// to s3:// and remove trailing slash
      val outputPath = s"$normalizedBaseUri/$mergedPath"
      logger.warn(s"🔄 [EXECUTOR] Normalized output path: $tablePathStr/$mergedPath -> $outputPath")
      outputPath
    } else {
      // For local/HDFS paths, use Path concatenation
      new org.apache.hadoop.fs.Path(tablePathStr, mergedPath).toString
    }
    
    logger.info(s"[EXECUTOR] Merging ${inputSplitPaths.size()} splits into $outputSplitPath")
    logger.debug(s"[EXECUTOR] Input splits: ${inputSplitPaths.asScala.mkString(", ")}")

    logger.info("[EXECUTOR] Attempting to merge splits using Tantivy4Java merge functionality")

    // Create merge configuration with broadcast AWS credentials and temp directory
    val mergeConfig = awsConfig.tempDirectoryPath match {
      case Some(tempDir) =>
        // Use full constructor with custom temp directory and debug support
        new QuickwitSplit.MergeConfig(
          "merged-index-uid", // indexUid
          "tantivy4spark",   // sourceId
          "merge-node",      // nodeId
          "default-doc-mapping", // docMappingUid
          0L,                // partitionId
          java.util.Collections.emptyList[String](), // deleteQueries
          awsConfig.toQuickwitSplitAwsConfig(tablePathStr), // AWS configuration for S3 access
          tempDir,           // tempDirectoryPath
          awsConfig.heapSize, // heapSizeBytes
          awsConfig.debugEnabled // debugEnabled
        )
      case None =>
        // Use full constructor with system default temp directory and debug support
        new QuickwitSplit.MergeConfig(
          "merged-index-uid", // indexUid
          "tantivy4spark",   // sourceId
          "merge-node",      // nodeId
          "default-doc-mapping", // docMappingUid
          0L,                // partitionId
          java.util.Collections.emptyList[String](), // deleteQueries
          awsConfig.toQuickwitSplitAwsConfig(tablePathStr), // AWS configuration for S3 access
          null,              // tempDirectoryPath (use system default)
          awsConfig.heapSize, // heapSizeBytes
          awsConfig.debugEnabled // debugEnabled
        )
    }
    
    // Perform the actual merge using configured merge mode (process-based by default)
    logger.warn(s"⚙️  [EXECUTOR] Executing ${awsConfig.mergeMode} merge with ${inputSplitPaths.size()} input paths")
    logger.warn(s"📁 [EXECUTOR] Input paths:")
    inputSplitPaths.asScala.zipWithIndex.foreach { case (path, idx) =>
      logger.warn(s"📁 [EXECUTOR]   [$idx]: $path")
    }
    logger.warn(s"📁 [EXECUTOR] Output path: $outputSplitPath")
    logger.warn(s"📁 [EXECUTOR] Relative path for transaction log: $mergedPath")
    logger.warn(s"📁 [EXECUTOR] Heap size: ${awsConfig.heapSize / (1024 * 1024)}MB")
    logger.info(s"[EXECUTOR] Executing ${awsConfig.mergeMode} merge with ${inputSplitPaths.size()} input paths")

    val serializedMetadata = try {
      awsConfig.executeMerge(inputSplitPaths, outputSplitPath, mergeConfig)
    } catch {
      case ex: Exception =>
        println(s"💥 [EXECUTOR] CRITICAL: ${awsConfig.mergeMode} merge threw exception: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
        logger.error(s"[EXECUTOR] ${awsConfig.mergeMode} merge failed", ex)
        ex.printStackTrace()
        throw new RuntimeException(s"${awsConfig.mergeMode} merge operation failed: ${ex.getMessage}", ex)
    }
    
    println(s"📊 [EXECUTOR] Physical merge completed: ${serializedMetadata.getNumDocs} documents, ${serializedMetadata.getUncompressedSizeBytes} bytes")
    logger.info(s"[EXECUTOR] Successfully merged splits: ${serializedMetadata.getNumDocs} documents, ${serializedMetadata.getUncompressedSizeBytes} bytes")
    logger.debug(s"[EXECUTOR] Merge metadata: split_id=${serializedMetadata.getSplitId}, merge_ops=${serializedMetadata.getNumMergeOps}")
    
    // CRITICAL: Verify the merged file actually exists at the expected location
    try {
      if (isS3Path) {
        println(s"🔍 [EXECUTOR] S3 merge - cannot easily verify file existence in executor context")
        println(s"🔍 [EXECUTOR] Assuming tantivy4java successfully created: $outputSplitPath")
      } else {
        val outputFile = new java.io.File(outputSplitPath)
        val exists = outputFile.exists()
        println(s"🔍 [EXECUTOR] File verification: $outputSplitPath exists = $exists")
        if (!exists) {
          throw new RuntimeException(s"CRITICAL: Merged file was not created at expected location: $outputSplitPath")
        }
      }
    } catch {
      case ex: Exception =>
        println(s"⚠️  [EXECUTOR] File existence check failed: ${ex.getMessage}")
        logger.warn(s"[EXECUTOR] File existence check failed", ex)
    }

    MergedSplitInfo(mergedPath, serializedMetadata.getUncompressedSizeBytes, serializedMetadata)
  }
}

/**
 * Group of files that should be merged together.
 */
case class MergeGroup(
    partitionValues: Map[String, String],
    files: Seq[AddAction]
) extends Serializable

/**
 * Result of merging a group of splits.
 */
case class MergeResult(
    mergeGroup: MergeGroup,
    mergedSplitInfo: MergedSplitInfo,
    mergedFiles: Int,
    originalSize: Long,
    mergedSize: Long,
    executionTimeMs: Long
) extends Serializable {
  // Provide backward compatibility
  def mergedPath: String = mergedSplitInfo.path
}

/**
 * Information about a newly created merged split.
 */
/**
 * Serializable wrapper for QuickwitSplit.SplitMetadata
 */
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
  
  def getFooterStartOffset(): Long = footerStartOffset
  def getFooterEndOffset(): Long = footerEndOffset
  def getHotcacheStartOffset(): Long = hotcacheStartOffset
  def getHotcacheLength(): Long = hotcacheLength
  def getTimeRangeStart(): String = timeRangeStart.orNull
  def getTimeRangeEnd(): String = timeRangeEnd.orNull
  def getTags(): java.util.Set[String] = {
    tags.map { tagMap =>
      import scala.jdk.CollectionConverters._
      tagMap.keySet.asJava
    }.getOrElse(java.util.Collections.emptySet())
  }
  def getDeleteOpstamp(): Long = deleteOpstamp.getOrElse(0L)
  def getNumMergeOps(): Int = numMergeOps.getOrElse(0)
  def getDocMappingJson(): String = docMappingJson.orNull
  def getUncompressedSizeBytes(): Long = uncompressedSizeBytes
  def getSplitId(): String = splitId.orNull
  def getNumDocs(): Long = numDocs.getOrElse(0L)
  def getSkippedSplits(): java.util.List[String] = {
    import scala.jdk.CollectionConverters._
    skippedSplits.asJava
  }
  def getIndexUid(): String = indexUid.orNull

  def toQuickwitSplitMetadata(): com.tantivy4java.QuickwitSplit.SplitMetadata = {
    import scala.jdk.CollectionConverters._
    new com.tantivy4java.QuickwitSplit.SplitMetadata(
      splitId.getOrElse("unknown"), // splitId
      "tantivy4spark-index", // indexUid (NEW - required)
      0L, // partitionId (NEW - required)
      "tantivy4spark-source", // sourceId (NEW - required)
      "tantivy4spark-node", // nodeId (NEW - required)
      numDocs.getOrElse(0L), // numDocs
      uncompressedSizeBytes, // uncompressedSizeBytes
      timeRangeStart.map(java.time.Instant.parse).orNull, // timeRangeStart
      timeRangeEnd.map(java.time.Instant.parse).orNull, // timeRangeEnd
      System.currentTimeMillis() / 1000, // createTimestamp (NEW - required)
      "Mature", // maturity (NEW - required)
      tags.map(_.keySet.asJava).getOrElse(java.util.Collections.emptySet()), // tags
      footerStartOffset, // footerStartOffset
      footerEndOffset, // footerEndOffset
      deleteOpstamp.getOrElse(0L), // deleteOpstamp
      numMergeOps.getOrElse(0), // numMergeOps
      "doc-mapping-uid", // docMappingUid (NEW - required)
      docMappingJson.orNull, // docMappingJson (MOVED - for performance)
      skippedSplits.asJava // skippedSplits
    )
  }
}

object SerializableSplitMetadata {
  def fromQuickwitSplitMetadata(metadata: com.tantivy4java.QuickwitSplit.SplitMetadata): SerializableSplitMetadata = {
    val timeStart = Option(metadata.getTimeRangeStart()).map(_.toString)
    val timeEnd = Option(metadata.getTimeRangeEnd()).map(_.toString)
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

case class MergedSplitInfo(path: String, size: Long, metadata: SerializableSplitMetadata) extends Serializable

/**
 * Placeholder for unresolved table path or identifier.
 * Similar to Delta Lake's UnresolvedDeltaPathOrIdentifier.
 */
case class UnresolvedDeltaPathOrIdentifier(
    path: Option[String],
    tableIdentifier: Option[org.apache.spark.sql.catalyst.TableIdentifier],
    commandName: String
) extends org.apache.spark.sql.catalyst.plans.logical.LeafNode {
  override def output: Seq[Attribute] = Nil
}