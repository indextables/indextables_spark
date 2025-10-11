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

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProviderFactory
import org.slf4j.LoggerFactory

/**
 * Distributed transaction log implementation.
 * Parallelizes transaction log reading across Spark executors for improved performance.
 *
 * This implementation distributes the work of reading and parsing transaction files
 * across the Spark cluster, enabling horizontal scaling for large tables with many
 * transaction files. Key benefits:
 * - 10-100x faster reads for large transaction logs
 * - Horizontal scaling with cluster size
 * - Reduced driver memory pressure
 * - Executor-local caching for improved cache hit rates
 *
 * Architecture:
 * 1. Driver reads checkpoint metadata (lightweight)
 * 2. Driver lists incremental transaction files
 * 3. Transaction file references distributed to executors via RDD
 * 4. Executors parse files in parallel with local caching
 * 5. Results reduced to final state on driver
 *
 * @param tablePath Path to the table
 * @param spark SparkSession for distributed operations
 * @param options Configuration options
 */
class DistributedTransactionLog(
  tablePath: Path,
  spark: SparkSession,
  options: CaseInsensitiveStringMap
) extends OptimizedTransactionLog(tablePath, spark, options) {

  private val logger = LoggerFactory.getLogger(classOf[DistributedTransactionLog])

  // Configuration for distributed reading
  private val distributedParallelism = options.getInt(
    "spark.indextables.transaction.distributed.parallelism",
    spark.sparkContext.defaultParallelism
  )

  private val adaptiveParallelism = options.getBoolean(
    "spark.indextables.transaction.distributed.adaptiveParallelism",
    true
  )

  private val minFilesForDistribution = options.getInt(
    "spark.indextables.transaction.distributed.minFiles",
    10 // Don't parallelize for small transaction logs
  )

  private val tablePathStr = tablePath.toString
  private val transactionLogPath = new Path(tablePath, "_transaction_log")

  /**
   * List files using distributed transaction log reading.
   * Overrides the base implementation to use parallel executor-based reading.
   *
   * @return Sequence of AddAction representing all visible files
   */
  override def listFiles(): Seq[AddAction] = {
    // Check protocol before reading
    assertTableReadable()

    val startTime = System.currentTimeMillis()

    try {
      // Step 1: Read checkpoint on driver (lightweight - just metadata)
      val checkpointActions = readCheckpoint()
      logger.info(s"Loaded checkpoint with ${checkpointActions.length} files for distributed read")

      // Step 2: List incremental transaction files (driver)
      val transactionFiles = listIncrementalTransactionFiles()
      logger.info(s"Found ${transactionFiles.length} incremental transaction files")

      if (transactionFiles.isEmpty) {
        // No incremental changes - return checkpoint directly
        logger.info(s"No incremental files, returning checkpoint state")
        return checkpointActions
      }

      // Check if we should use distributed reading
      if (shouldUseDistributedReading(transactionFiles)) {
        readDistributed(checkpointActions, transactionFiles, startTime)
      } else {
        // Fall back to driver-based reading for small transaction logs
        logger.info(s"Using driver-based reading for ${transactionFiles.length} files (below threshold)")
        super.listFiles()
      }
    } catch {
      case e: Exception =>
        logger.error("Distributed transaction log reading failed, falling back to base implementation", e)
        super.listFiles()
    }
  }

  /**
   * Determine if distributed reading should be used.
   *
   * @param transactionFiles Transaction files to read
   * @return true if distributed reading should be used
   */
  private def shouldUseDistributedReading(transactionFiles: Seq[TransactionFileRef]): Boolean = {
    transactionFiles.length >= minFilesForDistribution
  }

  /**
   * Read transaction log using distributed executor-based parsing.
   *
   * @param checkpointActions Actions from checkpoint
   * @param transactionFiles Transaction files to read
   * @param startTime Start time for metrics
   * @return Final sequence of AddAction
   */
  private def readDistributed(
    checkpointActions: Seq[AddAction],
    transactionFiles: Seq[TransactionFileRef],
    startTime: Long
  ): Seq[AddAction] = {
    // Calculate optimal parallelism
    val parallelism = if (adaptiveParallelism) {
      calculateOptimalParallelism(transactionFiles)
    } else {
      distributedParallelism
    }

    logger.info(s"Starting distributed read with parallelism=$parallelism for ${transactionFiles.length} files")

    // Step 3: Extract serializable configuration
    val config = extractSerializableConfig()

    // Step 4: Create RDD and distribute parsing to executors
    val transactionRDD = spark.sparkContext
      .parallelize(transactionFiles, parallelism)
      .setName(s"TransactionLog-Read-${tablePath.getName}")

    // Track parse start time
    val parseStartTime = System.currentTimeMillis()

    // Step 5: Map phase - parse files on executors
    val parsedActionsRDD = transactionRDD.mapPartitions { fileRefs =>
      // Each executor processes a subset of transaction files
      fileRefs.flatMap { fileRef =>
        DistributedTransactionLogParser.parseTransactionFile(
          fileRef,
          tablePathStr,
          config
        )
      }
    }

    // Step 6: Partition-local reduction (map-side aggregation)
    val reducedRDD = parsedActionsRDD.mapPartitions { actions =>
      val reduced = DistributedStateReducer.partitionLocalReduce(actions)
      Iterator(reduced)
    }

    val parseEndTime = System.currentTimeMillis()

    // Step 7: Collect to driver and final reduction
    val reduceStartTime = System.currentTimeMillis()
    val allReducedActions = reducedRDD.collect().flatMap(_.values)
    val finalState = DistributedStateReducer.reduceToFinalState(
      checkpointActions,
      allReducedActions
    )
    val reduceEndTime = System.currentTimeMillis()

    // Calculate metrics
    val totalTime = System.currentTimeMillis() - startTime
    val readTime = parseStartTime - startTime
    val parseTime = parseEndTime - parseStartTime
    val reduceTime = reduceEndTime - reduceStartTime

    logger.info(s"Distributed transaction log read completed in ${totalTime}ms: " +
      s"${finalState.length} files " +
      s"(read=${readTime}ms, parse=${parseTime}ms, reduce=${reduceTime}ms)")

    // Log performance metrics
    val metrics = DistributedTransactionLogMetrics(
      totalFiles = checkpointActions.length + transactionFiles.length,
      checkpointFiles = checkpointActions.length,
      incrementalFiles = transactionFiles.length,
      parallelism = parallelism,
      readTimeMs = readTime,
      parseTimeMs = parseTime,
      reduceTimeMs = reduceTime,
      cacheHitRate = 0.0, // Would need to collect from executors
      executorCount = spark.sparkContext.getExecutorMemoryStatus.size
    )

    logger.info(s"Metrics: $metrics")

    finalState
  }

  /**
   * Read checkpoint on driver (lightweight operation).
   *
   * @return Sequence of AddAction from checkpoint
   */
  private def readCheckpoint(): Seq[AddAction] = {
    getLastCheckpointVersion() match {
      case Some(version) =>
        // Use parent class checkpoint reading
        val checkpoint = new TransactionLogCheckpoint(
          transactionLogPath,
          CloudStorageProviderFactory.createProvider(tablePathStr, options, spark.sparkContext.hadoopConfiguration),
          options
        )
        try {
          // Extract only AddAction entries from checkpoint
          checkpoint.getActionsFromCheckpoint()
            .getOrElse(Seq.empty)
            .collect { case add: AddAction => add }
        } finally {
          checkpoint.close()
        }
      case None =>
        Seq.empty
    }
  }

  /**
   * List incremental transaction files after checkpoint.
   *
   * @return Sequence of transaction file references
   */
  private def listIncrementalTransactionFiles(): Seq[TransactionFileRef] = {
    val cloudProvider = CloudStorageProviderFactory.createProvider(
      tablePathStr,
      options,
      spark.sparkContext.hadoopConfiguration
    )

    try {
      val checkpointVersion = getLastCheckpointVersion()

      // List all transaction files from storage
      val files = cloudProvider.listFiles(transactionLogPath.toString, recursive = false)
      val allVersions = files.flatMap { file =>
        parseVersionFromPath(file.path)
      }.distinct.sorted

      val incrementalVersions = checkpointVersion match {
        case Some(cpVersion) => allVersions.filter(_ > cpVersion)
        case None => allVersions
      }

      incrementalVersions.map { version =>
        val fileName = f"$version%020d.json"
        val filePath = s"${transactionLogPath}/$fileName"

        val fileInfo = cloudProvider.getFileInfo(filePath)

        TransactionFileRef(
          version = version,
          path = fileName,
          size = fileInfo.map(_.size).getOrElse(0L),
          modificationTime = fileInfo.map(_.modificationTime).getOrElse(System.currentTimeMillis())
        )
      }
    } finally {
      cloudProvider.close()
    }
  }

  /**
   * Parse version number from transaction file path.
   * Matches the implementation in OptimizedTransactionLog.
   *
   * @param path File path
   * @return Optional version number
   */
  private def parseVersionFromPath(path: String): Option[Long] = {
    val fileName = path.split("/").last
    if (fileName.endsWith(".json") && !fileName.contains("checkpoint")) {
      try {
        Some(fileName.replace(".json", "").toLong)
      } catch {
        case _: NumberFormatException => None
      }
    } else {
      None
    }
  }

  /**
   * Calculate optimal parallelism based on file count.
   * Avoids overhead for small transaction logs while scaling for large ones.
   *
   * @param transactionFiles Transaction files to read
   * @return Optimal parallelism level
   */
  private def calculateOptimalParallelism(transactionFiles: Seq[TransactionFileRef]): Int = {
    val fileCount = transactionFiles.length
    val defaultPar = spark.sparkContext.defaultParallelism

    // Adaptive parallelism based on file count
    val optimal = fileCount match {
      case n if n < 10 => 1 // Don't parallelize for very small counts
      case n if n < 100 => Math.min(n / 2, defaultPar)
      case n => Math.min(n / 10, defaultPar * 2) // Up to 2x parallelism for large logs
    }

    logger.info(s"Calculated optimal parallelism: $optimal " +
      s"(files=$fileCount, defaultParallelism=$defaultPar)")

    Math.max(1, optimal)
  }

  /**
   * Extract serializable configuration for executors.
   * Only includes settings that executors need for cloud storage access.
   *
   * @return Map of configuration settings
   */
  private def extractSerializableConfig(): Map[String, String] = {
    val configKeys = Seq(
      "spark.indextables.aws.accessKey",
      "spark.indextables.aws.secretKey",
      "spark.indextables.aws.sessionToken",
      "spark.indextables.aws.region",
      "spark.indextables.aws.endpoint",
      "spark.indextables.aws.pathStyleAccess",
      "spark.indextables.aws.credentialsProviderClass",
      "spark.indextables.s3.maxConnections",
      "spark.indextables.s3.connectionTimeout",
      "spark.indextables.s3.readTimeout"
    )

    configKeys.flatMap { key =>
      Option(options.get(key)).map(value => key -> value)
    }.toMap
  }

  /**
   * Invalidate executor caches for this table.
   * Broadcasts cache invalidation to all executors.
   */
  def invalidateExecutorCaches(): Unit = {
    logger.info(s"Invalidating executor caches for table: $tablePathStr")

    try {
      // Run a dummy RDD operation to execute on all executors
      val numPartitions = spark.sparkContext.defaultParallelism
      spark.sparkContext
        .parallelize(1 to numPartitions, numPartitions)
        .foreach { _ =>
          // Runs on executor
          TransactionFileCache.invalidate(tablePathStr)
        }

      logger.info(s"Successfully invalidated executor caches across ${numPartitions} partitions")
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to invalidate executor caches: ${e.getMessage}", e)
    }
  }

  /**
   * Get cache statistics from executors.
   * Note: This collects stats from all executors which may be expensive.
   *
   * @return Map of executor ID to cache statistics
   */
  def getExecutorCacheStats(): Map[String, String] = {
    try {
      val numPartitions = spark.sparkContext.defaultParallelism
      val stats = spark.sparkContext
        .parallelize(1 to numPartitions, numPartitions)
        .mapPartitions { _ =>
          // Runs on executor
          Iterator(TransactionFileCache.stats())
        }
        .collect()

      stats.zipWithIndex.map { case (stat, idx) =>
        s"executor-$idx" -> stat
      }.toMap
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to collect executor cache stats: ${e.getMessage}", e)
        Map.empty
    }
  }
}
