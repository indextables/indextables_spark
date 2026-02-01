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

package io.indextables.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.fs.Path

import io.indextables.spark.core.{IndexTables4SparkMultiSplitInputPartition, IndexTables4SparkReaderFactory}
import io.indextables.spark.storage.DriverSplitLocalityManager
import io.indextables.spark.transaction.{AddAction, PartitionPruning, TransactionLogInterface}

import org.slf4j.LoggerFactory

/**
 * MicroBatchStream implementation for IndexTables4Spark.
 *
 * This class implements Spark Structured Streaming's MicroBatchStream interface,
 * providing incremental data processing capabilities for IndexTables4Spark tables.
 *
 * Key behaviors:
 * - Uses transaction log versions as streaming offsets
 * - Filters out merged splits (dataChange=false) to avoid duplicate data
 * - Reuses existing partition reader infrastructure
 * - No limit on results per split (returns all matching documents)
 *
 * @param sparkSession Active Spark session
 * @param tablePath Path to the IndexTables4Spark table
 * @param transactionLog Transaction log interface for file listing
 * @param readSchema Schema for reading data (may be pruned)
 * @param fullTableSchema Full table schema for type lookup
 * @param partitionColumns Partition column names
 * @param pushedFilters Filters pushed down from Spark
 * @param indexQueryFilters IndexQuery filters for full-text search
 * @param config Configuration map
 */
class IndexTables4SparkMicroBatchStream(
    sparkSession: SparkSession,
    tablePath: String,
    transactionLog: TransactionLogInterface,
    readSchema: StructType,
    fullTableSchema: StructType,
    partitionColumns: Seq[String],
    pushedFilters: Array[Filter],
    indexQueryFilters: Array[Any],
    config: Map[String, String]
) extends MicroBatchStream {

  private val logger = LoggerFactory.getLogger(getClass)

  // Offset cache to avoid repeated transaction log reads during rapid polling
  @volatile private var cachedLatestOffset: Option[IndexTables4SparkStreamingOffset] = None
  @volatile private var cachedLatestOffsetTimestamp: Long = 0L
  private val OFFSET_CACHE_TTL_MS = 1000L // 1 second cache

  // ============================================================================
  // MicroBatchStream Interface
  // ============================================================================

  /**
   * Returns the latest available offset.
   *
   * This reads the current transaction log version and returns it as an offset.
   * Results are cached briefly to avoid hammering the transaction log during
   * rapid polling by Spark's streaming engine.
   */
  override def latestOffset(): Offset = {
    val now = System.currentTimeMillis()

    cachedLatestOffset match {
      case Some(offset) if (now - cachedLatestOffsetTimestamp) < OFFSET_CACHE_TTL_MS =>
        logger.debug(s"Using cached latest offset: version ${offset.version}")
        offset
      case _ =>
        val currentVersion = transactionLog.getCurrentVersion()
        val offset = IndexTables4SparkStreamingOffset(
          version = currentVersion,
          timestamp = Some(now)
        )
        cachedLatestOffset = Some(offset)
        cachedLatestOffsetTimestamp = now
        logger.debug(s"Fetched latest offset: version ${offset.version}")
        offset
    }
  }

  /**
   * Returns the initial offset for a fresh streaming query.
   *
   * Supports multiple modes (Delta Lake compatible):
   * - startingOffset="earliest": Start from version -1 (all data)
   * - startingOffset="latest": Start from current version (new data only)
   * - startingVersion=N: Start from specific version N (processes version N onwards)
   * - startingTimestamp="2024-01-01T00:00:00": Start from version at or after timestamp
   *
   * Priority: startingTimestamp > startingVersion > startingOffset
   */
  override def initialOffset(): Offset = {
    logger.debug(s"initialOffset called, config keys: ${config.keys.mkString(", ")}")

    // Helper to get config value with case-insensitive fallback
    def getConfig(key: String): Option[String] =
      config.get(key).orElse(config.get(key.toLowerCase))

    // Check for startingTimestamp first (highest priority, Delta Lake compatible)
    val timestampOpt = getConfig("spark.indextables.streaming.startingTimestamp")
    val versionOpt = getConfig("spark.indextables.streaming.startingVersion")
    val offsetOpt = getConfig("spark.indextables.streaming.startingOffset")

    timestampOpt match {
      case Some(timestampStr) =>
        val timestamp = parseTimestamp(timestampStr)
        transactionLog.getVersionAtOrAfterTimestamp(timestamp) match {
          case Some(version) =>
            // Convert to exclusive bound (start processing from this version)
            val offsetVersion = version - 1
            logger.info(s"Starting streaming from timestamp $timestampStr (version $version, offset $offsetVersion)")
            IndexTables4SparkStreamingOffset(version = offsetVersion)
          case None =>
            throw new IllegalArgumentException(
              s"No version found at or after timestamp: $timestampStr"
            )
        }

      case None => versionOpt match {
        case Some(versionStr) =>
          try {
            val version = versionStr.toLong - 1 // Convert to exclusive bound
            logger.info(s"Starting streaming from explicit version $versionStr (offset version $version)")
            IndexTables4SparkStreamingOffset(version = version)
          } catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(
                s"Invalid startingVersion: $versionStr. Must be a valid version number."
              )
          }

        case None =>
          // Fall back to startingOffset (original option)
          val startingOffset = offsetOpt.getOrElse("latest")
          logger.debug(s"startingOffset value = '$startingOffset'")

          startingOffset.toLowerCase match {
            case "earliest" =>
              logger.info("Starting streaming from earliest offset (version -1)")
              IndexTables4SparkStreamingOffset(version = -1L)
            case "latest" =>
              val offset = latestOffset().asInstanceOf[IndexTables4SparkStreamingOffset]
              logger.info(s"Starting streaming from latest offset (version ${offset.version})")
              offset
            case versionStr =>
              try {
                val version = versionStr.toLong - 1 // Convert to exclusive bound
                logger.info(s"Starting streaming from explicit version $versionStr (offset version $version)")
                IndexTables4SparkStreamingOffset(version = version)
              } catch {
                case _: NumberFormatException =>
                  throw new IllegalArgumentException(
                    s"Invalid starting offset: $versionStr. Use 'earliest', 'latest', or a version number."
                  )
              }
          }
      }
    }
  }

  /**
   * Parse a timestamp string into milliseconds since epoch.
   * Supports ISO-8601 format and epoch milliseconds.
   */
  private def parseTimestamp(timestampStr: String): Long = {
    import java.time.{Instant, LocalDateTime, ZoneOffset}
    import java.time.format.DateTimeFormatter

    try {
      // Try parsing as epoch milliseconds first
      timestampStr.toLong
    } catch {
      case _: NumberFormatException =>
        try {
          // Try ISO-8601 with timezone (e.g., "2024-01-01T00:00:00Z")
          Instant.parse(timestampStr).toEpochMilli
        } catch {
          case _: Exception =>
            try {
              // Try ISO-8601 without timezone (assumes UTC)
              val dt = LocalDateTime.parse(timestampStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
              dt.toInstant(ZoneOffset.UTC).toEpochMilli
            } catch {
              case _: Exception =>
                try {
                  // Try date-only format (e.g., "2024-01-01")
                  val dt = LocalDateTime.parse(timestampStr + "T00:00:00", DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                  dt.toInstant(ZoneOffset.UTC).toEpochMilli
                } catch {
                  case _: Exception =>
                    throw new IllegalArgumentException(
                      s"Invalid timestamp format: $timestampStr. " +
                        "Use ISO-8601 format (e.g., '2024-01-01T00:00:00Z', '2024-01-01T00:00:00', '2024-01-01') " +
                        "or epoch milliseconds."
                    )
                }
            }
        }
    }
  }

  /**
   * Deserializes an offset from JSON (used for checkpoint recovery).
   */
  override def deserializeOffset(json: String): Offset = {
    IndexTables4SparkStreamingOffset.fromJson(json)
  }

  /**
   * Plans input partitions for a micro-batch.
   *
   * CRITICAL: This method filters out merged splits (dataChange=false) to prevent
   * streaming the same data twice. When splits are merged:
   * - Original splits had dataChange=true (streamed when added)
   * - Merged result has dataChange=false (should NOT be streamed)
   *
   * @param start Exclusive start offset (process versions > start.version)
   * @param end Inclusive end offset (process versions <= end.version)
   */
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffset = start.asInstanceOf[IndexTables4SparkStreamingOffset]
    val endOffset = end.asInstanceOf[IndexTables4SparkStreamingOffset]

    logger.debug(s"Planning partitions for streaming batch: versions ${startOffset.version + 1} to ${endOffset.version}")

    if (startOffset.version >= endOffset.version) {
      logger.debug("No new versions to process")
      return Array.empty
    }

    // Get files added between versions using the transaction log
    val addedFiles = transactionLog.getFilesAddedBetweenVersions(
      fromVersion = startOffset.version,
      toVersion = endOffset.version
    )

    logger.debug(s"Found ${addedFiles.size} files added between versions ${startOffset.version + 1} and ${endOffset.version}")

    // CRITICAL: Filter out merged splits to avoid data duplication
    // Merged splits have dataChange=false - they contain the same data
    // that was already streamed when the original splits were added
    val dataChangeFiles = addedFiles.filter(_.dataChange)
    val skippedMergedFiles = addedFiles.size - dataChangeFiles.size

    if (skippedMergedFiles > 0) {
      logger.info(s"Filtered out $skippedMergedFiles merged splits (dataChange=false) to avoid duplicates")
    }

    if (dataChangeFiles.isEmpty) {
      logger.debug("No data change files to process after filtering merged splits")
      return Array.empty
    }

    // Apply partition pruning if partition filters exist
    val partitionPrunedFiles = applyPartitionPruning(dataChangeFiles)

    if (partitionPrunedFiles.size < dataChangeFiles.size) {
      logger.info(s"After partition pruning: ${partitionPrunedFiles.size} files to process " +
        s"(pruned ${dataChangeFiles.size - partitionPrunedFiles.size} files)")
    }

    // Create input partitions using existing infrastructure
    // Note: Data skipping (min/max stats) is applied during batch read via the existing
    // IndexTables4SparkScan infrastructure. For streaming, partition pruning is the
    // primary optimization since we're processing incremental additions.
    createInputPartitions(partitionPrunedFiles)
  }

  /**
   * Creates the partition reader factory.
   *
   * IMPORTANT: Streaming has no limit - returns all matching documents from each split.
   * This reuses the existing IndexTables4SparkReaderFactory with limit = None.
   */
  override def createReaderFactory(): PartitionReaderFactory = {
    new IndexTables4SparkReaderFactory(
      readSchema = readSchema,
      limit = None, // STREAMING: No limit - return all results
      config = config,
      tablePath = new Path(tablePath),
      metricsAccumulator = None
    )
  }

  /**
   * Called when a micro-batch completes successfully.
   *
   * The actual offset persistence is handled by Spark's checkpoint mechanism.
   * This method is primarily used for logging and metrics.
   */
  override def commit(end: Offset): Unit = {
    val endOffset = end.asInstanceOf[IndexTables4SparkStreamingOffset]
    logger.info(s"Committed streaming batch up to version ${endOffset.version}")
  }

  /**
   * Called when the streaming query stops.
   */
  override def stop(): Unit = {
    logger.info("Stopping IndexTables4Spark streaming source")
    cachedLatestOffset = None
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  /**
   * Apply partition pruning using pushed partition filters.
   */
  private def applyPartitionPruning(files: Seq[AddAction]): Seq[AddAction] = {
    if (partitionColumns.isEmpty || pushedFilters.isEmpty) {
      return files
    }

    // Extract partition-only filters
    val partitionFilters = pushedFilters.filter { filter =>
      val cols = getFilterReferencedColumns(filter)
      cols.nonEmpty && cols.forall(partitionColumns.contains)
    }

    if (partitionFilters.isEmpty) {
      return files
    }

    logger.debug(s"Applying ${partitionFilters.length} partition filters for pruning")

    // Use existing partition pruning logic
    PartitionPruning.prunePartitions(files, partitionColumns, partitionFilters)
  }

  /**
   * Create input partitions using existing IndexTables4SparkMultiSplitInputPartition.
   */
  private def createInputPartitions(files: Seq[AddAction]): Array[InputPartition] = {
    if (files.isEmpty) {
      return Array.empty
    }

    val splitsPerTask = config.getOrElse(
      "spark.indextables.streaming.splitsPerTask",
      config.getOrElse("spark.indextables.read.splitsPerTask", "4")
    ).toInt

    // Use DriverSplitLocalityManager for locality assignment (reuse batch infrastructure)
    val sparkContext = sparkSession.sparkContext
    val availableHosts = DriverSplitLocalityManager.getAvailableHosts(sparkContext)
    val splitPaths = files.map(_.path)
    val assignments = DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, availableHosts)

    logger.debug(s"Assigned ${files.size} splits across ${availableHosts.size} available hosts")

    // Group by host for locality-aware batching
    val filesByHost = files.groupBy(f => assignments.getOrElse(f.path, "unknown"))

    // Create partitions using existing MultiSplitInputPartition
    var partitionId = 0
    filesByHost.flatMap { case (host, hostFiles) =>
      hostFiles.grouped(splitsPerTask).map { batch =>
        val partition = new IndexTables4SparkMultiSplitInputPartition(
          addActions = batch,
          readSchema = readSchema,
          fullTableSchema = fullTableSchema,
          filters = pushedFilters,
          partitionId = partitionId,
          limit = None, // STREAMING: No limit
          indexQueryFilters = indexQueryFilters,
          preferredHost = if (host == "unknown") None else Some(host)
        )
        partitionId += 1
        partition
      }
    }.toArray
  }

  /**
   * Extract column names referenced by a filter.
   */
  private def getFilterReferencedColumns(filter: Filter): Set[String] = {
    filter match {
      case EqualTo(attr, _)            => Set(attr)
      case EqualNullSafe(attr, _)      => Set(attr)
      case GreaterThan(attr, _)        => Set(attr)
      case GreaterThanOrEqual(attr, _) => Set(attr)
      case LessThan(attr, _)           => Set(attr)
      case LessThanOrEqual(attr, _)    => Set(attr)
      case In(attr, _)                 => Set(attr)
      case IsNull(attr)                => Set(attr)
      case IsNotNull(attr)             => Set(attr)
      case And(left, right)            => getFilterReferencedColumns(left) ++ getFilterReferencedColumns(right)
      case Or(left, right)             => getFilterReferencedColumns(left) ++ getFilterReferencedColumns(right)
      case Not(child)                  => getFilterReferencedColumns(child)
      case StringStartsWith(attr, _)   => Set(attr)
      case StringEndsWith(attr, _)     => Set(attr)
      case StringContains(attr, _)     => Set(attr)
      case _                           => Set.empty
    }
  }
}
