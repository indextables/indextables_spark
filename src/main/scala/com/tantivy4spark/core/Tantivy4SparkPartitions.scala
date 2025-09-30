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


package com.tantivy4spark.core

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.transaction.{AddAction, PartitionUtils}
import com.tantivy4spark.search.{TantivySearchEngine, SplitSearchEngine}
import com.tantivy4spark.storage.{SplitCacheConfig, GlobalSplitCacheManager, SplitLocationRegistry, BroadcastSplitLocalityManager}
import com.tantivy4spark.prewarm.PreWarmManager
import com.tantivy4spark.util.StatisticsCalculator
import java.util.UUID
import com.tantivy4spark.io.{CloudStorageProviderFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory
import java.io.{IOException, ByteArrayOutputStream}
import scala.jdk.CollectionConverters._

/**
 * Utility for consistent path resolution across different scan types.
 */
object PathResolutionUtils {

  /**
   * Resolves a path from AddAction against a table path, handling absolute and relative paths correctly.
   *
   * @param splitPath Path from AddAction (could be relative like "part-00000-xxx.split" or absolute)
   * @param tablePath Base table path for resolving relative paths
   * @return Resolved Hadoop Path object
   */
  def resolveSplitPath(splitPath: String, tablePath: String): Path = {
    if (isAbsolutePath(splitPath)) {
      // Already absolute path - handle file:/ URIs properly
      if (splitPath.startsWith("file:")) {
        // For file:/ URIs, use the URI directly rather than Hadoop Path constructor
        // to avoid path resolution issues
        new Path(java.net.URI.create(splitPath))
      } else {
        new Path(splitPath)
      }
    } else {
      // Relative path, resolve against table path
      new Path(tablePath, splitPath)
    }
  }

  /**
   * Resolves a path and returns it as a string suitable for tantivy4java.
   *
   * @param splitPath Path from AddAction
   * @param tablePath Base table path for resolving relative paths
   * @return Resolved path as string
   */
  def resolveSplitPathAsString(splitPath: String, tablePath: String): String = {
    if (isAbsolutePath(splitPath)) {
      // Already absolute path - handle file:/ URIs properly
      if (splitPath.startsWith("file:")) {
        // Keep file URIs as URIs for tantivy4java to avoid working directory resolution issues
        splitPath
      } else {
        splitPath
      }
    } else {
      // Relative path, resolve against table path
      // Handle case where tablePath might already be a file:/ URI to avoid double-prefixing
      if (tablePath.startsWith("file:")) {
        // Convert file URI to local path, resolve, then convert back to avoid Path constructor issues
        val tableDirPath = new java.io.File(java.net.URI.create(tablePath)).getAbsolutePath
        new java.io.File(tableDirPath, splitPath).getAbsolutePath
      } else {
        new Path(tablePath, splitPath).toString
      }
    }
  }

  /**
   * Checks if a path is absolute (starts with "/", contains "://" for URLs, or starts with "file:").
   */
  private def isAbsolutePath(path: String): Boolean = {
    path.startsWith("/") || path.contains("://") || path.startsWith("file:")
  }
}

class Tantivy4SparkInputPartition(
    val addAction: AddAction,
    val readSchema: StructType,
    val filters: Array[Filter],
    val partitionId: Int,
    val limit: Option[Int] = None,
    val indexQueryFilters: Array[Any] = Array.empty
) extends InputPartition {

  /**
   * Provide preferred locations for this partition based on split cache locality.
   * Spark will try to schedule tasks on these hosts to take advantage of cached splits.
   * Uses broadcast-based locality information for accurate cluster-wide cache tracking.
   */
  override def preferredLocations(): Array[String] = {
    println(s"🎯 [PARTITION-${partitionId}] preferredLocations() called for split: ${addAction.path}")

    val preferredHosts = BroadcastSplitLocalityManager.getPreferredHosts(addAction.path)
    if (preferredHosts.nonEmpty) {
      println(s"🎯 [PARTITION-${partitionId}] Using broadcast preferred hosts: ${preferredHosts.mkString(", ")}")
      preferredHosts
    } else {
      println(s"🎯 [PARTITION-${partitionId}] No broadcast hosts found, trying legacy registry")
      // Fallback to legacy registry for compatibility
      val legacyHosts = SplitLocationRegistry.getPreferredHosts(addAction.path)
      if (legacyHosts.nonEmpty) {
        println(s"🎯 [PARTITION-${partitionId}] Using legacy preferred hosts: ${legacyHosts.mkString(", ")}")
        legacyHosts
      } else {
        println(s"🎯 [PARTITION-${partitionId}] No preferred hosts found - letting Spark decide")
        // No cache history available, let Spark decide
        Array.empty[String]
      }
    }
  }
}

class Tantivy4SparkReaderFactory(
    readSchema: StructType,
    limit: Option[Int] = None,
    config: Map[String, String],  // Direct config instead of broadcast
    tablePath: Path
) extends PartitionReaderFactory {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkReaderFactory])

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val tantivyPartition = partition.asInstanceOf[Tantivy4SparkInputPartition]
    logger.info(s"Creating reader for partition ${tantivyPartition.partitionId}")

    new Tantivy4SparkPartitionReader(
      tantivyPartition.addAction,
      readSchema,
      tantivyPartition.filters,
      tantivyPartition.limit.orElse(limit),
      config,
      tablePath,
      tantivyPartition.indexQueryFilters
    )
  }
}

class Tantivy4SparkPartitionReader(
    addAction: AddAction,
    readSchema: StructType,
    filters: Array[Filter],
    limit: Option[Int] = None,
    config: Map[String, String],  // Direct config instead of broadcast
    tablePath: Path,
    indexQueryFilters: Array[Any] = Array.empty
) extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkPartitionReader])

  // Calculate effective limit: use pushed limit or fall back to default 5000
  private val effectiveLimit: Int = limit.getOrElse(5000)

  // Resolve relative path from AddAction against table path
  private val filePath = PathResolutionUtils.resolveSplitPathAsString(addAction.path, tablePath.toString)

  private var splitSearchEngine: SplitSearchEngine = _
  private var resultIterator: Iterator[InternalRow] = Iterator.empty
  private var initialized = false


  private def createCacheConfig(): SplitCacheConfig = {
    logger.error(s"🔍 ENTERING createCacheConfig - parsing configuration values...")

    // Access the broadcast configuration in executor
    val broadcasted = config

    // Debug: Log broadcast configuration received in executor
    logger.error(s"🔍 PartitionReader received ${broadcasted.size} broadcast configs")
    broadcasted.foreach { case (k, v) =>
      val safeValue = Option(v).getOrElse("null")
      logger.error(s"🔍 Broadcast config: $k -> $safeValue")
    }

    // Helper function to get config from broadcast with defaults
    def getBroadcastConfig(configKey: String, default: String = ""): String = {
      val value = broadcasted.getOrElse(configKey, default)
      val safeValue = Option(value).getOrElse(default)
      logger.error(s"🔍 PartitionReader broadcast config for $configKey: ${Option(safeValue).getOrElse("null")}")
      safeValue
    }

    def getBroadcastConfigOption(configKey: String): Option[String] = {
      // Try both the original key and lowercase version (CaseInsensitiveStringMap lowercases keys)
      val value = broadcasted.get(configKey).orElse(broadcasted.get(configKey.toLowerCase))
      logger.info(s"🔍 PartitionReader broadcast config for $configKey: ${value.getOrElse("None")}")
      value
    }

    val cacheConfig = SplitCacheConfig(
      cacheName = {
        val configName = getBroadcastConfig("spark.indextables.cache.name", "")
        if (configName.trim().nonEmpty) {
          configName.trim()
        } else {
          // Use table path as cache name for table-specific caching
          val safeTablePath = Option(tablePath).map(_.toString).getOrElse("unknown")
          s"tantivy4spark-${safeTablePath.replaceAll("[^a-zA-Z0-9]", "_")}"
        }
      },
      maxCacheSize = {
        val value = getBroadcastConfig("spark.indextables.cache.maxSize", "200000000")
        try {
          value.toLong
        } catch {
          case e: NumberFormatException =>
            logger.error(s"Invalid numeric value for spark.indextables.cache.maxSize: '$value'")
            throw e
        }
      },
      maxConcurrentLoads = {
        val value = getBroadcastConfig("spark.indextables.cache.maxConcurrentLoads", "8")
        try {
          value.toInt
        } catch {
          case e: NumberFormatException =>
            logger.error(s"Invalid numeric value for spark.indextables.cache.maxConcurrentLoads: '$value'")
            throw e
        }
      },
      enableQueryCache = getBroadcastConfig("spark.indextables.cache.queryCache", "true").toBoolean,
      splitCachePath = getBroadcastConfigOption("spark.indextables.cache.directoryPath")
        .orElse(com.tantivy4spark.storage.SplitCacheConfig.getDefaultCachePath()),
      // AWS configuration from broadcast
      awsAccessKey = getBroadcastConfigOption("spark.indextables.aws.accessKey"),
      awsSecretKey = getBroadcastConfigOption("spark.indextables.aws.secretKey"),
      awsSessionToken = getBroadcastConfigOption("spark.indextables.aws.sessionToken"),
      awsRegion = getBroadcastConfigOption("spark.indextables.aws.region"),
      awsEndpoint = getBroadcastConfigOption("spark.indextables.s3.endpoint"),
      awsPathStyleAccess = getBroadcastConfigOption("spark.indextables.s3.pathStyleAccess").map(_.toBoolean),
      // Azure configuration from broadcast
      azureAccountName = getBroadcastConfigOption("spark.indextables.azure.accountName"),
      azureAccountKey = getBroadcastConfigOption("spark.indextables.azure.accountKey"),
      azureConnectionString = getBroadcastConfigOption("spark.indextables.azure.connectionString"),
      azureEndpoint = getBroadcastConfigOption("spark.indextables.azure.endpoint"),
      // GCP configuration from broadcast
      gcpProjectId = getBroadcastConfigOption("spark.indextables.gcp.projectId"),
      gcpServiceAccountKey = getBroadcastConfigOption("spark.indextables.gcp.serviceAccountKey"),
      gcpCredentialsFile = getBroadcastConfigOption("spark.indextables.gcp.credentialsFile"),
      gcpEndpoint = getBroadcastConfigOption("spark.indextables.gcp.endpoint")
    )

    // Debug: Log final cache configuration
    logger.debug(s"🔍 Created SplitCacheConfig with AWS region: ${cacheConfig.awsRegion.getOrElse("None")}")
    logger.debug(s"🔍 Created SplitCacheConfig with AWS endpoint: ${cacheConfig.awsEndpoint.getOrElse("None")}")
    logger.debug(s"🔍 Created SplitCacheConfig with AWS pathStyleAccess: ${cacheConfig.awsPathStyleAccess.getOrElse("None")}")

    cacheConfig
  }

  private def initialize(): Unit = {
    if (!initialized) {
      try {
        logger.error(s"🔍 ENTERING initialize() for split: ${addAction.path}")
        logger.info(s"🔍 V2 PartitionReader initializing for split: ${addAction.path}")

        // Record that this host has accessed this split for future scheduling locality
        val currentHostname = SplitLocationRegistry.getCurrentHostname
        // Record in both systems for transition period
        SplitLocationRegistry.recordSplitAccess(addAction.path, currentHostname)
        BroadcastSplitLocalityManager.recordSplitAccess(addAction.path, currentHostname)
        logger.debug(s"Recorded split access for locality: ${addAction.path} on host $currentHostname")

        // Check if pre-warm is enabled and try to join warmup future
        val broadcasted = config
        val isPreWarmEnabled = broadcasted.getOrElse("spark.indextables.cache.prewarm.enabled", "true").toBoolean
        if (isPreWarmEnabled) {
          // Generate query hash from filters for warmup future lookup
          val allFilters = filters.asInstanceOf[Array[Any]] ++ indexQueryFilters
          val queryHash = generateQueryHash(allFilters)
          val warmupJoined = PreWarmManager.joinWarmupFuture(addAction.path, queryHash, isPreWarmEnabled)
          if (warmupJoined) {
            logger.info(s"🔥 Successfully joined warmup future for split: ${addAction.path}")
          }
        }

        // Create cache configuration from Spark options
        logger.error(s"🔍 ABOUT TO CALL createCacheConfig()...")
        logger.info(s"🔍 Creating cache configuration for split read...")
        val cacheConfig = createCacheConfig()
        logger.error(s"🔍 createCacheConfig() COMPLETED SUCCESSFULLY")
        logger.info(s"🔍 Cache config created with: awsRegion=${cacheConfig.awsRegion.getOrElse("None")}, awsEndpoint=${cacheConfig.awsEndpoint.getOrElse("None")}")

        // Create split search engine using footer offset optimization when available
        // Use raw filesystem path for tantivy4java compatibility
        val actualPath = if (filePath.toString.startsWith("file:")) {
          // Keep file URIs as URIs for tantivy4java to avoid working directory resolution issues
          filePath.toString
        } else if (filePath.toString.startsWith("s3a://") || filePath.toString.startsWith("s3n://")) {
          // Normalize s3 paths for tantivy4java compatibility (s3a:// -> s3://)
          filePath.toString.replaceFirst("^s3[an]://", "s3://")
        } else {
          filePath.toString
        }

        // Footer offset metadata is required for all split reading operations
        if (!addAction.hasFooterOffsets || addAction.footerStartOffset.isEmpty) {
          throw new RuntimeException(s"AddAction for $actualPath does not contain required footer offsets. All 'add' entries in the transaction log must contain footer offset metadata.")
        }

        // Reconstruct COMPLETE SplitMetadata from AddAction - all fields required for proper operation
        import java.time.Instant
        import scala.jdk.CollectionConverters._

        // Safe conversion functions for Option[Any] to Long to handle JSON deserialization type variations
        def toLongSafeOption(opt: Option[Any]): Long = opt match {
          case Some(value) => value match {
            case l: Long => l
            case i: Int => i.toLong
            case i: java.lang.Integer => i.toLong
            case l: java.lang.Long => l
            case _ => value.toString.toLong
          }
          case None => 0L
        }

        logger.warn(s"🔍 RECONSTRUCTING SplitMetadata from AddAction - docMappingJson: ${if (addAction.docMappingJson.isDefined) s"PRESENT (${addAction.docMappingJson.get.length} chars)" else "MISSING/NULL"}")
        if (addAction.docMappingJson.isDefined) {
          logger.warn(s"🔍 AddAction docMappingJson content preview: ${addAction.docMappingJson.get.take(200)}${if (addAction.docMappingJson.get.length > 200) "..." else ""}")
        }

        val splitMetadata = new com.tantivy4java.QuickwitSplit.SplitMetadata(
          addAction.path.split("/").last.replace(".split", ""), // splitId from filename
          "tantivy4spark-index", // indexUid (NEW - required)
          0L, // partitionId (NEW - required)
          "tantivy4spark-source", // sourceId (NEW - required)
          "tantivy4spark-node", // nodeId (NEW - required)
          toLongSafeOption(addAction.numRecords), // numDocs
          toLongSafeOption(addAction.uncompressedSizeBytes), // uncompressedSizeBytes
          addAction.timeRangeStart.map(Instant.parse).orNull, // timeRangeStart
          addAction.timeRangeEnd.map(Instant.parse).orNull, // timeRangeEnd
          System.currentTimeMillis() / 1000, // createTimestamp (NEW - required)
          "Mature", // maturity (NEW - required)
          addAction.splitTags.getOrElse(Set.empty[String]).asJava, // tags
          toLongSafeOption(addAction.footerStartOffset), // footerStartOffset
          toLongSafeOption(addAction.footerEndOffset), // footerEndOffset
          toLongSafeOption(addAction.deleteOpstamp), // deleteOpstamp
          addAction.numMergeOps.getOrElse(0), // numMergeOps (Int is OK for this field)
          "doc-mapping-uid", // docMappingUid (NEW - required)
          addAction.docMappingJson.orNull, // docMappingJson (MOVED - for performance)
          java.util.Collections.emptyList[String]() // skippedSplits
        )

        // Use full readSchema since partition values are stored directly in splits (consistent with Quickwit)
        splitSearchEngine = SplitSearchEngine.fromSplitFileWithMetadata(readSchema, actualPath, splitMetadata, cacheConfig)

        // Get the schema from the split to validate filters
        val splitSchema = splitSearchEngine.getSchema()
        val splitFieldNames = try {
          import scala.jdk.CollectionConverters._
          val fieldNames = splitSchema.getFieldNames().asScala.toSet
          logger.info(s"Split schema contains fields: ${fieldNames.mkString(", ")}")
          fieldNames
        } catch {
          case e: Exception =>
            logger.warn(s"Could not retrieve field names from split schema: ${e.getMessage}")
            Set.empty[String]
        }

        // Log the filters and limit for debugging
        logger.error(s"🔍 PARTITION DEBUG: Pushdown configuration for ${addAction.path}:")
        logger.error(s"  - Filters: ${filters.length} filter(s) - ${filters.mkString(", ")}")
        logger.error(s"  - IndexQuery Filters: ${indexQueryFilters.length} filter(s) - ${indexQueryFilters.mkString(", ")}")
        logger.error(s"  - Limit: $effectiveLimit")

        // Since partition values are stored in splits, we can apply all filters at the split level
        // (no need to exclude partition filters since they're queryable in the split data)
        val allFilters: Array[Any] = filters.asInstanceOf[Array[Any]] ++ indexQueryFilters
        logger.error(s"  - Combined Filters: ${allFilters.length} total filters")

        // Convert filters to SplitQuery object with schema validation
        val splitQuery = if (allFilters.nonEmpty) {
          // Create options map from broadcast config for field configuration
          import scala.jdk.CollectionConverters._
          val optionsFromBroadcast = new org.apache.spark.sql.util.CaseInsensitiveStringMap(config.asJava)

          val queryObj = if (splitFieldNames.nonEmpty) {
            // Use mixed filter converter to handle both Spark filters and IndexQuery filters
            val validatedQuery = FiltersToQueryConverter.convertToSplitQuery(allFilters, splitSearchEngine, Some(splitFieldNames), Some(optionsFromBroadcast))
            logger.info(s"  - SplitQuery (with schema validation): ${validatedQuery.getClass.getSimpleName}")
            validatedQuery
          } else {
            // Fall back to no schema validation if we can't get field names
            val fallbackQuery = FiltersToQueryConverter.convertToSplitQuery(allFilters, splitSearchEngine, None, Some(optionsFromBroadcast))
            logger.info(s"  - SplitQuery (no schema validation): ${fallbackQuery.getClass.getSimpleName}")
            fallbackQuery
          }
          queryObj
        } else {
          import com.tantivy4java.SplitMatchAllQuery
          new SplitMatchAllQuery() // Use match-all query for no filters
        }

        // Push down SplitQuery and limit to split searcher
        logger.info(s"Executing search with SplitQuery object and limit: $effectiveLimit")
        val searchResults = splitSearchEngine.search(splitQuery, limit = effectiveLimit)
        logger.info(s"Search returned ${searchResults.length} results (pushed limit: $effectiveLimit)")

        // Partition values are stored directly in splits, so no reconstruction needed
        resultIterator = searchResults.iterator
        initialized = true
        logger.info(s"Pushdown complete for ${addAction.path}: splitQuery='$splitQuery', limit=$effectiveLimit, results=${searchResults.length}")

      } catch {
        case ex: Exception =>
          logger.error(s"Failed to initialize reader for ${addAction.path}", ex)
          // Set safe default for resultIterator to prevent NPE in next() calls
          resultIterator = Iterator.empty
          initialized = true
          // Still throw the exception to signal the failure
          throw new IOException(s"Failed to read Tantivy index: ${ex.getMessage}", ex)
      }
    }
  }

  override def next(): Boolean = {
    try {
      initialize()
      if (resultIterator != null) {
        resultIterator.hasNext
      } else {
        false
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Error in next() for ${addAction.path}", ex)
        // Ensure resultIterator is set to avoid repeated failures
        if (resultIterator == null) {
          resultIterator = Iterator.empty
        }
        false
    }
  }

  override def get(): InternalRow = {
    if (resultIterator != null) {
      resultIterator.next()
    } else {
      throw new IllegalStateException(s"No data available for ${addAction.path}")
    }
  }

  override def close(): Unit = {
    if (splitSearchEngine != null) {
      splitSearchEngine.close()
    }
  }

  /**
   * Generate a consistent hash for the query filters to identify warmup futures.
   */
  private def generateQueryHash(allFilters: Array[Any]): String = {
    val filterString = allFilters.map(_.toString).mkString("|")
    java.util.UUID.nameUUIDFromBytes(filterString.getBytes).toString.take(8)
  }
}

class Tantivy4SparkWriterFactory(
    tablePath: Path,
    writeSchema: StructType,
    serializedOptions: Map[String, String],
    serializedHadoopConfig: Map[String, String],  // Use serializable Map instead of Configuration
    partitionColumns: Seq[String] = Seq.empty
) extends DataWriterFactory {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkWriterFactory])

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    logger.info(s"Creating writer for partition $partitionId, task $taskId")
    if (partitionColumns.nonEmpty) {
      logger.info(s"Creating partitioned writer with columns: ${partitionColumns.mkString(", ")}")
    }

    // Reconstruct Hadoop Configuration from serialized properties
    val reconstructedHadoopConf = new org.apache.hadoop.conf.Configuration()
    serializedHadoopConfig.foreach { case (key, value) =>
      reconstructedHadoopConf.set(key, value)
    }

    new Tantivy4SparkDataWriter(tablePath, writeSchema, partitionId, taskId, serializedOptions, reconstructedHadoopConf, partitionColumns)
  }
}

class Tantivy4SparkDataWriter(
    tablePath: Path,
    writeSchema: StructType,
    partitionId: Int,
    taskId: Long,
    serializedOptions: Map[String, String],
    hadoopConf: org.apache.hadoop.conf.Configuration,
    partitionColumns: Seq[String] = Seq.empty  // Partition columns from metadata
) extends DataWriter[InternalRow] {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkDataWriter])

  // Create CaseInsensitiveStringMap from serialized options for components that need it
  private lazy val options: CaseInsensitiveStringMap = {
    import scala.jdk.CollectionConverters._
    new CaseInsensitiveStringMap(serializedOptions.asJava)
  }

  // Normalize table path for consistent S3 protocol handling (s3a:// -> s3://)
  private val normalizedTablePath = {
    val pathStr = tablePath.toString
    if (pathStr.startsWith("s3a://") || pathStr.startsWith("s3n://")) {
      val normalizedStr = pathStr.replaceFirst("^s3[an]://", "s3://")
      new Path(normalizedStr)
    } else {
      tablePath
    }
  }

  // Debug: Log options and hadoop config available in executor
  if (logger.isDebugEnabled) {
    logger.debug("Tantivy4SparkDataWriter executor options:")
    options.entrySet().asScala.foreach { entry =>
      val value = if (entry.getKey.contains("secretKey")) "***" else entry.getValue
      logger.debug(s"  ${entry.getKey} = $value")
    }
    logger.debug("Tantivy4SparkDataWriter hadoop config keys containing 'tantivy4spark':")
    hadoopConf.iterator().asScala.filter(_.getKey.contains("tantivy4spark")).foreach { entry =>
      val value = if (entry.getKey.contains("secretKey")) "***" else entry.getValue
      logger.debug(s"  ${entry.getKey} = $value")
    }
  }

  // For partitioned tables, we need to maintain separate writers per unique partition value combination
  // Key: serialized partition values (e.g., "event_date=2023-01-15"), Value: (searchEngine, statistics, recordCount)
  private val partitionWriters = scala.collection.mutable.Map[String, (TantivySearchEngine, StatisticsCalculator.DatasetStatistics, Long)]()

  // For non-partitioned tables, use a single writer
  private var singleWriter: Option[(TantivySearchEngine, StatisticsCalculator.DatasetStatistics, Long)] =
    if (partitionColumns.isEmpty) Some((new TantivySearchEngine(writeSchema, options, hadoopConf), new StatisticsCalculator.DatasetStatistics(writeSchema), 0L)) else None

  // Debug: log partition columns being used
  logger.warn(s"🔍 DATAWRITER INIT: partition $partitionId, partitionColumns: ${partitionColumns.mkString("[", ", ", "]")}")

  override def write(record: InternalRow): Unit = {
    if (partitionColumns.isEmpty) {
      // Non-partitioned write - use single writer
      val (engine, stats, count) = singleWriter.get
      engine.addDocument(record)
      stats.updateRow(record)
      singleWriter = Some((engine, stats, count + 1))
    } else {
      // Partitioned write - extract partition values and route to appropriate writer
      val partitionValues = PartitionUtils.extractPartitionValues(record, writeSchema, partitionColumns)
      val partitionKey = PartitionUtils.createPartitionPath(partitionValues, partitionColumns)

      // Debug: log record details for the problematic partition
      val idValue = if (writeSchema.fieldNames.contains("id")) {
        try {
          val idIndex = writeSchema.fieldIndex("id")
          if (!record.isNullAt(idIndex)) record.getUTF8String(idIndex).toString else "null"
        } catch {
          case _: Exception => "unknown"
        }
      } else "no-id"

      logger.warn(s"🔍 WRITE DEBUG: partition $partitionId writing record to partition '$partitionKey' with id=$idValue, values=$partitionValues")

      // Get or create writer for this partition value combination
      val (engine, stats, count) = partitionWriters.getOrElseUpdate(partitionKey, {
        logger.info(s"Creating new writer for partition values: $partitionValues")
        (new TantivySearchEngine(writeSchema, options, hadoopConf), new StatisticsCalculator.DatasetStatistics(writeSchema), 0L)
      })

      // Store the complete record in the split (including partition columns)
      engine.addDocument(record)
      stats.updateRow(record)
      partitionWriters(partitionKey) = (engine, stats, count + 1)
    }
  }

  override def commit(): WriterCommitMessage = {
    val allAddActions = scala.collection.mutable.ArrayBuffer[AddAction]()

    // Handle non-partitioned writes
    if (singleWriter.isDefined) {
      val (searchEngine, statistics, recordCount) = singleWriter.get
      if (recordCount == 0) {
        logger.info(s"⚠️  Skipping transaction log entry for partition $partitionId - no records written")
        return Tantivy4SparkCommitMessage(Seq.empty)
      }

      val addAction = commitWriter(searchEngine, statistics, recordCount, Map.empty, "")
      allAddActions += addAction
    }

    // Handle partitioned writes - commit each partition writer separately
    if (partitionWriters.nonEmpty) {
      logger.info(s"Committing ${partitionWriters.size} partition writers")

      partitionWriters.foreach { case (partitionKey, (searchEngine, statistics, recordCount)) =>
        if (recordCount > 0) {
          val partitionValues = parsePartitionKey(partitionKey)
          val addAction = commitWriter(searchEngine, statistics, recordCount, partitionValues, partitionKey)
          allAddActions += addAction
        } else {
          logger.warn(s"Skipping empty partition: $partitionKey")
        }
      }
    }

    if (allAddActions.isEmpty) {
      logger.info(s"⚠️  No records written in partition $partitionId")
      return Tantivy4SparkCommitMessage(Seq.empty)
    }

    logger.info(s"Committed partition $partitionId with ${allAddActions.size} split(s)")
    Tantivy4SparkCommitMessage(allAddActions.toSeq)
  }

  private def parsePartitionKey(partitionKey: String): Map[String, String] = {
    // Parse partition key like "event_date=2023-01-15" into Map("event_date" -> "2023-01-15")
    partitionKey.split("/").map { part =>
      val Array(key, value) = part.split("=", 2)
      key -> value
    }.toMap
  }

  private def commitWriter(
      searchEngine: TantivySearchEngine,
      statistics: StatisticsCalculator.DatasetStatistics,
      recordCount: Long,
      partitionValues: Map[String, String],
      partitionKey: String
  ): AddAction = {
    logger.debug(s"Committing Tantivy index with $recordCount documents for partition: $partitionKey")

    // Create split file name with UUID for guaranteed uniqueness
    // Format: [partitionDir/]part-{partitionId}-{taskId}-{uuid}.split
    val splitId = UUID.randomUUID().toString
    val fileName = f"part-$partitionId%05d-$taskId-$splitId.split"

    // For partitioned tables, create file in partition directory
    val filePath = if (partitionValues.nonEmpty) {
      val partitionDir = new Path(normalizedTablePath, partitionKey)
      new Path(partitionDir, fileName)
    } else {
      new Path(normalizedTablePath, fileName)
    }

    // Use raw filesystem path for tantivy, not file:// URI
    // For S3Mock, apply path flattening via CloudStorageProvider
    val outputPath = if (filePath.toString.startsWith("file:")) {
      // Extract the local filesystem path from file:// URI
      new java.io.File(filePath.toUri).getAbsolutePath
    } else {
      // For cloud paths (S3), normalize the path for storage compatibility
      val normalized = CloudStorageProviderFactory.normalizePathForTantivy(filePath.toString, options, hadoopConf)
      normalized
    }

    // Generate node ID for the split (hostname + executor ID)
    val nodeId = java.net.InetAddress.getLocalHost.getHostName + "-" +
                 Option(System.getProperty("spark.executor.id")).getOrElse("driver")

    // Create split from the index using the search engine
    val (splitPath, splitMetadata) = searchEngine.commitAndCreateSplit(outputPath, partitionId.toLong, nodeId)

    // Get split file size using cloud storage provider
    val splitSize = {
      val cloudProvider = CloudStorageProviderFactory.createProvider(outputPath, options, hadoopConf)
      try {
        val fileInfo = cloudProvider.getFileInfo(outputPath)
        fileInfo.map(_.size).getOrElse {
          logger.warn(s"Could not get file info for $outputPath using cloud provider")
          0L
        }
      } finally {
        cloudProvider.close()
      }
    }

    // Normalize the splitPath for tantivy4java compatibility (convert s3a:// to s3://)
    val _ = {
      val cloudProvider = CloudStorageProviderFactory.createProvider(outputPath, options, hadoopConf)
      try {
        cloudProvider.normalizePathForTantivy(splitPath)
      } finally {
        cloudProvider.close()
      }
    }

    logger.info(s"Created split file $fileName with $splitSize bytes, $recordCount records")

    val minValues = statistics.getMinValues
    val maxValues = statistics.getMaxValues

    // For AddAction path, we need to store the relative path including partition directory
    // Format: [partitionDir/]filename.split
    val addActionPath = if (partitionValues.nonEmpty) {
      // Include partition directory in the path
      s"$partitionKey/$fileName"
    } else if (outputPath != filePath.toString) {
      // Path normalization was applied - calculate relative path from table path to normalized output path
      val tablePath = normalizedTablePath.toString
      val tableUri = java.net.URI.create(tablePath)
      val outputUri = java.net.URI.create(outputPath)

      if (tableUri.getScheme == outputUri.getScheme && tableUri.getHost == outputUri.getHost) {
        // Same scheme and host - calculate relative path
        val tableKey = tableUri.getPath.stripPrefix("/")
        val outputKey = outputUri.getPath.stripPrefix("/")

        // For S3Mock flattening, we need to store the complete relative path that will
        // resolve to the flattened location when combined with the table path
        if (outputKey.contains("___") && !tableKey.contains("___")) {
          // S3Mock flattening occurred - store the entire flattened key relative to bucket
          val flattenedKey = outputKey
          flattenedKey
        } else if (outputKey.startsWith(tableKey)) {
          // Standard case - remove table prefix to get relative path
          val relativePath = outputKey.substring(tableKey.length).stripPrefix("/")
          relativePath
        } else {
          fileName
        }
      } else {
        fileName
      }
    } else {
      fileName  // No normalization was applied
    }

    // Extract ALL metadata from tantivy4java SplitMetadata for complete pipeline coverage
    val (footerStartOffset, footerEndOffset, hasFooterOffsets,
         timeRangeStart, timeRangeEnd, splitTags, deleteOpstamp, numMergeOps, docMappingJson, uncompressedSizeBytes) =
      if (splitMetadata != null) {
        val timeStart = Option(splitMetadata.getTimeRangeStart()).map(_.toString)
        val timeEnd = Option(splitMetadata.getTimeRangeEnd()).map(_.toString)
        val tags = Option(splitMetadata.getTags()).filter(!_.isEmpty).map { tagSet =>
          import scala.jdk.CollectionConverters._
          tagSet.asScala.toSet
        }
        val originalDocMapping = Option(splitMetadata.getDocMappingJson())
        logger.warn(s"🔍 EXTRACTED docMappingJson from tantivy4java: ${if (originalDocMapping.isDefined) s"PRESENT (${originalDocMapping.get.length} chars)" else "MISSING/NULL"}")

        val docMapping = if (originalDocMapping.isDefined) {
          logger.warn(s"🔍 docMappingJson FULL CONTENT: ${originalDocMapping.get}")
          originalDocMapping
        } else {
          // WORKAROUND: If tantivy4java didn't provide docMappingJson, create a minimal schema mapping
          logger.warn(s"🔧 WORKAROUND: tantivy4java docMappingJson is missing - creating minimal field mapping")

          // Create a minimal field mapping that tantivy4java can understand
          // Based on Quickwit/Tantivy schema format expectations
          val fieldMappings = writeSchema.fields.map { field =>
            val fieldType = field.dataType.typeName match {
              case "string" => "text"
              case "integer" | "long" => "i64"
              case "float" | "double" => "f64"
              case "boolean" => "bool"
              case "date" | "timestamp" => "datetime"
              case _ => "text" // Default fallback
            }
            s""""${field.name}": {"type": "$fieldType", "indexed": true}"""
          }.mkString(", ")

          val minimalSchema = s"""{"fields": {$fieldMappings}}"""
          logger.warn(s"🔧 Using minimal field mapping as docMappingJson: ${minimalSchema.take(200)}${if (minimalSchema.length > 200) "..." else ""}")

          Some(minimalSchema)
        }

        if (splitMetadata.hasFooterOffsets()) {
          (Some(splitMetadata.getFooterStartOffset()),
           Some(splitMetadata.getFooterEndOffset()),
           true,
           timeStart,
           timeEnd,
           tags,
           Some(splitMetadata.getDeleteOpstamp()),
           Some(splitMetadata.getNumMergeOps()),
           docMapping,
           Some(splitMetadata.getUncompressedSizeBytes()))
        } else {
          (None, None, false,
           timeStart, timeEnd, tags,
           Some(splitMetadata.getDeleteOpstamp()),
           Some(splitMetadata.getNumMergeOps()),
           docMapping,
           Some(splitMetadata.getUncompressedSizeBytes()))
        }
      } else {
        (None, None, false, None, None, None, None, None, None, None)
      }

    val addAction = AddAction(
      path = addActionPath,  // Use the path that will correctly resolve during read
      partitionValues = partitionValues,  // Use extracted partition values for metadata
      size = splitSize,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(recordCount),
      minValues = if (minValues.nonEmpty) Some(minValues) else None,
      maxValues = if (maxValues.nonEmpty) Some(maxValues) else None,
      // Footer offset optimization metadata for 87% network traffic reduction
      footerStartOffset = footerStartOffset,
      footerEndOffset = footerEndOffset,
      // Hotcache fields deprecated in v0.24.1 - no longer stored in transaction log
      hotcacheStartOffset = None,
      hotcacheLength = None,
      hasFooterOffsets = hasFooterOffsets,
      // Complete tantivy4java SplitMetadata fields for full pipeline coverage
      timeRangeStart = timeRangeStart,
      timeRangeEnd = timeRangeEnd,
      splitTags = splitTags,
      deleteOpstamp = deleteOpstamp,
      numMergeOps = numMergeOps,
      docMappingJson = docMappingJson,
      uncompressedSizeBytes = uncompressedSizeBytes
    )

    if (partitionValues.nonEmpty) {
      logger.info(s"📁 Created partitioned split with values: ${partitionValues}")
    }

    // Log footer offset optimization status
    if (hasFooterOffsets) {
      logger.info(s"🚀 FOOTER OFFSET OPTIMIZATION: Split created with metadata for 87% network traffic reduction")
      logger.debug(s"   Footer offsets: ${footerStartOffset.get}-${footerEndOffset.get}")
      logger.debug(s"   Hotcache: deprecated (using footer offsets instead)")
    } else {
      logger.debug(s"📁 STANDARD: Split created without footer offset optimization")
    }

    logger.info(s"📝 AddAction created with path: ${addAction.path}")

    addAction
  }

  override def abort(): Unit = {
    logger.warn(s"Aborting writer for partition $partitionId")
    singleWriter.foreach { case (engine, _, _) => engine.close() }
    partitionWriters.values.foreach { case (engine, _, _) => engine.close() }
  }

  override def close(): Unit = {
    singleWriter.foreach { case (engine, _, _) => engine.close() }
    partitionWriters.values.foreach { case (engine, _, _) => engine.close() }
  }
}