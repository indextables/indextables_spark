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

package io.indextables.spark.xref

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.UUID

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.sql.{SerializableAwsConfig, SerializableAzureConfig}
import io.indextables.spark.storage.{BroadcastSplitLocalityManager, GlobalSplitCacheManager}
import io.indextables.spark.transaction.AddAction
import io.indextables.spark.util.{CloudConfigExtractor, ConfigNormalization, ConfigUtils}
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import io.indextables.tantivy4java.xref.{XRefMetadata, XRefSourceSplit, XRefSplit}
import io.indextables.tantivy4java.xref.{XRefBuildConfig => TantivyXRefBuildConfig}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

/**
 * Result of XRef build operation containing metadata about the built XRef.
 */
case class XRefBuildOutput(
  sizeBytes: Long,
  totalTerms: Long,
  footerStartOffset: Long,
  footerEndOffset: Long,
  numDocs: Long,
  docMappingJson: Option[String] = None
)

/**
 * Source split information for XRef building.
 */
case class XRefSourceSplitInfo(
  path: String,
  footerStartOffset: Long,
  footerEndOffset: Long,
  numRecords: Long
)

/**
 * Result of footer extraction from source splits.
 */
private[xref] case class SourceSplitFooter(
  path: String,
  splitId: String,
  footerStartOffset: Long,
  footerEndOffset: Long,
  numRecords: Long,
  hostname: String // Host that read this footer (for locality tracking)
) extends Serializable

/**
 * Partition for distributed footer extraction.
 *
 * Groups source splits by their preferred host for locality-aware execution.
 */
private[xref] class FooterExtractionPartition(
  val partitionId: Int,
  val sourceSplits: Seq[XRefSourceSplitInfo],
  val preferredHosts: Array[String]
) extends Partition with Serializable {
  override def index: Int = partitionId
}

/**
 * RDD for distributed footer extraction from source splits.
 *
 * This RDD reads footer metadata from source splits on executors,
 * honoring locality preferences to ensure reads happen on nodes
 * where splits are cached.
 */
private[xref] class FooterExtractionRDD(
  sc: SparkContext,
  partitionedSplits: Seq[(Seq[XRefSourceSplitInfo], Array[String])],
  tablePath: String,
  splitCacheConfigMap: Map[String, String]
) extends RDD[SourceSplitFooter](sc, Nil) {

  private val logger = LoggerFactory.getLogger(classOf[FooterExtractionRDD])

  private val broadcastTablePath = sc.broadcast(tablePath)
  private val broadcastConfig = sc.broadcast(splitCacheConfigMap)

  override protected def getPartitions: Array[Partition] = {
    partitionedSplits.zipWithIndex.map { case ((splits, preferredHosts), idx) =>
      new FooterExtractionPartition(idx, splits, preferredHosts)
    }.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[FooterExtractionPartition].preferredHosts.toSeq
  }

  override def compute(split: Partition, context: TaskContext): Iterator[SourceSplitFooter] = {
    val partition = split.asInstanceOf[FooterExtractionPartition]
    val tablePath = broadcastTablePath.value
    val configMap = broadcastConfig.value
    val hostname = Try(java.net.InetAddress.getLocalHost.getHostName).getOrElse("unknown")

    logger.info(s"Extracting footers from ${partition.sourceSplits.size} splits on host $hostname")

    partition.sourceSplits.iterator.flatMap { splitInfo =>
      // Record access for locality tracking
      BroadcastSplitLocalityManager.recordSplitAccess(splitInfo.path, hostname)

      Try {
        extractFooterFromSplit(splitInfo, tablePath, configMap, hostname)
      } match {
        case Success(footer) =>
          logger.debug(s"Extracted footer from ${splitInfo.path}: ${footer.footerStartOffset}-${footer.footerEndOffset}")
          Some(footer)
        case Failure(e) =>
          logger.warn(s"Failed to extract footer from ${splitInfo.path}: ${e.getMessage}")
          // Still include the split with original offsets
          Some(SourceSplitFooter(
            path = splitInfo.path,
            splitId = extractSplitId(splitInfo.path),
            footerStartOffset = splitInfo.footerStartOffset,
            footerEndOffset = splitInfo.footerEndOffset,
            numRecords = splitInfo.numRecords,
            hostname = hostname
          ))
      }
    }
  }

  private def extractFooterFromSplit(
    splitInfo: XRefSourceSplitInfo,
    tablePath: String,
    configMap: Map[String, String],
    hostname: String
  ): SourceSplitFooter = {
    // For now, we use the footer offsets from transaction log metadata
    // Future enhancement: could read fresh footer from split file
    SourceSplitFooter(
      path = splitInfo.path,
      splitId = extractSplitId(splitInfo.path),
      footerStartOffset = splitInfo.footerStartOffset,
      footerEndOffset = splitInfo.footerEndOffset,
      numRecords = splitInfo.numRecords,
      hostname = hostname
    )
  }

  private def extractSplitId(path: String): String = {
    val fileName = path.split('/').last
    fileName.replace(".split", "")
  }
}

/**
 * Partition for distributed XRef building.
 *
 * Represents a single XRef build task with its source splits.
 */
private[xref] class XRefBuildPartition(
  val partitionId: Int,
  val xrefId: String,
  val outputPath: String,
  val sourceSplits: Seq[SourceSplitFooter],
  val preferredHosts: Array[String]
) extends Partition with Serializable {
  override def index: Int = partitionId
}

/**
 * RDD for distributed XRef building.
 *
 * This RDD builds XRef splits on executors. Each partition represents
 * one XRef to build, with preferred locations based on where source
 * splits are cached.
 */
private[xref] class XRefBuildRDD(
  sc: SparkContext,
  xrefBuilds: Seq[(String, String, Seq[SourceSplitFooter], Array[String])], // (xrefId, outputPath, footers, preferredHosts)
  tablePath: String,
  config: XRefConfig,
  splitCacheConfigMap: Map[String, String],
  awsConfig: SerializableAwsConfig,
  azureConfig: SerializableAzureConfig
) extends RDD[XRefBuildOutput](sc, Nil) {

  private val logger = LoggerFactory.getLogger(classOf[XRefBuildRDD])

  private val broadcastTablePath = sc.broadcast(tablePath)
  private val broadcastConfig = sc.broadcast(splitCacheConfigMap)
  private val broadcastXRefConfig = sc.broadcast(XRefConfigSerializable.fromConfig(config))
  private val broadcastAwsConfig = sc.broadcast(awsConfig)
  private val broadcastAzureConfig = sc.broadcast(azureConfig)

  override protected def getPartitions: Array[Partition] = {
    xrefBuilds.zipWithIndex.map { case ((xrefId, outputPath, footers, preferredHosts), idx) =>
      new XRefBuildPartition(idx, xrefId, outputPath, footers, preferredHosts)
    }.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[XRefBuildPartition].preferredHosts.toSeq
  }

  override def compute(split: Partition, context: TaskContext): Iterator[XRefBuildOutput] = {
    val partition = split.asInstanceOf[XRefBuildPartition]
    val tablePath = broadcastTablePath.value
    val configMap = broadcastConfig.value
    val xrefConfig = broadcastXRefConfig.value.toConfig
    val awsConfig = broadcastAwsConfig.value
    val azureConfig = broadcastAzureConfig.value
    val hostname = Try(java.net.InetAddress.getLocalHost.getHostName).getOrElse("unknown")

    logger.info(s"Building XRef ${partition.xrefId} with ${partition.sourceSplits.size} splits on host $hostname")

    Try {
      buildXRefOnExecutor(
        partition.xrefId,
        partition.outputPath,
        partition.sourceSplits,
        tablePath,
        xrefConfig,
        awsConfig,
        azureConfig
      )
    } match {
      case Success(result) =>
        logger.info(s"XRef ${partition.xrefId} built successfully: ${result.totalTerms} terms, ${result.sizeBytes} bytes")
        // Record XRef split access for future locality
        BroadcastSplitLocalityManager.recordSplitAccess(partition.outputPath, hostname)

        // Report output metrics to Spark UI (consistent with normal write operations)
        // This makes XRef builds visible in Spark UI metrics alongside regular data writes
        if (org.apache.spark.sql.indextables.OutputMetricsUpdater.updateOutputMetrics(result.sizeBytes, result.numDocs)) {
          logger.debug(s"Reported XRef output metrics: ${result.sizeBytes} bytes, ${result.numDocs} records")
        }

        Iterator(result)
      case Failure(e) =>
        logger.error(s"Failed to build XRef ${partition.xrefId}: ${e.getMessage}", e)
        throw e
    }
  }

  private def buildXRefOnExecutor(
    xrefId: String,
    outputPath: String,
    sourceSplits: Seq[SourceSplitFooter],
    tablePath: String,
    config: XRefConfig,
    awsConfig: SerializableAwsConfig,
    azureConfig: SerializableAzureConfig
  ): XRefBuildOutput = {

    // Determine if output is cloud path
    val isCloudPath = CloudConfigExtractor.isCloudPath(outputPath)

    // Resolve temp directory
    val tempDir = resolveTempDirectory(config)

    // Resolve heap size
    val heapSize = config.build.heapSize.getOrElse(50L * 1024 * 1024)

    try {
      // Normalize paths for tantivy4java
      val normalizedOutputPath = io.indextables.spark.util.ProtocolNormalizer.normalizeAllProtocols(outputPath)

      // Build XRefBuildConfig
      val configBuilder = TantivyXRefBuildConfig.builder()
        .xrefId(xrefId)
        .indexUid(UUID.randomUUID().toString)
        .includePositions(config.build.includePositions)
        .tempDirectoryPath(tempDir.getAbsolutePath)
        .heapSize(heapSize)

      // Add source splits
      sourceSplits.foreach { footer =>
        val normalizedPath = io.indextables.spark.util.ProtocolNormalizer.normalizeAllProtocols(footer.path)
        val xrefSourceSplit = new XRefSourceSplit(
          normalizedPath,
          footer.splitId,
          footer.footerStartOffset,
          footer.footerEndOffset
        )
        configBuilder.addSourceSplit(xrefSourceSplit)
      }

      // Add AWS/Azure config if needed
      addCloudConfig(configBuilder, tablePath, awsConfig, azureConfig)

      val tantivyConfig = configBuilder.build()

      val metadata = if (isCloudPath) {
        // Build locally, then upload
        val localXRefPath = new File(tempDir, s"xref-$xrefId.split").getAbsolutePath
        val xrefMetadata = XRefSplit.build(tantivyConfig, localXRefPath)

        // Upload to cloud
        uploadXRefToCloud(localXRefPath, outputPath, awsConfig, azureConfig)
        Files.deleteIfExists(Paths.get(localXRefPath))

        xrefMetadata
      } else {
        // Ensure parent directory exists for local paths
        val outputFile = new File(normalizedOutputPath)
        val parentDir = outputFile.getParentFile
        if (parentDir != null && !parentDir.exists()) {
          logger.info(s"Creating parent directory for XRef output: ${parentDir.getAbsolutePath}")
          parentDir.mkdirs()
        }
        // Build directly to output path
        XRefSplit.build(tantivyConfig, normalizedOutputPath)
      }

      // Convert to XRefBuildOutput
      val splitMetadata = metadata.toSplitMetadata()
      XRefBuildOutput(
        sizeBytes = metadata.getBuildStats.getOutputSizeBytes,
        totalTerms = metadata.getTotalTerms,
        footerStartOffset = metadata.getFooterStartOffset,
        footerEndOffset = metadata.getFooterEndOffset,
        numDocs = metadata.getTotalSourceDocs,
        docMappingJson = Option(splitMetadata.getDocMappingJson).filter(_.nonEmpty)
      )
    } finally {
      cleanupTempDirectory(tempDir)
    }
  }

  private def addCloudConfig(
    configBuilder: TantivyXRefBuildConfig.Builder,
    tablePath: String,
    awsConfig: SerializableAwsConfig,
    azureConfig: SerializableAzureConfig
  ): Unit = {
    if (CloudConfigExtractor.isS3Path(tablePath)) {
      if (awsConfig.accessKey.nonEmpty && awsConfig.secretKey.nonEmpty) {
        val quickwitAwsConfig = awsConfig.toQuickwitSplitAwsConfig(tablePath)
        configBuilder.awsConfig(quickwitAwsConfig)
      }
    } else if (CloudConfigExtractor.isAzurePath(tablePath)) {
      val quickwitAzureConfig = azureConfig.toQuickwitSplitAzureConfig()
      if (quickwitAzureConfig != null) {
        configBuilder.azureConfig(quickwitAzureConfig)
      }
    }
  }

  private def uploadXRefToCloud(
    localPath: String,
    cloudPath: String,
    awsConfig: SerializableAwsConfig,
    azureConfig: SerializableAzureConfig
  ): Unit = {
    // Build options map from serializable configs
    val options = new java.util.HashMap[String, String]()

    // Add AWS credentials
    if (awsConfig.accessKey.nonEmpty) options.put("spark.indextables.aws.accessKey", awsConfig.accessKey)
    if (awsConfig.secretKey.nonEmpty) options.put("spark.indextables.aws.secretKey", awsConfig.secretKey)
    awsConfig.sessionToken.foreach(v => options.put("spark.indextables.aws.sessionToken", v))
    if (awsConfig.region.nonEmpty) options.put("spark.indextables.aws.region", awsConfig.region)
    awsConfig.endpoint.foreach(v => options.put("spark.indextables.s3.endpoint", v))
    options.put("spark.indextables.s3.pathStyleAccess", awsConfig.pathStyleAccess.toString)

    // Add Azure credentials
    azureConfig.accountName.foreach(v => options.put("spark.indextables.azure.accountName", v))
    azureConfig.accountKey.foreach(v => options.put("spark.indextables.azure.accountKey", v))
    azureConfig.connectionString.foreach(v => options.put("spark.indextables.azure.connectionString", v))
    azureConfig.endpoint.foreach(v => options.put("spark.indextables.azure.endpoint", v))
    azureConfig.bearerToken.foreach(v => options.put("spark.indextables.azure.bearerToken", v))

    val caseInsensitiveOptions = new CaseInsensitiveStringMap(options)

    // Create Hadoop configuration with credentials
    val hadoopConf = new Configuration()
    options.forEach((k, v) => hadoopConf.set(k, v))

    val cloudProvider = CloudStorageProviderFactory.createProvider(cloudPath, caseInsensitiveOptions, hadoopConf)
    try {
      val content = Files.readAllBytes(Paths.get(localPath))
      cloudProvider.writeFile(cloudPath, content)
    } finally {
      cloudProvider.close()
    }
  }

  private def resolveTempDirectory(config: XRefConfig): File = {
    val tempPath = config.build.tempDirectoryPath
      .orElse(detectLocalDisk0())
      .getOrElse(System.getProperty("java.io.tmpdir"))

    val tempDir = new File(tempPath, s"xref-build-${UUID.randomUUID().toString.take(8)}")
    tempDir.mkdirs()
    tempDir
  }

  private def detectLocalDisk0(): Option[String] = {
    val localDisk0 = new File("/local_disk0/tmp")
    if (localDisk0.exists() || localDisk0.mkdirs()) {
      Some("/local_disk0/tmp")
    } else {
      None
    }
  }

  private def cleanupTempDirectory(tempDir: File): Unit = {
    if (tempDir.exists()) {
      try {
        deleteRecursively(tempDir)
      } catch {
        case _: Exception => // ignore
      }
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}

/**
 * Serializable wrapper for XRefConfig.
 */
private[xref] case class XRefConfigSerializable(
  autoIndexEnabled: Boolean,
  autoIndexMinSplitsToTrigger: Int,
  autoIndexMaxSourceSplits: Int,
  autoIndexRebuildOnSourceChange: Boolean,
  queryEnabled: Boolean,
  queryMinSplitsForXRef: Int,
  queryTimeoutMs: Int,
  queryFallbackOnError: Boolean,
  queryDistributedSearch: Boolean,
  buildIncludePositions: Boolean,
  buildTempDirectoryPath: Option[String],
  buildHeapSize: Option[Long],
  buildDistributedBuild: Boolean,
  storageDirectory: String,
  localCacheEnabled: Boolean,
  localCacheDirectory: Option[String]
) extends Serializable {

  def toConfig: XRefConfig = XRefConfig(
    autoIndex = XRefAutoIndexConfig(
      enabled = autoIndexEnabled,
      minSplitsToTrigger = autoIndexMinSplitsToTrigger,
      maxSourceSplits = autoIndexMaxSourceSplits,
      rebuildOnSourceChange = autoIndexRebuildOnSourceChange
    ),
    query = XRefQueryConfig(
      enabled = queryEnabled,
      minSplitsForXRef = queryMinSplitsForXRef,
      timeoutMs = queryTimeoutMs,
      fallbackOnError = queryFallbackOnError,
      distributedSearch = queryDistributedSearch
    ),
    build = XRefBuildConfig(
      includePositions = buildIncludePositions,
      tempDirectoryPath = buildTempDirectoryPath,
      heapSize = buildHeapSize,
      distributedBuild = buildDistributedBuild
    ),
    storage = XRefStorageConfig(
      directory = storageDirectory
    ),
    localCache = XRefLocalCacheConfig(
      enabled = localCacheEnabled,
      directory = localCacheDirectory
    )
  )
}

private[xref] object XRefConfigSerializable {
  def fromConfig(config: XRefConfig): XRefConfigSerializable = XRefConfigSerializable(
    autoIndexEnabled = config.autoIndex.enabled,
    autoIndexMinSplitsToTrigger = config.autoIndex.minSplitsToTrigger,
    autoIndexMaxSourceSplits = config.build.maxSourceSplits,
    autoIndexRebuildOnSourceChange = config.autoIndex.rebuildOnSourceChange,
    queryEnabled = config.query.enabled,
    queryMinSplitsForXRef = config.query.minSplitsForXRef,
    queryTimeoutMs = config.query.timeoutMs,
    queryFallbackOnError = config.query.fallbackOnError,
    queryDistributedSearch = config.query.distributedSearch,
    buildIncludePositions = config.build.includePositions,
    buildTempDirectoryPath = config.build.tempDirectoryPath,
    buildHeapSize = config.build.heapSize,
    buildDistributedBuild = config.build.distributedBuild,
    storageDirectory = config.storage.directory,
    localCacheEnabled = config.localCache.enabled,
    localCacheDirectory = config.localCache.directory
  )
}

/**
 * Distributed XRef builder.
 *
 * This object provides distributed XRef building capabilities that run on executors
 * with locality preferences, using the existing BroadcastSplitLocalityManager
 * infrastructure for cache-aware scheduling.
 *
 * The build process:
 * 1. Partition source splits by locality (group by preferred hosts)
 * 2. Extract footers on executors where splits are cached
 * 3. Build XRef on executor with best locality for source splits
 * 4. Upload to cloud storage if needed
 */
object DistributedXRefBuilder {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Build XRef splits distributed across executors.
   *
   * @param tablePath Table root path
   * @param xrefId XRef identifier
   * @param outputPath XRef output path
   * @param sourceSplits Source splits to include
   * @param config XRef configuration
   * @param sparkSession Spark session
   * @param overrideConfigMap Optional pre-merged config map. When provided (e.g., during write operations),
   *                          this takes precedence. When None, configs are extracted from hadoop + spark.
   * @return XRef build output
   */
  def buildXRefDistributed(
    tablePath: String,
    xrefId: String,
    outputPath: String,
    sourceSplits: Seq[AddAction],
    config: XRefConfig,
    sparkSession: SparkSession,
    overrideConfigMap: Option[Map[String, String]] = None
  ): XRefBuildOutput = {

    val sc = sparkSession.sparkContext

    // Update locality information before scheduling
    try {
      BroadcastSplitLocalityManager.updateBroadcastLocality(sc)
    } catch {
      case ex: Exception =>
        logger.warn(s"Failed to update broadcast locality before XRef build: ${ex.getMessage}")
    }

    // Convert AddActions to XRefSourceSplitInfo
    val sourceSplitInfos = sourceSplits.map { split =>
      val fullPath = resolveSplitPath(tablePath, split.path)
      XRefSourceSplitInfo(
        path = fullPath,
        footerStartOffset = split.footerStartOffset.getOrElse(0L),
        footerEndOffset = split.footerEndOffset.getOrElse(0L),
        numRecords = split.numRecords.getOrElse(0L)
      )
    }

    // Partition source splits by locality
    val partitionedSplits = partitionByLocality(sourceSplitInfos)

    logger.info(s"Building XRef $xrefId with ${sourceSplits.size} source splits across ${partitionedSplits.size} locality groups")

    // Use override config map if provided (e.g., during write operations with write options)
    // Otherwise, extract from hadoop + spark (for SQL command invocations)
    val configMap = overrideConfigMap.getOrElse {
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
      val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
      val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
      ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)
    }

    logger.debug(s"XRef build using config map with ${configMap.size} entries")

    // Extract cloud configs using CloudConfigExtractor for proper credential handling
    // Pass the override config map so write options take precedence when provided
    val awsConfig = CloudConfigExtractor.extractAwsConfig(sparkSession, overrideConfigMap)
    val azureConfig = CloudConfigExtractor.extractAzureConfig(sparkSession, overrideConfigMap)

    // Step 1: Extract footers distributed (with locality)
    val footerRDD = new FooterExtractionRDD(sc, partitionedSplits, tablePath, configMap)

    sc.setJobGroup(
      "tantivy4spark-xref-footers",
      s"XRef build phase 1: extracting footers from ${sourceSplits.size} source splits",
      interruptOnCancel = false
    )

    val footers = try {
      footerRDD.collect().toSeq
    } finally {
      sc.clearJobGroup()
    }

    logger.info(s"Extracted ${footers.size} footers for XRef build")

    // Step 2: Determine best executor for XRef build based on footer locality
    val preferredHosts = computePreferredHostsForBuild(footers)

    // Step 3: Build XRef on executor with best locality (with proper cloud configs)
    val xrefBuilds = Seq((xrefId, outputPath, footers, preferredHosts))
    val buildRDD = new XRefBuildRDD(sc, xrefBuilds, tablePath, config, configMap, awsConfig, azureConfig)

    sc.setJobGroup(
      "tantivy4spark-xref-build",
      s"XRef build phase 2: building XRef with ${footers.size} source splits",
      interruptOnCancel = false
    )

    try {
      val results = buildRDD.collect()
      if (results.isEmpty) {
        throw new RuntimeException(s"XRef build failed: no results returned")
      }
      results.head
    } finally {
      sc.clearJobGroup()
    }
  }

  /**
   * Partition source splits by their preferred hosts for locality-aware execution.
   */
  private def partitionByLocality(
    splits: Seq[XRefSourceSplitInfo]
  ): Seq[(Seq[XRefSourceSplitInfo], Array[String])] = {
    // Group splits by their preferred host(s)
    val splitsByHost = splits.groupBy { split =>
      val hosts = BroadcastSplitLocalityManager.getPreferredHosts(split.path)
      if (hosts.nonEmpty) hosts.sorted.mkString(",") else "no-locality"
    }

    splitsByHost.map { case (hostKey, splitsForHost) =>
      val preferredHosts = if (hostKey == "no-locality") Array.empty[String] else hostKey.split(",")
      (splitsForHost, preferredHosts)
    }.toSeq
  }

  /**
   * Compute preferred hosts for XRef build based on where footers were extracted.
   */
  private def computePreferredHostsForBuild(footers: Seq[SourceSplitFooter]): Array[String] = {
    // Count how many splits were read from each host
    val hostCounts = footers.groupBy(_.hostname).map { case (host, splits) => (host, splits.size) }

    if (hostCounts.isEmpty) {
      Array.empty
    } else {
      // Return hosts sorted by count (most splits first)
      hostCounts.toSeq.sortBy(-_._2).map(_._1).toArray
    }
  }

  private def resolveSplitPath(tablePath: String, splitPath: String): String = {
    if (splitPath.startsWith("/") || splitPath.contains("://")) {
      splitPath
    } else {
      new Path(tablePath, splitPath).toString
    }
  }

  /**
   * Build multiple XRef splits in parallel across executors.
   *
   * This method builds multiple XRefs concurrently by creating a single Spark job
   * with one partition per XRef. This is much more efficient than building XRefs
   * sequentially when you have many XRefs to build.
   *
   * @param tablePath Table root path
   * @param xrefSpecs Sequence of (xrefId, outputPath, sourceSplits) tuples
   * @param config XRef configuration
   * @param sparkSession Spark session
   * @param overrideConfigMap Optional pre-merged config map
   * @return Map of xrefId -> XRefBuildOutput
   */
  def buildXRefsDistributedBatch(
    tablePath: String,
    xrefSpecs: Seq[(String, String, Seq[AddAction])],
    config: XRefConfig,
    sparkSession: SparkSession,
    overrideConfigMap: Option[Map[String, String]] = None
  ): Map[String, XRefBuildOutput] = {

    if (xrefSpecs.isEmpty) {
      return Map.empty
    }

    val sc = sparkSession.sparkContext

    // Update locality information before scheduling
    try {
      BroadcastSplitLocalityManager.updateBroadcastLocality(sc)
    } catch {
      case ex: Exception =>
        logger.warn(s"Failed to update broadcast locality before XRef batch build: ${ex.getMessage}")
    }

    // Use override config map if provided, otherwise extract from hadoop + spark
    val configMap = overrideConfigMap.getOrElse {
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
      val sparkConfigs = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
      val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
      ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs)
    }

    // Extract cloud configs
    val awsConfig = CloudConfigExtractor.extractAwsConfig(sparkSession, overrideConfigMap)
    val azureConfig = CloudConfigExtractor.extractAzureConfig(sparkSession, overrideConfigMap)

    logger.info(s"Building ${xrefSpecs.size} XRefs in parallel batch")

    // Step 1: Prepare all XRef builds with their footers
    // For batch builds, we use the transaction log metadata directly (no distributed footer extraction)
    val xrefBuilds = xrefSpecs.map { case (xrefId, outputPath, sourceSplits) =>
      val footers = sourceSplits.map { split =>
        val fullPath = resolveSplitPath(tablePath, split.path)
        SourceSplitFooter(
          path = fullPath,
          splitId = fullPath.split('/').last.replace(".split", ""),
          footerStartOffset = split.footerStartOffset.getOrElse(0L),
          footerEndOffset = split.footerEndOffset.getOrElse(0L),
          numRecords = split.numRecords.getOrElse(0L),
          hostname = "driver" // Footer extracted on driver from transaction log
        )
      }

      // Get preferred hosts based on source split locality
      val preferredHosts = computePreferredHostsForBuild(footers)

      logger.info(s"XRef $xrefId: ${sourceSplits.size} source splits, preferred hosts: ${preferredHosts.mkString(",")}")

      (xrefId, outputPath, footers, preferredHosts)
    }

    // Step 2: Build all XRefs in parallel using a single RDD
    val buildRDD = new XRefBuildRDD(sc, xrefBuilds, tablePath, config, configMap, awsConfig, azureConfig)

    sc.setJobGroup(
      "tantivy4spark-xref-batch-build",
      s"XRef batch build: building ${xrefSpecs.size} XRefs in parallel",
      interruptOnCancel = false
    )

    try {
      // Collect results - each partition returns one XRefBuildOutput
      val results = buildRDD.collect()

      // Map results back to xrefIds
      xrefSpecs.zip(results).map { case ((xrefId, _, _), output) =>
        xrefId -> output
      }.toMap
    } finally {
      sc.clearJobGroup()
    }
  }

  /**
   * Check if distributed XRef build should be used.
   */
  def shouldUseDistributedBuild(
    sourceSplits: Seq[AddAction],
    sparkSession: SparkSession
  ): Boolean = {
    // Use distributed build if:
    // 1. We have multiple source splits
    // 2. Cluster has multiple executors
    val numExecutors = sparkSession.sparkContext.getExecutorMemoryStatus.size
    sourceSplits.size > 10 && numExecutors > 1
  }
}
