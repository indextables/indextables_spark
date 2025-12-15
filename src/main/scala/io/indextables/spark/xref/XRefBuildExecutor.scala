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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.AddAction
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import io.indextables.tantivy4java.xref.{XRefMetadata, XRefSourceSplit, XRefSplit}
import io.indextables.tantivy4java.xref.{XRefBuildConfig => TantivyXRefBuildConfig}
import org.slf4j.LoggerFactory

/**
 * Result of XRef build operation containing metadata about the built XRef.
 */
case class XRefBuildOutput(
  sizeBytes: Long,
  totalTerms: Long,
  footerStartOffset: Long,
  footerEndOffset: Long,
  numDocs: Long
)

/**
 * Executor for building cross-reference (XRef) splits.
 *
 * This class handles the actual tantivy4java integration for building XRef splits from source splits. It supports both
 * local and cloud storage paths, with proper credential handling for S3 and Azure.
 *
 * The build process:
 *   1. Collects footer metadata from all source splits
 *   2. Builds a consolidated term dictionary XRef
 *   3. Uploads the XRef split to storage
 *   4. Returns metadata about the built XRef
 */
object XRefBuildExecutor {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Build an XRef split from source splits.
   *
   * @param tablePath
   *   The table root path
   * @param xrefId
   *   Unique identifier for the XRef
   * @param xrefOutputPath
   *   Full path where XRef split will be written
   * @param sourceSplits
   *   Source splits to include in the XRef
   * @param config
   *   XRef configuration
   * @param sparkSession
   *   The Spark session
   * @return
   *   Build result containing XRef metadata
   */
  def buildXRef(
    tablePath: String,
    xrefId: String,
    xrefOutputPath: String,
    sourceSplits: Seq[AddAction],
    config: XRefConfig,
    sparkSession: SparkSession
  ): XRefBuildOutput = {

    logger.info(s"Building XRef $xrefId with ${sourceSplits.size} source splits")
    logger.info(s"Output path: $xrefOutputPath")

    // Determine storage type and prepare paths
    val isS3Path = tablePath.toLowerCase.startsWith("s3://") ||
      tablePath.toLowerCase.startsWith("s3a://") ||
      tablePath.toLowerCase.startsWith("s3n://")

    val isAzurePath = tablePath.toLowerCase.startsWith("abfss://") ||
      tablePath.toLowerCase.startsWith("abfs://") ||
      tablePath.toLowerCase.startsWith("wasbs://") ||
      tablePath.toLowerCase.startsWith("wasb://") ||
      tablePath.toLowerCase.startsWith("azure://")

    // Resolve temp directory for local build
    val tempDir = resolveTempDirectory(config, sparkSession)

    // Resolve heap size for XRef build (XRef-specific config or fall back to indexWriter.heapSize)
    val heapSize = resolveHeapSize(config, sparkSession)

    // Ensure parent directory exists for local paths
    if (!isS3Path && !isAzurePath) {
      val parentDir = new File(xrefOutputPath).getParentFile
      if (parentDir != null && !parentDir.exists()) {
        logger.info(s"Creating parent directory: ${parentDir.getAbsolutePath}")
        parentDir.mkdirs()
      }
    }

    try {
      // Build source split information for tantivy4java
      val sourceSplitInfos = sourceSplits.map { split =>
        val fullSplitPath = resolveSplitPath(tablePath, split.path)
        XRefSourceSplitInfo(
          path = fullSplitPath,
          footerStartOffset = split.footerStartOffset.getOrElse(0L),
          footerEndOffset = split.footerEndOffset.getOrElse(0L),
          numRecords = split.numRecords.getOrElse(0L)
        )
      }

      logger.debug(s"Source splits prepared: ${sourceSplitInfos.map(_.path).mkString(", ")}")

      // Build the XRef using tantivy4java
      // NOTE: This is the integration point with tantivy4java XRef API
      // The actual implementation depends on the tantivy4java XRefSplit API
      val buildResult = buildXRefWithTantivy(
        xrefId = xrefId,
        outputPath = xrefOutputPath,
        sourceSplits = sourceSplitInfos,
        tempDir = tempDir,
        heapSize = heapSize,
        includePositions = config.build.includePositions,
        tablePath = tablePath,
        sparkSession = sparkSession
      )

      logger.info(s"XRef $xrefId built successfully: ${buildResult.totalTerms} terms, ${buildResult.sizeBytes} bytes")

      buildResult
    } finally {
      // Cleanup temp directory if we created it
      cleanupTempDirectory(tempDir)
    }
  }

  /**
   * Build XRef using tantivy4java.
   *
   * This is the main integration point with tantivy4java. The implementation uses the tantivy4java XRef API to build a
   * consolidated term dictionary from source splits.
   */
  private def buildXRefWithTantivy(
    xrefId: String,
    outputPath: String,
    sourceSplits: Seq[XRefSourceSplitInfo],
    tempDir: File,
    heapSize: Long,
    includePositions: Boolean,
    tablePath: String,
    sparkSession: SparkSession
  ): XRefBuildOutput = {

    logger.info(s"Building XRef with tantivy4java: $xrefId -> $outputPath")

    // TODO: Integrate with actual tantivy4java XRef API when available
    // The expected API would be something like:
    //
    // val xrefBuilder = XRefSplit.builder()
    //   .outputPath(outputPath)
    //   .includePositions(includePositions)
    //   .awsConfig(awsConfig)
    //   .azureConfig(azureConfig)
    //
    // sourceSplits.foreach { split =>
    //   xrefBuilder.addSourceSplit(split.path, split.footerStartOffset, split.footerEndOffset)
    // }
    //
    // val result = xrefBuilder.build()
    //
    // For now, we'll throw a clear error indicating this needs tantivy4java XRef support

    // Check if tantivy4java XRef API is available
    val xrefApiAvailable = checkXRefApiAvailable()

    if (!xrefApiAvailable) {
      throw new UnsupportedOperationException(
        s"XRef build requires tantivy4java XRef API support. " +
          s"Please ensure tantivy4java version includes XRefSplit.build() functionality. " +
          s"Source splits: ${sourceSplits.size}, Output: $outputPath"
      )
    }

    // Placeholder for actual tantivy4java integration
    // This will be replaced with real API calls once tantivy4java XRef support is confirmed
    executeXRefBuild(xrefId, outputPath, sourceSplits, tempDir, heapSize, includePositions, tablePath, sparkSession)
  }

  /**
   * Check if tantivy4java XRef API is available.
   *
   * This checks if the required classes/methods exist in the classpath.
   */
  private def checkXRefApiAvailable(): Boolean =
    try {
      // Try to load the XRef-related classes from tantivy4java
      // Adjust class name based on actual tantivy4java API
      Class.forName("io.indextables.tantivy4java.xref.XRefSplit")
      true
    } catch {
      case _: ClassNotFoundException =>
        logger.warn("tantivy4java XRef API not found in classpath")
        false
    }

  /**
   * Execute XRef build with tantivy4java.
   *
   * This is the actual tantivy4java integration using the XRefSplit API.
   */
  private def executeXRefBuild(
    xrefId: String,
    outputPath: String,
    sourceSplits: Seq[XRefSourceSplitInfo],
    tempDir: File,
    heapSize: Long,
    includePositions: Boolean,
    tablePath: String,
    sparkSession: SparkSession
  ): XRefBuildOutput = {

    logger.info(s"Executing XRef build: $xrefId with ${sourceSplits.size} source splits")

    // Normalize paths for tantivy4java
    val normalizedOutputPath = normalizePathForTantivy(outputPath)
    val normalizedSourcePaths = sourceSplits.map(s => s.copy(path = normalizePathForTantivy(s.path)))

    logger.debug(s"Normalized output path: $normalizedOutputPath")
    logger.debug(s"Normalized source paths (first 5): ${normalizedSourcePaths.take(5).map(s => s"${s.path} [footer=${s.footerStartOffset}-${s.footerEndOffset}]").mkString(", ")}")

    // Build XRefBuildConfig
    val configBuilder = TantivyXRefBuildConfig.builder()
      .xrefId(xrefId)
      .indexUid(UUID.randomUUID().toString)
      .includePositions(includePositions)

    // Set temp directory and heap size for XRef build
    configBuilder.tempDirectoryPath(tempDir.getAbsolutePath)
    configBuilder.heapSize(heapSize)

    logger.info(s"XRef build configuration: xrefId=$xrefId, includePositions=$includePositions")

    // Add source splits
    normalizedSourcePaths.foreach { splitInfo =>
      val splitId = extractSplitId(splitInfo.path)
      val xrefSourceSplit = new XRefSourceSplit(
        splitInfo.path,
        splitId,
        splitInfo.footerStartOffset,
        splitInfo.footerEndOffset
      )
      configBuilder.addSourceSplit(xrefSourceSplit)
    }

    // Extract and add AWS configuration if applicable
    val awsConfig = extractAwsConfig(sparkSession, tablePath)
    if (awsConfig != null) {
      configBuilder.awsConfig(awsConfig)
    }

    // Extract and add Azure configuration if applicable
    val azureConfig = extractAzureConfig(sparkSession, tablePath)
    if (azureConfig != null) {
      configBuilder.azureConfig(azureConfig)
    }

    val config = configBuilder.build()

    // Determine if output is a cloud path
    val isCloudPath = normalizedOutputPath.startsWith("s3://") || normalizedOutputPath.startsWith("azure://")

    val metadata = if (isCloudPath) {
      // For cloud storage: build locally, then upload
      val localXRefPath = new File(tempDir, s"xref-$xrefId.split").getAbsolutePath

      logger.info(s"Building XRef locally: $xrefId -> $localXRefPath")
      val xrefMetadata = XRefSplit.build(config, localXRefPath)
      logger.info(s"XRef build completed: ${xrefMetadata.getTotalTerms} terms")

      // Upload to cloud storage
      try {
        uploadXRefToCloud(localXRefPath, outputPath, sparkSession)
        logger.info(s"XRef uploaded to cloud: $outputPath")
      } finally {
        // Clean up local temp file
        Files.deleteIfExists(Paths.get(localXRefPath))
      }

      xrefMetadata
    } else {
      // For local paths: build directly to output
      logger.info(s"Building XRef directly: $xrefId -> $normalizedOutputPath")
      val xrefMetadata = XRefSplit.build(config, normalizedOutputPath)
      logger.info(s"XRef build completed: ${xrefMetadata.getTotalTerms} terms, footer offsets: ${xrefMetadata.getFooterStartOffset}-${xrefMetadata.getFooterEndOffset}")
      xrefMetadata
    }

    // Convert XRefMetadata to XRefBuildOutput
    XRefBuildOutput(
      sizeBytes = metadata.getBuildStats.getOutputSizeBytes,
      totalTerms = metadata.getTotalTerms,
      footerStartOffset = metadata.getFooterStartOffset,
      footerEndOffset = metadata.getFooterEndOffset,
      numDocs = metadata.getTotalSourceDocs
    )
  }

  /**
   * Upload XRef split file to cloud storage.
   */
  private def uploadXRefToCloud(localPath: String, cloudPath: String, sparkSession: SparkSession): Unit = {
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val options = extractCloudOptions(sparkSession)

    val cloudProvider = CloudStorageProviderFactory.createProvider(cloudPath, options, hadoopConf)
    try {
      val xrefFile = Paths.get(localPath)
      val fileSize = Files.size(xrefFile)

      logger.info(s"Uploading XRef split to $cloudPath (${fileSize / 1024} KB)")

      if (fileSize > 100L * 1024 * 1024) {
        // Use streaming upload for large files
        val inputStream = Files.newInputStream(xrefFile)
        try {
          cloudProvider.writeFileFromStream(cloudPath, inputStream, Some(fileSize))
        } finally {
          inputStream.close()
        }
      } else {
        // Use traditional upload for smaller files
        val content = Files.readAllBytes(xrefFile)
        cloudProvider.writeFile(cloudPath, content)
      }
    } finally {
      cloudProvider.close()
    }
  }

  /**
   * Extract cloud storage options from SparkSession for upload.
   */
  private def extractCloudOptions(sparkSession: SparkSession): CaseInsensitiveStringMap = {
    val options = new java.util.HashMap[String, String]()
    val sparkConf = sparkSession.conf

    // AWS options
    Option(sparkConf.get("spark.indextables.aws.accessKey", null)).foreach(v => options.put("spark.indextables.aws.accessKey", v))
    Option(sparkConf.get("spark.indextables.aws.secretKey", null)).foreach(v => options.put("spark.indextables.aws.secretKey", v))
    Option(sparkConf.get("spark.indextables.aws.sessionToken", null)).foreach(v => options.put("spark.indextables.aws.sessionToken", v))
    Option(sparkConf.get("spark.indextables.aws.region", null)).foreach(v => options.put("spark.indextables.aws.region", v))
    Option(sparkConf.get("spark.indextables.s3.endpoint", null)).foreach(v => options.put("spark.indextables.s3.endpoint", v))
    Option(sparkConf.get("spark.indextables.s3.maxConcurrency", null)).foreach(v => options.put("spark.indextables.s3.maxConcurrency", v))
    Option(sparkConf.get("spark.indextables.s3.partSize", null)).foreach(v => options.put("spark.indextables.s3.partSize", v))

    // Azure options
    Option(sparkConf.get("spark.indextables.azure.accountName", null)).foreach(v => options.put("spark.indextables.azure.accountName", v))
    Option(sparkConf.get("spark.indextables.azure.accountKey", null)).foreach(v => options.put("spark.indextables.azure.accountKey", v))

    new CaseInsensitiveStringMap(options)
  }

  /**
   * Extract split ID from path.
   */
  private def extractSplitId(path: String): String = {
    val fileName = path.split('/').last
    fileName.replace(".split", "")
  }

  /**
   * Extract AWS configuration from SparkSession.
   */
  private def extractAwsConfig(sparkSession: SparkSession, tablePath: String): QuickwitSplit.AwsConfig = {
    val isS3Path = tablePath.toLowerCase.startsWith("s3://") ||
      tablePath.toLowerCase.startsWith("s3a://") ||
      tablePath.toLowerCase.startsWith("s3n://")

    if (!isS3Path) return null

    try {
      val sparkConf = sparkSession.conf
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

      def getConfigWithFallback(key: String): Option[String] = {
        val value = Option(sparkConf.get(key, null))
          .orElse(Option(hadoopConf.get(key)))
          .filter(_.nonEmpty)
        value
      }

      val accessKey = getConfigWithFallback("spark.indextables.aws.accessKey")
        .orElse(getConfigWithFallback("fs.s3a.access.key"))
      val secretKey = getConfigWithFallback("spark.indextables.aws.secretKey")
        .orElse(getConfigWithFallback("fs.s3a.secret.key"))
      val sessionToken = getConfigWithFallback("spark.indextables.aws.sessionToken")
        .orElse(getConfigWithFallback("fs.s3a.session.token"))
      val region = getConfigWithFallback("spark.indextables.aws.region")
        .orElse(getConfigWithFallback("fs.s3a.endpoint.region"))
      // Only use explicitly configured endpoint, don't extract from Hadoop defaults.
      // For real AWS S3, the region alone is sufficient - endpoint is only needed
      // for custom S3-compatible services (MinIO, localstack, etc.)
      val endpoint = getConfigWithFallback("spark.indextables.s3.endpoint")
        .map(normalizeEndpointUrl)
      val pathStyleAccess = getConfigWithFallback("spark.indextables.s3.pathStyleAccess")
        .orElse(getConfigWithFallback("fs.s3a.path.style.access"))
        .exists(v => v.equalsIgnoreCase("true") || v == "1")

      if (accessKey.isEmpty && secretKey.isEmpty) {
        logger.debug("No explicit AWS credentials found, relying on default credential chain")
        return null
      }

      new QuickwitSplit.AwsConfig(
        accessKey.getOrElse(""),
        secretKey.getOrElse(""),
        sessionToken.orNull,
        region.orNull,
        endpoint.orNull,
        pathStyleAccess
      )
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to extract AWS config: ${e.getMessage}")
        null
    }
  }

  /**
   * Extract Azure configuration from SparkSession.
   */
  private def extractAzureConfig(sparkSession: SparkSession, tablePath: String): QuickwitSplit.AzureConfig = {
    val isAzurePath = tablePath.toLowerCase.startsWith("abfss://") ||
      tablePath.toLowerCase.startsWith("abfs://") ||
      tablePath.toLowerCase.startsWith("wasbs://") ||
      tablePath.toLowerCase.startsWith("wasb://") ||
      tablePath.toLowerCase.startsWith("azure://")

    if (!isAzurePath) return null

    try {
      val sparkConf = sparkSession.conf

      def getConfigWithFallback(key: String): Option[String] =
        Option(sparkConf.get(key, null)).filter(_.nonEmpty)

      val accountName = getConfigWithFallback("spark.indextables.azure.accountName")
      val accountKey = getConfigWithFallback("spark.indextables.azure.accountKey")

      if (accountName.isEmpty) {
        logger.debug("No Azure account name found")
        return null
      }

      if (accountKey.isEmpty) {
        logger.debug("No Azure account key found")
        return null
      }

      new QuickwitSplit.AzureConfig(
        accountName.getOrElse(""),
        accountKey.getOrElse("")
      )
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to extract Azure config: ${e.getMessage}")
        null
    }
  }

  /**
   * Resolve the full path for a split.
   */
  private def resolveSplitPath(tablePath: String, splitPath: String): String =
    if (splitPath.startsWith("/") || splitPath.contains("://")) {
      splitPath
    } else {
      new Path(tablePath, splitPath).toString
    }

  /**
   * Normalize path for tantivy4java compatibility.
   *
   * Converts s3a:// and s3n:// to s3://, and Azure paths to azure://.
   */
  private def normalizePathForTantivy(path: String): String =
    io.indextables.spark.util.ProtocolNormalizer.normalizeAllProtocols(path)

  /**
   * Normalize endpoint URL to ensure it has a protocol prefix.
   *
   * The Rust SDK expects full URLs like https://s3.amazonaws.com, but Hadoop S3A config
   * often has just the hostname like s3.amazonaws.com.
   */
  private def normalizeEndpointUrl(endpoint: String): String =
    if (endpoint.startsWith("http://") || endpoint.startsWith("https://")) {
      endpoint
    } else {
      s"https://$endpoint"
    }

  /**
   * Resolve the temp directory for XRef build.
   *
   * Fallback order: XRef-specific config -> indexWriter.tempDirectoryPath -> /local_disk0 detection -> system temp
   */
  private def resolveTempDirectory(config: XRefConfig, sparkSession: SparkSession): File = {
    val tempPath = config.build.tempDirectoryPath
      .orElse(Option(sparkSession.conf.get("spark.indextables.xref.build.tempDirectoryPath", null)))
      .orElse(Option(sparkSession.conf.get("spark.indextables.indexWriter.tempDirectoryPath", null)))
      .orElse(detectLocalDisk0())
      .getOrElse(System.getProperty("java.io.tmpdir"))

    val tempDir = new File(tempPath, s"xref-build-${UUID.randomUUID().toString.take(8)}")
    tempDir.mkdirs()
    tempDir
  }

  /**
   * Resolve heap size for XRef build.
   *
   * Fallback order: XRef-specific config -> indexWriter.heapSize -> default (50MB)
   */
  private def resolveHeapSize(config: XRefConfig, sparkSession: SparkSession): Long = {
    // Default heap size for XRef builds (50MB - tantivy4java XRefBuildConfig default)
    val defaultHeapSize = 50L * 1024 * 1024

    config.build.heapSize
      .orElse {
        // Try XRef-specific spark config
        Option(sparkSession.conf.get("spark.indextables.xref.build.heapSize", null))
          .filter(_.nonEmpty)
          .map(parseSize)
      }
      .orElse {
        // Fall back to indexWriter.heapSize
        Option(sparkSession.conf.get("spark.indextables.indexWriter.heapSize", null))
          .filter(_.nonEmpty)
          .map(parseSize)
      }
      .getOrElse(defaultHeapSize)
  }

  // Use SizeParser utility for parsing size strings
  private def parseSize(value: String): Long =
    io.indextables.spark.util.SizeParser.parseSize(value)

  /**
   * Detect /local_disk0 if available (Databricks/EMR).
   */
  private def detectLocalDisk0(): Option[String] = {
    val localDisk0 = new File("/local_disk0/tmp")
    if (localDisk0.exists() || localDisk0.mkdirs()) {
      Some("/local_disk0/tmp")
    } else {
      None
    }
  }

  /**
   * Cleanup temporary directory.
   */
  private def cleanupTempDirectory(tempDir: File): Unit =
    if (tempDir.exists()) {
      try {
        deleteRecursively(tempDir)
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to cleanup temp directory ${tempDir.getAbsolutePath}: ${e.getMessage}")
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
 * Information about a source split for XRef building.
 */
case class XRefSourceSplitInfo(
  path: String,
  footerStartOffset: Long,
  footerEndOffset: Long,
  numRecords: Long
)
