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

package io.indextables.spark.sync

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.indextables.OutputMetricsUpdater

import io.indextables.spark.transaction.AddAction
import io.indextables.tantivy4java.split.merge.QuickwitSplit
import io.indextables.tantivy4java.split.ParquetCompanionConfig
import org.slf4j.LoggerFactory

/** Serializable configuration for a sync task, passed from driver to executor. */
case class SyncConfig(
  indexingModes: Map[String, String],
  fastFieldMode: String,
  storageConfig: Map[String, String],
  splitTablePath: String,
  writerHeapSize: Long = 2L * 1024L * 1024L * 1024L, // 2GB default
  readerBatchSize: Int = 8192,
  schemaSourceParquetFile: Option[String] = None,
  columnNameMapping: Map[String, String] = Map.empty,
  autoDetectNameMapping: Boolean = false)
    extends Serializable

/**
 * Represents a group of parquet files to be indexed into a single companion split. Sent from driver to executor as part
 * of a Spark task.
 */
case class SyncIndexingGroup(
  parquetFiles: Seq[String],
  parquetTableRoot: String,
  partitionValues: Map[String, String],
  groupIndex: Int)
    extends Serializable

/** Result of indexing a single group of parquet files into a companion split. Returned from executor to driver. */
case class SyncTaskResult(
  addAction: AddAction,
  bytesDownloaded: Long,
  bytesUploaded: Long,
  parquetFilesIndexed: Int)
    extends Serializable

/**
 * Executor-side companion split creation. Handles one indexing group: downloads parquet files, calls
 * QuickwitSplit.createFromParquet(), uploads the result.
 */
object SyncTaskExecutor {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Execute a sync task for one indexing group.
   *
   * @param group
   *   The indexing group containing parquet file paths and partition info
   * @param config
   *   Sync configuration (indexing modes, credentials, etc.)
   * @return
   *   SyncTaskResult with the AddAction and metrics
   */
  def execute(group: SyncIndexingGroup, config: SyncConfig): SyncTaskResult = {
    val startTime = System.currentTimeMillis()
    val tempDir   = createTempDir()
    logger.info(
      s"Sync task ${group.groupIndex}: indexing ${group.parquetFiles.size} parquet files into companion split"
    )

    try {
      // 1. Download parquet files to local temp in parallel, preserving relative paths
      val downloadParallelism  = math.min(8, math.max(1, group.parquetFiles.size))
      val downloadPool         = java.util.concurrent.Executors.newFixedThreadPool(downloadParallelism)
      val totalBytesDownloaded = new java.util.concurrent.atomic.AtomicLong(0L)

      val downloadFutures = group.parquetFiles.map { parquetPath =>
        downloadPool.submit(new java.util.concurrent.Callable[String] {
          override def call(): String = {
            val relativePath = extractRelativePath(parquetPath, group.parquetTableRoot)
            val localFile    = new File(tempDir, relativePath)
            localFile.getParentFile.mkdirs()

            val bytesDownloaded = downloadFile(parquetPath, localFile, config.storageConfig, group.parquetTableRoot)
            totalBytesDownloaded.addAndGet(bytesDownloaded)
            localFile.getAbsolutePath
          }
        })
      }

      val localFiles =
        try
          downloadFutures.map { f =>
            try
              f.get()
            catch {
              case e: java.util.concurrent.ExecutionException => throw e.getCause
            }
          }
        finally
          downloadPool.shutdownNow()

      OutputMetricsUpdater.updateInputMetrics(totalBytesDownloaded.get(), group.parquetFiles.size)

      // 2. Build ParquetCompanionConfig
      val companionConfig = new ParquetCompanionConfig(tempDir.getAbsolutePath)
        .withFastFieldMode(
          ParquetCompanionConfig.FastFieldMode.valueOf(config.fastFieldMode)
        )
        .withWriterHeapSize(config.writerHeapSize)
        .withReaderBatchSize(config.readerBatchSize)

      // Apply indexing modes: "text" fields get a tokenizer override (forces TEXT indexing),
      // "ip"/"ipaddress" fields get registered as IP address fields
      if (config.indexingModes.nonEmpty) {
        val tokenizerOverrides = config.indexingModes.collect {
          case (field, mode) if mode.toLowerCase == "text" => field -> "default"
        }
        if (tokenizerOverrides.nonEmpty) {
          logger.info(s"Sync task ${group.groupIndex}: applying tokenizer overrides for TEXT fields: ${tokenizerOverrides.keys.mkString(", ")}")
          companionConfig.withTokenizerOverrides(tokenizerOverrides.asJava)
        }

        val ipFields = config.indexingModes.collect {
          case (field, mode) if mode.toLowerCase == "ipaddress" || mode.toLowerCase == "ip" => field
        }.toArray
        if (ipFields.nonEmpty) {
          logger.info(s"Sync task ${group.groupIndex}: applying IP address fields: ${ipFields.mkString(", ")}")
          companionConfig.withIpAddressFields(ipFields: _*)
        }

        val jsonFields = config.indexingModes.collect {
          case (field, mode) if mode.toLowerCase == "json" => field
        }.toArray
        if (jsonFields.nonEmpty) {
          logger.info(s"Sync task ${group.groupIndex}: applying JSON fields: ${jsonFields.mkString(", ")}")
          companionConfig.withJsonFields(jsonFields: _*)
        }
      }

      // Apply column name mapping (physical → logical) for Iceberg/Delta with column mapping
      if (config.columnNameMapping.nonEmpty) {
        logger.info(
          s"Sync task ${group.groupIndex}: applying column name mapping (${config.columnNameMapping.size} columns)"
        )
        companionConfig.withFieldIdMapping(config.columnNameMapping.asJava)
      }
      if (config.autoDetectNameMapping) {
        companionConfig.withAutoDetectNameMapping(true)
      }

      // 3. Call QuickwitSplit.createFromParquet()
      val splitId        = UUID.randomUUID().toString
      val splitFileName  = s"companion-${group.groupIndex}-$splitId.split"
      val localSplitPath = new File(tempDir, splitFileName).getAbsolutePath

      val metadata = QuickwitSplit.createFromParquet(
        localFiles.asJava,
        localSplitPath,
        companionConfig
      )

      // 4. Upload companion split to destination
      val partitionPrefix = if (group.partitionValues.nonEmpty) {
        group.partitionValues.toSeq.sorted
          .map { case (k, v) => s"$k=$v" }
          .mkString("/") + "/"
      } else {
        ""
      }
      val destSplitPath = s"${config.splitTablePath.stripSuffix("/")}/$partitionPrefix$splitFileName"
      val splitSize     = uploadSplit(localSplitPath, destSplitPath, config.storageConfig, config.splitTablePath)

      OutputMetricsUpdater.updateOutputMetrics(splitSize, 1)

      // 5. Compute relative parquet file paths for manifest
      val relativeParquetPaths = group.parquetFiles.map(path => extractRelativePath(path, group.parquetTableRoot))

      // 6. Build AddAction
      val addAction = AddAction(
        path = s"$partitionPrefix$splitFileName",
        partitionValues = group.partitionValues,
        size = splitSize,
        modificationTime = System.currentTimeMillis(),
        dataChange = true,
        numRecords = Some(metadata.getNumDocs),
        footerStartOffset = if (metadata.hasFooterOffsets()) Some(metadata.getFooterStartOffset) else None,
        footerEndOffset = if (metadata.hasFooterOffsets()) Some(metadata.getFooterEndOffset) else None,
        hasFooterOffsets = metadata.hasFooterOffsets(),
        docMappingJson = Option(metadata.getDocMappingJson),
        uncompressedSizeBytes = Some(metadata.getUncompressedSizeBytes),
        companionSourceFiles = Some(relativeParquetPaths),
        companionDeltaVersion = None, // Set by driver after task completion
        companionFastFieldMode = Some(config.fastFieldMode)
      )

      val durationMs = System.currentTimeMillis() - startTime
      logger.info(
        s"Sync task ${group.groupIndex}: completed in ${durationMs}ms, " +
          s"downloaded ${totalBytesDownloaded.get()} bytes, uploaded $splitSize bytes, " +
          s"${metadata.getNumDocs} documents"
      )

      SyncTaskResult(
        addAction = addAction,
        bytesDownloaded = totalBytesDownloaded.get(),
        bytesUploaded = splitSize,
        parquetFilesIndexed = group.parquetFiles.size
      )
    } finally
      deleteRecursively(tempDir)
  }

  private def createTempDir(): File = {
    // Prefer /local_disk0 for Databricks/EMR
    val baseDir = if (new File("/local_disk0").isDirectory) {
      new File("/local_disk0/temp")
    } else {
      new File(System.getProperty("java.io.tmpdir"))
    }
    baseDir.mkdirs()
    val tempDir = new File(baseDir, s"sync-${UUID.randomUUID()}")
    tempDir.mkdirs()
    tempDir
  }

  private def extractRelativePath(absolutePath: String, tableRoot: String): String = {
    val normalizedPath = absolutePath.stripSuffix("/")
    val normalizedRoot = tableRoot.stripSuffix("/")
    if (normalizedPath.startsWith(normalizedRoot)) {
      normalizedPath.substring(normalizedRoot.length).stripPrefix("/")
    } else if (tableRoot.contains("/") || tableRoot.contains(":\\")) {
      // tableRoot is a filesystem path but doesn't match — extract filename
      new File(absolutePath).getName
    } else {
      // tableRoot is not a filesystem path (e.g., Iceberg table identifier "default.table")
      // — store the full path so anti-join can match on re-sync
      normalizedPath
    }
  }

  private def downloadFile(
    sourcePath: String,
    destFile: File,
    storageConfig: Map[String, String],
    tableRoot: String
  ): Long = {
    logger.debug(s"Downloading $sourcePath to ${destFile.getAbsolutePath}")

    if (sourcePath.startsWith("s3://") || sourcePath.startsWith("s3a://")) {
      downloadFromS3(sourcePath, destFile, storageConfig, tableRoot)
    } else if (sourcePath.startsWith("abfss://") || sourcePath.startsWith("wasbs://")) {
      downloadFromAzure(sourcePath, destFile, storageConfig)
    } else {
      // Local filesystem copy
      val sourceFile = new File(sourcePath)
      Files.copy(sourceFile.toPath, destFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      sourceFile.length()
    }
  }

  private def downloadFromS3(
    s3Path: String,
    destFile: File,
    storageConfig: Map[String, String],
    tableRoot: String
  ): Long = {
    import io.indextables.spark.utils.CredentialProviderFactory

    // Resolve credentials JIT using a path suitable for the credential provider.
    // For Delta/Parquet, tableRoot is an S3 path — use it directly for cache-friendly resolution.
    // For Iceberg, tableRoot is a table identifier (e.g., "default.my_table") — not a valid S3 URI.
    // In that case, derive the table base path from the file being downloaded by stripping
    // the filename and any Hive-style partition segments (key=value/), so the credential
    // provider (e.g., UnityCatalogAWSCredentialProvider) receives a properly scoped S3 URI.
    val credentialPath = if (tableRoot.startsWith("s3://") || tableRoot.startsWith("s3a://")) {
      tableRoot
    } else {
      extractTableBasePath(s3Path)
    }
    val resolvedCreds = CredentialProviderFactory.resolveAWSCredentialsFromConfig(storageConfig, credentialPath)

    val (bucket, key) = parseS3Path(s3Path)
    val clientBuilder = software.amazon.awssdk.services.s3.S3Client.builder()

    resolvedCreds.foreach { creds =>
      val awsCreds = software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
        creds.accessKey,
        creds.secretKey
      )
      val credProvider = creds.sessionToken match {
        case Some(token) =>
          software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
            software.amazon.awssdk.auth.credentials.AwsSessionCredentials.create(
              creds.accessKey,
              creds.secretKey,
              token
            )
          )
        case None =>
          software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(awsCreds)
      }
      clientBuilder.credentialsProvider(credProvider)
    }

    storageConfig.get("spark.indextables.aws.region").foreach { region =>
      clientBuilder.region(software.amazon.awssdk.regions.Region.of(region))
    }

    storageConfig.get("spark.indextables.s3.endpoint").foreach { endpoint =>
      clientBuilder.endpointOverride(java.net.URI.create(endpoint))
    }

    val pathStyle = storageConfig
      .get("spark.indextables.s3.pathStyleAccess")
      .orElse(storageConfig.get("spark.indextables.aws.pathStyleAccess"))
      .exists(_.equalsIgnoreCase("true"))
    if (pathStyle) {
      clientBuilder.forcePathStyle(true)
    }

    val client = clientBuilder.build()
    try {
      val request = software.amazon.awssdk.services.s3.model.GetObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .build()

      val response = client.getObject(request, destFile.toPath)
      destFile.length()
    } finally
      client.close()
  }

  private def downloadFromAzure(
    azurePath: String,
    destFile: File,
    storageConfig: Map[String, String]
  ): Long = {
    // For Azure, use Hadoop FileSystem API since azure-storage-blob has different URL formats
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    storageConfig.foreach { case (k, v) => hadoopConf.set(k, v) }
    val fs          = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(azurePath), hadoopConf)
    val path        = new org.apache.hadoop.fs.Path(azurePath)
    val inputStream = fs.open(path)
    try {
      Files.copy(inputStream, destFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      destFile.length()
    } finally
      inputStream.close()
  }

  private def uploadSplit(
    localPath: String,
    destPath: String,
    storageConfig: Map[String, String],
    tablePath: String
  ): Long = {
    // Remove source table ID from config so uploads resolve credentials for the
    // DESTINATION path, not the source Iceberg table. The table ID is only valid
    // for the source table's storage location.
    val uploadConfig = storageConfig - "spark.indextables.iceberg.uc.tableId"
    io.indextables.spark.io.merge.MergeUploader.uploadWithRetry(localPath, destPath, uploadConfig, tablePath)
  }

  private def parseS3Path(s3Path: String): (String, String) = {
    val path       = s3Path.replaceFirst("^s3a?://", "")
    val slashIndex = path.indexOf('/')
    if (slashIndex < 0) {
      (path, "")
    } else {
      (path.substring(0, slashIndex), path.substring(slashIndex + 1))
    }
  }

  /**
   * Extract the table base path from a full file path by stripping the filename and any trailing Hive-style partition
   * segments (key=value/).
   *
   * Example: s3://bucket/warehouse/db/table/data/region=us-east/file.parquet -> s3://bucket/warehouse/db/table/data
   */
  def extractTableBasePath(filePath: String): String = {
    // Strip scheme prefix, remember it for reconstruction
    val schemePattern     = "^(s3a?://|abfss?://|wasbs?://)".r
    val scheme            = schemePattern.findFirstIn(filePath).getOrElse("")
    val pathWithoutScheme = filePath.substring(scheme.length)

    // Split into segments, drop the filename (last segment)
    val segments = pathWithoutScheme.split("/").dropRight(1)

    // Walk backwards, dropping Hive-style partition segments (contain '=')
    val baseSegments = segments.reverse.dropWhile(_.contains("=")).reverse

    scheme + baseSegments.mkString("/")
  }

  private def deleteRecursively(file: File): Unit =
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
      file.delete()
    } else {
      file.delete()
    }
}
