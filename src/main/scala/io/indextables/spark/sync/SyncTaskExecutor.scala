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

import io.indextables.spark.transaction.AddAction
import io.indextables.tantivy4java.split.ParquetCompanionConfig
import io.indextables.tantivy4java.split.merge.QuickwitSplit

import org.apache.spark.sql.indextables.OutputMetricsUpdater

import org.slf4j.LoggerFactory

/**
 * Serializable configuration for a sync task, passed from driver to executor.
 */
case class SyncConfig(
  indexingModes: Map[String, String],
  fastFieldMode: String,
  splitCredentials: Map[String, String],
  parquetCredentials: Map[String, String],
  splitTablePath: String,
  writerHeapSize: Long = 2L * 1024L * 1024L * 1024L, // 2GB default
  readerBatchSize: Int = 8192)
    extends Serializable

/**
 * Represents a group of parquet files to be indexed into a single companion split.
 * Sent from driver to executor as part of a Spark task.
 */
case class SyncIndexingGroup(
  parquetFiles: Seq[String],
  parquetTableRoot: String,
  partitionValues: Map[String, String],
  groupIndex: Int)
    extends Serializable

/**
 * Result of indexing a single group of parquet files into a companion split.
 * Returned from executor to driver.
 */
case class SyncTaskResult(
  addAction: AddAction,
  bytesDownloaded: Long,
  bytesUploaded: Long,
  parquetFilesIndexed: Int)
    extends Serializable

/**
 * Executor-side companion split creation. Handles one indexing group:
 * downloads parquet files, calls QuickwitSplit.createFromParquet(), uploads the result.
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
    val tempDir = createTempDir()
    logger.info(
      s"Sync task ${group.groupIndex}: indexing ${group.parquetFiles.size} parquet files into companion split"
    )

    try {
      // 1. Download parquet files to local temp, preserving relative paths
      var totalBytesDownloaded = 0L
      val localFiles = group.parquetFiles.map { parquetPath =>
        val relativePath = extractRelativePath(parquetPath, group.parquetTableRoot)
        val localFile = new File(tempDir, relativePath)
        localFile.getParentFile.mkdirs()

        val bytesDownloaded = downloadFile(parquetPath, localFile, config.parquetCredentials)
        totalBytesDownloaded += bytesDownloaded
        localFile.getAbsolutePath
      }

      OutputMetricsUpdater.updateInputMetrics(totalBytesDownloaded, group.parquetFiles.size)

      // 2. Build ParquetCompanionConfig
      val companionConfig = new ParquetCompanionConfig(tempDir.getAbsolutePath)
        .withFastFieldMode(
          ParquetCompanionConfig.FastFieldMode.valueOf(config.fastFieldMode)
        )
        .withWriterHeapSize(config.writerHeapSize)
        .withReaderBatchSize(config.readerBatchSize)

      // 3. Call QuickwitSplit.createFromParquet()
      val splitId = UUID.randomUUID().toString
      val splitFileName = s"companion-${group.groupIndex}-$splitId.split"
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
      val destSplitPath = s"${config.splitTablePath}/$partitionPrefix$splitFileName"
      val splitSize = uploadSplit(localSplitPath, destSplitPath, config.splitCredentials)

      OutputMetricsUpdater.updateOutputMetrics(splitSize, 1)

      // 5. Compute relative parquet file paths for manifest
      val relativeParquetPaths = group.parquetFiles.map { path =>
        extractRelativePath(path, group.parquetTableRoot)
      }

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
          s"downloaded ${totalBytesDownloaded} bytes, uploaded $splitSize bytes, " +
          s"${metadata.getNumDocs} documents"
      )

      SyncTaskResult(
        addAction = addAction,
        bytesDownloaded = totalBytesDownloaded,
        bytesUploaded = splitSize,
        parquetFilesIndexed = group.parquetFiles.size
      )
    } finally {
      deleteRecursively(tempDir)
    }
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
    } else {
      // For cloud paths, extract the last path components
      new File(absolutePath).getName
    }
  }

  private def downloadFile(
    sourcePath: String,
    destFile: File,
    credentials: Map[String, String]
  ): Long = {
    logger.debug(s"Downloading $sourcePath to ${destFile.getAbsolutePath}")

    if (sourcePath.startsWith("s3://") || sourcePath.startsWith("s3a://")) {
      downloadFromS3(sourcePath, destFile, credentials)
    } else if (sourcePath.startsWith("abfss://") || sourcePath.startsWith("wasbs://")) {
      downloadFromAzure(sourcePath, destFile, credentials)
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
    credentials: Map[String, String]
  ): Long = {
    import io.indextables.spark.utils.CredentialProviderFactory

    val resolvedCreds = CredentialProviderFactory.resolveAWSCredentialsFromConfig(credentials, s3Path)

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

    credentials.get("spark.indextables.aws.region").foreach { region =>
      clientBuilder.region(software.amazon.awssdk.regions.Region.of(region))
    }

    val client = clientBuilder.build()
    try {
      val request = software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build()

      val response = client.getObject(request, destFile.toPath)
      destFile.length()
    } finally {
      client.close()
    }
  }

  private def downloadFromAzure(
    azurePath: String,
    destFile: File,
    credentials: Map[String, String]
  ): Long = {
    // For Azure, use Hadoop FileSystem API since azure-storage-blob has different URL formats
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    credentials.foreach { case (k, v) => hadoopConf.set(k, v) }
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(azurePath), hadoopConf)
    val path = new org.apache.hadoop.fs.Path(azurePath)
    val inputStream = fs.open(path)
    try {
      Files.copy(inputStream, destFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      destFile.length()
    } finally {
      inputStream.close()
    }
  }

  private def uploadSplit(
    localPath: String,
    destPath: String,
    credentials: Map[String, String]
  ): Long = {
    logger.debug(s"Uploading split from $localPath to $destPath")
    val localFile = new File(localPath)

    if (destPath.startsWith("s3://") || destPath.startsWith("s3a://")) {
      uploadToS3(localFile, destPath, credentials)
    } else if (destPath.startsWith("abfss://") || destPath.startsWith("wasbs://")) {
      uploadToAzure(localFile, destPath, credentials)
    } else {
      // Local filesystem copy
      val destFile = new File(destPath)
      destFile.getParentFile.mkdirs()
      Files.copy(localFile.toPath, destFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      localFile.length()
    }
  }

  private def uploadToS3(
    localFile: File,
    s3Path: String,
    credentials: Map[String, String]
  ): Long = {
    import io.indextables.spark.utils.CredentialProviderFactory

    val resolvedCreds = CredentialProviderFactory.resolveAWSCredentialsFromConfig(credentials, s3Path)
    val (bucket, key) = parseS3Path(s3Path)
    val clientBuilder = software.amazon.awssdk.services.s3.S3Client.builder()

    resolvedCreds.foreach { creds =>
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
          software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
            software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
              creds.accessKey,
              creds.secretKey
            )
          )
      }
      clientBuilder.credentialsProvider(credProvider)
    }

    credentials.get("spark.indextables.aws.region").foreach { region =>
      clientBuilder.region(software.amazon.awssdk.regions.Region.of(region))
    }

    val client = clientBuilder.build()
    try {
      val request = software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build()

      client.putObject(request, localFile.toPath)
      localFile.length()
    } finally {
      client.close()
    }
  }

  private def uploadToAzure(
    localFile: File,
    azurePath: String,
    credentials: Map[String, String]
  ): Long = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    credentials.foreach { case (k, v) => hadoopConf.set(k, v) }
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(azurePath), hadoopConf)
    val outputStream = fs.create(new org.apache.hadoop.fs.Path(azurePath))
    try {
      val inputStream = new java.io.FileInputStream(localFile)
      try {
        val buffer = new Array[Byte](65536)
        var bytesRead = inputStream.read(buffer)
        while (bytesRead != -1) {
          outputStream.write(buffer, 0, bytesRead)
          bytesRead = inputStream.read(buffer)
        }
      } finally {
        inputStream.close()
      }
    } finally {
      outputStream.close()
    }
    localFile.length()
  }

  private def parseS3Path(s3Path: String): (String, String) = {
    val path = s3Path.replaceFirst("^s3a?://", "")
    val slashIndex = path.indexOf('/')
    if (slashIndex < 0) {
      (path, "")
    } else {
      (path.substring(0, slashIndex), path.substring(slashIndex + 1))
    }
  }

  private def deleteRecursively(file: File): Unit =
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
      file.delete()
    } else {
      file.delete()
    }
}
