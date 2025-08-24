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

package com.tantivy4spark.transaction

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.util.JsonUtil
import com.tantivy4spark.io.{CloudStorageProvider, CloudStorageProviderFactory}
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.util.{Try, Success, Failure}
import java.io.{ByteArrayOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

/**
 * Cloud-aware transaction log that uses high-performance cloud storage providers
 * instead of Hadoop filesystem abstraction. Provides better performance and
 * reliability for S3, Azure, and GCP storage.
 */
class CloudTransactionLog(
    tablePath: String, 
    spark: SparkSession, 
    options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
) {
  
  private val logger = LoggerFactory.getLogger(classOf[CloudTransactionLog])
  private val hadoopConf = spark.sparkContext.hadoopConfiguration
  private val storageProvider = CloudStorageProviderFactory.createProvider(tablePath, options, hadoopConf)
  // Use a transaction log path structure that works with S3Mock (avoid underscore prefix)
  private val transactionLogPath = if (tablePath.endsWith("/")) s"${tablePath}tx-log" else s"$tablePath/tx-log"

  def initialize(schema: StructType): Unit = {
    // Check if transaction log directory exists by looking for any files
    val existingFiles = storageProvider.listFiles(transactionLogPath, recursive = false)
    
    if (existingFiles.isEmpty) {
      // For cloud storage (S3, Azure, GCP), directories don't need to be explicitly created
      // Just write the initial metadata file - the directory will be created implicitly
      
      // Write initial metadata file
      val metadataAction = MetadataAction(
        id = java.util.UUID.randomUUID().toString,
        name = None,
        description = None,
        format = FileFormat("tantivy4spark", Map.empty),
        schemaString = schema.json,
        partitionColumns = Seq.empty,
        configuration = Map.empty,
        createdTime = Some(System.currentTimeMillis())
      )
      
      writeAction(0, metadataAction)
      logger.info(s"Initialized transaction log at $transactionLogPath with schema")
    } else {
      logger.info(s"Transaction log already exists at $transactionLogPath with ${existingFiles.size} files")
    }
  }

  def addFile(addAction: AddAction): Long = {
    val version = getLatestVersion() + 1
    writeAction(version, addAction)
    logger.info(s"Added file to transaction log version $version: ${addAction.path}")
    version
  }

  /**
   * Add multiple files in a single transaction (like Delta Lake).
   * This creates one JSON file with multiple ADD entries.
   */
  def addFiles(addActions: Seq[AddAction]): Long = {
    if (addActions.isEmpty) {
      return getLatestVersion()
    }
    
    val version = getLatestVersion() + 1
    writeActions(version, addActions)
    logger.info(s"Added ${addActions.size} files to transaction log version $version")
    version
  }

  def listFiles(): Seq[AddAction] = {
    val versions = getVersions()
    if (versions.isEmpty) {
      return Seq.empty
    }
    
    // Read all transaction log files in parallel for better performance
    val versionFilePaths = versions.map { version =>
      val versionFileName = f"$version%020d.json"
      s"$transactionLogPath/$versionFileName"
    }
    
    logger.debug(s"Reading ${versionFilePaths.size} transaction log files in parallel")
    val fileContents = storageProvider.readFilesParallel(versionFilePaths)
    
    val files = ListBuffer[AddAction]()
    
    // Process transaction log files in version order
    for (version <- versions) {
      val versionFileName = f"$version%020d.json"
      val versionFilePath = s"$transactionLogPath/$versionFileName"
      
      fileContents.get(versionFilePath) match {
        case Some(content) =>
          val actions = parseTransactionLogContent(new String(content, StandardCharsets.UTF_8))
          actions.foreach {
            case add: AddAction => files += add
            case remove: RemoveAction => files --= files.filter(_.path == remove.path)
            case _ => // Ignore other actions for file listing
          }
        case None =>
          logger.warn(s"Failed to read transaction log version $version in parallel")
      }
    }
    
    logger.debug(s"Listed ${files.size} files from transaction log using parallel reads")
    files.toSeq
  }

  def getSchema(): Option[StructType] = {
    Try {
      val versions = getVersions()
      if (versions.nonEmpty) {
        val actions = readVersion(versions.head)
        actions.collectFirst {
          case metadata: MetadataAction => DataType.fromJson(metadata.schemaString).asInstanceOf[StructType]
        }
      } else {
        None
      }
    }.getOrElse(None)
  }

  def removeFile(path: String, deletionTimestamp: Long = System.currentTimeMillis()): Long = {
    val version = getLatestVersion() + 1
    val removeAction = RemoveAction(
      path = path,
      deletionTimestamp = Some(deletionTimestamp),
      dataChange = true,
      extendedFileMetadata = None,
      partitionValues = None,
      size = None
    )
    writeAction(version, removeAction)
    logger.info(s"Removed file from transaction log version $version: $path")
    version
  }

  private def writeAction(version: Long, action: Action): Unit = {
    writeActions(version, Seq(action))
  }

  private def writeActions(version: Long, actions: Seq[Action]): Unit = {
    val versionFileName = f"$version%020d.json"
    val versionFilePath = s"$transactionLogPath/$versionFileName"
    
    try {
      // Build JSON content in memory
      val buffer = new ByteArrayOutputStream()
      val writer = new OutputStreamWriter(buffer, StandardCharsets.UTF_8)
      
      try {
        actions.foreach { action =>
          // Wrap actions in the appropriate delta log format
          val wrappedAction = action match {
            case metadata: MetadataAction => Map("metaData" -> metadata)
            case add: AddAction => Map("add" -> add)
            case remove: RemoveAction => Map("remove" -> remove)
          }
          
          val actionJson = JsonUtil.mapper.writeValueAsString(wrappedAction)
          writer.write(actionJson)
          writer.write("\n")
        }
      } finally {
        writer.close()
      }
      
      // Write to cloud storage
      storageProvider.writeFile(versionFilePath, buffer.toByteArray)
      
      logger.debug(s"Written ${actions.length} actions to version $version: ${actions.map(_.getClass.getSimpleName).mkString(", ")}")
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to write transaction log version $version", ex)
        throw new RuntimeException(s"Failed to write transaction log: ${ex.getMessage}", ex)
    }
  }

  /**
   * Get the current metadata action from the transaction log.
   */
  def getMetadata(): MetadataAction = {
    val latestVersion = getLatestVersion()
    
    // Look for metadata in reverse chronological order
    for (version <- latestVersion to 0L by -1) {
      val actions = readVersion(version)
      actions.collectFirst {
        case metadata: MetadataAction => metadata
      } match {
        case Some(metadata) => return metadata
        case None => // Continue searching
      }
    }
    
    throw new RuntimeException("No metadata found in transaction log")
  }

  private def readVersion(version: Long): Seq[Action] = {
    val versionFileName = f"$version%020d.json"
    val versionFilePath = s"$transactionLogPath/$versionFileName"
    
    if (!storageProvider.exists(versionFilePath)) {
      return Seq.empty
    }

    Try {
      logger.debug(s"Reading transaction log version $version from $versionFilePath")
      val content = new String(storageProvider.readFile(versionFilePath), StandardCharsets.UTF_8)
      val lines = content.split("\n").filter(_.nonEmpty)
      
      lines.map { line =>
        val jsonNode = JsonUtil.mapper.readTree(line)
        
        if (jsonNode.has("metaData")) {
          val metadataNode = jsonNode.get("metaData")
          JsonUtil.mapper.readValue(metadataNode.toString, classOf[MetadataAction])
        } else if (jsonNode.has("add")) {
          val addNode = jsonNode.get("add")
          JsonUtil.mapper.readValue(addNode.toString, classOf[AddAction])
        } else if (jsonNode.has("remove")) {
          val removeNode = jsonNode.get("remove")
          JsonUtil.mapper.readValue(removeNode.toString, classOf[RemoveAction])
        } else {
          throw new IllegalArgumentException(s"Unknown action type in line: $line")
        }
      }.toSeq
    } match {
      case Success(actions) => 
        logger.debug(s"Read ${actions.size} actions from version $version")
        actions
      case Failure(ex) =>
        logger.error(s"Failed to read version $version", ex)
        Seq.empty
    }
  }

  private def getVersions(): Seq[Long] = {
    Try {
      val files = storageProvider.listFiles(transactionLogPath, recursive = false)
      val versions = files
        .filter(!_.isDirectory)
        .map(_.path)
        .map { path =>
          val fileName = path.substring(path.lastIndexOf("/") + 1)
          fileName
        }
        .filter(_.endsWith(".json"))
        .map(_.replace(".json", "").toLong)
        .sorted
      
      logger.debug(s"Found ${versions.size} transaction log versions: ${versions.mkString(", ")}")
      versions
    }.getOrElse {
      logger.debug("No transaction log versions found")
      Seq.empty[Long]
    }
  }

  private def getLatestVersion(): Long = {
    val versions = getVersions()
    if (versions.nonEmpty) versions.max else -1L
  }
  
  def close(): Unit = {
    storageProvider.close()
    logger.debug(s"Closed cloud transaction log for $tablePath")
  }
  
  /**
   * Parse transaction log file content into actions
   */
  private def parseTransactionLogContent(content: String): Seq[Action] = {
    try {
      val lines = content.split("\n").filter(_.nonEmpty)
      
      lines.map { line =>
        val jsonNode = JsonUtil.mapper.readTree(line)
        
        if (jsonNode.has("metaData")) {
          val metadataNode = jsonNode.get("metaData")
          JsonUtil.mapper.readValue(metadataNode.toString, classOf[MetadataAction])
        } else if (jsonNode.has("add")) {
          val addNode = jsonNode.get("add")
          JsonUtil.mapper.readValue(addNode.toString, classOf[AddAction])
        } else if (jsonNode.has("remove")) {
          val removeNode = jsonNode.get("remove")
          JsonUtil.mapper.readValue(removeNode.toString, classOf[RemoveAction])
        } else {
          throw new IllegalArgumentException(s"Unknown action type in line: $line")
        }
      }.toSeq
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to parse transaction log content", ex)
        Seq.empty
    }
  }
}