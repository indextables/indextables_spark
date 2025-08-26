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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.util.JsonUtil
import com.tantivy4spark.io.{ProtocolBasedIOFactory, CloudStorageProviderFactory}
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.util.{Try, Success, Failure}
import java.io.ByteArrayInputStream
import scala.jdk.CollectionConverters._

class TransactionLog(tablePath: Path, spark: SparkSession, options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())) extends AutoCloseable {
  
  private val logger = LoggerFactory.getLogger(classOf[TransactionLog])
  
  // Determine if we should use cloud-optimized transaction log
  private val protocol = ProtocolBasedIOFactory.determineProtocol(tablePath.toString)
  private val useCloudOptimized = protocol match {
    case ProtocolBasedIOFactory.S3Protocol => !options.getBoolean("spark.tantivy4spark.transaction.force.hadoop", false)
    case _ => false
  }
  
  // Use cloud storage provider instead of direct Hadoop filesystem
  private val cloudProvider = CloudStorageProviderFactory.createProvider(tablePath.toString, options, spark.sparkContext.hadoopConfiguration)
  private val transactionLogPath = new Path(tablePath, "_transaction_log")
  private val transactionLogPathStr = transactionLogPath.toString

  def getTablePath(): Path = tablePath

  override def close(): Unit = {
    cloudProvider.close()
  }

  def initialize(schema: StructType): Unit = {
    initialize(schema, Seq.empty)
  }
  
  def initialize(schema: StructType, partitionColumns: Seq[String]): Unit = {
        if (!cloudProvider.exists(transactionLogPathStr)) {
          cloudProvider.createDirectory(transactionLogPathStr)
          
          // Validate partition columns exist in schema
          val schemaFields = schema.fieldNames.toSet
          val invalidPartitionCols = partitionColumns.filterNot(schemaFields.contains)
          if (invalidPartitionCols.nonEmpty) {
            throw new IllegalArgumentException(s"Partition columns ${invalidPartitionCols.mkString(", ")} not found in schema")
          }
          
          // DEBUG: Log the original schema being written
          logger.info(s"Writing schema to transaction log: ${schema.prettyJson}")
          logger.info(s"Partition columns: ${partitionColumns.mkString(", ")}")
          schema.fields.foreach { field =>
            logger.info(s"Field: ${field.name}, Type: ${field.dataType}, DataType class: ${field.dataType.getClass.getName}")
          }
          
          // Write initial metadata file
          val metadataAction = MetadataAction(
            id = java.util.UUID.randomUUID().toString,
            name = None,
            description = None,
            format = FileFormat("tantivy4spark", Map.empty),
            schemaString = schema.json,
            partitionColumns = partitionColumns,
            configuration = Map.empty,
            createdTime = Some(System.currentTimeMillis())
          )
          
          writeAction(0, metadataAction)
        }
  }

  def addFile(addAction: AddAction): Long = {
        // Legacy Hadoop implementation
        val version = getLatestVersion() + 1
        writeAction(version, addAction)
        version
  }

  /**
   * Add multiple files in a single transaction (like Delta Lake).
   * This creates one JSON file with multiple ADD entries.
   */
  def addFiles(addActions: Seq[AddAction]): Long = {
        // Legacy Hadoop implementation
        if (addActions.isEmpty) {
          return getLatestVersion()
        }
        
        val version = getLatestVersion() + 1
        writeActions(version, addActions)
        version
  }
  
  /**
   * Add files in overwrite mode - removes all existing files and adds new ones.
   * This is similar to Delta Lake's overwrite mode.
   */
  def overwriteFiles(addActions: Seq[AddAction]): Long = {
    if (addActions.isEmpty) {
      logger.warn("Overwrite operation with no files to add")
    }
    
    // Get all existing files to remove
    val existingFiles = listFiles()
    val removeActions = existingFiles.map { existingFile =>
      RemoveAction(
        path = existingFile.path,
        deletionTimestamp = Some(System.currentTimeMillis()),
        dataChange = true,
        extendedFileMetadata = None,
        partitionValues = Some(existingFile.partitionValues),
        size = Some(existingFile.size)
      )
    }
    
    val version = getLatestVersion() + 1
    
    // Write both REMOVE and ADD actions in a single transaction
    val allActions = removeActions ++ addActions
    writeActions(version, allActions)
    
    logger.info(s"Overwrite operation: removed ${removeActions.length} files, added ${addActions.length} files in version $version")
    version
  }

  def listFiles(): Seq[AddAction] = {
        // Legacy Hadoop implementation
        val files = ListBuffer[AddAction]()
        val versions = getVersions()
        
        for (version <- versions) {
          val actions = readVersion(version)
          actions.foreach {
            case add: AddAction => files += add
            case remove: RemoveAction => files --= files.filter(_.path == remove.path)
            case _ => // Ignore other actions for file listing
          }
        }
        
        files.toSeq
  }
  
  /**
   * Get the total row count across all active files.
   */
  def getTotalRowCount(): Long = {
    listFiles().map { file =>
      file.numRecords.map { (count: Any) =>
        // Handle any numeric type and convert to Long
        count match {
          case l: Long => l
          case i: Int => i.toLong
          case i: java.lang.Integer => i.toLong
          case _ => count.toString.toLong
        }
      }.getOrElse(0L)
    }.sum
  }

  def getSchema(): Option[StructType] = {
        // Legacy Hadoop implementation
        Try {
          val versions = getVersions()
          if (versions.nonEmpty) {
            val actions = readVersion(versions.head)
            actions.collectFirst {
              case metadata: MetadataAction => 
                // DEBUG: Log the schema being read from transaction log
                logger.info(s"Reading schema from transaction log: ${metadata.schemaString}")
                val deserializedSchema = DataType.fromJson(metadata.schemaString).asInstanceOf[StructType]
                logger.info(s"Deserialized schema: ${deserializedSchema.prettyJson}")
                deserializedSchema.fields.foreach { field =>
                  logger.info(s"Field: ${field.name}, Type: ${field.dataType}, DataType class: ${field.dataType.getClass.getName}")
                }
                deserializedSchema
            }
          } else {
            None
          }
        }.getOrElse(None)
  }

  def removeFile(path: String, deletionTimestamp: Long = System.currentTimeMillis()): Long = {
        // Legacy Hadoop implementation
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
        version
  }

  private def writeAction(version: Long, action: Action): Unit = {
    writeActions(version, Seq(action))
  }

  private def writeActions(version: Long, actions: Seq[Action]): Unit = {
    val versionFile = new Path(transactionLogPath, f"$version%020d.json")
    val versionFilePath = versionFile.toString
    
    val content = new StringBuilder()
    actions.foreach { action =>
      // Wrap actions in the appropriate delta log format
      val wrappedAction = action match {
        case metadata: MetadataAction => Map("metaData" -> metadata)
        case add: AddAction => Map("add" -> add)
        case remove: RemoveAction => Map("remove" -> remove)
      }
      
      val actionJson = JsonUtil.mapper.writeValueAsString(wrappedAction)
      content.append(actionJson).append("\n")
    }
    
    cloudProvider.writeFile(versionFilePath, content.toString.getBytes("UTF-8"))
    
    logger.info(s"Written ${actions.length} actions to version $version: ${actions.map(_.getClass.getSimpleName).mkString(", ")}")
  }

  /**
   * Get partition columns from metadata.
   */
  def getPartitionColumns(): Seq[String] = {
    try {
      getMetadata().partitionColumns
    } catch {
      case _: Exception => Seq.empty
    }
  }
  
  /**
   * Check if the table is partitioned.
   */
  def isPartitioned(): Boolean = {
    getPartitionColumns().nonEmpty
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
    val versionFile = new Path(transactionLogPath, f"$version%020d.json")
    val versionFilePath = versionFile.toString
    
    if (!cloudProvider.exists(versionFilePath)) {
      return Seq.empty
    }

    Try {
      val content = new String(cloudProvider.readFile(versionFilePath), "UTF-8")
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
      case Success(actions) => actions
      case Failure(ex) =>
        logger.error(s"Failed to read version $version", ex)
        Seq.empty
    }
  }

  private def getVersions(): Seq[Long] = {
    Try {
      val files = cloudProvider.listFiles(transactionLogPathStr, recursive = false)
      val result: Seq[Long] = files
        .filter(!_.isDirectory)
        .map { fileInfo =>
          val path = new Path(fileInfo.path)
          path.getName
        }
        .filter(_.endsWith(".json"))
        .map(_.replace(".json", "").toLong)
        .sorted
      result
    } match {
      case Success(versions) => versions
      case Failure(_) => 
        logger.debug(s"Transaction log directory does not exist yet (normal for new tables): $transactionLogPathStr")
        Seq.empty[Long]
    }
  }

  private def getLatestVersion(): Long = {
    val versions = getVersions()
    if (versions.nonEmpty) versions.max else -1L
  }
}
