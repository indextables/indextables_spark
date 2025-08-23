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
import com.tantivy4spark.util.JsonUtil
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.util.{Try, Success, Failure}

class TransactionLog(tablePath: Path, spark: SparkSession) {
  
  private val logger = LoggerFactory.getLogger(classOf[TransactionLog])
  private val fs = tablePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
  private val transactionLogPath = new Path(tablePath, "_transaction_log")

  def initialize(schema: StructType): Unit = {
    if (!fs.exists(transactionLogPath)) {
      fs.mkdirs(transactionLogPath)
      
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
    }
  }

  def addFile(addAction: AddAction): Long = {
    val version = getLatestVersion() + 1
    writeAction(version, addAction)
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
    version
  }

  def listFiles(): Seq[AddAction] = {
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
    version
  }

  private def writeAction(version: Long, action: Action): Unit = {
    writeActions(version, Seq(action))
  }

  private def writeActions(version: Long, actions: Seq[Action]): Unit = {
    val versionFile = new Path(transactionLogPath, f"$version%020d.json")
    
    val output = fs.create(versionFile)
    try {
      actions.foreach { action =>
        // Wrap actions in the appropriate delta log format
        val wrappedAction = action match {
          case metadata: MetadataAction => Map("metaData" -> metadata)
          case add: AddAction => Map("add" -> add)
          case remove: RemoveAction => Map("remove" -> remove)
        }
        
        val actionJson = JsonUtil.mapper.writeValueAsString(wrappedAction)
        output.writeBytes(actionJson + "\n")
      }
    } finally {
      output.close()
    }
    
    logger.info(s"Written ${actions.length} actions to version $version: ${actions.map(_.getClass.getSimpleName).mkString(", ")}")
  }

  private def readVersion(version: Long): Seq[Action] = {
    val versionFile = new Path(transactionLogPath, f"$version%020d.json")
    
    if (!fs.exists(versionFile)) {
      return Seq.empty
    }

    Try {
      val input = fs.open(versionFile)
      try {
        val content = scala.io.Source.fromInputStream(input).getLines().mkString("\n")
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
      } finally {
        input.close()
      }
    } match {
      case Success(actions) => actions
      case Failure(ex) =>
        logger.error(s"Failed to read version $version", ex)
        Seq.empty
    }
  }

  private def getVersions(): Seq[Long] = {
    Try {
      val status = fs.listStatus(transactionLogPath)
      val result: Seq[Long] = status
        .filter(_.isFile)
        .map(_.getPath.getName)
        .filter(_.endsWith(".json"))
        .map(_.replace(".json", "").toLong)
        .sorted.toSeq
      result
    }.getOrElse(Seq.empty[Long])
  }

  private def getLatestVersion(): Long = {
    val versions = getVersions()
    if (versions.nonEmpty) versions.max else -1L
  }
}