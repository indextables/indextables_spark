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

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.{StructType, DataType}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.tantivy4spark.config.TantivyConfig
import com.tantivy4spark.storage.{FileProtocolUtils, StandardFileWriter}
import org.apache.hadoop.conf.Configuration
import scala.util.{Try, Success, Failure}

object TransactionLog {
  private val nextTransactionId = new AtomicLong(System.currentTimeMillis())
}

case class TransactionEntry(
    timestamp: Long,
    operation: String,
    filePath: String,
    metadata: Map[String, String],
    schemaJson: Option[String] = None
)

case class WriteResult(
    filePath: String,
    bytesWritten: Long,
    recordCount: Long,
    checksum: String
)

class TransactionLog(basePath: String, options: Map[String, String]) {
  
  private val transactionId = TransactionLog.nextTransactionId.incrementAndGet()
  private val entries = ListBuffer[TransactionEntry]()
  private val logPath = s"$basePath/_transaction_log"
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  
  private val standardWriter = new StandardFileWriter(new Configuration(), options)
  private var datasetSchema: Option[StructType] = None
  
  def setDatasetSchema(schema: StructType): Unit = {
    datasetSchema = Some(schema)
  }
  
  def appendEntry(writeResult: WriteResult): Unit = {
    val entry = TransactionEntry(
      timestamp = System.currentTimeMillis(),
      operation = "WRITE",
      filePath = writeResult.filePath,
      metadata = Map(
        "bytes_written" -> writeResult.bytesWritten.toString,
        "record_count" -> writeResult.recordCount.toString,
        "checksum" -> writeResult.checksum,
        "transaction_id" -> transactionId.toString
      ),
      schemaJson = datasetSchema.map(schema => objectMapper.writeValueAsString(schema.json))
    )
    entries += entry
  }
  
  def appendSchemaEntry(schema: StructType): Unit = {
    val schemaJson = objectMapper.writeValueAsString(schema.json)
    val entry = TransactionEntry(
      timestamp = System.currentTimeMillis(),
      operation = "SCHEMA",
      filePath = logPath,
      metadata = Map(
        "transaction_id" -> transactionId.toString,
        "schema_version" -> "1.0"
      ),
      schemaJson = Some(schemaJson)
    )
    entries += entry
    datasetSchema = Some(schema)
  }
  
  def commit(): Unit = {
    val commitEntry = TransactionEntry(
      timestamp = System.currentTimeMillis(),
      operation = "COMMIT",
      filePath = logPath,
      metadata = Map(
        "transaction_id" -> transactionId.toString,
        "entries_count" -> entries.length.toString
      )
    )
    entries += commitEntry
    
    writeLogToStorage()
  }
  
  def rollback(): Unit = {
    val rollbackEntry = TransactionEntry(
      timestamp = System.currentTimeMillis(),
      operation = "ROLLBACK",
      filePath = logPath,
      metadata = Map(
        "transaction_id" -> transactionId.toString,
        "entries_count" -> entries.length.toString
      )
    )
    entries += rollbackEntry
    
    writeLogToStorage()
    cleanupFiles()
  }
  
  private def writeLogToStorage(): Unit = {
    try {
      val logEntries = entries.map { entry =>
        Map(
          "timestamp" -> entry.timestamp,
          "operation" -> entry.operation,
          "filePath" -> entry.filePath,
          "metadata" -> entry.metadata,
          "schemaJson" -> entry.schemaJson.getOrElse("")
        )
      }
      
      val logData = objectMapper.writeValueAsString(logEntries.toList)
      val logFileName = s"${logPath}_${transactionId}.json"
      
      if (FileProtocolUtils.shouldUseS3OptimizedIO(logFileName, options)) {
        // For S3, we'll still use standard writer for simplicity in transaction logs
        standardWriter.writeToFile(logFileName, logData.getBytes("UTF-8")) match {
          case Success(_) => println(s"[DEBUG] Transaction log written to: $logFileName")
          case Failure(exception) => println(s"[ERROR] Failed to write transaction log: ${exception.getMessage}")
        }
      } else {
        standardWriter.writeToFile(logFileName, logData.getBytes("UTF-8")) match {
          case Success(_) => println(s"[DEBUG] Transaction log written to: $logFileName")
          case Failure(exception) => println(s"[ERROR] Failed to write transaction log: ${exception.getMessage}")
        }
      }
    } catch {
      case e: Exception => println(s"[ERROR] Failed to write transaction log: ${e.getMessage}")
    }
  }
  
  private def cleanupFiles(): Unit = {
    // TODO: Remove files created during this transaction
  }
  
  def getEntries: List[TransactionEntry] = entries.toList
  
  def getDatasetSchema: Option[StructType] = datasetSchema
  
  def inferSchemaFromTransactionLog(basePath: String): Option[StructType] = {
    try {
      val logFiles = standardWriter.listFiles(basePath)
        .filter(_.contains("_transaction_log_"))
        .filter(_.endsWith(".json"))
        .sorted.reverse // Get most recent first
      
      for (logFile <- logFiles) {
        val schemaOpt = extractSchemaFromLogFile(logFile)
        if (schemaOpt.isDefined) {
          return schemaOpt
        }
      }
      
      None
    } catch {
      case e: Exception =>
        println(s"[ERROR] Failed to infer schema from transaction log: ${e.getMessage}")
        None
    }
  }
  
  private def extractSchemaFromLogFile(logFilePath: String): Option[StructType] = {
    try {
      import org.apache.hadoop.fs.{FileSystem, Path}
      import org.apache.hadoop.conf.Configuration
      
      val path = new Path(logFilePath)
      val fs = FileSystem.get(path.toUri, new Configuration())
      
      if (fs.exists(path)) {
        val inputStream = fs.open(path)
        try {
          val jsonContent = scala.io.Source.fromInputStream(inputStream).mkString
          val entries = objectMapper.readValue(jsonContent, classOf[List[Map[String, Any]]])
          
          // Look for the most recent schema entry or write entry with schema
          for (entryMap <- entries.reverse) {
            val operation = entryMap.getOrElse("operation", "").toString
            val schemaJsonOpt = Option(entryMap.getOrElse("schemaJson", "").toString)
              .filter(_.nonEmpty)
            
            if ((operation == "SCHEMA" || operation == "WRITE") && schemaJsonOpt.isDefined) {
              try {
                val schemaJson = schemaJsonOpt.get
                val sparkSchema = DataType.fromJson(schemaJson).asInstanceOf[StructType]
                return Some(sparkSchema)
              } catch {
                case _: Exception => // Continue to next entry
              }
            }
          }
        } finally {
          inputStream.close()
        }
      }
      None
    } catch {
      case e: Exception =>
        println(s"[DEBUG] Could not extract schema from log file $logFilePath: ${e.getMessage}")
        None
    }
  }
}