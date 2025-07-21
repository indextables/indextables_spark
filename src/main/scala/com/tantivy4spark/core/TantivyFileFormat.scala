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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

class TantivyFileFormat extends FileFormat with DataSourceRegister {
  
  override def shortName(): String = "tantivy"
  
  override def toString: String = "Tantivy"
  
  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    
    println(s"[DEBUG] inferSchema called with ${files.length} files")
    println(s"[DEBUG] Options available: ${options.keys.mkString(", ")}")
    println(s"[DEBUG] Files: ${files.map(_.getPath.toString).mkString(", ")}")
    
    // Always try to infer from transaction log first, regardless of file presence
    // Get the base path from options or derive it from the first file if available
    val basePath = if (files.nonEmpty) {
      files.head.getPath.getParent.toString
    } else {
      // If no files, try to get path from various option keys
      val possiblePaths = Seq("path", "load.path", "tantivy.base.path")
        .flatMap(options.get)
        .filter(_.nonEmpty)
      
      if (possiblePaths.nonEmpty) {
        val selectedPath = possiblePaths.head
        println(s"[DEBUG] Using path from options: $selectedPath")
        selectedPath
      } else {
        println(s"[DEBUG] No files and no path in options - cannot infer schema")
        return None
      }
    }
    
    println(s"[DEBUG] Trying to infer schema from base path: $basePath")
    
    // Try to find the transaction log - it could be directly in basePath or in subdirectories
    import com.tantivy4spark.transaction.TransactionLog
    
    // First try the basePath directly
    val txnLog = new TransactionLog(basePath, options)
    val schemaOpt = txnLog.inferSchemaFromTransactionLog(basePath)
    
    if (schemaOpt.isDefined) {
      println(s"[DEBUG] Found schema in transaction log for path: $basePath")
      return schemaOpt
    }
    
    // If basePath is from tantivy.base.path, try looking in subdirectories
    if (options.get("tantivy.base.path").contains(basePath)) {
      println(s"[DEBUG] Searching for transaction logs in subdirectories of: $basePath")
      val indexId = options.getOrElse("index.id", "default")
      
      // Try common dataset path patterns
      val possibleDatasetPaths = Seq(
        s"$basePath/${indexId.replace("_test", "_index")}",  // e.g. large_dataset_test -> large_dataset_index
        s"$basePath/$indexId",
        s"$basePath/${indexId}_index"
      )
      
      for (datasetPath <- possibleDatasetPaths) {
        println(s"[DEBUG] Checking dataset path: $datasetPath")
        val datasetTxnLog = new TransactionLog(datasetPath, options)
        val datasetSchemaOpt = datasetTxnLog.inferSchemaFromTransactionLog(datasetPath)
        
        if (datasetSchemaOpt.isDefined) {
          println(s"[DEBUG] Found schema in transaction log for dataset path: $datasetPath")
          return datasetSchemaOpt
        }
      }
    }
    
    // Try additional base paths if we have multiple files
    val additionalBasePaths = files.map(_.getPath.getParent.toString).distinct.filter(_ != basePath)
    for (additionalBasePath <- additionalBasePaths) {
      println(s"[DEBUG] Trying additional base path: $additionalBasePath")
      
      val additionalTxnLog = new TransactionLog(additionalBasePath, options)
      val additionalSchemaOpt = additionalTxnLog.inferSchemaFromTransactionLog(additionalBasePath)
      
      if (additionalSchemaOpt.isDefined) {
        println(s"[DEBUG] Found schema in transaction log for path: $additionalBasePath")
        return additionalSchemaOpt
      }
    }
    
    // Fallback to inferring from index files if we have any
    for (file <- files) {
      println(s"[DEBUG] Trying to infer schema from index file: ${file.getPath}")
      
      import com.tantivy4spark.config.TantivyConfig
      import scala.util.{Success, Failure}
      
      TantivyConfig.inferSchemaFromIndex(file.getPath.toString) match {
        case Success(schema) =>
          println(s"[DEBUG] Inferred schema from index with ${schema.fields.length} fields")
          return Some(schema)
        case Failure(exception) =>
          println(s"[DEBUG] Failed to infer schema from index: ${exception.getMessage}")
      }
    }
    
    // Final fallback to default schema
    println(s"[DEBUG] Using default schema as fallback")
    Some(createDefaultSchema())
  }
  
  private def createDefaultSchema(): StructType = {
    import org.apache.spark.sql.types._
    
    new StructType(Array(
      StructField("id", StringType, nullable = true),
      StructField("content", StringType, nullable = true),
      StructField("timestamp", TimestampType, nullable = true)
    ))
  }
  
  private def writeSchemaToTransactionLog(outputPath: String, dataSchema: StructType, options: Map[String, String]): Unit = {
    try {
      import com.tantivy4spark.transaction.TransactionLog
      println(s"[DEBUG] Writing schema to transaction log at final path: $outputPath")
      
      val txnLog = new TransactionLog(outputPath, options)
      txnLog.appendSchemaEntry(dataSchema)
      txnLog.commit()
      
      println(s"[DEBUG] Successfully wrote schema to transaction log with ${dataSchema.fields.length} fields")
    } catch {
      case e: Exception =>
        println(s"[WARNING] Failed to write schema to transaction log at $outputPath: ${e.getMessage}")
        e.printStackTrace()
    }
  }
  
  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    
    // Get the final dataset path from Spark's output configuration
    val outputPath = job.getConfiguration.get("mapreduce.output.fileoutputformat.outputdir", "")
    println(s"[DEBUG] TantivyFileFormat.prepareWrite - Final output path: $outputPath")
    
    // Write schema to transaction log at the final location
    if (outputPath.nonEmpty) {
      writeSchemaToTransactionLog(outputPath, dataSchema, options)
    }
    
    // Pass the final dataset path in options
    val updatedOptions = options + ("dataset.final.path" -> outputPath)
    new TantivyOutputWriterFactory(updatedOptions, dataSchema)
  }
  
  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[org.apache.spark.sql.sources.Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    
    (partitionedFile: PartitionedFile) => {
      new TantivyFileReader(
        partitionedFile,
        requiredSchema,
        filters,
        options,
        hadoopConf
      ).read()
    }
  }
  
  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = true
  
  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: org.apache.hadoop.fs.Path): Boolean = true
}