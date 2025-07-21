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
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.mapreduce.TaskAttemptContext
import com.tantivy4spark.transaction.TransactionLog
import com.tantivy4spark.search.TantivyIndexWriter
import com.tantivy4spark.storage.FileProtocolUtils

class TantivyOutputWriter(
    outputPath: String,
    dataSchema: StructType,
    options: Map[String, String],
    context: TaskAttemptContext
) extends OutputWriter {
  
  override def path(): String = outputPath
  
  // Use the final dataset path if available, otherwise fall back to outputPath
  private val finalDatasetPath = options.getOrElse("dataset.final.path", outputPath)
  private val transactionLog = new TransactionLog(finalDatasetPath, options)
  private val indexWriter = new TantivyIndexWriter(outputPath, dataSchema, options)
  
  // Set the schema in transaction log for this write session
  transactionLog.setDatasetSchema(dataSchema)
  
  // Validate schema compatibility for existing datasets using the final dataset path
  private val validationResult = SchemaCompatibilityValidator.validateSchemaForWrite(finalDatasetPath, dataSchema, options)
  if (!validationResult.isCompatible) {
    val errorMessage = s"Schema validation failed: ${validationResult.errors.mkString(", ")}"
    println(s"[ERROR] $errorMessage")
    throw new IllegalArgumentException(errorMessage)
  }
  
  // Log warnings if any
  validationResult.warnings.foreach { warning =>
    println(s"[WARNING] Schema validation: $warning")
  }
  
  // Log the storage strategy being used
  private val usingS3Optimization = FileProtocolUtils.shouldUseS3OptimizedIO(outputPath, options)
  println(s"[INFO] TantivyOutputWriter using ${if (usingS3Optimization) "S3-optimized" else "standard Hadoop"} storage for path: $outputPath")
  
  override def write(row: InternalRow): Unit = {
    val writeResult = indexWriter.writeRow(row)
    transactionLog.appendEntry(writeResult)
  }
  
  override def close(): Unit = {
    indexWriter.close()
    transactionLog.commit()
  }
}