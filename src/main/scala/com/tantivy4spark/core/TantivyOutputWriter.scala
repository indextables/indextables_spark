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
  
  private val transactionLog = new TransactionLog(outputPath, options)
  private val indexWriter = new TantivyIndexWriter(outputPath, dataSchema, options)
  
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