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
    // TODO: Implement schema inference from Tantivy index metadata
    None
  }
  
  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    new TantivyOutputWriterFactory(options, dataSchema)
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