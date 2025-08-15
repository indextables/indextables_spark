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
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.transaction.AddAction
import com.tantivy4spark.search.TantivySearchEngine
import com.tantivy4spark.storage.{S3OptimizedReader, StandardFileReader, TantivyArchiveFormat}
import com.tantivy4spark.util.StatisticsCalculator
import com.tantivy4spark.bloom.{BloomFilterManager, BloomFilterStorage}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.io.{IOException, ByteArrayOutputStream}

class Tantivy4SparkInputPartition(
    val addAction: AddAction,
    val readSchema: StructType,
    val filters: Array[Filter],
    val options: CaseInsensitiveStringMap,
    val partitionId: Int
) extends InputPartition

class Tantivy4SparkReaderFactory(
    readSchema: StructType,
    options: CaseInsensitiveStringMap
) extends PartitionReaderFactory {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkReaderFactory])

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val tantivyPartition = partition.asInstanceOf[Tantivy4SparkInputPartition]
    logger.info(s"Creating reader for partition ${tantivyPartition.partitionId}")
    
    new Tantivy4SparkPartitionReader(
      tantivyPartition.addAction,
      readSchema,
      tantivyPartition.filters,
      options
    )
  }
}

class Tantivy4SparkPartitionReader(
    addAction: AddAction,
    readSchema: StructType,
    filters: Array[Filter],
    options: CaseInsensitiveStringMap
) extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkPartitionReader])
  private val spark = SparkSession.active
  private val hadoopConf = spark.sparkContext.hadoopConfiguration
  
  private val filePath = new Path(addAction.path)
  private val reader = createStorageReader()
  
  private var searchEngine: TantivySearchEngine = _
  private var resultIterator: Iterator[InternalRow] = Iterator.empty
  private var initialized = false

  private def createStorageReader() = {
    val protocol = filePath.toUri.getScheme
    val forceStandard = options.getBoolean("spark.tantivy4spark.storage.force.standard", false)
    
    if (!forceStandard && (protocol == "s3" || protocol == "s3a" || protocol == "s3n")) {
      logger.info(s"Using S3OptimizedReader for path: ${addAction.path}")
      new S3OptimizedReader(filePath, hadoopConf)
    } else {
      logger.info(s"Using StandardFileReader for path: ${addAction.path}")
      new StandardFileReader(filePath, hadoopConf)
    }
  }

  private def initialize(): Unit = {
    if (!initialized) {
      try {
        // Read and parse the Tantivy archive
        new com.tantivy4spark.storage.TantivyArchiveReader(reader)
        val indexComponents = TantivyArchiveFormat.readAllComponents(reader)
        
        logger.info(s"Read Tantivy archive with ${indexComponents.size} components: ${indexComponents.keys.mkString(", ")}")
        
        // Create search engine from the archived index components
        searchEngine = TantivySearchEngine.fromIndexComponents(readSchema, indexComponents)
        
        // Convert filters to Tantivy query
        val query = FiltersToQueryConverter.convert(filters)
        
        if (query.nonEmpty) {
          val results = searchEngine.search(query, limit = 10000)
          resultIterator = results.iterator
        } else {
          // No filters, return all documents (this would need special handling in real implementation)
          resultIterator = Iterator.empty
        }
        
        initialized = true
        logger.info(s"Initialized reader for ${addAction.path} with query: $query")
        
      } catch {
        case ex: Exception =>
          logger.error(s"Failed to initialize reader for ${addAction.path}", ex)
          throw new IOException(s"Failed to read Tantivy index: ${ex.getMessage}", ex)
      }
    }
  }

  override def next(): Boolean = {
    initialize()
    resultIterator.hasNext
  }

  override def get(): InternalRow = {
    resultIterator.next()
  }

  override def close(): Unit = {
    if (searchEngine != null) {
      searchEngine.close()
    }
    reader.close()
  }
}

class Tantivy4SparkWriterFactory(
    tablePath: Path,
    writeSchema: StructType,
    options: CaseInsensitiveStringMap,
    hadoopConf: org.apache.hadoop.conf.Configuration
) extends DataWriterFactory {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkWriterFactory])

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    logger.info(s"Creating writer for partition $partitionId, task $taskId")
    new Tantivy4SparkDataWriter(tablePath, writeSchema, partitionId, taskId, options, hadoopConf)
  }
}

class Tantivy4SparkDataWriter(
    tablePath: Path,
    writeSchema: StructType,
    partitionId: Int,
    taskId: Long,
    options: CaseInsensitiveStringMap,
    hadoopConf: org.apache.hadoop.conf.Configuration
) extends DataWriter[InternalRow] {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkDataWriter])
  private val searchEngine = new TantivySearchEngine(writeSchema)
  private val statistics = new StatisticsCalculator.DatasetStatistics(writeSchema)
  private val bloomFilterManager = new BloomFilterManager()
  private val bloomFilterStorage = BloomFilterStorage.getInstance
  private var recordCount = 0L
  
  // Track text values for bloom filter creation
  private val textColumnData = scala.collection.mutable.Map[String, scala.collection.mutable.ArrayBuffer[String]]()
  
  // Initialize text column tracking
  writeSchema.fields.foreach { field =>
    if (field.dataType.typeName == "string") {
      textColumnData(field.name) = scala.collection.mutable.ArrayBuffer[String]()
    }
  }

  override def write(record: InternalRow): Unit = {
    //println(s"Adding document $recordCount to Tantivy index")
    searchEngine.addDocument(record)
    statistics.updateRow(record)
    
    // Collect text values for bloom filter creation
    writeSchema.fields.zipWithIndex.foreach { case (field, index) =>
      if (field.dataType.typeName == "string" && !record.isNullAt(index)) {
        val value = record.getString(index)
        if (value != null && value.nonEmpty) {
          textColumnData(field.name) += value
        }
      }
    }
    
    recordCount += 1
  }

  override def commit(): WriterCommitMessage = {
    println(s"Committing Tantivy index with $recordCount documents")
    // Commit the search engine and get index components
    val indexComponents = searchEngine.commitAndGetComponents()
    println(s"Got ${indexComponents.size} index components: ${indexComponents.keys.mkString(", ")}")
    
    val fileName = f"part-$partitionId%05d-$taskId.tnt4s"
    val filePath = new Path(tablePath, fileName)
    
    // Create the Tantivy archive with all index components
    val archiveData = createTantivyArchive(indexComponents)
    
    // Write archive to storage
    val fileSystem = filePath.getFileSystem(hadoopConf)
    val outputStream = fileSystem.create(filePath)
    
    try {
      outputStream.write(archiveData)
      logger.info(s"Written Tantivy archive $fileName with ${archiveData.length} bytes, $recordCount records")
    } finally {
      outputStream.close()
    }
    
    val minValues = statistics.getMinValues
    val maxValues = statistics.getMaxValues
    
    // Create bloom filters for text columns
    logger.info(s"Creating bloom filters for ${textColumnData.size} text columns")
    val bloomFilters = if (textColumnData.nonEmpty) {
      val filters = bloomFilterManager.createBloomFilters(textColumnData.toMap)
      Some(bloomFilterStorage.encodeBloomFilters(filters))
    } else {
      None
    }
    
    val addAction = AddAction(
      path = filePath.toString,
      partitionValues = Map.empty,
      size = archiveData.length.toLong,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(recordCount),
      minValues = if (minValues.nonEmpty) Some(minValues) else None,
      maxValues = if (maxValues.nonEmpty) Some(maxValues) else None,
      bloomFilters = bloomFilters
    )
    
    logger.info(s"Committed writer for partition $partitionId with $recordCount records")
    Tantivy4SparkCommitMessage(addAction)
  }
  
  private def createTantivyArchive(components: Map[String, Array[Byte]]): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    TantivyArchiveFormat.createArchive(components, outputStream)
    outputStream.toByteArray
  }

  override def abort(): Unit = {
    logger.warn(s"Aborting writer for partition $partitionId")
    searchEngine.close()
  }

  override def close(): Unit = {
    searchEngine.close()
  }
}
