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
import com.tantivy4spark.search.{TantivySearchEngine, SplitSearchEngine}
import com.tantivy4spark.storage.{SplitCacheConfig, GlobalSplitCacheManager}
import com.tantivy4spark.util.StatisticsCalculator
import com.tantivy4spark.io.{CloudStorageProviderFactory, ProtocolBasedIOFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.io.{IOException, ByteArrayOutputStream}
import scala.jdk.CollectionConverters._

class Tantivy4SparkInputPartition(
    val addAction: AddAction,
    val readSchema: StructType,
    val filters: Array[Filter],
    val options: CaseInsensitiveStringMap,
    val partitionId: Int,
    val limit: Option[Int] = None
) extends InputPartition

class Tantivy4SparkReaderFactory(
    readSchema: StructType,
    options: CaseInsensitiveStringMap,
    limit: Option[Int] = None
) extends PartitionReaderFactory {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkReaderFactory])

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val tantivyPartition = partition.asInstanceOf[Tantivy4SparkInputPartition]
    logger.info(s"Creating reader for partition ${tantivyPartition.partitionId}")
    
    new Tantivy4SparkPartitionReader(
      tantivyPartition.addAction,
      readSchema,
      tantivyPartition.filters,
      options,
      tantivyPartition.limit.orElse(limit)
    )
  }
}

class Tantivy4SparkPartitionReader(
    addAction: AddAction,
    readSchema: StructType,
    filters: Array[Filter],
    options: CaseInsensitiveStringMap,
    limit: Option[Int] = None
) extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkPartitionReader])
  
  // Calculate effective limit: use pushed limit or fall back to Int.MaxValue
  private val effectiveLimit: Int = limit.getOrElse(Int.MaxValue)
  private val spark = SparkSession.active
  private val hadoopConf = spark.sparkContext.hadoopConfiguration
  
  private val filePath = new Path(addAction.path)
  
  private var splitSearchEngine: SplitSearchEngine = _
  private var resultIterator: Iterator[InternalRow] = Iterator.empty
  private var initialized = false

  
  private def createCacheConfig(): SplitCacheConfig = {
    SplitCacheConfig(
      cacheName = options.get("spark.tantivy4spark.cache.name", "tantivy4spark-cache"),
      maxCacheSize = options.getLong("spark.tantivy4spark.cache.maxSize", 200000000L),
      maxConcurrentLoads = options.getInt("spark.tantivy4spark.cache.maxConcurrentLoads", 8),
      enableQueryCache = options.getBoolean("spark.tantivy4spark.cache.queryCache", true),
      // AWS configuration
      awsAccessKey = Option(options.get("spark.tantivy4spark.aws.accessKey")),
      awsSecretKey = Option(options.get("spark.tantivy4spark.aws.secretKey")),
      awsSessionToken = Option(options.get("spark.tantivy4spark.aws.sessionToken")),
      awsRegion = Option(options.get("spark.tantivy4spark.aws.region")),
      awsEndpoint = Option(options.get("spark.tantivy4spark.aws.endpoint")),
      // Azure configuration
      azureAccountName = Option(options.get("spark.tantivy4spark.azure.accountName")),
      azureAccountKey = Option(options.get("spark.tantivy4spark.azure.accountKey")),
      azureConnectionString = Option(options.get("spark.tantivy4spark.azure.connectionString")),
      azureEndpoint = Option(options.get("spark.tantivy4spark.azure.endpoint")),
      // GCP configuration
      gcpProjectId = Option(options.get("spark.tantivy4spark.gcp.projectId")),
      gcpServiceAccountKey = Option(options.get("spark.tantivy4spark.gcp.serviceAccountKey")),
      gcpCredentialsFile = Option(options.get("spark.tantivy4spark.gcp.credentialsFile")),
      gcpEndpoint = Option(options.get("spark.tantivy4spark.gcp.endpoint"))
    )
  }

  private def initialize(): Unit = {
    if (!initialized) {
      try {
        logger.info(s"Reading Tantivy split: ${addAction.path}")
        
        // Create cache configuration from Spark options
        val cacheConfig = createCacheConfig()
        
        // Create split search engine using the split file directly
        splitSearchEngine = SplitSearchEngine.fromSplitFile(readSchema, addAction.path, cacheConfig)
        
        // Get the schema from the split to validate filters
        val splitSchema = splitSearchEngine.getSchema()
        val splitFieldNames = try {
          import scala.jdk.CollectionConverters._
          val fieldNames = splitSchema.getFieldNames().asScala.toSet
          logger.info(s"Split schema contains fields: ${fieldNames.mkString(", ")}")
          fieldNames
        } catch {
          case e: Exception =>
            logger.warn(s"Could not retrieve field names from split schema: ${e.getMessage}")
            Set.empty[String]
        }
        
        // Log the filters before conversion
        if (filters.nonEmpty) {
          logger.info(s"Applying ${filters.length} filters: ${filters.mkString(", ")}")
        }
        
        // Convert filters to Tantivy query with schema validation
        val query = if (splitFieldNames.nonEmpty) {
          val validatedQuery = FiltersToQueryConverter.convert(filters, Some(splitFieldNames))
          logger.info(s"Generated query after schema validation: '$validatedQuery'")
          validatedQuery
        } else {
          // Fall back to no schema validation if we can't get field names
          val fallbackQuery = FiltersToQueryConverter.convert(filters)
          logger.info(s"Generated query without schema validation: '$fallbackQuery'")
          fallbackQuery
        }
        
        if (query.nonEmpty) {
          val results = splitSearchEngine.search(query, limit = effectiveLimit)
          resultIterator = results.iterator
        } else {
          // No filters, return all documents
          val results = splitSearchEngine.searchAll(limit = effectiveLimit)
          resultIterator = results.iterator
        }
        
        initialized = true
        logger.info(s"Initialized split reader for ${addAction.path} with query: $query, limit: $effectiveLimit")
        
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
    if (splitSearchEngine != null) {
      splitSearchEngine.close()
    }
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
  
  // Debug: Log options and hadoop config available in executor
  println(s"🔧 Tantivy4SparkDataWriter executor options:")
  options.entrySet().asScala.foreach { entry =>
    val value = if (entry.getKey.contains("secretKey")) "***" else entry.getValue
    println(s"  ${entry.getKey} = $value")
  }
  println(s"🔧 Tantivy4SparkDataWriter hadoop config keys containing 'tantivy4spark':")
  hadoopConf.iterator().asScala.filter(_.getKey.contains("tantivy4spark")).foreach { entry =>
    val value = if (entry.getKey.contains("secretKey")) "***" else entry.getValue
    println(s"  ${entry.getKey} = $value")
  }
  
  private val searchEngine = new TantivySearchEngine(writeSchema, options, hadoopConf)
  private val statistics = new StatisticsCalculator.DatasetStatistics(writeSchema)
  private var recordCount = 0L

  override def write(record: InternalRow): Unit = {
    //println(s"Adding document $recordCount to Tantivy index")
    searchEngine.addDocument(record)
    statistics.updateRow(record)
    recordCount += 1
  }

  override def commit(): WriterCommitMessage = {
    logger.info(s"Committing Tantivy index with $recordCount documents")
    
    // Create split file name - change extension from .tnt4s to .split
    val fileName = f"part-$partitionId%05d-$taskId.split"
    val filePath = new Path(tablePath, fileName)
    val outputPath = filePath.toString
    
    // Generate node ID for the split (hostname + executor ID)
    val nodeId = java.net.InetAddress.getLocalHost.getHostName + "-" + 
                 Option(System.getProperty("spark.executor.id")).getOrElse("driver")
    
    // Create split from the index using the search engine
    val splitPath = searchEngine.commitAndCreateSplit(outputPath, partitionId.toLong, nodeId)
    
    // Get split file size using cloud storage provider
    val splitSize = {
      val cloudProvider = CloudStorageProviderFactory.createProvider(outputPath, options, hadoopConf)
      try {
        val fileInfo = cloudProvider.getFileInfo(outputPath)
        fileInfo.map(_.size).getOrElse {
          logger.warn(s"Could not get file info for $outputPath using cloud provider")
          0L
        }
      } finally {
        cloudProvider.close()
      }
    }
    
    logger.info(s"Created split file $fileName with $splitSize bytes, $recordCount records")
    
    val minValues = statistics.getMinValues
    val maxValues = statistics.getMaxValues
    
    val addAction = AddAction(
      path = splitPath,
      partitionValues = Map.empty,
      size = splitSize,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(recordCount),
      minValues = if (minValues.nonEmpty) Some(minValues) else None,
      maxValues = if (maxValues.nonEmpty) Some(maxValues) else None
    )
    
    logger.info(s"Committed writer for partition $partitionId with $recordCount records")
    Tantivy4SparkCommitMessage(addAction)
  }

  override def abort(): Unit = {
    logger.warn(s"Aborting writer for partition $partitionId")
    searchEngine.close()
  }

  override def close(): Unit = {
    searchEngine.close()
  }
}
