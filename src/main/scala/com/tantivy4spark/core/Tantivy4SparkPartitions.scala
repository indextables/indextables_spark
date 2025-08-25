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
import com.tantivy4spark.storage.{SplitCacheConfig, GlobalSplitCacheManager, SplitLocationRegistry}
import com.tantivy4spark.util.StatisticsCalculator
import com.tantivy4spark.io.{CloudStorageProviderFactory, ProtocolBasedIOFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast
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
) extends InputPartition {
  
  /**
   * Provide preferred locations for this partition based on split cache locality.
   * Spark will try to schedule tasks on these hosts to take advantage of cached splits.
   */
  override def preferredLocations(): Array[String] = {
    val preferredHosts = SplitLocationRegistry.getPreferredHosts(addAction.path)
    if (preferredHosts.nonEmpty) {
      preferredHosts
    } else {
      // No cache history available, let Spark decide
      Array.empty[String]
    }
  }
}

class Tantivy4SparkReaderFactory(
    readSchema: StructType,
    options: CaseInsensitiveStringMap,
    limit: Option[Int] = None,
    broadcastConfig: Broadcast[Map[String, String]]
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
      tantivyPartition.limit.orElse(limit),
      broadcastConfig
    )
  }
}

class Tantivy4SparkPartitionReader(
    addAction: AddAction,
    readSchema: StructType,
    filters: Array[Filter],
    options: CaseInsensitiveStringMap,
    limit: Option[Int] = None,
    broadcastConfig: Broadcast[Map[String, String]]
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
    // Access the broadcast configuration in executor
    val broadcasted = broadcastConfig.value
    
    // Helper function to get config with broadcast fallback
    def getConfigWithBroadcast(configKey: String): Option[String] = {
      val optionsValue = Option(options.get(configKey))
      val broadcastValue = broadcasted.get(configKey)
      val result = optionsValue.orElse(broadcastValue)
      
      logger.info(s"🔍 PartitionReader broadcast config for $configKey: options=${optionsValue.getOrElse("None")}, broadcast=${broadcastValue.getOrElse("None")}, final=${result.getOrElse("None")}")
      result
    }
    
    SplitCacheConfig(
      cacheName = options.get("spark.tantivy4spark.cache.name", "tantivy4spark-cache"),
      maxCacheSize = options.getLong("spark.tantivy4spark.cache.maxSize", 200000000L),
      maxConcurrentLoads = options.getInt("spark.tantivy4spark.cache.maxConcurrentLoads", 8),
      enableQueryCache = options.getBoolean("spark.tantivy4spark.cache.queryCache", true),
      // AWS configuration with broadcast fallback
      awsAccessKey = getConfigWithBroadcast("spark.tantivy4spark.aws.accessKey"),
      awsSecretKey = getConfigWithBroadcast("spark.tantivy4spark.aws.secretKey"),
      awsSessionToken = getConfigWithBroadcast("spark.tantivy4spark.aws.sessionToken"),
      awsRegion = getConfigWithBroadcast("spark.tantivy4spark.aws.region"),
      awsEndpoint = getConfigWithBroadcast("spark.tantivy4spark.s3.endpoint"),
      // Azure configuration with broadcast fallback
      azureAccountName = getConfigWithBroadcast("spark.tantivy4spark.azure.accountName"),
      azureAccountKey = getConfigWithBroadcast("spark.tantivy4spark.azure.accountKey"),
      azureConnectionString = getConfigWithBroadcast("spark.tantivy4spark.azure.connectionString"),
      azureEndpoint = getConfigWithBroadcast("spark.tantivy4spark.azure.endpoint"),
      // GCP configuration with broadcast fallback
      gcpProjectId = getConfigWithBroadcast("spark.tantivy4spark.gcp.projectId"),
      gcpServiceAccountKey = getConfigWithBroadcast("spark.tantivy4spark.gcp.serviceAccountKey"),
      gcpCredentialsFile = getConfigWithBroadcast("spark.tantivy4spark.gcp.credentialsFile"),
      gcpEndpoint = getConfigWithBroadcast("spark.tantivy4spark.gcp.endpoint")
    )
  }

  private def initialize(): Unit = {
    if (!initialized) {
      try {
        logger.info(s"Reading Tantivy split: ${addAction.path}")
        
        // Record that this host has accessed this split for future scheduling locality
        val currentHostname = SplitLocationRegistry.getCurrentHostname
        SplitLocationRegistry.recordSplitAccess(addAction.path, currentHostname)
        logger.debug(s"Recorded split access for locality: ${addAction.path} on host $currentHostname")
        
        // Create cache configuration from Spark options
        val cacheConfig = createCacheConfig()
        
        // Create split search engine using the split file directly
        // Normalize path for tantivy4java compatibility (s3a:// -> s3://)
        val normalizedPath = if (addAction.path.startsWith("s3a://") || addAction.path.startsWith("s3n://")) {
          addAction.path.replaceFirst("^s3[an]://", "s3://")
        } else {
          addAction.path
        }
        splitSearchEngine = SplitSearchEngine.fromSplitFile(readSchema, normalizedPath, cacheConfig)
        
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
        
        // Log the filters and limit for debugging
        logger.info(s"Pushdown configuration for ${addAction.path}:")
        logger.info(s"  - Filters: ${filters.length} filter(s) - ${filters.mkString(", ")}")
        logger.info(s"  - Limit: $effectiveLimit")
        
        // Convert filters to Tantivy Query object with schema validation
        val query = if (filters.nonEmpty) {
          val queryObj = if (splitFieldNames.nonEmpty) {
            val validatedQuery = FiltersToQueryConverter.convertToQuery(filters, splitSchema, Some(splitFieldNames))
            logger.info(s"  - Query (with schema validation): ${validatedQuery.getClass.getSimpleName}")
            validatedQuery
          } else {
            // Fall back to no schema validation if we can't get field names
            val fallbackQuery = FiltersToQueryConverter.convertToQuery(filters, splitSchema)
            logger.info(s"  - Query (no schema validation): ${fallbackQuery.getClass.getSimpleName}")
            fallbackQuery
          }
          queryObj
        } else {
          null // Use null to indicate no filters
        }
        
        // Push down query and limit to Quickwit searcher
        val results = if (query != null) {
          logger.info(s"Executing search with Query object and limit: $effectiveLimit")
          val searchResults = splitSearchEngine.search(query, limit = effectiveLimit)
          logger.info(s"Search returned ${searchResults.length} results (pushed limit: $effectiveLimit)")
          searchResults
        } else {
          // No filters, but still push down the limit
          logger.info(s"No query filters, executing searchAll with pushed limit: $effectiveLimit")
          val allResults = splitSearchEngine.searchAll(limit = effectiveLimit)
          logger.info(s"SearchAll returned ${allResults.length} results (pushed limit: $effectiveLimit)")
          allResults
        }
        
        resultIterator = results.iterator
        initialized = true
        logger.info(s"Pushdown complete for ${addAction.path}: query='$query', limit=$effectiveLimit, results=${results.length}")
        
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

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkDataWriter])
  
  // Debug: Log options and hadoop config available in executor
  if (logger.isDebugEnabled) {
    logger.debug("Tantivy4SparkDataWriter executor options:")
    options.entrySet().asScala.foreach { entry =>
      val value = if (entry.getKey.contains("secretKey")) "***" else entry.getValue
      logger.debug(s"  ${entry.getKey} = $value")
    }
    logger.debug("Tantivy4SparkDataWriter hadoop config keys containing 'tantivy4spark':")
    hadoopConf.iterator().asScala.filter(_.getKey.contains("tantivy4spark")).foreach { entry =>
      val value = if (entry.getKey.contains("secretKey")) "***" else entry.getValue
      logger.debug(s"  ${entry.getKey} = $value")
    }
  }
  
  private val searchEngine = new TantivySearchEngine(writeSchema, options, hadoopConf)
  private val statistics = new StatisticsCalculator.DatasetStatistics(writeSchema)
  private var recordCount = 0L

  override def write(record: InternalRow): Unit = {
    if (recordCount < 5) { // Log first 5 records for debugging
      logger.warn(s"Writing record ${recordCount + 1}: ${record}")
    }
    searchEngine.addDocument(record)
    statistics.updateRow(record)
    recordCount += 1
  }

  override def commit(): WriterCommitMessage = {
    logger.warn(s"Committing Tantivy index with $recordCount documents")
    
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
    
    // Normalize the splitPath for tantivy4java compatibility (convert s3a:// to s3://)
    val normalizedSplitPath = {
      val cloudProvider = CloudStorageProviderFactory.createProvider(outputPath, options, hadoopConf)
      try {
        cloudProvider.normalizePathForTantivy(splitPath)
      } finally {
        cloudProvider.close()
      }
    }
    
    logger.info(s"Created split file $fileName with $splitSize bytes, $recordCount records")
    
    val minValues = statistics.getMinValues
    val maxValues = statistics.getMaxValues
    
    val addAction = AddAction(
      path = normalizedSplitPath,
      partitionValues = Map.empty,
      size = splitSize,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(recordCount),
      minValues = if (minValues.nonEmpty) Some(minValues) else None,
      maxValues = if (maxValues.nonEmpty) Some(maxValues) else None
    )
    
    logger.info(s"📝 AddAction created with path: ${addAction.path}")
    
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
