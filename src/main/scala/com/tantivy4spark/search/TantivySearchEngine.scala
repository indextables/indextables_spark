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


package com.tantivy4spark.search

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

class TantivySearchEngine private (
  private val directInterface: TantivyDirectInterface,
  private val options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
  private val hadoopConf: Configuration = new Configuration()
) extends AutoCloseable {
  private val logger = LoggerFactory.getLogger(classOf[TantivySearchEngine])
  
  // Primary constructor for creating new search engines
  def this(schema: StructType) = this(new TantivyDirectInterface(schema))
  
  // Constructor with cloud storage support
  def this(schema: StructType, options: CaseInsensitiveStringMap, hadoopConf: Configuration) = 
    this(new TantivyDirectInterface(schema, None, options, hadoopConf), options, hadoopConf)

  def addDocument(row: InternalRow): Unit = {
    directInterface.addDocument(row)
  }

  def addDocuments(rows: Iterator[InternalRow]): Unit = {
    directInterface.addDocuments(rows)
  }

  def commit(): Unit = {
    directInterface.commit()
  }
  
  def commitAndCreateSplit(outputPath: String, partitionId: Long, nodeId: String): (String, com.tantivy4java.QuickwitSplit.SplitMetadata) = {
    // Use commitAndClose to follow write-only pattern for production
    directInterface.commitAndClose()
    
    try {
      // Delay cleanup of temporary directory until after split creation
      directInterface.delayCleanupForSplit()
      
      // Get temporary directory path from the direct interface
      val tempIndexPath = directInterface.getIndexPath()
      
      // Use SplitManager to create split from the temporary index with cloud storage support
      import com.tantivy4spark.storage.SplitManager
      val metadata = SplitManager.createSplit(tempIndexPath, outputPath, partitionId, nodeId, options, hadoopConf)
      
      // Return both the split file path and metadata with footer offsets
      (outputPath, metadata)
      
    } finally {
      // Force cleanup now that split creation is complete
      directInterface.forceCleanup()
    }
  }

  // Search methods removed - use SplitSearchEngine for reading from splits
  // The write-only architecture means TantivySearchEngine only creates indexes,
  // not reads from them

  override def close(): Unit = {
    directInterface.close()
  }
}

object TantivySearchEngine {
  private val logger = LoggerFactory.getLogger(TantivySearchEngine.getClass)
  
  /**
   * Creates a TantivySearchEngine from a split file.
   * This replaces the previous ZIP-based component system.
   */
  def fromSplitFile(schema: StructType, splitPath: String): SplitSearchEngine = {
    logger.info(s"Creating SplitSearchEngine from split file: $splitPath")
    
    // Try to extract configuration from active Spark session
    val cacheConfig = extractCacheConfigFromSparkSession()
    SplitSearchEngine.fromSplitFile(schema, splitPath, cacheConfig)
  }
  
  /**
   * Extract SplitCacheConfig from active Spark session when available.
   * Falls back to default config if no session is available.
   */
  private def extractCacheConfigFromSparkSession(): com.tantivy4spark.storage.SplitCacheConfig = {
    try {
      import org.apache.spark.sql.SparkSession
      val spark = SparkSession.getActiveSession
      
      spark match {
        case Some(session) =>
          val sparkConf = session.conf
          val hadoopConf = session.sparkContext.hadoopConfiguration
          
          // Helper function to get config with Hadoop fallback (same pattern as CloudStorageProviderFactory)
          def getConfigWithFallback(sparkKey: String): Option[String] = {
            val sparkValue = sparkConf.getOption(sparkKey)
            val hadoopValue = Option(hadoopConf.get(sparkKey))
            val result = sparkValue.orElse(hadoopValue)
            
            logger.info(s"🔍 Config fallback for $sparkKey: spark=${sparkValue.getOrElse("None")}, hadoop=${hadoopValue.getOrElse("None")}, final=${result.getOrElse("None")}")
            result
          }
          
          logger.debug(s"🔍 Extracting SplitCacheConfig from SparkSession (executor context: ${session.sparkContext.isLocal})")
          
          com.tantivy4spark.storage.SplitCacheConfig(
            cacheName = sparkConf.get("spark.tantivy4spark.cache.name", "tantivy4spark-cache"),
            maxCacheSize = sparkConf.get("spark.tantivy4spark.cache.maxSize", "200000000").toLong,
            maxConcurrentLoads = sparkConf.get("spark.tantivy4spark.cache.maxConcurrentLoads", "8").toInt,
            enableQueryCache = sparkConf.get("spark.tantivy4spark.cache.queryCache", "true").toBoolean,
            // AWS configuration with session token support - use Hadoop fallback for executor context
            awsAccessKey = getConfigWithFallback("spark.tantivy4spark.aws.accessKey"),
            awsSecretKey = getConfigWithFallback("spark.tantivy4spark.aws.secretKey"), 
            awsSessionToken = getConfigWithFallback("spark.tantivy4spark.aws.sessionToken"),
            awsRegion = getConfigWithFallback("spark.tantivy4spark.aws.region"),
            awsEndpoint = getConfigWithFallback("spark.tantivy4spark.s3.endpoint"),
            awsPathStyleAccess = getConfigWithFallback("spark.tantivy4spark.s3.pathStyleAccess").map(_.toLowerCase == "true"),
            // Azure configuration
            azureAccountName = getConfigWithFallback("spark.tantivy4spark.azure.accountName"),
            azureAccountKey = getConfigWithFallback("spark.tantivy4spark.azure.accountKey"),
            azureConnectionString = getConfigWithFallback("spark.tantivy4spark.azure.connectionString"),
            azureEndpoint = getConfigWithFallback("spark.tantivy4spark.azure.endpoint"),
            // GCP configuration
            gcpProjectId = getConfigWithFallback("spark.tantivy4spark.gcp.projectId"),
            gcpServiceAccountKey = getConfigWithFallback("spark.tantivy4spark.gcp.serviceAccountKey"),
            gcpCredentialsFile = getConfigWithFallback("spark.tantivy4spark.gcp.credentialsFile"),
            gcpEndpoint = getConfigWithFallback("spark.tantivy4spark.gcp.endpoint")
          )
        case None =>
          logger.debug("No active Spark session found, using default SplitCacheConfig")
          com.tantivy4spark.storage.SplitCacheConfig()
      }
    } catch {
      case ex: Exception =>
        logger.warn("Failed to extract cache config from Spark session, using defaults", ex)
        com.tantivy4spark.storage.SplitCacheConfig()
    }
  }
  
  /**
   * Creates a TantivySearchEngine from a split file with custom cache configuration.
   */
  def fromSplitFileWithCache(schema: StructType, splitPath: String, cacheConfig: com.tantivy4spark.storage.SplitCacheConfig): SplitSearchEngine = {
    logger.info(s"Creating SplitSearchEngine from split file with cache: $splitPath")
    
    SplitSearchEngine.fromSplitFile(schema, splitPath, cacheConfig)
  }
  
  /**
   * Creates a TantivySearchEngine from a direct interface (for write operations).
   */
  def fromDirectInterface(directInterface: TantivyDirectInterface): TantivySearchEngine = {
    new TantivySearchEngine(directInterface)
  }
}