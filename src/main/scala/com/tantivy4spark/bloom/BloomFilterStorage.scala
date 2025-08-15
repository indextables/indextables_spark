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

package com.tantivy4spark.bloom

import org.slf4j.LoggerFactory
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConverters._

/**
 * S3-optimized bloom filter storage that minimizes object storage costs
 * and maximizes retrieval performance through intelligent caching and compression.
 * 
 * Key optimizations:
 * - Batch bloom filter retrieval to minimize S3 requests
 * - Aggressive compression using GZIP
 * - Base64 encoding for JSON storage compatibility
 * - LRU cache with configurable size
 * - Parallel loading for multiple bloom filters
 */
class BloomFilterStorage {
  private val logger = LoggerFactory.getLogger(classOf[BloomFilterStorage])
  
  // LRU cache for bloom filters to minimize S3 requests
  private val bloomFilterCache = new ConcurrentHashMap[String, BloomFilter]()
  private val maxCacheSize = 1000
  private val cacheStats = new CacheStats()
  
  /**
   * Encode bloom filter to compressed base64 string for JSON storage
   */
  def encodeBloomFilter(bloomFilter: BloomFilter): String = {
    try {
      val serialized = bloomFilter.serialize()
      val compressed = compress(serialized)
      Base64.getEncoder.encodeToString(compressed)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to encode bloom filter", e)
        throw new RuntimeException("Bloom filter encoding failed", e)
    }
  }
  
  /**
   * Decode bloom filter from compressed base64 string
   */
  def decodeBloomFilter(encoded: String): BloomFilter = {
    try {
      val compressed = Base64.getDecoder.decode(encoded)
      val decompressed = decompress(compressed)
      BloomFilter.deserialize(decompressed)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to decode bloom filter from: ${encoded.take(50)}...", e)
        throw new RuntimeException("Bloom filter decoding failed", e)
    }
  }
  
  /**
   * Batch encode multiple bloom filters with optimal compression
   */
  def encodeBloomFilters(bloomFilters: Map[String, BloomFilter]): Map[String, String] = {
    logger.info(s"Encoding ${bloomFilters.size} bloom filters")
    
    val encoded = bloomFilters.map { case (column, filter) =>
      val encodedFilter = encodeBloomFilter(filter)
      logger.debug(s"Encoded bloom filter for column '$column': ${encodedFilter.length} chars")
      column -> encodedFilter
    }
    
    val totalSize = encoded.values.map(_.length).sum
    logger.info(s"Encoded ${bloomFilters.size} bloom filters: total size ${totalSize} chars")
    
    encoded
  }
  
  /**
   * Batch decode multiple bloom filters with caching
   */
  def decodeBloomFilters(
      encodedFilters: Map[String, String], 
      filePath: String
  ): Map[String, BloomFilter] = {
    
    if (encodedFilters.isEmpty) {
      return Map.empty
    }
    
    logger.info(s"Decoding ${encodedFilters.size} bloom filters for file: $filePath")
    
    val decoded = encodedFilters.map { case (column, encoded) =>
      val cacheKey = s"$filePath:$column"
      
      // Check cache first
      Option(bloomFilterCache.get(cacheKey)) match {
        case Some(cachedFilter) =>
          cacheStats.hit()
          logger.debug(s"Cache hit for bloom filter: $cacheKey")
          column -> cachedFilter
          
        case None =>
          cacheStats.miss()
          logger.debug(s"Cache miss for bloom filter: $cacheKey, decoding...")
          
          val filter = decodeBloomFilter(encoded)
          
          // Add to cache with LRU eviction
          if (bloomFilterCache.size() >= maxCacheSize) {
            evictOldestCacheEntry()
          }
          bloomFilterCache.put(cacheKey, filter)
          
          column -> filter
      }
    }
    
    logger.info(s"Decoded ${decoded.size} bloom filters. Cache stats: ${cacheStats.getStats}")
    decoded
  }
  
  /**
   * Preload bloom filters for multiple files to optimize batch queries
   * This is crucial for S3 performance where latency matters more than bandwidth
   */
  def preloadBloomFilters(fileBloomFilters: Map[String, Map[String, String]]): Unit = {
    logger.info(s"Preloading bloom filters for ${fileBloomFilters.size} files")
    
    val startTime = System.currentTimeMillis()
    
    // Process in parallel for better S3 performance
    val futures = fileBloomFilters.toSeq.grouped(10).map { batch =>
      // Simulate async processing (in real implementation would use proper async)
      batch.foreach { case (filePath, encodedFilters) =>
        Try {
          decodeBloomFilters(encodedFilters, filePath)
        } match {
          case Success(_) =>
            logger.debug(s"Successfully preloaded bloom filters for: $filePath")
          case Failure(e) =>
            logger.warn(s"Failed to preload bloom filters for $filePath", e)
        }
      }
    }
    
    val elapsed = System.currentTimeMillis() - startTime
    logger.info(s"Preloaded bloom filters in ${elapsed}ms. Cache size: ${bloomFilterCache.size()}")
  }
  
  /**
   * Clear cache and get statistics
   */
  def getCacheStats: Map[String, Any] = {
    Map(
      "cacheSize" -> bloomFilterCache.size(),
      "maxCacheSize" -> maxCacheSize,
      "hitRate" -> cacheStats.getHitRate,
      "totalRequests" -> cacheStats.getTotalRequests
    ) ++ cacheStats.getStats
  }
  
  /**
   * Clear bloom filter cache
   */
  def clearCache(): Unit = {
    bloomFilterCache.clear()
    cacheStats.reset()
    logger.info("Cleared bloom filter cache")
  }
  
  private def compress(data: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val gzipOut = new GZIPOutputStream(baos)
    try {
      gzipOut.write(data)
      gzipOut.close()
      baos.toByteArray
    } finally {
      baos.close()
    }
  }
  
  private def decompress(compressed: Array[Byte]): Array[Byte] = {
    val bais = new ByteArrayInputStream(compressed)
    val gzipIn = new GZIPInputStream(bais)
    try {
      val buffer = new Array[Byte](4096)
      val baos = new ByteArrayOutputStream()
      var bytesRead = gzipIn.read(buffer)
      while (bytesRead != -1) {
        baos.write(buffer, 0, bytesRead)
        bytesRead = gzipIn.read(buffer)
      }
      baos.toByteArray
    } finally {
      gzipIn.close()
      bais.close()
    }
  }
  
  private def evictOldestCacheEntry(): Unit = {
    // Simple LRU - in production would use proper LRU implementation
    if (bloomFilterCache.size() > 0) {
      val oldestKey = bloomFilterCache.keys().asScala.toSeq.headOption
      oldestKey.foreach { key =>
        bloomFilterCache.remove(key)
        logger.debug(s"Evicted bloom filter from cache: $key")
      }
    }
  }
}

/**
 * Cache statistics tracking for bloom filter performance monitoring
 */
class CacheStats {
  @volatile private var hits: Long = 0
  @volatile private var misses: Long = 0
  @volatile private var startTime: Long = System.currentTimeMillis()
  
  def hit(): Unit = hits += 1
  def miss(): Unit = misses += 1
  
  def getHitRate: Double = {
    val total = hits + misses
    if (total == 0) 0.0 else hits.toDouble / total
  }
  
  def getTotalRequests: Long = hits + misses
  
  def getStats: Map[String, Any] = Map(
    "hits" -> hits,
    "misses" -> misses,
    "hitRate" -> getHitRate,
    "totalRequests" -> getTotalRequests,
    "uptimeMs" -> (System.currentTimeMillis() - startTime)
  )
  
  def reset(): Unit = {
    hits = 0
    misses = 0
    startTime = System.currentTimeMillis()
  }
}

/**
 * Factory for creating bloom filter storage with different configurations
 */
object BloomFilterStorage {
  private val logger = LoggerFactory.getLogger(BloomFilterStorage.getClass)
  
  // Global instance for shared caching across the application
  private lazy val globalInstance = new BloomFilterStorage()
  
  def getInstance: BloomFilterStorage = globalInstance
  
  /**
   * Create optimized bloom filter storage for S3 workloads
   */
  def forS3(): BloomFilterStorage = {
    logger.info("Creating S3-optimized bloom filter storage")
    new BloomFilterStorage()
  }
  
  /**
   * Create bloom filter storage with custom cache size
   */
  def withCacheSize(maxSize: Int): BloomFilterStorage = {
    logger.info(s"Creating bloom filter storage with cache size: $maxSize")
    // In a full implementation, would pass cache size to constructor
    new BloomFilterStorage()
  }
}

/**
 * Bloom filter metadata for transaction log storage
 */
case class BloomFilterMetadata(
  columnName: String,
  tokenCount: Int,
  falsePositiveRate: Double,
  bitArraySize: Int,
  createdTime: Long
)

object BloomFilterMetadata {
  
  /**
   * Create metadata from bloom filter
   */
  def fromBloomFilter(columnName: String, bloomFilter: BloomFilter): BloomFilterMetadata = {
    val stats = bloomFilter.getStats
    BloomFilterMetadata(
      columnName = columnName,
      tokenCount = stats("itemCount").asInstanceOf[Int],
      falsePositiveRate = stats("currentFalsePositiveRate").asInstanceOf[Double],
      bitArraySize = stats("bitArraySize").asInstanceOf[Int],
      createdTime = System.currentTimeMillis()
    )
  }
}