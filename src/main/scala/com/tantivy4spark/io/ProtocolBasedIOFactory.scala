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

package com.tantivy4spark.io

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import com.tantivy4spark.storage.DataLocation

object ProtocolBasedIOFactory {
  
  // Supported storage protocols
  sealed trait StorageProtocol
  case object S3Protocol extends StorageProtocol
  case object HDFSProtocol extends StorageProtocol 
  case object LocalProtocol extends StorageProtocol
  case object FileProtocol extends StorageProtocol
  
  /**
   * Determine storage protocol from path
   */
  def determineProtocol(path: String): StorageProtocol = {
    if (path.startsWith("s3://") || path.startsWith("s3a://") || path.startsWith("s3n://")) {
      S3Protocol
    } else if (path.startsWith("hdfs://")) {
      HDFSProtocol
    } else if (path.startsWith("file://")) {
      FileProtocol
    } else {
      LocalProtocol  // Default for relative paths, absolute paths without protocol
    }
  }
  
  /**
   * Get protocol name as string for logging
   */
  def protocolName(protocol: StorageProtocol): String = protocol match {
    case S3Protocol => "s3"
    case HDFSProtocol => "hdfs"
    case FileProtocol => "file"
    case LocalProtocol => "local"
  }
  
  /**
   * Check if protocol supports advanced optimizations
   */
  def supportsAdvancedOptimizations(protocol: StorageProtocol): Boolean = protocol match {
    case S3Protocol => true    // S3 multipart uploads, transfer acceleration, etc.
    case HDFSProtocol => true  // Block placement, data locality, erasure coding, etc.
    case FileProtocol => false // Standard file I/O
    case LocalProtocol => false // Standard file I/O
  }
  
  /**
   * Get recommended buffer size for protocol
   */
  def getOptimalBufferSize(protocol: StorageProtocol): Int = protocol match {
    case S3Protocol => 16 * 1024 * 1024    // 16MB - optimal for S3 multipart uploads
    case HDFSProtocol => 128 * 1024 * 1024  // 128MB - HDFS block size alignment
    case FileProtocol => 4 * 1024 * 1024    // 4MB - balance memory and I/O efficiency
    case LocalProtocol => 1024 * 1024       // 1MB - conservative for local disk
  }
  
  /**
   * Get compression recommendation for protocol
   */
  def getCompressionRecommendation(protocol: StorageProtocol): Option[String] = protocol match {
    case S3Protocol => Some("gzip")      // Good compression ratio for network transfer
    case HDFSProtocol => Some("snappy")  // Fast compression/decompression for big data
    case FileProtocol => None            // Let filesystem handle compression
    case LocalProtocol => None           // No compression for local development/testing
  }
  
  /**
   * Create protocol-specific configuration
   */
  def createProtocolConfig(protocol: StorageProtocol, options: Map[String, String]): Map[String, String] = {
    val baseConfig = Map(
      "buffer.size" -> getOptimalBufferSize(protocol).toString,
      "protocol" -> protocolName(protocol),
      "advanced.optimizations.enabled" -> supportsAdvancedOptimizations(protocol).toString
    )
    
    val compressionConfig = getCompressionRecommendation(protocol) match {
      case Some(codec) => Map("compression.codec" -> codec)
      case None => Map.empty[String, String]
    }
    
    // Protocol-specific configurations
    val protocolSpecificConfig = protocol match {
      case S3Protocol => Map(
        "s3.multipart.threshold" -> options.getOrElse("s3.multipart.threshold", (100 * 1024 * 1024).toString),
        "s3.multipart.size" -> options.getOrElse("s3.multipart.size", (16 * 1024 * 1024).toString),
        "s3.max.connections" -> options.getOrElse("s3.max.connections", "50"),
        "s3.connection.timeout" -> options.getOrElse("s3.connection.timeout", "10000")
      )
      case HDFSProtocol => Map(
        "dfs.blocksize" -> options.getOrElse("dfs.blocksize", (128 * 1024 * 1024).toString),
        "dfs.replication" -> options.getOrElse("dfs.replication", "3"),
        "dfs.client.read.shortcircuit" -> "true",
        "dfs.client.use.legacy.blockreader.local" -> "false"
      )
      case _ => Map.empty[String, String]
    }
    
    baseConfig ++ compressionConfig ++ protocolSpecificConfig
  }
}