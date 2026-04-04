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

package io.indextables.spark.io

object ProtocolBasedIOFactory {

  // Supported storage protocols
  sealed trait StorageProtocol
  case object S3Protocol    extends StorageProtocol
  case object AzureProtocol extends StorageProtocol
  case object HDFSProtocol  extends StorageProtocol
  case object LocalProtocol extends StorageProtocol
  case object FileProtocol  extends StorageProtocol

  /** Determine storage protocol from path */
  def determineProtocol(path: String): StorageProtocol =
    if (path.startsWith("s3://") || path.startsWith("s3a://") || path.startsWith("s3n://")) {
      S3Protocol
    } else if (
      path.startsWith("azure://") ||
      path.startsWith("wasb://") || path.startsWith("wasbs://") ||
      path.startsWith("abfs://") || path.startsWith("abfss://")
    ) {
      AzureProtocol
    } else if (path.startsWith("hdfs://")) {
      HDFSProtocol
    } else if (path.startsWith("file://")) {
      FileProtocol
    } else {
      LocalProtocol // Default for relative paths, absolute paths without protocol
    }

  /** Get protocol name as string for logging */
  def protocolName(protocol: StorageProtocol): String = protocol match {
    case S3Protocol    => "s3"
    case AzureProtocol => "azure"
    case HDFSProtocol  => "hdfs"
    case FileProtocol  => "file"
    case LocalProtocol => "local"
  }
}
