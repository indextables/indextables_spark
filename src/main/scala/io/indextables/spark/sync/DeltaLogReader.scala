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

package io.indextables.spark.sync

import scala.jdk.CollectionConverters._

import io.indextables.tantivy4java.delta.{DeltaFileEntry, DeltaTableReader}

import org.slf4j.LoggerFactory

/** Simplified representation of a Delta AddFile action. */
case class DeltaAddFile(
  path: String,
  partitionValues: Map[String, String],
  size: Long)
    extends Serializable

/**
 * Wraps tantivy4java's DeltaTableReader (delta-kernel-rs) to provide a simple
 * interface for reading Delta transaction logs.
 *
 * Uses Rust-based delta-kernel-rs via JNI — no Hadoop dependency, no Java-side
 * credential chain issues. Native S3/Azure support with credentials passed directly.
 *
 * @param deltaTablePath
 *   Path to the Delta table root (containing `_delta_log/`)
 * @param sourceCredentials
 *   Credentials for accessing the Delta table's storage (spark.indextables.* keys).
 *   Translated to delta-kernel-rs credential keys (aws_access_key_id, etc.).
 */
class DeltaLogReader(deltaTablePath: String, sourceCredentials: Map[String, String]) {
  private val logger = LoggerFactory.getLogger(classOf[DeltaLogReader])

  private val deltaConfig: java.util.Map[String, String] =
    translateCredentials(sourceCredentials)

  // Lazily list files once and cache (used by currentVersion, getAllFiles, partitionColumns)
  private lazy val fileEntries: java.util.List[DeltaFileEntry] = {
    logger.info(s"Reading Delta table at $deltaTablePath via DeltaTableReader")
    DeltaTableReader.listFiles(deltaTablePath, deltaConfig)
  }

  /** Get the current (latest) version of the Delta table. */
  def currentVersion(): Long = {
    val entries = fileEntries
    val version = if (entries.isEmpty) -1L else entries.get(0).getTableVersion
    logger.info(s"Delta table at $deltaTablePath: current version = $version")
    version
  }

  /** Get all AddFile actions at the current snapshot (full file listing). */
  def getAllFiles(): Seq[DeltaAddFile] = {
    val entries = fileEntries
    logger.info(s"Delta snapshot contains ${entries.size} files")
    entries.asScala.toSeq.map { entry =>
      DeltaAddFile(
        path = entry.getPath,
        partitionValues = entry.getPartitionValues.asScala.toMap,
        size = entry.getSize
      )
    }
  }

  /** Get partition columns from Delta table file entries. */
  def partitionColumns(): Seq[String] = {
    val entries = fileEntries
    if (entries.isEmpty) Seq.empty
    else entries.get(0).getPartitionValues.keySet.asScala.toSeq.sorted
  }

  /**
   * Translate spark.indextables.* credential keys to the keys expected
   * by tantivy4java's DeltaTableReader (delta-kernel-rs).
   */
  private def translateCredentials(
    creds: Map[String, String]
  ): java.util.Map[String, String] = {
    val config = new java.util.HashMap[String, String]()
    // AWS
    creds.get("spark.indextables.aws.accessKey")
      .foreach(v => config.put("aws_access_key_id", v))
    creds.get("spark.indextables.aws.secretKey")
      .foreach(v => config.put("aws_secret_access_key", v))
    creds.get("spark.indextables.aws.sessionToken")
      .foreach(v => config.put("aws_session_token", v))
    creds.get("spark.indextables.aws.region")
      .foreach(v => config.put("aws_region", v))
    // Azure
    creds.get("spark.indextables.azure.accountName")
      .foreach(v => config.put("azure_account_name", v))
    creds.get("spark.indextables.azure.accountKey")
      .foreach(v => config.put("azure_access_key", v))
    config
  }
}
