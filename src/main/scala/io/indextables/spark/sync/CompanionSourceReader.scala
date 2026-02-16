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

import org.apache.spark.sql.types.StructType

/**
 * A source file from a companion source (Delta, Parquet directory, Iceberg).
 */
case class CompanionSourceFile(
  path: String,
  partitionValues: Map[String, String],
  size: Long)
    extends Serializable

/**
 * Abstraction for reading source table metadata in BUILD COMPANION operations.
 * Implementations exist for Delta, bare Parquet directories, and Iceberg tables.
 */
trait CompanionSourceReader {

  /** Source version: Delta version, Iceberg snapshot ID, or None for unversioned sources. */
  def sourceVersion(): Option[Long]

  /** All data files in the current snapshot. */
  def getAllFiles(): Seq[CompanionSourceFile]

  /** Partition column names (sorted). */
  def partitionColumns(): Seq[String]

  /** Spark schema of the source table. */
  def schema(): StructType

  /**
   * Path to a single parquet file suitable for schema extraction by tantivy4java.
   * Used when creating ParquetCompanionConfig on executors.
   * Returns None if the source provides schema via other means (e.g., Iceberg catalog).
   */
  def schemaSourceParquetFile(): Option[String]

  /**
   * Physical-to-logical column name mapping for sources where parquet files use
   * physical column names that differ from logical schema names.
   *
   * Examples:
   *   - Iceberg via Databricks: col_1 -> id, col_2 -> name
   *   - Delta with columnMapping.mode=name: col-abc123 -> id
   *
   * Returns empty map if physical names equal logical names (identity mapping).
   */
  def columnNameMapping(): Map[String, String] = Map.empty

  /**
   * The root storage path for resolving relative file paths returned by getAllFiles().
   * For Iceberg, this is the actual S3/Azure base path (e.g., "s3://bucket/warehouse/db/table/data").
   * For Delta/Parquet, file paths are relative to sourcePath so this returns None.
   *
   * When present, relative paths from getAllFiles() are resolved against this root
   * for downloads, while the anti-join uses the relative paths directly for
   * bucket-independent comparison (supports cross-region failover).
   */
  def storageRoot(): Option[String] = None

  /** Release any resources held by this reader. */
  def close(): Unit = {}
}
