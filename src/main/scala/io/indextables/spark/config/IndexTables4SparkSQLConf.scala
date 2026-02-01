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

package io.indextables.spark.config

/** SQL configuration settings for IndexTables4Spark. */
object IndexTables4SparkSQLConf {

  /////////////////////
  // AWS Configuration
  /////////////////////

  val TANTIVY4SPARK_AWS_ACCESS_KEY    = "spark.indextables.aws.accessKey"
  val TANTIVY4SPARK_AWS_SECRET_KEY    = "spark.indextables.aws.secretKey"
  val TANTIVY4SPARK_AWS_SESSION_TOKEN = "spark.indextables.aws.sessionToken"
  val TANTIVY4SPARK_AWS_REGION        = "spark.indextables.aws.region"
  val TANTIVY4SPARK_S3_ENDPOINT       = "spark.indextables.s3.endpoint"

  /////////////////////
  // S3 Upload Configuration
  /////////////////////

  val TANTIVY4SPARK_S3_STREAMING_THRESHOLD = "spark.indextables.s3.streamingThreshold"
  val TANTIVY4SPARK_S3_MULTIPART_THRESHOLD = "spark.indextables.s3.multipartThreshold"
  val TANTIVY4SPARK_S3_MAX_CONCURRENCY     = "spark.indextables.s3.maxConcurrency"
  val TANTIVY4SPARK_S3_PART_SIZE           = "spark.indextables.s3.partSize"
  val TANTIVY4SPARK_S3_MAX_QUEUE_SIZE      = "spark.indextables.s3.maxQueueSize"

  /////////////////////
  // Working Directory Configuration
  /////////////////////

  val TANTIVY4SPARK_MERGE_TEMP_DIRECTORY_PATH        = "spark.indextables.merge.tempDirectoryPath"
  val TANTIVY4SPARK_INDEX_WRITER_TEMP_DIRECTORY_PATH = "spark.indextables.indexWriter.tempDirectoryPath"
  val TANTIVY4SPARK_CACHE_DIRECTORY_PATH             = "spark.indextables.cache.directoryPath"

  /////////////////////
  // Index Writer Configuration
  /////////////////////

  val TANTIVY4SPARK_INDEX_WRITER_HEAP_SIZE             = "spark.indextables.indexWriter.heapSize"
  val TANTIVY4SPARK_INDEX_WRITER_THREADS               = "spark.indextables.indexWriter.threads"
  val TANTIVY4SPARK_INDEX_WRITER_BATCH_SIZE            = "spark.indextables.indexWriter.batchSize"
  val TANTIVY4SPARK_INDEX_WRITER_MAX_BATCH_BUFFER_SIZE = "spark.indextables.indexWriter.maxBatchBufferSize"
  val TANTIVY4SPARK_INDEX_WRITER_USE_BATCH             = "spark.indextables.indexWriter.useBatch"
  val TANTIVY4SPARK_SPLIT_CONVERSION_MAX_PARALLELISM   = "spark.indextables.splitConversion.maxParallelism"

  /////////////////////
  // General Configuration
  /////////////////////

  val TANTIVY4SPARK_STORAGE_FORCE_STANDARD = "spark.indextables.storage.force.standard"
  val TANTIVY4SPARK_BLOOM_FILTERS_ENABLED  = "spark.indextables.bloom.filters.enabled"

  /////////////////////
  // Skipped Files Configuration
  /////////////////////

  val TANTIVY4SPARK_SKIPPED_FILES_COOLDOWN_DURATION = "spark.indextables.skippedFiles.cooldownDuration"
  val TANTIVY4SPARK_SKIPPED_FILES_TRACKING_ENABLED  = "spark.indextables.skippedFiles.trackingEnabled"

  /////////////////////
  // Merge Splits Configuration
  /////////////////////

  /** Maximum number of source splits that can be merged in a single merge operation (default: 1000) */
  val TANTIVY4SPARK_MERGE_MAX_SOURCE_SPLITS_PER_MERGE         = "spark.indextables.merge.maxSourceSplitsPerMerge"
  val TANTIVY4SPARK_MERGE_MAX_SOURCE_SPLITS_PER_MERGE_DEFAULT = 1000

  /////////////////////
  // Async Merge-On-Write Configuration
  /////////////////////

  /** Enable merge-on-write feature (default: false) */
  val TANTIVY4SPARK_MERGE_ON_WRITE_ENABLED         = "spark.indextables.mergeOnWrite.enabled"
  val TANTIVY4SPARK_MERGE_ON_WRITE_ENABLED_DEFAULT = false

  /** Enable asynchronous merge execution in background thread (default: true) */
  val TANTIVY4SPARK_MERGE_ON_WRITE_ASYNC_ENABLED         = "spark.indextables.mergeOnWrite.async.enabled"
  val TANTIVY4SPARK_MERGE_ON_WRITE_ASYNC_ENABLED_DEFAULT = true

  /** Fraction of cluster CPUs to use per merge batch (default: 0.167 = 1/6) */
  val TANTIVY4SPARK_MERGE_ON_WRITE_BATCH_CPU_FRACTION         = "spark.indextables.mergeOnWrite.batchCpuFraction"
  val TANTIVY4SPARK_MERGE_ON_WRITE_BATCH_CPU_FRACTION_DEFAULT = 0.167

  /** Maximum number of merge batches running simultaneously (default: 3) */
  val TANTIVY4SPARK_MERGE_ON_WRITE_MAX_CONCURRENT_BATCHES = "spark.indextables.mergeOnWrite.maxConcurrentBatches"
  val TANTIVY4SPARK_MERGE_ON_WRITE_MAX_CONCURRENT_BATCHES_DEFAULT = 3

  /** Minimum batches worth of merge groups required to trigger merge (default: 1) */
  val TANTIVY4SPARK_MERGE_ON_WRITE_MIN_BATCHES_TO_TRIGGER         = "spark.indextables.mergeOnWrite.minBatchesToTrigger"
  val TANTIVY4SPARK_MERGE_ON_WRITE_MIN_BATCHES_TO_TRIGGER_DEFAULT = 1

  /** Target size for merged splits (default: 4G) */
  val TANTIVY4SPARK_MERGE_ON_WRITE_TARGET_SIZE         = "spark.indextables.mergeOnWrite.targetSize"
  val TANTIVY4SPARK_MERGE_ON_WRITE_TARGET_SIZE_DEFAULT = "4G"

  /** Graceful shutdown timeout in milliseconds (default: 300000 = 5 minutes) */
  val TANTIVY4SPARK_MERGE_ON_WRITE_SHUTDOWN_TIMEOUT_MS         = "spark.indextables.mergeOnWrite.shutdownTimeoutMs"
  val TANTIVY4SPARK_MERGE_ON_WRITE_SHUTDOWN_TIMEOUT_MS_DEFAULT = 300000L

  /////////////////////
  // Merge Download Configuration (cloud storage I/O for merge operations)
  /////////////////////

  /** Maximum concurrent downloads per CPU core (default: 8, e.g., 64 on 8-core machine) */
  val TANTIVY4SPARK_MERGE_DOWNLOAD_MAX_CONCURRENCY_PER_CORE = "spark.indextables.merge.download.maxConcurrencyPerCore"
  val TANTIVY4SPARK_MERGE_DOWNLOAD_MAX_CONCURRENCY_PER_CORE_DEFAULT = 8

  /** Memory budget for download operations (default: 2GB) */
  val TANTIVY4SPARK_MERGE_DOWNLOAD_MEMORY_BUDGET         = "spark.indextables.merge.download.memoryBudget"
  val TANTIVY4SPARK_MERGE_DOWNLOAD_MEMORY_BUDGET_DEFAULT = "2G"

  /** Number of retry attempts for failed downloads (default: 3) */
  val TANTIVY4SPARK_MERGE_DOWNLOAD_RETRIES         = "spark.indextables.merge.download.retries"
  val TANTIVY4SPARK_MERGE_DOWNLOAD_RETRIES_DEFAULT = 3

  /** Maximum concurrent upload threads for merged splits (default: 6) */
  val TANTIVY4SPARK_MERGE_UPLOAD_MAX_CONCURRENCY         = "spark.indextables.merge.upload.maxConcurrency"
  val TANTIVY4SPARK_MERGE_UPLOAD_MAX_CONCURRENCY_DEFAULT = 6

  /** Base delay in milliseconds for exponential backoff on retries (default: 1000) */
  val TANTIVY4SPARK_MERGE_DOWNLOAD_RETRY_BASE_DELAY_MS         = "spark.indextables.merge.download.retryBaseDelayMs"
  val TANTIVY4SPARK_MERGE_DOWNLOAD_RETRY_BASE_DELAY_MS_DEFAULT = 1000L

  /** Maximum delay in milliseconds for exponential backoff on retries (default: 30000) */
  val TANTIVY4SPARK_MERGE_DOWNLOAD_RETRY_MAX_DELAY_MS         = "spark.indextables.merge.download.retryMaxDelayMs"
  val TANTIVY4SPARK_MERGE_DOWNLOAD_RETRY_MAX_DELAY_MS_DEFAULT = 30000L

  /////////////////////
  // Schema Deduplication Configuration
  /////////////////////

  /**
   * Filter out object fields with empty field_mappings from docMappingJson during schema restoration. This fixes
   * aggregation failures on tables with always-null struct/array fields. Default: true (enabled)
   */
  val TANTIVY4SPARK_SCHEMA_FILTER_EMPTY_OBJECTS         = "spark.indextables.schema.filterEmptyObjects"
  val TANTIVY4SPARK_SCHEMA_FILTER_EMPTY_OBJECTS_DEFAULT = true

  /////////////////////
  // L2 Disk Cache Configuration (persistent NVMe caching)
  /////////////////////

  val TANTIVY4SPARK_DISK_CACHE_ENABLED                = "spark.indextables.cache.disk.enabled"
  val TANTIVY4SPARK_DISK_CACHE_PATH                   = "spark.indextables.cache.disk.path"
  val TANTIVY4SPARK_DISK_CACHE_MAX_SIZE               = "spark.indextables.cache.disk.maxSize"
  val TANTIVY4SPARK_DISK_CACHE_COMPRESSION            = "spark.indextables.cache.disk.compression"
  val TANTIVY4SPARK_DISK_CACHE_MIN_COMPRESS_SIZE      = "spark.indextables.cache.disk.minCompressSize"
  val TANTIVY4SPARK_DISK_CACHE_MANIFEST_SYNC_INTERVAL = "spark.indextables.cache.disk.manifestSyncInterval"

  /////////////////////
  // IndexQuery Safety Configuration
  /////////////////////

  /** Maximum fields for unqualified _indexall queries (default: 10, 0 = disabled) */
  val TANTIVY4SPARK_INDEXALL_MAX_UNQUALIFIED_FIELDS = "spark.indextables.indexquery.indexall.maxUnqualifiedFields"
  val TANTIVY4SPARK_INDEXALL_MAX_UNQUALIFIED_FIELDS_DEFAULT = 10
}
