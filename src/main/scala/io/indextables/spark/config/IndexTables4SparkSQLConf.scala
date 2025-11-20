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
  // Optimized Write
  /////////////////////

  // Configuration keys - these will be used to read from SparkConf
  val TANTIVY4SPARK_OPTIMIZE_WRITE_ENABLED                  = "spark.indextables.optimizeWrite.enabled"
  val TANTIVY4SPARK_OPTIMIZE_WRITE_TARGET_RECORDS_PER_SPLIT = "spark.indextables.optimizeWrite.targetRecordsPerSplit"
  val TANTIVY4SPARK_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS   = "spark.indextables.optimizeWrite.maxShufflePartitions"
  val TANTIVY4SPARK_OPTIMIZE_WRITE_MIN_SHUFFLE_PARTITIONS   = "spark.indextables.optimizeWrite.minShufflePartitions"

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
  // Auto-Sizing Configuration
  /////////////////////

  val TANTIVY4SPARK_AUTO_SIZE_ENABLED           = "spark.indextables.autoSize.enabled"
  val TANTIVY4SPARK_AUTO_SIZE_TARGET_SPLIT_SIZE = "spark.indextables.autoSize.targetSplitSize"
  val TANTIVY4SPARK_AUTO_SIZE_INPUT_ROW_COUNT   = "spark.indextables.autoSize.inputRowCount"

  /////////////////////
  // Index Writer Configuration
  /////////////////////

  val TANTIVY4SPARK_INDEX_WRITER_HEAP_SIZE           = "spark.indextables.indexWriter.heapSize"
  val TANTIVY4SPARK_INDEX_WRITER_THREADS             = "spark.indextables.indexWriter.threads"
  val TANTIVY4SPARK_INDEX_WRITER_BATCH_SIZE          = "spark.indextables.indexWriter.batchSize"
  val TANTIVY4SPARK_INDEX_WRITER_USE_BATCH           = "spark.indextables.indexWriter.useBatch"
  val TANTIVY4SPARK_SPLIT_CONVERSION_MAX_PARALLELISM = "spark.indextables.splitConversion.maxParallelism"

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
}
