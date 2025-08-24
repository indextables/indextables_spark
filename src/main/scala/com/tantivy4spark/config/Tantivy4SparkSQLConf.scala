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

package com.tantivy4spark.config

import org.apache.spark.sql.internal.SQLConf

/**
 * SQL configuration settings for Tantivy4Spark.
 */
object Tantivy4SparkSQLConf {

  /////////////////////
  // Optimized Write
  /////////////////////

  // Configuration keys - these will be used to read from SparkConf
  val TANTIVY4SPARK_OPTIMIZE_WRITE_ENABLED = "spark.tantivy4spark.optimizeWrite.enabled"
  val TANTIVY4SPARK_OPTIMIZE_WRITE_TARGET_RECORDS_PER_SPLIT = "spark.tantivy4spark.optimizeWrite.targetRecordsPerSplit"  
  val TANTIVY4SPARK_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS = "spark.tantivy4spark.optimizeWrite.maxShufflePartitions"
  val TANTIVY4SPARK_OPTIMIZE_WRITE_MIN_SHUFFLE_PARTITIONS = "spark.tantivy4spark.optimizeWrite.minShufflePartitions"

  /////////////////////
  // AWS Configuration
  /////////////////////

  val TANTIVY4SPARK_AWS_ACCESS_KEY = "spark.tantivy4spark.aws.accessKey"
  val TANTIVY4SPARK_AWS_SECRET_KEY = "spark.tantivy4spark.aws.secretKey"
  val TANTIVY4SPARK_AWS_SESSION_TOKEN = "spark.tantivy4spark.aws.sessionToken"
  val TANTIVY4SPARK_AWS_REGION = "spark.tantivy4spark.aws.region"
  val TANTIVY4SPARK_AWS_ENDPOINT = "spark.tantivy4spark.aws.endpoint"

  /////////////////////
  // General Configuration
  /////////////////////

  val TANTIVY4SPARK_STORAGE_FORCE_STANDARD = "spark.tantivy4spark.storage.force.standard"
  val TANTIVY4SPARK_BLOOM_FILTERS_ENABLED = "spark.tantivy4spark.bloom.filters.enabled"
}