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

package io.indextables.spark.transaction

import org.apache.spark.sql.types.StructType

/**
 * Result of a native Arrow FFI listFiles call. Combines:
 * - File listing (already filtered by partition pruning, data skipping, cooldown)
 * - Table metadata (schema, partition columns, protocol) — eliminates separate JNI calls
 * - Filtering metrics (for DataSkippingMetrics / Spark UI)
 */
case class NativeListFilesResult(
  files: Seq[AddAction],
  schema: Option[StructType],
  partitionColumns: Seq[String],
  protocol: ProtocolAction,
  metadataConfig: Map[String, String],
  metrics: NativeFilteringMetrics)

/**
 * Metrics from native-side filtering, returned alongside the file listing.
 * Used by DataSkippingMetrics.recordScan() and DESCRIBE DATA SKIPPING STATS.
 */
case class NativeFilteringMetrics(
  totalFilesBeforeFiltering: Long,
  filesAfterPartitionPruning: Long,
  filesAfterDataSkipping: Long,
  filesAfterCooldownFiltering: Long,
  manifestsTotal: Long,
  manifestsPruned: Long)
