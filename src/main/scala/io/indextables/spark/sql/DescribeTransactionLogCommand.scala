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

package io.indextables.spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
 * SQL command to describe the contents of an IndexTables4Spark transaction log.
 *
 * Syntax:
 *   DESCRIBE INDEXTABLES TRANSACTION LOG '/path/to/table'
 *   DESCRIBE INDEXTABLES TRANSACTION LOG '/path/to/table' INCLUDE ALL
 *   DESCRIBE INDEXTABLES TRANSACTION LOG my_catalog.my_database.my_table
 *   DESCRIBE INDEXTABLES TRANSACTION LOG my_catalog.my_database.my_table INCLUDE ALL
 *
 * Without INCLUDE ALL:
 *   Returns current state from latest checkpoint forward (more efficient)
 *
 * With INCLUDE ALL:
 *   Returns complete transaction log history from version 0
 *
 * Returns a DataFrame with all transaction log actions including:
 *   - AddAction: Files added to the table
 *   - RemoveAction: Files removed from the table
 *   - SkipAction: Files that were skipped during operations
 *   - ProtocolAction: Protocol version changes
 *   - MetadataAction: Schema and configuration changes
 */
case class DescribeTransactionLogCommand(
    override val child: LogicalPlan,
    tablePath: String,
    includeAll: Boolean)
    extends RunnableCommand
    with UnaryNode {

  private val logger = LoggerFactory.getLogger(classOf[DescribeTransactionLogCommand])

  override protected def withNewChildInternal(newChild: LogicalPlan): DescribeTransactionLogCommand =
    copy(child = newChild)

  override val output: Seq[Attribute] = Seq(
    // Core identification
    AttributeReference("version", LongType, nullable = false)(),
    AttributeReference("log_file_path", StringType, nullable = false)(),
    AttributeReference("action_type", StringType, nullable = false)(),

    // Common fields (shared across add/remove/skip)
    AttributeReference("path", StringType, nullable = true)(),
    AttributeReference("partition_values", StringType, nullable = true)(),
    AttributeReference("size", LongType, nullable = true)(),
    AttributeReference("data_change", BooleanType, nullable = true)(),
    AttributeReference("tags", StringType, nullable = true)(),

    // AddAction specific fields
    AttributeReference("modification_time", TimestampType, nullable = true)(),
    AttributeReference("stats", StringType, nullable = true)(),
    AttributeReference("min_values", StringType, nullable = true)(),
    AttributeReference("max_values", StringType, nullable = true)(),
    AttributeReference("num_records", LongType, nullable = true)(),
    AttributeReference("footer_start_offset", LongType, nullable = true)(),
    AttributeReference("footer_end_offset", LongType, nullable = true)(),
    AttributeReference("hotcache_start_offset", LongType, nullable = true)(),
    AttributeReference("hotcache_length", LongType, nullable = true)(),
    AttributeReference("has_footer_offsets", BooleanType, nullable = true)(),
    AttributeReference("time_range_start", StringType, nullable = true)(),
    AttributeReference("time_range_end", StringType, nullable = true)(),
    AttributeReference("split_tags", StringType, nullable = true)(),
    AttributeReference("delete_opstamp", LongType, nullable = true)(),
    AttributeReference("num_merge_ops", IntegerType, nullable = true)(),
    AttributeReference("doc_mapping_json", StringType, nullable = true)(),
    AttributeReference("uncompressed_size_bytes", LongType, nullable = true)(),

    // RemoveAction specific fields
    AttributeReference("deletion_timestamp", TimestampType, nullable = true)(),
    AttributeReference("extended_file_metadata", BooleanType, nullable = true)(),

    // SkipAction specific fields
    AttributeReference("skip_timestamp", TimestampType, nullable = true)(),
    AttributeReference("skip_reason", StringType, nullable = true)(),
    AttributeReference("skip_operation", StringType, nullable = true)(),
    AttributeReference("skip_retry_after", TimestampType, nullable = true)(),
    AttributeReference("skip_count", IntegerType, nullable = true)(),

    // ProtocolAction specific fields
    AttributeReference("protocol_min_reader_version", IntegerType, nullable = true)(),
    AttributeReference("protocol_min_writer_version", IntegerType, nullable = true)(),
    AttributeReference("protocol_reader_features", StringType, nullable = true)(),
    AttributeReference("protocol_writer_features", StringType, nullable = true)(),

    // MetadataAction specific fields
    AttributeReference("metadata_id", StringType, nullable = true)(),
    AttributeReference("metadata_name", StringType, nullable = true)(),
    AttributeReference("metadata_description", StringType, nullable = true)(),
    AttributeReference("metadata_format_provider", StringType, nullable = true)(),
    AttributeReference("metadata_format_options", StringType, nullable = true)(),
    AttributeReference("metadata_schema_string", StringType, nullable = true)(),
    AttributeReference("metadata_partition_columns", StringType, nullable = true)(),
    AttributeReference("metadata_configuration", StringType, nullable = true)(),
    AttributeReference("metadata_created_time", TimestampType, nullable = true)(),

    // Checkpoint marker
    AttributeReference("is_checkpoint", BooleanType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info(s"Describing transaction log for table: $tablePath (includeAll=$includeAll)")

    val executor = new DescribeTransactionLogExecutor(
      spark = sparkSession,
      tablePath = tablePath,
      includeAll = includeAll
    )

    executor.execute()
  }
}
