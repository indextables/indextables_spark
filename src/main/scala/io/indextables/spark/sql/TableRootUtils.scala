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

import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

import io.indextables.spark.util.ProtocolNormalizer

object TableRootUtils {

  /** Metadata key prefix for table root entries. */
  val TABLE_ROOT_PREFIX = "indextables.companion.tableRoots."

  /** Build the metadata key for a named table root. */
  def rootKey(rootName: String): String = s"$TABLE_ROOT_PREFIX$rootName"

  /** Build the metadata key for a root's timestamp. */
  def timestampKey(rootName: String): String = s"$TABLE_ROOT_PREFIX$rootName.timestamp"

  /** Check if a metadata key is a table root entry (not a timestamp). */
  def isRootKey(key: String): Boolean =
    key.startsWith(TABLE_ROOT_PREFIX) && !key.endsWith(".timestamp")

  /** Extract the root name from a metadata key. */
  def extractRootName(key: String): String = key.stripPrefix(TABLE_ROOT_PREFIX)

  /** Resolve a path-or-table-identifier string to a Hadoop Path. */
  def resolveTablePath(pathOrTable: String, sparkSession: SparkSession): Path = {
    val isCloudOrLocalPath = ProtocolNormalizer.isS3Path(pathOrTable) ||
      ProtocolNormalizer.isAzurePath(pathOrTable) ||
      pathOrTable.startsWith("/") ||
      pathOrTable.startsWith("hdfs://") ||
      pathOrTable.startsWith("file://")

    if (isCloudOrLocalPath) {
      new Path(pathOrTable)
    } else {
      try {
        val tableIdentifier = sparkSession.sessionState.sqlParser.parseTableIdentifier(pathOrTable)
        val catalog         = sparkSession.sessionState.catalog
        if (catalog.tableExists(tableIdentifier)) {
          val tableMetadata = catalog.getTableMetadata(tableIdentifier)
          new Path(tableMetadata.location)
        } else {
          throw new IllegalArgumentException(s"Table not found: $pathOrTable")
        }
      } catch {
        case _: Exception => new Path(pathOrTable)
      }
    }
  }
}
