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

package io.indextables.spark.utils

/**
 * Contains both the table UUID and the storage location (e.g., S3 path) for a catalog table. Used by catalog-aware
 * credential providers that can resolve both in a single API call.
 *
 * @param tableId
 *   The unique table identifier (e.g., a UUID string)
 * @param storageLocation
 *   The table's storage location (e.g., "s3://bucket/path/to/table")
 */
case class TableInfo(tableId: String, storageLocation: String)

/**
 * Trait for credential providers that support table-level credential resolution.
 *
 * Credential provider companion objects that implement this trait declare that they can resolve credentials scoped to a
 * specific table (identified by UUID) rather than a storage path. This is used by catalog systems (e.g., Unity Catalog)
 * that vend temporary credentials per table.
 *
 * To add table credential support to a new provider:
 *   1. Have the provider's companion object extend this trait 2. Implement `resolveTableId` and `getTableCredentials`
 *      3. The credential factory will automatically detect and route to the trait methods when
 *      `spark.indextables.iceberg.uc.tableId` is present in the config (key name is historical; used for both Iceberg
 *      and Delta UC tables)
 *
 * Example:
 * {{{
 * object MyCredentialProvider extends TableCredentialProvider {
 *   def resolveTableId(fullTableName: String, config: Map[String, String]): String = ???
 *   def getTableCredentials(tableId: String, config: Map[String, String]): BasicAWSCredentials = ???
 * }
 * }}}
 */
trait TableCredentialProvider {

  /**
   * Resolve a catalog table name to its unique table identifier (e.g., UUID). Called once on the driver side during
   * BUILD COMPANION setup.
   *
   * @param fullTableName
   *   The fully-qualified table name (e.g., "catalog.namespace.table")
   * @param config
   *   Configuration map with provider-specific settings
   * @return
   *   The unique table identifier (e.g., a UUID string)
   */
  def resolveTableId(fullTableName: String, config: Map[String, String]): String

  /**
   * Get AWS credentials scoped to a specific table using its unique identifier. Called by executors when the table ID
   * is available in the config.
   *
   * @param tableId
   *   The table's unique identifier (from resolveTableId)
   * @param config
   *   Configuration map with provider-specific settings
   * @return
   *   AWS credentials for accessing the table's storage
   */
  def getTableCredentials(
    tableId: String,
    config: Map[String, String]
  ): CredentialProviderFactory.BasicAWSCredentials

  /**
   * Provide default Iceberg catalog configuration derived from the provider's settings.
   *
   * Returns a map of `spark.indextables.iceberg.*` keys that will be merged into the config with lower precedence than
   * user-explicit values. This allows providers to auto-derive catalog connection settings (URI, token, catalog type)
   * from their own configuration, so users don't need to set them manually.
   *
   * For example, a Unity Catalog provider can derive the Iceberg REST endpoint from the workspace URL and reuse the API
   * token for catalog authentication.
   *
   * Default implementation returns an empty map (no auto-derived config).
   *
   * @param config
   *   Configuration map with provider-specific settings
   * @return
   *   Map of `spark.indextables.iceberg.*` defaults to merge
   */
  /**
   * Resolve a catalog table name to both its unique table identifier and storage location. Called once on the driver
   * side during BUILD COMPANION setup.
   *
   * This is useful for formats like Delta where the storage location is needed to read the table's data files, but the
   * table is identified by name in a catalog.
   *
   * Default implementation delegates to resolveTableId and returns an empty storage location for backward compatibility
   * with providers that don't implement this method.
   *
   * @param fullTableName
   *   The fully-qualified table name (e.g., "catalog.namespace.table")
   * @param config
   *   Configuration map with provider-specific settings
   * @return
   *   TableInfo containing the table ID and storage location
   */
  def resolveTableInfo(fullTableName: String, config: Map[String, String]): TableInfo =
    TableInfo(resolveTableId(fullTableName, config), "")

  def icebergCatalogDefaults(config: Map[String, String]): Map[String, String] = Map.empty
}
