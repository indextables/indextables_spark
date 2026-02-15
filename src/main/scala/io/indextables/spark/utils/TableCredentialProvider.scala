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
 * Trait for credential providers that support table-level credential resolution.
 *
 * Credential provider companion objects that implement this trait declare that they
 * can resolve credentials scoped to a specific table (identified by UUID) rather than
 * a storage path. This is used by catalog systems (e.g., Unity Catalog) that vend
 * temporary credentials per table.
 *
 * To add table credential support to a new provider:
 *   1. Have the provider's companion object extend this trait
 *   2. Implement `resolveTableId` and `getTableCredentials`
 *   3. The credential factory will automatically detect and route to the trait methods
 *      when `spark.indextables.iceberg.uc.tableId` is present in the config
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
   * Resolve a catalog table name to its unique table identifier (e.g., UUID).
   * Called once on the driver side during BUILD COMPANION setup.
   *
   * @param fullTableName The fully-qualified table name (e.g., "catalog.namespace.table")
   * @param config        Configuration map with provider-specific settings
   * @return The unique table identifier (e.g., a UUID string)
   */
  def resolveTableId(fullTableName: String, config: Map[String, String]): String

  /**
   * Get AWS credentials scoped to a specific table using its unique identifier.
   * Called by executors when the table ID is available in the config.
   *
   * @param tableId The table's unique identifier (from resolveTableId)
   * @param config  Configuration map with provider-specific settings
   * @return AWS credentials for accessing the table's storage
   */
  def getTableCredentials(
    tableId: String,
    config: Map[String, String]
  ): CredentialProviderFactory.BasicAWSCredentials
}
