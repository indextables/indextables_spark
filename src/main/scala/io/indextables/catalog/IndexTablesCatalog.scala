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

package io.indextables.catalog

import io.indextables.spark.catalog.IndexTables4SparkCatalog

/**
 * Public alias for IndexTables4SparkCatalog.
 *
 * Provides a stable, user-facing class name following the same two-tier pattern as
 * IndexTablesProvider / IndexTables4SparkTableProvider.
 *
 * Register via Spark configuration:
 * {{{
 *   spark.sql.catalog.indextables = io.indextables.catalog.IndexTablesCatalog
 * }}}
 *
 * Usage:
 * {{{
 *   spark.read.table("indextables.unity_catalog.production.users")
 *   spark.sql("SELECT * FROM indextables.unity_catalog.production.users WHERE ...")
 * }}}
 */
class IndexTablesCatalog extends IndexTables4SparkCatalog
