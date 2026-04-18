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

package io.indextables.spark.catalog

import java.util
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import com.google.common.cache.{Cache, CacheBuilder}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{
  Identifier,
  NamespaceChange,
  SupportsNamespaces,
  Table,
  TableCapability,
  TableCatalog,
  TableChange
}

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.core.IndexTables4SparkTable
import io.indextables.spark.transaction.TransactionLogFactory

import org.slf4j.LoggerFactory

/**
 * A read-only Spark V2 named catalog that resolves companion IndexTables indexes by source table
 * name rather than raw storage paths.
 *
 * Register via Spark configuration:
 * {{{
 *   spark.sql.catalog.indextables = io.indextables.catalog.IndexTablesCatalog
 * }}}
 *
 * Usage:
 * {{{
 *   spark.read.table("indextables.unity_catalog.production.users")
 * }}}
 *
 * The identifier `indextables.unity_catalog.production.users` is resolved as:
 *   - Source catalog:    `unity_catalog`
 *   - Source namespace:  `["production"]`
 *   - Source table:      `users`
 *
 * The catalog reads TBLPROPERTIES from the source table to find the index storage path:
 *   - `indextables.companion.indexroot.{region}` — region-specific index storage root
 *   - `indextables.companion.indexroot`           — fallback index storage root
 *   - `indextables.companion.tableroot.relativepath` — relative path within the index root
 *
 * Resolved paths are cached (default: 1 hour TTL, 1000 entries max) to avoid repeated catalog
 * lookups during query planning.
 *
 * Write operations are intentionally unsupported — use IndexTablesProvider or
 * BUILD INDEXTABLES COMPANION for writes.
 */
class IndexTables4SparkCatalog extends TableCatalog with SupportsNamespaces {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkCatalog])

  private var catalogName: String = _

  // Cache: identifier string → resolved absolute index path
  // Isolates callers from repeated CatalogManager.loadTable() calls during Spark query planning.
  private var pathCache: Cache[String, String] = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name

    // Option key is scoped to catalog options, not SparkConf, so no spark.indextables.* prefix.
    // Users set it as: spark.sql.catalog.<catalogName>.catalog.cache.ttl.minutes=<value>
    val ttlMinutes = Option(options.get(IndexTables4SparkCatalog.CATALOG_CACHE_TTL_OPTION))
      .flatMap(s => scala.util.Try(s.toLong).toOption)
      .getOrElse(60L)

    pathCache = CacheBuilder
      .newBuilder()
      .maximumSize(1000)
      .expireAfterWrite(ttlMinutes, TimeUnit.MINUTES)
      .build[String, String]()

    logger.info(
      s"IndexTables4SparkCatalog '$catalogName' initialized (path cache TTL: ${ttlMinutes}m)"
    )
  }

  override def name(): String = catalogName

  // ---------------------------------------------------------------------------
  // TableCatalog — read
  // ---------------------------------------------------------------------------

  override def loadTable(ident: Identifier): Table = {
    val ns = ident.namespace()

    if (ns.isEmpty) {
      // Note: AnalysisException in Spark 3.5.3 exposes only secondary constructors that require
      // an errorClass string key from Spark's internal error catalog — there is no public
      // plain-message constructor accessible from Scala 2. IllegalArgumentException surfaces
      // the same message to the user and is the correct behaviour for an invalid identifier.
      throw new IllegalArgumentException(
        s"Invalid identifier '${ident.name()}' for IndexTablesCatalog '$catalogName': " +
        s"expected at least <source_catalog>.<table_name> " +
        s"(e.g., '$catalogName.spark_catalog.default.my_table'). " +
        s"The first namespace segment must be the source catalog name."
      )
    }

    val sourceCatalogName = ns(0)
    val sourceNamespace   = ns.drop(1)
    val sourceTableName   = ident.name()

    val cacheKey = (ns ++ Array(sourceTableName)).mkString(".")
    logger.debug(s"loadTable: catalog='$catalogName', key='$cacheKey'")

    val resolvedPath =
      try {
        pathCache.get(
          cacheKey,
          () => IndexTableResolver.resolveFromCatalog(sourceCatalogName, sourceNamespace, sourceTableName, SparkSession.active)
        )
      } catch {
        // Guava wraps Callable exceptions — unwrap to surface the original cause.
        // UncheckedExecutionException wraps RuntimeExceptions; ExecutionException wraps checked ones
        // (e.g. NoSuchTableException). Both must be handled to avoid opaque wrapper exceptions.
        case e: com.google.common.util.concurrent.UncheckedExecutionException =>
          throw e.getCause
        case e: java.util.concurrent.ExecutionException =>
          throw e.getCause
      }

    val spark  = SparkSession.active
    val txLog  = TransactionLogFactory.create(new Path(resolvedPath), spark)
    val schema = try {
      txLog.getSchema().getOrElse {
        // Path resolved from TBLPROPERTIES but the index doesn't exist yet (or was deleted).
        // Evict the cache entry so a retry won't use a stale path.
        pathCache.invalidate(cacheKey)
        throw new IllegalArgumentException(
          s"No transaction log found at resolved index path '$resolvedPath' " +
          s"for source table '$cacheKey' in catalog '$catalogName'. " +
          s"Ensure the companion index has been built and the source table has the " +
          s"required TBLPROPERTIES set:\n" +
          s"  ALTER TABLE $cacheKey SET TBLPROPERTIES (\n" +
          s"    '${IndexTableResolver.PROP_INDEX_ROOT_PREFIX}' = 's3://my-index-bucket/indexes',\n" +
          s"    '${IndexTableResolver.PROP_RELATIVE_PATH}' = 'my/table'\n" +
          s"  )"
        )
      }
    } finally {
      txLog.close()
    }

    val tableOptions = new CaseInsensitiveStringMap(
      Map("path" -> resolvedPath).asJava
    )
    // Return a read-only view: suppress the BATCH_WRITE / OVERWRITE_BY_FILTER / TRUNCATE
    // capabilities that IndexTables4SparkTable normally advertises so the Spark planner
    // does not treat catalog-resolved tables as writable.
    // Note: restricting TableCapability to BATCH_READ does NOT affect filter, column, or
    // aggregate pushdown — those go through ScanBuilder interfaces (SupportsPushDownFilters,
    // SupportsPushDownRequiredColumns, SupportsPushDownAggregates) which are independent of
    // TableCapability. All pushdown behaviour is fully preserved for catalog-resolved tables.
    new IndexTables4SparkTable(resolvedPath, schema, tableOptions, Array.empty) {
      override def capabilities(): java.util.Set[TableCapability] =
        Set(TableCapability.BATCH_READ).asJava
    }
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    logger.warn(
      s"IndexTablesCatalog '$catalogName': listTables() is not supported — " +
      s"catalog does not enumerate source tables. Returning empty result."
    )
    Array.empty
  }

  // ---------------------------------------------------------------------------
  // TableCatalog — write operations (intentionally unsupported)
  // ---------------------------------------------------------------------------

  override def createTable(
    ident: Identifier,
    schema: StructType,
    partitions: Array[Transform],
    properties: util.Map[String, String]
  ): Table =
    throw new UnsupportedOperationException(
      s"IndexTablesCatalog '$catalogName' does not support CREATE TABLE. " +
      s"Use IndexTablesProvider for direct writes, or BUILD INDEXTABLES COMPANION " +
      s"to create a companion index for an existing source table."
    )

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException(
      s"IndexTablesCatalog '$catalogName' does not support ALTER TABLE. " +
      s"Modify the source table TBLPROPERTIES directly."
    )

  override def dropTable(ident: Identifier): Boolean = {
    logger.warn(
      s"IndexTablesCatalog '$catalogName': dropTable() is not supported — returning false. " +
      s"Use IndexTablesProvider or PURGE INDEXTABLE for index deletion."
    )
    false
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException(
      s"IndexTablesCatalog '$catalogName' does not support RENAME TABLE."
    )

  // ---------------------------------------------------------------------------
  // SupportsNamespaces — minimal implementation to satisfy SHOW NAMESPACES etc.
  // ---------------------------------------------------------------------------

  override def listNamespaces(): Array[Array[String]] = {
    logger.warn(
      s"IndexTablesCatalog '$catalogName': listNamespaces() is not supported — " +
      s"catalog does not enumerate source namespaces. Returning empty result."
    )
    Array.empty
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    logger.warn(
      s"IndexTablesCatalog '$catalogName': listNamespaces(${namespace.mkString(".")}) is not supported. " +
      s"Returning empty result."
    )
    Array.empty
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    logger.warn(
      s"IndexTablesCatalog '$catalogName': loadNamespaceMetadata(${namespace.mkString(".")}) " +
      s"is not supported. Returning empty metadata."
    )
    java.util.Collections.emptyMap()
  }

  override def namespaceExists(namespace: Array[String]): Boolean = {
    logger.warn(
      s"IndexTablesCatalog '$catalogName': namespaceExists(${namespace.mkString(".")}) " +
      s"always returns false — catalog does not track namespaces."
    )
    false
  }

  override def createNamespace(
    namespace: Array[String],
    metadata: util.Map[String, String]
  ): Unit =
    throw new UnsupportedOperationException(
      s"IndexTablesCatalog '$catalogName' does not support CREATE NAMESPACE."
    )

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    throw new UnsupportedOperationException(
      s"IndexTablesCatalog '$catalogName' does not support ALTER NAMESPACE."
    )

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean =
    throw new UnsupportedOperationException(
      s"IndexTablesCatalog '$catalogName' does not support DROP NAMESPACE."
    )
}

object IndexTables4SparkCatalog {
  // Catalog options are scoped to the catalog's own options map (passed by Spark from
  // spark.sql.catalog.<name>.* properties), so this key has no spark.indextables.* prefix.
  // Users set it as: spark.sql.catalog.<catalogName>.catalog.cache.ttl.minutes=<value>
  val CATALOG_CACHE_TTL_OPTION = "catalog.cache.ttl.minutes"
}
