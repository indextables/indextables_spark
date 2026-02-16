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

package io.indextables.spark.core

import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{
  Scan,
  ScanBuilder,
  SupportsPushDownAggregates,
  SupportsPushDownFilters,
  SupportsPushDownLimit,
  SupportsPushDownRequiredColumns,
  SupportsPushDownV2Filters
}
import org.apache.spark.sql.sources.{Filter, IsNotNull, IsNull, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import io.indextables.spark.exceptions.IndexQueryParseException
import io.indextables.spark.expressions.{
  BucketAggregationConfig,
  BucketExpression,
  BucketExpressions,
  DateHistogramConfig,
  DateHistogramExpression,
  HistogramConfig,
  HistogramExpression,
  RangeBucket,
  RangeConfig,
  RangeExpression
}
import io.indextables.spark.filters.{
  IndexQueryAllFilter,
  IndexQueryFilter,
  MixedAndFilter,
  MixedBooleanFilter,
  MixedIndexQuery,
  MixedIndexQueryAll,
  MixedNotFilter,
  MixedOrFilter,
  MixedSparkFilter
}
import io.indextables.spark.transaction.{EnhancedTransactionLogCache, TransactionLog}
import org.slf4j.LoggerFactory

class IndexTables4SparkScanBuilder(
  sparkSession: SparkSession,
  transactionLog: TransactionLog,
  schema: StructType,
  options: CaseInsensitiveStringMap,
  config: Map[String, String] // Direct config instead of broadcast
) extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownV2Filters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownLimit
    with SupportsPushDownAggregates {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkScanBuilder])

  logger.debug(s"SCAN BUILDER CREATED: NEW ScanBuilder instance ${System.identityHashCode(this)}")
  // Filters that have been pushed down and will be applied by the data source
  private var _pushedFilters      = Array.empty[Filter]
  private var _unsupportedFilters = Array.empty[Filter] // Track filters Spark will re-evaluate
  private var requiredSchema      = schema
  private var _limit: Option[Int] = None

  // Aggregate pushdown state
  private var _pushedAggregation: Option[Aggregation] = None
  private var _pushedGroupBy: Option[Array[String]]   = None

  // Bucket aggregation state (DateHistogram, Histogram, Range)
  private var _bucketConfig: Option[BucketAggregationConfig] = None

  // Inject companion mode config from transaction log metadata (lazy, computed once).
  // Resolves credentials for parquet file access on the driver so executors can use them.
  private lazy val effectiveConfig: Map[String, String] = {
    if (config.contains("spark.indextables.companion.parquetTableRoot")) {
      logger.info(s"effectiveConfig: companion parquetTableRoot already in config: ${config("spark.indextables.companion.parquetTableRoot")}")
      config
    } else {
      try {
        val metadata    = transactionLog.getMetadata()
        val isCompanion = metadata.configuration.getOrElse("indextables.companion.enabled", "false") == "true"
        if (isCompanion) {
          metadata.configuration.get("indextables.companion.sourceTablePath") match {
            case Some(path) =>
              val sourceFormat = metadata.configuration.getOrElse("indextables.companion.sourceFormat", "delta")

              // For Iceberg: use stored S3 root for parquetTableRoot (sourcePath is a
              // table identifier like "prod.events", not an S3 path), and try table-based
              // credential resolution at runtime.
              val (effectivePath, baseConfig) = if (sourceFormat == "iceberg") {
                val storageRoot = metadata.configuration.get("indextables.companion.parquetStorageRoot")
                val root = storageRoot.getOrElse {
                  logger.warn(
                    s"Iceberg companion: no parquetStorageRoot in metadata, " +
                      s"falling back to sourceTablePath=$path"
                  )
                  path
                }
                logger.info(s"Iceberg companion mode detected: parquetTableRoot=$root (sourceTablePath=$path)")
                val enriched = tryResolveIcebergTableCredentials(metadata.configuration, config)
                (root, enriched)
              } else if (
                sourceFormat == "delta" &&
                metadata.configuration.contains("indextables.companion.deltaTableName")
              ) {
                // Delta + UC table name: resolve credentials via table-based API
                val storedPath = metadata.configuration
                  .get("indextables.companion.parquetStorageRoot")
                  .getOrElse(path)
                logger.info(
                  s"Delta UC companion mode detected: parquetTableRoot=$storedPath " +
                    s"(deltaTableName=${metadata.configuration.getOrElse("indextables.companion.deltaTableName", "")})"
                )
                val enriched = tryResolveDeltaTableCredentials(metadata.configuration, config)
                (storedPath, enriched)
              } else {
                logger.info(s"Companion mode detected: injecting parquetTableRoot=$path")
                (path, config)
              }

              var enrichedConfig = baseConfig + ("spark.indextables.companion.parquetTableRoot" -> effectivePath)

              // Resolve credentials for the parquet table root on the driver.
              // On Databricks/Unity Catalog, the companion index and Delta table may need
              // different credentials (different external locations or vended tokens).
              try {
                io.indextables.spark.utils.CredentialProviderFactory
                  .resolveAWSCredentialsFromConfig(enrichedConfig, effectivePath)
                  .foreach { creds =>
                    logger.info(s"Companion mode: resolved parquet credentials for $effectivePath (accessKey=${creds.accessKey.take(4)}...)")
                    enrichedConfig = enrichedConfig +
                      ("spark.indextables.companion.parquet.aws.accessKey" -> creds.accessKey) +
                      ("spark.indextables.companion.parquet.aws.secretKey" -> creds.secretKey)
                    creds.sessionToken.foreach { token =>
                      enrichedConfig = enrichedConfig +
                        ("spark.indextables.companion.parquet.aws.sessionToken" -> token)
                    }
                  }
                // Propagate region and endpoint from main config
                enrichedConfig
                  .get("spark.indextables.aws.region")
                  .orElse(enrichedConfig.get("spark.indextables.aws.region".toLowerCase))
                  .foreach { region =>
                    enrichedConfig = enrichedConfig +
                      ("spark.indextables.companion.parquet.aws.region" -> region)
                  }
                enrichedConfig
                  .get("spark.indextables.s3.endpoint")
                  .orElse(enrichedConfig.get("spark.indextables.s3.endpoint".toLowerCase))
                  .foreach { endpoint =>
                    enrichedConfig = enrichedConfig +
                      ("spark.indextables.companion.parquet.aws.endpoint" -> endpoint)
                  }
              } catch {
                case e: Exception =>
                  logger.warn(s"Failed to resolve parquet credentials for companion mode: ${e.getMessage}")
              }
              // Propagate stored indexing modes as typemap entries so that
              // isFieldSuitableForExactMatching() knows which fields are TEXT vs STRING.
              // Without this, TEXT fields default to "string" and get incorrect EqualTo pushdown.
              metadata.configuration.get("indextables.companion.indexingModes").foreach { json =>
                try {
                  val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
                  import scala.jdk.CollectionConverters._
                  val modes: java.util.Map[String, String] =
                    mapper.readValue(json, classOf[java.util.HashMap[String, String]])
                  modes.asScala.foreach {
                    case (field, mode) =>
                      val key = s"spark.indextables.indexing.typemap.$field"
                      if (!enrichedConfig.contains(key)) {
                        logger.info(s"Companion mode: propagating indexingMode $field -> $mode as typemap entry")
                        enrichedConfig = enrichedConfig + (key -> mode)
                      }
                  }
                } catch {
                  case e: Exception =>
                    logger.warn(s"Failed to parse companion indexingModes: ${e.getMessage}")
                }
              }

              // Strip tableId from the final config so that split credential resolution
              // uses path-based credentials (for the indextables split storage), not
              // table-based credentials (scoped to the Iceberg source table). Companion
              // parquet credentials are already stored in separate companion.parquet.* keys.
              enrichedConfig - "spark.indextables.iceberg.uc.tableId"
            case None =>
              logger.warn("Companion mode enabled but no sourceTablePath in metadata")
              config
          }
        } else {
          logger.info(
            s"effectiveConfig: not a companion table (indextables.companion.enabled=${metadata.configuration.getOrElse("indextables.companion.enabled", "not set")})"
          )
          config
        }
      } catch {
        case e: Exception =>
          logger.warn(s"Could not read metadata for companion mode detection: ${e.getMessage}", e)
          config
      }
    }
  }

  /**
   * For Iceberg companions: try to resolve table credentials at read time by reconstructing the full table name from
   * stored catalog coordinates and calling the TableCredentialProvider to get a fresh table ID. This enables the
   * Priority 1.5 path in CredentialProviderFactory (table-based credential vending).
   *
   * Falls back to the original config (path-based resolution) on any failure.
   */
  private def tryResolveIcebergTableCredentials(
    companionConfig: Map[String, String],
    baseConfig: Map[String, String]
  ): Map[String, String] = {
    val providerOpt = baseConfig
      .get("spark.indextables.aws.credentialsProviderClass")
      .filter(_.nonEmpty)
      .flatMap(io.indextables.spark.utils.CredentialProviderFactory.resolveTableCredentialProvider)

    providerOpt match {
      case Some(provider) =>
        try {
          val catalog       = companionConfig.getOrElse("indextables.companion.icebergCatalog", "default")
          val tablePath     = companionConfig.getOrElse("indextables.companion.sourceTablePath", "")
          val fullTableName = s"$catalog.$tablePath"

          // Merge auto-derived catalog defaults (URI, token, etc.) from provider
          val withDefaults = provider.icebergCatalogDefaults(baseConfig).foldLeft(baseConfig) {
            case (acc, (key, value)) => if (acc.contains(key)) acc else acc + (key -> value)
          }

          val tableId = provider.resolveTableId(fullTableName, withDefaults)
          logger.info(s"Resolved Iceberg table ID for '$fullTableName': $tableId")

          // Inject tableId so Priority 1.5 in CredentialProviderFactory activates
          withDefaults + ("spark.indextables.iceberg.uc.tableId" -> tableId)
        } catch {
          case e: Exception =>
            logger.warn(
              s"Failed to resolve Iceberg table credentials at read time: ${e.getMessage}. " +
                s"Falling back to path-based credential resolution."
            )
            baseConfig
        }
      case None =>
        baseConfig
    }
  }

  /**
   * For Delta UC companions: try to resolve table credentials at read time by reconstructing the full table name from
   * stored catalog coordinates and calling the TableCredentialProvider to get a fresh table ID. This enables the
   * Priority 1.5 path in CredentialProviderFactory (table-based credential vending).
   *
   * Falls back to the original config (path-based resolution) on any failure.
   */
  private def tryResolveDeltaTableCredentials(
    companionConfig: Map[String, String],
    baseConfig: Map[String, String]
  ): Map[String, String] = {
    val providerOpt = baseConfig
      .get("spark.indextables.aws.credentialsProviderClass")
      .filter(_.nonEmpty)
      .flatMap(io.indextables.spark.utils.CredentialProviderFactory.resolveTableCredentialProvider)

    providerOpt match {
      case Some(provider) =>
        try {
          val catalog       = companionConfig.getOrElse("indextables.companion.deltaCatalog", "")
          val tableName     = companionConfig.getOrElse("indextables.companion.deltaTableName", "")
          val fullTableName = if (catalog.nonEmpty) s"$catalog.$tableName" else tableName

          val tableInfo = provider.resolveTableInfo(fullTableName, baseConfig)
          logger.info(s"Resolved Delta table ID for '$fullTableName': ${tableInfo.tableId}")

          // Inject tableId so Priority 1.5 in CredentialProviderFactory activates
          baseConfig + ("spark.indextables.iceberg.uc.tableId" -> tableInfo.tableId)
        } catch {
          case e: Exception =>
            logger.warn(
              s"Failed to resolve Delta table credentials at read time: ${e.getMessage}. " +
                s"Falling back to path-based credential resolution."
            )
            baseConfig
        }
      case None =>
        baseConfig
    }
  }

  // IMPORTANT: Do NOT capture relation at construction time!
  // ScanBuilders may be created before V2IndexQueryExpressionRule runs.
  // Instead, look up the relation from ThreadLocal at usage time (in build() and pushFilters()).
  // ThreadLocal is set by V2IndexQueryExpressionRule during optimization.

  override def build(): Scan = {
    val aggregation = _pushedAggregation

    // Look up relation from ThreadLocal at usage time (not at construction time!)
    val relationForIndexQuery = IndexTables4SparkScanBuilder.getCurrentRelation()

    logger.debug(s"BUILD: ScanBuilder.build() called on instance ${System.identityHashCode(this)}")
    logger.debug(s"BUILD: Aggregation present: ${aggregation.isDefined}, filters: ${_pushedFilters.length}")

    // CRITICAL: Handle multiple optimization passes
    // Spark runs V2ScanRelationPushDown multiple times, creating fresh ScanBuilders each time
    // Only builders with Filter nodes get pushFilters() called
    // The FINAL builder (which might not have pushFilters called) is the one executed
    // Solution: If instance filters are empty, try retrieving from relation object storage
    val effectiveFilters = if (_pushedFilters.nonEmpty) {
      logger.debug(s"BUILD: Using instance variable filters: ${_pushedFilters.length}")
      _pushedFilters
    } else {
      // Instance filters empty - try relation object storage
      relationForIndexQuery match {
        case Some(relation) =>
          val relationFilters = IndexTables4SparkScanBuilder.getPushedFilters(relation)
          logger.debug(s"BUILD: Instance filters empty, retrieved ${relationFilters.length} from relation object: ${System.identityHashCode(relation)}")
          relationFilters
        case None =>
          logger.warn(s"BUILD: Instance filters empty and no relation in ThreadLocal, cannot retrieve stored filters")
          Array.empty[Filter]
      }
    }

    logger.debug(s"BUILD: Effective filters count: ${effectiveFilters.length}")
    effectiveFilters.foreach(filter => logger.debug(s"BUILD:   - Effective filter: $filter"))

    // Create the scan
    val scan = aggregation match {
      case Some(agg) =>
        logger.debug(s"BUILD: Creating aggregate scan for pushed aggregation")
        createAggregateScan(agg, effectiveFilters)
      case None =>
        // Regular scan
        logger.debug(s"BUILD: Creating regular scan (no aggregation pushdown)")

        // CRITICAL: Check if unsupported filters blocked aggregate pushdown
        // If there are unsupported filters, Spark won't even call pushAggregation(),
        // which means any aggregation in the query will produce incorrect results.
        // We check if the query contains an aggregation and fail if so.
        //
        // NOTE: Just like effectiveFilters, we must also retrieve unsupported filters from
        // the relation object storage because build() may be called on a different ScanBuilder
        // instance than the one where pushFilters() was called.
        val effectiveUnsupportedFilters = if (_unsupportedFilters.nonEmpty) {
          logger.debug(s"BUILD: Using instance variable unsupported filters: ${_unsupportedFilters.length}")
          _unsupportedFilters
        } else {
          relationForIndexQuery match {
            case Some(relation) =>
              val filters = IndexTables4SparkScanBuilder.getUnsupportedFilters(relation)
              logger.debug(s"BUILD: Retrieved ${filters.length} unsupported filters from relation object: ${System.identityHashCode(relation)}")
              filters
            case None =>
              logger.debug(s"BUILD: No relation in ThreadLocal, cannot retrieve unsupported filters")
              Array.empty[Filter]
          }
        }

        // Check for unsupported filters that would block aggregate pushdown.
        // When Spark has unsupported filters, it won't call pushAggregation(), which means
        // any aggregation in the query will produce incorrect results (due to our default limit).
        //
        // Note: IsNull/IsNotNull on non-FAST fields are "safely unsupported" - they will be
        // handled by Spark's post-filtering, which doesn't affect aggregate correctness.
        // We only block on truly problematic unsupported filters.
        //
        // Use explicit isInstanceOf checks to ensure proper filtering (pattern matching can
        // be tricky with Filter subclasses).
        def isSafelyUnsupportedFilter(f: Filter): Boolean =
          f.isInstanceOf[IsNull] || f.isInstanceOf[IsNotNull]

        val safelyUnsupportedFilters   = effectiveUnsupportedFilters.filter(isSafelyUnsupportedFilter)
        val blockingUnsupportedFilters = effectiveUnsupportedFilters.filterNot(isSafelyUnsupportedFilter)

        logger.debug(
          s"BUILD: effectiveUnsupportedFilters=${effectiveUnsupportedFilters.length}, " +
            s"safelyUnsupported=${safelyUnsupportedFilters.length}, blocking=${blockingUnsupportedFilters.length}"
        )
        effectiveUnsupportedFilters.foreach(f =>
          logger.debug(
            s"BUILD:   - Unsupported filter: $f (class=${f.getClass.getName}, isSafe=${isSafelyUnsupportedFilter(f)})"
          )
        )

        val hasAggregateInPlan = detectAggregateInQueryPlan()
        logger.debug(s"BUILD: hasAggregateInPlan=$hasAggregateInPlan")

        if (blockingUnsupportedFilters.nonEmpty && hasAggregateInPlan) {
          val unsupportedDesc = blockingUnsupportedFilters.map(_.toString).mkString(", ")
          // Build specific guidance for string pattern filters
          val hasStringStartsWith    = blockingUnsupportedFilters.exists(_.isInstanceOf[StringStartsWith])
          val hasStringEndsWith      = blockingUnsupportedFilters.exists(_.isInstanceOf[StringEndsWith])
          val hasStringContains      = blockingUnsupportedFilters.exists(_.isInstanceOf[StringContains])
          val hasStringPatternFilter = hasStringStartsWith || hasStringEndsWith || hasStringContains

          val stringPatternHint = if (hasStringPatternFilter) {
            val patternTypes = Seq(
              if (hasStringStartsWith) "stringStartsWith" else "",
              if (hasStringEndsWith) "stringEndsWith" else "",
              if (hasStringContains) "stringContains" else ""
            ).filter(_.nonEmpty)

            s" To enable string pattern pushdown, set " +
              s"spark.indextables.filter.stringPattern.pushdown=true (enables all patterns) " +
              s"or individually: ${patternTypes.map(t => s"spark.indextables.filter.$t.pushdown=true").mkString(", ")}."
          } else ""

          throw new IllegalStateException(
            s"Aggregate pushdown blocked by unsupported filter(s): [$unsupportedDesc]. " +
              s"IndexTables4Spark requires aggregate pushdown for correct COUNT/SUM/AVG/MIN/MAX results. " +
              s"The filter type(s) used are not fully supported, which prevents aggregate optimization. " +
              s"Supported filter types: EqualTo, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, In, IsNull (FAST fields), IsNotNull (FAST fields), And, Or, Not. " +
              s"Note: IsNull/IsNotNull on non-FAST fields are handled by Spark post-filtering and don't block aggregates." +
              stringPatternHint
          )
        }

        // DIRECT EXTRACTION: Extract IndexQuery expressions directly from the current logical plan
        val extractedIndexQueryFilters = extractIndexQueriesFromCurrentPlan()

        logger.debug(
          s"BUILD DEBUG: Extracted ${extractedIndexQueryFilters.length} IndexQuery filters directly from plan"
        )
        extractedIndexQueryFilters.foreach(filter => logger.debug(s"  - Extracted IndexQuery: $filter"))

        // CRITICAL: Validate IndexQuery field existence on driver before creating scan
        // This prevents task failures on executors when fields don't exist
        // Same pattern as PR #122's syntax validation - fail fast on driver
        validateIndexQueryFieldsExist()
        logger.debug(s"BUILD: IndexQuery field validation passed for regular scan")

        // Note: IS NULL/IS NOT NULL validation no longer needed here because isSupportedFilter
        // only returns true for these filters when the field is fast. Non-fast fields won't
        // have the filter pushed down - Spark will handle them after reading data.

        logger.debug(s"BUILD: Creating IndexTables4SparkScan on instance ${System.identityHashCode(this)} with ${effectiveFilters.length} pushed filters")
        effectiveFilters.foreach(filter => logger.debug(s"BUILD:   - Creating scan with filter: $filter"))
        new IndexTables4SparkScan(
          sparkSession,
          transactionLog,
          requiredSchema,
          effectiveFilters,
          options,
          _limit,
          effectiveConfig,
          extractedIndexQueryFilters
        )
    }

    // CRITICAL: Clear stored filters AND IndexQueries after scan is created to prevent filter pollution across queries
    // The relation-based storage is meant to share filters across optimization passes of the SAME query,
    // not to carry filters from one query to the next query on the same DataFrame
    relationForIndexQuery.foreach { relation =>
      IndexTables4SparkScanBuilder.clearPushedFilters(relation)
      IndexTables4SparkScanBuilder.clearIndexQueries(relation)
      logger.debug(s"BUILD: Cleared stored filters and IndexQueries for relation: ${System.identityHashCode(relation)}")
    }

    scan
  }

  /** Create an aggregate scan for pushed aggregations. */
  private def createAggregateScan(aggregation: Aggregation, effectiveFilters: Array[Filter]): Scan = {
    import io.indextables.spark.catalyst.V2BucketExpressionRule

    // Check for bucket aggregation first (DateHistogram, Histogram, Range)
    // Try instance variable first, then check V2BucketExpressionRule storage
    val effectiveBucketConfig: Option[BucketAggregationConfig] = _bucketConfig
      .orElse {
        import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
        val relationForBucket = IndexTables4SparkScanBuilder.getCurrentRelation()
        relationForBucket
          .collect { case rel: DataSourceV2Relation => rel }
          .flatMap(rel => V2BucketExpressionRule.getBucketConfig(rel))
      }
      .filter { bucketCfg =>
        // CRITICAL: Only use the bucket config if the bucket field is in the current query's GROUP BY
        // This prevents stale bucket configs from previous queries (stored in WeakHashMap) from
        // affecting non-bucket queries on the same view/relation (e.g., partition GROUP BY count(*))
        val groupByColumns       = _pushedGroupBy.getOrElse(Array.empty[String])
        val bucketFieldInGroupBy = groupByColumns.contains(bucketCfg.fieldName)
        if (!bucketFieldInGroupBy) {
          logger.debug(s"AGGREGATE SCAN: Ignoring stale bucket config for field '${bucketCfg.fieldName}' - not in GROUP BY columns: ${groupByColumns.mkString(", ")}")
        }
        bucketFieldInGroupBy
      }

    effectiveBucketConfig match {
      case Some(bucketCfg) =>
        logger.info(s"AGGREGATE SCAN: Creating bucket aggregation scan: ${bucketCfg.description}")
        createBucketAggregateScan(aggregation, bucketCfg, effectiveFilters)

      case None =>
        // Regular aggregation handling
        _pushedGroupBy match {
          case Some(groupByColumns) =>
            // GROUP BY aggregation
            logger.info(
              s"AGGREGATE SCAN: Creating GROUP BY aggregation scan for columns: ${groupByColumns.mkString(", ")}"
            )
            createGroupByAggregateScan(aggregation, groupByColumns, effectiveFilters)
          case None =>
            // Simple aggregation without GROUP BY
            // Check if we can use transaction log count optimization
            if (canUseTransactionLogCount(aggregation, effectiveFilters)) {
              logger.debug(s"AGGREGATE SCAN: Using transaction log count optimization")
              createTransactionLogCountScan(aggregation, effectiveFilters)
            } else {
              logger.debug(s"AGGREGATE SCAN: Creating simple aggregation scan")
              createSimpleAggregateScan(aggregation, effectiveFilters)
            }
        }
    }
  }

  /** Create a GROUP BY aggregation scan. */
  private def createGroupByAggregateScan(
    aggregation: Aggregation,
    groupByColumns: Array[String],
    effectiveFilters: Array[Filter]
  ): Scan = {
    val extractedIndexQueryFilters = extractIndexQueriesFromCurrentPlan()

    // Check if we can use transaction log optimization for partition-only GROUP BY COUNT
    if (canUseTransactionLogGroupByCount(aggregation, groupByColumns, effectiveFilters)) {
      val hasAggregations = aggregation.aggregateExpressions().nonEmpty
      logger.info(
        s"Using transaction log optimization for partition-only GROUP BY COUNT, hasAggregations=$hasAggregations"
      )
      new TransactionLogCountScan(
        sparkSession,
        transactionLog,
        effectiveFilters,
        options,
        effectiveConfig,
        Some(groupByColumns), // Pass GROUP BY columns for grouped aggregation
        hasAggregations,      // Indicate if this has aggregations or is just DISTINCT
        Some(schema)          // Pass table schema for proper type conversion
      )
    } else {
      // Regular GROUP BY scan using tantivy aggregations
      // Pass partition columns for optimization - avoid Tantivy aggregation on partition columns
      val partitionCols = getPartitionColumns()
      new IndexTables4SparkGroupByAggregateScan(
        sparkSession,
        transactionLog,
        schema,
        effectiveFilters,
        options,
        effectiveConfig,
        aggregation,
        groupByColumns,
        extractedIndexQueryFilters,
        bucketConfig = None,
        partitionColumns = partitionCols
      )
    }
  }

  /** Create a simple aggregation scan (no GROUP BY). */
  private def createSimpleAggregateScan(aggregation: Aggregation, effectiveFilters: Array[Filter]): Scan = {

    val extractedIndexQueryFilters = extractIndexQueriesFromCurrentPlan()
    new IndexTables4SparkSimpleAggregateScan(
      sparkSession,
      transactionLog,
      schema,
      effectiveFilters,
      options,
      effectiveConfig,
      aggregation,
      extractedIndexQueryFilters
    )
  }

  /** Create a bucket aggregation scan (DateHistogram, Histogram, Range). */
  private def createBucketAggregateScan(
    aggregation: Aggregation,
    bucketConfig: BucketAggregationConfig,
    effectiveFilters: Array[Filter]
  ): Scan = {
    val extractedIndexQueryFilters = extractIndexQueriesFromCurrentPlan()

    // Bucket aggregations use the GROUP BY scan with bucketConfig
    // The groupByColumns must include the bucket field PLUS any additional GROUP BY columns
    // Example: GROUP BY indextables_date_histogram(ts, '15m'), hostname
    //   -> groupByColumns = Array("ts", "hostname")
    //   -> bucketConfig applies to position 0 (ts)
    //
    // Multi-key bucket aggregations are supported using nested TermsAggregation:
    // - The bucket aggregation (DateHistogram/Histogram/Range) is the outer aggregation
    // - Additional GROUP BY columns use nested TermsAggregation as sub-aggregations
    // - Results are flattened: [bucket_key, term_key_1, term_key_2, ..., aggregation_values]
    val additionalGroupByColumns = _pushedGroupBy
      .getOrElse(Array.empty[String])
      .filterNot(_ == bucketConfig.fieldName) // Remove bucket field if already present
    val groupByColumns = Array(bucketConfig.fieldName) ++ additionalGroupByColumns

    logger.debug(s"BUCKET SCAN: Creating bucket aggregate scan for field '${bucketConfig.fieldName}' with config: ${bucketConfig.description}")
    logger.debug(s"BUCKET SCAN: All GROUP BY columns: ${groupByColumns.mkString(", ")}")
    if (additionalGroupByColumns.nonEmpty) {
      logger.debug(s"BUCKET SCAN: Additional GROUP BY columns will use nested TermsAggregation: ${additionalGroupByColumns.mkString(", ")}")
    }

    // Pass partition columns for optimization (bucket aggregations typically don't use partition columns in GROUP BY)
    val partitionCols = getPartitionColumns()
    new IndexTables4SparkGroupByAggregateScan(
      sparkSession,
      transactionLog,
      schema,
      effectiveFilters,
      options,
      effectiveConfig,
      aggregation,
      groupByColumns,
      extractedIndexQueryFilters,
      bucketConfig = Some(bucketConfig),
      partitionColumns = partitionCols
    )
  }

  /** Check if we can optimize COUNT queries using transaction log. */
  private def canUseTransactionLogCount(aggregation: Aggregation, effectiveFilters: Array[Filter]): Boolean = {
    import org.apache.spark.sql.connector.expressions.aggregate.{Count, CountStar}

    aggregation.aggregateExpressions.foreach(expr =>
      logger.debug(s"SCAN BUILDER: Aggregate expression: $expr (${expr.getClass.getSimpleName})")
    )
    logger.debug(s"SCAN BUILDER: Number of effective filters: ${effectiveFilters.length}")
    effectiveFilters.foreach(filter => logger.debug(s"SCAN BUILDER: Effective filter: $filter"))

    // Extract IndexQuery filters to check if we have any
    val indexQueryFilters = extractIndexQueriesFromCurrentPlan()
    logger.debug(s"SCAN BUILDER: Number of IndexQuery filters: ${indexQueryFilters.length}")

    // Transaction log optimization requires:
    // 1. Exactly ONE aggregate expression (no mixing COUNT with MAX, AVG, SUM, MIN)
    // 2. That expression must be COUNT or COUNT(*)
    // 3. Filters must be only on partition columns (or no filters)
    //
    // IMPORTANT: If there are multiple aggregates (e.g., SELECT COUNT(*), MAX(field)),
    // we CANNOT use transaction log for COUNT because:
    // - MAX/AVG/MIN/SUM require searching tantivy
    // - COUNT must count the SAME documents that MAX/AVG/MIN/SUM operate on
    // - Therefore, ALL aggregates must go to tantivy together
    val result = aggregation.aggregateExpressions.length == 1 && {
      aggregation.aggregateExpressions.head match {
        case _: Count =>
          // Transaction log optimization works when filters are only on partition columns
          // Range filters (>=, <=, <, >) on partition columns are valid because partition pruning
          // ensures all documents in a split have the same partition value
          val hasOnlyPartitionFilters = effectiveFilters.forall(isPartitionFilter) && indexQueryFilters.isEmpty
          logger.debug(s"SCAN BUILDER: COUNT - Can use transaction log optimization: $hasOnlyPartitionFilters (filters: ${effectiveFilters.mkString(", ")})")
          hasOnlyPartitionFilters
        case _: CountStar =>
          // Transaction log optimization works when filters are only on partition columns
          // Range filters (>=, <=, <, >) on partition columns are valid because partition pruning
          // ensures all documents in a split have the same partition value
          val hasOnlyPartitionFilters = effectiveFilters.forall(isPartitionFilter) && indexQueryFilters.isEmpty
          logger.debug(s"SCAN BUILDER: COUNT(*) - Can use transaction log optimization: $hasOnlyPartitionFilters (filters: ${effectiveFilters.mkString(", ")})")
          hasOnlyPartitionFilters
        case _ =>
          logger.debug(s"SCAN BUILDER: Not a COUNT aggregation, cannot use transaction log")
          false
      }
    }

    if (!result && aggregation.aggregateExpressions.length > 1) {
      logger.debug(s"SCAN BUILDER: Multiple aggregates present (${aggregation.aggregateExpressions.length}) - routing all aggregates to tantivy including COUNT")
    }

    logger.debug(s"SCAN BUILDER: canUseTransactionLogCount returning: $result")
    result
  }

  /** Check if we can optimize GROUP BY partition columns COUNT using transaction log. */
  private def canUseTransactionLogGroupByCount(
    aggregation: Aggregation,
    groupByColumns: Array[String],
    effectiveFilters: Array[Filter]
  ): Boolean = {
    import org.apache.spark.sql.connector.expressions.aggregate.{Count, CountStar}

    // Check 1: All GROUP BY columns must be partition columns
    val partitionColumns               = transactionLog.getPartitionColumns()
    val allGroupByColumnsArePartitions = groupByColumns.forall(partitionColumns.contains)

    if (!allGroupByColumnsArePartitions) {
      return false
    }

    // Check 2: Only COUNT aggregations are supported
    // IMPORTANT: If there are ANY non-COUNT aggregates (MAX, AVG, MIN, SUM),
    // we CANNOT use transaction log because:
    // - MAX/AVG/MIN/SUM require searching tantivy
    // - COUNT must count the SAME documents that MAX/AVG/MIN/SUM operate on
    // - Therefore, ALL aggregates must go to tantivy together
    val onlyCountAggregations = aggregation.aggregateExpressions.forall {
      case _: Count | _: CountStar => true
      case _                       => false
    }

    if (!onlyCountAggregations) {
      logger.debug(s"SCAN BUILDER: GROUP BY has non-COUNT aggregates - routing all aggregates to tantivy")
      return false
    }

    // Check 3: Only partition filters are allowed (or no filters), AND no IndexQuery filters
    val indexQueryFilters       = extractIndexQueriesFromCurrentPlan()
    val hasOnlyPartitionFilters = effectiveFilters.forall(isPartitionFilter) && indexQueryFilters.isEmpty

    if (!hasOnlyPartitionFilters) {
      return false
    }

    true
  }

  /** Create a specialized scan that returns count from transaction log. */
  private def createTransactionLogCountScan(aggregation: Aggregation, effectiveFilters: Array[Filter]): Scan =
    new TransactionLogCountScan(
      sparkSession,
      transactionLog,
      effectiveFilters,
      options,
      effectiveConfig,
      None,        // No GROUP BY columns for simple count
      true,        // hasAggregations
      Some(schema) // Pass table schema for proper type conversion
    )

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    logger.debug(
      s"PUSHFILTERS: pushFilters called on instance ${System.identityHashCode(this)} with ${filters.length} filters"
    )
    filters.foreach { filter =>
      logger.debug(s"PUSHFILTERS:   - Input filter: $filter (${filter.getClass.getSimpleName})")
    }

    // Since IndexQuery expressions are now handled directly by the V2IndexQueryExpressionRule,
    // we only need to handle regular Spark filters here.
    val (supported, unsupported) = filters.partition(isSupportedFilter)

    // NOTE: IsNull and IsNotNull are marked as "supported" for regular fields (not JSON fields)
    // in isSupportedFilter. The query converter uses wildcardQuery to properly filter these:
    // - IsNotNull(field) → wildcardQuery(field, "*") - matches docs where field has a value
    // - IsNull(field) → for now returns allQuery() (TODO: implement proper null handling)
    // This allows aggregate pushdown to work correctly with null/not-null filters.

    // Store filters in instance variables
    _pushedFilters = supported
    _unsupportedFilters = unsupported

    // CRITICAL FIX: Store by relation object (not table path) to survive across multiple optimization passes
    // Spark runs V2ScanRelationPushDown multiple times, creating fresh ScanBuilders each time
    // Only the builders that have Filter nodes get pushFilters() called
    // But the final ScanBuilder (which might not have pushFilters called) is the one executed
    // Different optimization passes may use different Table instances with different paths
    // Solution: Store filters by DataSourceV2Relation object (same approach as IndexQueries)
    // All ScanBuilders share the same relation object even across optimization passes
    IndexTables4SparkScanBuilder.getCurrentRelation() match {
      case Some(relation) =>
        IndexTables4SparkScanBuilder.storePushedFilters(relation, supported)
        IndexTables4SparkScanBuilder.storeUnsupportedFilters(relation, unsupported)
        logger.debug(
          s"PUSHFILTERS: Stored ${supported.length} supported, ${unsupported.length} unsupported filters by relation object: ${System
              .identityHashCode(relation)}"
        )
      case None =>
        logger.warn(s"PUSHFILTERS: No relation in ThreadLocal, cannot store filters for future ScanBuilder instances")
    }

    logger.debug(s"PUSHFILTERS: Supported=${supported.length}, Unsupported=${unsupported.length}")
    supported.foreach(filter => logger.debug(s"PUSHFILTERS:   ✓ SUPPORTED: $filter"))
    unsupported.foreach(filter => logger.debug(s"PUSHFILTERS:   ✗ UNSUPPORTED: $filter"))

    logger.info(s"Filter pushdown summary:")
    logger.info(s"  - ${supported.length} filters FULLY SUPPORTED by data source (will NOT be re-evaluated by Spark)")
    supported.foreach(filter => logger.info(s"    ✓ PUSHED: $filter"))

    logger.info(s"  - ${unsupported.length} filters NOT SUPPORTED (will be re-evaluated by Spark after reading)")
    unsupported.foreach(filter => logger.info(s"    ✗ NOT PUSHED: $filter"))

    // Return only unsupported filters - Spark will re-evaluate these after reading data
    unsupported
  }

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    logger.debug(s"PUSHPREDICATES DEBUG: pushPredicates called with ${predicates.length} predicates")
    predicates.foreach(predicate => logger.info(s"  - Input predicate: $predicate (${predicate.getClass.getSimpleName})"))

    // Convert predicates that we can handle and extract IndexQuery information
    val (supported, unsupported) = predicates.partition(isSupportedPredicate)

    // Store supported predicates - for now, just log them
    logger.info(s"Predicate pushdown summary:")
    logger.info(s"  - ${supported.length} predicates FULLY SUPPORTED by data source (will NOT be re-evaluated by Spark)")
    supported.foreach(predicate => logger.info(s"    ✓ PUSHED: $predicate"))

    logger.info(s"  - ${unsupported.length} predicates NOT SUPPORTED (will be re-evaluated by Spark after reading)")
    unsupported.foreach(predicate => logger.info(s"    ✗ NOT PUSHED: $predicate"))

    // Return only unsupported predicates - Spark will re-evaluate these
    unsupported
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def pushedPredicates(): Array[Predicate] = Array.empty // V2 interface method

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
    logger.info(s"Pruned columns to: ${requiredSchema.fieldNames.mkString(", ")}")
  }

  override def pushLimit(limit: Int): Boolean =
    // Databricks notebooks add LIMIT 10001 by default for display purposes.
    // Ignore this artificial limit to avoid affecting query performance.
    if (limit == 10001) {
      logger.info(s"Ignoring Databricks default display limit: $limit")
      false // Tell Spark we didn't accept this limit
    } else {
      _limit = Some(limit)
      logger.info(s"Pushed limit: $limit")
      true // We support limit pushdown
    }

  override def supportCompletePushDown(aggregation: Aggregation): Boolean = {
    // Return false to allow Spark to handle final aggregation combining partial results
    // This enables proper distributed aggregation where:
    // - AVG is transformed to SUM + COUNT by Spark
    // - Partial results from each partition are combined correctly
    logger.debug(s"AGGREGATE PUSHDOWN: supportCompletePushDown called - returning false for distributed aggregation")
    false
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    logger.debug(s"AGGREGATE PUSHDOWN: Received aggregation request: $aggregation")
    logger.debug(s"AGGREGATE PUSHDOWN: Number of pushed filters: ${_pushedFilters.length}")
    _pushedFilters.foreach(f => logger.debug(s"AGGREGATE PUSHDOWN: Pushed filter: $f"))

    // Check if this is a GROUP BY aggregation
    val groupByExpressions = aggregation.groupByExpressions()
    val hasGroupBy         = groupByExpressions != null && groupByExpressions.nonEmpty

    logger.debug(s"GROUP BY CHECK: hasGroupBy = $hasGroupBy")
    if (hasGroupBy) {
      logger.debug(s"GROUP BY DETECTED: Found ${groupByExpressions.length} GROUP BY expressions")
      groupByExpressions.foreach(expr => logger.debug(s"GROUP BY EXPRESSION: $expr (class: ${expr.getClass.getName})"))

      // FIRST: Check for bucket expressions (DateHistogram, Histogram, Range)
      // Bucket expressions come through as GROUP BY expressions
      val bucketConfig = detectBucketExpression(groupByExpressions)
      if (bucketConfig.isDefined) {
        logger.info(s"BUCKET AGGREGATION: Detected ${bucketConfig.get.description}")

        // Validate aggregations are compatible with bucket aggregations
        if (!areAggregationsCompatibleWithBucket(aggregation)) {
          logger.debug(s"BUCKET AGGREGATION: REJECTED - aggregations not compatible")
          return false
        }

        // Store bucket config and accept the aggregation
        _bucketConfig = bucketConfig
        _pushedAggregation = Some(aggregation)
        logger.info(s"BUCKET AGGREGATION: ✅ ACCEPTED - bucket aggregation will be pushed down")
        return true
      }

      // Not a bucket expression - proceed with regular GROUP BY handling
      // Extract GROUP BY column names
      val groupByColumns = groupByExpressions.map(extractFieldNameFromExpression)
      logger.debug(s"GROUP BY COLUMNS: ${groupByColumns.mkString(", ")}")

      // Validate GROUP BY columns are supported - throw exception if not
      logger.debug(s"GROUP BY VALIDATION: About to check areGroupByColumnsSupported")
      validateGroupByColumnsOrThrow(groupByColumns)
      logger.debug(s"GROUP BY VALIDATION: areGroupByColumnsSupported passed")

      // Check if aggregation is compatible with GROUP BY - throw exception if not
      logger.debug(s"GROUP BY VALIDATION: About to check isAggregationCompatibleWithGroupBy")
      validateAggregationCompatibilityOrThrow(aggregation)
      logger.debug(s"GROUP BY VALIDATION: isAggregationCompatibleWithGroupBy passed")

      // Store GROUP BY information
      _pushedGroupBy = Some(groupByColumns)
      logger.debug(s"GROUP BY PUSHDOWN: ACCEPTED - GROUP BY will be pushed down")
    } else {
      logger.debug(s"SIMPLE AGGREGATION: No GROUP BY expressions found")
    }

    // Validate aggregation is supported (both simple and GROUP BY)
    logger.debug(s"AGGREGATE PUSHDOWN: About to check isAggregationSupported")
    if (!isAggregationSupported(aggregation)) {
      logger.debug(s"AGGREGATE PUSHDOWN: REJECTED - aggregation not supported")
      return false
    }
    logger.debug(s"AGGREGATE PUSHDOWN: isAggregationSupported passed")

    // Check if filters are compatible with aggregate pushdown
    logger.debug(s"AGGREGATE PUSHDOWN: About to check areFiltersCompatibleWithAggregation")
    try {
      if (!areFiltersCompatibleWithAggregation()) {
        logger.debug(s"AGGREGATE PUSHDOWN: REJECTED - filters not compatible")
        return false
      }
      logger.debug(s"AGGREGATE PUSHDOWN: areFiltersCompatibleWithAggregation passed")

      // Validate IndexQuery filter fields exist in schema - throw exception if not
      // This prevents silent failures where queries with non-existent fields return all data
      validateIndexQueryFieldsExist()
      logger.debug(s"AGGREGATE PUSHDOWN: IndexQuery field validation passed")

      // Note: IS NULL/IS NOT NULL validation no longer needed here because isSupportedFilter
      // only returns true for these filters when the field is fast.
    } catch {
      case e: IllegalArgumentException if e.getMessage.contains("IndexQuery references non-existent") =>
        // Rethrow IndexQuery field validation errors - these should fail the query
        logger.error(s"AGGREGATE PUSHDOWN: IndexQuery field validation error - ${e.getMessage}")
        throw e
      case e: IllegalArgumentException if e.getMessage.contains("filter on field") =>
        // Rethrow IS NULL/IS NOT NULL validation errors - these should fail the query
        logger.error(s"AGGREGATE PUSHDOWN: Null filter validation error - ${e.getMessage}")
        throw e
      case e: IllegalArgumentException if e.getMessage.contains("filter references non-existent") =>
        // Rethrow field existence errors - these should fail the query
        logger.error(s"AGGREGATE PUSHDOWN: Null filter field not found - ${e.getMessage}")
        throw e
      case e: IllegalArgumentException =>
        logger.debug(s"AGGREGATE PUSHDOWN: REJECTED - ${e.getMessage}")
        return false
    }

    // Store for later use in build()
    _pushedAggregation = Some(aggregation)
    logger.debug(s"AGGREGATE PUSHDOWN: ✅ ACCEPTED - aggregation will be pushed down")
    logger.debug(s"AGGREGATE PUSHDOWN: Returning true")
    true
  }

  /** Extract field name from Spark expression for GROUP BY detection. */
  private def extractFieldNameFromExpression(expression: org.apache.spark.sql.connector.expressions.Expression)
    : String = {
    // Use toString and try to extract field name
    val exprStr = expression.toString
    logger.debug(s"FIELD EXTRACTION: Expression string: '$exprStr'")
    logger.debug(s"FIELD EXTRACTION: Expression class: ${expression.getClass.getSimpleName}")

    // Check if it's a FieldReference by class name
    if (expression.getClass.getSimpleName == "FieldReference") {
      // For FieldReference, toString() returns the field name directly
      val fieldName = exprStr
      logger.debug(s"FIELD EXTRACTION: Extracted field name from FieldReference: '$fieldName'")
      fieldName
    } else if (exprStr.startsWith("FieldReference(")) {
      // Fallback for other FieldReference string formats
      val pattern = """FieldReference\(([^)]+)\)""".r
      pattern.findFirstMatchIn(exprStr) match {
        case Some(m) =>
          val fieldName = m.group(1)
          logger.debug(s"FIELD EXTRACTION: Extracted field name from pattern: '$fieldName'")
          fieldName
        case None =>
          logger.debug(s"FIELD EXTRACTION: Could not extract field name from expression: $expression")
          logger.warn(s"Could not extract field name from expression: $expression")
          "unknown_field"
      }
    } else {
      logger.debug(s"FIELD EXTRACTION: Unsupported expression type for field extraction: $expression")
      logger.warn(s"Unsupported expression type for field extraction: $expression")
      "unknown_field"
    }
  }

  // Configuration helpers for string pattern filter pushdown
  // Helper to get config value from both options (reader options) and config (session config)
  private def getConfigValue(key: String): Option[String] =
    // First check reader options, then session config
    Option(options.get(key)).orElse(config.get(key))

  // Master switch to enable all string pattern pushdowns at once
  private def isAllStringPatternPushdownEnabled: Boolean =
    getConfigValue("spark.indextables.filter.stringPattern.pushdown")
      .map(_.toLowerCase == "true")
      .getOrElse(false)

  private def isStringContainsPushdownEnabled: Boolean =
    isAllStringPatternPushdownEnabled ||
      getConfigValue("spark.indextables.filter.stringContains.pushdown")
        .map(_.toLowerCase == "true")
        .getOrElse(false)

  private def isStringStartsWithPushdownEnabled: Boolean =
    isAllStringPatternPushdownEnabled ||
      getConfigValue("spark.indextables.filter.stringStartsWith.pushdown")
        .map(_.toLowerCase == "true")
        .getOrElse(false)

  private def isStringEndsWithPushdownEnabled: Boolean =
    isAllStringPatternPushdownEnabled ||
      getConfigValue("spark.indextables.filter.stringEndsWith.pushdown")
        .map(_.toLowerCase == "true")
        .getOrElse(false)

  private def isSupportedFilter(filter: Filter): Boolean = {
    import org.apache.spark.sql.sources._

    // Check if a field is fast (for IsNull/IsNotNull support)
    def isFieldFast(fieldName: String): Boolean = {
      val fastFields = getActualFastFieldsFromSchema()
      // Check direct match or parent field for nested JSON fields
      if (fieldName.contains(".")) {
        val parentField = fieldName.substring(0, fieldName.lastIndexOf('.'))
        fastFields.contains(fieldName) || fastFields.contains(parentField)
      } else {
        fastFields.contains(fieldName)
      }
    }

    filter match {
      case EqualTo(attribute, _)            => isFieldSuitableForExactMatching(attribute)
      case EqualNullSafe(attribute, _)      => isFieldSuitableForExactMatching(attribute)
      case GreaterThan(attribute, _)        => true // Support range on all fields (both regular and JSON)
      case GreaterThanOrEqual(attribute, _) => true // Support range on all fields (both regular and JSON)
      case LessThan(attribute, _)           => true // Support range on all fields (both regular and JSON)
      case LessThanOrEqual(attribute, _)    => true // Support range on all fields (both regular and JSON)
      case _: In                            => true
      // IsNull/IsNotNull only supported when field is fast (ExistsQuery requires FAST field)
      // If field is not fast, filter will be handled by Spark instead (not pushed down)
      case IsNull(attribute)    => isFieldFast(attribute)
      case IsNotNull(attribute) => isFieldFast(attribute)
      case And(left, right)     => isSupportedFilter(left) && isSupportedFilter(right)
      case Or(left, right)      => isSupportedFilter(left) && isSupportedFilter(right)
      case Not(child)           => isSupportedFilter(child) // NOT is supported only if child is supported
      case _: StringStartsWith => isStringStartsWithPushdownEnabled // Enabled via config, Tantivy prefix queries are efficient
      case _: StringEndsWith => isStringEndsWithPushdownEnabled // Enabled via config, less efficient than prefix
      case _: StringContains => isStringContainsPushdownEnabled // Enabled via config, least efficient (full scan)
      case _                 => false
    }
  }

  private def isSupportedPredicate(predicate: Predicate): Boolean = {
    // For V2 predicates, we need to inspect the actual predicate type
    // For now, let's accept all predicates and see what we get
    logger.debug(s"isSupportedPredicate: Checking predicate $predicate")

    // TODO: Implement proper predicate type checking based on Spark's V2 Predicate types
    true // Accept all for now to see what comes through
  }

  /**
   * Check if a field is suitable for exact matching at the data source level. String fields (raw tokenizer) support
   * exact matching. Text fields (default tokenizer) should be filtered by Spark for exact matches. Nested JSON fields
   * (containing dots) are always supported for pushdown via JsonPredicateTranslator.
   */
  private def isFieldSuitableForExactMatching(attribute: String): Boolean = {
    // Check if this is a nested field (JSON field)
    if (attribute.contains(".")) {
      logger.debug(s"Field '$attribute' is a nested JSON field - supporting pushdown via JsonPredicateTranslator")
      return true
    }

    // Check the field type configuration for top-level fields.
    // Use effectiveConfig (not raw config) so companion indexingModes are visible.
    // Use lowercase attribute: CaseInsensitiveStringMap lowercases keys, so config has
    // "spark.indextables.indexing.typemap.myfield" even if user wrote "typemap.myField".
    val fieldTypeKey = s"spark.indextables.indexing.typemap.${attribute.toLowerCase}"
    val fieldType    = effectiveConfig.get(fieldTypeKey)

    fieldType match {
      case Some("string") =>
        logger.debug(s"Field '$attribute' configured as 'string' - supporting exact matching")
        true
      case Some("text") =>
        logger.debug(s"Field '$attribute' configured as 'text' - deferring exact matching to Spark")
        false
      case Some("ip") =>
        logger.debug(s"Field '$attribute' configured as 'ip' - supporting exact matching")
        true
      case Some(other) =>
        logger.debug(s"Field '$attribute' configured as '$other' - supporting exact matching")
        true
      case None =>
        // No explicit configuration - assume string type (new default)
        logger.debug(s"Field '$attribute' has no type configuration - assuming 'string', supporting exact matching")
        true
    }
  }

  /**
   * Detect if the current query plan contains an Aggregate operator. This is used to fail fast when aggregate pushdown
   * is blocked by unsupported filters.
   *
   * We inspect the query execution context to find Aggregate nodes in the logical plan.
   */
  private def detectAggregateInQueryPlan(): Boolean = {
    // Detect aggregation by examining the required schema
    // When an aggregate like COUNT(*) is used, the schema will have aggregate-like column names
    // or the schema will be radically different from the original table schema

    val aggregatePatterns = Seq("count(", "sum(", "avg(", "min(", "max(", "count_")

    // Check if required schema column names look like aggregate results
    val schemaLooksLikeAggregate = requiredSchema.fieldNames.exists { name =>
      val lowerName = name.toLowerCase
      aggregatePatterns.exists(lowerName.contains)
    }

    if (schemaLooksLikeAggregate) {
      logger.debug(
        s"Detected aggregate in query plan via schema inspection: ${requiredSchema.fieldNames.mkString(", ")}"
      )
      return true
    }

    // Fallback: check if the required schema is empty (COUNT(*) case)
    // or has significantly fewer columns than the original schema
    if (requiredSchema.isEmpty || (schema.length > 2 && requiredSchema.length == 1)) {
      // Could be an aggregate query - be conservative and assume yes
      logger.debug(s"Schema suggests possible aggregate query: original=${schema.length} cols, required=${requiredSchema.length} cols")
      return true
    }

    // Try string matching on relation as last resort
    try
      IndexTables4SparkScanBuilder.getCurrentRelation() match {
        case Some(relation) =>
          val planStr = relation.toString.toLowerCase
          val hasAgg = planStr.contains("aggregate") ||
            aggregatePatterns.exists(planStr.contains)
          if (hasAgg) {
            logger.debug("Detected aggregate via string matching on relation")
          }
          hasAgg
        case None =>
          false
      }
    catch {
      case _: Exception => false
    }
  }

  /**
   * Extract IndexQuery expressions directly using the companion object storage. This eliminates the need for global
   * registry by using instance-scoped storage.
   */
  private def extractIndexQueriesFromCurrentPlan(): Array[Any] = {
    logger.debug(s"EXTRACT DEBUG: Starting direct IndexQuery extraction")

    // Look up relation from ThreadLocal at usage time (not at construction time!)
    val relationForIndexQuery = IndexTables4SparkScanBuilder.getCurrentRelation()

    // Method 1: Get IndexQueries stored by V2IndexQueryExpressionRule for this relation object
    relationForIndexQuery match {
      case Some(relation) =>
        val storedQueries = IndexTables4SparkScanBuilder.getIndexQueries(relation)
        if (storedQueries.nonEmpty) {
          logger.debug(s"EXTRACT DEBUG: Found ${storedQueries.length} IndexQuery filters from relation storage")
          storedQueries.foreach(q => logger.debug(s"  - Relation IndexQuery: $q"))
          val result = storedQueries.toArray
          // CRITICAL: Validate IndexQuery syntax on driver before tasks are created
          // Clear cache on failure to prevent stale errors from persisting to future queries
          try
            validateIndexQueryFilters(result)
          catch {
            case e: Exception =>
              IndexTables4SparkScanBuilder.clearIndexQueries(relation)
              throw e
          }
          return result
        }
      case None =>
        logger.debug(s"EXTRACT DEBUG: No relation object available from ThreadLocal")
    }

    // Method 2: Fall back to registry (temporary until we fully eliminate it)
    import io.indextables.spark.filters.IndexQueryRegistry
    IndexQueryRegistry.getCurrentQueryId() match {
      case Some(queryId) =>
        val registryQueries = IndexQueryRegistry.getIndexQueriesForQuery(queryId)
        if (registryQueries.nonEmpty) {
          logger.debug(s"EXTRACT DEBUG: Found ${registryQueries.length} IndexQuery filters from registry as fallback")
          registryQueries.foreach(q => logger.debug(s"  - Registry IndexQuery: $q"))
          val result = registryQueries.toArray
          // CRITICAL: Validate IndexQuery syntax on driver before tasks are created
          validateIndexQueryFilters(result)
          return result
        }
      case None =>
        logger.debug(s"EXTRACT DEBUG: No query ID available in registry")
    }

    logger.debug(s"EXTRACT DEBUG: No IndexQuery filters found using any method")
    Array.empty[Any]
  }

  /**
   * Validate IndexQuery filters on the driver using tantivy's query parser. This prevents invalid queries from causing
   * task failures and retries. Throws IndexQueryParseException immediately if a query has invalid syntax.
   */
  private def validateIndexQueryFilters(indexQueryFilters: Array[Any]): Unit = {
    if (indexQueryFilters.isEmpty) return

    logger.debug(s"VALIDATE: Validating ${indexQueryFilters.length} IndexQuery filters on driver")

    // Create a minimal tantivy schema for query validation
    // We only need the field names and types for parseQuery to validate syntax
    val tantivySchema =
      try
        createTantivySchemaForValidation()
      catch {
        case e: Exception =>
          logger.warn(s"VALIDATE: Could not create tantivy schema for validation: ${e.getMessage}")
          return // Skip validation if we can't create a schema
      }

    try {
      indexQueryFilters.foreach {
        case filter: IndexQueryFilter =>
          validateSingleQuery(filter.queryString, Some(filter.columnName), tantivySchema)
        case filter: IndexQueryAllFilter =>
          validateSingleQuery(filter.queryString, None, tantivySchema)
        // Handle wrapped filter types from MixedBoolean tree (PR #120)
        case wrapped: MixedIndexQuery =>
          validateSingleQuery(wrapped.filter.queryString, Some(wrapped.filter.columnName), tantivySchema)
        case wrapped: MixedIndexQueryAll =>
          validateSingleQuery(wrapped.filter.queryString, None, tantivySchema)
        // Handle MixedBooleanFilter tree structures (AND, OR, NOT combinations)
        // These are created when IndexQuery is combined with Spark filters
        case tree: MixedAndFilter =>
          validateMixedBooleanTree(tree, tantivySchema)
        case tree: MixedOrFilter =>
          validateMixedBooleanTree(tree, tantivySchema)
        case tree: MixedNotFilter =>
          validateMixedBooleanTree(tree, tantivySchema)
        case _: MixedSparkFilter =>
          // Spark filters don't contain IndexQuery - skip
          ()
        case other =>
          logger.debug(s"VALIDATE: Skipping unknown filter type: ${other.getClass.getName}")
      }
      logger.debug(s"VALIDATE: All ${indexQueryFilters.length} IndexQuery filters passed validation")
    } finally
      tantivySchema.close()
  }

  /**
   * Validate a single query string using tantivy's parseQuery. Throws IndexQueryParseException if the query has invalid
   * syntax.
   *
   * tantivy4java's SplitQuery.parseQuery() throws descriptive exceptions for parse failures, which we wrap in
   * IndexQueryParseException for user-friendly error messages.
   */
  private def validateSingleQuery(
    queryString: String,
    fieldName: Option[String],
    tantivySchema: io.indextables.tantivy4java.core.Schema
  ): Unit = {
    import io.indextables.tantivy4java.split.SplitQuery
    import scala.jdk.CollectionConverters._

    try {
      val defaultFields = fieldName match {
        case Some(field) => Array(field)
        case None        => tantivySchema.getFieldNames.asScala.toArray
      }

      // Transform leading wildcard queries (e.g., *Configuration) to use explicit field syntax
      // (e.g., fieldname:*Configuration) which Tantivy's wildcard query builder supports.
      // This must match the transformation applied during query execution.
      val queryToValidate = fieldName match {
        case Some(field) =>
          FiltersToQueryConverter.transformLeadingWildcardQuery(queryString, field)
        case None =>
          queryString // Can't transform without a specific field
      }

      // Use tantivy's parseQuery to validate syntax
      // tantivy4java throws descriptive exceptions on parse failures
      SplitQuery.parseQuery(queryToValidate, tantivySchema, defaultFields)
      logger.debug(
        s"VALIDATE: Query '$queryString'" +
          (if (queryToValidate != queryString) s" (transformed to '$queryToValidate')" else "") +
          " validated successfully"
      )
    } catch {
      case e: IndexQueryParseException =>
        throw e // Re-throw our own exceptions
      case e: Exception =>
        // Wrap tantivy exceptions in IndexQueryParseException
        // The exception message from tantivy4java contains descriptive parse error details
        throw fieldName match {
          case Some(field) => IndexQueryParseException.forField(queryString, field, e)
          case None        => IndexQueryParseException.forAllFields(queryString, e)
        }
    }
  }

  /**
   * Recursively validate all IndexQuery filters within a MixedBooleanFilter tree. This handles cases where IndexQuery
   * is combined with Spark filters via AND/OR/NOT.
   */
  private def validateMixedBooleanTree(
    tree: MixedBooleanFilter,
    tantivySchema: io.indextables.tantivy4java.core.Schema
  ): Unit =
    tree match {
      case MixedIndexQuery(filter) =>
        validateSingleQuery(filter.queryString, Some(filter.columnName), tantivySchema)
      case MixedIndexQueryAll(filter) =>
        validateSingleQuery(filter.queryString, None, tantivySchema)
      case MixedAndFilter(left, right) =>
        validateMixedBooleanTree(left, tantivySchema)
        validateMixedBooleanTree(right, tantivySchema)
      case MixedOrFilter(left, right) =>
        validateMixedBooleanTree(left, tantivySchema)
        validateMixedBooleanTree(right, tantivySchema)
      case MixedNotFilter(child) =>
        validateMixedBooleanTree(child, tantivySchema)
      case _: MixedSparkFilter =>
        // Spark filters don't contain IndexQuery - nothing to validate
        ()
    }

  /**
   * Create a tantivy Schema from the docMappingJson in the transaction log. Uses tantivy4java's native
   * Schema.fromDocMappingJson() for accurate field configuration.
   */
  private def createTantivySchemaForValidation(): io.indextables.tantivy4java.core.Schema = {
    import io.indextables.tantivy4java.core.Schema

    val files = transactionLog.listFiles()
    val docMappingJson = files.headOption.flatMap(_.docMappingJson).getOrElse {
      throw new IllegalStateException(
        "No docMappingJson available in transaction log. Cannot validate IndexQuery syntax."
      )
    }

    Schema.fromDocMappingJson(docMappingJson)
  }

  /**
   * Detect bucket aggregation expressions in GROUP BY clauses. Returns configuration for DateHistogram, Histogram, or
   * Range aggregations.
   */
  private def detectBucketExpression(
    groupByExpressions: Array[org.apache.spark.sql.connector.expressions.Expression]
  ): Option[BucketAggregationConfig] = {
    // We only support a single bucket expression in GROUP BY
    if (groupByExpressions.length != 1) {
      logger.debug(
        s"BUCKET DETECTION: Multiple GROUP BY expressions (${groupByExpressions.length}), not a bucket aggregation"
      )
      return None
    }

    val expr      = groupByExpressions(0)
    val exprStr   = expr.toString
    val exprClass = expr.getClass.getName

    logger.debug(s"BUCKET DETECTION: Checking expression: $exprStr (class: $exprClass)")

    // The V2 expression wraps our Catalyst expressions
    // We need to look for our bucket function patterns in the expression string
    // or use reflection to access the underlying Catalyst expression

    // Pattern 1: Check for function call patterns in string representation
    if (exprStr.contains("indextables_date_histogram") || exprStr.contains("DateHistogramExpression")) {
      logger.debug(s"BUCKET DETECTION: Found date histogram pattern")
      return extractDateHistogramConfig(expr)
    }

    if (exprStr.contains("indextables_histogram") || exprStr.contains("HistogramExpression")) {
      logger.debug(s"BUCKET DETECTION: Found histogram pattern")
      return extractHistogramConfig(expr)
    }

    if (exprStr.contains("indextables_range") || exprStr.contains("RangeExpression")) {
      logger.debug(s"BUCKET DETECTION: Found range pattern")
      return extractRangeConfig(expr)
    }

    // Pattern 2: Try to access the underlying Catalyst expression via reflection
    try {
      val catalystExpr = extractCatalystExpression(expr)
      catalystExpr match {
        case Some(dh: DateHistogramExpression) =>
          logger.debug(s"BUCKET DETECTION: Found DateHistogramExpression via reflection")
          return Some(BucketAggregationConfig.fromExpression(dh))

        case Some(h: HistogramExpression) =>
          logger.debug(s"BUCKET DETECTION: Found HistogramExpression via reflection")
          return Some(BucketAggregationConfig.fromExpression(h))

        case Some(r: RangeExpression) =>
          logger.debug(s"BUCKET DETECTION: Found RangeExpression via reflection")
          return Some(BucketAggregationConfig.fromExpression(r))

        case _ =>
          logger.debug(s"BUCKET DETECTION: No bucket expression found via reflection")
      }
    } catch {
      case e: Exception =>
        logger.debug(s"BUCKET DETECTION: Exception during reflection: ${e.getMessage}")
    }

    logger.debug(s"BUCKET DETECTION: No bucket expression detected")
    None
  }

  /** Extract the underlying Catalyst expression from a V2 expression. */
  private def extractCatalystExpression(
    v2Expr: org.apache.spark.sql.connector.expressions.Expression
  ): Option[org.apache.spark.sql.catalyst.expressions.Expression] =
    try {
      // V2 expressions often wrap Catalyst expressions
      // Try common field names used in V2 wrappers
      val clazz = v2Expr.getClass

      // Try 'expr' field (common in V2Aggregation wrappers)
      val exprField =
        try
          Some(clazz.getDeclaredField("expr"))
        catch {
          case _: NoSuchFieldException =>
            try
              Some(clazz.getDeclaredField("expression"))
            catch {
              case _: NoSuchFieldException => None
            }
        }

      exprField.flatMap { field =>
        field.setAccessible(true)
        field.get(v2Expr) match {
          case catalyst: org.apache.spark.sql.catalyst.expressions.Expression => Some(catalyst)
          case _                                                              => None
        }
      }
    } catch {
      case _: Exception => None
    }

  /**
   * Extract DateHistogramConfig from expression string representation. Parses patterns like:
   * DateHistogramExpression(timestamp, 1h, ...)
   */
  private def extractDateHistogramConfig(
    expr: org.apache.spark.sql.connector.expressions.Expression
  ): Option[DateHistogramConfig] = {
    val exprStr = expr.toString
    logger.debug(s"BUCKET EXTRACTION: Parsing date histogram from: $exprStr")

    // Pattern: DateHistogramExpression(field, interval, ...)
    // or indextables_date_histogram(field, 'interval', ...)
    val pattern1 = """DateHistogramExpression\(([^,]+),\s*([^,\)]+)""".r
    val pattern2 = """indextables_date_histogram\(([^,]+),\s*'([^']+)'""".r

    val matched = pattern1.findFirstMatchIn(exprStr).orElse(pattern2.findFirstMatchIn(exprStr))

    matched.map { m =>
      val fieldName = m.group(1).trim
      val interval  = m.group(2).trim.replaceAll("'", "")

      logger.debug(s"BUCKET EXTRACTION: Extracted date histogram - field=$fieldName, interval=$interval")

      DateHistogramConfig(
        fieldName = fieldName,
        interval = interval
      )
    }
  }

  /** Extract HistogramConfig from expression string representation. */
  private def extractHistogramConfig(
    expr: org.apache.spark.sql.connector.expressions.Expression
  ): Option[HistogramConfig] = {
    val exprStr = expr.toString
    logger.debug(s"BUCKET EXTRACTION: Parsing histogram from: $exprStr")

    // Pattern: HistogramExpression(field, interval, ...)
    val pattern1 = """HistogramExpression\(([^,]+),\s*([0-9.]+)""".r
    val pattern2 = """indextables_histogram\(([^,]+),\s*([0-9.]+)""".r

    val matched = pattern1.findFirstMatchIn(exprStr).orElse(pattern2.findFirstMatchIn(exprStr))

    matched.map { m =>
      val fieldName = m.group(1).trim
      val interval  = m.group(2).trim.toDouble

      logger.debug(s"BUCKET EXTRACTION: Extracted histogram - field=$fieldName, interval=$interval")

      HistogramConfig(
        fieldName = fieldName,
        interval = interval
      )
    }
  }

  /** Extract RangeConfig from expression string representation. */
  private def extractRangeConfig(
    expr: org.apache.spark.sql.connector.expressions.Expression
  ): Option[RangeConfig] = {
    val exprStr = expr.toString
    logger.debug(s"BUCKET EXTRACTION: Parsing range from: $exprStr")

    // For now, basic extraction - more complex parsing would require full expression tree access
    // Pattern: RangeExpression(field, [ranges...])
    val fieldPattern = """RangeExpression\(([^,]+),""".r
    val fieldMatch   = fieldPattern.findFirstMatchIn(exprStr)

    fieldMatch.map { m =>
      val fieldName = m.group(1).trim

      // Extract ranges from the expression string
      val rangePattern = """(\w+):\[([^,]*),([^\)]*)\)""".r
      val ranges = rangePattern
        .findAllMatchIn(exprStr)
        .map { rm =>
          val key = rm.group(1)
          val from = rm.group(2).trim match {
            case "*" | "" => None
            case v        => Some(v.toDouble)
          }
          val to = rm.group(3).trim match {
            case "*" | "" => None
            case v        => Some(v.toDouble)
          }
          RangeBucket(key, from, to)
        }
        .toSeq

      if (ranges.isEmpty) {
        // Fallback: create a simple range config with the field name
        logger.debug(s"BUCKET EXTRACTION: Could not parse ranges, creating placeholder config for field=$fieldName")
        RangeConfig(fieldName = fieldName, ranges = Seq(RangeBucket("default", None, None)))
      } else {
        logger.debug(
          s"BUCKET EXTRACTION: Extracted range - field=$fieldName, ranges=${ranges.map(_.toString).mkString(", ")}"
        )
        RangeConfig(fieldName = fieldName, ranges = ranges)
      }
    }
  }

  /**
   * Check if aggregations are compatible with bucket aggregations. Supports: COUNT, COUNT(*), SUM, AVG (as SUM+COUNT),
   * MIN, MAX
   */
  private def areAggregationsCompatibleWithBucket(aggregation: Aggregation): Boolean = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    val result = aggregation.aggregateExpressions.forall { expr =>
      expr match {
        case _: Count | _: CountStar | _: Sum | _: Avg | _: Min | _: Max =>
          logger.debug(s"BUCKET COMPATIBILITY: ${expr.getClass.getSimpleName} is compatible")
          true
        case other =>
          logger.debug(s"BUCKET COMPATIBILITY: ${other.getClass.getSimpleName} is NOT compatible")
          false
      }
    }

    logger.debug(s"BUCKET COMPATIBILITY: Overall result = $result")
    result
  }

  /** Check if the aggregation is supported for pushdown. */
  private def isAggregationSupported(aggregation: Aggregation): Boolean = {
    import org.apache.spark.sql.connector.expressions.aggregate.{Count, CountStar, Sum, Avg, Min, Max}

    logger.debug(s"AGGREGATE VALIDATION: Checking ${aggregation.aggregateExpressions.length} aggregate expressions")
    aggregation.aggregateExpressions.zipWithIndex.foreach {
      case (expr, index) =>
        logger.debug(s"AGGREGATE VALIDATION: Expression $index: $expr (${expr.getClass.getSimpleName})")
    }

    val result = aggregation.aggregateExpressions.forall { expr =>
      val isSupported = expr match {
        case _: Count =>
          logger.debug(s"AGGREGATE VALIDATION: COUNT aggregation is supported")
          true
        case _: CountStar =>
          logger.debug(s"AGGREGATE VALIDATION: COUNT(*) aggregation is supported")
          true
        case sum: Sum =>
          val fieldName   = getFieldName(sum.column)
          val isSupported = isNumericFastField(fieldName)
          logger.debug(s"AGGREGATE VALIDATION: SUM on field '$fieldName' supported: $isSupported")
          isSupported
        case avg: Avg =>
          val fieldName   = getFieldName(avg.column)
          val isSupported = isNumericFastField(fieldName)
          logger.debug(s"AGGREGATE VALIDATION: AVG on field '$fieldName' supported: $isSupported")
          isSupported
        case min: Min =>
          val fieldName   = getFieldName(min.column)
          val isSupported = isNumericFastField(fieldName)
          logger.debug(s"AGGREGATE VALIDATION: MIN on field '$fieldName' supported: $isSupported")
          isSupported
        case max: Max =>
          val fieldName   = getFieldName(max.column)
          val isSupported = isNumericFastField(fieldName)
          logger.debug(s"AGGREGATE VALIDATION: MAX on field '$fieldName' supported: $isSupported")
          isSupported
        case other =>
          logger.debug(s"AGGREGATE VALIDATION: Unsupported aggregation type: ${other.getClass.getSimpleName}")
          false
      }
      logger.debug(s"AGGREGATE VALIDATION: Expression $expr supported: $isSupported")
      isSupported
    }
    logger.debug(s"AGGREGATE VALIDATION: Overall aggregation supported: $result")
    result
  }

  /** Extract field name from an aggregate expression column. */
  private def getFieldName(column: org.apache.spark.sql.connector.expressions.Expression): String = {
    // Use existing extractFieldNameFromExpression method
    val fieldName = extractFieldNameFromExpression(column)
    if (fieldName == "unknown_field") {
      throw new UnsupportedOperationException(s"Complex column expressions not supported for aggregation: $column")
    }
    fieldName
  }

  /** Check if a field is numeric and marked as fast in the schema. */
  private def isNumericFastField(fieldName: String): Boolean = {
    // Get actual fast fields from the schema/docMappingJson
    val fastFields = getActualFastFieldsFromSchema()

    // For nested JSON fields (containing dots), check if the field itself OR its parent struct is marked as fast
    if (fieldName.contains(".")) {
      val parentField  = fieldName.substring(0, fieldName.lastIndexOf('.'))
      val isFieldFast  = fastFields.contains(fieldName)
      val isParentFast = fastFields.contains(parentField)

      if (isFieldFast || isParentFast) {
        logger.debug(s"FAST FIELD VALIDATION: ✓ Nested field '$fieldName' accepted (field_fast=$isFieldFast, parent_fast=$isParentFast, parent='$parentField')")
        return true
      } else {
        logger.debug(s"FAST FIELD VALIDATION: ✗ Neither '$fieldName' nor parent '$parentField' marked as fast (available: ${fastFields.mkString(", ")})")
        return false
      }
    }

    // For top-level fields, check directly
    if (!fastFields.contains(fieldName)) {
      logger.debug(
        s"FAST FIELD VALIDATION: Field '$fieldName' is not marked as fast in schema (available fast fields: ${fastFields
            .mkString(", ")})"
      )
      return false
    }

    // Check if top-level field is numeric
    schema.fields.find(_.name == fieldName) match {
      case Some(field) if isNumericType(field.dataType) =>
        logger.debug(s"FAST FIELD VALIDATION: Field '$fieldName' is numeric and fast - supported")
        true
      case Some(field) =>
        logger.debug(s"FAST FIELD VALIDATION: Field '$fieldName' is not numeric (${field.dataType}) - not supported")
        false
      case None =>
        logger.debug(s"FAST FIELD VALIDATION: Field '$fieldName' not found in schema - not supported")
        false
    }
  }

  /**
   * Check if a field is marked as fast in the schema (without numeric type requirement). Used for MIN/MAX which can
   * operate on both numeric and string fast fields.
   */
  private def isFastField(fieldName: String): Boolean = {
    val fastFields = getActualFastFieldsFromSchema()

    // For nested JSON fields (containing dots), check if the field itself OR its parent struct is marked as fast
    if (fieldName.contains(".")) {
      val parentField  = fieldName.substring(0, fieldName.lastIndexOf('.'))
      val isFieldFast  = fastFields.contains(fieldName)
      val isParentFast = fastFields.contains(parentField)

      if (isFieldFast || isParentFast) {
        logger.debug(s"FAST FIELD VALIDATION: ✓ Nested field '$fieldName' is fast (field_fast=$isFieldFast, parent_fast=$isParentFast)")
        return true
      } else {
        logger.debug(s"FAST FIELD VALIDATION: ✗ Neither '$fieldName' nor parent '$parentField' marked as fast")
        return false
      }
    }

    // For top-level fields, check directly
    if (fastFields.contains(fieldName)) {
      logger.debug(s"FAST FIELD VALIDATION: ✓ Field '$fieldName' is marked as fast")
      true
    } else {
      logger.debug(
        s"FAST FIELD VALIDATION: ✗ Field '$fieldName' is not marked as fast (available: ${fastFields.mkString(", ")})"
      )
      false
    }
  }

  /** Check if a DataType is numeric. */
  private def isNumericType(dataType: org.apache.spark.sql.types.DataType): Boolean = {
    import org.apache.spark.sql.types._
    dataType match {
      case _: IntegerType | _: LongType | _: FloatType | _: DoubleType | _: DecimalType => true
      case _                                                                            => false
    }
  }

  /**
   * Check if current filters are compatible with aggregate pushdown. This validates fast field configuration and throws
   * exceptions for validation failures.
   */
  private def areFiltersCompatibleWithAggregation(): Boolean = {
    // If there are no filters, aggregation is compatible
    if (_pushedFilters.isEmpty) {
      return true
    }

    // Check if filter types are supported and if filter fields are fast fields
    _pushedFilters.foreach { filter =>
      val isFilterTypeSupported = filter match {
        // Supported filter types
        case _: org.apache.spark.sql.sources.EqualTo            => true
        case _: org.apache.spark.sql.sources.GreaterThan        => true
        case _: org.apache.spark.sql.sources.LessThan           => true
        case _: org.apache.spark.sql.sources.GreaterThanOrEqual => true
        case _: org.apache.spark.sql.sources.LessThanOrEqual    => true
        case _: org.apache.spark.sql.sources.In                 => true
        case _: org.apache.spark.sql.sources.IsNull             => true
        case _: org.apache.spark.sql.sources.IsNotNull          => true
        case _: org.apache.spark.sql.sources.And                => true
        case _: org.apache.spark.sql.sources.Or                 => true
        case _: org.apache.spark.sql.sources.StringContains     => true
        case _: org.apache.spark.sql.sources.StringStartsWith   => true
        case _: org.apache.spark.sql.sources.StringEndsWith     => true

        // Unsupported filter types that would break aggregation accuracy
        case filter if filter.getClass.getSimpleName.contains("RLike") =>
          logger.debug(s"FILTER COMPATIBILITY: Regular expression filter not supported for aggregation: $filter")
          false
        case other =>
          logger.debug(s"FILTER COMPATIBILITY: Unknown filter type, assuming supported: $other")
          true
      }

      // If filter type is supported, validate that filter fields are fast fields
      // For aggregate pushdown, ALL filters require fast field configuration to ensure correctness
      // This will throw IllegalArgumentException if validation fails
      if (isFilterTypeSupported) {
        validateFilterFieldsAreFast(filter)
      } else {
        throw new IllegalArgumentException(s"Filter type not supported for aggregation pushdown: $filter")
      }
    }

    true
  }

  /**
   * Validate that IndexQuery filter fields exist in the schema. Throws IllegalArgumentException if an IndexQuery
   * references a non-existent field.
   */
  private def validateIndexQueryFieldsExist(): Unit = {
    val indexQueryFilters = extractIndexQueriesFromCurrentPlan()
    if (indexQueryFilters.isEmpty) return

    // Get available fields from the schema
    val availableFields = getSchemaFieldNames()
    if (availableFields.isEmpty) {
      logger.debug("INDEXQUERY VALIDATION: No schema fields available, skipping validation")
      return
    }

    // Helper to validate a single field name
    def validateField(columnName: String): Unit =
      if (!availableFields.contains(columnName)) {
        val availableFieldsList = availableFields.toSeq.sorted.mkString(", ")
        throw new IllegalArgumentException(
          s"IndexQuery references non-existent field '$columnName'. " +
            s"Available fields are: [$availableFieldsList]"
        )
      }

    // Validate each IndexQuery filter field
    indexQueryFilters.foreach {
      case filter: io.indextables.spark.filters.IndexQueryFilter =>
        validateField(filter.columnName)

      case filter: io.indextables.spark.filters.IndexQueryV2Filter =>
        validateField(filter.columnName)

      case mixedFilter: io.indextables.spark.filters.MixedBooleanFilter =>
        // Extract all IndexQueryFilter instances from the MixedBooleanFilter tree and validate
        val extractedFilters = io.indextables.spark.filters.MixedBooleanFilter.extractIndexQueryFilters(mixedFilter)
        extractedFilters.foreach(f => validateField(f.columnName))

      case _ => // IndexQueryAllFilter and other types don't reference specific fields
    }
  }

  /**
   * Validate that IS NULL / IS NOT NULL filter fields are configured as FAST fields. ExistsQuery requires FAST fields
   * in Tantivy. This validation runs on the driver to provide clear error messages before task execution.
   *
   * @throws IllegalArgumentException
   *   if a null filter references a non-fast field
   */
  private def validateNullFilterFieldsAreFast(): Unit = {
    import org.apache.spark.sql.sources.{IsNull, IsNotNull, And, Or, Not}

    // Helper to extract null filters from nested filter trees
    def extractNullFilters(filter: Filter): Seq[(String, String)] = filter match {
      case IsNull(attr)     => Seq(("IS NULL", attr))
      case IsNotNull(attr)  => Seq(("IS NOT NULL", attr))
      case And(left, right) => extractNullFilters(left) ++ extractNullFilters(right)
      case Or(left, right)  => extractNullFilters(left) ++ extractNullFilters(right)
      case Not(child)       => extractNullFilters(child)
      case _                => Seq.empty
    }

    // Extract IsNull and IsNotNull filters from pushed filters
    val nullFilters = _pushedFilters.flatMap(extractNullFilters)

    if (nullFilters.isEmpty) return

    // Get actual fast fields from schema
    val fastFields      = getActualFastFieldsFromSchema()
    val availableFields = getSchemaFieldNames()

    nullFilters.foreach {
      case (filterType, fieldName) =>
        // Check field exists
        if (!availableFields.contains(fieldName)) {
          val availableList = availableFields.toSeq.sorted.mkString(", ")
          throw new IllegalArgumentException(
            s"$filterType filter references non-existent field '$fieldName'. " +
              s"Available fields: [$availableList]"
          )
        }

        // Check field is fast
        val isFast = if (fieldName.contains(".")) {
          // For nested JSON fields, check both field and parent
          val parentField = fieldName.substring(0, fieldName.lastIndexOf('.'))
          fastFields.contains(fieldName) || fastFields.contains(parentField)
        } else {
          fastFields.contains(fieldName)
        }

        if (!isFast) {
          val fastFieldsList = fastFields.toSeq.sorted.mkString(", ")
          throw new IllegalArgumentException(
            s"$filterType filter on field '$fieldName' requires FAST field configuration. " +
              s"ExistsQuery in Tantivy only works with FAST fields. " +
              s"To fix: Add '$fieldName' to spark.indextables.indexing.fastfields configuration, " +
              s"or for JSON fields use json.mode='full'. " +
              s"Currently configured fast fields: [${if (fastFieldsList.isEmpty) "none" else fastFieldsList}]"
          )
        }
    }
  }

  /** Get available field names from the schema. */
  private def getSchemaFieldNames(): Set[String] =
    try
      // Get field names from the Spark schema
      schema.fieldNames.toSet
    catch {
      case e: Exception =>
        logger.debug(s"SCHEMA FIELDS: Failed to get schema fields: ${e.getMessage}")
        Set.empty
    }

  /** Get partition columns from the transaction log metadata. */
  private def getPartitionColumns(): Set[String] =
    try {
      val metadata = transactionLog.getMetadata()
      metadata.partitionColumns.toSet
    } catch {
      case e: Exception =>
        logger.debug(s"PARTITION COLUMNS: Failed to get partition columns from transaction log: ${e.getMessage}")
        Set.empty
    }

  /**
   * Get fast fields from the actual table schema/docMappingJson, not from configuration. This reads the transaction log
   * to determine which fields are actually configured as fast.
   *
   * Uses cached DocMappingMetadata to avoid repeated JSON parsing.
   */
  private def getActualFastFieldsFromSchema(): Set[String] =
    try {
      logger.debug("Reading actual fast fields from transaction log")

      // Read existing files from transaction log to get docMappingJson
      val existingFiles       = transactionLog.listFiles()
      val firstWithDocMapping = existingFiles.find(_.docMappingJson.isDefined)

      firstWithDocMapping match {
        case Some(addAction) =>
          // Use cached DocMappingMetadata - no JSON parsing here
          val metadata = EnhancedTransactionLogCache.getDocMappingMetadata(addAction)
          logger.debug(s"Actual fast fields from schema: ${metadata.fastFields.mkString(", ")}")
          metadata.fastFields

        case None =>
          logger.debug("No doc mapping found - likely new table, falling back to configuration-based validation")
          // Fall back to configuration-based validation for new tables
          val fastFieldsStr = config
            .get("spark.indextables.indexing.fastfields")
            .getOrElse("")
          if (fastFieldsStr.nonEmpty) {
            fastFieldsStr.split(",").map(_.trim).filterNot(_.isEmpty).toSet
          } else {
            Set.empty[String]
          }
      }
    } catch {
      case e: Exception =>
        logger.debug(s"Failed to read fast fields from schema: ${e.getMessage}")
        // Fall back to configuration-based validation
        val fastFieldsStr = config
          .get("spark.indextables.indexing.fastfields")
          .getOrElse("")
        if (fastFieldsStr.nonEmpty) {
          fastFieldsStr.split(",").map(_.trim).filterNot(_.isEmpty).toSet
        } else {
          Set.empty[String]
        }
    }

  private def validateFilterFieldsAreFast(filter: org.apache.spark.sql.sources.Filter): Boolean = {
    // Get actual fast fields from the schema/docMappingJson
    val fastFields = getActualFastFieldsFromSchema()

    // Get partition columns - these don't need to be fast since they're handled by transaction log
    val partitionColumns = getPartitionColumns()

    // Extract field names from filter
    val filterFields = extractFieldNamesFromFilter(filter)

    // Exclude partition columns from fast field validation
    val nonPartitionFilterFields = filterFields -- partitionColumns

    logger.debug(s"FILTER VALIDATION: Filter: $filter")
    logger.debug(s"FILTER VALIDATION: All filter fields: ${filterFields.mkString(", ")}")
    logger.debug(s"FILTER VALIDATION: Non-partition filter fields: ${nonPartitionFilterFields.mkString(", ")}")
    logger.debug(s"FILTER VALIDATION: Fast fields from schema: ${fastFields.mkString(", ")}")

    // If all filter fields are partition columns, we don't need fast fields (transaction log optimization)
    if (nonPartitionFilterFields.isEmpty) {
      logger.debug(s"FILTER VALIDATION: All filters are on partition columns - no fast fields required")
      return true
    }

    // Check if all non-partition filter fields are configured as fast fields
    // For nested JSON fields (e.g., "metadata.field1"), check if either the field itself
    // OR its parent (e.g., "metadata") is marked as fast
    val missingFastFields = nonPartitionFilterFields.filterNot { fieldName =>
      if (fieldName.contains(".")) {
        // Nested field - check both field and parent
        val parentField  = fieldName.substring(0, fieldName.lastIndexOf('.'))
        val isFieldFast  = fastFields.contains(fieldName)
        val isParentFast = fastFields.contains(parentField)
        isFieldFast || isParentFast
      } else {
        // Top-level field - direct check
        fastFields.contains(fieldName)
      }
    }
    logger.debug(s"FILTER VALIDATION DEBUG: missingFastFields=$missingFastFields")

    if (missingFastFields.nonEmpty) {
      val columnList        = missingFastFields.mkString("'", "', '", "'")
      val currentFastFields = if (fastFields.nonEmpty) fastFields.mkString("'", "', '", "'") else "none"

      logger.debug(s"FILTER FAST FIELD VALIDATION: Missing fast fields for filter: $columnList")
      logger.debug(s"FILTER FAST FIELD VALIDATION: Current fast fields from schema: $currentFastFields")
      logger.debug(s"FILTER FAST FIELD VALIDATION: Filter rejected for aggregation pushdown: $filter")

      throw new IllegalArgumentException(
        s"""COUNT aggregation with filters requires fast field configuration.
           |
           |Missing fast fields for filter columns: $columnList
           |
           |The table schema shows these fields are not configured as fast fields.
           |With the new default behavior, numeric, string, and date fields should be fast by default.
           |
           |Current fast fields in schema: $currentFastFields
           |Required fast fields: ${(fastFields ++ missingFastFields).toSeq.distinct.mkString("'", "', '", "'")}
           |
           |Filter: $filter""".stripMargin
      )
    }

    true
  }

  /** Extract field names from a Spark Filter. */
  private def extractFieldNamesFromFilter(filter: org.apache.spark.sql.sources.Filter): Set[String] =
    filter match {
      case f: org.apache.spark.sql.sources.EqualTo            => Set(f.attribute)
      case f: org.apache.spark.sql.sources.GreaterThan        => Set(f.attribute)
      case f: org.apache.spark.sql.sources.LessThan           => Set(f.attribute)
      case f: org.apache.spark.sql.sources.GreaterThanOrEqual => Set(f.attribute)
      case f: org.apache.spark.sql.sources.LessThanOrEqual    => Set(f.attribute)
      case f: org.apache.spark.sql.sources.In                 => Set(f.attribute)
      case f: org.apache.spark.sql.sources.IsNull             => Set(f.attribute)
      case f: org.apache.spark.sql.sources.IsNotNull          => Set(f.attribute)
      case f: org.apache.spark.sql.sources.StringContains     => Set(f.attribute)
      case f: org.apache.spark.sql.sources.StringStartsWith   => Set(f.attribute)
      case f: org.apache.spark.sql.sources.StringEndsWith     => Set(f.attribute)
      case f: org.apache.spark.sql.sources.And =>
        extractFieldNamesFromFilter(f.left) ++ extractFieldNamesFromFilter(f.right)
      case f: org.apache.spark.sql.sources.Or =>
        extractFieldNamesFromFilter(f.left) ++ extractFieldNamesFromFilter(f.right)
      case f: org.apache.spark.sql.sources.Not =>
        extractFieldNamesFromFilter(f.child)
      case other =>
        logger.debug(s"FILTER FIELD EXTRACTION: Unknown filter type, cannot extract fields: $other")
        Set.empty[String]
    }

  /** Check if a filter is a partition filter. */
  private def isPartitionFilter(filter: org.apache.spark.sql.sources.Filter): Boolean = {
    val partitionColumns  = getPartitionColumns()
    val referencedColumns = getFilterReferencedColumns(filter)
    referencedColumns.nonEmpty && referencedColumns.forall(partitionColumns.contains)
  }

  /** Get columns referenced by a filter. */
  private def getFilterReferencedColumns(filter: org.apache.spark.sql.sources.Filter): Set[String] = {
    import org.apache.spark.sql.sources._
    filter match {
      case EqualTo(attribute, _)            => Set(attribute)
      case EqualNullSafe(attribute, _)      => Set(attribute)
      case GreaterThan(attribute, _)        => Set(attribute)
      case GreaterThanOrEqual(attribute, _) => Set(attribute)
      case LessThan(attribute, _)           => Set(attribute)
      case LessThanOrEqual(attribute, _)    => Set(attribute)
      case In(attribute, _)                 => Set(attribute)
      case IsNull(attribute)                => Set(attribute)
      case IsNotNull(attribute)             => Set(attribute)
      case StringStartsWith(attribute, _)   => Set(attribute)
      case StringEndsWith(attribute, _)     => Set(attribute)
      case StringContains(attribute, _)     => Set(attribute)
      case And(left, right)                 => getFilterReferencedColumns(left) ++ getFilterReferencedColumns(right)
      case Or(left, right)                  => getFilterReferencedColumns(left) ++ getFilterReferencedColumns(right)
      case Not(child)                       => getFilterReferencedColumns(child)
      case _                                => Set.empty[String]
    }
  }

  /** Validate GROUP BY columns and throw a descriptive exception if validation fails. */
  private def validateGroupByColumnsOrThrow(groupByColumns: Array[String]): Unit = {
    val missingFastFields = scala.collection.mutable.ArrayBuffer[String]()

    // Read actual fast fields from transaction log (docMappingJson), not from configuration
    val fastFields = getActualFastFieldsFromSchema()

    // Partition columns are resolved from split metadata at execution time,
    // not from tantivy fast fields - skip them in validation
    val partitionColumns   = getPartitionColumns()
    val dataGroupByColumns = groupByColumns.filterNot(partitionColumns.contains)

    dataGroupByColumns.foreach { columnName =>
      // Check if the column exists in the schema
      schema.fields.find(_.name == columnName) match {
        case Some(field) =>
          // Check if field is configured as fast field
          if (!fastFields.contains(columnName)) {
            missingFastFields += columnName
          }
        case None =>
          throw new IllegalArgumentException(
            s"GROUP BY column '$columnName' not found in schema. Available columns: ${schema.fields.map(_.name).mkString(", ")}"
          )
      }
    }

    if (missingFastFields.nonEmpty) {
      val columnList        = missingFastFields.mkString("'", "', '", "'")
      val currentFastFields = if (fastFields.nonEmpty) fastFields.mkString("'", "', '", "'") else "none"

      throw new IllegalArgumentException(
        s"""GROUP BY requires fast field configuration for efficient aggregation.
           |
           |Missing fast fields for GROUP BY columns: $columnList
           |
           |To fix this issue, configure these columns as fast fields:
           |  .option("spark.indexfiles.indexing.fastfields", "${(fastFields ++ missingFastFields).toSeq.distinct
            .mkString(",")}")
           |
           |Current fast fields: $currentFastFields
           |Required fast fields: ${(fastFields ++ missingFastFields).toSeq.distinct
            .mkString("'", "', '", "'")}""".stripMargin
      )
    }
  }

  /** Validate aggregation compatibility with GROUP BY and throw a descriptive exception if validation fails. */
  private def validateAggregationCompatibilityOrThrow(aggregation: Aggregation): Unit = {
    import org.apache.spark.sql.connector.expressions.aggregate._
    val missingFastFields = scala.collection.mutable.ArrayBuffer[String]()

    // Read actual fast fields from transaction log (docMappingJson), not from configuration
    // This matches the approach used in validateGroupByColumnsOrThrow for consistency
    val fastFields = getActualFastFieldsFromSchema()

    aggregation.aggregateExpressions.foreach { expr =>
      expr match {
        case _: Count | _: CountStar =>
        // COUNT and COUNT(*) don't require specific fast fields
        case sum: Sum =>
          val fieldName = getFieldName(sum.column)
          if (!isNumericFastField(fieldName)) {
            missingFastFields += fieldName
          }
        case avg: Avg =>
          val fieldName = getFieldName(avg.column)
          if (!isNumericFastField(fieldName)) {
            missingFastFields += fieldName
          }
        case min: Min =>
          // MIN can operate on both numeric and string fast fields
          val fieldName = getFieldName(min.column)
          if (!isFastField(fieldName)) {
            missingFastFields += fieldName
          }
        case max: Max =>
          // MAX can operate on both numeric and string fast fields
          val fieldName = getFieldName(max.column)
          if (!isFastField(fieldName)) {
            missingFastFields += fieldName
          }
        case other =>
          throw new UnsupportedOperationException(
            s"Aggregation function '${other.getClass.getSimpleName}' is not supported with GROUP BY operations"
          )
      }
    }

    if (missingFastFields.nonEmpty) {
      val columnList        = missingFastFields.mkString("'", "', '", "'")
      val currentFastFields = if (fastFields.nonEmpty) fastFields.mkString("'", "', '", "'") else "none"
      val allRequiredFields = (fastFields ++ missingFastFields).toSeq.distinct

      throw new IllegalArgumentException(
        s"""GROUP BY with aggregation functions requires fast field configuration for efficient processing.
           |
           |Missing fast fields for aggregation columns: $columnList
           |
           |To fix this issue, configure these columns as fast fields:
           |  .option("spark.indexfiles.indexing.fastfields", "${allRequiredFields.mkString(",")}")
           |
           |Current fast fields: $currentFastFields
           |Required fast fields: ${allRequiredFields.mkString("'", "', '", "'")}""".stripMargin
      )
    }
  }
}

/**
 * Companion object for ScanBuilder to store IndexQuery information. This provides a clean mechanism for
 * V2IndexQueryExpressionRule to pass IndexQuery expressions directly to the ScanBuilder without a global registry.
 */
object IndexTables4SparkScanBuilder {
  import java.util.WeakHashMap

  // WeakHashMap using DataSourceV2Relation object as key
  // The relation object is passed from V2IndexQueryExpressionRule and accessible during planning
  // WeakHashMap allows GC to clean up entries when relations are no longer referenced
  private val relationIndexQueries: WeakHashMap[AnyRef, Seq[Any]] = new WeakHashMap[AnyRef, Seq[Any]]()

  // SOLUTION: Store regular pushed filters by relation object (same approach as IndexQueries)
  // This solves the multi-pass optimization issue where different ScanBuilder instances
  // are created across optimization passes, but they all share the same DataSourceV2Relation
  private val relationPushedFilters: WeakHashMap[AnyRef, Array[Filter]] = new WeakHashMap[AnyRef, Array[Filter]]()

  // Store unsupported filters by relation object for aggregate exception detection
  private val relationUnsupportedFilters: WeakHashMap[AnyRef, Array[Filter]] = new WeakHashMap[AnyRef, Array[Filter]]()

  // ThreadLocal to pass the actual relation object from V2 rule to ScanBuilder
  // This works even with AQE because the same relation object is used throughout planning
  // Lifecycle: V2 rule checks relation identity → clears if different → sets new relation → ScanBuilder gets it
  // We track the relation's identity hash (stable within query, different for self-joins) to avoid clearing mid-query
  private val currentRelation: ThreadLocal[Option[AnyRef]] = ThreadLocal.withInitial(() => None)
  private val currentRelationId: ThreadLocal[Option[Int]]  = ThreadLocal.withInitial(() => None)

  /** Set the current relation object for this thread (called by V2IndexQueryExpressionRule). */
  def setCurrentRelation(relation: AnyRef): Unit = {
    // Use relation object identity - this is stable within a query execution
    // and correctly handles self-joins (which get different relation instances via newInstance())
    val relationId = System.identityHashCode(relation)
    currentRelation.set(Some(relation))
    currentRelationId.set(Some(relationId))
  }

  /** Get the current relation object for this thread (called by ScanBuilder). */
  def getCurrentRelation(): Option[AnyRef] =
    currentRelation.get()

  /**
   * Clear the current relation object for this thread, but only if it's a different relation. This allows the same
   * relation to be reused across multiple optimization phases.
   */
  def clearCurrentRelationIfDifferent(newRelation: Option[AnyRef]): Unit = {
    val currentId = currentRelationId.get()
    val newId     = newRelation.map(System.identityHashCode)

    if (currentId.isDefined && newId.isDefined && currentId != newId) {
      // Different relation - clear the old one
      currentRelation.remove()
      currentRelationId.remove()
    }
    // else if (currentId.isDefined && newId.isEmpty): Don't clear!
    // The V2IndexQueryExpressionRule is called on many plan types (LocalRelation, Project, etc.)
    // that don't contain DataSourceV2Relation. We should NOT clear the ThreadLocal just because
    // the current plan node doesn't have a relation - it was set by an earlier plan node that did.
    // Only clear when we encounter a DIFFERENT relation.
    // else: same relation or no new relation - keep ThreadLocal for reuse
  }

  /** Clear the current relation object for this thread (for tests). */
  def clearCurrentRelation(): Unit = {
    currentRelation.remove()
    currentRelationId.remove()
  }

  // Tracks whether stored IndexQueries have been "consumed" (read by build()/pushAggregation()).
  // Used by V2IndexQueryExpressionRule to detect stale IndexQueries from a previous failed query.
  //
  // Lifecycle:
  //   1. storeIndexQueries() sets consumed = false (fresh IndexQueries from current query's analysis)
  //   2. getIndexQueries() sets consumed = true when returning non-empty results (build() read them)
  //   3. V2IndexQueryExpressionRule.apply() checks: if consumed = true, stale IndexQueries from
  //      a previous query are present → clear them before processing the new query
  //
  // This prevents stale IndexQueries from persisting across queries when build() throws
  // (e.g., non-existent field validation), because the cleanup at build() lines 460-464
  // is only reached on success.
  private val indexQueriesConsumed: ThreadLocal[Boolean] = ThreadLocal.withInitial(() => false)

  /**
   * Store IndexQuery expressions for a specific relation object. IMPORTANT: This appends to existing entries to support
   * multiple IndexQueries on the same relation (e.g., IndexQuery in both inner CTE definition and outer query
   * referencing the CTE).
   */
  def storeIndexQueries(relation: AnyRef, indexQueries: Seq[Any]): Unit =
    relationIndexQueries.synchronized {
      val existing = Option(relationIndexQueries.get(relation)).getOrElse(Seq.empty)
      relationIndexQueries.put(relation, existing ++ indexQueries)
      indexQueriesConsumed.set(false) // Fresh IndexQueries - not yet consumed
    }

  /** Retrieve IndexQuery expressions for a specific relation object. */
  def getIndexQueries(relation: AnyRef): Seq[Any] =
    relationIndexQueries.synchronized {
      val result = Option(relationIndexQueries.get(relation)).getOrElse(Seq.empty)
      if (result.nonEmpty) indexQueriesConsumed.set(true) // Mark as consumed by build()/pushAggregation()
      result
    }

  /** Clear IndexQuery expressions for a specific relation object. */
  def clearIndexQueries(relation: AnyRef): Unit =
    relationIndexQueries.synchronized {
      relationIndexQueries.remove(relation)
    }

  /**
   * Check if stored IndexQueries have been consumed (read by build()) and are potentially stale. Called by
   * V2IndexQueryExpressionRule at the start of each new query's analysis phase.
   *
   * When this returns true, the caller should clear stored IndexQueries for the current relation because they are
   * leftover from a previous query that either:
   *   - Failed (build() threw before cleanup) - the stale state bug
   *   - Succeeded but cleanup already ran (IndexQueries already empty, so clearing is a no-op)
   */
  def wereIndexQueriesConsumed(): Boolean = indexQueriesConsumed.get()

  /** Reset the consumed flag after clearing stale IndexQueries. */
  def resetIndexQueriesConsumed(): Unit = indexQueriesConsumed.set(false)

  /** Get cache statistics for monitoring. */
  def getCacheStats(): String = {
    val size = relationIndexQueries.synchronized(relationIndexQueries.size())
    s"IndexQuery Cache Stats - Size: $size (WeakHashMap)"
  }

  /** Store pushed filters for a specific relation object. */
  def storePushedFilters(relation: AnyRef, filters: Array[Filter]): Unit =
    relationPushedFilters.synchronized {
      relationPushedFilters.put(relation, filters)
    }

  /** Retrieve pushed filters for a specific relation object. */
  def getPushedFilters(relation: AnyRef): Array[Filter] =
    relationPushedFilters.synchronized {
      Option(relationPushedFilters.get(relation)).getOrElse(Array.empty)
    }

  /** Clear pushed filters for a specific relation object. */
  def clearPushedFilters(relation: AnyRef): Unit =
    relationPushedFilters.synchronized {
      relationPushedFilters.remove(relation)
    }

  /** Store unsupported filters for a specific relation object. */
  def storeUnsupportedFilters(relation: AnyRef, filters: Array[Filter]): Unit =
    relationUnsupportedFilters.synchronized {
      relationUnsupportedFilters.put(relation, filters)
    }

  /** Retrieve unsupported filters for a specific relation object. */
  def getUnsupportedFilters(relation: AnyRef): Array[Filter] =
    relationUnsupportedFilters.synchronized {
      Option(relationUnsupportedFilters.get(relation)).getOrElse(Array.empty)
    }

}
