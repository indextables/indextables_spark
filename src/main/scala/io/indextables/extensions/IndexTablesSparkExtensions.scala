package io.indextables.extensions

import com.tantivy4spark.extensions.Tantivy4SparkExtensions

/**
 * Alias for Tantivy4SparkExtensions to provide a more generic interface.
 * This allows users to register Tantivy4Spark SQL extensions using the io.indextables namespace.
 *
 * Extensions provided (inherited from Tantivy4SparkExtensions):
 * - Custom SQL parser for MERGE SPLITS command
 * - Custom SQL parser for INVALIDATE TRANSACTION LOG CACHE command
 * - Custom SQL parser for indexquery operator
 * - Custom optimizer rule for V2 DataSource IndexQuery pushdown
 * - FLUSH TANTIVY4SPARK SEARCHER CACHE command
 *
 * Usage:
 *   spark.conf.set("spark.sql.extensions", "io.indextables.extensions.IndexTablesSparkExtensions")
 *
 * Or in spark-defaults.conf:
 *   spark.sql.extensions=io.indextables.extensions.IndexTablesSparkExtensions
 *
 * Configuration uses spark.indextables prefix which is interchangeable with spark.indextables:
 *   spark.conf.set("spark.indextables.merge.debug", "true")  // Same as spark.indextables.merge.debug
 */
class IndexTablesSparkExtensions extends Tantivy4SparkExtensions {
  // This class inherits all functionality from Tantivy4SparkExtensions
  // No additional implementation needed - it serves as a pure alias
}