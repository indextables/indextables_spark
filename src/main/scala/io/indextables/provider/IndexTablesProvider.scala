package io.indextables.provider

import io.indextables.spark.core.Tantivy4SparkTableProvider

/**
 * Alias for Tantivy4SparkTableProvider to provide a more generic interface. This allows users to access Tantivy4Spark
 * functionality using the io.indextables.provider.IndexTablesProvider format. Configuration uses spark.indextables
 * prefix which is interchangeable with spark.indextables.
 *
 * Usage: spark.read.format("io.indextables.provider.IndexTablesProvider").load("s3://bucket/path")
 * spark.write.format("io.indextables.provider.IndexTablesProvider").save("s3://bucket/path")
 *
 * Configuration examples: spark.conf.set("spark.indextables.indexing.typemap.content", "text") // Same as
 * spark.indextables.indexing.typemap.content spark.conf.set("spark.indextables.merge.debug", "true") // Same as
 * spark.indextables.merge.debug
 */
class IndexTablesProvider extends Tantivy4SparkTableProvider {
  // This class inherits all functionality from Tantivy4SparkTableProvider
  // No additional implementation needed - it serves as a pure alias
}
