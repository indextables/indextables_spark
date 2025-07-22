package com.tantivy4spark.core

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
// Removed unused import
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.transaction.TransactionLog
import org.slf4j.LoggerFactory

class Tantivy4SparkScanBuilder(
    transactionLog: TransactionLog,
    schema: StructType,
    options: CaseInsensitiveStringMap
) extends ScanBuilder 
    with SupportsPushDownFilters 
    with SupportsPushDownRequiredColumns {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkScanBuilder])
  // TODO: do we push-down?
  private var _pushedFilters = Array.empty[Filter]
  private var requiredSchema = schema

  override def build(): Scan = {
    new Tantivy4SparkScan(transactionLog, requiredSchema, _pushedFilters, options)
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (supported, unsupported) = filters.partition(isSupportedFilter)
    _pushedFilters = supported
    logger.info(s"Pushed ${supported.length} filters, ${unsupported.length} unsupported")
    unsupported
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
    logger.info(s"Pruned columns to: ${requiredSchema.fieldNames.mkString(", ")}")
  }

  private def isSupportedFilter(filter: Filter): Boolean = {
    import org.apache.spark.sql.sources._
    
    filter match {
      case _: EqualTo => true
      case _: EqualNullSafe => true
      case _: GreaterThan => true
      case _: GreaterThanOrEqual => true
      case _: LessThan => true
      case _: LessThanOrEqual => true
      case _: In => true
      case _: IsNull => true
      case _: IsNotNull => true
      case _: And => true
      case _: Or => true
      case _: Not => true
      case _: StringStartsWith => true
      case _: StringEndsWith => true
      case _: StringContains => true
      case _ => false
    }
  }
}
