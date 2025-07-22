package com.tantivy4spark.core

import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.hadoop.fs.Path
import com.tantivy4spark.transaction.TransactionLog
import org.slf4j.LoggerFactory

class Tantivy4SparkWriteBuilder(
    transactionLog: TransactionLog,
    tablePath: Path,
    info: LogicalWriteInfo,
    options: CaseInsensitiveStringMap,
    hadoopConf: org.apache.hadoop.conf.Configuration
) extends WriteBuilder {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkWriteBuilder])

  override def build(): org.apache.spark.sql.connector.write.Write = {
    logger.info(s"Building write for table at: $tablePath")
    new Tantivy4SparkBatchWrite(transactionLog, tablePath, info, options, hadoopConf)
  }
}