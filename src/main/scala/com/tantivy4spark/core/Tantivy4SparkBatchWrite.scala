package com.tantivy4spark.core

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.transaction.{TransactionLog, AddAction}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.slf4j.LoggerFactory

class Tantivy4SparkBatchWrite(
    transactionLog: TransactionLog,
    tablePath: Path,
    writeInfo: LogicalWriteInfo,
    options: CaseInsensitiveStringMap,
    hadoopConf: org.apache.hadoop.conf.Configuration
) extends BatchWrite with org.apache.spark.sql.connector.write.Write {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkBatchWrite])

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    logger.info(s"Creating batch writer factory for ${info.numPartitions} partitions")
    new Tantivy4SparkWriterFactory(tablePath, writeInfo.schema(), options, hadoopConf)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logger.info(s"Committing ${messages.length} writer messages")
    
    // Initialize transaction log with schema if this is the first commit
    transactionLog.initialize(writeInfo.schema())
    
    val addActions = messages.collect {
      case msg: Tantivy4SparkCommitMessage => msg.addAction
    }

    addActions.foreach { addAction =>
      val version = transactionLog.addFile(addAction)
      logger.info(s"Added file ${addAction.path} at version $version")
    }
    
    logger.info(s"Successfully committed ${addActions.length} files")
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logger.warn(s"Aborting write with ${messages.length} messages")
    
    // Clean up any files that were created but not committed
    val addActions = messages.collect {
      case msg: Tantivy4SparkCommitMessage => msg.addAction
    }

    // In a real implementation, we would delete the physical files here
    logger.warn(s"Would clean up ${addActions.length} uncommitted files")
  }
}

case class Tantivy4SparkCommitMessage(addAction: AddAction) extends WriterCommitMessage