package com.tantivy4spark.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.io.Closeable

trait StorageStrategy extends Closeable {
  def readFile(): Array[Byte]
  def readRange(offset: Long, length: Long): Array[Byte]
  def getFileSize(): Long
}

object StorageStrategyFactory {
  
  def createReader(path: Path, conf: Configuration): StorageStrategy = {
    val protocol = path.toUri.getScheme
    val forceStandard = conf.getBoolean("spark.tantivy4spark.storage.force.standard", false)
    
    if (!forceStandard && (protocol == "s3" || protocol == "s3a" || protocol == "s3n")) {
      new S3OptimizedReader(path, conf)
    } else {
      new StandardFileReader(path, conf)
    }
  }
}