package com.tantivy4spark.debug

import com.tantivy4spark.TestBase
import org.apache.spark.sql.{SaveMode, Row}
import org.apache.spark.sql.functions._
import java.nio.file.Files

class OverwriteDebugTest extends TestBase {
  
  test("simple overwrite debug test") {
    val testDir = Files.createTempDirectory("simple_overwrite_debug_")
    val testPath = testDir.toUri.toString
    
    // Write initial data - 3 rows
    val df1 = spark.range(3).toDF("id")
      .withColumn("type", lit("initial"))
    
    df1.write
      .format("tantivy4spark")
      .mode(SaveMode.Overwrite)
      .save(testPath)
    
    // Verify initial data
    val read1 = spark.read.format("tantivy4spark").load(testPath)
    println(s"Initial count: ${read1.count()}")
    read1.show()
    
    // Overwrite with different data - 2 rows  
    val df2 = spark.range(2).toDF("id")
      .withColumn("type", lit("overwritten"))
    
    println("=== Starting overwrite operation ===")
    df2.write
      .format("tantivy4spark")
      .mode(SaveMode.Overwrite)
      .save(testPath)
    println("=== Completed overwrite operation ===")
    
    // Verify overwritten data
    val read2 = spark.read.format("tantivy4spark").load(testPath)
    println(s"After overwrite count: ${read2.count()}")
    read2.show()
    
    // Check what's in the transaction log
    import com.tantivy4spark.transaction.TransactionLog
    import org.apache.hadoop.fs.Path
    val transactionLog = new TransactionLog(new Path(testPath), spark)
    val files = transactionLog.listFiles()
    println(s"Files in transaction log: ${files.length}")
    println(s"Table path: $testPath")
    println(s"Transaction log table path: ${transactionLog.getTablePath()}")
    files.foreach { f => 
      println(s"  File: ${f.path}, numRecords: ${f.numRecords}")
      val fullPath = new Path(transactionLog.getTablePath(), f.path)
      println(s"  Full path would be: ${fullPath}")
      val actualFile = new java.io.File(fullPath.toUri)
      println(s"  File exists: ${actualFile.exists()}")
    }
    
    // Clean up
    Files.walk(testDir)
      .sorted(java.util.Comparator.reverseOrder())
      .forEach(Files.deleteIfExists(_))
  }
}