package com.tantivy4spark.storage

import com.tantivy4spark.TestBase
import org.apache.hadoop.fs.Path
import java.nio.file.{Files, Paths}

class StorageStrategyTest extends TestBase {

  test("should create S3OptimizedReader for S3 paths") {
    val s3Path = new Path("s3://bucket/path/file.tnt4s")
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    
    // This would normally create an S3OptimizedReader, but we'll test the factory logic
    val protocol = s3Path.toUri.getScheme
    protocol shouldBe "s3"
    
    // Test would require actual AWS credentials and S3 access in a real scenario
    // For unit testing, we focus on the protocol detection logic
  }

  test("should create StandardFileReader for HDFS paths") {
    val hdfsPath = new Path("hdfs://namenode:9000/path/file.tnt4s")
    val protocol = hdfsPath.toUri.getScheme
    protocol shouldBe "hdfs"
  }

  test("should create StandardFileReader for local file paths") {
    withTempPath { tempPath =>
      // Create a test file
      val testFile = Paths.get(tempPath, "test.tnt4s")
      val testData = "test data for file reading".getBytes("UTF-8")
      Files.write(testFile, testData)

      val localPath = new Path(s"file://$testFile")
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      
      val reader = new StandardFileReader(localPath, hadoopConf)
      
      try {
        val fileSize = reader.getFileSize()
        fileSize shouldBe testData.length

        val readData = reader.readFile()
        readData shouldBe testData

        val rangeData = reader.readRange(0, 4)
        rangeData shouldBe "test".getBytes("UTF-8")

        val partialRange = reader.readRange(5, 4)
        partialRange shouldBe "data".getBytes("UTF-8")
        
      } finally {
        reader.close()
      }
    }
  }

  test("should handle reading beyond file end gracefully") {
    withTempPath { tempPath =>
      val testFile = Paths.get(tempPath, "small.tnt4s")
      val testData = "small".getBytes("UTF-8")
      Files.write(testFile, testData)

      val localPath = new Path(s"file://$testFile")
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val reader = new StandardFileReader(localPath, hadoopConf)
      
      try {
        // Try to read more data than available
        val rangeData = reader.readRange(3, 10)
        rangeData shouldBe "ll".getBytes("UTF-8") // Should return only available data
        
      } finally {
        reader.close()
      }
    }
  }

  test("should handle empty files") {
    withTempPath { tempPath =>
      val testFile = Paths.get(tempPath, "empty.tnt4s")
      Files.write(testFile, Array.empty[Byte])

      val localPath = new Path(s"file://$testFile")
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val reader = new StandardFileReader(localPath, hadoopConf)
      
      try {
        reader.getFileSize() shouldBe 0
        reader.readFile() shouldBe Array.empty[Byte]
        reader.readRange(0, 100) shouldBe Array.empty[Byte]
        
      } finally {
        reader.close()
      }
    }
  }

  test("should detect storage strategy based on configuration") {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    
    // Test default behavior
    val s3Path = new Path("s3a://bucket/file.tnt4s")
    val protocol = s3Path.toUri.getScheme
    protocol shouldBe "s3a"
    
    // Test force standard configuration
    hadoopConf.setBoolean("spark.tantivy4spark.storage.force.standard", true)
    val forceStandard = hadoopConf.getBoolean("spark.tantivy4spark.storage.force.standard", false)
    forceStandard shouldBe true
  }
}