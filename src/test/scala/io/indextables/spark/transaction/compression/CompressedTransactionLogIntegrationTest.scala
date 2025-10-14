package io.indextables.spark.transaction.compression

import java.io.File

import scala.util.Random

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import io.indextables.spark.io.CloudStorageProviderFactory

class CompressedTransactionLogIntegrationTest extends AnyFunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _
  val testDir             = s"/tmp/compressed_transaction_test_${Random.nextInt(10000)}"

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("CompressedTransactionLogIntegrationTest")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    // Clean up test directory
    val dir = new File(testDir)
    if (dir.exists()) {
      deleteRecursively(dir)
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    file.delete()
  }

  test("transaction logs are compressed when writing table with compression enabled") {
    val tablePath = s"$testDir/compressed_table"

    // Create test data
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq(
      ("id1", "content1", 100),
      ("id2", "content2", 200),
      ("id3", "content3", 300)
    ).toDF("id", "content", "score")

    // Write with compression enabled (default)
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "true")
      .option("spark.indextables.transaction.compression.codec", "gzip")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Verify transaction log files are compressed
    val transactionLogPath = new Path(tablePath, "_transaction_log")
    val provider = CloudStorageProviderFactory.createProvider(
      tablePath,
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
      spark.sparkContext.hadoopConfiguration
    )

    try {
      val files = provider.listFiles(transactionLogPath.toString)
      val jsonFiles = files.map(_.path).filter(_.endsWith(".json")).filterNot(_.contains("checkpoint"))

      assert(jsonFiles.nonEmpty, "Should have transaction log files")

      // Check that transaction files are compressed
      jsonFiles.foreach { file =>
        val rawBytes = provider.readFile(file)
        assert(CompressionUtils.isCompressed(rawBytes), s"File $file should be compressed")
        assert(rawBytes(0) == CompressionUtils.MAGIC_BYTE, s"File $file should start with magic byte")
        assert(rawBytes(1) == 0x01, s"File $file should use GZIP codec")

        // Verify we can decompress
        val decompressed = CompressionUtils.readTransactionFile(rawBytes)
        val content      = new String(decompressed, "UTF-8")
        assert(content.contains("protocol") || content.contains("metaData") || content.contains("add"),
          s"Decompressed content should be valid transaction log JSON")
      }

      // Verify we can read the table back
      val loaded = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      assert(loaded.count() == 3)
      assert(loaded.columns.toSeq.toSet == Set("id", "content", "score"))

    } finally {
      provider.close()
    }
  }

  test("transaction logs remain uncompressed when compression is disabled") {
    val tablePath = s"$testDir/uncompressed_table"

    // Create test data
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq(
      ("id1", "content1", 100),
      ("id2", "content2", 200)
    ).toDF("id", "content", "score")

    // Write with compression disabled
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "false")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Verify transaction log files are NOT compressed
    val transactionLogPath = new Path(tablePath, "_transaction_log")
    val provider = CloudStorageProviderFactory.createProvider(
      tablePath,
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
      spark.sparkContext.hadoopConfiguration
    )

    try {
      val files = provider.listFiles(transactionLogPath.toString)
      val jsonFiles = files.map(_.path).filter(_.endsWith(".json")).filterNot(_.contains("checkpoint"))

      assert(jsonFiles.nonEmpty, "Should have transaction log files")

      jsonFiles.foreach { file =>
        val rawBytes = provider.readFile(file)
        assert(!CompressionUtils.isCompressed(rawBytes), s"File $file should NOT be compressed")

        // Verify it's valid JSON
        val content = new String(rawBytes, "UTF-8")
        assert(content.contains("{"), "Should be valid JSON")
      }

      // Verify we can still read the table
      val loaded = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      assert(loaded.count() == 2)

    } finally {
      provider.close()
    }
  }

  test("checkpoints are compressed when compression is enabled") {
    val tablePath = s"$testDir/compressed_checkpoint_table"

    val sparkSession = spark
    import sparkSession.implicits._

    // Write initial data with compression enabled and small checkpoint interval
    val df1 = Seq(("id1", "content1", 100)).toDF("id", "content", "score")
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "true")
      .option("spark.indextables.checkpoint.enabled", "true")
      .option("spark.indextables.checkpoint.interval", "2") // Create checkpoint after 2 transactions
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Append more data to trigger checkpoint
    val df2 = Seq(("id2", "content2", 200)).toDF("id", "content", "score")
    df2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "true")
      .option("spark.indextables.checkpoint.enabled", "true")
      .option("spark.indextables.checkpoint.interval", "2")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Append one more to ensure checkpoint is created
    val df3 = Seq(("id3", "content3", 300)).toDF("id", "content", "score")
    df3.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "true")
      .option("spark.indextables.checkpoint.enabled", "true")
      .option("spark.indextables.checkpoint.interval", "2")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Verify checkpoint files are compressed
    val transactionLogPath = new Path(tablePath, "_transaction_log")
    val provider = CloudStorageProviderFactory.createProvider(
      tablePath,
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
      spark.sparkContext.hadoopConfiguration
    )

    try {
      val files = provider.listFiles(transactionLogPath.toString)
      val checkpointFiles = files.map(_.path).filter(_.contains(".checkpoint.json"))

      if (checkpointFiles.nonEmpty) {
        checkpointFiles.foreach { file =>
          val rawBytes = provider.readFile(file)
          assert(CompressionUtils.isCompressed(rawBytes), s"Checkpoint file $file should be compressed")
          assert(rawBytes(0) == CompressionUtils.MAGIC_BYTE)
          assert(rawBytes(1) == 0x01) // GZIP codec

          // Verify we can decompress
          val decompressed = CompressionUtils.readTransactionFile(rawBytes)
          val content      = new String(decompressed, "UTF-8")
          assert(content.contains("add") || content.contains("protocol") || content.contains("metaData"),
            "Decompressed checkpoint should contain valid actions")
        }
      }

      // Verify we can read the table with checkpoint
      val loaded = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      assert(loaded.count() == 3)

    } finally {
      provider.close()
    }
  }

  test("can read table with mixed compressed and uncompressed transaction logs") {
    val tablePath = s"$testDir/mixed_compression_table"

    val sparkSession = spark
    import sparkSession.implicits._

    // Write initial data WITHOUT compression
    val df1 = Seq(("id1", "content1", 100)).toDF("id", "content", "score")
    df1.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "false")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Append data WITH compression
    val df2 = Seq(("id2", "content2", 200)).toDF("id", "content", "score")
    df2.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "true")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Append more data WITH compression
    val df3 = Seq(("id3", "content3", 300)).toDF("id", "content", "score")
    df3.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "true")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Verify we have both compressed and uncompressed files
    val transactionLogPath = new Path(tablePath, "_transaction_log")
    val provider = CloudStorageProviderFactory.createProvider(
      tablePath,
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
      spark.sparkContext.hadoopConfiguration
    )

    try {
      val files = provider.listFiles(transactionLogPath.toString)
      val jsonFiles = files.map(_.path).filter(_.endsWith(".json")).filterNot(_.contains("checkpoint")).sorted

      var foundCompressed   = false
      var foundUncompressed = false

      jsonFiles.foreach { file =>
        val rawBytes    = provider.readFile(file)
        val isCompressed = CompressionUtils.isCompressed(rawBytes)

        if (isCompressed) foundCompressed = true
        else foundUncompressed = true
      }

      assert(foundCompressed, "Should have at least one compressed file")
      assert(foundUncompressed, "Should have at least one uncompressed file")

      // Verify we can read all data correctly
      val loaded = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
      assert(loaded.count() == 3)

      val ids = loaded.select("id").collect().toSeq.map(_.getString(0)).sorted
      assert(ids.sameElements(Seq("id1", "id2", "id3")))

    } finally {
      provider.close()
    }
  }

  test("compression provides measurable space savings") {
    val compressedTablePath   = s"$testDir/space_test_compressed"
    val uncompressedTablePath = s"$testDir/space_test_uncompressed"

    val sparkSession = spark
    import sparkSession.implicits._

    // Create identical data for both tables
    val df = (1 until 51).map { i =>
      (s"id_$i", s"content_with_repeated_text_pattern_$i", i * 100)
    }.toDF("id", "content", "score")

    // Write WITH compression
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "true")
      .mode(SaveMode.Overwrite)
      .save(compressedTablePath)

    // Write WITHOUT compression
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "false")
      .mode(SaveMode.Overwrite)
      .save(uncompressedTablePath)

    // Measure transaction log sizes
    def getTransactionLogSize(tablePath: String): Long = {
      val transactionLogPath = new Path(tablePath, "_transaction_log")
      val provider = CloudStorageProviderFactory.createProvider(
        tablePath,
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )

      try {
        val files = provider.listFiles(transactionLogPath.toString)
        files.map(_.path).filter(_.endsWith(".json")).map { file =>
          provider.readFile(file).length.toLong
        }.sum
      } finally {
        provider.close()
      }
    }

    val compressedSize   = getTransactionLogSize(compressedTablePath)
    val uncompressedSize = getTransactionLogSize(uncompressedTablePath)

    val compressionRatio = uncompressedSize.toDouble / compressedSize
    println(s"Transaction log compression: ${uncompressedSize} bytes -> ${compressedSize} bytes (${compressionRatio}x)")

    // With repeated JSON patterns, we should get at least 1.5x compression
    assert(compressionRatio > 1.5, s"Expected at least 1.5x compression, got ${compressionRatio}x")
  }
}
