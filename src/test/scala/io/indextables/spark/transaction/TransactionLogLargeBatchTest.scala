package io.indextables.spark.transaction

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.TestBase
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

/**
 * Integration test to validate transaction log file naming with large batches (>100 actions). This test ensures the bug
 * where version files were named "2_000.json" instead of "00000000000000000002.json" is caught and prevented.
 */
class TransactionLogLargeBatchTest extends TestBase {

  test("transaction log should create properly named files when writing >100 actions") {
    withTempPath { tempDir =>
      val tablePath = new Path(tempDir, "large_batch_table")
      // Use JSON format for this test since it checks JSON file naming conventions
      val jsonOptions = new CaseInsensitiveStringMap(
        Map("spark.indextables.state.format" -> "json").asJava
      )
      val transactionLog = TransactionLogFactory.create(tablePath, spark, jsonOptions)

      try {
        // Initialize with schema
        val schema = StructType(
          Seq(
            StructField("id", IntegerType, nullable = false),
            StructField("value", StringType, nullable = false)
          )
        )
        transactionLog.initialize(schema)

        // Create 150 AddActions (triggers parallel write path with >100 actions)
        val addActions = (1 to 150).map { i =>
          AddAction(
            path = s"s3://bucket/file-$i.split",
            partitionValues = Map.empty,
            size = 1024L * i,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(100L)
          )
        }

        // Write the actions - this should trigger writeBatchParallel
        val version = transactionLog.addFiles(addActions)

        // Verify the version file has correct naming format
        val transactionLogPath = new Path(tablePath, "_transaction_log")
        val fs                 = transactionLogPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val files              = fs.listStatus(transactionLogPath)

        val versionFiles = files.map(_.getPath.getName).filter(_.endsWith(".json"))
        println(s"Transaction log files created: ${versionFiles.mkString(", ")}")

        // Should have exactly 2 files: version 0 (metadata) and version 1 (our adds)
        versionFiles.length should be(2)

        // Verify version 0 has correct format
        versionFiles should contain("00000000000000000000.json")

        // Verify version 1 has correct format (20-digit zero-padded)
        versionFiles should contain("00000000000000000001.json")

        // Ensure NO files with wrong pattern exist (version_index.json)
        val wrongPatternFiles = versionFiles.filter(f => f.matches("\\d+_\\d+\\.json") // Pattern like 1_000.json
        )
        wrongPatternFiles should be(empty)

        // Verify we can read back the files
        val readFiles = transactionLog.listFiles()
        readFiles.length should be(150)

      } finally
        transactionLog.close()
    }
  }

  test("transaction log should create properly named files for versions 2-24") {
    withTempPath { tempDir =>
      val tablePath = new Path(tempDir, "multi_version_table")
      // Use JSON format for this test since it checks JSON file naming conventions
      val jsonOptions = new CaseInsensitiveStringMap(
        Map("spark.indextables.state.format" -> "json").asJava
      )
      val transactionLog = TransactionLogFactory.create(tablePath, spark, jsonOptions)

      try {
        // Initialize with schema
        val schema = StructType(
          Seq(
            StructField("id", IntegerType, nullable = false),
            StructField("value", StringType, nullable = false)
          )
        )
        transactionLog.initialize(schema)

        // Create 24 transactions, each with >100 actions
        (1 to 24).foreach { txn =>
          val addActions = (1 to 101).map { i =>
            AddAction(
              path = s"s3://bucket/txn$txn-file-$i.split",
              partitionValues = Map.empty,
              size = 1024L,
              modificationTime = System.currentTimeMillis(),
              dataChange = true,
              numRecords = Some(10L)
            )
          }
          transactionLog.addFiles(addActions)
        }

        // Verify all version files have correct naming
        val transactionLogPath = new Path(tablePath, "_transaction_log")
        val fs                 = transactionLogPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val files              = fs.listStatus(transactionLogPath)
        val versionFiles = files
          .map(_.getPath.getName)
          .filter(_.endsWith(".json"))
          .filterNot(_.contains("checkpoint"))
          .sorted

        println(s"All transaction files: ${versionFiles.mkString(", ")}")

        // Should have 25 files: version 0 + versions 1-24
        versionFiles.length should be(25)

        // Verify all have correct 20-digit zero-padded format
        versionFiles should contain("00000000000000000000.json") // version 0
        versionFiles should contain("00000000000000000002.json") // version 2
        versionFiles should contain("00000000000000000024.json") // version 24

        // Ensure NO files match the buggy pattern (2_000.json, 24_000.json)
        versionFiles should not contain "2_000.json"
        versionFiles should not contain "24_000.json"

        val wrongPatternFiles = versionFiles.filter(f => f.matches("\\d+_\\d+\\.json"))
        wrongPatternFiles should be(empty)

      } finally
        transactionLog.close()
    }
  }
}
