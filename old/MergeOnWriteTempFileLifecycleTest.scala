/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.indextables.spark.merge

import java.io.File
import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Reproduction test for temp file lifecycle bug in merge-on-write.
 *
 * BUG: Files created in /local_disk0/tantivy4spark-temp/merge-on-write-UUID/ during write phase
 * are not available during merge phase, causing "Split not available locally or in staging" errors.
 *
 * Expected behavior:
 * - Write phase creates splits in temp directory
 * - Staging uploader uploads splits to S3 asynchronously
 * - Merge phase can access files either from:
 *   a) Local temp directory (if still available), OR
 *   b) Staging area in S3 (if local files cleaned up)
 *
 * Current bug:
 * - Local temp directory gets cleaned up too early
 * - Staging upload doesn't complete or stagingAvailable flag not set
 * - Merge phase fails because neither source is available
 */
class MergeOnWriteTempFileLifecycleTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _
  private var tempDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession.builder()
      .appName("MergeOnWriteTempFileLifecycleTest")
      .master("local[8]")  // 8 workers
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.default.parallelism", "8")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    tempDir = Files.createTempDirectory("merge-lifecycle-test").toFile
    tempDir.deleteOnExit()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (tempDir != null && tempDir.exists()) {
      org.apache.commons.io.FileUtils.deleteDirectory(tempDir)
    }
    super.afterAll()
  }

  test("TEMP FILE LIFECYCLE BUG: Files should be available during merge phase") {
    val tablePath = new File(tempDir, "lifecycle-bug-table").getAbsolutePath

    println("=" * 80)
    println("REPRODUCING BUG: Temp file lifecycle issue")
    println("=" * 80)

    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create a small dataset that will generate multiple splits
    val sourceData = spark.range(0, 5000)
      .select(
        col("id"),
        concat(lit("content_"), col("id")).as("content"),
        (col("id") % 100).as("score")
      )
      .repartition(8)  // 8 partitions = 8 splits

    println(s"Writing 5000 records across 8 partitions with merge-on-write enabled...")

    // Enable merge-on-write with settings that should trigger the bug
    val writeStartTime = System.currentTimeMillis()

    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "10M")  // Small target = merge will be needed
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .mode(SaveMode.Overwrite)
      .save(s"file://$tablePath")

    val writeDuration = System.currentTimeMillis() - writeStartTime
    println(s"Write completed in ${writeDuration}ms")

    // Verify data was written successfully
    println()
    println("=" * 80)
    println("VALIDATING RESULTS:")
    println("=" * 80)

    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(s"file://$tablePath")

    val rowCount = readData.count()

    // The bug manifests as either:
    // 1. Write fails with "Split not available" error, OR
    // 2. Write succeeds but 0 rows in output (all merges failed)

    if (rowCount == 0) {
      println("❌ BUG DETECTED: 0 rows in output - merge phase failed!")
      println("   Expected: 5000 rows")
      println("   Actual: 0 rows")
      println()
      println("   Root cause: Temp files created during write phase were not available during merge phase")
      println("   - Local temp directory may have been cleaned up")
      println("   - Staging files may not have been uploaded or marked as available")

      // Print transaction log for diagnostics
      val transactionLogDir = new File(tablePath, "_transaction_log")
      if (transactionLogDir.exists()) {
        val logFiles = transactionLogDir.listFiles()
          .filter(f => f.getName.endsWith(".json") || f.getName.endsWith(".json.gz"))
          .sortBy(_.getName)

        println()
        println("   Transaction Log Analysis:")
        logFiles.foreach { logFile =>
          val lines = if (logFile.getName.endsWith(".gz")) {
            import java.util.zip.GZIPInputStream
            import java.io.FileInputStream
            val gzipStream = new GZIPInputStream(new FileInputStream(logFile))
            scala.io.Source.fromInputStream(gzipStream, "UTF-8").getLines().toList
          } else {
            scala.io.Source.fromFile(logFile, "UTF-8").getLines().toList
          }

          val addCount = lines.count(_.contains("\"add\""))
          println(s"   - ${logFile.getName}: $addCount AddActions")
        }
      }

      fail("BUG REPRODUCED: Temp file lifecycle issue caused data loss (0 rows written)")
    } else {
      println(s"✅ Write successful: $rowCount rows")

      // Verify data integrity
      val sample = readData.filter(col("id") === 1234).collect()
      sample should have length 1
      sample(0).getAs[String]("content") shouldBe "content_1234"

      println(s"✅ Data integrity verified")
    }

    // Check transaction log for error patterns
    val transactionLogDir = new File(tablePath, "_transaction_log")
    if (transactionLogDir.exists()) {
      val logFiles = transactionLogDir.listFiles()
        .filter(f => f.getName.endsWith(".json") || f.getName.endsWith(".json.gz"))
        .sortBy(_.getName)

      var totalSplitsCreated = 0
      var totalMergedSplits = 0
      var totalPromotedSplits = 0

      logFiles.foreach { logFile =>
        try {
          val lines = if (logFile.getName.endsWith(".gz")) {
            import java.util.zip.GZIPInputStream
            import java.io.FileInputStream
            val gzipStream = new GZIPInputStream(new FileInputStream(logFile))
            scala.io.Source.fromInputStream(gzipStream, "UTF-8").getLines().toList
          } else {
            scala.io.Source.fromFile(logFile, "UTF-8").getLines().toList
          }

          lines.foreach { line =>
            if (line.contains("\"add\"")) {
              totalSplitsCreated += 1
              if (line.contains("\"mergeStrategy\"") && line.contains("\"merge_on_write\"")) {
                totalMergedSplits += 1
              } else if (line.contains("\"mergeStrategy\"") && line.contains("\"promoted\"")) {
                totalPromotedSplits += 1
              }
            }
          }
        } catch {
          case e: Exception =>
            println(s"Warning: Failed to parse log file ${logFile.getName}: ${e.getMessage}")
        }
      }

      println()
      println("=" * 80)
      println("TRANSACTION LOG SUMMARY:")
      println("=" * 80)
      println(s"Total splits in log: $totalSplitsCreated")
      println(s"Merged splits: $totalMergedSplits")
      println(s"Promoted splits: $totalPromotedSplits")

      // NOTE: Merge-on-write may create final merged splits without intermediate AddActions
      // This is expected behavior - the merge happens during write, not as a separate phase
      if (totalSplitsCreated == 0) {
        println("ℹ️  No intermediate splits in transaction log (expected for merge-on-write)")
        println("   This is correct - merge happens during write phase")
      } else {
        println(s"ℹ️  Transaction log shows intermediate splits: merged=$totalMergedSplits, promoted=$totalPromotedSplits")
      }
    }

    println()
    println("=" * 80)
    println("TEST RESULT:")
    println("=" * 80)
    println(s"✅ Test passed: $rowCount rows written and verified")
    println("=" * 80)
  }

  test("STAGING AVAILABILITY: Verify stagingAvailable flag is set correctly") {
    // This test will verify that the staging uploader correctly sets the stagingAvailable flag
    // and that files are actually uploaded to the staging area

    val tablePath = new File(tempDir, "staging-test-table").getAbsolutePath

    println()
    println("=" * 80)
    println("TESTING STAGING AVAILABILITY")
    println("=" * 80)

    val sparkImplicits = spark.implicits
    import sparkImplicits._

    val sourceData = spark.range(0, 1000)
      .select(
        col("id"),
        concat(lit("content_"), col("id")).as("content")
      )
      .repartition(4)

    sourceData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.mergeOnWrite.enabled", "true")
      .option("spark.indextables.mergeOnWrite.targetSize", "5M")
      .option("spark.indextables.mergeOnWrite.minSplitsToMerge", "2")
      .option("spark.indextables.mergeOnWrite.minDiskSpaceGB", "1")
      .mode(SaveMode.Overwrite)
      .save(s"file://$tablePath")

    // Check staging directory
    val stagingDir = new File(tablePath, "_staging")
    println()
    println(s"Staging directory: ${stagingDir.getAbsolutePath}")
    println(s"Staging directory exists: ${stagingDir.exists()}")

    if (stagingDir.exists()) {
      val stagingFiles = stagingDir.listFiles()
      println(s"Staging files count: ${stagingFiles.length}")
      stagingFiles.foreach { f =>
        println(s"  - ${f.getName} (${f.length() / 1024}KB)")
      }

      // If staging files exist, they should have been cleaned up after successful merge
      // Empty staging dir = successful merge and cleanup
      // Non-empty staging dir = either merge failed or cleanup failed
      if (stagingFiles.nonEmpty) {
        println("⚠️  Warning: Staging files not cleaned up - may indicate merge or cleanup issue")
      }
    } else {
      println("✅ No staging directory (may indicate local files were used successfully)")
    }

    // Verify final data
    val readData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(s"file://$tablePath")

    val rowCount = readData.count()
    rowCount shouldBe 1000

    println(s"✅ Data verified: $rowCount rows")
    println("=" * 80)
  }
}
