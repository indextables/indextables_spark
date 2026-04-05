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

package io.indextables.spark.sql

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}

import scala.util.Random

import org.apache.spark.sql.{DataFrame, SparkSession}

import io.indextables.spark.TestBase
import org.apache.commons.io.FileUtils

/**
 * Standalone reproducer for the regression introduced in tantivy4java 0.34.1: RemoveAction tombstones written via
 * writeVersionArrowFfi are not being applied when reading back the Avro-format transaction log. After a MERGE SPLITS
 * operation (which adds new merged splits and removes old splits), the file listing returns both the old and new
 * splits, causing duplicate records.
 *
 * Expected: 1800 records after global merge Actual: ~5788 records (original splits + merged splits both visible)
 *
 * Run with: mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.sql.PartitionedMergeTest'
 */
class PartitionedMergeTest extends TestBase {

  var testDataPath: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.adaptive.enabled", "false")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    testDataPath = Files.createTempDirectory("partitioned_merge_test_").toString
  }

  override def afterEach(): Unit = {
    super.afterEach()
    try
      if (testDataPath != null) FileUtils.deleteDirectory(new File(testDataPath))
    catch {
      case _: Exception =>
    }
  }

  override def afterAll(): Unit =
    super.afterAll()

  test("Merge splits across all partitions without WHERE clause") {
    val timeSeriesData    = generateTimeSeriesData(spark, totalRecords = 1800, daysSpan = 3)
    val repartitionedData = timeSeriesData.repartition(30)

    repartitionedData.write
      .format(INDEXTABLES_FORMAT)
      .partitionBy("load_date", "load_hour")
      .option("spark.indextables.indexWriter.batchSize", "5")
      .mode("overwrite")
      .save(testDataPath)

    val filesBefore = countSplitFiles(testDataPath)
    println(s"Files before global merge: $filesBefore")

    spark.sql(s"""
      MERGE SPLITS '$testDataPath'
      TARGET SIZE 5M
      MAX DEST SPLITS 10
    """)

    val filesAfterGlobalMerge = countSplitFiles(testDataPath)
    println(s"Files after global merge: $filesAfterGlobalMerge")

    if (filesBefore > 1) {
      assert(filesAfterGlobalMerge <= filesBefore, "Should have same or fewer files after global merge")
    }

    val df                = spark.read.format(INDEXTABLES_FORMAT).load(testDataPath)
    val totalRecordsAfter = df.count()
    assert(totalRecordsAfter == 1800, "Should preserve all records after global merge")

    val partitionCount = df.select("load_date", "load_hour").distinct().count()
    println(s"Distinct partitions after merge: $partitionCount")
    assert(partitionCount >= 24, "Should maintain partition structure")
  }

  private def generateTimeSeriesData(
    spark: SparkSession,
    totalRecords: Int,
    daysSpan: Int,
    startDate: String = "2024-01-01"
  ): DataFrame = {
    import spark.implicits._

    val random     = new Random(42)
    val baseDate   = LocalDate.parse(startDate)
    val eventTypes = Array("system", "application", "security", "performance")
    val severities = Array("info", "warning", "error", "critical")

    val records = (0 until totalRecords).map { i =>
      val dayOffset = random.nextInt(daysSpan)
      val hour      = random.nextInt(24)
      val loadDate  = baseDate.plusDays(dayOffset).toString

      val eventType = eventTypes(random.nextInt(eventTypes.length))
      val severity  = severities(random.nextInt(severities.length))

      val messageTemplates = Array(
        s"$eventType $severity: Processing batch $i",
        s"$eventType $severity: Memory usage at ${random.nextInt(100)}%",
        s"$eventType $severity: Connection timeout after ${random.nextInt(30)} seconds",
        s"$eventType $severity: Performance degradation detected",
        s"$eventType $severity: User authentication ${if (random.nextBoolean()) "successful" else "failed"}",
        s"$eventType $severity: System configuration updated"
      )

      val message = messageTemplates(random.nextInt(messageTemplates.length))
      val timestamp = Timestamp.valueOf(
        LocalDateTime.parse(f"${loadDate}T$hour%02d:${random.nextInt(60)}%02d:${random.nextInt(60)}%02d")
      )

      TimeSeriesRecord(
        id = i.toLong,
        timestamp = timestamp,
        load_date = loadDate,
        load_hour = hour,
        event_type = eventType,
        severity = severity,
        message = message,
        metric_value = random.nextDouble() * 100,
        host_id = f"host-${random.nextInt(10)}%03d"
      )
    }

    spark.createDataFrame(records)
  }

  private def countSplitFiles(path: String): Int =
    try {
      import io.indextables.spark.transaction.TransactionLogFactory
      import org.apache.hadoop.fs.Path

      val transactionLog = TransactionLogFactory.create(new Path(path), spark)
      val activeFiles    = transactionLog.listFiles()
      activeFiles.length
    } catch {
      case _: Exception =>
        def countRecursively(dir: File): Int = {
          if (!dir.exists() || !dir.isDirectory) return 0
          val splitFiles  = dir.listFiles().filter(_.getName.endsWith(".split")).length
          val subDirFiles = dir.listFiles().filter(_.isDirectory).map(countRecursively).sum
          splitFiles + subDirFiles
        }
        countRecursively(new File(path))
    }
}
