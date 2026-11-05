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

package io.indextables.spark.sync

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLogFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Regression tests for GitHub issue #393: companion split S3 paths were ordered alphabetically by partition-column
 * name instead of by the table's declared partition-column order (e.g. `partitionBy("zone", "date")` should produce
 * `zone=.../date=.../...split`, not `date=.../zone=.../...split`).
 *
 * Covers both the pure ordering logic in `SyncTaskExecutor.buildPartitionPrefix` and an end-to-end local-filesystem
 * companion sync where "date" sorts alphabetically before "zone" but is declared second.
 */
class CompanionSyncPartitionPathOrderTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with io.indextables.spark.testutils.FileCleanupHelper {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("CompanionSyncPartitionPathOrderTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(
        "spark.sql.extensions",
        "io.indextables.spark.extensions.IndexTables4SparkExtensions," +
          "io.delta.sql.DeltaSparkSessionExtension"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.indextables.aws.accessKey", "test-default-access-key")
      .config("spark.indextables.aws.secretKey", "test-default-secret-key")
      .config("spark.indextables.aws.sessionToken", "test-default-session-token")
      .config("spark.indextables.s3.pathStyleAccess", "true")
      .config("spark.indextables.aws.region", "us-east-1")
      .config("spark.indextables.s3.endpoint", "http://localhost:10101")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    _root_.io.indextables.spark.storage.SplitConversionThrottle.initialize(
      maxParallelism = Runtime.getRuntime.availableProcessors() max 1
    )
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("tantivy4spark-partorder").toString
    try {
      try {
        import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
        GlobalSplitCacheManager.flushAllCaches()
        DriverSplitLocalityManager.clear()
      } catch {
        case _: Exception =>
      }
      f(path)
    } finally
      deleteRecursively(new File(path))
  }

  // -------------------------------------------------------
  //  Pure unit tests: SyncTaskExecutor.buildPartitionPrefix
  // -------------------------------------------------------

  test("buildPartitionPrefix orders by declared partitionColumns, not alphabetically") {
    val values  = Map("zone" -> "east", "date" -> "2024-01-01")
    val columns = Seq("zone", "date")

    SyncTaskExecutor.buildPartitionPrefix(values, columns) shouldBe "zone=east/date=2024-01-01/"
  }

  test("buildPartitionPrefix falls back to alphabetical when partitionColumns is empty") {
    val values = Map("zone" -> "east", "date" -> "2024-01-01")

    SyncTaskExecutor.buildPartitionPrefix(values, Seq.empty) shouldBe "date=2024-01-01/zone=east/"
  }

  test("buildPartitionPrefix drops a partitionValues key missing from partitionColumns") {
    val values  = Map("zone" -> "east", "date" -> "2024-01-01", "extra" -> "unexpected")
    val columns = Seq("zone", "date") // "extra" deliberately omitted

    SyncTaskExecutor.buildPartitionPrefix(values, columns) shouldBe "zone=east/date=2024-01-01/"
  }

  test("buildPartitionPrefix returns empty string for unpartitioned data") {
    SyncTaskExecutor.buildPartitionPrefix(Map.empty, Seq("zone", "date")) shouldBe ""
  }

  test("buildPartitionPrefix handles three-column declared order") {
    val values  = Map("year" -> "2024", "month" -> "01", "day" -> "15")
    val columns = Seq("day", "year", "month") // deliberately non-alphabetical declared order

    SyncTaskExecutor.buildPartitionPrefix(values, columns) shouldBe "day=15/year=2024/month=01/"
  }

  // -------------------------------------------------------
  //  Pure unit tests: IcebergSourceReader.parsePartitionSpecFieldNames
  // -------------------------------------------------------

  test("parsePartitionSpecFieldNames extracts field names in declared spec order") {
    val json =
      """{"spec-id":0,"fields":[{"source-id":2,"field-id":1000,"transform":"identity","name":"zone"},
        |{"source-id":1,"field-id":1001,"transform":"day","name":"date"}]}""".stripMargin

    IcebergSourceReader.parsePartitionSpecFieldNames(json) shouldBe Seq("zone", "date")
  }

  test("parsePartitionSpecFieldNames returns empty for null, blank, or malformed JSON") {
    IcebergSourceReader.parsePartitionSpecFieldNames(null) shouldBe Seq.empty
    IcebergSourceReader.parsePartitionSpecFieldNames("") shouldBe Seq.empty
    IcebergSourceReader.parsePartitionSpecFieldNames("not json") shouldBe Seq.empty
    IcebergSourceReader.parsePartitionSpecFieldNames("""{"spec-id":0}""") shouldBe Seq.empty
  }

  // -------------------------------------------------------
  //  End-to-end: local Delta table, two-column partitioning
  // -------------------------------------------------------

  test("companion sync of a two-column-partitioned Delta table orders paths by declared column order") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta_ordered").getAbsolutePath
      val indexPath = new File(tempDir, "companion_ordered").getAbsolutePath

      // "date" sorts alphabetically before "zone", but is declared SECOND — this is exactly the
      // scenario from issue #393 (alphabetically-earlier column not first in declared order).
      val ss = spark
      import ss.implicits._
      Seq(
        ("east", "2024-01-01", 1L),
        ("west", "2024-01-02", 2L)
      ).toDF("zone", "date", "id")
        .write
        .format("delta")
        .partitionBy("zone", "date")
        .save(deltaPath)

      // Force a checkpoint so getSnapshotInfo() (the declared-order source) is available —
      // matches the pattern used by DistributedDeltaSyncIntegrationTest's "(with checkpoint)" tests.
      val deltaLog = org.apache.spark.sql.delta.DeltaLog.forTable(spark, deltaPath)
      deltaLog.checkpoint()

      val result = spark.sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
      )
      val rows = result.collect()
      rows.length shouldBe 1
      rows(0).getString(2) shouldBe "success"

      val txLog = TransactionLogFactory.create(new Path(indexPath), spark)
      try {
        val files = txLog.listFiles()
        files should not be empty
        files.foreach { file =>
          val zoneIdx = file.path.indexOf("zone=")
          val dateIdx = file.path.indexOf("date=")
          withClue(s"split path was '${file.path}': ") {
            zoneIdx should be >= 0
            dateIdx should be >= 0
            zoneIdx should be < dateIdx // declared order (zone, date), not alphabetical (date, zone)
          }
        }
      } finally
        txLog.close()
    }
  }
}
