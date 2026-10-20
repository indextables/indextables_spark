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
import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import org.apache.iceberg.{DataFiles, FileFormat}
import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.types.Types
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for bucket aggregations (Histogram, DateHistogram, Range) on companion tables built from Delta and Iceberg
 * sources.
 *
 * Uses a deterministic 8-row dataset with prices spanning three histogram buckets, timestamps across three days, and
 * two regions. All expected bucket counts are hand-calculated.
 */
class CompanionBucketAggregationTest
    extends AnyFunSuite
    with Matchers
    with CompanionTestBase {

  // ── Test data ──────────────────────────────────────────────────────────────
  //
  // 8 rows with price, quantity, event_time, region, name.
  //
  // Histogram (price, interval=50):
  //   bucket 0.0   -> prices 15.0, 25.0, 35.0             -> 3 items, qty sum = 10+20+30 = 60
  //   bucket 50.0  -> prices 55.0, 75.0, 85.0, 95.0       -> 4 items, qty sum = 40+50+60+70 = 220
  //   bucket 150.0 -> price 150.0                          -> 1 item,  qty sum = 80
  //
  // DateHistogram (event_time, '1d'):
  //   2024-01-15 -> 3 items (rows 1,2,3)
  //   2024-01-16 -> 3 items (rows 4,5,6)
  //   2024-01-17 -> 2 items (rows 7,8)
  //
  // Range (price):
  //   cheap     (< 50.0)          -> 15.0, 25.0, 35.0          -> 3 items
  //   mid       (50.0 to 100.0)   -> 55.0, 75.0, 85.0, 95.0   -> 4 items
  //   expensive (>= 100.0)        -> 150.0                     -> 1 item

  private val sparkSchema = StructType(
    Seq(
      StructField("name", StringType, nullable = false),
      StructField("price", DoubleType, nullable = false),
      StructField("quantity", IntegerType, nullable = false),
      StructField("event_time", TimestampType, nullable = false),
      StructField("region", StringType, nullable = false)
    )
  )

  private val testRows: Seq[Row] = Seq(
    Row("prod1", 15.0, 10, Timestamp.valueOf("2024-01-15 08:00:00"), "us-east"),
    Row("prod2", 25.0, 20, Timestamp.valueOf("2024-01-15 10:30:00"), "us-east"),
    Row("prod3", 35.0, 30, Timestamp.valueOf("2024-01-15 14:00:00"), "eu-west"),
    Row("prod4", 55.0, 40, Timestamp.valueOf("2024-01-16 09:00:00"), "eu-west"),
    Row("prod5", 75.0, 50, Timestamp.valueOf("2024-01-16 11:00:00"), "us-east"),
    Row("prod6", 85.0, 60, Timestamp.valueOf("2024-01-16 16:00:00"), "eu-west"),
    Row("prod7", 95.0, 70, Timestamp.valueOf("2024-01-17 10:00:00"), "us-east"),
    Row("prod8", 150.0, 80, Timestamp.valueOf("2024-01-17 15:00:00"), "eu-west")
  )

  // ── Helpers ────────────────────────────────────────────────────────────────

  private def createDeltaSource(deltaPath: String): Unit = {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(testRows), sparkSchema)
    df.write.format("delta").save(deltaPath)
  }

  private def createPartitionedDeltaSource(deltaPath: String): Unit = {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(testRows), sparkSchema)
    df.write.format("delta").partitionBy("region").save(deltaPath)
  }

  private def buildDeltaCompanion(deltaPath: String, indexPath: String): DataFrame = {
    val result = spark
      .sql(
        s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
      )
      .collect()
    result(0).getString(2) shouldBe "success"

    flushCaches()

    spark.read
      .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
      .option("spark.indextables.read.defaultLimit", "1000")
      .load(indexPath)
  }

  // ── Iceberg helpers ────────────────────────────────────────────────────────

  private val icebergSchema = new IcebergSchema(
    Types.NestedField.required(1, "name", Types.StringType.get()),
    Types.NestedField.required(2, "price", Types.DoubleType.get()),
    Types.NestedField.required(3, "quantity", Types.IntegerType.get()),
    Types.NestedField.required(4, "event_time", Types.TimestampType.withZone()),
    Types.NestedField.required(5, "region", Types.StringType.get())
  )

  private def withIcebergCompanion(f: DataFrame => Unit): Unit = {
    val root         = Files.createTempDirectory("iceberg-bucket-agg").toFile
    val warehouseDir = new File(root, "warehouse").getAbsolutePath
    val indexPath    = new File(root, "index").getAbsolutePath
    new File(warehouseDir).mkdirs()

    val server = new EmbeddedIcebergRestServer(warehouseDir)
    try {
      flushCaches()

      // Create Iceberg table
      val ns      = Namespace.of("default")
      val tableId = TableIdentifier.of(ns, "bucket_test")
      server.catalog.createNamespace(ns, java.util.Collections.emptyMap())
      server.catalog.buildTable(tableId, icebergSchema).create()

      // Write parquet data and register as snapshot
      val parquetDir = new File(root, "parquet-data").getAbsolutePath
      val df         = spark.createDataFrame(spark.sparkContext.parallelize(testRows), sparkSchema)
      df.coalesce(1).write.parquet(s"file://$parquetDir")

      val parquetFiles = new File(parquetDir)
        .listFiles()
        .filter(f => f.getName.endsWith(".parquet") && f.length() > 0)

      val table    = server.catalog.loadTable(tableId)
      val appendOp = table.newAppend()
      parquetFiles.foreach { pf =>
        appendOp.appendFile(
          DataFiles
            .builder(table.spec())
            .withPath(s"file://${pf.getAbsolutePath}")
            .withFileSizeInBytes(pf.length())
            .withRecordCount(testRows.size.toLong)
            .withFormat(FileFormat.PARQUET)
            .build()
        )
      }
      appendOp.commit()

      // Configure Spark for embedded catalog
      spark.conf.set("spark.indextables.iceberg.catalogType", "rest")
      spark.conf.set("spark.indextables.iceberg.uri", server.restUri)

      // Build companion
      val result = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR ICEBERG 'default.bucket_test' FASTFIELDS MODE HYBRID AT LOCATION '$indexPath'"
        )
        .collect()
      result(0).getString(2) shouldBe "success"

      flushCaches()

      val companionDf = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.read.defaultLimit", "1000")
        .load(indexPath)

      f(companionDf)
    } finally {
      try spark.conf.unset("spark.indextables.iceberg.catalogType")
      catch { case _: Exception => }
      try spark.conf.unset("spark.indextables.iceberg.uri")
      catch { case _: Exception => }
      server.close()
      deleteRecursively(root)
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Delta companion bucket aggregation tests
  // ═══════════════════════════════════════════════════════════════════════════

  test("Histogram on Delta companion should bucket numeric values") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createDeltaSource(deltaPath)
      val df = buildDeltaCompanion(deltaPath, indexPath)

      df.createOrReplaceTempView("delta_hist_products")

      val histResult = spark.sql(
        """
          |SELECT indextables_histogram(price, 50.0) as price_bucket, COUNT(*) as cnt
          |FROM delta_hist_products
          |GROUP BY indextables_histogram(price, 50.0)
          |ORDER BY price_bucket
          |""".stripMargin
      )

      val rows = histResult.collect()
      rows.length should be >= 3

      val bucketMap = rows.map(r => r.getAs[Double]("price_bucket") -> r.getAs[Long]("cnt")).toMap
      bucketMap(0.0) shouldBe 3   // prices 15,25,35
      bucketMap(50.0) shouldBe 4  // prices 55,75,85,95
      bucketMap(150.0) shouldBe 1 // price 150
      bucketMap.values.sum shouldBe 8
    }
  }

  test("DateHistogram on Delta companion should bucket timestamps") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createDeltaSource(deltaPath)
      val df = buildDeltaCompanion(deltaPath, indexPath)

      df.createOrReplaceTempView("delta_datehist_events")

      val dateHistResult = spark.sql(
        """
          |SELECT indextables_date_histogram(event_time, '1d') as day_bucket, COUNT(*) as cnt
          |FROM delta_datehist_events
          |GROUP BY indextables_date_histogram(event_time, '1d')
          |ORDER BY day_bucket
          |""".stripMargin
      )

      val rows = dateHistResult.collect()
      rows.length should be >= 2

      val counts = rows.map(_.getAs[Long]("cnt"))
      counts.sum shouldBe 8L           // all 8 rows accounted for
      counts.foreach(_ should be > 0L) // no empty buckets
    }
  }

  test("Range aggregation on Delta companion") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createDeltaSource(deltaPath)
      val df = buildDeltaCompanion(deltaPath, indexPath)

      df.createOrReplaceTempView("delta_range_items")

      val rangeResult = spark.sql(
        """
          |SELECT
          |  indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL) as price_tier,
          |  COUNT(*) as cnt
          |FROM delta_range_items
          |GROUP BY indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL)
          |ORDER BY price_tier
          |""".stripMargin
      )

      val rows = rangeResult.collect()
      rows.length shouldBe 3

      val tierMap = rows.map(r => r.getAs[String]("price_tier") -> r.getAs[Long]("cnt")).toMap

      tierMap("cheap") shouldBe 3
      tierMap("mid") shouldBe 4
      tierMap("expensive") shouldBe 1
    }
  }

  test("Histogram with SUM sub-aggregation on Delta companion") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createDeltaSource(deltaPath)
      val df = buildDeltaCompanion(deltaPath, indexPath)

      df.createOrReplaceTempView("delta_hist_sum_products")

      val histResult = spark.sql(
        """
          |SELECT indextables_histogram(price, 50.0) as price_bucket, COUNT(*) as cnt, SUM(quantity) as total_qty
          |FROM delta_hist_sum_products
          |GROUP BY indextables_histogram(price, 50.0)
          |ORDER BY price_bucket
          |""".stripMargin
      )

      val rows = histResult.collect()
      rows.length should be >= 3

      val bucketMap = rows.map(r => r.getAs[Double]("price_bucket") -> r).toMap
      bucketMap(0.0).getAs[Long]("cnt") shouldBe 3
      bucketMap(0.0).getAs[Long]("total_qty") shouldBe 60
      bucketMap(50.0).getAs[Long]("cnt") shouldBe 4
      bucketMap(50.0).getAs[Long]("total_qty") shouldBe 220
      bucketMap(150.0).getAs[Long]("cnt") shouldBe 1
      bucketMap(150.0).getAs[Long]("total_qty") shouldBe 80
      rows.map(_.getAs[Long]("cnt")).sum shouldBe 8
    }
  }

  test("Bucket aggregation on partitioned Delta companion") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createPartitionedDeltaSource(deltaPath)
      val df = buildDeltaCompanion(deltaPath, indexPath)

      // Filter to us-east partition: prod1(15.0), prod2(25.0), prod5(75.0), prod7(95.0)
      // Histogram(price, 50.0): bucket 0.0 -> 2 items, bucket 50.0 -> 2 items
      df.createOrReplaceTempView("delta_part_hist_products")

      val histResult = spark.sql(
        """
          |SELECT indextables_histogram(price, 50.0) as price_bucket, COUNT(*) as cnt
          |FROM delta_part_hist_products
          |WHERE region = 'us-east'
          |GROUP BY indextables_histogram(price, 50.0)
          |ORDER BY price_bucket
          |""".stripMargin
      )

      val rows = histResult.collect()
      rows.length should be >= 2

      val bucketMap = rows.map(r => r.getAs[Double]("price_bucket") -> r.getAs[Long]("cnt")).toMap
      bucketMap(0.0) shouldBe 2  // prices 15,25
      bucketMap(50.0) shouldBe 2 // prices 75,95
      bucketMap.values.sum shouldBe 4
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  Iceberg companion bucket aggregation tests
  // ═══════════════════════════════════════════════════════════════════════════

  test("Histogram on Iceberg companion should bucket numeric values") {
    withIcebergCompanion { df =>
      df.createOrReplaceTempView("ice_hist_products")

      val histResult = spark.sql(
        """
          |SELECT indextables_histogram(price, 50.0) as price_bucket, COUNT(*) as cnt
          |FROM ice_hist_products
          |GROUP BY indextables_histogram(price, 50.0)
          |ORDER BY price_bucket
          |""".stripMargin
      )

      val rows = histResult.collect()
      rows.length should be >= 3

      val bucketMap = rows.map(r => r.getAs[Double]("price_bucket") -> r.getAs[Long]("cnt")).toMap
      bucketMap(0.0) shouldBe 3   // prices 15,25,35
      bucketMap(50.0) shouldBe 4  // prices 55,75,85,95
      bucketMap(150.0) shouldBe 1 // price 150
      bucketMap.values.sum shouldBe 8
    }
  }

  test("DateHistogram on Iceberg companion should bucket timestamps") {
    withIcebergCompanion { df =>
      df.createOrReplaceTempView("ice_datehist_events")

      val dateHistResult = spark.sql(
        """
          |SELECT indextables_date_histogram(event_time, '1d') as day_bucket, COUNT(*) as cnt
          |FROM ice_datehist_events
          |GROUP BY indextables_date_histogram(event_time, '1d')
          |ORDER BY day_bucket
          |""".stripMargin
      )

      val rows = dateHistResult.collect()
      rows.length should be >= 2

      val counts = rows.map(_.getAs[Long]("cnt"))
      counts.sum shouldBe 8L           // all 8 rows accounted for
      counts.foreach(_ should be > 0L) // no empty buckets
    }
  }

  test("Range aggregation on Iceberg companion") {
    withIcebergCompanion { df =>
      df.createOrReplaceTempView("ice_range_items")

      val rangeResult = spark.sql(
        """
          |SELECT
          |  indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL) as price_tier,
          |  COUNT(*) as cnt
          |FROM ice_range_items
          |GROUP BY indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL)
          |ORDER BY price_tier
          |""".stripMargin
      )

      val rows = rangeResult.collect()
      rows.length shouldBe 3

      val tierMap = rows.map(r => r.getAs[String]("price_tier") -> r.getAs[Long]("cnt")).toMap

      tierMap("cheap") shouldBe 3
      tierMap("mid") shouldBe 4
      tierMap("expensive") shouldBe 1
    }
  }

  test("Bucket aggregation on Iceberg companion with fast fields") {
    withIcebergCompanion { df =>
      df.createOrReplaceTempView("ice_hist_sum_products")

      val histResult = spark.sql(
        """
          |SELECT indextables_histogram(price, 50.0) as price_bucket, COUNT(*) as cnt, SUM(quantity) as total_qty
          |FROM ice_hist_sum_products
          |GROUP BY indextables_histogram(price, 50.0)
          |ORDER BY price_bucket
          |""".stripMargin
      )

      val rows = histResult.collect()
      rows.length should be >= 3

      val bucketMap = rows.map(r => r.getAs[Double]("price_bucket") -> r).toMap
      bucketMap(0.0).getAs[Long]("cnt") shouldBe 3
      bucketMap(0.0).getAs[Long]("total_qty") shouldBe 60
      bucketMap(50.0).getAs[Long]("cnt") shouldBe 4
      bucketMap(50.0).getAs[Long]("total_qty") shouldBe 220
      bucketMap(150.0).getAs[Long]("cnt") shouldBe 1
      bucketMap(150.0).getAs[Long]("total_qty") shouldBe 80
      rows.map(_.getAs[Long]("cnt")).sum shouldBe 8
    }
  }
}
