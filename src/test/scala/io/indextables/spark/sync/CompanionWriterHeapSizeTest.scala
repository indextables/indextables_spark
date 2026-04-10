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

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests for WRITER HEAP SIZE clause with Delta companion builds.
 *
 * Validates that the WRITER HEAP SIZE clause is accepted and produces correct companion indexes with different size
 * units (M, G), as well as confirming builds succeed without the clause.
 */
class CompanionWriterHeapSizeTest
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
      .appName("CompanionWriterHeapSizeTest")
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
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    _root_.io.indextables.spark.storage.SplitConversionThrottle.initialize(
      maxParallelism = Runtime.getRuntime.availableProcessors() max 1
    )
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private def flushCaches(): Unit = {
    import _root_.io.indextables.spark.storage.{DriverSplitLocalityManager, GlobalSplitCacheManager}
    GlobalSplitCacheManager.flushAllCaches()
    DriverSplitLocalityManager.clear()
  }

  private def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("companion-writer-heap-size").toString
    try {
      flushCaches()
      f(path)
    } finally
      deleteRecursively(new File(path))
  }

  private def readCompanion(indexPath: String): DataFrame =
    spark.read
      .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
      .option("spark.indextables.read.defaultLimit", "1000")
      .load(indexPath)

  private def createDeltaSource(deltaPath: String): Unit = {
    val ss = spark; import ss.implicits._
    Seq(
      (
        1,
        "alice",
        "The quick brown fox jumps over the lazy dog. This is a sample document with enough content to make heap size relevant."
      ),
      (
        2,
        "bob",
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
      ),
      (
        3,
        "charlie",
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
      ),
      (
        4,
        "dave",
        "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur."
      ),
      (
        5,
        "eve",
        "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
      ),
      (
        6,
        "frank",
        "A short text field with some content for testing writer heap size configurations in companion builds."
      ),
      (
        7,
        "grace",
        "Another piece of text content used in the companion writer heap size test to validate split creation."
      ),
      (
        8,
        "heidi",
        "Testing various heap size units including megabytes and gigabytes for the companion indexing pipeline."
      ),
      (
        9,
        "ivan",
        "The writer heap size controls how much memory the tantivy index writer allocates during split creation."
      ),
      (
        10,
        "judy",
        "Larger heap sizes can improve indexing throughput by reducing the number of intermediate segment merges."
      )
    ).toDF("id", "name", "content")
      .write
      .format("delta")
      .save(deltaPath)
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  WRITER HEAP SIZE tests
  // ═══════════════════════════════════════════════════════════════════════════

  test("WRITER HEAP SIZE should be applied to companion build") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createDeltaSource(deltaPath)
      val result = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WRITER HEAP SIZE 512M AT LOCATION '$indexPath'"
        )
        .collect()
      result(0).getString(2) shouldBe "success"
      result(0).getInt(4) should be > 0 // splits created
      readCompanion(indexPath).count() shouldBe 10
    }
  }

  test("WRITER HEAP SIZE with different units (G)") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createDeltaSource(deltaPath)
      val result = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' WRITER HEAP SIZE 2G AT LOCATION '$indexPath'"
        )
        .collect()
      result(0).getString(2) shouldBe "success"
      readCompanion(indexPath).count() shouldBe 10
    }
  }

  test("default build without WRITER HEAP SIZE should succeed") {
    withTempPath { tempDir =>
      val deltaPath = new File(tempDir, "delta").getAbsolutePath
      val indexPath = new File(tempDir, "index").getAbsolutePath

      createDeltaSource(deltaPath)
      val result = spark
        .sql(
          s"BUILD INDEXTABLES COMPANION FOR DELTA '$deltaPath' AT LOCATION '$indexPath'"
        )
        .collect()
      result(0).getString(2) shouldBe "success"
      readCompanion(indexPath).count() shouldBe 10
    }
  }
}
