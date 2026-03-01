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

import java.nio.file.Files

import org.apache.spark.sql.SparkSession

import io.indextables.spark.transaction.AddAction
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests for DistributedAntiJoin — verifies distributed anti-join logic for initial sync,
 * incremental sync, invalidation, and remaining-from-invalidated.
 */
class DistributedAntiJoinTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .appName("DistributedAntiJoinTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  private def makeSourceFile(path: String, size: Long = 1000L, partVals: Map[String, String] = Map.empty) =
    CompanionSourceFile(path, partVals, size)

  private def makeAddAction(
    path: String,
    companionFiles: Seq[String],
    partVals: Map[String, String] = Map.empty
  ) =
    AddAction(
      path = path,
      partitionValues = partVals,
      size = 5000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      companionSourceFiles = Some(companionFiles)
    )

  // ─── Initial Sync ───

  test("initial sync should return all source files") {
    val sc          = spark.sparkContext
    val sourceFiles = Seq(
      makeSourceFile("file1.parquet"),
      makeSourceFile("file2.parquet"),
      makeSourceFile("file3.parquet")
    )
    val sourceRDD = sc.parallelize(sourceFiles)

    val antiJoin = new DistributedAntiJoin(spark)
    val result   = antiJoin.computeChanges(sourceRDD, Seq.empty, isInitialSync = true)

    result.filesToIndex.size shouldBe 3
    result.splitsToInvalidate shouldBe empty
  }

  // ─── Incremental Sync: New Files ───

  test("incremental sync should detect new files") {
    val sc          = spark.sparkContext
    val sourceFiles = Seq(
      makeSourceFile("file1.parquet"),
      makeSourceFile("file2.parquet"),
      makeSourceFile("file3.parquet") // new file
    )
    val sourceRDD = sc.parallelize(sourceFiles)

    val existing = Seq(
      makeAddAction("split1.split", Seq("file1.parquet", "file2.parquet"))
    )

    val antiJoin = new DistributedAntiJoin(spark)
    val result   = antiJoin.computeChanges(sourceRDD, existing, isInitialSync = false)

    result.filesToIndex.size shouldBe 1
    result.filesToIndex.head.path shouldBe "file3.parquet"
    result.splitsToInvalidate shouldBe empty
  }

  test("incremental sync with no changes should return empty") {
    val sc          = spark.sparkContext
    val sourceFiles = Seq(
      makeSourceFile("file1.parquet"),
      makeSourceFile("file2.parquet")
    )
    val sourceRDD = sc.parallelize(sourceFiles)

    val existing = Seq(
      makeAddAction("split1.split", Seq("file1.parquet", "file2.parquet"))
    )

    val antiJoin = new DistributedAntiJoin(spark)
    val result   = antiJoin.computeChanges(sourceRDD, existing, isInitialSync = false)

    result.filesToIndex shouldBe empty
    result.splitsToInvalidate shouldBe empty
  }

  // ─── Incremental Sync: Invalidation ───

  test("incremental sync should detect gone files and invalidate splits") {
    val sc          = spark.sparkContext
    val sourceFiles = Seq(
      makeSourceFile("file1.parquet") // file2 is gone from source
    )
    val sourceRDD = sc.parallelize(sourceFiles)

    val existing = Seq(
      makeAddAction("split1.split", Seq("file1.parquet", "file2.parquet"))
    )

    val antiJoin = new DistributedAntiJoin(spark)
    val result   = antiJoin.computeChanges(sourceRDD, existing, isInitialSync = false)

    result.splitsToInvalidate.size shouldBe 1
    result.splitsToInvalidate.head.path shouldBe "split1.split"

    // file1 from invalidated split should be re-indexed
    result.filesToIndex.size shouldBe 1
    result.filesToIndex.head.path shouldBe "file1.parquet"
  }

  test("incremental sync should preserve partition values from invalidated splits") {
    val sc           = spark.sparkContext
    val partitionMap = Map("region" -> "east")
    val sourceFiles = Seq(
      makeSourceFile("file1.parquet", partVals = partitionMap)
      // file2 is gone
    )
    val sourceRDD = sc.parallelize(sourceFiles)

    val existing = Seq(
      makeAddAction("split1.split", Seq("file1.parquet", "file2.parquet"), partVals = partitionMap)
    )

    val antiJoin = new DistributedAntiJoin(spark)
    val result   = antiJoin.computeChanges(sourceRDD, existing, isInitialSync = false)

    result.filesToIndex.head.partitionValues shouldBe partitionMap
  }

  test("incremental sync should look up accurate file sizes from source") {
    val sc          = spark.sparkContext
    val sourceFiles = Seq(
      makeSourceFile("file1.parquet", size = 42000L)
    )
    val sourceRDD = sc.parallelize(sourceFiles)

    val existing = Seq(
      makeAddAction("split1.split", Seq("file1.parquet", "file2.parquet"))
    )

    val antiJoin = new DistributedAntiJoin(spark)
    val result   = antiJoin.computeChanges(sourceRDD, existing, isInitialSync = false)

    result.filesToIndex.head.size shouldBe 42000L
  }

  // ─── Deduplication ───

  test("should dedup files by path, preferring largest size") {
    val sc          = spark.sparkContext
    val sourceFiles = Seq(
      makeSourceFile("new_file.parquet", size = 5000L),
      makeSourceFile("file1.parquet", size = 10000L) // also in source, still valid
    )
    val sourceRDD = sc.parallelize(sourceFiles)

    // split1 tracks file1 and file2; file2 is gone → split1 invalidated
    // file1 re-indexed from invalidated split, new_file is new
    val existing = Seq(
      makeAddAction("split1.split", Seq("file1.parquet", "file2.parquet"))
    )

    val antiJoin = new DistributedAntiJoin(spark)
    val result   = antiJoin.computeChanges(sourceRDD, existing, isInitialSync = false)

    // new_file (new) + file1 (remaining from invalidated)
    result.filesToIndex.size shouldBe 2
    val file1 = result.filesToIndex.find(_.path == "file1.parquet")
    file1 shouldBe defined
    file1.get.size shouldBe 10000L // accurate size from source, not 0L
  }

  // ─── URL-encoded paths ───

  test("should handle URL-encoded paths in anti-join") {
    val sc          = spark.sparkContext
    val sourceFiles = Seq(
      makeSourceFile("path%20with%20spaces/file1.parquet"),
      makeSourceFile("normal/file2.parquet") // new
    )
    val sourceRDD = sc.parallelize(sourceFiles)

    val existing = Seq(
      makeAddAction("split1.split", Seq("path with spaces/file1.parquet"))
    )

    val antiJoin = new DistributedAntiJoin(spark)
    val result   = antiJoin.computeChanges(sourceRDD, existing, isInitialSync = false)

    // file1 matches via URL decoding, so only file2 is new
    result.filesToIndex.size shouldBe 1
    result.splitsToInvalidate shouldBe empty
  }

  // ─── Multiple splits ───

  test("should handle multiple companion splits correctly") {
    val sc          = spark.sparkContext
    val sourceFiles = Seq(
      makeSourceFile("file1.parquet"),
      makeSourceFile("file2.parquet"),
      makeSourceFile("file4.parquet") // new
      // file3 is gone
    )
    val sourceRDD = sc.parallelize(sourceFiles)

    val existing = Seq(
      makeAddAction("split1.split", Seq("file1.parquet")),
      makeAddAction("split2.split", Seq("file2.parquet", "file3.parquet"))
    )

    val antiJoin = new DistributedAntiJoin(spark)
    val result   = antiJoin.computeChanges(sourceRDD, existing, isInitialSync = false)

    // file4 is new, file2 is remaining from invalidated split2
    result.filesToIndex.map(_.path).toSet should contain("file4.parquet")
    result.filesToIndex.map(_.path).toSet should contain("file2.parquet")
    result.splitsToInvalidate.size shouldBe 1
    result.splitsToInvalidate.head.path shouldBe "split2.split"
  }
}
