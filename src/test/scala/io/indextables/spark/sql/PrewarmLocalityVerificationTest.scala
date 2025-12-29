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

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests to verify that prewarm command tracks and reports locality correctly.
 */
class PrewarmLocalityVerificationTest extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempTablePath: String = _

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("PrewarmLocalityVerificationTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

    tempTablePath = Files.createTempDirectory("locality_test_").toFile.getAbsolutePath
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }

    if (tempTablePath != null) {
      deleteRecursively(new File(tempTablePath))
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  test("prewarm result should include locality_match column") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data
    val data = (1 to 100).map(i => (i.toLong, s"title_$i", s"content for document $i", i * 1.5))
    val df = data.toDF("id", "title", "content", "score")

    // Write to IndexTables
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .save(tempTablePath)

    // Run prewarm
    val prewarmResult = spark.sql(s"PREWARM INDEXTABLES CACHE '$tempTablePath'")

    // Verify schema contains locality columns
    val columns = prewarmResult.columns
    assert(columns.contains("host"), "Result should contain 'host' column")
    assert(columns.contains("assigned_host"), "Result should contain 'assigned_host' column")
    assert(columns.contains("locality_hits"), "Result should contain 'locality_hits' column")
    assert(columns.contains("locality_misses"), "Result should contain 'locality_misses' column")
    assert(columns.contains("retries"), "Result should contain 'retries' column")
    assert(columns.contains("failed_splits_by_host"), "Result should contain 'failed_splits_by_host' column")

    // Verify column order matches expected schema
    assert(columns(0) == "host")
    assert(columns(1) == "assigned_host")
    assert(columns(2) == "locality_hits")
    assert(columns(3) == "locality_misses")
    assert(columns(10) == "retries")
    assert(columns(11) == "failed_splits_by_host")

    // Print results for visibility
    println("\n=== Prewarm Result with Locality Tracking ===")
    prewarmResult.show(truncate = false)

    // In local mode, host and assigned_host should match (all hits, no misses)
    val results = prewarmResult.collect()
    results.foreach { row =>
      val host = row.getAs[String]("host")
      val assignedHost = row.getAs[String]("assigned_host")
      val localityHits = row.getAs[Int]("locality_hits")
      val localityMisses = row.getAs[Int]("locality_misses")
      val retries = row.getAs[Int]("retries")
      val failedSplitsByHost = row.getAs[String]("failed_splits_by_host")

      println(s"Host: $host, Assigned: $assignedHost, Hits: $localityHits, Misses: $localityMisses, Retries: $retries")

      // In local mode, tasks should run on the assigned host (all hits)
      assert(localityMisses == 0, s"In local mode, should have no locality misses. Host=$host, Misses=$localityMisses")
      assert(localityHits > 0 || row.getAs[String]("status") == "no_splits",
        s"Should have locality hits or be no_splits status")
      // In local mode, no retries should be needed
      assert(retries == 0, s"In local mode, should have no retries. Retries=$retries")
      assert(failedSplitsByHost == null, s"In local mode, should have no failed splits. FailedSplits=$failedSplitsByHost")
    }
  }

  test("prewarm should report zero locality misses in local mode") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data
    val data = (1 to 50).map(i => (i.toLong, s"value_$i"))
    val df = data.toDF("id", "value")

    // Write to IndexTables
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tempTablePath)

    // Run prewarm
    val prewarmResult = spark.sql(s"PREWARM INDEXTABLES CACHE '$tempTablePath'")
    val results = prewarmResult.collect()

    // All results should have zero locality misses in local mode
    results.foreach { row =>
      val localityMisses = row.getAs[Int]("locality_misses")
      assert(localityMisses == 0, s"In local mode, should have zero locality misses but got $localityMisses")
    }

    println("✅ All prewarm tasks report zero locality misses")
  }

  test("prewarm with partition filter returning no splits should report zero hits and misses") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create partitioned test data
    val data = Seq(
      (1L, "2024-01-01", "value1"),
      (2L, "2024-01-01", "value2")
    )
    val df = data.toDF("id", "date", "value")

    // Write with partitioning
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .partitionBy("date")
      .save(tempTablePath)

    // Run prewarm with partition filter that matches no data
    val prewarmResult = spark.sql(
      s"PREWARM INDEXTABLES CACHE '$tempTablePath' WHERE date = '2099-12-31'"
    )
    val results = prewarmResult.collect()

    // Should return "no_splits" status with zero hits and misses (no tasks ran)
    assert(results.length == 1, "Should have one result row")
    val row = results(0)
    assert(row.getAs[String]("status") == "no_splits")
    assert(row.getAs[Int]("locality_hits") == 0, "No splits should mean zero locality hits")
    assert(row.getAs[Int]("locality_misses") == 0, "No splits should mean zero locality misses")

    println("✅ Prewarm with no matching splits correctly reports zero hits and misses")
  }
}
