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
    assert(columns.contains("locality_match"), "Result should contain 'locality_match' column")

    // Verify column order matches expected schema
    assert(columns(0) == "host")
    assert(columns(1) == "assigned_host")
    assert(columns(2) == "locality_match")

    // Print results for visibility
    println("\n=== Prewarm Result with Locality Tracking ===")
    prewarmResult.show(truncate = false)

    // In local mode, host and assigned_host should match
    val results = prewarmResult.collect()
    results.foreach { row =>
      val host = row.getAs[String]("host")
      val assignedHost = row.getAs[String]("assigned_host")
      val localityMatch = row.getAs[Boolean]("locality_match")

      println(s"Host: $host, Assigned: $assignedHost, Match: $localityMatch")

      // In local mode, tasks should run on the assigned host
      assert(localityMatch, s"In local mode, locality should match. Host=$host, Assigned=$assignedHost")
    }
  }

  test("prewarm should report locality_match=true in local mode") {
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

    // All results should have locality_match=true in local mode
    results.foreach { row =>
      val localityMatch = row.getAs[Boolean]("locality_match")
      assert(localityMatch, "locality_match should be true in local mode")
    }

    println("✅ All prewarm tasks report locality_match=true")
  }

  test("prewarm with partition filter returning no splits should report locality_match=true") {
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

    // Should return "no_splits" status with locality_match=true
    assert(results.length == 1, "Should have one result row")
    val row = results(0)
    assert(row.getAs[String]("status") == "no_splits")
    assert(row.getAs[Boolean]("locality_match") == true)

    println("✅ Prewarm with no matching splits correctly reports locality_match=true")
  }
}
