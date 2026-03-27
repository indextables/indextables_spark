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

package io.indextables.spark.benchmark

import java.io.File

import org.apache.spark.sql.DataFrame

import io.indextables.spark.write.ArrowFfiWriteConfig
import io.indextables.spark.TestBase

/**
 * A/B benchmark comparing TANT batch path vs Arrow FFI path on identical data. Both paths write the same dataset,
 * measuring wall-clock time, and the test asserts data correctness (read-back parity).
 *
 * Run: mvn test-compile scalatest:test -DwildcardSuites='io.indextables.spark.benchmark.ArrowFfiWriteBenchmark'
 *
 * Go/no-go criteria: If median speedup across all scenarios is <5%, the Arrow FFI path does not justify its complexity.
 * If >=5%, we keep it and enable by default in a future release.
 */
class ArrowFfiWriteBenchmark extends TestBase {

  private val FORMAT     = INDEXTABLES_FORMAT
  private val ITERATIONS = 3

  private case class BenchmarkResult(
    name: String,
    tantMedianMs: Long,
    arrowMedianMs: Long,
    speedupPct: Double,
    rowCount: Long,
    dataParity: Boolean)

  private val results = scala.collection.mutable.ArrayBuffer[BenchmarkResult]()

  private def median(times: Seq[Long]): Long = {
    val sorted = times.sorted
    if (sorted.length % 2 == 0) (sorted(sorted.length / 2 - 1) + sorted(sorted.length / 2)) / 2
    else sorted(sorted.length / 2)
  }

  private def timed(f: => Unit): Long = {
    val start = System.currentTimeMillis()
    f
    System.currentTimeMillis() - start
  }

  private def writeData(
    df: DataFrame,
    path: String,
    arrowFfiEnabled: Boolean,
    partitionCols: Seq[String] = Seq.empty
  ): Unit = {
    val writer = df.write
      .mode("overwrite")
      .format(FORMAT)
      .option(ArrowFfiWriteConfig.KEY_ENABLED, arrowFfiEnabled.toString)

    val partitioned = if (partitionCols.nonEmpty) writer.partitionBy(partitionCols: _*) else writer
    partitioned.save(path)
  }

  private def runBenchmark(
    name: String,
    df: DataFrame,
    partitionCols: Seq[String] = Seq.empty
  ): Unit =
    withTempPath { basePath =>
      val rowCount = df.count()

      // Warmup: one write of each path (excluded from timing)
      writeData(df, s"file://$basePath/warmup_tant", arrowFfiEnabled = false, partitionCols)
      writeData(df, s"file://$basePath/warmup_arrow", arrowFfiEnabled = true, partitionCols)

      // Timed runs: TANT batch
      val tantTimes = (1 to ITERATIONS).map { i =>
        val p = s"file://$basePath/tant_$i"
        timed(writeData(df, p, arrowFfiEnabled = false, partitionCols))
      }

      // Timed runs: Arrow FFI
      val arrowTimes = (1 to ITERATIONS).map { i =>
        val p = s"file://$basePath/arrow_$i"
        timed(writeData(df, p, arrowFfiEnabled = true, partitionCols))
      }

      // Data parity check
      val tantData  = spark.read.format(FORMAT).load(s"file://$basePath/tant_1")
      val arrowData = spark.read.format(FORMAT).load(s"file://$basePath/arrow_1")
      val parity    = tantData.count() == arrowData.count() && tantData.count() == rowCount

      val tantMedian  = median(tantTimes)
      val arrowMedian = median(arrowTimes)
      val speedup     = if (tantMedian > 0) (tantMedian - arrowMedian).toDouble / tantMedian * 100 else 0.0

      results += BenchmarkResult(name, tantMedian, arrowMedian, speedup, rowCount, parity)

      println(f"$name%-45s TANT: $tantMedian%6dms  Arrow: $arrowMedian%6dms  Δ: $speedup%+.1f%%  parity=$parity")
    }

  // ===== Benchmark Scenarios =====

  test("benchmark: narrow string-only (500K x 5)") {
    val df = spark
      .range(0, 500000)
      .selectExpr(
        "CAST(id AS STRING) as col1",
        "CAST(id * 2 AS STRING) as col2",
        "CAST(id * 3 AS STRING) as col3",
        "CAST(id * 4 AS STRING) as col4",
        "CAST(id * 5 AS STRING) as col5"
      )
    runBenchmark("Narrow string-only (500K x 5)", df)
  }

  test("benchmark: narrow mixed types (500K x 5)") {
    val df = spark
      .range(0, 500000)
      .selectExpr(
        "CAST(id AS STRING) as name",
        "id as long_val",
        "CAST(id * 1.5 AS DOUBLE) as double_val",
        "CAST(id % 2 = 0 AS BOOLEAN) as flag",
        "CAST('2024-01-01' AS TIMESTAMP) as ts"
      )
    runBenchmark("Narrow mixed types (500K x 5)", df)
  }

  test("benchmark: wide string-only (500K x 30)") {
    val cols = (1 to 30).map(i => s"CAST(id * $i AS STRING) as col$i").mkString(", ")
    val df   = spark.range(0, 500000).selectExpr(cols.split(", "): _*)
    runBenchmark("Wide string-only (500K x 30)", df)
  }

  test("benchmark: wide mixed types (500K x 30)") {
    val stringCols = (1 to 10).map(i => s"CAST(id * $i AS STRING) as str$i")
    val longCols   = (1 to 10).map(i => s"(id * $i) as long$i")
    val dblCols    = (1 to 10).map(i => s"CAST(id * $i * 0.1 AS DOUBLE) as dbl$i")
    val allCols    = (stringCols ++ longCols ++ dblCols).mkString(", ")
    val df         = spark.range(0, 500000).selectExpr(allCols.split(", "): _*)
    runBenchmark("Wide mixed types (500K x 30)", df)
  }

  test("benchmark: JSON/nested fields (200K x 10)") {
    val df = spark
      .range(0, 200000)
      .selectExpr(
        "id",
        "named_struct('x', CAST(id AS INT), 'y', CAST(id AS STRING)) as struct1",
        "named_struct('a', CAST(id AS DOUBLE), 'b', CAST(id % 10 AS INT)) as struct2",
        "named_struct('p', CAST(id AS STRING), 'q', CAST(id * 2 AS STRING)) as struct3",
        "CAST(id AS STRING) as str1",
        "CAST(id * 2 AS STRING) as str2",
        "CAST(id * 3 AS STRING) as str3",
        "CAST(id * 4 AS STRING) as str4",
        "CAST(id * 5 AS STRING) as str5"
      )
    runBenchmark("JSON/nested fields (200K x 10)", df)
  }

  test("benchmark: struct-heavy (200K x 8)") {
    val df = spark
      .range(0, 200000)
      .selectExpr(
        "id",
        "named_struct('name', CAST(id AS STRING), 'age', CAST(id % 100 AS INT)) as user",
        "named_struct('lat', CAST(id * 0.01 AS DOUBLE), 'lon', CAST(id * 0.02 AS DOUBLE)) as location",
        "named_struct('street', CAST(id AS STRING), 'city', CAST(id % 50 AS STRING), 'zip', CAST(id % 10000 AS INT)) as address",
        "CAST(id AS STRING) as label",
        "CAST(id * 2 AS STRING) as category",
        "id as score",
        "CAST(id % 2 = 0 AS BOOLEAN) as active"
      )
    runBenchmark("Struct-heavy (200K x 8)", df)
  }

  test("benchmark: array fields (200K x 6)") {
    val df = spark
      .range(0, 200000)
      .selectExpr(
        "id",
        "array(CAST(id AS STRING), CAST(id * 2 AS STRING), CAST(id * 3 AS STRING)) as tags",
        "array(CAST(id AS INT), CAST(id * 2 AS INT), CAST(id * 3 AS INT), CAST(id * 4 AS INT)) as scores",
        "array(CAST(id * 0.1 AS DOUBLE), CAST(id * 0.2 AS DOUBLE)) as values",
        "CAST(id AS STRING) as name",
        "id as num"
      )
    runBenchmark("Array fields (200K x 6)", df)
  }

  test("benchmark: map fields (200K x 5)") {
    val df = spark
      .range(0, 200000)
      .selectExpr(
        "id",
        "map('key1', CAST(id AS STRING), 'key2', CAST(id * 2 AS STRING)) as str_map",
        "map('a', id, 'b', id * 2, 'c', id * 3) as num_map",
        "CAST(id AS STRING) as label",
        "id as score"
      )
    runBenchmark("Map fields (200K x 5)", df)
  }

  test("benchmark: mixed complex types (200K x 10)") {
    val df = spark
      .range(0, 200000)
      .selectExpr(
        "id",
        "named_struct('name', CAST(id AS STRING), 'value', CAST(id AS INT)) as metadata",
        "array(CAST(id AS STRING), CAST(id * 2 AS STRING)) as tags",
        "map('k1', CAST(id AS STRING), 'k2', CAST(id * 2 AS STRING)) as attrs",
        "named_struct('x', CAST(id * 0.1 AS DOUBLE), 'y', CAST(id * 0.2 AS DOUBLE)) as coords",
        "array(CAST(id AS INT), CAST(id * 10 AS INT)) as nums",
        "CAST(id AS STRING) as name",
        "id as score",
        "CAST(id * 1.5 AS DOUBLE) as weight",
        "CAST(id % 2 = 0 AS BOOLEAN) as flag"
      )
    runBenchmark("Mixed complex types (200K x 10)", df)
  }

  test("benchmark: partitioned low cardinality (500K x 10 / 10p)") {
    val cols    = (1 to 9).map(i => s"CAST(id * $i AS STRING) as col$i")
    val allCols = cols :+ "CAST(id % 10 AS STRING) as part_col"
    val df      = spark.range(0, 500000).selectExpr(allCols: _*)
    runBenchmark("Partitioned low card (500K x 10 / 10p)", df, partitionCols = Seq("part_col"))
  }

  test("benchmark: partitioned high cardinality (500K x 10 / 100p)") {
    val cols    = (1 to 8).map(i => s"CAST(id * $i AS STRING) as col$i")
    val allCols = cols ++ Seq("CAST(id % 10 AS STRING) as part_a", "CAST(id % 10 AS STRING) as part_b")
    val df      = spark.range(0, 500000).selectExpr(allCols: _*)
    runBenchmark("Partitioned high card (500K x 10 / 100p)", df, partitionCols = Seq("part_a", "part_b"))
  }

  test("benchmark: small batch (1K x 10)") {
    val cols = (1 to 10).map(i => s"CAST(id * $i AS STRING) as col$i").mkString(", ")
    val df   = spark.range(0, 1000).selectExpr(cols.split(", "): _*)
    runBenchmark("Small batch (1K x 10)", df)
  }

  // ===== Summary =====

  test("benchmark summary and go/no-go") {
    if (results.isEmpty) {
      println("No benchmark results collected (run individual benchmark tests first)")
      succeed
    } else {
      println("\n" + "=" * 100)
      println("ARROW FFI WRITE BENCHMARK SUMMARY")
      println("=" * 100)
      println(f"${"Scenario"}%-45s ${"TANT"}%8s  ${"Arrow"}%8s  ${"Speedup"}%10s  ${"Parity"}%6s")
      println("-" * 100)
      results.foreach { r =>
        println(
          f"${r.name}%-45s ${r.tantMedianMs}%6dms  ${r.arrowMedianMs}%6dms  ${r.speedupPct}%+8.1f%%  ${r.dataParity}%6s"
        )
      }
      println("-" * 100)

      val avgSpeedup = results.map(_.speedupPct).sum / results.size
      val allParity  = results.forall(_.dataParity)

      println(f"Average speedup: $avgSpeedup%+.1f%%")
      println(s"Data parity: ${if (allParity) "PASS" else "FAIL"}")
      println(s"Go/no-go: ${if (avgSpeedup >= 5.0 && allParity) "GO" else "NO-GO"}")
      println("=" * 100)

      allParity shouldBe true
    }
  }
}
