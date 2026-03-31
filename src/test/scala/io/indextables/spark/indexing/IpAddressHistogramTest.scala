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

package io.indextables.spark.indexing

import java.nio.file.Files

import org.apache.spark.sql.{SaveMode, SparkSession}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Validates that IP CIDR/wildcard filters work correctly on the histogram/bucket
 * aggregation execution path (GroupByAggregateColumnarReader → nativeAggregateArrowFfi
 * → perform_search_async_impl_leaf_response_with_aggregations).
 *
 * Uses a dedicated Spark session (like BucketAggregationTest) to avoid codegen cache
 * interference from the shared TestBase session.
 */
class IpAddressHistogramTest extends AnyFunSuite with Matchers
    with io.indextables.spark.testutils.FileCleanupHelper {

  def createTestSession(): SparkSession =
    SparkSession
      .builder()
      .appName("IpAddressHistogramTest")
      .master("local[*]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()

  test("IP CIDR filter with histogram aggregation") {
    val spark = createTestSession()
    val tempDir = Files.createTempDirectory("ip-histogram-test").toFile

    try {
      import spark.implicits._

      // Two IPs inside 192.168.1.0/24 with requests in different histogram buckets,
      // one IP outside the CIDR that must be excluded by the pushed-down filter.
      val data = Seq(
        ("s1", "192.168.1.1", 15.0),  // bucket 0.0   (0–50)
        ("s2", "192.168.1.2", 75.0),  // bucket 50.0  (50–100)
        ("s3", "10.0.0.1",   200.0)   // outside CIDR — must NOT appear in results
      ).toDF("name", "ip", "requests")

      data.write
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .option("spark.indextables.indexing.fastfields", "requests")
        .mode(SaveMode.Overwrite)
        .save(tempDir.getAbsolutePath)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .load(tempDir.getAbsolutePath)

      df.createOrReplaceTempView("ip_hist")

      val result = spark.sql(
        """
          |SELECT indextables_histogram(requests, 50.0) AS bucket, COUNT(*) AS cnt
          |FROM ip_hist
          |WHERE ip = '192.168.1.0/24'
          |GROUP BY indextables_histogram(requests, 50.0)
          |ORDER BY bucket
          |""".stripMargin
      ).collect()

      // s1 → bucket 0.0; s2 → bucket 50.0; s3 excluded by CIDR filter
      result.length shouldBe 2
      result(0).getAs[Double]("bucket") shouldBe 0.0
      result(0).getAs[Long]("cnt") shouldBe 1
      result(1).getAs[Double]("bucket") shouldBe 50.0
      result(1).getAs[Long]("cnt") shouldBe 1

    } finally {
      try { deleteRecursively(tempDir) } finally { spark.stop() }
    }
  }

  test("IP wildcard filter with histogram aggregation") {
    val spark = createTestSession()
    val tempDir = Files.createTempDirectory("ip-wildcard-histogram-test").toFile

    try {
      import spark.implicits._

      val data = Seq(
        ("s1", "10.0.0.1",  20.0),   // bucket 0.0   — inside 10.0.*.*
        ("s2", "10.0.0.2",  80.0),   // bucket 50.0  — inside 10.0.*.*
        ("s3", "10.0.1.1", 120.0),   // bucket 100.0 — inside 10.0.*.*
        ("s4", "192.168.1.1", 999.0) // outside wildcard — must NOT appear
      ).toDF("name", "ip", "requests")

      data.write
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .option("spark.indextables.indexing.fastfields", "requests")
        .mode(SaveMode.Overwrite)
        .save(tempDir.getAbsolutePath)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .load(tempDir.getAbsolutePath)

      df.createOrReplaceTempView("ip_wildcard_hist")

      val result = spark.sql(
        """
          |SELECT indextables_histogram(requests, 50.0) AS bucket, COUNT(*) AS cnt
          |FROM ip_wildcard_hist
          |WHERE ip = '10.0.*.*'
          |GROUP BY indextables_histogram(requests, 50.0)
          |ORDER BY bucket
          |""".stripMargin
      ).collect()

      // s1 → 0.0; s2 → 50.0; s3 → 100.0; s4 excluded
      result.length shouldBe 3
      result(0).getAs[Double]("bucket") shouldBe 0.0
      result(0).getAs[Long]("cnt") shouldBe 1
      result(1).getAs[Double]("bucket") shouldBe 50.0
      result(1).getAs[Long]("cnt") shouldBe 1
      result(2).getAs[Double]("bucket") shouldBe 100.0
      result(2).getAs[Long]("cnt") shouldBe 1

    } finally {
      try { deleteRecursively(tempDir) } finally { spark.stop() }
    }
  }
}
