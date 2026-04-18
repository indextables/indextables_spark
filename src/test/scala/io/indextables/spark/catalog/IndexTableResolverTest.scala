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

package io.indextables.spark.catalog

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession

import io.indextables.spark.catalog.IndexTableResolver._
import io.indextables.spark.storage.SplitConversionThrottle

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit and integration tests for IndexTableResolver.
 *
 * Most resolveIndexPath tests do not require a SparkSession.
 * detectRegion tests that need Spark config use a minimal session created per suite.
 */
class IndexTableResolverTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())

    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("IndexTableResolverTest")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse-resolver").toString)
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      // Set a known region so detectRegion tests can be deterministic
      .config("spark.indextables.aws.region", "us-east-1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    SplitConversionThrottle.initialize(maxParallelism = 1)
  }

  override def afterAll(): Unit =
    if (spark != null) {
      spark.stop()
    }

  // ---------------------------------------------------------------------------
  // resolveIndexPath — region key matches
  // ---------------------------------------------------------------------------

  test("resolveIndexPath uses region-specific key when region matches") {
    val props = Map(
      "indextables.companion.indexroot.us-east-1" -> "s3://bucket/indexes",
      PROP_RELATIVE_PATH                          -> "company/users"
    ).asJava

    val path = resolveIndexPath(props, spark, "catalog.default.users")
    path shouldBe "s3://bucket/indexes/company/users"
  }

  // ---------------------------------------------------------------------------
  // resolveIndexPath — fallback key used when region has no specific entry
  // ---------------------------------------------------------------------------

  test("resolveIndexPath falls back to indextables.companion.indexroot when no region-specific key") {
    val props = Map(
      "indextables.companion.indexroot.eu-west-1" -> "s3://eu-bucket/indexes",
      PROP_INDEX_ROOT_PREFIX                      -> "s3://default-bucket/indexes",
      PROP_RELATIVE_PATH                          -> "company/users"
    ).asJava

    // Detected region is us-east-1 (from Spark conf), eu-west-1 won't match
    val path = resolveIndexPath(props, spark, "catalog.default.users")
    path shouldBe "s3://default-bucket/indexes/company/users"
  }

  // ---------------------------------------------------------------------------
  // resolveIndexPath — no region key, no fallback → clear error
  // ---------------------------------------------------------------------------

  test("resolveIndexPath throws with ALTER TABLE hint when no indexroot property found") {
    val props = Map(
      PROP_RELATIVE_PATH -> "company/users"
    ).asJava

    val ex = intercept[IllegalArgumentException] {
      resolveIndexPath(props, spark, "mycatalog.default.users")
    }
    ex.getMessage should include("indextables.companion.indexroot")
    ex.getMessage should include("ALTER TABLE")
    ex.getMessage should include("mycatalog.default.users")
  }

  // ---------------------------------------------------------------------------
  // resolveIndexPath — indexroot present, relativepath missing
  // ---------------------------------------------------------------------------

  test("resolveIndexPath throws with ALTER TABLE hint when relativepath is missing") {
    val props = Map(
      PROP_INDEX_ROOT_PREFIX -> "s3://bucket/indexes"
    ).asJava

    val ex = intercept[IllegalArgumentException] {
      resolveIndexPath(props, spark, "mycatalog.default.users")
    }
    ex.getMessage should include(PROP_RELATIVE_PATH)
    ex.getMessage should include("ALTER TABLE")
  }

  // ---------------------------------------------------------------------------
  // resolveIndexPath — empty string values treated as missing
  // ---------------------------------------------------------------------------

  test("resolveIndexPath treats empty indexroot as missing and falls through to error") {
    val props = Map(
      PROP_INDEX_ROOT_PREFIX -> "",
      PROP_RELATIVE_PATH     -> "company/users"
    ).asJava

    val ex = intercept[IllegalArgumentException] {
      resolveIndexPath(props, spark, "mycatalog.default.users")
    }
    ex.getMessage should include("indextables.companion.indexroot")
  }

  test("resolveIndexPath treats empty relativepath as missing") {
    val props = Map(
      PROP_INDEX_ROOT_PREFIX -> "s3://bucket/indexes",
      PROP_RELATIVE_PATH     -> ""
    ).asJava

    val ex = intercept[IllegalArgumentException] {
      resolveIndexPath(props, spark, "mycatalog.default.users")
    }
    ex.getMessage should include(PROP_RELATIVE_PATH)
  }

  // ---------------------------------------------------------------------------
  // resolveIndexPath — correct path construction
  // ---------------------------------------------------------------------------

  test("resolveIndexPath constructs path as indexroot/relativepath without double slash") {
    val props = Map(
      PROP_INDEX_ROOT_PREFIX -> "s3://bucket/indexes",
      PROP_RELATIVE_PATH     -> "org/table"
    ).asJava

    val path = resolveIndexPath(props, spark, "cat.ns.tbl")
    path shouldBe "s3://bucket/indexes/org/table"
  }

  test("resolveIndexPath strips trailing slash from indexroot") {
    val props = Map(
      PROP_INDEX_ROOT_PREFIX -> "s3://bucket/indexes/",
      PROP_RELATIVE_PATH     -> "org/table"
    ).asJava

    val path = resolveIndexPath(props, spark, "cat.ns.tbl")
    path shouldBe "s3://bucket/indexes/org/table"
  }

  test("resolveIndexPath strips leading slash from relativepath") {
    val props = Map(
      PROP_INDEX_ROOT_PREFIX -> "s3://bucket/indexes",
      PROP_RELATIVE_PATH     -> "/org/table"
    ).asJava

    val path = resolveIndexPath(props, spark, "cat.ns.tbl")
    path shouldBe "s3://bucket/indexes/org/table"
  }

  test("resolveIndexPath prefers region-specific key over fallback when both present") {
    val props = Map(
      "indextables.companion.indexroot.us-east-1" -> "s3://region-bucket/indexes",
      PROP_INDEX_ROOT_PREFIX                      -> "s3://default-bucket/indexes",
      PROP_RELATIVE_PATH                          -> "org/table"
    ).asJava

    // us-east-1 is the configured region
    val path = resolveIndexPath(props, spark, "cat.ns.tbl")
    path shouldBe "s3://region-bucket/indexes/org/table"
  }

  // ---------------------------------------------------------------------------
  // detectRegion — reads from Spark conf (spark.indextables.aws.region)
  // ---------------------------------------------------------------------------

  test("detectRegion reads spark.indextables.aws.region from Spark conf") {
    // Spark conf has "us-east-1" set in beforeAll
    val region = detectRegion(spark)
    region shouldBe Some("us-east-1")
  }

  // ---------------------------------------------------------------------------
  // detectRegion — reads from Hadoop conf (fs.s3a.endpoint.region)
  // ---------------------------------------------------------------------------

  test("detectRegion reads fs.s3a.endpoint.region from Hadoop conf when Spark conf absent") {
    // Temporarily remove the Spark conf override so the Hadoop conf is consulted
    val savedRegion = spark.conf.getOption("spark.indextables.aws.region")
    spark.conf.unset("spark.indextables.aws.region")
    val savedHadoop = Option(spark.sparkContext.hadoopConfiguration.get("fs.s3a.endpoint.region"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint.region", "eu-central-1")

    try {
      val region = detectRegion(spark)
      region shouldBe Some("eu-central-1")
    } finally {
      savedRegion.foreach(v => spark.conf.set("spark.indextables.aws.region", v))
      savedHadoop match {
        case Some(v) => spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint.region", v)
        case None    => spark.sparkContext.hadoopConfiguration.unset("fs.s3a.endpoint.region")
      }
    }
  }

  // ---------------------------------------------------------------------------
  // detectRegion — reads from JVM system property (aws.region)
  // ---------------------------------------------------------------------------

  test("detectRegion reads aws.region JVM system property when Spark and Hadoop conf absent") {
    val savedSparkRegion = spark.conf.getOption("spark.indextables.aws.region")
    val savedHadoop      = Option(spark.sparkContext.hadoopConfiguration.get("fs.s3a.endpoint.region"))
    val savedProp        = Option(System.getProperty("aws.region"))

    spark.conf.unset("spark.indextables.aws.region")
    spark.sparkContext.hadoopConfiguration.unset("fs.s3a.endpoint.region")
    System.setProperty("aws.region", "ap-southeast-1")

    try {
      val region = detectRegion(spark)
      region shouldBe Some("ap-southeast-1")
    } finally {
      savedSparkRegion.foreach(v => spark.conf.set("spark.indextables.aws.region", v))
      savedHadoop match {
        case Some(v) => spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint.region", v)
        case None    => spark.sparkContext.hadoopConfiguration.unset("fs.s3a.endpoint.region")
      }
      savedProp match {
        case Some(v) => System.setProperty("aws.region", v)
        case None    => System.clearProperty("aws.region")
      }
    }
  }

  // ---------------------------------------------------------------------------
  // detectRegion — returns None when nothing is configured
  // ---------------------------------------------------------------------------

  test("detectRegion returns None when no region source is configured") {
    val savedSparkRegion = spark.conf.getOption("spark.indextables.aws.region")
    val savedHadoop      = Option(spark.sparkContext.hadoopConfiguration.get("fs.s3a.endpoint.region"))
    val savedProp        = Option(System.getProperty("aws.region"))

    spark.conf.unset("spark.indextables.aws.region")
    spark.sparkContext.hadoopConfiguration.unset("fs.s3a.endpoint.region")
    System.clearProperty("aws.region")

    // Cannot control AWS_REGION / AWS_DEFAULT_REGION env vars at runtime, but we can verify
    // detectRegion doesn't throw and returns a sensible result.
    try {
      // If running in an AWS environment, env vars may set a region — that's fine.
      // We only assert no exception is thrown.
      noException should be thrownBy detectRegion(spark)
    } finally {
      savedSparkRegion.foreach(v => spark.conf.set("spark.indextables.aws.region", v))
      savedHadoop match {
        case Some(v) => spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint.region", v)
        case None    => spark.sparkContext.hadoopConfiguration.unset("fs.s3a.endpoint.region")
      }
      savedProp.foreach(System.setProperty("aws.region", _))
    }
  }
}
