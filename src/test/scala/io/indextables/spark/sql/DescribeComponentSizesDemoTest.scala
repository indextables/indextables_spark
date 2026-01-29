/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

package io.indextables.spark.sql

import java.nio.file.Files

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Demo test to show sample output from DESCRIBE INDEXTABLES COMPONENT SIZES.
 */
class DescribeComponentSizesDemoTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var testDir: java.nio.file.Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .appName("DescribeComponentSizesDemoTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    testDir = Files.createTempDirectory("component_sizes_demo_")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    if (testDir != null) {
      def deleteRecursively(file: java.io.File): Unit = {
        if (file.isDirectory) file.listFiles().foreach(deleteRecursively)
        file.delete()
      }
      deleteRecursively(testDir.toFile)
    }
    super.afterAll()
  }

  test("Demo: show sample output from DESCRIBE COMPONENT SIZES") {
    val tablePath = s"${testDir.toString}/demo_table"

    val sparkSession = spark
    import sparkSession.implicits._

    // Create test data
    val data = Seq(
      (1, "hello world example text", 10.5, "2024", "us-east"),
      (2, "test data sample content", 20.3, "2024", "us-west"),
      (3, "another text here today", 15.7, "2023", "us-east")
    )
    val df = data.toDF("id", "content", "score", "year", "region")

    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.fastfields", "score")
      .partitionBy("year", "region")
      .mode("overwrite")
      .save(tablePath)

    println("\n" + "=" * 100)
    println("DESCRIBE INDEXTABLES COMPONENT SIZES - Full Output")
    println("=" * 100)

    val result = spark.sql(s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath'")
    result.show(100, truncate = false)

    println("\n" + "=" * 100)
    println("Aggregated by component_type")
    println("=" * 100)

    result.createOrReplaceTempView("components")
    spark.sql("""
      SELECT component_type,
             COUNT(*) as count,
             SUM(size_bytes) as total_bytes,
             ROUND(AVG(size_bytes), 1) as avg_bytes
      FROM components
      GROUP BY component_type
      ORDER BY total_bytes DESC
    """).show(truncate = false)

    println("\n" + "=" * 100)
    println("With WHERE clause: year = '2024'")
    println("=" * 100)

    spark.sql(s"DESCRIBE INDEXTABLES COMPONENT SIZES '$tablePath' WHERE year = '2024'").show(50, truncate = false)

    println("\n" + "=" * 100)
    println("Field-level breakdown (fastfield and fieldnorm only)")
    println("=" * 100)

    spark.sql("""
      SELECT field_name, component_type, SUM(size_bytes) as total_bytes
      FROM components
      WHERE field_name IS NOT NULL
      GROUP BY field_name, component_type
      ORDER BY field_name, component_type
    """).show(truncate = false)

    result.count() should be > 0L
  }
}
