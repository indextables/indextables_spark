/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */
package io.indextables.spark.core

import io.indextables.spark.TestBase
import org.scalatest.Assertion

/**
 * Tests that aggregate queries with unsupported filters fail with a clear exception rather than returning incorrect
 * results.
 */
class AggregateExceptionTest extends TestBase {

  test("COUNT with IsNull filter should throw exception") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data WITH SOME NULL VALUES so Spark actually pushes the IsNull filter
      // Without nulls, Spark optimizes away the filter entirely
      val data = (0 until 100).map { i =>
        if (i % 10 == 0) (i.toLong, i % 10, null.asInstanceOf[String]) // 10% null values
        else (i.toLong, i % 10, s"item_$i")
      }
      val df = data.toDF("id", "group_id", "name")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(testPath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)

      // COUNT with IsNull should throw exception
      val exception = intercept[IllegalStateException] {
        readDf.filter($"name".isNull).count()
      }

      assert(exception.getMessage.contains("Aggregate pushdown blocked"))
      assert(exception.getMessage.contains("IsNull"))
    }
  }

  test("COUNT with supported filters should work") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data
      val df = spark
        .range(0, 100)
        .toDF("id")
        .selectExpr("id", "id % 10 as group_id", "concat('item_', id) as name")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(testPath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)

      // COUNT with supported filters should work (IsNotNull is implicitly handled)
      val count = readDf.filter($"group_id" > 5).count()
      assert(count == 40L, s"Expected 40 but got $count")
    }
  }

  test("SELECT with IsNull filter should work (not aggregate)") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data
      val df = spark
        .range(0, 100)
        .toDF("id")
        .selectExpr("id", "id % 10 as group_id", "concat('item_', id) as name")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(testPath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)

      // Non-aggregate query with IsNull should work (returns 0 rows since no nulls)
      val rows = readDf.filter($"name".isNull).collect()
      assert(rows.isEmpty, "Expected 0 rows for isNull filter")
    }
  }

  test("SUM/AVG/MIN/MAX with IsNull filter should throw exception") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data WITH SOME NULL VALUES so Spark actually pushes the IsNull filter
      val data = (0 until 100).map { i =>
        if (i % 10 == 0) (i.toLong, i % 10, null.asInstanceOf[java.lang.Double]) // 10% null values
        else (i.toLong, i % 10, (i * 2).toDouble.asInstanceOf[java.lang.Double])
      }
      val df = data.toDF("id", "group_id", "value")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "value")
        .mode("overwrite")
        .save(testPath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)

      // SUM with IsNull should throw exception
      val exception = intercept[IllegalStateException] {
        readDf.filter($"value".isNull).agg(org.apache.spark.sql.functions.sum("value")).collect()
      }

      assert(exception.getMessage.contains("Aggregate pushdown blocked"))
    }
  }
}
