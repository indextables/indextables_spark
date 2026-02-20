/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */
package io.indextables.spark.core

import io.indextables.spark.TestBase

/**
 * Tests that aggregate queries with IsNull/IsNotNull filters work correctly. IsNull/IsNotNull filters are either pushed
 * down to Tantivy (for FAST fields) or handled by Spark's post-filtering (for non-FAST fields).
 */
class AggregateExceptionTest extends TestBase {

  test("COUNT with IsNull filter should work (handled by Spark)") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data WITH SOME NULL VALUES
      // IsNull on non-FAST field will be handled by Spark's post-filtering
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

      // COUNT with IsNull should now work - Spark handles the filter after reading data
      val count = readDf.filter($"name".isNull).count()
      // 10% of rows (0, 10, 20, ..., 90) have null name = 10 rows
      assert(count == 10L, s"Expected 10 null rows but got $count")
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

  test("SUM/AVG/MIN/MAX with IsNull filter should work (handled by Spark post-filtering)") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data WITH SOME NULL VALUES
      // IsNull on non-FAST field will be handled by Spark's post-filtering (not pushed down)
      val data = (0 until 100).map { i =>
        if (i % 10 == 0) (i.toLong, i % 10, null.asInstanceOf[java.lang.Double]) // 10% null values
        else (i.toLong, i % 10, (i * 2).toDouble.asInstanceOf[java.lang.Double])
      }
      val df = data.toDF("id", "group_id", "value")

      // Note: 'value' is NOT configured as a fast field, so IsNull won't be pushed down
      // This tests that IsNull filters don't block aggregation - Spark handles them via post-filtering
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(testPath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)

      // SUM with IsNull should now work - Spark handles the filter after reading data
      // The key is that this doesn't throw "Aggregate pushdown blocked" exception
      // It should complete successfully regardless of whether result is null or 0
      val sumResult = readDf.filter($"value".isNull).agg(org.apache.spark.sql.functions.sum("value")).collect()
      assert(sumResult.length == 1, "Expected 1 result row for SUM")

      // AVG with IsNull should also work
      val avgResult = readDf.filter($"value".isNull).agg(org.apache.spark.sql.functions.avg("value")).collect()
      assert(avgResult.length == 1, "Expected 1 result row for AVG")

      // COUNT should also work and return the correct number of null rows
      val count = readDf.filter($"value".isNull).count()
      assert(count == 10L, s"Expected 10 null rows but got $count")
    }
  }

  test("COUNT with filter on partition column should work (IsNotNull is tautology)") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create partitioned test data
      val df = (0 until 100).map(i => (i.toLong, s"region_${i % 3}", s"item_$i")).toDF("id", "region", "name")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("region")
        .mode("overwrite")
        .save(testPath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)

      // Spark adds implicit IsNotNull(region) for comparison predicates.
      // This should NOT block aggregate pushdown since partition columns are never null.
      val count = readDf.filter($"region" === "region_0").count()
      // i % 3 == 0 => i = 0,3,6,...,99 => 34 values
      assert(count == 34L, s"Expected 34 rows for region_0 but got $count")
    }
  }

  test("COUNT with filters on multi-partition table should work") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create multi-partition test data
      val df = (0 until 120)
        .map(i => (i.toLong, s"region_${i % 3}", s"type_${i % 2}", s"item_$i"))
        .toDF("id", "region", "type", "name")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("region", "type")
        .mode("overwrite")
        .save(testPath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)

      // Compound filter on both partition columns - Spark adds IsNotNull for both
      val count = readDf.filter($"region" === "region_0" && $"type" === "type_0").count()
      // i % 3 == 0 AND i % 2 == 0 => i % 6 == 0 => i = 0,6,12,...,114 => 20 values
      assert(count == 20L, s"Expected 20 rows but got $count")
    }
  }

  test("GROUP BY partition column with COUNT should work (reproduction case)") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Reproduce the exact scenario: partitioned dataset, indexed, then GROUP BY partition key.
      // Spark adds implicit IsNotNull(partition_key) for GROUP BY, which must be supported
      // to allow aggregate pushdown.
      val df = (0 until 300)
        .map(i => (i.toLong, s"region_${i % 3}", s"item_$i"))
        .toDF("id", "region", "name")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .partitionBy("region")
        .mode("overwrite")
        .save(testPath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)

      // GROUP BY partition column - this is the exact reproduction case.
      // Before the fix, IsNotNull("region") was unsupported which blocked aggregate pushdown.
      val result = readDf.groupBy("region").count().collect()
      assert(result.length == 3, s"Expected 3 groups but got ${result.length}")

      val countMap = result.map(r => r.getString(0) -> r.getLong(1)).toMap
      assert(countMap("region_0") == 100L, s"Expected 100 for region_0 but got ${countMap("region_0")}")
      assert(countMap("region_1") == 100L, s"Expected 100 for region_1 but got ${countMap("region_1")}")
      assert(countMap("region_2") == 100L, s"Expected 100 for region_2 but got ${countMap("region_2")}")
    }
  }

  test("GROUP BY partition column with filter and aggregate should work") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      val df = (0 until 300)
        .map(i => (i.toLong, s"region_${i % 3}", i * 10.0))
        .toDF("id", "region", "value")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.fastfields", "id,value")
        .partitionBy("region")
        .mode("overwrite")
        .save(testPath)

      val readDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(testPath)

      // Filter on partition column + GROUP BY partition column + COUNT
      // Spark adds IsNotNull("region") for both the filter and GROUP BY.
      val result = readDf
        .filter($"region" === "region_0")
        .groupBy("region")
        .count()
        .collect()

      assert(result.length == 1, s"Expected 1 group but got ${result.length}")
      assert(result(0).getLong(1) == 100L, s"Expected 100 for region_0 but got ${result(0).getLong(1)}")
    }
  }
}
