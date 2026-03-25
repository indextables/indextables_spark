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

package io.indextables.spark.core

import org.apache.spark.sql.connector.expressions.aggregate.{CountStar, AggregateFunc}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for assembleBucketBatch with empty splits (0 rows).
 *
 * Uses CountStar for all aggregate expressions because FieldReference is package-private
 * in Spark 3.5. The fix is about column count sizing, not agg types.
 */
class BucketAggregationEmptySplitTest extends AnyFunSuite with Matchers {

  private def emptyBatch: ColumnarBatch = new ColumnarBatch(Array.empty, 0)

  test("assembleBucketBatch should handle empty split (0 rows) without exception") {
    val schema = StructType(Seq(
      StructField("bucket_key", StringType),
      StructField("cnt", LongType)
    ))
    val aggExprs: Array[AggregateFunc] = Array(new CountStar())
    val dataGroupByCols                = Array("bucket_key")

    val result = GroupByColumnarReaderUtils.assembleBucketBatch(
      emptyBatch, Array.empty[String], aggExprs, dataGroupByCols, schema
    )
    try {
      result.numRows() shouldBe 0
      result.numCols() shouldBe 2
    } finally result.close()
  }

  test("assembleBucketBatch empty split column count equals groupByCols + aggExprs") {
    val schema = StructType(Seq(
      StructField("col1", StringType),
      StructField("col2", StringType),
      StructField("cnt", LongType),
      StructField("total", LongType),
      StructField("min_val", LongType)
    ))
    val aggExprs: Array[AggregateFunc] = Array(
      new CountStar(),
      new CountStar(),
      new CountStar()
    )
    val dataGroupByCols = Array("col1", "col2")

    val result = GroupByColumnarReaderUtils.assembleBucketBatch(
      emptyBatch, Array.empty[String], aggExprs, dataGroupByCols, schema
    )
    try {
      result.numRows() shouldBe 0
      result.numCols() shouldBe 5
    } finally result.close()
  }

  test("assembleBucketBatch empty split with single agg and no GROUP BY columns") {
    val schema = StructType(Seq(
      StructField("cnt", LongType)
    ))
    val aggExprs: Array[AggregateFunc] = Array(new CountStar())
    val dataGroupByCols                = Array.empty[String]

    val result = GroupByColumnarReaderUtils.assembleBucketBatch(
      emptyBatch, Array.empty[String], aggExprs, dataGroupByCols, schema
    )
    try {
      result.numRows() shouldBe 0
      result.numCols() shouldBe 1
    } finally result.close()
  }

  test("assembleBucketBatch empty split should produce columns with correct types") {
    val schema = StructType(Seq(
      StructField("dt", DateType),
      StructField("cnt", LongType)
    ))
    val aggExprs: Array[AggregateFunc] = Array(new CountStar())
    val dataGroupByCols                = Array("dt")

    val result = GroupByColumnarReaderUtils.assembleBucketBatch(
      emptyBatch, Array.empty[String], aggExprs, dataGroupByCols, schema
    )
    try {
      result.numRows() shouldBe 0
      result.numCols() shouldBe 2
      result.column(0).dataType() shouldBe DateType
      result.column(1).dataType() shouldBe LongType
    } finally result.close()
  }

  test("assembleBucketBatch empty split with multiple GROUP BY and multiple aggs") {
    val schema = StructType(Seq(
      StructField("a", StringType),
      StructField("b", StringType),
      StructField("c", StringType),
      StructField("cnt", LongType),
      StructField("total", LongType)
    ))
    val aggExprs: Array[AggregateFunc] = Array(
      new CountStar(),
      new CountStar()
    )
    val dataGroupByCols = Array("a", "b", "c")

    val result = GroupByColumnarReaderUtils.assembleBucketBatch(
      emptyBatch, Array.empty[String], aggExprs, dataGroupByCols, schema
    )
    try {
      result.numRows() shouldBe 0
      result.numCols() shouldBe 5
    } finally result.close()
  }

  test("assembleBucketBatch empty split with multiple GROUP BY cols produces correct column count") {
    val schema = StructType(Seq(
      StructField("x", StringType),
      StructField("y", StringType),
      StructField("cnt", LongType)
    ))
    val aggExprs: Array[AggregateFunc] = Array(new CountStar())
    val dataGroupByCols                = Array("x", "y")

    val result = GroupByColumnarReaderUtils.assembleBucketBatch(
      emptyBatch, Array.empty[String], aggExprs, dataGroupByCols, schema
    )
    try {
      result.numCols() shouldBe 3
    } finally result.close()
  }

  test("assembleBucketBatch empty split with zero GROUP BY cols and zero aggs") {
    val schema          = StructType(Seq.empty)
    val aggExprs        = Array.empty[AggregateFunc]
    val dataGroupByCols = Array.empty[String]

    val result = GroupByColumnarReaderUtils.assembleBucketBatch(
      emptyBatch, Array.empty[String], aggExprs, dataGroupByCols, schema
    )
    try {
      result.numRows() shouldBe 0
      result.numCols() shouldBe 0
    } finally result.close()
  }
}
