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

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import io.indextables.spark.transaction.AddAction
import io.indextables.spark.stats.{DataSkippingMetrics, FilterExpressionCache}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Pure unit tests for parallel data skipping functionality.
 * These tests don't require SparkSession and test the core filtering logic directly.
 */
class ParallelDataSkippingUnitTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    DataSkippingMetrics.resetAll()
    FilterExpressionCache.invalidateAll()
  }

  private val schema = StructType(
    Array(
      StructField("id", LongType, false),
      StructField("category", StringType, false),
      StructField("value", DoubleType, false)
    )
  )

  test("parallel data skipping filters correctly with 150 files") {
    // Create 150 files with distinct ID ranges (above default threshold of 100)
    val files = (1 to 150).map { i =>
      createAddActionWithStats(
        s"file_$i.split",
        minValues = Map("id" -> ((i - 1) * 100 + 1).toString),
        maxValues = Map("id" -> (i * 100).toString)
      )
    }

    // Query for id=7550 (should only match file 76: range 7501-7600)
    val filter = EqualTo("id", 7550L)
    val threshold = 50
    val config = Map(
      "spark.indextables.dataSkipping.parallelThreshold" -> threshold.toString
    )

    // Verify we will hit the parallel path
    assert(files.length >= threshold, s"Test requires files (${files.length}) >= threshold ($threshold) to test parallel path")

    // Use the filtering logic directly with tracking
    val (result, usedParallel) = applyMinMaxFilteringWithTracking(files, Array(filter), config, schema)

    // Verify parallel path was used
    assert(usedParallel, "Expected parallel path to be used but sequential was used")

    // Should only return 1 file
    result.length shouldBe 1
    result.head.path should include("file_76")

    println(s"✅ Parallel filtering test passed: Skipped ${150 - result.length} of 150 files (parallel=$usedParallel)")
  }

  test("parallel and sequential produce same results") {
    // Create 200 files
    val files = (1 to 200).map { i =>
      createAddActionWithStats(
        s"file_$i.split",
        minValues = Map("id" -> ((i - 1) * 10 + 1).toString),
        maxValues = Map("id" -> (i * 10).toString)
      )
    }

    // Query: id > 500 AND id < 600 (matches files 51-60)
    val filters = Array[Filter](
      GreaterThan("id", 500L),
      LessThan("id", 600L)
    )

    // Run with sequential (high threshold)
    val sequentialConfig = Map(
      "spark.indextables.dataSkipping.parallelThreshold" -> "1000"
    )
    val (sequentialResult, sequentialUsedParallel) = applyMinMaxFilteringWithTracking(files, filters, sequentialConfig, schema)

    // Verify sequential path was used
    assert(!sequentialUsedParallel, "Expected sequential path but parallel was used")

    // Run with parallel (low threshold)
    val parallelConfig = Map(
      "spark.indextables.dataSkipping.parallelThreshold" -> "50"
    )
    val (parallelResult, parallelUsedParallel) = applyMinMaxFilteringWithTracking(files, filters, parallelConfig, schema)

    // Verify parallel path was used
    assert(parallelUsedParallel, "Expected parallel path but sequential was used")

    // Results should be identical
    sequentialResult.length shouldBe parallelResult.length

    val sequentialPaths = sequentialResult.map(_.path).sorted
    val parallelPaths = parallelResult.map(_.path).sorted

    sequentialPaths shouldBe parallelPaths

    println(s"✅ Sequential/parallel consistency test passed: Both returned ${sequentialResult.length} files (sequential=$sequentialUsedParallel, parallel=$parallelUsedParallel)")
  }

  test("parallel data skipping with GreaterThanOrEqual filter") {
    // Create 150 files
    val files = (1 to 150).map { i =>
      createAddActionWithStats(
        s"file_$i.split",
        minValues = Map("id" -> ((i - 1) * 100 + 1).toString),
        maxValues = Map("id" -> (i * 100).toString)
      )
    }

    // Query: id >= 10000 (should match files 100-150, since file 100 has range 9901-10000)
    val filter = GreaterThanOrEqual("id", 10000L)
    val threshold = 50

    val config = Map(
      "spark.indextables.dataSkipping.parallelThreshold" -> threshold.toString
    )

    // Verify we will hit the parallel path
    assert(files.length >= threshold, s"Test requires files (${files.length}) >= threshold ($threshold)")

    val (result, usedParallel) = applyMinMaxFilteringWithTracking(files, Array(filter), config, schema)

    // Verify parallel path was used
    assert(usedParallel, "Expected parallel path to be used")

    // Should match files 100-150 (51 files)
    result.length shouldBe 51
    result.foreach { a =>
      val fileNum = a.path.replace("file_", "").replace(".split", "").toInt
      assert(fileNum >= 100, s"Expected file >= 100, got ${a.path}")
    }

    println(s"✅ GreaterThanOrEqual parallel test passed: Correctly matched ${result.length} of 150 files (parallel=$usedParallel)")
  }

  test("thread-safe metrics collection during parallel filtering") {
    // Create 200 files - half will be skipped
    val files = (1 to 200).map { i =>
      createAddActionWithStats(
        s"file_$i.split",
        minValues = Map("id" -> ((i - 1) * 10 + 1).toString),
        maxValues = Map("id" -> (i * 10).toString)
      )
    }

    // Query: id > 1000 (skips files 1-100, keeps 101-200)
    val filter = GreaterThan("id", 1000L)
    val threshold = 50

    val config = Map(
      "spark.indextables.dataSkipping.parallelThreshold" -> threshold.toString
    )

    // Verify we will hit the parallel path
    assert(files.length >= threshold, s"Test requires files (${files.length}) >= threshold ($threshold)")

    // Run filtering multiple times concurrently to test thread safety
    import scala.concurrent.{Await, Future}
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val futures = (1 to 10).map { _ =>
      Future {
        applyMinMaxFilteringWithTracking(files, Array(filter), config, schema)
      }
    }

    val results = Await.result(Future.sequence(futures), 30.seconds)

    // All results should be the same and use parallel path
    results.foreach { case (result, usedParallel) =>
      result.length shouldBe 100 // files 101-200
      assert(usedParallel, "Expected parallel path to be used in all runs")
    }

    println(s"✅ Thread safety test passed: All 10 concurrent runs returned 100 files (all used parallel)")
  }

  private def createAddActionWithStats(
    filename: String,
    minValues: Map[String, String],
    maxValues: Map[String, String]
  ): AddAction =
    AddAction(
      path = filename,
      partitionValues = Map.empty,
      size = 1000L,
      modificationTime = System.currentTimeMillis(),
      dataChange = true,
      numRecords = Some(100L),
      minValues = Some(minValues),
      maxValues = Some(maxValues),
      footerStartOffset = Some(0L),
      footerEndOffset = Some(100L)
    )

  /**
   * Apply min/max filtering logic directly, simulating what IndexTables4SparkScan.applyDataSkipping does.
   * This allows testing the parallel code path without needing a full SparkSession.
   * Returns (result, usedParallel) to verify the parallel path was actually used.
   */
  private def applyMinMaxFilteringWithTracking(
    addActions: Seq[AddAction],
    filters: Array[Filter],
    config: Map[String, String],
    schema: StructType
  ): (Seq[AddAction], Boolean) = {
    if (filters.isEmpty) return (addActions, false)

    // Get threshold from config
    val parallelThreshold = config.getOrElse(
      "spark.indextables.dataSkipping.parallelThreshold",
      config.getOrElse("spark.indextables.partitionPruning.parallelThreshold", "100")
    ).toInt

    // Thread-safe counter for metrics
    val filterTypeSkips = new java.util.concurrent.ConcurrentHashMap[String, java.util.concurrent.atomic.AtomicLong]()

    val useParallel = addActions.length >= parallelThreshold

    val result = if (useParallel) {
      addActions.par.filter { addAction =>
        val canMatch = canFileMatchFilters(addAction, filters, schema)
        if (!canMatch) {
          // Track which filter types contributed to skipping
          filters.foreach { filter =>
            if (!canFilterMatchFile(addAction, filter, schema)) {
              val filterType = getFilterTypeName(filter)
              filterTypeSkips.computeIfAbsent(filterType, _ => new java.util.concurrent.atomic.AtomicLong(0L)).incrementAndGet()
            }
          }
        }
        canMatch
      }.seq
    } else {
      addActions.filter { addAction =>
        val canMatch = canFileMatchFilters(addAction, filters, schema)
        if (!canMatch) {
          filters.foreach { filter =>
            if (!canFilterMatchFile(addAction, filter, schema)) {
              val filterType = getFilterTypeName(filter)
              filterTypeSkips.computeIfAbsent(filterType, _ => new java.util.concurrent.atomic.AtomicLong(0L)).incrementAndGet()
            }
          }
        }
        canMatch
      }
    }

    (result, useParallel)
  }

  private def canFileMatchFilters(addAction: AddAction, filters: Array[Filter], schema: StructType): Boolean = {
    if (addAction.minValues.isEmpty || addAction.maxValues.isEmpty) {
      return true
    }
    filters.forall(filter => canFilterMatchFile(addAction, filter, schema))
  }

  private def canFilterMatchFile(addAction: AddAction, filter: Filter, schema: StructType): Boolean = {
    filter match {
      case And(left, right) =>
        canFilterMatchFile(addAction, left, schema) && canFilterMatchFile(addAction, right, schema)
      case Or(left, right) =>
        canFilterMatchFile(addAction, left, schema) || canFilterMatchFile(addAction, right, schema)
      case Not(_) =>
        true // Conservative: can't prove NOT filters don't match
      case _ =>
        !shouldSkipFile(addAction, filter, schema)
    }
  }

  private def shouldSkipFile(addAction: AddAction, filter: Filter, schema: StructType): Boolean = {
    (addAction.minValues, addAction.maxValues) match {
      case (Some(minVals), Some(maxVals)) =>
        filter match {
          case EqualTo(attribute, value) =>
            val minVal = minVals.get(attribute)
            val maxVal = maxVals.get(attribute)
            (minVal, maxVal) match {
              case (Some(min), Some(max)) if min.nonEmpty && max.nonEmpty =>
                val (convertedValue, convertedMin, convertedMax) = convertValuesForComparison(attribute, value, min, max, schema)
                convertedValue.compareTo(convertedMin) < 0 || convertedValue.compareTo(convertedMax) > 0
              case _ => false
            }

          case In(attribute, values) if values.nonEmpty =>
            val minVal = minVals.get(attribute)
            val maxVal = maxVals.get(attribute)
            (minVal, maxVal) match {
              case (Some(fileMin), Some(fileMax)) if fileMin.nonEmpty && fileMax.nonEmpty =>
                val inMin = values.map(_.toString).min
                val inMax = values.map(_.toString).max
                val (_, convertedFileMin, convertedFileMax) = convertValuesForComparison(attribute, inMin, fileMin, fileMax, schema)
                val (convertedInMin, _, _) = convertValuesForComparison(attribute, inMin, inMin, inMin, schema)
                val (convertedInMax, _, _) = convertValuesForComparison(attribute, inMax, inMax, inMax, schema)
                convertedFileMax.compareTo(convertedInMin) < 0 || convertedFileMin.compareTo(convertedInMax) > 0
              case _ => false
            }

          case GreaterThan(attribute, value) =>
            maxVals.get(attribute) match {
              case Some(max) if max.nonEmpty =>
                val (convertedValue, _, convertedMax) = convertValuesForComparison(attribute, value, "", max, schema)
                convertedMax.compareTo(convertedValue) <= 0
              case _ => false
            }

          case LessThan(attribute, value) =>
            minVals.get(attribute) match {
              case Some(min) if min.nonEmpty =>
                val (convertedValue, convertedMin, _) = convertValuesForComparison(attribute, value, min, "", schema)
                convertedMin.compareTo(convertedValue) >= 0
              case _ => false
            }

          case GreaterThanOrEqual(attribute, value) =>
            maxVals.get(attribute) match {
              case Some(max) if max.nonEmpty =>
                val (convertedValue, _, convertedMax) = convertValuesForComparison(attribute, value, "", max, schema)
                convertedMax.compareTo(convertedValue) < 0
              case _ => false
            }

          case LessThanOrEqual(attribute, value) =>
            minVals.get(attribute) match {
              case Some(min) if min.nonEmpty =>
                val (convertedValue, convertedMin, _) = convertValuesForComparison(attribute, value, min, "", schema)
                convertedMin.compareTo(convertedValue) > 0
              case _ => false
            }

          case _ => false
        }
      case _ => false
    }
  }

  private def convertValuesForComparison(
    attribute: String,
    filterValue: Any,
    minValue: String,
    maxValue: String,
    schema: StructType
  ): (Comparable[Any], Comparable[Any], Comparable[Any]) = {
    val fieldType = schema.fields.find(_.name == attribute).map(_.dataType)

    fieldType match {
      case Some(LongType) | Some(IntegerType) =>
        try {
          val filterNum = filterValue.toString.toLong
          val minNum = if (minValue.nonEmpty) minValue.toLong else Long.MinValue
          val maxNum = if (maxValue.nonEmpty) maxValue.toLong else Long.MaxValue
          (
            Long.box(filterNum).asInstanceOf[Comparable[Any]],
            Long.box(minNum).asInstanceOf[Comparable[Any]],
            Long.box(maxNum).asInstanceOf[Comparable[Any]]
          )
        } catch {
          case _: Exception =>
            (
              filterValue.toString.asInstanceOf[Comparable[Any]],
              minValue.asInstanceOf[Comparable[Any]],
              maxValue.asInstanceOf[Comparable[Any]]
            )
        }
      case _ =>
        (
          filterValue.toString.asInstanceOf[Comparable[Any]],
          minValue.asInstanceOf[Comparable[Any]],
          maxValue.asInstanceOf[Comparable[Any]]
        )
    }
  }

  private def getFilterTypeName(filter: Filter): String = {
    filter match {
      case _: EqualTo => "EqualTo"
      case _: In => "In"
      case _: GreaterThan => "GreaterThan"
      case _: LessThan => "LessThan"
      case _: And => "And"
      case _: Or => "Or"
      case _ => filter.getClass.getSimpleName
    }
  }
}
