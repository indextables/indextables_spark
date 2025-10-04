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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Unit tests for auto-sizing options in IndexTables4SparkOptions that don't require Spark. */
class AutoSizeOptionsUnitTest extends AnyFunSuite with Matchers {

  test("IndexTables4SparkOptions should parse autoSizeEnabled correctly") {
    // Test enabled
    val enabledOptions        = Map("spark.indextables.autoSize.enabled" -> "true")
    val enabledMap            = new CaseInsensitiveStringMap(enabledOptions.asJava)
    val enabledTantivyOptions = IndexTables4SparkOptions(enabledMap)
    assert(enabledTantivyOptions.autoSizeEnabled.contains(true))

    // Test disabled
    val disabledOptions        = Map("spark.indextables.autoSize.enabled" -> "false")
    val disabledMap            = new CaseInsensitiveStringMap(disabledOptions.asJava)
    val disabledTantivyOptions = IndexTables4SparkOptions(disabledMap)
    assert(disabledTantivyOptions.autoSizeEnabled.contains(false))

    // Test not specified (should be None)
    val emptyOptions        = Map.empty[String, String]
    val emptyMap            = new CaseInsensitiveStringMap(emptyOptions.asJava)
    val emptyTantivyOptions = IndexTables4SparkOptions(emptyMap)
    assert(emptyTantivyOptions.autoSizeEnabled.isEmpty)
  }

  test("IndexTables4SparkOptions should parse autoSizeTargetSplitSize correctly") {
    // Test with megabytes
    val mbOptions        = Map("spark.indextables.autoSize.targetSplitSize" -> "100M")
    val mbMap            = new CaseInsensitiveStringMap(mbOptions.asJava)
    val mbTantivyOptions = IndexTables4SparkOptions(mbMap)
    assert(mbTantivyOptions.autoSizeTargetSplitSize.contains("100M"))

    // Test with gigabytes
    val gbOptions        = Map("spark.indextables.autoSize.targetSplitSize" -> "2G")
    val gbMap            = new CaseInsensitiveStringMap(gbOptions.asJava)
    val gbTantivyOptions = IndexTables4SparkOptions(gbMap)
    assert(gbTantivyOptions.autoSizeTargetSplitSize.contains("2G"))

    // Test with bytes
    val bytesOptions        = Map("spark.indextables.autoSize.targetSplitSize" -> "1048576")
    val bytesMap            = new CaseInsensitiveStringMap(bytesOptions.asJava)
    val bytesTantivyOptions = IndexTables4SparkOptions(bytesMap)
    assert(bytesTantivyOptions.autoSizeTargetSplitSize.contains("1048576"))

    // Test not specified
    val emptyOptions        = Map.empty[String, String]
    val emptyMap            = new CaseInsensitiveStringMap(emptyOptions.asJava)
    val emptyTantivyOptions = IndexTables4SparkOptions(emptyMap)
    assert(emptyTantivyOptions.autoSizeTargetSplitSize.isEmpty)
  }

  test("IndexTables4SparkOptions should parse autoSizeInputRowCount correctly") {
    // Test with valid row count
    val rowCountOptions        = Map("spark.indextables.autoSize.inputRowCount" -> "10000")
    val rowCountMap            = new CaseInsensitiveStringMap(rowCountOptions.asJava)
    val rowCountTantivyOptions = IndexTables4SparkOptions(rowCountMap)
    assert(rowCountTantivyOptions.autoSizeInputRowCount.contains(10000L))

    // Test with large row count
    val largeCountOptions        = Map("spark.indextables.autoSize.inputRowCount" -> "1000000")
    val largeCountMap            = new CaseInsensitiveStringMap(largeCountOptions.asJava)
    val largeCountTantivyOptions = IndexTables4SparkOptions(largeCountMap)
    assert(largeCountTantivyOptions.autoSizeInputRowCount.contains(1000000L))

    // Test not specified
    val emptyOptions        = Map.empty[String, String]
    val emptyMap            = new CaseInsensitiveStringMap(emptyOptions.asJava)
    val emptyTantivyOptions = IndexTables4SparkOptions(emptyMap)
    assert(emptyTantivyOptions.autoSizeInputRowCount.isEmpty)
  }

  test("IndexTables4SparkOptions should handle invalid autoSizeInputRowCount gracefully") {
    // Test with invalid row count (non-numeric)
    val invalidOptions = Map("spark.indextables.autoSize.inputRowCount" -> "not_a_number")
    val invalidMap     = new CaseInsensitiveStringMap(invalidOptions.asJava)

    intercept[NumberFormatException] {
      val tantivyOptions = IndexTables4SparkOptions(invalidMap)
      tantivyOptions.autoSizeInputRowCount // This should throw when parsed
    }
  }

  test("IndexTables4SparkOptions should combine all options correctly") {
    val fullOptions = Map(
      "spark.indextables.autoSize.enabled"         -> "true",
      "spark.indextables.autoSize.targetSplitSize" -> "100M",
      "spark.indextables.autoSize.inputRowCount"   -> "50000",
      "optimizeWrite"                              -> "true",
      "targetRecordsPerSplit"                      -> "25000"
    )

    val fullMap        = new CaseInsensitiveStringMap(fullOptions.asJava)
    val tantivyOptions = IndexTables4SparkOptions(fullMap)

    // Verify auto-sizing options
    assert(tantivyOptions.autoSizeEnabled.contains(true))
    assert(tantivyOptions.autoSizeTargetSplitSize.contains("100M"))
    assert(tantivyOptions.autoSizeInputRowCount.contains(50000L))

    // Verify other options still work
    assert(tantivyOptions.optimizeWrite.contains(true))
    assert(tantivyOptions.targetRecordsPerSplit.contains(25000L))
  }

  test("IndexTables4SparkOptions constants should match implementation") {
    // Verify that the constants match the actual keys used in the implementation
    assert(IndexTables4SparkOptions.AUTO_SIZE_ENABLED == "spark.indextables.autoSize.enabled")
    assert(IndexTables4SparkOptions.AUTO_SIZE_TARGET_SPLIT_SIZE == "spark.indextables.autoSize.targetSplitSize")
    assert(IndexTables4SparkOptions.AUTO_SIZE_INPUT_ROW_COUNT == "spark.indextables.autoSize.inputRowCount")
  }

  test("IndexTables4SparkOptions should work with Map constructor") {
    val optionsMap = Map(
      "spark.indextables.autoSize.enabled"         -> "true",
      "spark.indextables.autoSize.targetSplitSize" -> "200M"
    )

    val tantivyOptions = IndexTables4SparkOptions(optionsMap)

    assert(tantivyOptions.autoSizeEnabled.contains(true))
    assert(tantivyOptions.autoSizeTargetSplitSize.contains("200M"))
  }

  test("IndexTables4SparkOptions getAllOptions should include auto-sizing options") {
    val options = Map(
      "spark.indextables.autoSize.enabled"         -> "true",
      "spark.indextables.autoSize.targetSplitSize" -> "500M",
      "spark.indextables.autoSize.inputRowCount"   -> "100000",
      "other.option"                               -> "value"
    )

    val optionsMap     = new CaseInsensitiveStringMap(options.asJava)
    val tantivyOptions = IndexTables4SparkOptions(optionsMap)

    val allOptions = tantivyOptions.getAllOptions
    assert(allOptions.contains("spark.indextables.autoSize.enabled"))
    assert(allOptions.contains("spark.indextables.autoSize.targetSplitSize"))
    assert(allOptions.contains("spark.indextables.autoSize.inputRowCount"))
    assert(allOptions.contains("other.option"))

    assert(allOptions("spark.indextables.autoSize.enabled") == "true")
    assert(allOptions("spark.indextables.autoSize.targetSplitSize") == "500M")
    assert(allOptions("spark.indextables.autoSize.inputRowCount") == "100000")
  }

  test("IndexTables4SparkOptions should handle empty and null values gracefully") {
    // Test with empty string values
    val emptyOptions = Map(
      "spark.indextables.autoSize.enabled"         -> "",
      "spark.indextables.autoSize.targetSplitSize" -> "",
      "spark.indextables.autoSize.inputRowCount"   -> ""
    )

    val emptyMap       = new CaseInsensitiveStringMap(emptyOptions.asJava)
    val tantivyOptions = IndexTables4SparkOptions(emptyMap)

    // Empty strings should be treated as None for Option fields
    assert(tantivyOptions.autoSizeEnabled.isEmpty)
    assert(tantivyOptions.autoSizeTargetSplitSize.isEmpty)
    assert(tantivyOptions.autoSizeInputRowCount.isEmpty)
  }

  test("IndexTables4SparkOptions should handle boolean parsing edge cases") {
    // Test different boolean representations
    val trueVariants  = List("true", "TRUE", "True", "1", "yes", "on")
    val falseVariants = List("false", "FALSE", "False", "0", "no", "off")

    trueVariants.foreach { trueValue =>
      val options        = Map("spark.indextables.autoSize.enabled" -> trueValue)
      val map            = new CaseInsensitiveStringMap(options.asJava)
      val tantivyOptions = IndexTables4SparkOptions(map)
      assert(tantivyOptions.autoSizeEnabled.contains(true), s"Should parse '$trueValue' as true")
    }

    falseVariants.foreach { falseValue =>
      val options        = Map("spark.indextables.autoSize.enabled" -> falseValue)
      val map            = new CaseInsensitiveStringMap(options.asJava)
      val tantivyOptions = IndexTables4SparkOptions(map)
      assert(tantivyOptions.autoSizeEnabled.contains(false), s"Should parse '$falseValue' as false")
    }
  }

  test("IndexTables4SparkOptions should handle numeric parsing edge cases") {
    // Test zero row count
    val zeroOptions        = Map("spark.indextables.autoSize.inputRowCount" -> "0")
    val zeroMap            = new CaseInsensitiveStringMap(zeroOptions.asJava)
    val zeroTantivyOptions = IndexTables4SparkOptions(zeroMap)
    assert(zeroTantivyOptions.autoSizeInputRowCount.contains(0L))

    // Test very large row count
    val largeOptions        = Map("spark.indextables.autoSize.inputRowCount" -> "9223372036854775807") // Long.MAX_VALUE
    val largeMap            = new CaseInsensitiveStringMap(largeOptions.asJava)
    val largeTantivyOptions = IndexTables4SparkOptions(largeMap)
    assert(largeTantivyOptions.autoSizeInputRowCount.contains(Long.MaxValue))
  }
}
