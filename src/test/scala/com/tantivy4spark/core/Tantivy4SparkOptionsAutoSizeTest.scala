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

package com.tantivy4spark.core

import com.tantivy4spark.TestBase
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.jdk.CollectionConverters._

/** Tests for auto-sizing options in Tantivy4SparkOptions. */
class Tantivy4SparkOptionsAutoSizeTest extends TestBase {

  test("Tantivy4SparkOptions should parse autoSizeEnabled correctly") {
    // Test enabled
    val enabledOptions        = Map("spark.indextables.autoSize.enabled" -> "true")
    val enabledMap            = new CaseInsensitiveStringMap(enabledOptions.asJava)
    val enabledTantivyOptions = Tantivy4SparkOptions(enabledMap)
    assert(enabledTantivyOptions.autoSizeEnabled.contains(true))

    // Test disabled
    val disabledOptions        = Map("spark.indextables.autoSize.enabled" -> "false")
    val disabledMap            = new CaseInsensitiveStringMap(disabledOptions.asJava)
    val disabledTantivyOptions = Tantivy4SparkOptions(disabledMap)
    assert(disabledTantivyOptions.autoSizeEnabled.contains(false))

    // Test not specified (should be None)
    val emptyOptions        = Map.empty[String, String]
    val emptyMap            = new CaseInsensitiveStringMap(emptyOptions.asJava)
    val emptyTantivyOptions = Tantivy4SparkOptions(emptyMap)
    assert(emptyTantivyOptions.autoSizeEnabled.isEmpty)
  }

  test("Tantivy4SparkOptions should parse autoSizeTargetSplitSize correctly") {
    // Test with megabytes
    val mbOptions        = Map("spark.indextables.autoSize.targetSplitSize" -> "100M")
    val mbMap            = new CaseInsensitiveStringMap(mbOptions.asJava)
    val mbTantivyOptions = Tantivy4SparkOptions(mbMap)
    assert(mbTantivyOptions.autoSizeTargetSplitSize.contains("100M"))

    // Test with gigabytes
    val gbOptions        = Map("spark.indextables.autoSize.targetSplitSize" -> "2G")
    val gbMap            = new CaseInsensitiveStringMap(gbOptions.asJava)
    val gbTantivyOptions = Tantivy4SparkOptions(gbMap)
    assert(gbTantivyOptions.autoSizeTargetSplitSize.contains("2G"))

    // Test with bytes
    val bytesOptions        = Map("spark.indextables.autoSize.targetSplitSize" -> "1048576")
    val bytesMap            = new CaseInsensitiveStringMap(bytesOptions.asJava)
    val bytesTantivyOptions = Tantivy4SparkOptions(bytesMap)
    assert(bytesTantivyOptions.autoSizeTargetSplitSize.contains("1048576"))

    // Test not specified
    val emptyOptions        = Map.empty[String, String]
    val emptyMap            = new CaseInsensitiveStringMap(emptyOptions.asJava)
    val emptyTantivyOptions = Tantivy4SparkOptions(emptyMap)
    assert(emptyTantivyOptions.autoSizeTargetSplitSize.isEmpty)
  }

  test("Tantivy4SparkOptions should parse autoSizeInputRowCount correctly") {
    // Test with valid row count
    val rowCountOptions        = Map("spark.indextables.autoSize.inputRowCount" -> "10000")
    val rowCountMap            = new CaseInsensitiveStringMap(rowCountOptions.asJava)
    val rowCountTantivyOptions = Tantivy4SparkOptions(rowCountMap)
    assert(rowCountTantivyOptions.autoSizeInputRowCount.contains(10000L))

    // Test with large row count
    val largeCountOptions        = Map("spark.indextables.autoSize.inputRowCount" -> "1000000")
    val largeCountMap            = new CaseInsensitiveStringMap(largeCountOptions.asJava)
    val largeCountTantivyOptions = Tantivy4SparkOptions(largeCountMap)
    assert(largeCountTantivyOptions.autoSizeInputRowCount.contains(1000000L))

    // Test not specified
    val emptyOptions        = Map.empty[String, String]
    val emptyMap            = new CaseInsensitiveStringMap(emptyOptions.asJava)
    val emptyTantivyOptions = Tantivy4SparkOptions(emptyMap)
    assert(emptyTantivyOptions.autoSizeInputRowCount.isEmpty)
  }

  test("Tantivy4SparkOptions should handle case insensitive option keys") {
    // Test different cases
    val mixedCaseOptions = Map(
      "spark.indextables.autosize.enabled"         -> "true", // lowercase
      "spark.indextables.AUTOSIZE.TARGETSPLITSIZE" -> "50M",  // uppercase
      "spark.indextables.AutoSize.InputRowCount"   -> "5000"  // mixed case
    )

    val mixedCaseMap   = new CaseInsensitiveStringMap(mixedCaseOptions.asJava)
    val tantivyOptions = Tantivy4SparkOptions(mixedCaseMap)

    // CaseInsensitiveStringMap should handle case variations
    assert(tantivyOptions.autoSizeEnabled.contains(true))          // Should work with case insensitive map
    assert(tantivyOptions.autoSizeTargetSplitSize.contains("50M")) // Should work with case insensitive map
    assert(tantivyOptions.autoSizeInputRowCount.contains(5000L))   // Should work with case insensitive map
  }

  test("Tantivy4SparkOptions should handle invalid autoSizeInputRowCount gracefully") {
    // Test with invalid row count (non-numeric)
    val invalidOptions = Map("spark.indextables.autoSize.inputRowCount" -> "not_a_number")
    val invalidMap     = new CaseInsensitiveStringMap(invalidOptions.asJava)

    intercept[NumberFormatException] {
      val tantivyOptions = Tantivy4SparkOptions(invalidMap)
      tantivyOptions.autoSizeInputRowCount // This should throw when parsed
    }
  }

  test("Tantivy4SparkOptions should combine all options correctly") {
    val fullOptions = Map(
      "spark.indextables.autoSize.enabled"         -> "true",
      "spark.indextables.autoSize.targetSplitSize" -> "100M",
      "spark.indextables.autoSize.inputRowCount"   -> "50000",
      "optimizeWrite"                              -> "true",
      "targetRecordsPerSplit"                      -> "25000"
    )

    val fullMap        = new CaseInsensitiveStringMap(fullOptions.asJava)
    val tantivyOptions = Tantivy4SparkOptions(fullMap)

    // Verify auto-sizing options
    assert(tantivyOptions.autoSizeEnabled.contains(true))
    assert(tantivyOptions.autoSizeTargetSplitSize.contains("100M"))
    assert(tantivyOptions.autoSizeInputRowCount.contains(50000L))

    // Verify other options still work
    assert(tantivyOptions.optimizeWrite.contains(true))
    assert(tantivyOptions.targetRecordsPerSplit.contains(25000L))
  }

  test("Tantivy4SparkOptions constants should match implementation") {
    // Verify that the constants match the actual keys used in the implementation
    assert(Tantivy4SparkOptions.AUTO_SIZE_ENABLED == "spark.indextables.autoSize.enabled")
    assert(Tantivy4SparkOptions.AUTO_SIZE_TARGET_SPLIT_SIZE == "spark.indextables.autoSize.targetSplitSize")
    assert(Tantivy4SparkOptions.AUTO_SIZE_INPUT_ROW_COUNT == "spark.indextables.autoSize.inputRowCount")
  }

  test("Tantivy4SparkOptions should work with Map constructor") {
    val optionsMap = Map(
      "spark.indextables.autoSize.enabled"         -> "true",
      "spark.indextables.autoSize.targetSplitSize" -> "200M"
    )

    val tantivyOptions = Tantivy4SparkOptions(optionsMap)

    assert(tantivyOptions.autoSizeEnabled.contains(true))
    assert(tantivyOptions.autoSizeTargetSplitSize.contains("200M"))
  }

  test("Tantivy4SparkOptions getAllOptions should include auto-sizing options") {
    val options = Map(
      "spark.indextables.autoSize.enabled"         -> "true",
      "spark.indextables.autoSize.targetSplitSize" -> "500M",
      "spark.indextables.autoSize.inputRowCount"   -> "100000",
      "other.option"                               -> "value"
    )

    val optionsMap     = new CaseInsensitiveStringMap(options.asJava)
    val tantivyOptions = Tantivy4SparkOptions(optionsMap)

    val allOptions = tantivyOptions.getAllOptions
    assert(allOptions.contains("spark.indextables.autoSize.enabled"))
    assert(allOptions.contains("spark.indextables.autoSize.targetSplitSize"))
    assert(allOptions.contains("spark.indextables.autoSize.inputRowCount"))
    assert(allOptions.contains("other.option"))

    assert(allOptions("spark.indextables.autoSize.enabled") == "true")
    assert(allOptions("spark.indextables.autoSize.targetSplitSize") == "500M")
    assert(allOptions("spark.indextables.autoSize.inputRowCount") == "100000")
  }

  test("Tantivy4SparkOptions should handle empty and null values gracefully") {
    // Test with empty string values
    val emptyOptions = Map(
      "spark.indextables.autoSize.enabled"         -> "",
      "spark.indextables.autoSize.targetSplitSize" -> "",
      "spark.indextables.autoSize.inputRowCount"   -> ""
    )

    val emptyMap       = new CaseInsensitiveStringMap(emptyOptions.asJava)
    val tantivyOptions = Tantivy4SparkOptions(emptyMap)

    // Empty strings should be treated as None for Option fields
    assert(tantivyOptions.autoSizeEnabled.isEmpty)
    assert(tantivyOptions.autoSizeTargetSplitSize.isEmpty)
    assert(tantivyOptions.autoSizeInputRowCount.isEmpty)
  }
}
