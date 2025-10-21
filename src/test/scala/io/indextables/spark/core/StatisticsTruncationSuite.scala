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

import io.indextables.spark.util.StatisticsTruncation
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StatisticsTruncationSuite extends AnyFunSuite with Matchers {

  test("statistics truncation should drop long string values by default") {
    val minValues = Map(
      "id"        -> "doc1",
      "long_text" -> ("x" * 500) // Exceeds 256 character limit
    )
    val maxValues = Map(
      "id"        -> "doc999",
      "long_text" -> ("y" * 500)
    )

    val config = Map.empty[String, String] // Use defaults

    val (truncatedMin, truncatedMax) = StatisticsTruncation.truncateStatistics(minValues, maxValues, config)

    // Short values preserved
    truncatedMin should contain key "id"
    truncatedMax should contain key "id"

    // Long values dropped
    truncatedMin should not contain key("long_text")
    truncatedMax should not contain key("long_text")
  }

  test("statistics truncation should respect custom length threshold") {
    val minValues = Map("text" -> ("x" * 300))
    val maxValues = Map("text" -> ("y" * 300))

    val config = Map("spark.indextables.stats.truncation.maxLength" -> "200")

    val (truncatedMin, truncatedMax) = StatisticsTruncation.truncateStatistics(minValues, maxValues, config)

    truncatedMin should not contain key("text")
    truncatedMax should not contain key("text")
  }

  test("statistics truncation should be disabled when configured") {
    val minValues = Map("text" -> ("x" * 500))
    val maxValues = Map("text" -> ("y" * 500))

    val config = Map("spark.indextables.stats.truncation.enabled" -> "false")

    val (truncatedMin, truncatedMax) = StatisticsTruncation.truncateStatistics(minValues, maxValues, config)

    // Statistics should be preserved when truncation is disabled
    truncatedMin should contain key "text"
    truncatedMax should contain key "text"
    truncatedMin("text") should have length 500
    truncatedMax("text") should have length 500
  }

  test("statistics truncation should handle empty maps") {
    val minValues = Map.empty[String, String]
    val maxValues = Map.empty[String, String]

    val config = Map.empty[String, String]

    val (truncatedMin, truncatedMax) = StatisticsTruncation.truncateStatistics(minValues, maxValues, config)

    truncatedMin shouldBe empty
    truncatedMax shouldBe empty
  }

  test("statistics truncation should handle null values gracefully") {
    val minValues = Map("field1" -> "value", "field2" -> null)
    val maxValues = Map("field1" -> "value", "field2" -> null)

    val config = Map.empty[String, String]

    val (truncatedMin, truncatedMax) = StatisticsTruncation.truncateStatistics(minValues, maxValues, config)

    // Non-null short values preserved
    truncatedMin should contain key "field1"
    truncatedMax should contain key "field1"

    // Null values preserved (not considered "long")
    truncatedMin should contain key "field2"
    truncatedMax should contain key "field2"
  }

  test("statistics truncation should drop column if either min or max exceeds threshold") {
    val minValues = Map(
      "field1" -> "short",
      "field2" -> ("x" * 500) // Min exceeds threshold
    )
    val maxValues = Map(
      "field1" -> "short",
      "field2" -> "also_short" // Max is short
    )

    val config = Map.empty[String, String]

    val (truncatedMin, truncatedMax) = StatisticsTruncation.truncateStatistics(minValues, maxValues, config)

    // field1 preserved (both min and max are short)
    truncatedMin should contain key "field1"
    truncatedMax should contain key "field1"

    // field2 dropped (min exceeds threshold, even though max is short)
    truncatedMin should not contain key("field2")
    truncatedMax should not contain key("field2")
  }

  test("statistics truncation should handle mixed short and long values") {
    val minValues = Map(
      "id"      -> "doc1",
      "title"   -> "Short Title",
      "content" -> ("x" * 1000),
      "score"   -> "100"
    )
    val maxValues = Map(
      "id"      -> "doc999",
      "title"   -> "Another Short Title",
      "content" -> ("y" * 1000),
      "score"   -> "999"
    )

    val config = Map.empty[String, String]

    val (truncatedMin, truncatedMax) = StatisticsTruncation.truncateStatistics(minValues, maxValues, config)

    // Short values preserved
    truncatedMin should contain key "id"
    truncatedMin should contain key "title"
    truncatedMin should contain key "score"
    truncatedMax should contain key "id"
    truncatedMax should contain key "title"
    truncatedMax should contain key "score"

    // Long value dropped
    truncatedMin should not contain key("content")
    truncatedMax should not contain key("content")
  }

  test("statistics truncation should respect exactly 256 character threshold") {
    val minValues = Map(
      "exactly_256" -> ("x" * 256), // Exactly at threshold
      "one_more"    -> ("x" * 257)  // One over threshold
    )
    val maxValues = Map(
      "exactly_256" -> ("y" * 256),
      "one_more"    -> ("y" * 257)
    )

    val config = Map.empty[String, String]

    val (truncatedMin, truncatedMax) = StatisticsTruncation.truncateStatistics(minValues, maxValues, config)

    // Exactly 256 characters should be preserved
    truncatedMin should contain key "exactly_256"
    truncatedMax should contain key "exactly_256"

    // 257 characters should be dropped
    truncatedMin should not contain key("one_more")
    truncatedMax should not contain key("one_more")
  }

  test("statistics truncation should handle asymmetric min/max lengths") {
    val minValues = Map(
      "field1" -> "short_min",
      "field2" -> ("x" * 500) // Long min
    )
    val maxValues = Map(
      "field1" -> ("z" * 500), // Long max
      "field2" -> "short_max"
    )

    val config = Map.empty[String, String]

    val (truncatedMin, truncatedMax) = StatisticsTruncation.truncateStatistics(minValues, maxValues, config)

    // Both fields should be dropped (each has one long value)
    truncatedMin should not contain key("field1")
    truncatedMax should not contain key("field1")
    truncatedMin should not contain key("field2")
    truncatedMax should not contain key("field2")
  }

  test("statistics truncation with very large threshold should preserve all values") {
    val minValues = Map(
      "text1" -> ("x" * 500),
      "text2" -> ("x" * 1000)
    )
    val maxValues = Map(
      "text1" -> ("y" * 500),
      "text2" -> ("y" * 1000)
    )

    val config = Map("spark.indextables.stats.truncation.maxLength" -> "10000")

    val (truncatedMin, truncatedMax) = StatisticsTruncation.truncateStatistics(minValues, maxValues, config)

    // All values should be preserved with large threshold
    truncatedMin should contain key "text1"
    truncatedMin should contain key "text2"
    truncatedMax should contain key "text1"
    truncatedMax should contain key "text2"
  }

  test("statistics truncation with zero threshold should drop all non-empty values") {
    val minValues = Map(
      "field1" -> "a",
      "field2" -> "longer"
    )
    val maxValues = Map(
      "field1" -> "b",
      "field2" -> "longer_max"
    )

    val config = Map("spark.indextables.stats.truncation.maxLength" -> "0")

    val (truncatedMin, truncatedMax) = StatisticsTruncation.truncateStatistics(minValues, maxValues, config)

    // All non-empty values should be dropped with zero threshold
    truncatedMin shouldBe empty
    truncatedMax shouldBe empty
  }

  test("statistics truncation should preserve numeric string values under threshold") {
    val minValues = Map(
      "id"          -> "1",
      "score"       -> "100",
      "timestamp"   -> "1234567890",
      "description" -> ("x" * 500) // Long text
    )
    val maxValues = Map(
      "id"          -> "999",
      "score"       -> "9999",
      "timestamp"   -> "9999999999",
      "description" -> ("y" * 500)
    )

    val config = Map.empty[String, String]

    val (truncatedMin, truncatedMax) = StatisticsTruncation.truncateStatistics(minValues, maxValues, config)

    // Numeric strings preserved
    truncatedMin should contain key "id"
    truncatedMin should contain key "score"
    truncatedMin should contain key "timestamp"
    truncatedMax should contain key "id"
    truncatedMax should contain key "score"
    truncatedMax should contain key "timestamp"

    // Long text dropped
    truncatedMin should not contain key("description")
    truncatedMax should not contain key("description")
  }
}
