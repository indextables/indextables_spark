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

package com.tantivy4spark.util

import com.tantivy4spark.TestBase

/**
 * Comprehensive tests for SizeParser utility - parsing size strings in various formats.
 */
class SizeParserTest extends TestBase {

  test("SizeParser should parse bytes correctly") {
    assert(SizeParser.parseSize("123456") == 123456L)
    assert(SizeParser.parseSize("0") == 0L)
    assert(SizeParser.parseSize("1") == 1L)
    assert(SizeParser.parseSize("999999999") == 999999999L)
  }

  test("SizeParser should parse megabytes correctly") {
    assert(SizeParser.parseSize("1M") == 1024L * 1024L)
    assert(SizeParser.parseSize("100M") == 100L * 1024L * 1024L)
    assert(SizeParser.parseSize("500M") == 500L * 1024L * 1024L)
    assert(SizeParser.parseSize("1000M") == 1000L * 1024L * 1024L)
  }

  test("SizeParser should parse gigabytes correctly") {
    assert(SizeParser.parseSize("1G") == 1024L * 1024L * 1024L)
    assert(SizeParser.parseSize("2G") == 2L * 1024L * 1024L * 1024L)
    assert(SizeParser.parseSize("10G") == 10L * 1024L * 1024L * 1024L)
  }

  test("SizeParser should parse kilobytes correctly") {
    assert(SizeParser.parseSize("1K") == 1024L)
    assert(SizeParser.parseSize("100K") == 100L * 1024L)
    assert(SizeParser.parseSize("512K") == 512L * 1024L)
  }

  test("SizeParser should handle lowercase units") {
    assert(SizeParser.parseSize("1m") == 1024L * 1024L)
    assert(SizeParser.parseSize("2g") == 2L * 1024L * 1024L * 1024L)
    assert(SizeParser.parseSize("100k") == 100L * 1024L)
  }

  test("SizeParser should handle mixed case units") {
    assert(SizeParser.parseSize("1m") == 1024L * 1024L)
    assert(SizeParser.parseSize("1M") == 1024L * 1024L)
  }

  test("SizeParser should handle whitespace") {
    assert(SizeParser.parseSize("  100M  ") == 100L * 1024L * 1024L)
    assert(SizeParser.parseSize("1G ") == 1024L * 1024L * 1024L)
    assert(SizeParser.parseSize(" 512K") == 512L * 1024L)
  }

  test("SizeParser should throw exception for null input") {
    intercept[IllegalArgumentException] {
      SizeParser.parseSize(null)
    }
  }

  test("SizeParser should throw exception for empty input") {
    intercept[IllegalArgumentException] {
      SizeParser.parseSize("")
    }
    intercept[IllegalArgumentException] {
      SizeParser.parseSize("   ")
    }
  }

  test("SizeParser should throw exception for invalid formats") {
    intercept[IllegalArgumentException] {
      SizeParser.parseSize("abc")
    }
    intercept[IllegalArgumentException] {
      SizeParser.parseSize("100X")
    }
    intercept[IllegalArgumentException] {
      SizeParser.parseSize("M100")
    }
    intercept[IllegalArgumentException] {
      SizeParser.parseSize("100.5M") // No decimal support
    }
  }

  test("SizeParser should throw exception for zero or negative values") {
    intercept[IllegalArgumentException] {
      SizeParser.parseSize("0M")
    }
    intercept[IllegalArgumentException] {
      SizeParser.parseSize("-100M")
    }
    intercept[IllegalArgumentException] {
      SizeParser.parseSize("-1G")
    }
  }

  test("SizeParser should format bytes correctly") {
    assert(SizeParser.formatBytes(123456) == "123456 bytes")
    assert(SizeParser.formatBytes(1) == "1 bytes")
    assert(SizeParser.formatBytes(0) == "0 bytes")
  }

  test("SizeParser should format kilobytes correctly") {
    assert(SizeParser.formatBytes(1024L) == "1K")
    assert(SizeParser.formatBytes(2048L) == "2K")
    assert(SizeParser.formatBytes(512L * 1024L) == "512K")
  }

  test("SizeParser should format megabytes correctly") {
    assert(SizeParser.formatBytes(1024L * 1024L) == "1M")
    assert(SizeParser.formatBytes(100L * 1024L * 1024L) == "100M")
    assert(SizeParser.formatBytes(500L * 1024L * 1024L) == "500M")
  }

  test("SizeParser should format gigabytes correctly") {
    assert(SizeParser.formatBytes(1024L * 1024L * 1024L) == "1G")
    assert(SizeParser.formatBytes(2L * 1024L * 1024L * 1024L) == "2G")
    assert(SizeParser.formatBytes(10L * 1024L * 1024L * 1024L) == "10G")
  }

  test("SizeParser should format non-exact sizes as bytes") {
    assert(SizeParser.formatBytes(1025L) == "1025 bytes") // Not exactly 1K
    assert(SizeParser.formatBytes(1024L * 1024L + 1L) == "1048577 bytes") // Not exactly 1M
    assert(SizeParser.formatBytes(1024L * 1024L * 1024L + 1L) == "1073741825 bytes") // Not exactly 1G
  }

  test("SizeParser should validate size formats correctly") {
    assert(SizeParser.isValidSizeFormat("100M") == true)
    assert(SizeParser.isValidSizeFormat("1G") == true)
    assert(SizeParser.isValidSizeFormat("123456") == true)
    assert(SizeParser.isValidSizeFormat("512K") == true)

    assert(SizeParser.isValidSizeFormat("abc") == false)
    assert(SizeParser.isValidSizeFormat("100X") == false)
    assert(SizeParser.isValidSizeFormat("") == false)
    assert(SizeParser.isValidSizeFormat(null) == false)
    assert(SizeParser.isValidSizeFormat("0M") == false)
    assert(SizeParser.isValidSizeFormat("-100M") == false)
  }

  test("SizeParser should handle round trip conversion correctly") {
    val testSizes = List(
      1024L,         // 1K
      1024L * 1024L, // 1M
      2L * 1024L * 1024L, // 2M
      1024L * 1024L * 1024L, // 1G
      5L * 1024L * 1024L * 1024L  // 5G
    )

    testSizes.foreach { size =>
      val formatted = SizeParser.formatBytes(size)
      val parsed = SizeParser.parseSize(formatted)
      assert(parsed == size, s"Round trip failed: $size -> $formatted -> $parsed")
    }
  }

  test("SizeParser should handle edge cases for large values") {
    // Test with large but valid values
    assert(SizeParser.parseSize("1000G") == 1000L * 1024L * 1024L * 1024L)

    // Test formatting large values
    val largeSize = 1000L * 1024L * 1024L * 1024L
    assert(SizeParser.formatBytes(largeSize) == "1000G")
  }
}