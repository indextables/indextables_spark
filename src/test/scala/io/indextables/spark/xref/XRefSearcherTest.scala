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

package io.indextables.spark.xref

import org.scalatest.BeforeAndAfterEach

import io.indextables.spark.TestBase
import io.indextables.spark.transaction.AddXRefAction

class XRefSearcherTest extends TestBase with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    XRefSearcher.resetAvailabilityCheck()
  }

  // Test query term parsing
  test("parseQueryTerms should parse simple terms") {
    val terms = XRefSearcher.parseQueryTerms("hello world")
    assert(terms == Seq("hello", "world"))
  }

  test("parseQueryTerms should parse quoted phrases") {
    val terms = XRefSearcher.parseQueryTerms("\"hello world\"")
    assert(terms == Seq("hello world"))
  }

  test("parseQueryTerms should parse mixed terms and phrases") {
    val terms = XRefSearcher.parseQueryTerms("foo \"hello world\" bar")
    assert(terms == Seq("foo", "hello world", "bar"))
  }

  test("parseQueryTerms should handle empty string") {
    val terms = XRefSearcher.parseQueryTerms("")
    assert(terms.isEmpty)
  }

  test("parseQueryTerms should handle null") {
    val terms = XRefSearcher.parseQueryTerms(null)
    assert(terms.isEmpty)
  }

  test("parseQueryTerms should handle whitespace-only string") {
    val terms = XRefSearcher.parseQueryTerms("   \t\n  ")
    assert(terms.isEmpty)
  }

  test("parseQueryTerms should handle multiple spaces between terms") {
    val terms = XRefSearcher.parseQueryTerms("hello    world")
    assert(terms == Seq("hello", "world"))
  }

  test("parseQueryTerms should handle unclosed quote") {
    val terms = XRefSearcher.parseQueryTerms("foo \"unclosed phrase")
    assert(terms == Seq("foo", "unclosed phrase"))
  }

  test("parseQueryTerms should handle empty quotes") {
    val terms = XRefSearcher.parseQueryTerms("foo \"\" bar")
    assert(terms == Seq("foo", "bar"))
  }

  test("parseQueryTerms should handle tabs and newlines") {
    val terms = XRefSearcher.parseQueryTerms("hello\tworld\ntest")
    assert(terms == Seq("hello", "world", "test"))
  }

  test("parseQueryTerms should preserve whitespace inside quotes") {
    val terms = XRefSearcher.parseQueryTerms("\"hello   world\"")
    assert(terms == Seq("hello   world"))
  }

  // Test availability check
  test("isAvailable should return false when XRef API not implemented") {
    // Reset and check
    XRefSearcher.resetAvailabilityCheck()
    val available = XRefSearcher.isAvailable()
    // Currently returns false as XRef API is not implemented
    assert(!available)
  }

  test("isAvailable should be idempotent") {
    XRefSearcher.resetAvailabilityCheck()
    val first = XRefSearcher.isAvailable()
    val second = XRefSearcher.isAvailable()
    assert(first == second)
  }

  // Test searchSplits behavior when API not available
  test("searchSplits should return all source splits when API not available") {
    XRefSearcher.resetAvailabilityCheck()

    val sourceSplits = Seq(
      "/table/split1.split",
      "/table/split2.split",
      "/table/split3.split"
    )
    val xref = AddXRefAction(
      path = "_xrefsplits/test/test-xref-001.split",
      xrefId = "test-xref-001",
      sourceSplitPaths = sourceSplits,
      sourceSplitCount = sourceSplits.size,
      size = 10000L,
      totalTerms = 1000L,
      footerStartOffset = 0L,
      footerEndOffset = 100L,
      createdTime = System.currentTimeMillis(),
      buildDurationMs = 1000L,
      maxSourceSplits = 100
    )

    val results = XRefSearcher.searchSplits(
      xrefPath = "/table/_xrefsplits/test/test-xref-001.split",
      xref = xref,
      query = "test query",
      timeoutMs = 5000,
      tablePath = "/table",
      sparkSession = spark
    )

    // Should return all source split filenames when API not available
    assert(results.size == 3)
    assert(results.contains("split1.split"))
    assert(results.contains("split2.split"))
    assert(results.contains("split3.split"))
  }

  test("searchSplits should extract filenames correctly") {
    XRefSearcher.resetAvailabilityCheck()

    val sourceSplits = Seq(
      "s3://bucket/table/date=2024-01-01/part-00001.split",
      "s3://bucket/table/date=2024-01-02/part-00002.split"
    )
    val xref = AddXRefAction(
      path = "_xrefsplits/test/test-xref-002.split",
      xrefId = "test-xref-002",
      sourceSplitPaths = sourceSplits,
      sourceSplitCount = sourceSplits.size,
      size = 5000L,
      totalTerms = 500L,
      footerStartOffset = 0L,
      footerEndOffset = 50L,
      createdTime = System.currentTimeMillis(),
      buildDurationMs = 500L,
      maxSourceSplits = 100
    )

    val results = XRefSearcher.searchSplits(
      xrefPath = "s3://bucket/table/_xrefsplits/test/test-xref-002.split",
      xref = xref,
      query = "search term",
      timeoutMs = 5000,
      tablePath = "s3://bucket/table",
      sparkSession = spark
    )

    // Should extract just filenames
    assert(results.size == 2)
    assert(results.contains("part-00001.split"))
    assert(results.contains("part-00002.split"))
  }
}
