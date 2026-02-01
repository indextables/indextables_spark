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

package io.indextables.spark.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for TablePathNormalizer utility.
 *
 * This utility is critical for credential provider caching - it ensures that different splits from the same table all
 * normalize to the same cache key.
 */
class TablePathNormalizerTest extends AnyFunSuite with Matchers {

  test("normalizeToTablePath should strip .split file extension") {
    TablePathNormalizer.normalizeToTablePath("s3://bucket/table/part-00000.split") shouldBe "s3://bucket/table"
  }

  test("normalizeToTablePath should strip .avro file extension") {
    TablePathNormalizer.normalizeToTablePath("s3://bucket/table/data.avro") shouldBe "s3://bucket/table"
  }

  test("normalizeToTablePath should strip .json file extension") {
    TablePathNormalizer.normalizeToTablePath("s3://bucket/table/metadata.json") shouldBe "s3://bucket/table"
  }

  test("normalizeToTablePath should strip .gz file extension") {
    TablePathNormalizer.normalizeToTablePath("s3://bucket/table/data.json.gz") shouldBe "s3://bucket/table"
  }

  test("normalizeToTablePath should strip Hive-style partition paths") {
    TablePathNormalizer.normalizeToTablePath("s3://bucket/table/year=2024") shouldBe "s3://bucket/table"
    TablePathNormalizer.normalizeToTablePath("s3://bucket/table/year=2024/month=01") shouldBe "s3://bucket/table"
    TablePathNormalizer.normalizeToTablePath("s3://bucket/table/year=2024/month=01/day=15") shouldBe "s3://bucket/table"
  }

  test("normalizeToTablePath should strip both partitions and file extension") {
    TablePathNormalizer.normalizeToTablePath(
      "s3://bucket/table/year=2024/month=01/part-00000.split"
    ) shouldBe "s3://bucket/table"
  }

  test("normalizeToTablePath should strip transaction log paths") {
    TablePathNormalizer.normalizeToTablePath(
      "s3://bucket/table/_transaction_log/00000.json"
    ) shouldBe "s3://bucket/table"
    TablePathNormalizer.normalizeToTablePath(
      "s3://bucket/table/_transaction_log/checkpoint/v1.checkpoint.json"
    ) shouldBe "s3://bucket/table"
  }

  test("normalizeToTablePath should preserve table path without file/partition suffix") {
    TablePathNormalizer.normalizeToTablePath("s3://bucket/table") shouldBe "s3://bucket/table"
    // Note: Trailing slashes are preserved since they don't match data file patterns
    TablePathNormalizer.normalizeToTablePath("s3://bucket/table/") shouldBe "s3://bucket/table/"
  }

  test("normalizeToTablePath should handle nested table paths") {
    TablePathNormalizer.normalizeToTablePath(
      "s3://bucket/data/warehouse/table/part-00000.split"
    ) shouldBe "s3://bucket/data/warehouse/table"
  }

  test("normalizeToTablePath should preserve different S3 protocol variants") {
    TablePathNormalizer.normalizeToTablePath("s3a://bucket/table/part-00000.split") shouldBe "s3a://bucket/table"
    TablePathNormalizer.normalizeToTablePath("s3n://bucket/table/part-00000.split") shouldBe "s3n://bucket/table"
  }

  test("normalizeToTablePath should handle Azure paths") {
    TablePathNormalizer.normalizeToTablePath(
      "abfss://container@account.dfs.core.windows.net/table/part-00000.split"
    ) shouldBe
      "abfss://container@account.dfs.core.windows.net/table"

    TablePathNormalizer.normalizeToTablePath(
      "wasbs://container@account.blob.core.windows.net/table/year=2024/data.split"
    ) shouldBe
      "wasbs://container@account.blob.core.windows.net/table"
  }

  test("normalizeToTablePath should handle null and empty input") {
    TablePathNormalizer.normalizeToTablePath(null) shouldBe null
    TablePathNormalizer.normalizeToTablePath("") shouldBe ""
  }

  test("normalizeToTablePath should not strip paths that look like partitions but aren't") {
    // Directory names with '=' that aren't Hive partitions
    // The method is conservative - it only strips trailing segments with '='
    TablePathNormalizer.normalizeToTablePath("s3://bucket/config=dev/table") shouldBe "s3://bucket/config=dev/table"
  }

  test("isHivePartitionSegment should identify partition segments") {
    TablePathNormalizer.isHivePartitionSegment("year=2024") shouldBe true
    TablePathNormalizer.isHivePartitionSegment("month=01") shouldBe true
    TablePathNormalizer.isHivePartitionSegment("key=value") shouldBe true

    // Edge cases
    TablePathNormalizer.isHivePartitionSegment("") shouldBe false
    TablePathNormalizer.isHivePartitionSegment("=value") shouldBe false
    TablePathNormalizer.isHivePartitionSegment("noequals") shouldBe false
    TablePathNormalizer.isHivePartitionSegment("file.split") shouldBe false
  }
}
