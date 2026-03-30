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

package io.indextables.spark.sync

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExtractTableBasePathTest extends AnyFunSuite with Matchers {

  test("file:// URI without partitions") {
    SyncTaskExecutor.extractTableBasePath("file:///path/to/table/part-00000.parquet") shouldBe
      "file:///path/to/table"
  }

  test("file:// URI with Hive-style partitions") {
    SyncTaskExecutor.extractTableBasePath(
      "file:///path/to/table/key=val/part-00000.parquet"
    ) shouldBe "file:///path/to/table"
  }

  test("file:// URI with multiple partition segments") {
    SyncTaskExecutor.extractTableBasePath(
      "file:///warehouse/db/table/data/region=us-east/date=2024-01-01/part-00000.parquet"
    ) shouldBe "file:///warehouse/db/table/data"
  }

  test("s3:// path without partitions") {
    SyncTaskExecutor.extractTableBasePath("s3://bucket/warehouse/db/table/file.parquet") shouldBe
      "s3://bucket/warehouse/db/table"
  }

  test("s3:// path with Hive-style partitions") {
    SyncTaskExecutor.extractTableBasePath(
      "s3://bucket/warehouse/db/table/data/region=us-east/file.parquet"
    ) shouldBe "s3://bucket/warehouse/db/table/data"
  }

  test("s3a:// path preserves scheme") {
    SyncTaskExecutor.extractTableBasePath("s3a://bucket/table/data/file.parquet") shouldBe
      "s3a://bucket/table/data"
  }

  test("bare path without scheme") {
    SyncTaskExecutor.extractTableBasePath("/tmp/table/data/part-00000.parquet") shouldBe
      "/tmp/table/data"
  }

  test("bare path with Hive-style partitions") {
    SyncTaskExecutor.extractTableBasePath("/tmp/table/data/key=val/part-00000.parquet") shouldBe
      "/tmp/table/data"
  }
}
