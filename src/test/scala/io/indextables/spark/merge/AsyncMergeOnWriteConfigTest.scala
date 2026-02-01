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

package io.indextables.spark.merge

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AsyncMergeOnWriteConfigTest extends AnyFunSuite with Matchers {

  test("default configuration values") {
    val config = AsyncMergeOnWriteConfig.default

    config.enabled shouldBe false
    config.asyncEnabled shouldBe true
    config.batchCpuFraction shouldBe 0.167
    config.maxConcurrentBatches shouldBe 3
    config.minBatchesToTrigger shouldBe 1
    config.targetSizeBytes shouldBe 4L * 1024 * 1024 * 1024
    config.shutdownTimeoutMs shouldBe 300000L
  }

  test("calculateBatchSize - 24 CPUs with default fraction") {
    val config = AsyncMergeOnWriteConfig.default
    // 24 * 0.167 = 4.008, truncated to 4
    config.calculateBatchSize(24) shouldBe 4
  }

  test("calculateBatchSize - minimum of 1") {
    val config = AsyncMergeOnWriteConfig.default
    // 1 * 0.167 = 0.167, but minimum is 1
    config.calculateBatchSize(1) shouldBe 1
    config.calculateBatchSize(0) shouldBe 1
  }

  test("calculateBatchSize - 100 CPUs with default fraction") {
    val config = AsyncMergeOnWriteConfig.default
    // 100 * 0.167 = 16.7, truncated to 16
    config.calculateBatchSize(100) shouldBe 16
  }

  test("calculateBatchSize - custom fraction") {
    val config = AsyncMergeOnWriteConfig(batchCpuFraction = 0.25)
    // 24 * 0.25 = 6
    config.calculateBatchSize(24) shouldBe 6
  }

  test("calculateMergeThreshold - formula: batchSize * minBatchesToTrigger") {
    val config = AsyncMergeOnWriteConfig.default
    // batchSize(24) = 4, threshold = 4 * 1 = 4
    config.calculateMergeThreshold(24) shouldBe 4
  }

  test("calculateMergeThreshold - with minBatchesToTrigger = 2") {
    val config = AsyncMergeOnWriteConfig(minBatchesToTrigger = 2)
    // batchSize(24) = 4, threshold = 4 * 2 = 8
    config.calculateMergeThreshold(24) shouldBe 8
  }

  test("targetSizeString - formats bytes correctly") {
    val config = AsyncMergeOnWriteConfig.default
    config.targetSizeString shouldBe "4G"

    val config2 = AsyncMergeOnWriteConfig(targetSizeBytes = 512L * 1024 * 1024)
    config2.targetSizeString shouldBe "512M"
  }

  test("parse bytes from string - various units") {
    AsyncMergeOnWriteConfig.parseBytes("1024") shouldBe 1024L
    AsyncMergeOnWriteConfig.parseBytes("1K") shouldBe 1024L
    AsyncMergeOnWriteConfig.parseBytes("1k") shouldBe 1024L
    AsyncMergeOnWriteConfig.parseBytes("2M") shouldBe 2L * 1024 * 1024
    AsyncMergeOnWriteConfig.parseBytes("2m") shouldBe 2L * 1024 * 1024
    AsyncMergeOnWriteConfig.parseBytes("4G") shouldBe 4L * 1024 * 1024 * 1024
    AsyncMergeOnWriteConfig.parseBytes("4g") shouldBe 4L * 1024 * 1024 * 1024
    AsyncMergeOnWriteConfig.parseBytes("1T") shouldBe 1L * 1024 * 1024 * 1024 * 1024
  }

  test("format bytes to string") {
    AsyncMergeOnWriteConfig.formatBytes(512L) shouldBe "512B"
    AsyncMergeOnWriteConfig.formatBytes(1024L) shouldBe "1K"
    AsyncMergeOnWriteConfig.formatBytes(2L * 1024 * 1024) shouldBe "2M"
    AsyncMergeOnWriteConfig.formatBytes(4L * 1024 * 1024 * 1024) shouldBe "4G"
    AsyncMergeOnWriteConfig.formatBytes(1L * 1024 * 1024 * 1024 * 1024) shouldBe "1T"
  }

  test("fromOptions with custom values") {
    val options = new CaseInsensitiveStringMap(
      Map(
        "spark.indextables.mergeOnWrite.enabled"              -> "true",
        "spark.indextables.mergeOnWrite.async.enabled"        -> "false",
        "spark.indextables.mergeOnWrite.batchCpuFraction"     -> "0.25",
        "spark.indextables.mergeOnWrite.maxConcurrentBatches" -> "5",
        "spark.indextables.mergeOnWrite.minBatchesToTrigger"  -> "2",
        "spark.indextables.mergeOnWrite.targetSize"           -> "8G",
        "spark.indextables.mergeOnWrite.shutdownTimeoutMs"    -> "600000"
      ).asJava
    )

    val config = AsyncMergeOnWriteConfig.fromOptions(options)

    config.enabled shouldBe true
    config.asyncEnabled shouldBe false
    config.batchCpuFraction shouldBe 0.25
    config.maxConcurrentBatches shouldBe 5
    config.minBatchesToTrigger shouldBe 2
    config.targetSizeBytes shouldBe 8L * 1024 * 1024 * 1024
    config.shutdownTimeoutMs shouldBe 600000L
  }

  test("fromOptions with default values when not specified") {
    val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
    val config  = AsyncMergeOnWriteConfig.fromOptions(options)

    config shouldBe AsyncMergeOnWriteConfig.default
  }

  test("fromMap with custom values") {
    val configs = Map(
      "spark.indextables.mergeOnWrite.enabled"              -> "true",
      "spark.indextables.mergeOnWrite.batchCpuFraction"     -> "0.5",
      "spark.indextables.mergeOnWrite.maxConcurrentBatches" -> "6"
    )

    val config = AsyncMergeOnWriteConfig.fromMap(configs)

    config.enabled shouldBe true
    config.batchCpuFraction shouldBe 0.5
    config.maxConcurrentBatches shouldBe 6
  }

  test("fromMap is case insensitive") {
    val configs = Map(
      "SPARK.INDEXTABLES.MERGEONWRITE.ENABLED"          -> "true",
      "SPARK.INDEXTABLES.MERGEONWRITE.BATCHCPUFRACTION" -> "0.3"
    )

    val config = AsyncMergeOnWriteConfig.fromMap(configs)
    config.enabled shouldBe true
    config.batchCpuFraction shouldBe 0.3
  }

  test("validate throws on invalid batchCpuFraction - zero") {
    an[IllegalArgumentException] should be thrownBy {
      AsyncMergeOnWriteConfig(batchCpuFraction = 0.0).validate()
    }
  }

  test("validate throws on invalid batchCpuFraction - negative") {
    an[IllegalArgumentException] should be thrownBy {
      AsyncMergeOnWriteConfig(batchCpuFraction = -0.1).validate()
    }
  }

  test("validate throws on invalid batchCpuFraction - greater than 1") {
    an[IllegalArgumentException] should be thrownBy {
      AsyncMergeOnWriteConfig(batchCpuFraction = 1.1).validate()
    }
  }

  test("validate throws on invalid maxConcurrentBatches - zero") {
    an[IllegalArgumentException] should be thrownBy {
      AsyncMergeOnWriteConfig(maxConcurrentBatches = 0).validate()
    }
  }

  test("validate throws on invalid maxConcurrentBatches - negative") {
    an[IllegalArgumentException] should be thrownBy {
      AsyncMergeOnWriteConfig(maxConcurrentBatches = -1).validate()
    }
  }

  test("validate throws on invalid minBatchesToTrigger - zero") {
    an[IllegalArgumentException] should be thrownBy {
      AsyncMergeOnWriteConfig(minBatchesToTrigger = 0).validate()
    }
  }

  test("validate throws on invalid targetSizeBytes - zero") {
    an[IllegalArgumentException] should be thrownBy {
      AsyncMergeOnWriteConfig(targetSizeBytes = 0).validate()
    }
  }

  test("validate throws on invalid shutdownTimeoutMs - zero") {
    an[IllegalArgumentException] should be thrownBy {
      AsyncMergeOnWriteConfig(shutdownTimeoutMs = 0).validate()
    }
  }

  test("validate passes for valid configuration") {
    val config = AsyncMergeOnWriteConfig(
      enabled = true,
      asyncEnabled = true,
      batchCpuFraction = 0.5,
      maxConcurrentBatches = 5,
      minBatchesToTrigger = 2,
      targetSizeBytes = 1L * 1024 * 1024 * 1024,
      shutdownTimeoutMs = 60000L
    )

    noException should be thrownBy {
      config.validate()
    }
  }

  test("validate passes for edge case - batchCpuFraction = 1.0") {
    val config = AsyncMergeOnWriteConfig(batchCpuFraction = 1.0)

    noException should be thrownBy {
      config.validate()
    }
  }

  test("validate passes for edge case - minimum valid values") {
    val config = AsyncMergeOnWriteConfig(
      batchCpuFraction = 0.001, // very small but positive
      maxConcurrentBatches = 1,
      minBatchesToTrigger = 1,
      targetSizeBytes = 1,
      shutdownTimeoutMs = 1
    )

    noException should be thrownBy {
      config.validate()
    }
  }

  test("fromOptions handles invalid boolean gracefully") {
    val options = new CaseInsensitiveStringMap(
      Map(
        "spark.indextables.mergeOnWrite.enabled" -> "not_a_boolean"
      ).asJava
    )

    // Should fall back to default value with warning
    val config = AsyncMergeOnWriteConfig.fromOptions(options)
    config.enabled shouldBe false
  }

  test("fromOptions handles invalid integer gracefully") {
    val options = new CaseInsensitiveStringMap(
      Map(
        "spark.indextables.mergeOnWrite.maxConcurrentBatches" -> "not_an_int"
      ).asJava
    )

    // Should fall back to default value with warning
    val config = AsyncMergeOnWriteConfig.fromOptions(options)
    config.maxConcurrentBatches shouldBe 3
  }

  test("fromOptions handles invalid double gracefully") {
    val options = new CaseInsensitiveStringMap(
      Map(
        "spark.indextables.mergeOnWrite.batchCpuFraction" -> "not_a_double"
      ).asJava
    )

    // Should fall back to default value with warning
    val config = AsyncMergeOnWriteConfig.fromOptions(options)
    config.batchCpuFraction shouldBe 0.167
  }
}
