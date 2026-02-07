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

package io.indextables.spark.write

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OptimizedWriteConfigTest extends AnyFunSuite with Matchers {

  private def makeOptions(entries: (String, String)*): CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(entries.toMap.asJava)

  test("default values") {
    val config = OptimizedWriteConfig.default
    config.enabled shouldBe false
    config.targetSplitSizeBytes shouldBe (1L * 1024 * 1024 * 1024)
    config.samplingRatio shouldBe 1.1
    config.minRowsForEstimation shouldBe 10000L
    config.distributionMode shouldBe "hash"
    config.maxSplitSizeBytes shouldBe (4L * 1024 * 1024 * 1024)
  }

  test("fromOptions with all defaults") {
    val config = OptimizedWriteConfig.fromOptions(makeOptions())
    config.enabled shouldBe false
    config.targetSplitSizeBytes shouldBe (1L * 1024 * 1024 * 1024)
    config.samplingRatio shouldBe 1.1
    config.minRowsForEstimation shouldBe 10000L
    config.distributionMode shouldBe "hash"
    config.maxSplitSizeBytes shouldBe (4L * 1024 * 1024 * 1024)
  }

  test("fromOptions with all values specified") {
    val config = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_ENABLED -> "true",
      OptimizedWriteConfig.KEY_TARGET_SPLIT_SIZE -> "512M",
      OptimizedWriteConfig.KEY_SAMPLING_RATIO -> "1.5",
      OptimizedWriteConfig.KEY_MIN_ROWS_FOR_EST -> "5000",
      OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "none"
    ))
    config.enabled shouldBe true
    config.targetSplitSizeBytes shouldBe (512L * 1024 * 1024)
    config.samplingRatio shouldBe 1.5
    config.minRowsForEstimation shouldBe 5000L
    config.distributionMode shouldBe "none"
  }

  test("fromOptions parses size strings correctly") {
    val config1G = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_TARGET_SPLIT_SIZE -> "1G"
    ))
    config1G.targetSplitSizeBytes shouldBe (1L * 1024 * 1024 * 1024)

    val config2G = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_TARGET_SPLIT_SIZE -> "2G"
    ))
    config2G.targetSplitSizeBytes shouldBe (2L * 1024 * 1024 * 1024)

    val config256M = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_TARGET_SPLIT_SIZE -> "256M"
    ))
    config256M.targetSplitSizeBytes shouldBe (256L * 1024 * 1024)
  }

  test("fromOptions falls back to defaults for invalid boolean") {
    val config = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_ENABLED -> "notabool"
    ))
    config.enabled shouldBe false
  }

  test("fromOptions falls back to defaults for invalid sampling ratio") {
    // Zero ratio
    val config1 = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_SAMPLING_RATIO -> "0"
    ))
    config1.samplingRatio shouldBe 1.1

    // Negative ratio
    val config2 = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_SAMPLING_RATIO -> "-1.0"
    ))
    config2.samplingRatio shouldBe 1.1

    // Non-numeric
    val config3 = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_SAMPLING_RATIO -> "abc"
    ))
    config3.samplingRatio shouldBe 1.1
  }

  test("fromOptions falls back to defaults for invalid minRows") {
    val config = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_MIN_ROWS_FOR_EST -> "-100"
    ))
    config.minRowsForEstimation shouldBe 10000L
  }

  test("fromOptions falls back to defaults for invalid distribution mode") {
    val config = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "invalid"
    ))
    config.distributionMode shouldBe "hash"
  }

  test("fromOptions distribution mode is case-insensitive") {
    val config = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "HASH"
    ))
    config.distributionMode shouldBe "hash"

    val config2 = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "None"
    ))
    config2.distributionMode shouldBe "none"
  }

  test("fromOptions falls back to defaults for invalid size string") {
    val config = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_TARGET_SPLIT_SIZE -> "notasize"
    ))
    config.targetSplitSizeBytes shouldBe (1L * 1024 * 1024 * 1024)
  }

  test("fromMap with valid values") {
    val config = OptimizedWriteConfig.fromMap(Map(
      OptimizedWriteConfig.KEY_ENABLED -> "true",
      OptimizedWriteConfig.KEY_TARGET_SPLIT_SIZE -> "2G",
      OptimizedWriteConfig.KEY_SAMPLING_RATIO -> "1.3",
      OptimizedWriteConfig.KEY_MIN_ROWS_FOR_EST -> "20000",
      OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "none"
    ))
    config.enabled shouldBe true
    config.targetSplitSizeBytes shouldBe (2L * 1024 * 1024 * 1024)
    config.samplingRatio shouldBe 1.3
    config.minRowsForEstimation shouldBe 20000L
    config.distributionMode shouldBe "none"
  }

  test("fromMap is case-insensitive on keys") {
    val config = OptimizedWriteConfig.fromMap(Map(
      "SPARK.INDEXTABLES.WRITE.OPTIMIZEWRITE.ENABLED" -> "true"
    ))
    config.enabled shouldBe true
  }

  test("validate rejects negative target size") {
    an[IllegalArgumentException] should be thrownBy {
      OptimizedWriteConfig(targetSplitSizeBytes = -1).validate()
    }
  }

  test("validate rejects zero sampling ratio") {
    an[IllegalArgumentException] should be thrownBy {
      OptimizedWriteConfig(samplingRatio = 0.0).validate()
    }
  }

  test("validate rejects negative minRows") {
    an[IllegalArgumentException] should be thrownBy {
      OptimizedWriteConfig(minRowsForEstimation = -1).validate()
    }
  }

  test("validate rejects invalid distribution mode") {
    an[IllegalArgumentException] should be thrownBy {
      OptimizedWriteConfig(distributionMode = "random").validate()
    }
  }

  test("targetSplitSizeString formats correctly") {
    OptimizedWriteConfig(targetSplitSizeBytes = 1L * 1024 * 1024 * 1024).targetSplitSizeString shouldBe "1G"
    OptimizedWriteConfig(targetSplitSizeBytes = 512L * 1024 * 1024).targetSplitSizeString shouldBe "512M"
  }

  // ===== Balanced mode tests =====

  test("fromOptions parses balanced distribution mode") {
    val config = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "balanced"
    ))
    config.distributionMode shouldBe "balanced"
  }

  test("fromOptions balanced mode is case-insensitive") {
    val config = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "BALANCED"
    ))
    config.distributionMode shouldBe "balanced"
  }

  test("fromOptions parses maxSplitSize") {
    val config = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_MAX_SPLIT_SIZE -> "2G"
    ))
    config.maxSplitSizeBytes shouldBe (2L * 1024 * 1024 * 1024)

    val config512M = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_MAX_SPLIT_SIZE -> "512M"
    ))
    config512M.maxSplitSizeBytes shouldBe (512L * 1024 * 1024)
  }

  test("fromOptions falls back to default for invalid maxSplitSize") {
    val config = OptimizedWriteConfig.fromOptions(makeOptions(
      OptimizedWriteConfig.KEY_MAX_SPLIT_SIZE -> "notasize"
    ))
    config.maxSplitSizeBytes shouldBe (4L * 1024 * 1024 * 1024)
  }

  test("fromMap parses balanced mode and maxSplitSize") {
    val config = OptimizedWriteConfig.fromMap(Map(
      OptimizedWriteConfig.KEY_DISTRIBUTION_MODE -> "balanced",
      OptimizedWriteConfig.KEY_MAX_SPLIT_SIZE -> "2G"
    ))
    config.distributionMode shouldBe "balanced"
    config.maxSplitSizeBytes shouldBe (2L * 1024 * 1024 * 1024)
  }

  test("validate rejects negative maxSplitSize") {
    an[IllegalArgumentException] should be thrownBy {
      OptimizedWriteConfig(maxSplitSizeBytes = -1).validate()
    }
  }

  test("validate rejects zero maxSplitSize") {
    an[IllegalArgumentException] should be thrownBy {
      OptimizedWriteConfig(maxSplitSizeBytes = 0).validate()
    }
  }

  test("validate accepts balanced distribution mode") {
    noException should be thrownBy {
      OptimizedWriteConfig(distributionMode = "balanced").validate()
    }
  }

  test("maxSplitSizeString formats correctly") {
    OptimizedWriteConfig(maxSplitSizeBytes = 4L * 1024 * 1024 * 1024).maxSplitSizeString shouldBe "4G"
    OptimizedWriteConfig(maxSplitSizeBytes = 512L * 1024 * 1024).maxSplitSizeString shouldBe "512M"
  }
}
