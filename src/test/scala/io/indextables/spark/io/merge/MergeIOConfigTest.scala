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

package io.indextables.spark.io.merge

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MergeIOConfigTest extends AnyFunSuite with Matchers {

  test("default configuration values") {
    val config = MergeIOConfig.default

    config.maxConcurrencyPerCore shouldBe 8
    config.memoryBudgetBytes shouldBe 2L * 1024 * 1024 * 1024
    config.downloadRetries shouldBe 3
    config.uploadMaxConcurrency shouldBe 6
    config.retryBaseDelayMs shouldBe 1000L
    config.retryMaxDelayMs shouldBe 30000L
  }

  test("maxConcurrentDownloads calculation") {
    val config     = MergeIOConfig(maxConcurrencyPerCore = 4)
    val processors = Runtime.getRuntime.availableProcessors()

    config.maxConcurrentDownloads shouldBe (processors * 4)
  }

  test("parse bytes from string - various units") {
    MergeIOConfig.parseBytes("1024") shouldBe 1024L
    MergeIOConfig.parseBytes("1K") shouldBe 1024L
    MergeIOConfig.parseBytes("1k") shouldBe 1024L
    MergeIOConfig.parseBytes("2M") shouldBe 2L * 1024 * 1024
    MergeIOConfig.parseBytes("2m") shouldBe 2L * 1024 * 1024
    MergeIOConfig.parseBytes("4G") shouldBe 4L * 1024 * 1024 * 1024
    MergeIOConfig.parseBytes("4g") shouldBe 4L * 1024 * 1024 * 1024
    MergeIOConfig.parseBytes("1T") shouldBe 1L * 1024 * 1024 * 1024 * 1024
  }

  test("format bytes to string") {
    MergeIOConfig.formatBytes(512L) shouldBe "512B"
    MergeIOConfig.formatBytes(1024L) shouldBe "1K"
    MergeIOConfig.formatBytes(2L * 1024 * 1024) shouldBe "2M"
    MergeIOConfig.formatBytes(4L * 1024 * 1024 * 1024) shouldBe "4G"
    MergeIOConfig.formatBytes(1L * 1024 * 1024 * 1024 * 1024) shouldBe "1T"
  }

  test("fromOptions with custom values") {
    val options = new CaseInsensitiveStringMap(
      Map(
        "spark.indextables.merge.download.maxConcurrencyPerCore" -> "16",
        "spark.indextables.merge.download.memoryBudget"          -> "4G",
        "spark.indextables.merge.download.retries"               -> "5",
        "spark.indextables.merge.upload.maxConcurrency"          -> "10"
      ).asJava
    )

    val config = MergeIOConfig.fromOptions(options)

    config.maxConcurrencyPerCore shouldBe 16
    config.memoryBudgetBytes shouldBe 4L * 1024 * 1024 * 1024
    config.downloadRetries shouldBe 5
    config.uploadMaxConcurrency shouldBe 10
  }

  test("fromOptions with default values when not specified") {
    val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
    val config  = MergeIOConfig.fromOptions(options)

    config shouldBe MergeIOConfig.default
  }

  test("fromMap with custom values") {
    val configs = Map(
      "spark.indextables.merge.download.maxConcurrencyPerCore" -> "12",
      "spark.indextables.merge.download.memoryBudget"          -> "1G",
      "spark.indextables.merge.download.retries"               -> "2"
    )

    val config = MergeIOConfig.fromMap(configs)

    config.maxConcurrencyPerCore shouldBe 12
    config.memoryBudgetBytes shouldBe 1L * 1024 * 1024 * 1024
    config.downloadRetries shouldBe 2
  }

  test("fromMap is case insensitive") {
    val configs = Map(
      "SPARK.INDEXTABLES.MERGE.DOWNLOAD.MAXCONCURRENCYPERCORE" -> "20"
    )

    val config = MergeIOConfig.fromMap(configs)
    config.maxConcurrencyPerCore shouldBe 20
  }

  test("validate throws on invalid values") {
    an[IllegalArgumentException] should be thrownBy {
      MergeIOConfig(maxConcurrencyPerCore = 0).validate()
    }

    an[IllegalArgumentException] should be thrownBy {
      MergeIOConfig(memoryBudgetBytes = -1).validate()
    }

    an[IllegalArgumentException] should be thrownBy {
      MergeIOConfig(downloadRetries = -1).validate()
    }

    an[IllegalArgumentException] should be thrownBy {
      MergeIOConfig(uploadMaxConcurrency = 0).validate()
    }

    an[IllegalArgumentException] should be thrownBy {
      MergeIOConfig(retryBaseDelayMs = 0).validate()
    }

    an[IllegalArgumentException] should be thrownBy {
      MergeIOConfig(retryBaseDelayMs = 5000, retryMaxDelayMs = 1000).validate()
    }
  }

  test("validate passes for valid configuration") {
    val config = MergeIOConfig(
      maxConcurrencyPerCore = 1,
      memoryBudgetBytes = 1,
      downloadRetries = 0,
      uploadMaxConcurrency = 1,
      retryBaseDelayMs = 100,
      retryMaxDelayMs = 100
    )

    noException should be thrownBy {
      config.validate()
    }
  }
}
