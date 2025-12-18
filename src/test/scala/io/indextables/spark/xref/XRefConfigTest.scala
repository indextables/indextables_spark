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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class XRefConfigTest extends AnyFunSuite with Matchers {

  test("default configuration should have sensible defaults") {
    val config = XRefConfig.default()

    // Auto-index defaults
    config.autoIndex.enabled shouldBe false  // Disabled by default
    config.autoIndex.maxSourceSplits shouldBe 1024
    config.autoIndex.minSplitsToTrigger shouldBe 10
    config.autoIndex.rebuildOnSourceChange shouldBe true

    // Build defaults (note: includePositions removed in FuseXRef migration)
    config.build.parallelism shouldBe None
    config.build.tempDirectoryPath shouldBe None
    config.build.maxSourceSplits shouldBe 1024  // New canonical location

    // Query defaults
    config.query.enabled shouldBe true
    config.query.minSplitsForXRef shouldBe 128
    config.query.timeoutMs shouldBe 5000
    config.query.fallbackOnError shouldBe true

    // Storage defaults
    config.storage.directory shouldBe "_xrefsplits"
    config.storage.compressionEnabled shouldBe true
  }

  test("fromOptions should parse auto-index configuration") {
    val options = new CaseInsensitiveStringMap(Map(
      "spark.indextables.xref.autoIndex.enabled" -> "false",
      "spark.indextables.xref.autoIndex.maxSourceSplits" -> "2048",
      "spark.indextables.xref.autoIndex.minSplitsToTrigger" -> "50",
      "spark.indextables.xref.autoIndex.rebuildOnSourceChange" -> "false"
    ).asJava)

    val config = XRefConfig.fromOptions(options)

    config.autoIndex.enabled shouldBe false
    config.autoIndex.maxSourceSplits shouldBe 2048
    config.autoIndex.minSplitsToTrigger shouldBe 50
    config.autoIndex.rebuildOnSourceChange shouldBe false
  }

  test("fromOptions should parse build configuration") {
    val options = new CaseInsensitiveStringMap(Map(
      "spark.indextables.xref.build.parallelism" -> "8",
      "spark.indextables.xref.build.tempDirectoryPath" -> "/tmp/xref-build",
      "spark.indextables.xref.build.maxSourceSplits" -> "512"
    ).asJava)

    val config = XRefConfig.fromOptions(options)

    // Note: includePositions was removed in FuseXRef migration
    config.build.parallelism shouldBe Some(8)
    config.build.tempDirectoryPath shouldBe Some("/tmp/xref-build")
    config.build.maxSourceSplits shouldBe 512
  }

  test("fromOptions should use old autoIndex.maxSourceSplits key for backward compatibility") {
    // When only the old key is provided, build.maxSourceSplits should use it
    val options = new CaseInsensitiveStringMap(Map(
      "spark.indextables.xref.autoIndex.maxSourceSplits" -> "768"
    ).asJava)

    val config = XRefConfig.fromOptions(options)

    // New location should get value from old key
    config.build.maxSourceSplits shouldBe 768
    // Old location still works too
    config.autoIndex.maxSourceSplits shouldBe 768
  }

  test("fromOptions should prefer new build.maxSourceSplits key over old autoIndex key") {
    // When both keys are provided, new key takes precedence
    val options = new CaseInsensitiveStringMap(Map(
      "spark.indextables.xref.build.maxSourceSplits" -> "2048",
      "spark.indextables.xref.autoIndex.maxSourceSplits" -> "512"
    ).asJava)

    val config = XRefConfig.fromOptions(options)

    // New key takes precedence
    config.build.maxSourceSplits shouldBe 2048
    // Old location still gets its value
    config.autoIndex.maxSourceSplits shouldBe 512
  }

  test("fromOptions should parse query configuration") {
    val options = new CaseInsensitiveStringMap(Map(
      "spark.indextables.xref.query.enabled" -> "false",
      "spark.indextables.xref.query.minSplitsForXRef" -> "256",
      "spark.indextables.xref.query.timeoutMs" -> "10000",
      "spark.indextables.xref.query.fallbackOnError" -> "false"
    ).asJava)

    val config = XRefConfig.fromOptions(options)

    config.query.enabled shouldBe false
    config.query.minSplitsForXRef shouldBe 256
    config.query.timeoutMs shouldBe 10000
    config.query.fallbackOnError shouldBe false
  }

  test("fromOptions should parse storage configuration") {
    val options = new CaseInsensitiveStringMap(Map(
      "spark.indextables.xref.storage.directory" -> "_custom_xrefs",
      "spark.indextables.xref.storage.compressionEnabled" -> "false"
    ).asJava)

    val config = XRefConfig.fromOptions(options)

    config.storage.directory shouldBe "_custom_xrefs"
    config.storage.compressionEnabled shouldBe false
  }

  test("fromOptions should use defaults for missing options") {
    val options = new CaseInsensitiveStringMap(Map(
      "spark.indextables.xref.autoIndex.maxSourceSplits" -> "512"
    ).asJava)

    val config = XRefConfig.fromOptions(options)

    // Specified option
    config.autoIndex.maxSourceSplits shouldBe 512

    // Defaults for unspecified options
    config.autoIndex.enabled shouldBe false  // Disabled by default
    config.autoIndex.minSplitsToTrigger shouldBe 10
    config.query.enabled shouldBe true
    config.storage.directory shouldBe "_xrefsplits"
  }

  test("configuration keys should be defined correctly") {
    import XRefConfig.Keys._

    AUTO_INDEX_ENABLED shouldBe "spark.indextables.xref.autoIndex.enabled"
    AUTO_INDEX_MAX_SOURCE_SPLITS shouldBe "spark.indextables.xref.autoIndex.maxSourceSplits"
    AUTO_INDEX_MIN_SPLITS shouldBe "spark.indextables.xref.autoIndex.minSplitsToTrigger"
    AUTO_INDEX_REBUILD_ON_CHANGE shouldBe "spark.indextables.xref.autoIndex.rebuildOnSourceChange"

    // Note: BUILD_INCLUDE_POSITIONS was removed in FuseXRef migration
    BUILD_PARALLELISM shouldBe "spark.indextables.xref.build.parallelism"
    BUILD_TEMP_DIRECTORY shouldBe "spark.indextables.xref.build.tempDirectoryPath"
    BUILD_MAX_SOURCE_SPLITS shouldBe "spark.indextables.xref.build.maxSourceSplits"

    QUERY_ENABLED shouldBe "spark.indextables.xref.query.enabled"
    QUERY_MIN_SPLITS shouldBe "spark.indextables.xref.query.minSplitsForXRef"
    QUERY_TIMEOUT_MS shouldBe "spark.indextables.xref.query.timeoutMs"
    QUERY_FALLBACK_ON_ERROR shouldBe "spark.indextables.xref.query.fallbackOnError"

    STORAGE_DIRECTORY shouldBe "spark.indextables.xref.storage.directory"
    STORAGE_COMPRESSION shouldBe "spark.indextables.xref.storage.compressionEnabled"
  }

  test("XRefAutoIndexConfig should have correct case class properties") {
    val config = XRefAutoIndexConfig(
      enabled = true,
      maxSourceSplits = 500,
      minSplitsToTrigger = 20,
      rebuildOnSourceChange = false
    )

    config.enabled shouldBe true
    config.maxSourceSplits shouldBe 500
    config.minSplitsToTrigger shouldBe 20
    config.rebuildOnSourceChange shouldBe false
  }

  test("XRefBuildConfig should have correct case class properties") {
    // Note: includePositions was removed in FuseXRef migration
    val config = XRefBuildConfig(
      parallelism = Some(4),
      tempDirectoryPath = Some("/custom/temp"),
      maxSourceSplits = 2048
    )

    config.parallelism shouldBe Some(4)
    config.tempDirectoryPath shouldBe Some("/custom/temp")
    config.maxSourceSplits shouldBe 2048
  }

  test("XRefQueryConfig should have correct case class properties") {
    val config = XRefQueryConfig(
      enabled = false,
      minSplitsForXRef = 64,
      timeoutMs = 2000,
      fallbackOnError = false
    )

    config.enabled shouldBe false
    config.minSplitsForXRef shouldBe 64
    config.timeoutMs shouldBe 2000
    config.fallbackOnError shouldBe false
  }

  test("XRefStorageConfig should have correct case class properties") {
    val config = XRefStorageConfig(
      directory = "_my_xrefs",
      compressionEnabled = false
    )

    config.directory shouldBe "_my_xrefs"
    config.compressionEnabled shouldBe false
  }

  test("XRefConfig should compose all sub-configs correctly") {
    // Note: includePositions was removed in FuseXRef migration
    val config = XRefConfig(
      autoIndex = XRefAutoIndexConfig(enabled = false),
      build = XRefBuildConfig(distributedBuild = true),
      query = XRefQueryConfig(minSplitsForXRef = 64),
      storage = XRefStorageConfig(directory = "_custom")
    )

    config.autoIndex.enabled shouldBe false
    config.build.distributedBuild shouldBe true
    config.query.minSplitsForXRef shouldBe 64
    config.storage.directory shouldBe "_custom"

    // Check that defaults are used for unspecified fields
    config.autoIndex.maxSourceSplits shouldBe 1024
    config.build.parallelism shouldBe None
    config.query.timeoutMs shouldBe 5000
    config.storage.compressionEnabled shouldBe true
  }
}
