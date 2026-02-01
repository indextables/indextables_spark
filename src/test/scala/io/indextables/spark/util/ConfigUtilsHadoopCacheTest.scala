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
import org.scalatest.BeforeAndAfterEach

/**
 * Tests for ConfigUtils Hadoop Configuration caching.
 *
 * The Hadoop Configuration cache is a performance optimization that avoids repeated creation of expensive Configuration
 * objects when processing many splits.
 */
class ConfigUtilsHadoopCacheTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit =
    // Clear the cache before each test for isolation
    ConfigUtils.clearHadoopConfigCache()

  test("getOrCreateHadoopConfiguration should return Configuration with indextables keys") {
    val config = Map(
      "spark.indextables.aws.accessKey" -> "AKIATEST",
      "spark.indextables.aws.secretKey" -> "secret",
      "unrelated.key"                   -> "ignored"
    )

    val hadoopConf = ConfigUtils.getOrCreateHadoopConfiguration(config)

    hadoopConf.get("spark.indextables.aws.accessKey") shouldBe "AKIATEST"
    hadoopConf.get("spark.indextables.aws.secretKey") shouldBe "secret"
    hadoopConf.get("unrelated.key") shouldBe null // Non-indextables keys are not copied
  }

  test("getOrCreateHadoopConfiguration should return same instance for same config") {
    val config = Map(
      "spark.indextables.aws.accessKey" -> "AKIATEST",
      "spark.indextables.aws.secretKey" -> "secret"
    )

    val conf1 = ConfigUtils.getOrCreateHadoopConfiguration(config)
    val conf2 = ConfigUtils.getOrCreateHadoopConfiguration(config)

    // Should return the same cached instance
    conf1 shouldBe theSameInstanceAs(conf2)
  }

  test("getOrCreateHadoopConfiguration should return different instances for different configs") {
    val config1 = Map("spark.indextables.aws.accessKey" -> "AKIATEST1")
    val config2 = Map("spark.indextables.aws.accessKey" -> "AKIATEST2")

    val conf1 = ConfigUtils.getOrCreateHadoopConfiguration(config1)
    val conf2 = ConfigUtils.getOrCreateHadoopConfiguration(config2)

    // Should be different instances with different values
    conf1 should not be theSameInstanceAs(conf2)
    conf1.get("spark.indextables.aws.accessKey") shouldBe "AKIATEST1"
    conf2.get("spark.indextables.aws.accessKey") shouldBe "AKIATEST2"
  }

  test("getOrCreateHadoopConfiguration should ignore non-indextables keys for caching") {
    // These two configs have the same indextables keys, so should produce same cache entry
    val config1 = Map(
      "spark.indextables.aws.accessKey" -> "AKIATEST",
      "spark.executor.memory"           -> "4g" // Not indextables, should be ignored
    )
    val config2 = Map(
      "spark.indextables.aws.accessKey" -> "AKIATEST",
      "spark.executor.memory"           -> "8g" // Different non-indextables key
    )

    val conf1 = ConfigUtils.getOrCreateHadoopConfiguration(config1)
    val conf2 = ConfigUtils.getOrCreateHadoopConfiguration(config2)

    // Should return the same cached instance since indextables keys are the same
    conf1 shouldBe theSameInstanceAs(conf2)
  }

  test("clearHadoopConfigCache should invalidate cached configurations") {
    val config = Map("spark.indextables.aws.accessKey" -> "AKIATEST")

    val conf1 = ConfigUtils.getOrCreateHadoopConfiguration(config)

    ConfigUtils.clearHadoopConfigCache()

    val conf2 = ConfigUtils.getOrCreateHadoopConfiguration(config)

    // Should be different instances after cache clear
    conf1 should not be theSameInstanceAs(conf2)
  }

  test("getOrCreateHadoopConfiguration should handle empty config") {
    val config = Map.empty[String, String]

    val hadoopConf = ConfigUtils.getOrCreateHadoopConfiguration(config)

    hadoopConf should not be null
  }

  test("getOrCreateHadoopConfiguration should be thread-safe") {
    val config = Map("spark.indextables.aws.accessKey" -> "AKIATEST")

    // Run concurrent accesses
    val results = (1 to 100).par.map(_ => ConfigUtils.getOrCreateHadoopConfiguration(config)).toList

    // All results should be the same instance
    results.foreach(conf => conf shouldBe theSameInstanceAs(results.head))
  }
}
