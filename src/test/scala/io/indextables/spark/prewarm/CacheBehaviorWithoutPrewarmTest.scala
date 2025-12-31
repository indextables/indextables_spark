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

package io.indextables.spark.prewarm

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

/**
 * Test to verify that the L2 disk cache works correctly WITHOUT prewarming:
 * - First query should populate the cache
 * - Second identical query should get 100% cache hits (no new components)
 */
class CacheBehaviorWithoutPrewarmTest extends AnyFunSuite with BeforeAndAfterEach {

  private val logger = LoggerFactory.getLogger(classOf[CacheBehaviorWithoutPrewarmTest])

  var spark: SparkSession = _
  var testDir: String = _
  var cacheDir: String = _

  override def beforeEach(): Unit = {
    testDir = Files.createTempDirectory("cache_behavior_test_").toFile.getAbsolutePath
    cacheDir = Files.createTempDirectory("disk_cache_test_").toFile.getAbsolutePath

    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("CacheBehaviorWithoutPrewarmTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.indextables.cache.disk.enabled", "true")
      .config("spark.indextables.cache.disk.path", cacheDir)
      .config("spark.indextables.prewarm.enabled", "false") // Explicitly disable prewarm
      .getOrCreate()
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    deleteRecursively(new File(testDir))
    deleteRecursively(new File(cacheDir))
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  private def countCacheFiles(): Int = {
    val cacheFile = new File(cacheDir)
    if (cacheFile.exists()) {
      countFilesRecursively(cacheFile)
    } else 0
  }

  private def countFilesRecursively(dir: File): Int = {
    if (!dir.exists()) return 0
    val files = Option(dir.listFiles()).getOrElse(Array.empty)
    files.count(f => f.isFile && !f.getName.startsWith(".")) +
      files.filter(_.isDirectory).map(countFilesRecursively).sum
  }

  test("first query populates cache, second query gets cache hits") {
    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Create test data
    val data = (1 to 100).map(i => (i.toLong, s"title_$i", s"content for document $i", i * 1.5))
    val df = data.toDF("id", "title", "content", "score")

    // Write to IndexTables
    logger.info("Writing test data...")
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .option("spark.indextables.indexing.typemap.content", "text")
      .option("spark.indextables.indexing.fastfields", "id,score")  // Add id as fast field for range queries
      .save(testDir)

    // Read with prewarm DISABLED
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.cache.disk.enabled", "true")
      .option("spark.indextables.cache.disk.path", cacheDir)
      .option("spark.indextables.prewarm.enabled", "false")
      .load(testDir)

    val cacheBeforeQuery1 = countCacheFiles()
    println(s"📁 Cache files BEFORE first query: $cacheBeforeQuery1")

    // First query - should populate cache
    println("=== First query (should populate cache) ===")
    val count1 = readDf.filter($"id" > 50).count()
    println(s"First query result: $count1 rows")

    val cacheAfterQuery1 = countCacheFiles()
    val componentsFromQuery1 = cacheAfterQuery1 - cacheBeforeQuery1
    println(s"📁 Cache files AFTER first query: $cacheAfterQuery1")
    println(s"📊 Components cached by first query: $componentsFromQuery1")

    // Verify first query populated cache
    assert(cacheAfterQuery1 > cacheBeforeQuery1,
      s"First query should populate cache. Before: $cacheBeforeQuery1, After: $cacheAfterQuery1")

    // Second query - should hit cache (no new components)
    println("=== Second query (should hit cache) ===")
    val count2 = readDf.filter($"id" > 50).count()
    println(s"Second query result: $count2 rows")

    val cacheAfterQuery2 = countCacheFiles()
    val newComponentsFromQuery2 = cacheAfterQuery2 - cacheAfterQuery1
    println(s"📁 Cache files AFTER second query: $cacheAfterQuery2")
    println(s"📊 New components after second query: $newComponentsFromQuery2")

    // Verify second query got cache hits
    assert(cacheAfterQuery2 == cacheAfterQuery1,
      s"Second query should get 100% cache hits (no new components). " +
        s"After query 1: $cacheAfterQuery1, After query 2: $cacheAfterQuery2, " +
        s"New components: $newComponentsFromQuery2")

    // Both queries should return same result
    assert(count1 == count2, s"Both queries should return same count: $count1 vs $count2")
    assert(count1 == 50, s"Expected 50 rows with id > 50, got $count1")

    println("✅ Cache behavior verified: first query populates, second query hits cache")
  }
}
