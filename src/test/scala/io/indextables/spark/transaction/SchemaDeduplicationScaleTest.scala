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

package io.indextables.spark.transaction

import java.nio.file.Files
import java.util.UUID

import scala.util.Random

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.indextables.spark.transaction.avro.StateManifestIO
import io.indextables.spark.io.CloudStorageProviderFactory

/**
 * Scale test to validate schema deduplication works correctly with:
 * - Multiple columns including a struct column with 10 fields
 * - Several thousand rows with random values
 * - 200+ splits
 * - Validates exactly 1 schema ref after checkpoint
 *
 * This test catches the bug where different JSON field orderings from tantivy4java
 * would produce different schema hashes, leading to N schema registry entries
 * instead of 1.
 */
class SchemaDeduplicationScaleTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tempDir: java.nio.file.Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[4]")
      .appName("SchemaDeduplicationScaleTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.default.parallelism", "200")
      // Register SQL extensions for CHECKPOINT command
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      // Use Avro state format
      .config("spark.indextables.state.format", "avro")
      // Set low threshold to ensure renormalization triggers
      .config("spark.indextables.state.schema.renormalizeThreshold", "5")
      .getOrCreate()

    tempDir = Files.createTempDirectory("schema-dedup-scale-test")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (tempDir != null) {
      deleteRecursively(tempDir.toFile)
    }
    super.afterAll()
  }

  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  /**
   * Read the _manifest.avro directly to count schema refs.
   */
  private def countSchemaRefs(tablePath: String): (Int, Long) = {
    val transactionLogPath = s"$tablePath/_transaction_log"

    // Find state directories and read _manifest.avro
    val txLogDir = new java.io.File(transactionLogPath)
    val stateDirs = txLogDir.listFiles().filter(f => f.isDirectory && f.getName.startsWith("state-v"))

    if (stateDirs.isEmpty) {
      return (0, 0L)
    }

    val latestStateDir = stateDirs.maxBy(_.getName)

    // Read using StateManifestIO since format is now Avro binary
    val cloudProvider = CloudStorageProviderFactory.createProvider(
      transactionLogPath,
      new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
      spark.sparkContext.hadoopConfiguration
    )
    val manifestIO = StateManifestIO(cloudProvider)
    val manifest = manifestIO.readStateManifest(latestStateDir.getAbsolutePath)
    cloudProvider.close()

    val schemaRefCount = manifest.schemaRegistry.size
    val numFiles = manifest.numFiles

    (schemaRefCount, numFiles)
  }

  /**
   * SCALE TEST: 200+ splits with struct column should produce exactly 1 schema ref.
   *
   * This test validates that:
   * 1. Schema normalization correctly handles struct fields with multiple sub-fields
   * 2. All 200+ splits consolidate to a single schema entry in the registry
   * 3. The checkpoint correctly deduplicates schemas regardless of JSON field ordering
   */
  test("200+ splits with struct column should produce exactly 1 schema ref after checkpoint") {
    val tablePath = tempDir.resolve("scale-test-table").toString
    val random = new Random(42) // Fixed seed for reproducibility

    // Define schema with struct column containing 10 fields
    val structType = StructType(Seq(
      StructField("field_01_string", StringType, nullable = true),
      StructField("field_02_int", IntegerType, nullable = true),
      StructField("field_03_long", LongType, nullable = true),
      StructField("field_04_double", DoubleType, nullable = true),
      StructField("field_05_boolean", BooleanType, nullable = true),
      StructField("field_06_string", StringType, nullable = true),
      StructField("field_07_int", IntegerType, nullable = true),
      StructField("field_08_long", LongType, nullable = true),
      StructField("field_09_double", DoubleType, nullable = true),
      StructField("field_10_string", StringType, nullable = true)
    ))

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("category", StringType, nullable = true),
      StructField("value", DoubleType, nullable = true),
      StructField("metadata", structType, nullable = true),
      StructField("tags", StringType, nullable = true)
    ))

    // Generate 10,000 rows with random values
    val numRows = 10000
    val rows = (0 until numRows).map { i =>
      Row(
        UUID.randomUUID().toString,
        System.currentTimeMillis() + random.nextInt(1000000),
        s"category_${random.nextInt(100)}",
        random.nextDouble() * 1000,
        Row(
          s"str_${random.nextInt(10000)}",
          random.nextInt(1000000),
          random.nextLong(),
          random.nextDouble(),
          random.nextBoolean(),
          s"another_${random.nextInt(10000)}",
          random.nextInt(500000),
          random.nextLong(),
          random.nextDouble(),
          s"final_${random.nextInt(10000)}"
        ),
        s"tag_${random.nextInt(50)}"
      )
    }

    val rdd = spark.sparkContext.parallelize(rows, 200) // Force 200 partitions
    val df = spark.createDataFrame(rdd, schema)

    // Write to 200+ splits
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.state.format", "avro")
      .mode("overwrite")
      .save(tablePath)

    // Count schema refs by reading the manifest directly
    val (schemaRefCount, numFiles) = countSchemaRefs(tablePath)

    withClue(s"Should have at least 200 files, got $numFiles") {
      numFiles should be >= 200L
    }

    withClue(
      s"""Schema deduplication failed!
         |Expected exactly 1 unique schema ref, but found $schemaRefCount.
         |
         |This indicates the schema normalization is not working correctly.
         |All splits should have identical schemas that normalize to the same hash.
         |
         |Total files: $numFiles
         |""".stripMargin
    ) {
      schemaRefCount shouldBe 1
    }

    println(s"SUCCESS: $numFiles files consolidated to $schemaRefCount schema ref")
  }

  /**
   * SCALE TEST: Multiple writes should still produce exactly 1 schema ref.
   *
   * This test validates that incremental writes (append mode) also correctly
   * deduplicate schemas across all splits.
   */
  test("multiple append writes should still produce exactly 1 schema ref after checkpoint") {
    val tablePath = tempDir.resolve("incremental-test-table").toString
    val random = new Random(123)

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("data", StructType(Seq(
        StructField("name", StringType),
        StructField("value", IntegerType),
        StructField("score", DoubleType),
        StructField("active", BooleanType),
        StructField("count", LongType),
        StructField("label", StringType),
        StructField("priority", IntegerType),
        StructField("weight", DoubleType),
        StructField("enabled", BooleanType),
        StructField("description", StringType)
      )), nullable = true)
    ))

    def generateRows(count: Int, partitions: Int): org.apache.spark.sql.DataFrame = {
      val rows = (0 until count).map { _ =>
        Row(
          UUID.randomUUID().toString,
          Row(
            s"name_${random.nextInt(1000)}",
            random.nextInt(10000),
            random.nextDouble(),
            random.nextBoolean(),
            random.nextLong(),
            s"label_${random.nextInt(100)}",
            random.nextInt(10),
            random.nextDouble(),
            random.nextBoolean(),
            s"desc_${random.nextInt(5000)}"
          )
        )
      }
      val rdd = spark.sparkContext.parallelize(rows, partitions)
      spark.createDataFrame(rdd, schema)
    }

    // Write in 5 batches of 50 partitions each = 250 splits total
    for (batch <- 1 to 5) {
      val mode = if (batch == 1) "overwrite" else "append"
      generateRows(2000, 50).write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.state.format", "avro")
        .mode(mode)
        .save(tablePath)
    }

    // Force a checkpoint to consolidate
    spark.sql(s"CHECKPOINT INDEXTABLES '$tablePath'")

    // Count schema refs by reading the manifest directly
    val (schemaRefCount, numFiles) = countSchemaRefs(tablePath)

    withClue("Should have at least 250 files from 5 batches of 50 partitions") {
      numFiles should be >= 250L
    }

    withClue(
      s"""Incremental write schema deduplication failed!
         |Expected exactly 1 unique schema ref after 5 append writes.
         |Found $schemaRefCount schema refs for $numFiles files.
         |""".stripMargin
    ) {
      schemaRefCount shouldBe 1
    }

    println(s"SUCCESS: $numFiles files from 5 batches consolidated to $schemaRefCount schema ref")
  }
}
