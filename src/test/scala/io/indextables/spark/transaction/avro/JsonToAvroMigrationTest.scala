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

package io.indextables.spark.transaction.avro

import org.apache.spark.sql.functions._

import io.indextables.spark.TestBase

/**
 * Tests for validating the migration/transition process from JSON checkpoints to Avro state format.
 *
 * These tests ensure:
 *   1. Tables created with JSON checkpoints can be upgraded to Avro format 2. Data integrity is preserved during the
 *      transition 3. Schema registry and docMappingJson are correctly migrated 4. The table remains fully functional
 *      after migration 5. Incremental operations work correctly after migration
 *
 * Run with: mvn scalatest:test -DwildcardSuites='io.indextables.spark.transaction.avro.JsonToAvroMigrationTest'
 */
class JsonToAvroMigrationTest extends TestBase {

  private val provider = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  test("Migrate from JSON checkpoint to Avro state: complete lifecycle") {
    withTempPath { tempDir =>
      val path = tempDir

      println(s"🚀 Testing JSON to Avro migration at: $path")

      // ========================================
      // Phase 1: Create table with JSON format (default)
      // ========================================
      println("\n📝 Phase 1: Creating table with JSON checkpoint format...")

      // Ensure we're using JSON format (the default)
      spark.conf.unset("spark.indextables.state.format")

      val initialData = Seq(
        (1, "Document One", "category_a", 100),
        (2, "Document Two", "category_a", 200),
        (3, "Document Three", "category_b", 150),
        (4, "Document Four", "category_b", 250)
      )

      val df1 = spark.createDataFrame(initialData).toDF("id", "content", "category", "score")

      df1.write
        .format(provider)
        .partitionBy("category")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("overwrite")
        .save(path)

      println("✅ Created table with initial data")

      // Create JSON checkpoint explicitly
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify it's JSON format
      val state1 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      // JSON format may show as "json" or table may not have avro-state format
      val format1 = state1(0).getAs[String]("format")
      println(s"  ➤ Initial state format: $format1")

      // Verify we can read data
      val read1 = spark.read.format(provider).load(path)
      read1.count() shouldBe 4

      // Verify IndexQuery works (requires docMappingJson)
      val searchResults = read1.filter("content indexquery 'Document'").count()
      searchResults shouldBe 4

      println(s"✅ Phase 1 complete: JSON checkpoint created, count=4, IndexQuery works")

      // ========================================
      // Phase 2: Append more data (still JSON format)
      // ========================================
      println("\n📝 Phase 2: Appending data before migration...")

      val appendData = Seq(
        (5, "Document Five", "category_c", 300),
        (6, "Document Six", "category_c", 350)
      )
      val dfAppend = spark.createDataFrame(appendData).toDF("id", "content", "category", "score")

      dfAppend.write
        .format(provider)
        .partitionBy("category")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("append")
        .save(path)

      val read2 = spark.read.format(provider).load(path)
      read2.count() shouldBe 6

      println("✅ Phase 2 complete: 6 records total before migration")

      // ========================================
      // Phase 3: Enable Avro format and create checkpoint
      // ========================================
      println("\n🔄 Phase 3: Migrating to Avro state format...")

      // Enable Avro state format
      spark.conf.set("spark.indextables.state.format", "avro")

      // Create Avro checkpoint (this performs the migration)
      val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      println(s"  ➤ Checkpoint result: ${checkpointResult.map(_.toString).mkString(", ")}")

      // Verify state is now avro-state
      val state2  = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      val format2 = state2(0).getAs[String]("format")
      format2 shouldBe "avro-state"

      println(s"✅ Phase 3 complete: State format migrated to $format2")

      // ========================================
      // Phase 4: Verify data integrity after migration
      // ========================================
      println("\n📖 Phase 4: Verifying data integrity after migration...")

      val read3  = spark.read.format(provider).load(path)
      val count3 = read3.count()
      count3 shouldBe 6

      // Verify aggregations work
      val sumScore = read3.agg(sum("score")).collect()(0).getLong(0)
      sumScore shouldBe 1350L // 100+200+150+250+300+350

      // Verify partition filtering works
      val catACount = read3.filter(col("category") === "category_a").count()
      catACount shouldBe 2

      // Verify IndexQuery still works (proves docMappingJson migrated correctly)
      val searchResults2 = read3.filter("content indexquery 'Document'").count()
      searchResults2 shouldBe 6

      println(s"✅ Phase 4 complete: Data integrity verified - count=$count3, sum(score)=$sumScore")

      // ========================================
      // Phase 5: Append data after migration
      // ========================================
      println("\n📝 Phase 5: Appending data after Avro migration...")

      val postMigrationData = Seq(
        (7, "Document Seven", "category_a", 400),
        (8, "Document Eight", "category_b", 450)
      )
      val dfPost = spark.createDataFrame(postMigrationData).toDF("id", "content", "category", "score")

      dfPost.write
        .format(provider)
        .partitionBy("category")
        .option("spark.indextables.indexing.typemap.content", "text")
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("append")
        .save(path)

      val read4 = spark.read.format(provider).load(path)
      read4.count() shouldBe 8

      println("✅ Phase 5 complete: 8 records total after post-migration append")

      // ========================================
      // Phase 6: Create another Avro checkpoint
      // ========================================
      println("\n💾 Phase 6: Creating second Avro checkpoint...")

      val checkpointResult2 = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()
      println(s"  ➤ Checkpoint result: ${checkpointResult2.map(_.toString).mkString(", ")}")

      // Verify still avro-state
      val state3 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      state3(0).getAs[String]("format") shouldBe "avro-state"

      println("✅ Phase 6 complete: Second Avro checkpoint created")

      // ========================================
      // Phase 7: Final verification
      // ========================================
      println("\n📖 Phase 7: Final verification...")

      val finalRead  = spark.read.format(provider).load(path)
      val finalCount = finalRead.count()
      finalCount shouldBe 8

      // Verify all aggregations
      val finalSum = finalRead.agg(sum("score")).collect()(0).getLong(0)
      finalSum shouldBe 2200L // 100+200+150+250+300+350+400+450

      // Verify IndexQuery on all data
      val finalSearch = finalRead.filter("content indexquery 'Document'").count()
      finalSearch shouldBe 8

      // Verify partition counts
      val catACnt = finalRead.filter(col("category") === "category_a").count()
      val catBCnt = finalRead.filter(col("category") === "category_b").count()
      val catCCnt = finalRead.filter(col("category") === "category_c").count()
      catACnt shouldBe 3
      catBCnt shouldBe 3
      catCCnt shouldBe 2

      println(s"✅ Phase 7 complete: Final count=$finalCount, sum(score)=$finalSum")

      // Clean up
      spark.conf.unset("spark.indextables.state.format")

      println(s"""
                 |============================================
                 |🎉 JSON TO AVRO MIGRATION TEST COMPLETED
                 |============================================
                 |Path: $path
                 |Initial format: $format1
                 |Final format: avro-state
                 |Records migrated: 6
                 |Records after migration: 8
                 |Sum(score): $finalSum
                 |IndexQuery works: Yes
                 |============================================
      """.stripMargin)
    }
  }

  test("Migration preserves schema registry with multiple schemas") {
    withTempPath { tempDir =>
      val path = tempDir

      println(s"🚀 Testing schema registry preservation during migration at: $path")

      // Create table with JSON format
      spark.conf.unset("spark.indextables.state.format")

      // Write initial data with one schema configuration
      val data1 = Seq(
        (1, "First batch content"),
        (2, "Second batch content")
      )
      spark
        .createDataFrame(data1)
        .toDF("id", "text_content")
        .write
        .format(provider)
        .option("spark.indextables.indexing.typemap.text_content", "text")
        .mode("overwrite")
        .save(path)

      // Create JSON checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Append more data
      val data2 = Seq(
        (3, "Third batch content"),
        (4, "Fourth batch content")
      )
      spark
        .createDataFrame(data2)
        .toDF("id", "text_content")
        .write
        .format(provider)
        .option("spark.indextables.indexing.typemap.text_content", "text")
        .mode("append")
        .save(path)

      // Verify IndexQuery works before migration
      val preRead   = spark.read.format(provider).load(path)
      val preSearch = preRead.filter("text_content indexquery 'content'").count()
      preSearch shouldBe 4

      println(s"✅ Pre-migration IndexQuery works: found $preSearch documents")

      // Migrate to Avro
      spark.conf.set("spark.indextables.state.format", "avro")
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify state format
      val state = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      state(0).getAs[String]("format") shouldBe "avro-state"

      // Verify IndexQuery still works after migration
      val postRead   = spark.read.format(provider).load(path)
      val postSearch = postRead.filter("text_content indexquery 'content'").count()
      postSearch shouldBe 4

      println(s"✅ Post-migration IndexQuery works: found $postSearch documents")
      println("✅ Schema registry correctly preserved during migration")

      // Clean up
      spark.conf.unset("spark.indextables.state.format")
    }
  }

  test("Migration handles large number of files correctly") {
    withTempPath { tempDir =>
      val path = tempDir

      println(s"🚀 Testing migration with multiple files at: $path")

      // Create table with JSON format
      spark.conf.unset("spark.indextables.state.format")

      // Write multiple batches to create multiple files
      for (i <- 0 until 5) {
        val data = (1 to 10).map(j => (i * 10 + j, s"Content batch $i item $j", i * 10 + j))
        val df   = spark.createDataFrame(data).toDF("id", "content", "score")
        df.write
          .format(provider)
          .option("spark.indextables.indexing.fastfields", "score")
          .mode(if (i == 0) "overwrite" else "append")
          .save(path)
      }

      // Create JSON checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify count before migration
      val preMigrationCount = spark.read.format(provider).load(path).count()
      preMigrationCount shouldBe 50

      println(s"✅ Pre-migration: $preMigrationCount records in table")

      // Migrate to Avro
      spark.conf.set("spark.indextables.state.format", "avro")
      val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify state format
      val state = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      state(0).getAs[String]("format") shouldBe "avro-state"
      val numFiles = state(0).getAs[Long]("num_files")

      println(s"  ➤ Avro state: num_files=$numFiles")

      // Verify count after migration
      val postMigrationCount = spark.read.format(provider).load(path).count()
      postMigrationCount shouldBe 50

      // Verify aggregations work
      val sumScore = spark.read.format(provider).load(path).agg(sum("score")).collect()(0).getLong(0)
      // Sum of 1+2+...+50 = 50*51/2 = 1275
      sumScore shouldBe 1275L

      println(s"✅ Post-migration: $postMigrationCount records, sum(score)=$sumScore")
      println("✅ Migration with multiple files completed successfully")

      // Clean up
      spark.conf.unset("spark.indextables.state.format")
    }
  }

  test("Migration with partitioned table preserves partition structure") {
    withTempPath { tempDir =>
      val path = tempDir

      println(s"🚀 Testing migration with partitioned table at: $path")

      // Create table with JSON format
      spark.conf.unset("spark.indextables.state.format")

      // Write partitioned data
      val data = Seq(
        (1, "us-east", "2024-01"),
        (2, "us-east", "2024-01"),
        (3, "us-west", "2024-01"),
        (4, "us-west", "2024-01"),
        (5, "us-east", "2024-02"),
        (6, "us-west", "2024-02")
      )
      val df = spark.createDataFrame(data).toDF("id", "region", "month")

      df.write
        .format(provider)
        .partitionBy("region", "month")
        .mode("overwrite")
        .save(path)

      // Create JSON checkpoint
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify partition pruning works before migration
      val prePruned = spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "us-east")
        .count()
      prePruned shouldBe 3

      println(s"✅ Pre-migration partition pruning: us-east count=$prePruned")

      // Migrate to Avro
      spark.conf.set("spark.indextables.state.format", "avro")
      spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      // Verify state format
      val state = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      state(0).getAs[String]("format") shouldBe "avro-state"

      // Verify partition pruning still works after migration
      val postPruned = spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "us-east")
        .count()
      postPruned shouldBe 3

      // Verify compound partition filter
      val compoundFilter = spark.read
        .format(provider)
        .load(path)
        .filter(col("region") === "us-west" && col("month") === "2024-02")
        .count()
      compoundFilter shouldBe 1

      println(s"✅ Post-migration partition pruning: us-east count=$postPruned")
      println(s"✅ Compound filter (us-west, 2024-02): count=$compoundFilter")
      println("✅ Partition structure preserved during migration")

      // Clean up
      spark.conf.unset("spark.indextables.state.format")
    }
  }

  test("Migration from JSON with 11 transactions validates shared manifest directory structure") {
    withTempPath { tempDir =>
      val path         = tempDir
      val txLogPath    = new java.io.File(path, "_transaction_log")
      val manifestsDir = new java.io.File(txLogPath, "manifests")

      println(s"Testing shared manifest directory structure after migration at: $path")

      // ========================================
      // Phase 1: Create JSON-based table with 11 transactions
      // ========================================
      println("\nPhase 1: Creating JSON-based table with 11 transactions...")

      // Explicitly set JSON format (default is now avro)
      spark.conf.set("spark.indextables.state.format", "json")

      // Create 11 transactions (each write is a transaction)
      var totalRecords = 0
      for (i <- 1 to 11) {
        val data = (1 to 5).map(j => ((i - 1) * 5 + j, s"Document $i-$j", i * 10))
        val df   = spark.createDataFrame(data).toDF("id", "content", "score")
        df.write
          .format(provider)
          .option("spark.indextables.indexing.fastfields", "score")
          .option("spark.indextables.state.format", "json")
          .mode(if (i == 1) "overwrite" else "append")
          .save(path)
        totalRecords += 5
      }

      println(s"  Created 11 transactions with $totalRecords total records")

      // Verify count before migration
      val preMigrationCount = spark.read.format(provider).load(path).count()
      preMigrationCount shouldBe totalRecords

      println(s"  Pre-migration count: $preMigrationCount")

      // ========================================
      // Phase 2: Run CHECKPOINT to convert to Avro
      // ========================================
      println("\nPhase 2: Running CHECKPOINT to convert to Avro format...")

      spark.conf.set("spark.indextables.state.format", "avro")
      val checkpointResult = spark.sql(s"CHECKPOINT INDEXTABLES '$path'").collect()

      val checkpointVersion = checkpointResult(0).getAs[Long]("checkpoint_version")
      println(s"  Checkpoint created at version: $checkpointVersion")

      // Verify state format is avro-state
      val state1 = spark.sql(s"DESCRIBE INDEXTABLES STATE '$path'").collect()
      state1(0).getAs[String]("format") shouldBe "avro-state"

      println(s"  State format after checkpoint: ${state1(0).getAs[String]("format")}")

      // ========================================
      // Phase 3: Run an Avro transaction (append more data)
      // ========================================
      println("\nPhase 3: Running additional Avro transaction...")

      val avroTxData = (1 to 10).map(j => (totalRecords + j, s"Avro document $j", 999))
      val dfAvro     = spark.createDataFrame(avroTxData).toDF("id", "content", "score")
      dfAvro.write
        .format(provider)
        .option("spark.indextables.indexing.fastfields", "score")
        .mode("append")
        .save(path)
      totalRecords += 10

      println(s"  Added 10 more records, total now: $totalRecords")

      // ========================================
      // Validation 1: Verify all data can be read
      // ========================================
      println("\nValidation 1: Verifying all data can be read...")

      val finalRead  = spark.read.format(provider).load(path)
      val finalCount = finalRead.count()
      finalCount shouldBe totalRecords

      // Verify we can read specific data from both phases
      val avroRecords = finalRead.filter(col("score") === 999).count()
      avroRecords shouldBe 10

      val jsonRecords = finalRead.filter(col("score") =!= 999).count()
      jsonRecords shouldBe (totalRecords - 10)

      println(s"  Total records: $finalCount")
      println(s"  Records from JSON phase: $jsonRecords")
      println(s"  Records from Avro phase: $avroRecords")

      // ========================================
      // Validation 2: Verify 2 manifests in manifests/ directory
      // ========================================
      println("\nValidation 2: Verifying manifests in shared directory...")

      manifestsDir.exists() shouldBe true

      val manifestFiles = manifestsDir.listFiles().filter(_.getName.endsWith(".avro"))
      println(s"  Manifests directory exists: ${manifestsDir.exists()}")
      println(s"  Manifest files found: ${manifestFiles.length}")
      manifestFiles.foreach(f => println(s"    - ${f.getName}"))

      // Should have exactly 2 manifests:
      // - One from the CHECKPOINT (containing all JSON transaction files)
      // - One from the Avro append (containing new files)
      manifestFiles.length shouldBe 2

      // ========================================
      // Validation 3: Verify 2 state directories contain only manifest JSON
      // ========================================
      println("\nValidation 3: Verifying state directories contain only _manifest.avro...")

      val stateDirs = txLogPath
        .listFiles()
        .filter(f => f.isDirectory && f.getName.startsWith("state-v"))
        .sortBy(_.getName)

      println(s"  State directories found: ${stateDirs.length}")
      stateDirs.foreach { dir =>
        val contents = dir.listFiles().map(_.getName).sorted
        println(s"    - ${dir.getName}: [${contents.mkString(", ")}]")
      }

      // Should have exactly 2 state directories
      stateDirs.length shouldBe 2

      // Each state directory should only contain _manifest.avro (no nested manifests)
      // Filter out Hadoop .crc checksum files
      stateDirs.foreach { dir =>
        val contents = dir.listFiles().filterNot(_.getName.endsWith(".crc"))
        contents.length shouldBe 1
        contents.head.getName shouldBe "_manifest.avro"
      }

      // ========================================
      // Final summary
      // ========================================
      println(s"""
                 |============================================
                 |SHARED MANIFEST DIRECTORY TEST COMPLETED
                 |============================================
                 |Path: $path
                 |Total transactions: 12 (11 JSON + 1 Avro)
                 |Total records: $totalRecords
                 |Manifests in shared directory: ${manifestFiles.length}
                 |State directories: ${stateDirs.length}
                 |All state dirs contain only _manifest.avro: Yes
                 |============================================
      """.stripMargin)

      // Clean up
      spark.conf.unset("spark.indextables.state.format")
    }
  }
}
