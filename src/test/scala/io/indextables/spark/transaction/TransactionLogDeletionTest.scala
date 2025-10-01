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

import io.indextables.spark.TestBase
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.jdk.CollectionConverters._
import java.io.File

class TransactionLogDeletionTest extends TestBase {

  test("demonstrate actual file deletion behavior") {
    withTempPath { tempPath =>
      val tempDir   = new File(tempPath)
      val tablePath = new Path(tempDir.getCanonicalPath, "test_deletion_demo")
      val schema = StructType(
        Seq(
          StructField("id", StringType, nullable = false)
        )
      )

      // Use very short retention to force deletion in tests
      val options = new CaseInsensitiveStringMap(
        Map(
          "spark.indextables.checkpoint.enabled"        -> "true",
          "spark.indextables.checkpoint.interval"       -> "3",  // Checkpoint every 3 transactions
          "spark.indextables.logRetention.duration"     -> "50", // 50ms retention (very short)
          "spark.indextables.transaction.cache.enabled" -> "false"
        ).asJava
      )

      val log = TransactionLogFactory.create(tablePath, spark, options)
      try {
        log.initialize(schema)

        // Add 8 files to trigger multiple checkpoints and demonstrate deletion
        for (i <- 1 to 8) {
          val addAction = AddAction(
            path = s"deletion_demo_file_$i.split",
            partitionValues = Map.empty,
            size = 1000L + i,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            stats = None,
            tags = None
          )
          log.addFile(addAction)
        }

        // Check files before cleanup
        val checkpointDir      = new Path(tablePath, "_transaction_log")
        val fs                 = checkpointDir.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val filesBeforeCleanup = fs.listStatus(checkpointDir).map(_.getPath.getName).sorted

        println("=== FILES BEFORE CLEANUP ===")
        filesBeforeCleanup.foreach(println)

        // Wait for files to age beyond retention period
        Thread.sleep(100) // Wait 100ms so files are older than 50ms retention

        // Add one more file to trigger cleanup
        val triggerAction = AddAction(
          path = s"deletion_demo_file_9.split",
          partitionValues = Map.empty,
          size = 1009L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          stats = None,
          tags = None
        )
        log.addFile(triggerAction)

        // Check files after cleanup
        val filesAfterCleanup = fs.listStatus(checkpointDir).map(_.getPath.getName).sorted

        println("\n=== FILES AFTER CLEANUP ===")
        filesAfterCleanup.foreach(println)

        // Show what was deleted
        val deletedFiles = filesBeforeCleanup.diff(filesAfterCleanup)
        println(s"\n=== DELETED FILES (${deletedFiles.length} files) ===")
        deletedFiles.foreach(file => println(s"DELETED: $file"))

        // Show what was preserved
        val preservedFiles = filesAfterCleanup
        println(s"\n=== PRESERVED FILES (${preservedFiles.length} files) ===")
        preservedFiles.foreach(file => println(s"PRESERVED: $file"))

        // Verify we can still read all files correctly
        val finalFiles = log.listFiles()
        println(s"\n=== DATA CONSISTENCY CHECK ===")
        println(s"Total files accessible: ${finalFiles.length}")
        println(s"Files: ${finalFiles.map(_.path).mkString(", ")}")

        assert(finalFiles.length == 9, s"Expected 9 files after deletion, got ${finalFiles.length}")

        for (i <- 1 to 9) {
          val expectedFile = s"deletion_demo_file_$i.split"
          assert(
            finalFiles.exists(_.path == expectedFile),
            s"Missing file $expectedFile after deletion - data consistency failure!"
          )
        }

        println("✅ File deletion test passed - data remains consistent despite cleanup!")

      } finally
        log.close()
    }
  }
}
