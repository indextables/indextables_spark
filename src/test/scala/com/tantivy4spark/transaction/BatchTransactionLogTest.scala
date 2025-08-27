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

package com.tantivy4spark.transaction

import com.tantivy4spark.TestBase
import com.tantivy4spark.io.CloudStorageProviderFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class BatchTransactionLogTest extends TestBase {

  test("should create single transaction with multiple ADD entries like Delta Lake") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val transactionLog = new TransactionLog(tablePath, spark)

      try {

      val schema = StructType(Array(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType, nullable = false)
      ))

      // Initialize with schema
      transactionLog.initialize(schema)

      // Create multiple ADD actions for a single transaction
      val addActions = Seq(
        AddAction(
          path = "file1.split",
          partitionValues = Map.empty,
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          stats = None,
          tags = None,
          numRecords = Some(100L)
        ),
        AddAction(
          path = "file2.split",
          partitionValues = Map.empty,
          size = 2000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          stats = None,
          tags = None,
          numRecords = Some(200L)
        ),
        AddAction(
          path = "file3.split",
          partitionValues = Map.empty,
          size = 1500L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          stats = None,
          tags = None,
          numRecords = Some(150L)
        )
      )

      // Add all files in a single transaction
      val transactionVersion = transactionLog.addFiles(addActions)

      // Verify single transaction was created
      transactionVersion shouldBe 1L

      // Verify all files are listed
      val listedFiles = transactionLog.listFiles()
      listedFiles should have length 3

      val filePaths = listedFiles.map(_.path).toSet
      filePaths should contain("file1.split")
      filePaths should contain("file2.split")
      filePaths should contain("file3.split")

      // Verify the transaction log structure
      val transactionLogPath = new Path(tablePath, "_transaction_log")
      val cloudProvider = CloudStorageProviderFactory.createProvider(
        tablePath.toString,
        new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
        spark.sparkContext.hadoopConfiguration
      )
      val (versionFiles, content) = try {
        val files = cloudProvider.listFiles(transactionLogPath.toString, recursive = false)
        val versionFiles = files
          .filter(!_.isDirectory)
          .map { fileInfo =>
            val path = new Path(fileInfo.path)
            path.getName
          }
          .filter(_.endsWith(".json"))
          .sorted

        // Should have exactly 2 files: 00000000000000000000.json (metadata) and 00000000000000000001.json (batch ADD)
        versionFiles should have length 2
        versionFiles should contain("00000000000000000000.json") // metadata
        versionFiles should contain("00000000000000000001.json") // batch transaction

        // Read the batch transaction file and verify it contains multiple ADD entries
        val batchTransactionFile = new Path(transactionLogPath, "00000000000000000001.json")
        val content = new String(cloudProvider.readFile(batchTransactionFile.toString), "UTF-8").split("\n").toList.filter(_.nonEmpty)
        
        (versionFiles, content)
      } finally {
        cloudProvider.close()
      }

      // Should have exactly 3 lines (one ADD entry per line)
      content should have length 3
      
      // Each line should be an ADD action
      content.foreach { line =>
        line should include("\"add\"")
        line should (include("file1.split") or include("file2.split") or include("file3.split"))
      }

      println("✅ Batch transaction log test passed!")
      println(s"   - Created single transaction version $transactionVersion with ${addActions.length} files")
      println(s"   - Transaction log contains ${versionFiles.length} files (metadata + batch transaction)")
      println(s"   - Batch transaction file contains ${content.length} ADD entries")
      } finally {
        transactionLog.close()
      }
    }
  }

  test("should handle empty batch gracefully") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val transactionLog = new TransactionLog(tablePath, spark)

      try {

      val schema = StructType(Array(
        StructField("id", LongType, nullable = false)
      ))

      transactionLog.initialize(schema)

      // Try to add empty batch
      val version = transactionLog.addFiles(Seq.empty)
      
      // Should return current version (0 after metadata)
      version shouldBe 0L

      // Should still have only the metadata file
      val listedFiles = transactionLog.listFiles()
      listedFiles should have length 0
      } finally {
        transactionLog.close()
      }
    }
  }

  test("should work correctly with mixed individual and batch operations") {
    withTempPath { tempPath =>
      val tablePath = new Path(tempPath)
      val transactionLog = new TransactionLog(tablePath, spark)

      try {

      val schema = StructType(Array(
        StructField("id", LongType, nullable = false)
      ))

      transactionLog.initialize(schema)

      // Add single file
      val singleAdd = AddAction(
        path = "single.split",
        partitionValues = Map.empty,
        size = 500L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true,
        stats = None,
        tags = None,
        numRecords = Some(50L)
      )

      val version1 = transactionLog.addFile(singleAdd)
      version1 shouldBe 1L

      // Add batch of files
      val batchAdds = Seq(
        AddAction(
          path = "batch1.split",
          partitionValues = Map.empty,
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          stats = None,
          tags = None,
          numRecords = Some(100L)
        ),
        AddAction(
          path = "batch2.split",
          partitionValues = Map.empty,
          size = 2000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          stats = None,
          tags = None,
          numRecords = Some(200L)
        )
      )

      val version2 = transactionLog.addFiles(batchAdds)
      version2 shouldBe 2L

      // Verify all files are present
      val listedFiles = transactionLog.listFiles()
      listedFiles should have length 3

      val filePaths = listedFiles.map(_.path).toSet
      filePaths should contain("single.split")
      filePaths should contain("batch1.split") 
      filePaths should contain("batch2.split")

      println("✅ Mixed operation test passed!")
      println(s"   - Individual add created version $version1")
      println(s"   - Batch add created version $version2")
      println(s"   - Total files: ${listedFiles.length}")
      } finally {
        transactionLog.close()
      }
    }
  }
}