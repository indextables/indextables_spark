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
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TransactionLogTest extends TestBase {

  test("should initialize transaction log with schema") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)
      val testSchema     = getTestSchema()

      try {
        transactionLog.initialize(testSchema)

        val cloudProvider = CloudStorageProviderFactory.createProvider(
          tablePath.toString,
          new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
          spark.sparkContext.hadoopConfiguration
        )
        try {
          val transactionLogPath = new Path(tablePath, "_transaction_log")

          cloudProvider.exists(transactionLogPath.toString) shouldBe true
          cloudProvider.exists(new Path(transactionLogPath, "00000000000000000000.json").toString) shouldBe true

          val retrievedSchema = transactionLog.getSchema()
          retrievedSchema shouldBe defined
          retrievedSchema.get shouldBe testSchema
        } finally
          cloudProvider.close()
      } finally
        transactionLog.close()
    }
  }

  test("should add and list files") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        val addAction1 = AddAction(
          path = "file1.tnt4s",
          partitionValues = Map.empty,
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(100L)
        )

        val addAction2 = AddAction(
          path = "file2.tnt4s",
          partitionValues = Map("year" -> "2023"),
          size = 2000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(200L)
        )

        val version1 = transactionLog.addFile(addAction1)
        val version2 = transactionLog.addFile(addAction2)

        version1 shouldBe 1L
        version2 shouldBe 2L

        val files = transactionLog.listFiles()
        files should have length 2
        files should contain(addAction1)
        files should contain(addAction2)
      } finally
        transactionLog.close()
    }
  }

  test("should handle file removal") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        val addAction = AddAction(
          path = "file1.tnt4s",
          partitionValues = Map.empty,
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(100L)
        )

        transactionLog.addFile(addAction)

        val filesBeforeRemoval = transactionLog.listFiles()
        filesBeforeRemoval should have length 1

        transactionLog.removeFile("file1.tnt4s")

        val filesAfterRemoval = transactionLog.listFiles()
        filesAfterRemoval should be(empty)
      } finally
        transactionLog.close()
    }
  }

  test("should maintain version ordering") {
    withTempPath { tempPath =>
      val tablePath      = new Path(tempPath)
      val transactionLog = TransactionLogFactory.create(tablePath, spark)

      try {
        transactionLog.initialize(getTestSchema())

        val actions = (1 to 5).map { i =>
          AddAction(
            path = s"file$i.tnt4s",
            partitionValues = Map.empty,
            size = i * 1000L,
            modificationTime = System.currentTimeMillis(),
            dataChange = true,
            numRecords = Some(i * 100L)
          )
        }

        val versions = actions.map(transactionLog.addFile)

        versions shouldBe Seq(1L, 2L, 3L, 4L, 5L)

        val files = transactionLog.listFiles()
        files should have length 5
        files.map(_.path) should contain theSameElementsAs actions.map(_.path)
      } finally
        transactionLog.close()
    }
  }

  test("should handle missing transaction log directory") {
    withTempPath { tempPath =>
      val nonExistentPath = new Path(tempPath, "nonexistent")
      val transactionLog  = TransactionLogFactory.create(nonExistentPath, spark)

      try {
        val schema = transactionLog.getSchema()
        schema shouldBe None

        val files = transactionLog.listFiles()
        files should be(empty)
      } finally
        transactionLog.close()
    }
  }
}
