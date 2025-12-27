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

package io.indextables.spark.performance

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import io.indextables.spark.TestBase

class BulkLoadValidationTest extends TestBase {

  private def isNativeLibraryAvailable(): Boolean =
    try {
      import io.indextables.spark.search.TantivyNative
      TantivyNative.ensureLibraryLoaded()
    } catch {
      case _: Exception => false
    }

  ignore("should write 439,999 records using V2 provider and read them back") {
    assume(isNativeLibraryAvailable(), "Native Tantivy library not available - skipping bulk load test")

    withTempPath { tempPath =>
      // Generate test data
      val testData = spark
        .range(439999)
        .select(
          col("id"),
          concat(lit("User"), col("id")).as("name"),
          (col("id") % 10).as("category")
        )

      // Write using V2 provider
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode(SaveMode.Overwrite)
        .save(tempPath)

      // Read back using V2 provider
      val readData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tempPath)
        .limit(9999999)

      // Validate count
      val actualCount = readData.count()
      actualCount shouldBe 439999
    }
  }
}
