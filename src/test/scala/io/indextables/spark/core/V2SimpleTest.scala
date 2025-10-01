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

package io.indextables.spark.core

import io.indextables.spark.TestBase
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/** Simple V2 test to debug issues */
class V2SimpleTest extends TestBase with BeforeAndAfterAll with BeforeAndAfterEach {

  ignore("should perform basic V2 write and read") {
    withTempPath { path =>
      // Create very simple data
      val data = spark
        .range(0, 3)
        .select(
          col("id"),
          concat(lit("item_"), col("id")).as("name")
        )

      // Write using V2 API
      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      // Read using V2 API
      val result = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      result.count() shouldBe 3
      result.collect().map(_.getString(1)) should contain theSameElementsAs Array("item_0", "item_1", "item_2")
    }
  }
}
