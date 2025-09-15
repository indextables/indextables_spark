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

package com.tantivy4spark.indexing

import com.tantivy4spark.TestBase
import org.scalatest.matchers.should.Matchers

class SimpleIndexingTest extends TestBase with Matchers {

  test("basic write and read should work") {
    withTempPath { tablePath =>
      val data = spark.createDataFrame(Seq(
        ("doc1", "content one")
      )).toDF("id", "content")

      // Write data using V2 provider
      data.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").mode("overwrite").save(tablePath)

      // Read back using V2 provider
      val df = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(tablePath)
      val results = df.collect()

      results should have length 1
      results(0).getString(0) should be("doc1")
      results(0).getString(1) should be("content one")
    }
  }
}