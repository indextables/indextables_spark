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

class FieldTypeTest extends TestBase with Matchers {

  test("explicit text field configuration should work") {
    withTempPath { tablePath =>
      val data = spark.createDataFrame(Seq(
        ("doc1", "machine learning algorithms"),
        ("doc2", "deep learning networks")
      )).toDF("id", "content")

      // Configure content field as text type explicitly
      data.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .option("spark.tantivy4spark.indexing.typemap.content", "text")
        .save(tablePath)

      val df = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(tablePath)
      val results = df.collect()

      results should have length 2
      val ids = results.map(_.getString(0)).toSet
      ids should contain("doc1")
      ids should contain("doc2")
    }
  }

  test("explicit string field configuration should work") {
    withTempPath { tablePath =>
      val data = spark.createDataFrame(Seq(
        ("doc1", "exact string"),
        ("doc2", "another exact string")
      )).toDF("id", "content")

      // Configure content field as string type explicitly
      data.write.format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .mode("overwrite")
        .option("spark.tantivy4spark.indexing.typemap.content", "string")
        .save(tablePath)

      val df = spark.read.format("com.tantivy4spark.core.Tantivy4SparkTableProvider").load(tablePath)
      val results = df.collect()

      results should have length 2
      val ids = results.map(_.getString(0)).toSet
      ids should contain("doc1")
      ids should contain("doc2")
    }
  }
}